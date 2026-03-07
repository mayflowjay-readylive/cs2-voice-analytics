package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	s3Client *s3.Client
	bucket   string
)

func main() {
	bucket = os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "cs2-voice-analytics"
	}

	ctx := context.Background()
	var err error
	s3Client, err = newS3Client(ctx)
	if err != nil {
		log.Fatalf("S3 init failed: %v", err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/analysis/", handleAnalysis)
	mux.HandleFunc("/rounds/", handleRounds)
	mux.HandleFunc("/status/", handleStatus)

	handler := corsMiddleware(mux)

	log.Printf("✅ Voice analytics API listening on port %s", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}

// ─── S3 client ────────────────────────────────────────────────────────────────

func newS3Client(ctx context.Context) (*s3.Client, error) {
	endpoint := os.Getenv("S3_ENDPOINT")
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "auto"
	}
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			os.Getenv("AWS_ACCESS_KEY_ID"),
			os.Getenv("AWS_SECRET_ACCESS_KEY"),
			"",
		)),
	)
	if err != nil {
		return nil, err
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		}
	}), nil
}

// ─── CORS middleware ──────────────────────────────────────────────────────────

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleAnalysis(w http.ResponseWriter, r *http.Request) {
	matchID := extractMatchID(r.URL.Path, "/analysis/")
	if matchID == "" {
		http.Error(w, `{"error":"matchId required"}`, 400)
		return
	}
	serveR2JSON(w, r, fmt.Sprintf("matches/%s/analysis.json", matchID))
}

func handleRounds(w http.ResponseWriter, r *http.Request) {
	matchID := extractMatchID(r.URL.Path, "/rounds/")
	if matchID == "" {
		http.Error(w, `{"error":"matchId required"}`, 400)
		return
	}
	serveR2JSON(w, r, fmt.Sprintf("matches/%s/round_analyses.json", matchID))
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	matchID := extractMatchID(r.URL.Path, "/status/")
	if matchID == "" {
		http.Error(w, `{"error":"matchId required"}`, 400)
		return
	}
	serveR2JSON(w, r, fmt.Sprintf("matches/%s/meta.json", matchID))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func extractMatchID(path, prefix string) string {
	id := strings.TrimPrefix(path, prefix)
	id = strings.TrimSuffix(id, "/")
	return id
}

func serveR2JSON(w http.ResponseWriter, r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "NoSuchKey") || strings.Contains(errStr, "404") || strings.Contains(errStr, "not found") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			w.Write([]byte(`{"error":"not found"}`))
			return
		}
		log.Printf("R2 error for %s: %v", key, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		w.Write([]byte(fmt.Sprintf(`{"error":"%s"}`, errStr)))
		return
	}
	defer out.Body.Close()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "public, max-age=60")
	w.WriteHeader(200)
	io.Copy(w, out.Body)
}
