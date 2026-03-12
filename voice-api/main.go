package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
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
	mux.HandleFunc("/sessions/link", handleSessionsLink)

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
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "86400")

		if r.Method == "OPTIONS" {
			w.WriteHeader(204)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// ─── R2 helpers ───────────────────────────────────────────────────────────────

func getR2Json(ctx context.Context, key string) (map[string]interface{}, error) {
	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(out.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func putR2Json(ctx context.Context, key string, data interface{}) error {
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(jsonBytes)),
		ContentType: aws.String("application/json"),
	})
	return err
}

func listR2Prefixes(ctx context.Context, prefix, delimiter string) ([]string, error) {
	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(delimiter),
	})
	if err != nil {
		return nil, err
	}
	var prefixes []string
	for _, cp := range out.CommonPrefixes {
		prefixes = append(prefixes, *cp.Prefix)
	}
	return prefixes, nil
}

func listR2Keys(ctx context.Context, prefix string) ([]string, error) {
	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}
	var keys []string
	for _, obj := range out.Contents {
		keys = append(keys, *obj.Key)
	}
	return keys, nil
}

func copyR2Object(ctx context.Context, srcKey, dstKey string) error {
	_, err := s3Client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(bucket + "/" + srcKey),
		Key:        aws.String(dstKey),
	})
	return err
}

func deleteR2Object(ctx context.Context, key string) error {
	_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
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

// ─── /sessions/link handler ───────────────────────────────────────────────────

func handleSessionsLink(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	if r.Method != "POST" {
		http.Error(w, `{"error":"method not allowed"}`, 405)
		return
	}

	var req struct {
		MatchDate   string `json:"matchDate"`
		DemoMatchId string `json:"demoMatchId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.MatchDate == "" || req.DemoMatchId == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]string{"error": "matchDate and demoMatchId required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	targetTs, err := time.Parse(time.RFC3339, req.MatchDate)
	if err != nil {
		// Try parsing as other common formats
		targetTs, err = time.Parse("2006-01-02T15:04:05Z", req.MatchDate)
		if err != nil {
			targetTs, err = time.Parse("2006-01-02T15:04:05.000Z", req.MatchDate)
			if err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(400)
				json.NewEncoder(w).Encode(map[string]string{"error": "invalid matchDate format"})
				return
			}
		}
	}
	targetMs := targetTs.UnixMilli()

	// List all sessions
	prefixes, err := listR2Prefixes(ctx, "matches/", "/")
	if err != nil {
		log.Printf("[link] List error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	// Find closest session by startedAt timestamp
	type sessionInfo struct {
		matchId   string
		startedAt int64
		diff      int64
	}

	var candidates []sessionInfo
	threeHoursMs := int64(3 * 60 * 60 * 1000)

	for _, prefix := range prefixes {
		meta, err := getR2Json(ctx, prefix+"meta.json")
		if err != nil {
			continue
		}
		matchId, _ := meta["matchId"].(string)
		if matchId == "" || matchId == req.DemoMatchId {
			continue
		}
		startedAt := int64(0)
		if v, ok := meta["startedAt"].(float64); ok {
			startedAt = int64(v)
		}
		if startedAt == 0 {
			continue
		}
		diff := targetMs - startedAt
		if diff < 0 {
			diff = -diff
		}
		if diff < threeHoursMs {
			candidates = append(candidates, sessionInfo{matchId: matchId, startedAt: startedAt, diff: diff})
		}
	}

	if len(candidates) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "No matching voice session found within 3 hours of matchDate"})
		return
	}

	// Find closest
	closest := candidates[0]
	for _, c := range candidates[1:] {
		if c.diff < closest.diff {
			closest = c
		}
	}

	// Rename session: copy all objects, then delete originals
	oldPrefix := "matches/" + closest.matchId + "/"
	newPrefix := "matches/" + req.DemoMatchId + "/"

	keys, err := listR2Keys(ctx, oldPrefix)
	if err != nil || len(keys) == 0 {
		log.Printf("[link] No objects found under %s: %v", oldPrefix, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("No objects found under %s", oldPrefix)})
		return
	}

	// Copy all objects to new prefix
	for _, key := range keys {
		newKey := newPrefix + key[len(oldPrefix):]
		if err := copyR2Object(ctx, key, newKey); err != nil {
			log.Printf("[link] Copy error %s → %s: %v", key, newKey, err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("copy failed: %v", err)})
			return
		}
	}

	// Delete old objects
	for _, key := range keys {
		if err := deleteR2Object(ctx, key); err != nil {
			log.Printf("[link] Delete error %s: %v (non-fatal)", key, err)
		}
	}

	// Update meta.json with new matchId
	newMeta, err := getR2Json(ctx, newPrefix+"meta.json")
	if err == nil {
		newMeta["matchId"] = req.DemoMatchId
		putR2Json(ctx, newPrefix+"meta.json", newMeta)
	}

	log.Printf("✅ Linked session %s → %s (diff: %ds)", closest.matchId, req.DemoMatchId, closest.diff/1000)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"linked":          true,
		"oldMatchId":      closest.matchId,
		"newMatchId":      req.DemoMatchId,
		"timeDiffSeconds": int(math.Round(float64(closest.diff) / 1000)),
	})
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
