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
	"sort"
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
	mux.HandleFunc("/retry/", handleRetry)
	mux.HandleFunc("/sessions/link", handleSessionsLink)
	mux.HandleFunc("/bot/enabled", handleBotEnabled)
	mux.HandleFunc("/bot/excluded-players", handleExcludedPlayers)
	mux.HandleFunc("/pipeline/recent", handlePipelineRecent)

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

// r2ObjectExists returns true if the key exists in R2.
func r2ObjectExists(ctx context.Context, key string) bool {
	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err == nil
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

// ─── /retry/{matchId} ────────────────────────────────────────────────────────
//
// Smart retry: checks whether transcript.json actually exists before deciding
// whether to reset to pending_alignment or pending_transcription.

func handleRetry(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	if r.Method != "POST" {
		http.Error(w, `{"error":"method not allowed"}`, 405)
		return
	}

	matchID := extractMatchID(r.URL.Path, "/retry/")
	if matchID == "" {
		http.Error(w, `{"error":"matchId required"}`, 400)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	metaKey := fmt.Sprintf("matches/%s/meta.json", matchID)
	data, err := getR2Json(ctx, metaKey)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "match not found in voice pipeline"})
		return
	}

	currentStatus, _ := data["status"].(string)

	// Determine the correct restart point by checking which files actually exist.
	transcriptKey := fmt.Sprintf("matches/%s/transcript.json", matchID)
	hasTranscript := r2ObjectExists(ctx, transcriptKey)

	var newStatus string
	var reason string

	switch {
	case strings.HasPrefix(currentStatus, "error_transcription"),
		currentStatus == "transcribing":
		newStatus = "pending_transcription"
		reason = "resetting to transcription"

	case strings.HasPrefix(currentStatus, "error_alignment"),
		currentStatus == "aligning":
		if hasTranscript {
			newStatus = "pending_alignment"
			reason = "transcript exists — resetting to alignment"
		} else {
			newStatus = "pending_transcription"
			reason = "transcript missing — resetting to transcription"
		}

	case strings.HasPrefix(currentStatus, "error_analysis"),
		currentStatus == "analyzing":
		newStatus = "pending_analysis"
		reason = "resetting to analysis"

	case currentStatus == "complete":
		newStatus = "pending_analysis"
		reason = "re-running analysis"

	case currentStatus == "pending_transcription",
		currentStatus == "pending_alignment",
		currentStatus == "pending_analysis":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"matchId": matchID,
			"status":  currentStatus,
			"message": "Already pending — pipeline will pick it up shortly",
		})
		return

	default:
		// Unknown status — use file presence to decide
		if hasTranscript {
			newStatus = "pending_alignment"
			reason = "unknown status, transcript exists — resetting to alignment"
		} else {
			newStatus = "pending_transcription"
			reason = "unknown status, no transcript — resetting to transcription"
		}
	}

	data["status"] = newStatus
	data["statusUpdatedAt"] = time.Now().UTC().Format(time.RFC3339)
	delete(data, "error")
	delete(data, "blocked")
	data["retryCount"] = 0

	if err := putR2Json(ctx, metaKey, data); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	log.Printf("🔄 Retry for %s: %s → %s (%s)", matchID, currentStatus, newStatus, reason)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"matchId":        matchID,
		"previousStatus": currentStatus,
		"newStatus":      newStatus,
		"reason":         reason,
		"message":        "Pipeline will retry within 30 seconds",
	})
}

// ─── /pipeline/recent ────────────────────────────────────────────────────────
//
// Returns the N most recent matches with their pipeline status, per-player
// transcription progress, file presence flags, and any recorded errors.
// Used by the Lovable frontend to show a match health dashboard.

func handlePipelineRecent(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	if r.Method != "GET" {
		http.Error(w, `{"error":"method not allowed"}`, 405)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	prefixes, err := listR2Prefixes(ctx, "matches/", "/")
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	type matchSummary struct {
		MatchID                string                 `json:"matchId"`
		Status                 string                 `json:"status"`
		StatusUpdatedAt        string                 `json:"statusUpdatedAt"`
		Map                    string                 `json:"map,omitempty"`
		PlayerCount            int                    `json:"playerCount"`
		CompletedPlayers       int                    `json:"completedPlayers"`
		Error                  string                 `json:"error,omitempty"`
		RetryCount             int                    `json:"retryCount"`
		TranscriptionProgress  map[string]interface{} `json:"transcriptionProgress,omitempty"`
		HasTranscript          bool                   `json:"hasTranscript"`
		HasAnalysis            bool                   `json:"hasAnalysis"`
		HasErrors              bool                   `json:"hasErrors"`
		RecordingStartMs       int64                  `json:"recordingStartMs,omitempty"`
	}

	var summaries []matchSummary

	for _, prefix := range prefixes {
		matchID := strings.Trim(strings.TrimPrefix(prefix, "matches/"), "/")
		if matchID == "" {
			continue
		}

		meta, err := getR2Json(ctx, prefix+"meta.json")
		if err != nil {
			continue
		}

		ms := matchSummary{
			MatchID: matchID,
		}

		ms.Status, _ = meta["status"].(string)
		ms.StatusUpdatedAt, _ = meta["statusUpdatedAt"].(string)
		ms.Map, _ = meta["map"].(string)
		ms.Error, _ = meta["error"].(string)

		if rc, ok := meta["retryCount"].(float64); ok {
			ms.RetryCount = int(rc)
		}
		if rs, ok := meta["recordingStartMs"].(float64); ok {
			ms.RecordingStartMs = int64(rs)
		}

		if players, ok := meta["players"].([]interface{}); ok {
			ms.PlayerCount = len(players)
		}

		if tp, ok := meta["transcriptionProgress"].(map[string]interface{}); ok {
			ms.TranscriptionProgress = tp
			done := 0
			for _, v := range tp {
				if vm, ok := v.(map[string]interface{}); ok {
					if s, _ := vm["status"].(string); s == "done" || s == "skipped_too_short" || s == "skipped_no_audio" {
						done++
					}
				}
			}
			ms.CompletedPlayers = done
		}

		// Check file presence (fast HeadObject calls)
		ms.HasTranscript = r2ObjectExists(ctx, fmt.Sprintf("matches/%s/transcript.json", matchID))
		ms.HasAnalysis = r2ObjectExists(ctx, fmt.Sprintf("matches/%s/analysis.json", matchID))
		ms.HasErrors = r2ObjectExists(ctx, fmt.Sprintf("matches/%s/errors.json", matchID))

		summaries = append(summaries, ms)
	}

	// Sort by statusUpdatedAt descending (most recently updated first)
	sort.Slice(summaries, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, summaries[i].StatusUpdatedAt)
		tj, _ := time.Parse(time.RFC3339, summaries[j].StatusUpdatedAt)
		return ti.After(tj)
	})

	// Limit to 30 most recent
	if len(summaries) > 30 {
		summaries = summaries[:30]
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"matches":     summaries,
		"total":       len(summaries),
		"generatedAt": time.Now().UTC().Format(time.RFC3339),
	})
}

// ─── /bot/enabled handler ─────────────────────────────────────────────────

func handleBotEnabled(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	configKey := "config/bot_enabled.json"

	if r.Method == "GET" {
		data, err := getR2Json(ctx, configKey)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(map[string]interface{}{"enabled": true})
			return
		}

		// Auto-re-enable after 4 hours to prevent accidental lockout
		enabled, _ := data["enabled"].(bool)
		if !enabled {
			if updatedStr, ok := data["updatedAt"].(string); ok {
				if updatedAt, err := time.Parse(time.RFC3339, updatedStr); err == nil {
					if time.Since(updatedAt) > 4*time.Hour {
						log.Printf("🤖 Bot was disabled for >4 hours — auto-re-enabling")
						data["enabled"] = true
						data["updatedAt"] = time.Now().UTC().Format(time.RFC3339)
						data["autoReenabled"] = true
						putR2Json(ctx, configKey, data)
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(data)
		return
	}

	if r.Method == "POST" {
		var req struct {
			Enabled bool `json:"enabled"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
			return
		}

		data := map[string]interface{}{
			"enabled":   req.Enabled,
			"updatedAt": time.Now().UTC().Format(time.RFC3339),
		}
		if err := putR2Json(ctx, configKey, data); err != nil {
			log.Printf("[bot/enabled] Failed to save: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		log.Printf("🤖 Bot enabled set to: %v", req.Enabled)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(data)
		return
	}

	http.Error(w, `{"error":"method not allowed"}`, 405)
}

// ─── /bot/excluded-players handler ────────────────────────────────────────

func handleExcludedPlayers(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	configKey := "config/excluded_players.json"

	if r.Method == "GET" {
		data, err := getR2Json(ctx, configKey)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(200)
			json.NewEncoder(w).Encode(map[string]interface{}{"players": []interface{}{}})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(data)
		return
	}

	if r.Method == "POST" {
		var req map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON"})
			return
		}

		req["updatedAt"] = time.Now().UTC().Format(time.RFC3339)

		if err := putR2Json(ctx, configKey, req); err != nil {
			log.Printf("[excluded-players] Failed to save: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(500)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		log.Printf("🚫 Excluded players updated")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		json.NewEncoder(w).Encode(req)
		return
	}

	http.Error(w, `{"error":"method not allowed"}`, 405)
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
		MatchDate   string   `json:"matchDate"`
		DemoMatchId string   `json:"demoMatchId"`
		PlayerIds   []string `json:"playerIds"`
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
		targetTs, err = time.Parse("2006-01-02T15:04:05Z", req.MatchDate)
		if err != nil {
			targetTs, err = time.Parse("2006-01-02T15:04:05.000Z", req.MatchDate)
			if err != nil {
				targetTs, err = time.Parse("2006-01-02 15:04:05+00", req.MatchDate)
				if err != nil {
					targetTs, err = time.Parse("2006-01-02 15:04:05.000+00", req.MatchDate)
					if err != nil {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(400)
						json.NewEncoder(w).Encode(map[string]string{"error": "invalid matchDate format"})
						return
					}
				}
			}
		}
	}
	targetMs := targetTs.UnixMilli()

	demoPlayers := make(map[string]bool)
	for _, pid := range req.PlayerIds {
		if pid != "" && !strings.HasPrefix(pid, "discord_") {
			demoPlayers[pid] = true
		}
	}

	if len(demoPlayers) == 0 {
		parseKey := "matches/" + req.DemoMatchId + "/parse_result.json"
		if prData, err := getR2Json(ctx, parseKey); err == nil {
			if players, ok := prData["players"].([]interface{}); ok {
				for _, p := range players {
					if pm, ok := p.(map[string]interface{}); ok {
						if sid, ok := pm["steamId"].(string); ok && sid != "" {
							demoPlayers[sid] = true
						}
					}
				}
			}
		}
	}

	log.Printf("[link] Matching for %s (time: %s, demo players: %d)", req.DemoMatchId, targetTs.Format(time.RFC3339), len(demoPlayers))

	prefixes, err := listR2Prefixes(ctx, "matches/", "/")
	if err != nil {
		log.Printf("[link] List error: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	type sessionInfo struct {
		matchId       string
		startedAt     int64
		timeDiff      int64
		playerIds     []string
		playerOverlap int
		score         float64
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
		if diff >= threeHoursMs {
			continue
		}

		var voicePlayers []string
		if players, ok := meta["players"].([]interface{}); ok {
			for _, p := range players {
				if pm, ok := p.(map[string]interface{}); ok {
					if sid, ok := pm["steamId"].(string); ok && sid != "" && !strings.HasPrefix(sid, "discord_") {
						voicePlayers = append(voicePlayers, sid)
					}
				}
			}
		}

		overlap := 0
		if len(demoPlayers) > 0 {
			for _, vid := range voicePlayers {
				if demoPlayers[vid] {
					overlap++
				}
			}
		}

		timeDiffSec := float64(diff) / 1000.0
		score := float64(overlap)*1000.0 - timeDiffSec

		// Strongly prefer sessions that are pending_transcription (fresh uploads)
		// over sessions that already have errors or completed transcripts.
		// This prevents old failed sessions from being picked over tonight's recording
		// when player overlap is 0 (e.g. all discord_ IDs due to missing steam_links).
		status, _ := meta["status"].(string)
		switch status {
		case "pending_transcription":
			score += 5000.0 // strong preference for fresh sessions
		case "error_transcription", "error_alignment", "error_analysis":
			score -= 5000.0 // strongly deprioritize failed sessions
		case "complete":
			score -= 10000.0 // never pick already-completed sessions
		}

		candidates = append(candidates, sessionInfo{
			matchId:       matchId,
			startedAt:     startedAt,
			timeDiff:      diff,
			playerIds:     voicePlayers,
			playerOverlap: overlap,
			score:         score,
		})
	}

	if len(candidates) == 0 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "No matching voice session found within 3 hours of matchDate"})
		return
	}

	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.score > best.score {
			best = c
		}
	}

	log.Printf("[link] Best match: %s (overlap: %d/%d players, timeDiff: %ds, score: %.1f)",
		best.matchId, best.playerOverlap, len(demoPlayers), best.timeDiff/1000, best.score)

	oldPrefix := "matches/" + best.matchId + "/"
	newPrefix := "matches/" + req.DemoMatchId + "/"

	keys, err := listR2Keys(ctx, oldPrefix)
	if err != nil || len(keys) == 0 {
		log.Printf("[link] No objects found under %s: %v", oldPrefix, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(500)
		json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf("No objects found under %s", oldPrefix)})
		return
	}

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

	for _, key := range keys {
		if err := deleteR2Object(ctx, key); err != nil {
			log.Printf("[link] Delete error %s: %v (non-fatal)", key, err)
		}
	}

	newMeta, err := getR2Json(ctx, newPrefix+"meta.json")
	if err == nil {
		newMeta["matchId"] = req.DemoMatchId

		if players, ok := newMeta["players"].([]interface{}); ok {
			for _, p := range players {
				if pm, ok := p.(map[string]interface{}); ok {
					if audioKey, ok := pm["audioKey"].(string); ok {
						parts := strings.Split(audioKey, "/")
						filename := parts[len(parts)-1]
						pm["audioKey"] = newPrefix + filename
					}
				}
			}
		}

		if tk, ok := newMeta["transcriptKey"].(string); ok && strings.Contains(tk, best.matchId) {
			newMeta["transcriptKey"] = strings.Replace(tk, best.matchId, req.DemoMatchId, 1)
		}

		putR2Json(ctx, newPrefix+"meta.json", newMeta)
	}

	log.Printf("✅ Linked session %s → %s (overlap: %d, diff: %ds)",
		best.matchId, req.DemoMatchId, best.playerOverlap, best.timeDiff/1000)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"linked":          true,
		"oldMatchId":      best.matchId,
		"newMatchId":      req.DemoMatchId,
		"timeDiffSeconds": int(math.Round(float64(best.timeDiff) / 1000)),
		"playerOverlap":   best.playerOverlap,
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
