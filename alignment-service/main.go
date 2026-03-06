// Alignment Service
//
// Polls cs2-voice-analytics bucket for sessions with status=pending_alignment.
// For each session:
//   1. Finds the matching .dem file in oldboyz-demo-bucket by timestamp proximity
//   2. Generates a presigned URL for the demo
//   3. POSTs that URL to the demo parser service
//   4. Merges ParseResult + transcript into a per-round timeline
//   5. Writes timeline_merged.json and advances status to pending_analysis

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ─── Types: Demo parser output (matches your existing Go parser) ──────────────

type KillEvent struct {
	RoundNumber   int     `json:"roundNumber"`
	Tick          int     `json:"tick"`
	TimeSeconds   float64 `json:"timeSeconds"`
	KillerSteamID string  `json:"killerSteamId"`
	VictimSteamID string  `json:"victimSteamId"`
	Weapon        string  `json:"weapon"`
	IsHeadshot    bool    `json:"isHeadshot"`
	KillerPlace   string  `json:"killerPlace"`
	VictimPlace   string  `json:"victimPlace"`
	IsFirstKill   bool    `json:"isFirstKill"`
	IsTradeKill   bool    `json:"isTradeKill"`
	KillerSide    string  `json:"killerSide"`
	VictimSide    string  `json:"victimSide"`
}

type BombEvent struct {
	RoundNumber   int    `json:"roundNumber"`
	Tick          int    `json:"tick"`
	EventType     string `json:"eventType"` // plant_begin, planted, defuse_begin, defused, exploded
	PlayerSteamID string `json:"playerSteamId"`
	Site          string `json:"site"`
}

type RoundResult struct {
	WinnerTeam           int     `json:"winnerTeam"`
	WinReason            string  `json:"winReason"`
	Team1Score           int     `json:"team1Score"`
	Team2Score           int     `json:"team2Score"`
	RoundDurationSeconds float64 `json:"roundDurationSeconds"`
	Team1Side            string  `json:"team1Side"`
	Team2Side            string  `json:"team2Side"`
}

type PlayerStats struct {
	Name    string `json:"name"`
	SteamID string `json:"steamId"`
	Team    int    `json:"team"`
	Kills   int    `json:"kills"`
	Deaths  int    `json:"deaths"`
	Assists int    `json:"assists"`
}

type ParseResult struct {
	MapName     string        `json:"mapName"`
	Team1Name   string        `json:"team1Name"`
	Team2Name   string        `json:"team2Name"`
	ScoreTeam1  int           `json:"scoreTeam1"`
	ScoreTeam2  int           `json:"scoreTeam2"`
	Players     []PlayerStats `json:"players"`
	Rounds      []RoundResult `json:"rounds"`
	KillEvents  []KillEvent   `json:"killEvents"`
	BombEvents  []BombEvent   `json:"bombEvents"`
}

// ─── Types: Transcript ────────────────────────────────────────────────────────

type Utterance struct {
	SteamID    string  `json:"steam_id"`
	TStart     float64 `json:"t_start"`
	TEnd       float64 `json:"t_end"`
	Text       string  `json:"text"`
	Confidence float64 `json:"confidence"`
}

type TranscriptData struct {
	MatchID          string      `json:"matchId"`
	RecordingStartMs int64       `json:"recordingStartMs"`
	Utterances       []Utterance `json:"utterances"`
}

// ─── Types: Merged timeline ───────────────────────────────────────────────────

type TimelineEvent struct {
	T          float64 `json:"t"`
	Kind       string  `json:"type"`
	SteamID    string  `json:"steamId,omitempty"`
	Text       string  `json:"text,omitempty"`
	Victim     string  `json:"victim,omitempty"`
	Weapon     string  `json:"weapon,omitempty"`
	Site       string  `json:"site,omitempty"`
	Winner     string  `json:"winner,omitempty"`
	Reason     string  `json:"reason,omitempty"`
	Headshot   bool    `json:"headshot,omitempty"`
	FirstKill  bool    `json:"firstKill,omitempty"`
	TradeKill  bool    `json:"tradeKill,omitempty"`
	Side       string  `json:"side,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type MergedRound struct {
	RoundNum  int             `json:"roundNum"`
	WinReason string          `json:"winReason,omitempty"`
	Winner    int             `json:"winner,omitempty"`
	Events    []TimelineEvent `json:"events"`
}

type MergedTimeline struct {
	MatchID          string        `json:"matchId"`
	MapName          string        `json:"mapName"`
	Team1Name        string        `json:"team1Name"`
	Team2Name        string        `json:"team2Name"`
	ScoreTeam1       int           `json:"scoreTeam1"`
	ScoreTeam2       int           `json:"scoreTeam2"`
	RecordingStartMs int64         `json:"recordingStartMs"`
	AlignmentOffsetS float64       `json:"alignmentOffsetSeconds"`
	Rounds           []MergedRound `json:"rounds"`
	MergedAt         string        `json:"mergedAt"`
}

// ─── S3 client setup ──────────────────────────────────────────────────────────

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

func getJSON(ctx context.Context, client *s3.Client, bucket, key string, v any) error {
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer out.Body.Close()
	return json.NewDecoder(out.Body).Decode(v)
}

func putJSON(ctx context.Context, client *s3.Client, bucket, key string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

// ─── Find matching demo in oldboyz-demo-bucket ────────────────────────────────

// Demo files are named: demo_{timestampMs}_{random}.dem
// We match by finding the demo whose timestamp is closest to recordingStartMs,
// within a ±30 minute window.
func findMatchingDemo(ctx context.Context, client *s3.Client, demoBucket string, recordingStartMs int64) (string, error) {
	var allKeys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(demoBucket),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return "", fmt.Errorf("list demos: %w", err)
		}
		for _, obj := range page.Contents {
			if strings.HasSuffix(*obj.Key, ".dem") {
				allKeys = append(allKeys, *obj.Key)
			}
		}
	}

	if len(allKeys) == 0 {
		return "", fmt.Errorf("no demo files found in %s", demoBucket)
	}

	bestKey := ""
	bestDelta := int64(math.MaxInt64)
	windowMs := int64(30 * 60 * 1000) // 30 minutes

	for _, key := range allKeys {
		// Parse timestamp from filename: demo_{timestamp}_{random}.dem
		base := strings.TrimSuffix(key, ".dem")
		parts := strings.Split(base, "_")
		if len(parts) < 2 {
			continue
		}
		ts, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			continue
		}
		delta := ts - recordingStartMs
		if delta < 0 {
			delta = -delta
		}
		if delta < bestDelta && delta < windowMs {
			bestDelta = delta
			bestKey = key
		}
	}

	if bestKey == "" {
		return "", fmt.Errorf("no demo found within 30 minutes of recording start (recordingStartMs=%d)", recordingStartMs)
	}

	log.Printf("  Matched demo: %s (delta: %dms)", bestKey, bestDelta)
	return bestKey, nil
}

// ─── Generate presigned URL for demo ─────────────────────────────────────────

func presignDemoURL(ctx context.Context, client *s3.Client, bucket, key string) (string, error) {
	presignClient := s3.NewPresignClient(client)
	req, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(o *s3.PresignOptions) {
		o.Expires = 30 * time.Minute
	})
	if err != nil {
		return "", fmt.Errorf("presign: %w", err)
	}
	return req.URL, nil
}

// ─── Call demo parser service ─────────────────────────────────────────────────

func callDemoParser(parserURL, demoURL string) (*ParseResult, error) {
	body, _ := json.Marshal(map[string]string{"url": demoURL})
	req, err := http.NewRequest("POST", parserURL+"/parse", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	secret := os.Getenv("PARSER_SECRET")
	if secret != "" {
		req.Header.Set("X-Parse-Secret", secret)
	}

	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("parser request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("parser returned %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var result ParseResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode parser response: %w", err)
	}
	return &result, nil
}

// ─── Alignment logic ──────────────────────────────────────────────────────────

// buildRoundTimeBoundaries infers the approximate game-time boundaries for each
// round by looking at the min/max TimeSeconds across all kill events in that round.
// These are approximate — used only to assign utterances to rounds.
func buildRoundTimeBoundaries(parsed *ParseResult) map[int][2]float64 {
	bounds := make(map[int][2]float64)
	for _, ke := range parsed.KillEvents {
		if ke.RoundNumber <= 0 {
			continue
		}
		b, ok := bounds[ke.RoundNumber]
		if !ok {
			bounds[ke.RoundNumber] = [2]float64{ke.TimeSeconds, ke.TimeSeconds}
		} else {
			if ke.TimeSeconds < b[0] {
				b[0] = ke.TimeSeconds
			}
			if ke.TimeSeconds > b[1] {
				b[1] = ke.TimeSeconds
			}
			bounds[ke.RoundNumber] = b
		}
	}
	// Expand each round window: start 2 minutes before first kill, end 30s after last
	expanded := make(map[int][2]float64)
	for round, b := range bounds {
		start := b[0] - 120.0
		if start < 0 {
			start = 0
		}
		expanded[round] = [2]float64{start, b[1] + 30.0}
	}
	return expanded
}

// findRoundForTime returns the round number that best fits the given game time.
// Falls back to the nearest round if no exact match.
func findRoundForTime(t float64, bounds map[int][2]float64) int {
	for round, b := range bounds {
		if t >= b[0] && t <= b[1] {
			return round
		}
	}
	// Fallback: nearest round by distance to window
	bestRound := 1
	bestDist := math.MaxFloat64
	for round, b := range bounds {
		mid := (b[0] + b[1]) / 2
		dist := math.Abs(t - mid)
		if dist < bestDist {
			bestDist = dist
			bestRound = round
		}
	}
	return bestRound
}

func alignTimelines(parsed *ParseResult, transcript *TranscriptData, alignmentOffset float64) *MergedTimeline {
	merged := &MergedTimeline{
		MatchID:          transcript.MatchID,
		MapName:          parsed.MapName,
		Team1Name:        parsed.Team1Name,
		Team2Name:        parsed.Team2Name,
		ScoreTeam1:       parsed.ScoreTeam1,
		ScoreTeam2:       parsed.ScoreTeam2,
		RecordingStartMs: transcript.RecordingStartMs,
		AlignmentOffsetS: alignmentOffset,
		MergedAt:         time.Now().UTC().Format(time.RFC3339),
	}

	// Build per-round event maps
	roundEvents := make(map[int][]TimelineEvent)

	// Add kill events
	for _, ke := range parsed.KillEvents {
		roundEvents[ke.RoundNumber] = append(roundEvents[ke.RoundNumber], TimelineEvent{
			T:         ke.TimeSeconds,
			Kind:      "kill",
			SteamID:   ke.KillerSteamID,
			Victim:    ke.VictimSteamID,
			Weapon:    ke.Weapon,
			Headshot:  ke.IsHeadshot,
			FirstKill: ke.IsFirstKill,
			TradeKill: ke.IsTradeKill,
			Side:      ke.KillerSide,
		})
	}

	// Add bomb events (no TimeSeconds in parser output, group by round only)
	for _, be := range parsed.BombEvents {
		// Only include meaningful bomb events
		if be.EventType == "planted" || be.EventType == "defused" || be.EventType == "exploded" {
			roundEvents[be.RoundNumber] = append(roundEvents[be.RoundNumber], TimelineEvent{
				Kind:    be.EventType,
				SteamID: be.PlayerSteamID,
				Site:    be.Site,
			})
		}
	}

	// Build round time boundaries for utterance assignment
	roundBounds := buildRoundTimeBoundaries(parsed)

	// Add utterances — convert recording-relative time to game time
	for _, utt := range transcript.Utterances {
		gameTime := utt.TStart - alignmentOffset
		round := findRoundForTime(gameTime, roundBounds)
		roundEvents[round] = append(roundEvents[round], TimelineEvent{
			T:          gameTime,
			Kind:       "utterance",
			SteamID:    utt.SteamID,
			Text:       utt.Text,
			Confidence: utt.Confidence,
		})
	}

	// Build sorted round list
	var roundNums []int
	for r := range roundEvents {
		roundNums = append(roundNums, r)
	}
	// Also include rounds that had results but no kill events
	for i := range parsed.Rounds {
		rn := i + 1
		if _, ok := roundEvents[rn]; !ok {
			roundEvents[rn] = nil
		}
		roundNums = append(roundNums, rn)
	}
	// Deduplicate and sort
	seen := make(map[int]bool)
	var uniqueRounds []int
	for _, r := range roundNums {
		if !seen[r] {
			seen[r] = true
			uniqueRounds = append(uniqueRounds, r)
		}
	}
	sort.Ints(uniqueRounds)

	for _, rn := range uniqueRounds {
		events := roundEvents[rn]

		// Sort events by time (bomb events with T=0 go to end of round)
		sort.Slice(events, func(i, j int) bool {
			if events[i].T == 0 && events[j].T != 0 {
				return false
			}
			if events[j].T == 0 && events[i].T != 0 {
				return true
			}
			return events[i].T < events[j].T
		})

		mr := MergedRound{
			RoundNum: rn,
			Events:   events,
		}

		// Attach round result if available
		if rn-1 < len(parsed.Rounds) {
			rr := parsed.Rounds[rn-1]
			mr.WinReason = rr.WinReason
			mr.Winner = rr.WinnerTeam
		}

		merged.Rounds = append(merged.Rounds, mr)
	}

	return merged
}

// ─── Session processing ───────────────────────────────────────────────────────

func processSession(ctx context.Context, client *s3.Client, voiceBucket, demoBucket, parserURL, matchID string) error {
	prefix := "matches/" + matchID
	log.Printf("Aligning match: %s", matchID)

	// Load transcript
	var transcript TranscriptData
	if err := getJSON(ctx, client, voiceBucket, prefix+"/transcript.json", &transcript); err != nil {
		return fmt.Errorf("load transcript: %w", err)
	}
	log.Printf("  Transcript: %d utterances, recordingStartMs=%d", len(transcript.Utterances), transcript.RecordingStartMs)

	// Load meta for recordingStartMs (may differ from transcript if GSI timing)
	var rawMeta map[string]interface{}
	if err := getJSON(ctx, client, voiceBucket, prefix+"/meta.json", &rawMeta); err != nil {
		return fmt.Errorf("load meta: %w", err)
	}

	recordingStartMs := transcript.RecordingStartMs
	if v, ok := rawMeta["recordingStartMs"]; ok {
		if f, ok := v.(float64); ok {
			recordingStartMs = int64(f)
		}
	}

	// Find matching demo
	demoKey, err := findMatchingDemo(ctx, client, demoBucket, recordingStartMs)
	if err != nil {
		return fmt.Errorf("find demo: %w", err)
	}

	// Generate presigned URL
	demoURL, err := presignDemoURL(ctx, client, demoBucket, demoKey)
	if err != nil {
		return fmt.Errorf("presign demo: %w", err)
	}

	// Call demo parser
	log.Printf("  Calling parser for %s...", demoKey)
	parsed, err := callDemoParser(parserURL, demoURL)
	if err != nil {
		return fmt.Errorf("parse demo: %w", err)
	}
	log.Printf("  Parsed: map=%s, rounds=%d, kills=%d", parsed.MapName, len(parsed.Rounds), len(parsed.KillEvents))

	// Alignment offset:
	// With GSI auto-start, recording starts when map.phase=live which is
	// the same zero point as demo match start, so offset = 0.
	// For manual /match start there's a small human delay but it's acceptable.
	alignmentOffset := 0.0

	// Build merged timeline
	timeline := alignTimelines(parsed, &transcript, alignmentOffset)

	// Upload timeline
	if err := putJSON(ctx, client, voiceBucket, prefix+"/timeline_merged.json", timeline); err != nil {
		return fmt.Errorf("upload timeline: %w", err)
	}

	// Update status
	rawMeta["status"] = "pending_analysis"
	rawMeta["timelineKey"] = prefix + "/timeline_merged.json"
	rawMeta["mapName"] = parsed.MapName
	rawMeta["demoKey"] = demoKey
	if err := putJSON(ctx, client, voiceBucket, prefix+"/meta.json", rawMeta); err != nil {
		return fmt.Errorf("update meta: %w", err)
	}

	totalEvents := 0
	for _, r := range timeline.Rounds {
		totalEvents += len(r.Events)
	}
	log.Printf("✅ Alignment complete for %s: %d rounds, %d total events", matchID, len(timeline.Rounds), totalEvents)
	return nil
}

// ─── Poll for pending sessions ────────────────────────────────────────────────

func listPendingSessions(ctx context.Context, client *s3.Client, bucket string) ([]string, error) {
	var matchIDs []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String("matches/"),
		Delimiter: aws.String("/"),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			matchID := (*cp.Prefix)[len("matches/") : len(*cp.Prefix)-1]
			var rawMeta map[string]interface{}
			if err := getJSON(ctx, client, bucket, "matches/"+matchID+"/meta.json", &rawMeta); err != nil {
				continue
			}
			status, _ := rawMeta["status"].(string)
			if status == "pending_alignment" || status == "aligning" {
				matchIDs = append(matchIDs, matchID)
			}
		}
	}
	return matchIDs, nil
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	voiceBucket := os.Getenv("S3_BUCKET")          // cs2-voice-analytics
	demoBucket := os.Getenv("DEMO_BUCKET")          // oldboyz-demo-bucket
	parserURL := os.Getenv("PARSER_URL")            // e.g. https://your-parser.railway.app
	pollInterval := 30 * time.Second

	if voiceBucket == "" || demoBucket == "" || parserURL == "" {
		log.Fatalf("Missing required env vars: S3_BUCKET, DEMO_BUCKET, PARSER_URL")
	}

	ctx := context.Background()
	client, err := newS3Client(ctx)
	if err != nil {
		log.Fatalf("S3 init failed: %v", err)
	}

	log.Printf("🔗 Alignment service started")
	log.Printf("   Voice bucket: %s", voiceBucket)
	log.Printf("   Demo bucket:  %s", demoBucket)
	log.Printf("   Parser URL:   %s", parserURL)
	log.Printf("   Poll interval: %s", pollInterval)

	for {
		pending, err := listPendingSessions(ctx, client, voiceBucket)
		if err != nil {
			log.Printf("List error: %v", err)
		} else if len(pending) > 0 {
			log.Printf("Found %d pending session(s): %v", len(pending), pending)
			for _, matchID := range pending {
				if err := processSession(ctx, client, voiceBucket, demoBucket, parserURL, matchID); err != nil {
					log.Printf("❌ Error processing %s: %v", matchID, err)
					// Mark as error so it doesn't keep retrying
					prefix := "matches/" + matchID
					var rawMeta map[string]interface{}
					if getErr := getJSON(ctx, client, voiceBucket, prefix+"/meta.json", &rawMeta); getErr == nil {
						rawMeta["status"] = "error_alignment"
						rawMeta["error"] = err.Error()
						putJSON(ctx, client, voiceBucket, prefix+"/meta.json", rawMeta)
					}
				}
			}
		}
		time.Sleep(pollInterval)
	}
}
