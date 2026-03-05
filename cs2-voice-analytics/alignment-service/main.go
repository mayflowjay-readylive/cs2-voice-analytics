// Alignment Service
//
// Polls S3 for sessions with status=pending_alignment.
// Merges demo parser output (from your existing Go service) with
// voice transcripts into a unified per-round timeline.
// Uploads merged timeline and triggers AI analysis.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ─── Types: Demo parser output (your existing format) ────────────────────────

type DemoEvent struct {
	Tick   int64  `json:"tick"`
	Type   string `json:"type"`   // kill, death, bomb_plant, bomb_defuse, round_end, utility_thrown, etc.
	Actor  string `json:"actor"`  // steamId
	Victim string `json:"victim"` // steamId (for kills)
	Weapon string `json:"weapon"`
	Site   string `json:"site"`
	Winner string `json:"winner"`
	Reason string `json:"reason"`
}

type DemoRound struct {
	RoundNum  int         `json:"roundNum"`
	StartTick int64       `json:"startTick"`
	EndTick   int64       `json:"endTick"`
	Events    []DemoEvent `json:"events"`
}

type DemoData struct {
	MatchID        string      `json:"matchId"`
	TickRate       int         `json:"tickRate"`
	MatchStartTick int64       `json:"matchStartTick"`
	Rounds         []DemoRound `json:"rounds"`
}

// ─── Types: Transcript output ─────────────────────────────────────────────────

type Utterance struct {
	SteamID    string  `json:"steam_id"`
	TStart     float64 `json:"t_start"` // seconds since recording start
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

type EventKind string

const (
	KindUtterance   EventKind = "utterance"
	KindKill        EventKind = "kill"
	KindDeath       EventKind = "death"
	KindBombPlant   EventKind = "bomb_plant"
	KindBombDefuse  EventKind = "bomb_defuse"
	KindRoundEnd    EventKind = "round_end"
	KindUtility     EventKind = "utility_thrown"
)

type TimelineEvent struct {
	T        float64   `json:"t"`               // seconds from match start
	Kind     EventKind `json:"type"`
	SteamID  string    `json:"steamId,omitempty"`
	Text     string    `json:"text,omitempty"`     // utterance only
	Victim   string    `json:"victim,omitempty"`   // kill only
	Weapon   string    `json:"weapon,omitempty"`
	Site     string    `json:"site,omitempty"`
	Winner   string    `json:"winner,omitempty"`
	Reason   string    `json:"reason,omitempty"`
	Confidence float64 `json:"confidence,omitempty"`
}

type MergedRound struct {
	RoundNum int             `json:"roundNum"`
	TStart   float64         `json:"tStart"`
	TEnd     float64         `json:"tEnd"`
	Events   []TimelineEvent `json:"events"`
}

type MergedTimeline struct {
	MatchID          string        `json:"matchId"`
	TickRate         int           `json:"tickRate"`
	RecordingStartMs int64         `json:"recordingStartMs"`
	AlignmentOffsetS float64       `json:"alignmentOffsetSeconds"`
	Rounds           []MergedRound `json:"rounds"`
	MergedAt         string        `json:"mergedAt"`
}

// ─── S3 session metadata ──────────────────────────────────────────────────────

// SessionMeta is a minimal view used only for status checks.
// The full meta.json is read/written as a raw map to preserve all fields.
type SessionMeta struct {
	MatchID string `json:"matchId"`
	Status  string `json:"status"`
}

// ─── Alignment logic ──────────────────────────────────────────────────────────

// tickToSeconds converts a demo tick to seconds from match start
func tickToSeconds(tick, matchStartTick int64, tickRate int) float64 {
	return float64(tick-matchStartTick) / float64(tickRate)
}

// alignTimelines merges demo data + transcript into per-round timelines.
// alignmentOffset accounts for any measured drift between bot start and
// actual match start (positive = bot started before match).
func alignTimelines(demo *DemoData, transcript *TranscriptData, alignmentOffset float64) *MergedTimeline {
	merged := &MergedTimeline{
		MatchID:          demo.MatchID,
		TickRate:         demo.TickRate,
		RecordingStartMs: transcript.RecordingStartMs,
		AlignmentOffsetS: alignmentOffset,
		MergedAt:         time.Now().UTC().Format(time.RFC3339),
	}

	for _, round := range demo.Rounds {
		tStart := tickToSeconds(round.StartTick, demo.MatchStartTick, demo.TickRate)
		tEnd := tickToSeconds(round.EndTick, demo.MatchStartTick, demo.TickRate)

		mergedRound := MergedRound{
			RoundNum: round.RoundNum,
			TStart:   tStart,
			TEnd:     tEnd,
		}

		// Add demo events for this round
		for _, ev := range round.Events {
			t := tickToSeconds(ev.Tick, demo.MatchStartTick, demo.TickRate)
			te := TimelineEvent{
				T:       t,
				Kind:    EventKind(ev.Type),
				SteamID: ev.Actor,
				Victim:  ev.Victim,
				Weapon:  ev.Weapon,
				Site:    ev.Site,
				Winner:  ev.Winner,
				Reason:  ev.Reason,
			}
			mergedRound.Events = append(mergedRound.Events, te)
		}

		// Add utterances that fall within this round's time window
		// Utterance time = t_start + alignmentOffset (bot offset from match start)
		for _, utt := range transcript.Utterances {
			uttT := utt.TStart - alignmentOffset
			if uttT >= tStart && uttT <= tEnd {
				te := TimelineEvent{
					T:          uttT,
					Kind:       KindUtterance,
					SteamID:    utt.SteamID,
					Text:       utt.Text,
					Confidence: utt.Confidence,
				}
				mergedRound.Events = append(mergedRound.Events, te)
			}
		}

		// Sort all events by time
		sort.Slice(mergedRound.Events, func(i, j int) bool {
			return mergedRound.Events[i].T < mergedRound.Events[j].T
		})

		merged.Rounds = append(merged.Rounds, mergedRound)
	}

	return merged
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
		Body:        bytesReader(data),
		ContentType: aws.String("application/json"),
	})
	return err
}

// ─── Worker loop ──────────────────────────────────────────────────────────────

func processSession(ctx context.Context, client *s3.Client, bucket, matchID string) error {
	prefix := "matches/" + matchID
	log.Printf("Aligning match: %s", matchID)

	// Load transcript
	var transcript TranscriptData
	if err := getJSON(ctx, client, bucket, prefix+"/transcript.json", &transcript); err != nil {
		return fmt.Errorf("load transcript: %w", err)
	}

	// Load demo data (written by your existing Go demo parser)
	var demo DemoData
	if err := getJSON(ctx, client, bucket, prefix+"/demo_parsed.json", &demo); err != nil {
		return fmt.Errorf("load demo: %w", err)
	}

	// Compute alignment offset.
	// With GSI, bot starts recording exactly when map.phase=live, which is the
	// same zero point as demo ticks. Offset = 0 is correct.
	// For manual /match start, a small human delay is unavoidable and acceptable.
	alignmentOffset := 0.0

	merged := alignTimelines(&demo, &transcript, alignmentOffset)

	if err := putJSON(ctx, client, bucket, prefix+"/timeline_merged.json", merged); err != nil {
		return fmt.Errorf("upload merged: %w", err)
	}

	// Update status using raw map to preserve all existing meta fields.
	// Reading into a typed struct and re-writing would silently drop any
	// fields not declared in the struct (players, recordingStartMs, etc).
	var rawMeta map[string]interface{}
	if err := getJSON(ctx, client, bucket, prefix+"/meta.json", &rawMeta); err != nil {
		return fmt.Errorf("load meta: %w", err)
	}
	rawMeta["status"] = "pending_analysis"
	rawMeta["timelineKey"] = prefix + "/timeline_merged.json"
	if err := putJSON(ctx, client, bucket, prefix+"/meta.json", rawMeta); err != nil {
		return fmt.Errorf("update meta: %w", err)
	}

	log.Printf("✅ Alignment complete for %s: %d rounds", matchID, len(merged.Rounds))
	return nil
}

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
			// Use raw map so we don't accidentally write back a trimmed struct
			var rawMeta map[string]interface{}
			if err := getJSON(ctx, client, bucket, "matches/"+matchID+"/meta.json", &rawMeta); err != nil {
				continue
			}
			status, _ := rawMeta["status"].(string)
			// Also recover sessions stuck in "aligning" due to a previous crash
			if status == "pending_alignment" || status == "aligning" {
				matchIDs = append(matchIDs, matchID)
			}
		}
	}
	return matchIDs, nil
}

func main() {
	bucket := os.Getenv("S3_BUCKET")
	pollInterval := 30 * time.Second

	ctx := context.Background()
	client, err := newS3Client(ctx)
	if err != nil {
		log.Fatalf("S3 init failed: %v", err)
	}

	log.Printf("🔗 Alignment service started, polling every %s", pollInterval)

	for {
		pending, err := listPendingSessions(ctx, client, bucket)
		if err != nil {
			log.Printf("List error: %v", err)
		} else {
			for _, matchID := range pending {
				if err := processSession(ctx, client, bucket, matchID); err != nil {
					log.Printf("Error processing %s: %v", matchID, err)
				}
			}
		}
		time.Sleep(pollInterval)
	}
}
