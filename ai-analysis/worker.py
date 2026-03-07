"""
AI Analysis Service
===================
Polls S3 for sessions with status=pending_analysis.
Feeds merged round timelines to Gemini and produces structured
per-player and per-match insights.
"""

import os
import json
import time
import logging
from datetime import datetime, timezone

import boto3
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

S3_BUCKET     = os.environ["S3_BUCKET"]
S3_ENDPOINT   = os.environ.get("S3_ENDPOINT")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
GEMINI_MODEL  = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

STALE_THRESHOLD_SECONDS = 900  # 15 minutes
MAX_RETRIES = 3

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "auto"),
)


# ─── Status helpers ───────────────────────────────────────────────────────────

def get_meta(match_id: str) -> dict:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
    return json.loads(obj["Body"].read())


def set_status(match_id: str, status: str, extra: dict = None):
    meta = get_meta(match_id)
    meta["status"] = status
    meta["statusUpdatedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if extra:
        meta.update(extra)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"matches/{match_id}/meta.json",
        Body=json.dumps(meta, indent=2),
        ContentType="application/json",
    )


# ─── Stuck session recovery ──────────────────────────────────────────────────

def check_stale_sessions():
    """Find sessions stuck in 'analyzing' and reset or error them."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match_prefix = prefix["Prefix"]
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{match_prefix}meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") != "analyzing":
                    continue

                # Check staleness using statusUpdatedAt, fall back to S3 LastModified
                updated_at = None
                if meta.get("statusUpdatedAt"):
                    updated_at = datetime.fromisoformat(meta["statusUpdatedAt"].replace("Z", "+00:00"))
                else:
                    updated_at = obj["LastModified"]

                age = datetime.now(timezone.utc) - updated_at
                if age.total_seconds() < STALE_THRESHOLD_SECONDS:
                    continue

                match_id = meta["matchId"]
                retry_count = meta.get("retryCount", 0)

                if retry_count >= MAX_RETRIES:
                    log.warning(f"⛔ Session {match_id} stuck in 'analyzing' after {MAX_RETRIES} retries — marking as error")
                    set_status(match_id, "error_analysis", {"error": f"Stuck in analyzing after {MAX_RETRIES} retries"})
                else:
                    log.warning(f"🔄 Session {match_id} stuck in 'analyzing' for {int(age.total_seconds())}s — resetting (retry {retry_count + 1}/{MAX_RETRIES})")
                    set_status(match_id, "pending_analysis", {"retryCount": retry_count + 1})

            except Exception:
                pass


# ─── Prompt builders ──────────────────────────────────────────────────────────

def build_player_context(timeline: dict) -> dict:
    """Build a steamId → Player_N alias map from all events in the timeline."""
    steam_ids = set()
    for round_ in timeline.get("rounds", []):
        for ev in round_.get("events", []):
            if ev.get("steamId"):
                steam_ids.add(ev["steamId"])
    return {sid: f"Player_{i+1}" for i, sid in enumerate(sorted(steam_ids))}


def render_round_for_prompt(round_: dict, player_names: dict) -> str:
    round_num = round_.get("roundNum", "?")
    win_reason = round_.get("winReason", "")
    winner = round_.get("winner", "")

    lines = [f"--- Round {round_num} (winner: team {winner}, reason: {win_reason}) ---"]

    for ev in round_.get("events", []):
        t = ev.get("t", 0)
        pid = player_names.get(ev.get("steamId", ""), ev.get("steamId", "?"))
        kind = ev.get("type", "")

        if kind == "utterance":
            lines.append(f"  [{t:6.1f}s] 🎙️ {pid}: \"{ev.get('text', '')}\"")
        elif kind == "kill":
            victim = player_names.get(ev.get("victim", ""), ev.get("victim", "?"))
            hs = " (HS)" if ev.get("headshot") else ""
            trade = " [TRADE]" if ev.get("tradeKill") else ""
            first = " [FIRST KILL]" if ev.get("firstKill") else ""
            lines.append(f"  [{t:6.1f}s] 💀 {pid} killed {victim} ({ev.get('weapon', '')}){hs}{trade}{first}")
        elif kind == "planted":
            lines.append(f"  [      ] 💣 {pid} planted on {ev.get('site', '?')}")
        elif kind == "defused":
            lines.append(f"  [      ] 🔧 {pid} defused on {ev.get('site', '?')}")
        elif kind == "exploded":
            lines.append(f"  [      ] 💥 Bomb exploded on {ev.get('site', '?')}")

    return "\n".join(lines)


ROUND_ANALYSIS_SYSTEM = """You are an expert CS2 analyst specializing in team communication analysis.
You will receive a CS2 round timeline combining voice communications (🎙️) with in-game events.
Players are identified by aliases like Player_1, Player_2, etc.
Voice comms may be in Danish mixed with English CS2 terms — analyse based on meaning, not language.
Analyze the communication quality and return ONLY valid JSON with no preamble or markdown.

Your JSON must match this schema exactly:
{
  "igl_candidate": "<Player_N alias or null>",
  "toxic_utterances": [{"player": "<Player_N>", "text": "...", "reason": "..."}],
  "miscommunications": [{"player": "<Player_N>", "said": "...", "but_did": "...", "context": "..."}],
  "motivating_moments": [{"player": "<Player_N>", "text": "..."}],
  "info_quality_scores": {"<Player_N>": <0.0-10.0>},
  "summary": "<2-3 sentence round communication summary>"
}

For miscommunications: compare what a player said they would do vs what the demo shows they did.
For IGL: look for imperative language, strategy calls, role assignments.
For toxicity: look for blame, insults, tilted/negative comments after deaths.
For info quality: rate how useful each player's callouts were (positions, counts, utility status).
"""

MATCH_SUMMARY_SYSTEM = """You are an expert CS2 communication analyst.
Given per-round analysis data (using Player_N aliases) and a player alias->steamId map,
produce a final match-level player assessment keyed by steamId.
Return ONLY valid JSON with no preamble or markdown.

Schema:
{
  "players": [
    {
      "steam_id": "<actual steamId>",
      "comms_score": <0-100>,
      "igl_likelihood": <0-100>,
      "toxicity_score": <0-100>,
      "motivation_score": <0-100>,
      "info_density": <0-100>,
      "callout_accuracy": <0-100>,
      "dominant_role": "<entry|support|lurk|igl|awper|unknown>",
      "notable_moments": ["..."],
      "improvement_tips": ["..."]
    }
  ],
  "team_chemistry_score": <0-100>,
  "communication_flow": "<description>",
  "key_miscommunications": [{"round": <n>, "steam_id": "...", "description": "..."}],
  "match_summary": "<3-5 sentence overall communication summary>"
}
"""


# ─── Analysis pipeline ────────────────────────────────────────────────────────

def gemini_json(prompt: str) -> dict:
    """Call Gemini and parse JSON response."""
    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )
    text = response.text.strip().replace("```json", "").replace("```", "").strip()
    return json.loads(text)


def analyse_round(round_: dict, player_names: dict) -> dict:
    # Skip rounds with no voice comms
    has_comms = any(ev.get("type") == "utterance" for ev in round_.get("events", []))
    if not has_comms:
        return {
            "igl_candidate": None,
            "toxic_utterances": [],
            "miscommunications": [],
            "motivating_moments": [],
            "info_quality_scores": {},
            "summary": "No voice communications this round.",
        }

    round_text = render_round_for_prompt(round_, player_names)
    prompt = ROUND_ANALYSIS_SYSTEM + "\n\n" + round_text
    return gemini_json(prompt)


def aggregate_and_summarise(round_analyses: list, timeline: dict, player_names: dict) -> dict:
    alias_to_steam = {alias: sid for sid, alias in player_names.items()}
    summary_input = {
        "match_id": timeline["matchId"],
        "map": timeline.get("mapName", "unknown"),
        "score": f"{timeline.get('team1Name','T1')} {timeline.get('scoreTeam1',0)} - {timeline.get('scoreTeam2',0)} {timeline.get('team2Name','T2')}",
        "round_count": len(round_analyses),
        "player_alias_map": alias_to_steam,
        "round_analyses": round_analyses,
    }
    prompt = MATCH_SUMMARY_SYSTEM + "\n\n" + json.dumps(summary_input, indent=2)
    return gemini_json(prompt)


def process_session(match_id: str):
    log.info(f"Analysing match: {match_id}")
    prefix = f"matches/{match_id}"

    # Set intermediate status so stuck detection works
    set_status(match_id, "analyzing")

    # Load merged timeline
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/timeline_merged.json")
    timeline = json.loads(obj["Body"].read())

    player_names = build_player_context(timeline)
    log.info(f"  Players: {player_names}")

    rounds = timeline.get("rounds", [])
    log.info(f"  Rounds: {len(rounds)}, map: {timeline.get('mapName', 'unknown')}")

    # Analyse each round
    round_analyses = []
    for i, round_ in enumerate(rounds):
        log.info(f"  Analysing round {round_.get('roundNum')} ({i+1}/{len(rounds)})…")
        try:
            analysis = analyse_round(round_, player_names)
            analysis["round_num"] = round_.get("roundNum")
            round_analyses.append(analysis)
        except Exception as e:
            log.error(f"  Round {round_.get('roundNum')} analysis failed: {e}")
        time.sleep(0.5)  # Rate limit courtesy pause

    # Upload round analyses
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/round_analyses.json",
        Body=json.dumps(round_analyses, indent=2),
        ContentType="application/json",
    )
    log.info(f"  Uploaded round_analyses.json ({len(round_analyses)} rounds)")

    # Final match-level summary
    log.info("  Generating match-level player scores…")
    match_summary = aggregate_and_summarise(round_analyses, timeline, player_names)
    match_summary["match_id"] = match_id
    match_summary["analysed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/analysis.json",
        Body=json.dumps(match_summary, indent=2),
        ContentType="application/json",
    )

    # Update status → complete
    set_status(match_id, "complete", {
        "analysisKey": f"{prefix}/analysis.json",
    })

    log.info(f"✅ Analysis complete for {match_id}")


def list_pending():
    pending = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            match_id = cp["Prefix"][len("matches/"):-1]
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") == "pending_analysis":
                    pending.append(match_id)
            except Exception:
                pass
    return pending


def main():
    log.info(f"🤖 AI analysis worker started (model: {GEMINI_MODEL})")
    while True:
        try:
            # Check for stuck sessions first
            check_stale_sessions()

            pending = list_pending()
            if pending:
                log.info(f"Found {len(pending)} pending session(s): {pending}")
            for match_id in pending:
                try:
                    process_session(match_id)
                except Exception as e:
                    log.error(f"Error analysing {match_id}: {e}")
                    try:
                        set_status(match_id, "error_analysis", {"error": str(e)})
                    except Exception:
                        pass
        except Exception as e:
            log.error(f"Worker error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
