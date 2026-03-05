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

import boto3
import google.generativeai as genai

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

S3_BUCKET     = os.environ["S3_BUCKET"]
S3_ENDPOINT   = os.environ.get("S3_ENDPOINT")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
GEMINI_MODEL  = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

genai.configure(api_key=os.environ["GEMINI_API_KEY"])
model = genai.GenerativeModel(GEMINI_MODEL)

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "auto"),
)


# ─── Prompt builders ──────────────────────────────────────────────────────────

def build_player_context(timeline: dict) -> dict:
    """Build a steamId → nickname map for the prompt (we only have steamIds)."""
    steam_ids = set()
    for round_ in timeline.get("rounds", []):
        for ev in round_.get("events", []):
            if ev.get("steamId"):
                steam_ids.add(ev["steamId"])
    return {sid: f"Player_{i+1}" for i, sid in enumerate(sorted(steam_ids))}


def render_round_for_prompt(round_: dict, player_names: dict) -> str:
    lines = [f"--- Round {round_['roundNum']} (T={round_['tStart']:.1f}s to {round_['tEnd']:.1f}s) ---"]
    for ev in round_["events"]:
        t = ev["t"]
        pid = player_names.get(ev.get("steamId", ""), ev.get("steamId", "?"))
        kind = ev["type"]

        if kind == "utterance":
            lines.append(f"  [{t:5.1f}s] 🎙️ {pid}: \"{ev['text']}\"")
        elif kind == "kill":
            victim = player_names.get(ev.get("victim", ""), ev.get("victim", "?"))
            lines.append(f"  [{t:5.1f}s] 💀 {pid} killed {victim} ({ev.get('weapon','')})")
        elif kind == "bomb_plant":
            lines.append(f"  [{t:5.1f}s] 💣 {pid} planted on {ev.get('site','?')}")
        elif kind == "bomb_defuse":
            lines.append(f"  [{t:5.1f}s] 🔧 {pid} defused")
        elif kind == "round_end":
            lines.append(f"  [{t:5.1f}s] 🏁 Round ended — {ev.get('winner','')} won ({ev.get('reason','')})")
        elif kind == "utility_thrown":
            lines.append(f"  [{t:5.1f}s] 💨 {pid} threw {ev.get('weapon','utility')}")

    return "\n".join(lines)


ROUND_ANALYSIS_SYSTEM = """You are an expert CS2 analyst specializing in team communication analysis.
You will receive a CS2 round timeline combining voice communications (🎙️) with in-game events.
Players are identified by aliases like Player_1, Player_2, etc.
Analyze the communication quality and return ONLY valid JSON with no preamble or markdown.

Your JSON must match this schema exactly:
{
  "igl_candidate": "<Player_N alias or null>",
  "toxic_utterances": [{"player": "<Player_N>", "text": "...", "reason": "..."}],
  "miscommunications": [{"player": "<Player_N>", "said": "...", "but_did": "...", "context": "..."}],
  "motivating_moments": [{"player": "<Player_N>", "text": "..."}],
  "info_quality_scores": {"<Player_N>": <0.0–10.0>, ...},
  "summary": "<2-3 sentence round communication summary>"
}

For miscommunications: compare what a player said they would do vs what the demo shows they did.
For IGL: look for imperative language, strategy calls, role assignments.
For toxicity: look for blame, insults, tilted/negative comments after deaths.
For info quality: rate how useful each player's callouts were (positions, counts, utility status).
"""

MATCH_SUMMARY_SYSTEM = """You are an expert CS2 communication analyst.
Given per-round analysis data (using Player_N aliases) and a player alias→steamId map,
produce a final match-level player assessment keyed by steamId.
Return ONLY valid JSON with no preamble or markdown.

Schema:
{
  "players": [
    {
      "steam_id": "<actual steamId from the provided map>",
      "comms_score": <0–100>,
      "igl_likelihood": <0–100>,
      "toxicity_score": <0–100>,
      "motivation_score": <0–100>,
      "info_density": <0–100>,
      "callout_accuracy": <0–100>,
      "dominant_role": "<entry|support|lurk|igl|awper|unknown>",
      "notable_moments": ["..."],
      "improvement_tips": ["..."]
    }
  ],
  "team_chemistry_score": <0–100>,
  "communication_flow": "<description>",
  "key_miscommunications": [{"round": <n>, "steam_id": "...", "description": "..."}],
  "match_summary": "<3-5 sentence overall communication summary>"
}
"""


# ─── Analysis pipeline ────────────────────────────────────────────────────────

def analyse_round(round_: dict, player_names: dict) -> dict:
    round_text = render_round_for_prompt(round_, player_names)

    # Skip rounds with no voice comms
    has_comms = any(ev["type"] == "utterance" for ev in round_.get("events", []))
    if not has_comms:
        return {
            "igl_candidate": None,
            "toxic_utterances": [],
            "miscommunications": [],
            "motivating_moments": [],
            "info_quality_scores": {},
            "summary": "No voice communications this round.",
        }

    prompt = ROUND_ANALYSIS_SYSTEM + "\n\n" + round_text
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    text = response.text.strip()
    text = text.replace("```json", "").replace("```", "").strip()
    return json.loads(text)


def aggregate_and_summarise(round_analyses: list[dict], timeline: dict, player_names: dict) -> dict:
    """Send all round analyses to Gemini for final player scoring.
    player_names is steamId → Player_N alias; we invert it for the prompt.
    """
    alias_to_steam = {alias: sid for sid, alias in player_names.items()}

    summary_input = {
        "match_id": timeline["matchId"],
        "round_count": len(round_analyses),
        "player_alias_map": alias_to_steam,
        "round_analyses": round_analyses,
    }

    prompt = MATCH_SUMMARY_SYSTEM + "\n\n" + json.dumps(summary_input, indent=2)
    response = model.generate_content(
        prompt,
        generation_config=genai.GenerationConfig(
            temperature=0.0,
            response_mime_type="application/json",
        ),
    )

    text = response.text.strip()
    text = text.replace("```json", "").replace("```", "").strip()
    return json.loads(text)


def process_session(match_id: str):
    log.info(f"Analysing match: {match_id}")
    prefix = f"matches/{match_id}"

    # Load merged timeline
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/timeline_merged.json")
    timeline = json.loads(obj["Body"].read())

    player_names = build_player_context(timeline)
    log.info(f"  Players: {player_names}")

    # Analyse each round
    round_analyses = []
    for i, round_ in enumerate(timeline.get("rounds", [])):
        log.info(f"  Analysing round {round_['roundNum']} ({i+1}/{len(timeline['rounds'])})…")
        try:
            analysis = analyse_round(round_, player_names)
            analysis["round_num"] = round_["roundNum"]
            round_analyses.append(analysis)
        except Exception as e:
            log.error(f"  Round {round_['roundNum']} analysis failed: {e}")

        time.sleep(0.5)  # Rate limit courtesy pause

    # Upload round analyses
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/round_analyses.json",
        Body=json.dumps(round_analyses, indent=2),
        ContentType="application/json",
    )

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

    # Update status → ready to surface in frontend
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/meta.json")
    meta = json.loads(obj["Body"].read())
    meta["status"] = "complete"
    meta["analysisKey"] = f"{prefix}/analysis.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/meta.json",
        Body=json.dumps(meta, indent=2),
        ContentType="application/json",
    )

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
    log.info("🤖 AI analysis worker started")
    while True:
        try:
            pending = list_pending()
            for match_id in pending:
                try:
                    process_session(match_id)
                except Exception as e:
                    log.error(f"Error analysing {match_id}: {e}")
        except Exception as e:
            log.error(f"Worker error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
