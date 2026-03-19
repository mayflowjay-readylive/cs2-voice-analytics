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

STALE_THRESHOLD_SECONDS = 900
MAX_RETRIES = 3

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "auto"),
)


def get_meta(match_id):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
    return json.loads(obj["Body"].read())

def set_status(match_id, status, extra=None):
    meta = get_meta(match_id)
    meta["status"] = status
    meta["statusUpdatedAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    if extra: meta.update(extra)
    s3.put_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json", Body=json.dumps(meta, indent=2), ContentType="application/json")

def check_stale_sessions():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match_prefix = prefix["Prefix"]
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{match_prefix}meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") != "analyzing": continue
                updated_at = None
                if meta.get("statusUpdatedAt"):
                    updated_at = datetime.fromisoformat(meta["statusUpdatedAt"].replace("Z", "+00:00"))
                else:
                    updated_at = obj["LastModified"]
                age = datetime.now(timezone.utc) - updated_at
                if age.total_seconds() < STALE_THRESHOLD_SECONDS: continue
                match_id = meta["matchId"]
                retry_count = meta.get("retryCount", 0)
                if retry_count >= MAX_RETRIES:
                    log.warning(f"Session {match_id} stuck after {MAX_RETRIES} retries")
                    set_status(match_id, "error_analysis", {"error": f"Stuck after {MAX_RETRIES} retries"})
                else:
                    log.warning(f"Session {match_id} stuck — resetting (retry {retry_count+1}/{MAX_RETRIES})")
                    set_status(match_id, "pending_analysis", {"retryCount": retry_count + 1})
            except Exception: pass


# ─── Player profiles ─────────────────────────────────────────────────────────

def load_player_profiles():
    profiles = {}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="config/player_profiles.json")
        data = json.loads(obj["Body"].read())
        for p in data.get("players", []):
            sid = p.get("steamId", "")
            if sid:
                profiles[sid] = {
                    "gamertag": p.get("gamertag", ""),
                    "realName": p.get("realName", ""),
                    "nicknames": p.get("nicknames", []),
                    "discordId": p.get("discordId", ""),
                }
        log.info(f"Loaded {len(profiles)} player profiles from config")
    except Exception as e:
        log.warning(f"Could not load player profiles: {e}")
    return profiles


# ─── Player identity resolution ──────────────────────────────────────────────

def build_player_context(timeline):
    steam_ids = set()
    for round_ in timeline.get("rounds", []):
        for ev in (round_.get("events") or []):
            if ev.get("steamId"): steam_ids.add(ev["steamId"])
    return {sid: f"Player_{i+1}" for i, sid in enumerate(sorted(steam_ids))}

def load_player_info(match_id, profiles):
    player_info = {}
    prefix = f"matches/{match_id}"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/parse_result.json")
        parse_result = json.loads(obj["Body"].read())
        for p in parse_result.get("players", []):
            sid = p.get("steamId", "")
            if sid:
                player_info[sid] = {"steamId": sid, "name": p.get("name", ""), "realName": "", "nicknames": [], "team": p.get("team", 0)}
        log.info(f"  Loaded {len(player_info)} players from parse_result.json")
    except Exception as e:
        log.warning(f"  Could not load parse_result.json: {e}")
    try:
        meta = get_meta(match_id)
        for p in meta.get("players", []):
            sid = p.get("steamId", "")
            if sid:
                if sid not in player_info:
                    player_info[sid] = {"steamId": sid, "name": "", "realName": "", "nicknames": [], "team": 0}
                player_info[sid]["discordId"] = p.get("discordId", "")
    except Exception as e:
        log.warning(f"  Could not load meta.json: {e}")
    for sid, info in player_info.items():
        profile = profiles.get(sid)
        if profile:
            info["realName"] = profile.get("realName", "")
            info["nicknames"] = profile.get("nicknames", [])
            if not info["name"] and profile.get("gamertag"): info["name"] = profile["gamertag"]
            if not info.get("discordId") and profile.get("discordId"): info["discordId"] = profile["discordId"]
    return player_info

def build_player_map(player_names, player_info):
    alias_to_steam = {alias: sid for sid, alias in player_names.items()}
    players = {}
    for sid, alias in player_names.items():
        info = player_info.get(sid, {})
        players[sid] = {
            "alias": alias,
            "name": info.get("name", ""),
            "realName": info.get("realName", ""),
            "nicknames": info.get("nicknames", []),
            "discordId": info.get("discordId", ""),
            "team": info.get("team", 0),
        }
    return {"alias_to_steam_id": alias_to_steam, "steam_id_to_alias": dict(player_names), "players": players}

def build_name_reference(player_names, player_info):
    lines = []
    for sid, alias in sorted(player_names.items(), key=lambda x: x[1]):
        info = player_info.get(sid, {})
        name = info.get("name", "unknown")
        real_name = info.get("realName", "")
        nicknames = info.get("nicknames", [])
        parts = [f'{alias} = "{name}"']
        if real_name: parts.append(f"real name: {real_name}")
        if nicknames: parts.append(f"may be called: {', '.join(nicknames)}")
        if real_name or nicknames:
            lines.append(f"{parts[0]} ({' — '.join(parts[1:])})")
        else:
            lines.append(parts[0])
    return "\n".join(lines)


# ─── Prompt builders ──────────────────────────────────────────────────────────

def render_round_for_prompt(round_, player_names):
    round_num = round_.get("roundNum", "?")
    win_reason = round_.get("winReason", "")
    winner = round_.get("winner", "")
    lines = [f"--- Round {round_num} (winner: team {winner}, reason: {win_reason}) ---"]
    for ev in (round_.get("events") or []):
        t = ev.get("t", 0)
        pid = player_names.get(ev.get("steamId", ""), ev.get("steamId", "?"))
        kind = ev.get("type", "")
        if kind == "utterance":
            lines.append(f"  [{t:6.1f}s] VOICE {pid}: \"{ev.get('text', '')}\"")
        elif kind == "kill":
            victim = player_names.get(ev.get("victim", ""), ev.get("victim", "?"))
            hs = " (HS)" if ev.get("headshot") else ""
            trade = " [TRADE]" if ev.get("tradeKill") else ""
            first = " [FIRST KILL]" if ev.get("firstKill") else ""
            lines.append(f"  [{t:6.1f}s] KILL {pid} killed {victim} ({ev.get('weapon', '')}){hs}{trade}{first}")
        elif kind == "planted":
            lines.append(f"  [      ] BOMB {pid} planted on {ev.get('site', '?')}")
        elif kind == "defused":
            lines.append(f"  [      ] DEFUSE {pid} defused on {ev.get('site', '?')}")
        elif kind == "exploded":
            lines.append(f"  [      ] EXPLODE Bomb exploded on {ev.get('site', '?')}")
    return "\n".join(lines)


def build_round_system_prompt(name_reference):
    return f"""You are an expert CS2 analyst specializing in team communication analysis.
You will receive a CS2 round timeline combining voice communications (VOICE) with in-game events (KILL, BOMB, DEFUSE, EXPLODE).
Players are identified by aliases like Player_1, Player_2, etc.
Voice comms may be in Danish mixed with English CS2 terms — analyse based on meaning, not language.

PLAYER IDENTITY REFERENCE:
{name_reference}

IMPORTANT: When you hear a real name or nickname in the transcript (e.g. "Lasse", "Disco", "Zukke"), use the reference above to attribute it to the correct Player_N. Do NOT guess — if someone says a name, match it to the player whose real name or nickname matches.

Your job is to analyse how communication CONNECTED to in-game outcomes. Every insight must link what was said to what happened.

Return ONLY valid JSON with no preamble or markdown.

{{
  "round_outcome": "<won|lost>",
  "comms_impact_on_outcome": "<positive|negative|neutral>",
  "igl_candidate": "<Player_N alias or null>",
  "toxic_utterances": [{{"player": "<Player_N>", "text": "...", "reason": "..."}}],
  "miscommunications": [{{"player": "<Player_N>", "said": "...", "but_did": "...", "context": "...", "cost": "<what it cost the team>"}}],
  "motivating_moments": [{{"player": "<Player_N>", "text": "...", "context": "<what triggered it>", "effect": "<how team responded>"}}],
  "key_callouts": [
    {{
      "player": "<Player_N>",
      "text": "...",
      "t": <seconds>,
      "type": "<info|strategy|utility_request|rotation|trade_call|clutch_call|economy>",
      "outcome": "<what happened as a direct result within 5-10 seconds>",
      "impact": "<high|medium|low>",
      "round_impact": "<won_round|lost_round|got_kill|got_trade|successful_retake|successful_execute|plant|defuse|no_direct_impact>"
    }}
  ],
  "info_quality_scores": {{
    "<Player_N>": {{"score": <0.0-10.0>, "reason": "<1 sentence why>"}}
  }},
  "silent_players": ["<Player_N aliases who said nothing useful>"],
  "summary": "<2-3 sentences connecting comms to outcome. Use gamertags not Player_N for readability.>"
}}

key_callouts: Only include callouts with a VISIBLE connection to an in-game event. If someone calls a position and a kill happens there within 5-10s, that's a key callout. Max 3-4 per round. Empty array is fine.

miscommunications: Always include what it COST the team.

motivating_moments: Include what TRIGGERED it and the EFFECT on team. Empty array is fine.

Toxicity: Be VERY conservative. Danish gaming culture is blunt and sarcastic — that's normal, not toxic. Only flag direct personal insults, sustained harassment, or discriminatory language.

summary: Use gamertags for readability. Explain HOW comms connected to the result, don't just describe events."""


MATCH_SUMMARY_SYSTEM = """You are an expert CS2 communication analyst.
Given per-round analysis data and a player alias->steamId map, produce a match-level assessment keyed by steamId.
Connect communication patterns to match outcomes. Every score must be backed by evidence.

Return ONLY valid JSON:

{{
  "players": [
    {{
      "steam_id": "<actual steamId>",
      "comms_score": <0-100>,
      "igl_likelihood": <0-100>,
      "toxicity_score": <0-100>,
      "motivation_score": <0-100>,
      "info_density": <0-100>,
      "callout_accuracy": <0-100>,
      "dominant_role": "<entry|support|lurk|igl|awper|unknown>",
      "notable_moments": ["<Round N: specific event with outcome>"],
      "improvement_tips": ["<Specific advice referencing actual rounds from this match>"],
      "key_callout": {{"text": "<best callout>", "round": <n>, "outcome": "<result>"}}
    }}
  ],
  "team_chemistry_score": <0-100>,
  "communication_flow": "<How comms evolved through the match, referencing specific moments>",
  "key_miscommunications": [{{"round": <n>, "steam_id": "...", "description": "...", "cost": "..."}}],
  "turning_points": [{{"round": <n>, "description": "<Round where comms changed match direction>"}}],
  "match_summary": "<3-5 sentences. Use gamertags. Reference specific rounds. Explain how comms contributed to the result.>"
}}

notable_moments: Every moment MUST reference a specific round and outcome. Max 2-3 per player.
improvement_tips: MUST reference specific rounds from THIS match. Max 2 per player.
turning_points: 1-3 rounds where comms shifted momentum. Can be positive or negative."""


# ─── Analysis pipeline ────────────────────────────────────────────────────────

def gemini_json(prompt):
    response = client.models.generate_content(
        model=GEMINI_MODEL, contents=prompt,
        config=types.GenerateContentConfig(temperature=0.0, response_mime_type="application/json"),
    )
    text = response.text.strip().replace("```json", "").replace("```", "").strip()
    return json.loads(text)

def analyse_round(round_, player_names, round_system_prompt):
    events = round_.get("events") or []
    has_comms = any(ev.get("type") == "utterance" for ev in events)
    if not has_comms:
        return {
            "round_outcome": None, "comms_impact_on_outcome": "neutral", "igl_candidate": None,
            "toxic_utterances": [], "miscommunications": [], "motivating_moments": [],
            "key_callouts": [], "info_quality_scores": {}, "silent_players": [],
            "summary": "No voice communications this round.",
        }
    round_text = render_round_for_prompt(round_, player_names)
    prompt = round_system_prompt + "\n\n" + round_text
    return gemini_json(prompt)

def aggregate_and_summarise(round_analyses, timeline, player_names, player_info):
    alias_to_steam = {alias: sid for sid, alias in player_names.items()}
    gamertag_map = {}
    for alias, sid in alias_to_steam.items():
        info = player_info.get(sid, {})
        gamertag_map[alias] = info.get("name", alias)
    summary_input = {
        "match_id": timeline["matchId"],
        "map": timeline.get("mapName", "unknown"),
        "score": f"{timeline.get('team1Name','T1')} {timeline.get('scoreTeam1',0)} - {timeline.get('scoreTeam2',0)} {timeline.get('team2Name','T2')}",
        "round_count": len(round_analyses),
        "player_alias_map": alias_to_steam,
        "player_gamertags": gamertag_map,
        "round_analyses": round_analyses,
    }
    prompt = MATCH_SUMMARY_SYSTEM + "\n\n" + json.dumps(summary_input, indent=2)
    return gemini_json(prompt)

def process_session(match_id, profiles):
    log.info(f"Analysing match: {match_id}")
    prefix = f"matches/{match_id}"
    set_status(match_id, "analyzing")

    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/timeline_merged.json")
    timeline = json.loads(obj["Body"].read())

    player_names = build_player_context(timeline)
    log.info(f"  Players: {player_names}")

    player_info = load_player_info(match_id, profiles)
    player_map = build_player_map(player_names, player_info)
    log.info(f"  Player map: {json.dumps(player_map['players'], indent=2)}")

    name_reference = build_name_reference(player_names, player_info)
    log.info(f"  Name reference:\n{name_reference}")
    round_system_prompt = build_round_system_prompt(name_reference)

    rounds = timeline.get("rounds", [])
    log.info(f"  Rounds: {len(rounds)}, map: {timeline.get('mapName', 'unknown')}")

    round_analyses = []
    for i, round_ in enumerate(rounds):
        log.info(f"  Analysing round {round_.get('roundNum')} ({i+1}/{len(rounds)})...")
        try:
            analysis = analyse_round(round_, player_names, round_system_prompt)
            analysis["round_num"] = round_.get("roundNum")
            round_analyses.append(analysis)
        except Exception as e:
            log.error(f"  Round {round_.get('roundNum')} analysis failed: {e}")
        time.sleep(0.5)

    round_analyses_output = {"match_id": match_id, "player_map": player_map, "rounds": round_analyses}
    s3.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/round_analyses.json", Body=json.dumps(round_analyses_output, indent=2), ContentType="application/json")
    log.info(f"  Uploaded round_analyses.json ({len(round_analyses)} rounds)")

    log.info("  Generating match summary...")
    match_summary = aggregate_and_summarise(round_analyses, timeline, player_names, player_info)
    match_summary["match_id"] = match_id
    match_summary["player_map"] = player_map
    match_summary["analysed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    s3.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/analysis.json", Body=json.dumps(match_summary, indent=2), ContentType="application/json")
    set_status(match_id, "complete", {"analysisKey": f"{prefix}/analysis.json"})
    log.info(f"Analysis complete for {match_id}")

def list_pending():
    pending = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            match_id = cp["Prefix"][len("matches/"):-1]
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") == "pending_analysis": pending.append(match_id)
            except Exception: pass
    return pending

def main():
    log.info(f"AI analysis worker started (model: {GEMINI_MODEL})")
    profiles = load_player_profiles()
    while True:
        try:
            check_stale_sessions()
            pending = list_pending()
            if pending: log.info(f"Found {len(pending)} pending session(s): {pending}")
            for match_id in pending:
                try:
                    process_session(match_id, profiles)
                except Exception as e:
                    log.error(f"Error analysing {match_id}: {e}")
                    try: set_status(match_id, "error_analysis", {"error": str(e)})
                    except Exception: pass
        except Exception as e:
            log.error(f"Worker error: {e}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
