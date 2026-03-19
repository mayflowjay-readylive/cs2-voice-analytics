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


# ─── Player identity resolution ──────────────────────────────────────────────

def build_player_context(timeline: dict) -> dict:
    """Build a steamId → Player_N alias map from all events in the timeline."""
    steam_ids = set()
    for round_ in timeline.get("rounds", []):
        for ev in (round_.get("events") or []):
            if ev.get("steamId"):
                steam_ids.add(ev["steamId"])
    return {sid: f"Player_{i+1}" for i, sid in enumerate(sorted(steam_ids))}


def load_player_info(match_id: str) -> dict:
    """Load player identity info from parse_result.json and meta.json.

    Returns a dict keyed by steamId with available info:
    {
        "76561197978593980": {
            "steamId": "76561197978593980",
            "name": "PlayerName",       # from parse_result (in-game name)
            "discordId": "362662...",    # from meta.json
            "team": 2                    # from parse_result
        }
    }
    """
    player_info = {}
    prefix = f"matches/{match_id}"

    # Load in-game names + teams from parse_result.json
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/parse_result.json")
        parse_result = json.loads(obj["Body"].read())
        for p in parse_result.get("players", []):
            sid = p.get("steamId", "")
            if sid:
                player_info[sid] = {
                    "steamId": sid,
                    "name": p.get("name", ""),
                    "team": p.get("team", 0),
                }
        log.info(f"  Loaded {len(player_info)} player names from parse_result.json")
    except Exception as e:
        log.warning(f"  Could not load parse_result.json for player names: {e}")

    # Enrich with discordId from meta.json
    try:
        meta = get_meta(match_id)
        for p in meta.get("players", []):
            sid = p.get("steamId", "")
            if sid:
                if sid not in player_info:
                    player_info[sid] = {"steamId": sid, "name": "", "team": 0}
                player_info[sid]["discordId"] = p.get("discordId", "")
    except Exception as e:
        log.warning(f"  Could not load meta.json for discord IDs: {e}")

    return player_info


def build_player_map(player_names: dict, player_info: dict) -> dict:
    """Build a complete player map for output files.

    Returns:
    {
        "alias_to_steam_id": {"Player_1": "76561197978593980", ...},
        "steam_id_to_alias": {"76561197978593980": "Player_1", ...},
        "players": {
            "76561197978593980": {
                "alias": "Player_1",
                "name": "PlayerName",
                "discordId": "362662...",
                "team": 2
            }
        }
    }
    """
    alias_to_steam = {alias: sid for sid, alias in player_names.items()}
    players = {}
    for sid, alias in player_names.items():
        info = player_info.get(sid, {})
        players[sid] = {
            "alias": alias,
            "name": info.get("name", ""),
            "discordId": info.get("discordId", ""),
            "team": info.get("team", 0),
        }
    return {
        "alias_to_steam_id": alias_to_steam,
        "steam_id_to_alias": dict(player_names),
        "players": players,
    }


# ─── Prompt builders ──────────────────────────────────────────────────────────

def render_round_for_prompt(round_: dict, player_names: dict) -> str:
    round_num = round_.get("roundNum", "?")
    win_reason = round_.get("winReason", "")
    winner = round_.get("winner", "")

    lines = [f"--- Round {round_num} (winner: team {winner}, reason: {win_reason}) ---"]

    for ev in (round_.get("events") or []):
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
You will receive a CS2 round timeline combining voice communications (🎙️) with in-game events (kills 💀, bomb plants 💣, defuses 🔧, explosions 💥).
Players are identified by aliases like Player_1, Player_2, etc.
Voice comms may be in Danish mixed with English CS2 terms — analyse based on meaning, not language.

Your job is to analyse how communication CONNECTED to in-game outcomes. Every insight must link what was said to what happened.

Analyze the communication quality and return ONLY valid JSON with no preamble or markdown.

Your JSON must match this schema exactly:
{
  "round_outcome": "<won|lost>",
  "comms_impact_on_outcome": "<positive|negative|neutral>",
  "igl_candidate": "<Player_N alias or null>",
  "toxic_utterances": [{"player": "<Player_N>", "text": "...", "reason": "..."}],
  "miscommunications": [{"player": "<Player_N>", "said": "...", "but_did": "...", "context": "...", "cost": "<what it cost the team — e.g. 'lost the round', 'gave up site control', 'player died', 'no impact'>"}],
  "motivating_moments": [{"player": "<Player_N>", "text": "...", "context": "<what triggered it — e.g. 'after teammate got a 3k', 'after losing pistol round', 'during eco round'>", "effect": "<how team responded — e.g. 'team rallied and won the force buy', 'no visible impact', 'improved coordination next round'>"}],
  "key_callouts": [
    {
      "player": "<Player_N>",
      "text": "...",
      "t": <seconds>,
      "type": "<info|strategy|utility_request|rotation|trade_call|clutch_call|economy>",
      "outcome": "<what happened as a direct result within 5-10 seconds — e.g. 'teammate got the kill 3s later', 'team rotated and retook site', 'call was ignored, site fell', 'led to successful B execute'>",
      "impact": "<high|medium|low>",
      "round_impact": "<won_round|lost_round|got_kill|got_trade|successful_retake|successful_execute|plant|defuse|no_direct_impact>"
    }
  ],
  "info_quality_scores": {
    "<Player_N>": {"score": <0.0-10.0>, "reason": "<1 sentence explaining why — e.g. 'Called 3 accurate positions that led to 2 kills' or 'Only spoke once with vague info'>"}
  },
  "silent_players": ["<Player_N aliases who had voice data but said nothing useful this round>"],
  "summary": "<2-3 sentence summary that specifically connects communication to the round outcome. Don't just describe what happened — explain HOW comms helped win or contributed to losing.>"
}

CRITICAL — key_callouts guidelines:
- Only include callouts that had a VISIBLE connection to an in-game event.
- Look at the timeline: if someone calls a position and a kill happens at that location within 5-10 seconds, that's a key callout.
- If someone calls a rotation and the team successfully retakes, that's a key callout.
- If someone requests utility and it leads to a successful execute, that's a key callout.
- Do NOT include generic filler or callouts with no outcome. "Nice" or "okay" alone is NOT a key callout.
- Maximum 3-4 key callouts per round — only the ones that actually mattered.
- An empty key_callouts array is fine if no callout had a measurable impact.

CRITICAL — miscommunications guidelines:
- Always include what the miscommunication COST the team. Did they lose the round? Did a player die? Did they lose map control?
- Compare what was said to what the demo events show actually happened.
- If a player called "all B" but kills happened at A, that's a miscommunication.

CRITICAL — motivating_moments guidelines:
- Include what TRIGGERED the moment (a good play, a bad loss, an eco round).
- Include the EFFECT — did the team play better after? Did coordination improve?
- Genuine hype after clutches, multi-kills, or comebacks.
- Encouragement after lost rounds that helped morale.
- Empty array is fine if nobody was particularly motivating.

CRITICAL — Toxicity detection guidelines (be VERY conservative):
- Danish gaming culture is naturally blunt, sarcastic, and uses dark humor. This is NORMAL, not toxic.
- Sarcasm praising a teammate ("he would have aced them all with his Deagle") is NOT toxic — it's a compliment wrapped in humor.
- Expressing disappointment about losing a round ("that's so annoying", "ugh", "hvor er det ærgerligt") is NOT toxic — it's normal emotional expression.
- Friendly banter, teasing, and joking at each other's expense is NOT toxic — it's team bonding.
- Swearing in frustration at the situation (not directed at a teammate) is NOT toxic.
- ONLY flag as toxic: direct personal insults aimed at a specific teammate with intent to hurt, sustained harassment or bullying, telling someone to shut up or stop playing, rage-quitting threats, or discriminatory language.
- When in doubt, it is NOT toxic. False positives are worse than false negatives here.
- An empty toxic_utterances array is the expected outcome for most rounds.

For summary: Don't just say "good communication this round." Say HOW the comms connected to the result — "Player_1's B rotation call gave Player_3 time to rotate, leading to a 2v1 retake win" or "Despite good callouts, the team couldn't capitalize because Player_2 and Player_4 peeked the same angle after being told to split."
"""

MATCH_SUMMARY_SYSTEM = """You are an expert CS2 communication analyst.
Given per-round analysis data (using Player_N aliases) and a player alias->steamId map,
produce a final match-level player assessment keyed by steamId.

Your job is to connect communication patterns to match outcomes. Every score and insight must be backed by specific evidence from the rounds.

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
      "notable_moments": [
        "<Specific moment with round number and outcome — e.g. 'Round 8: Called 2 players tunnels, teammate got double kill 3s later (round won)' or 'Round 15: Mid-round IGL call to rotate A, team got the retake'>",
        "<Another specific moment>"
      ],
      "improvement_tips": [
        "<Specific advice referencing actual rounds — e.g. 'In rounds 5 and 11, called positions but too late (after teammate already died). Try to call earlier when you first spot movement.' or 'Went silent in rounds 8-12 during CT side — more info calls when holding site would help teammates rotate faster.'>",
        "<Another specific tip>"
      ],
      "key_callout": {
        "text": "<their single best callout from the entire match>",
        "round": <round number>,
        "outcome": "<what happened as a result>"
      }
    }
  ],
  "team_chemistry_score": <0-100>,
  "communication_flow": "<Describe how comms evolved through the match — e.g. 'Strong early coordination on T-side with clean executes, but communication broke down after going 5-8 deficit. Recovered in second half when Player_2 started making mid-round calls.'>",
  "key_miscommunications": [
    {
      "round": <n>,
      "steam_id": "...",
      "description": "<what was said vs what happened>",
      "cost": "<what it cost — e.g. 'Lost the round, led to 3-round losing streak' or 'Gave up A site control but recovered'>"
    }
  ],
  "turning_points": [
    {
      "round": <n>,
      "description": "<A round where communication clearly changed the match direction — e.g. 'Round 12: Player_1 called a full B stack read, team rotated early and got a clean 5v3 retake. Won 4 of the next 5 rounds after this.'>"
    }
  ],
  "match_summary": "<3-5 sentence overall communication summary that explains how comms contributed to the final result. Reference specific rounds and patterns, not generic observations.>"
}

CRITICAL — notable_moments guidelines:
- Every moment MUST reference a specific round number and a specific outcome.
- "Good A-site calls" is too vague. "Round 8: Called AWP peek A long → teammate pre-aimed and got the opening pick (round won)" is good.
- Maximum 2-3 moments per player — only the ones that genuinely mattered.

CRITICAL — improvement_tips guidelines:
- Every tip MUST reference specific rounds or patterns from THIS match.
- "Communicate more" is useless. "Went silent in rounds 8-12 when holding B site — calling enemy utility usage would have helped Player_3 time rotations better" is useful.
- Maximum 2 tips per player. Be specific and actionable.

CRITICAL — key_callout guidelines:
- Pick each player's single most impactful callout from the entire match.
- Must include what happened as a result.
- If a player never made an impactful callout, use their best attempt with honest outcome.

CRITICAL — turning_points guidelines:
- Identify 1-3 rounds where communication clearly shifted the match momentum.
- These should be moments where a specific callout or communication pattern led to a streak change.
- Can be positive (comms won a crucial round) or negative (miscommunication triggered a losing streak).
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
    events = round_.get("events") or []

    # Skip rounds with no voice comms
    has_comms = any(ev.get("type") == "utterance" for ev in events)
    if not has_comms:
        return {
            "round_outcome": None,
            "comms_impact_on_outcome": "neutral",
            "igl_candidate": None,
            "toxic_utterances": [],
            "miscommunications": [],
            "motivating_moments": [],
            "key_callouts": [],
            "info_quality_scores": {},
            "silent_players": [],
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

    # Set intermediate status
    set_status(match_id, "analyzing")

    # Load merged timeline
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/timeline_merged.json")
    timeline = json.loads(obj["Body"].read())

    player_names = build_player_context(timeline)
    log.info(f"  Players: {player_names}")

    # Load player identity info (names, discordIds, teams)
    player_info = load_player_info(match_id)
    player_map = build_player_map(player_names, player_info)
    log.info(f"  Player map: {json.dumps(player_map['players'], indent=2)}")

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

    # Upload round analyses — include player_map so the frontend can resolve aliases
    round_analyses_output = {
        "match_id": match_id,
        "player_map": player_map,
        "rounds": round_analyses,
    }
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/round_analyses.json",
        Body=json.dumps(round_analyses_output, indent=2),
        ContentType="application/json",
    )
    log.info(f"  Uploaded round_analyses.json ({len(round_analyses)} rounds)")

    # Final match-level summary
    log.info("  Generating match-level player scores…")
    match_summary = aggregate_and_summarise(round_analyses, timeline, player_names)
    match_summary["match_id"] = match_id
    match_summary["player_map"] = player_map
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
