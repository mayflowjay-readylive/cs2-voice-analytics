"""
Transcription Worker (Gemini)
==============================
Polls S3 for sessions with status=pending_transcription.
Decodes per-player Opus packets to WAV, uploads to Gemini Files API,
transcribes in Danish, writes timestamped transcripts back to S3,
then deletes audio files from R2 ONLY after transcript.json is confirmed written.

Resilience improvements:
 - Audio retained until transcript.json is confirmed written to R2
 - Partial transcript saved after every player (crash-safe resume)
 - Per-player transcription progress tracked in meta.json
 - Full Gemini diagnostics logged on empty/failed response
 - Retry on empty Gemini response (up to MAX_GEMINI_RETRIES attempts)
 - Discord webhook alert posted when a session fails
 - errors.json written to matches/{id}/ on every failure (permanent log)
"""

import os
import json
import time
import logging
import struct
import wave
import tempfile
import urllib.request
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import boto3
import opuslib
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ─────────────────────────────────────────────────────────────────

S3_BUCKET             = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT           = os.environ.get("S3_ENDPOINT")
POLL_INTERVAL         = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
GEMINI_MODEL          = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash-lite")
DISCORD_WEBHOOK_URL   = os.environ.get("DISCORD_WEBHOOK_URL", "")  # optional

STALE_THRESHOLD_SECONDS = 900   # 15 minutes
MAX_RETRIES             = 3     # stale-session retries
MAX_GEMINI_RETRIES      = 3     # retries on empty Gemini response

# WAV format constants for duration calculation
WAV_SAMPLE_RATE       = 48000
WAV_CHANNELS          = 2
WAV_SAMPLE_WIDTH      = 2   # 16-bit
WAV_HEADER_SIZE       = 44
WAV_BYTES_PER_SECOND  = WAV_SAMPLE_RATE * WAV_CHANNELS * WAV_SAMPLE_WIDTH

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "auto"),
)

# ─── Data types ─────────────────────────────────────────────────────────────

@dataclass
class Utterance:
    steam_id: str
    t_start: float
    t_end: float
    text: str
    confidence: float


# ─── Discord alerting ────────────────────────────────────────────────────────

def send_discord_alert(title: str, description: str, color: int = 0xFF4444) -> None:
    """Post an embed to the Discord webhook. Silently no-ops if no webhook is configured."""
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        payload = json.dumps({
            "embeds": [{
                "title": title,
                "description": description,
                "color": color,
                "footer": {"text": "CS2 Voice Analytics — Transcription Worker"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        }).encode()
        req = urllib.request.Request(
            DISCORD_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status not in (200, 204):
                log.warning(f"Discord webhook returned {resp.status}")
    except Exception as e:
        log.warning(f"Discord alert failed (non-fatal): {e}")


# ─── Persistent error log ────────────────────────────────────────────────────

def append_error_log(match_id: str, stage: str, error: str, steam_id: str = None) -> None:
    """Append an error entry to matches/{match_id}/errors.json in R2.

    This file persists indefinitely and is the first place to look when
    diagnosing a failed session.
    """
    key = f"matches/{match_id}/errors.json"
    existing = []
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        existing = json.loads(obj["Body"].read())
    except Exception:
        pass  # File doesn't exist yet — that's fine

    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": stage,
        "error": error,
    }
    if steam_id:
        entry["steamId"] = steam_id

    existing.append(entry)

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(existing, indent=2),
            ContentType="application/json",
        )
    except Exception as e:
        log.warning(f"Could not write errors.json for {match_id}: {e}")


# ─── Audio decoding ─────────────────────────────────────────────────────────

def opus_to_wav(opus_bytes: bytes) -> bytes:
    """Decode length-prefixed raw Opus packets to WAV (48kHz stereo 16-bit)."""
    SAMPLE_RATE = 48000
    CHANNELS    = 2
    FRAME_SIZE  = 960  # 20ms at 48kHz

    decoder    = opuslib.Decoder(SAMPLE_RATE, CHANNELS)
    pcm_chunks = []

    offset = 0
    while offset + 4 <= len(opus_bytes):
        pkt_len = struct.unpack_from("<I", opus_bytes, offset)[0]
        offset += 4
        if offset + pkt_len > len(opus_bytes):
            break
        packet = opus_bytes[offset:offset + pkt_len]
        offset += pkt_len
        try:
            pcm = decoder.decode(bytes(packet), FRAME_SIZE)
            pcm_chunks.append(pcm)
        except Exception:
            pass

    if not pcm_chunks:
        return b""

    pcm_data = b"".join(pcm_chunks)

    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    with wave.open(tmp.name, "wb") as wf:
        wf.setnchannels(CHANNELS)
        wf.setsampwidth(2)
        wf.setframerate(SAMPLE_RATE)
        wf.writeframes(pcm_data)

    with open(tmp.name, "rb") as f:
        wav_bytes = f.read()
    os.unlink(tmp.name)
    return wav_bytes


def get_wav_duration(wav_bytes: bytes) -> float:
    """Calculate WAV audio duration in seconds from raw WAV bytes."""
    if len(wav_bytes) <= WAV_HEADER_SIZE:
        return 0.0
    return (len(wav_bytes) - WAV_HEADER_SIZE) / WAV_BYTES_PER_SECOND


# ─── Gemini transcription ──────────────────────────────────────────────────

CS2_CALLOUTS = [
    # Sites & areas
    "A site", "B site", "mid", "CT", "T side", "CT spawn", "T spawn",
    # General map areas
    "catwalk", "banana", "connector", "jungle", "palace", "apartments",
    "window", "pit", "arch", "library", "van", "truck", "heaven", "hell",
    "long", "short", "ramp", "squeaky", "vent", "tunnels", "underpass",
    "boost", "platform", "short corner", "long corner", "cat", "goose",
    "closet", "quad", "dark", "coffin", "new box", "old box",
    # Weapons
    "AWP", "AK", "AK-47", "M4", "M4A1", "M4A4", "MP9", "MAC-10", "UMP",
    "Deagle", "Desert Eagle", "P250", "Tec-9", "CZ", "FAMAS", "Galil",
    "SG", "SG553", "AUG", "SCAR", "G3SG1", "SSG", "Scout",
    # Utility
    "Molotov", "flash", "flashbang", "smoke", "HE", "grenade", "decoy",
    "incendiary", "pop flash", "one-way smoke",
    # Actions & tactics
    "defuse", "plant", "retake", "rotate", "rush", "eco", "force buy",
    "full buy", "half buy", "save", "drop", "peek", "hold", "anchor",
    "entry", "lurk", "IGL", "execute", "fake", "split", "stack",
    "one-tap", "spray", "burst", "wallbang", "boost", "jump peek",
    "shoulder peek", "wide peek", "jiggle peek",
    # Callout phrases
    "he's low", "I'm low", "need flash", "smoking", "flashing",
    "going B", "going A", "pushing mid", "they're rushing",
]


def build_transcription_prompt(map_name: str = None) -> str:
    map_line     = f"Map: {map_name}" if map_name else "Map: unknown (could be any CS2 map)"
    callout_list = ", ".join(f'"{c}"' for c in CS2_CALLOUTS)
    return f"""This is a voice recording from a CS2 (Counter-Strike 2) match.
The player is speaking Danish, frequently mixing in English CS2 terms.

{map_line}

Common CS2 terms and callouts you may hear (keep these exactly as written):
{callout_list}

Instructions:
- Transcribe exactly what is said in Danish
- Keep all English CS2 terms, weapon names, and callouts exactly as listed above
- If a word sounds like a CS2 term, use the correct spelling from the list
- Preserve natural Danish grammar and sentence structure
- Split into separate utterances at natural pauses (>1 second of silence)

Return ONLY a JSON array with this exact schema, no preamble or markdown:
[
  {{
    "t_start": <seconds as float>,
    "t_end": <seconds as float>,
    "text": "<transcribed text>",
    "confidence": <0.0-1.0>
  }}
]

If there is silence or no speech, return an empty array: []
"""

# NOTE: {transcript_json} is injected by cleanup_transcript() below
CLEANUP_PROMPT = """You are a transcript editor for CS2 voice recordings in Danish.
The following transcript was auto-generated from a Danish CS2 voice recording.
It may contain transcription errors that need fixing.

Known correct CS2 terms (fix any misspellings or misheard versions):
{callout_list}

Rules — fix ALL of the following issues:

1. CS2 TERM CORRECTIONS:
   - Fix obvious CS2 term errors (e.g. "a dæbliup" → "AWP", "molly" → "Molotov")
   - Fix Danish words that were clearly misheard CS2 terms

2. DUPLICATED WORDS AND STUTTERS:
   - Remove accidental word repetitions (e.g. "til til" → "til", "jeg jeg" → "jeg")
   - Remove transcription stutters where the same word appears twice in a row
   - Keep intentional repetitions for emphasis (e.g. "go go go" should stay)

3. GRAMMAR AND SENTENCE CLEANUP:
   - Fix broken Danish grammar caused by transcription errors
   - Fix words that were clearly misheard (e.g. wrong preposition, wrong verb form)
   - Merge sentence fragments that clearly belong together
   - Remove filler artifacts that aren't real words

4. PRESERVE:
   - Do NOT change the meaning or intent of what was said
   - Do NOT add content that wasn't spoken
   - Do NOT remove intentional filler words (e.g. "øh", "altså") — these are natural speech
   - Do NOT change correct Danish words or grammar
   - Keep all timestamps (t_start, t_end) exactly as they are

Transcript to correct:
{transcript_json}

Return ONLY the corrected JSON array in the exact same format as input, no preamble or markdown."""


def scale_timestamps(raw_utterances: list[dict], wav_duration: float) -> list[dict]:
    """Scale Gemini's compressed timestamps to match actual audio duration."""
    if not raw_utterances or wav_duration <= 0:
        return raw_utterances

    max_t = 0.0
    for u in raw_utterances:
        max_t = max(max_t, u.get("t_end", 0.0), u.get("t_start", 0.0))

    if max_t <= 0:
        return raw_utterances

    scale = wav_duration / max_t
    if scale <= 2.0:
        log.info(f"  Timestamps OK (scale={scale:.1f}x, audio={wav_duration:.0f}s, gemini_max={max_t:.1f}s)")
        return raw_utterances

    log.info(f"  ⚡ Scaling timestamps by {scale:.1f}x (audio={wav_duration:.0f}s, gemini_max={max_t:.1f}s)")
    for u in raw_utterances:
        u["t_start"] = u.get("t_start", 0.0) * scale
        u["t_end"]   = u.get("t_end", 0.0) * scale
    return raw_utterances


def _log_gemini_diagnostics(response, attempt: int, steam_id: str) -> None:
    """Log all available Gemini response diagnostics when response is empty or failed."""
    log.warning(f"  ⚠️  Gemini returned empty response for {steam_id} (attempt {attempt}/{MAX_GEMINI_RETRIES})")
    try:
        candidates = getattr(response, "candidates", None) or []
        if candidates:
            c              = candidates[0]
            finish_reason  = getattr(c, "finish_reason", "UNKNOWN")
            safety_ratings = getattr(c, "safety_ratings", [])
            log.warning(f"      finish_reason  : {finish_reason}")
            log.warning(f"      safety_ratings : {safety_ratings}")
            content = getattr(c, "content", None)
            if content:
                log.warning(f"      content.parts  : {getattr(content, 'parts', None)}")
        else:
            log.warning(f"      candidates     : (none)")

        prompt_feedback = getattr(response, "prompt_feedback", None)
        if prompt_feedback:
            log.warning(f"      prompt_feedback: {prompt_feedback}")

        usage = getattr(response, "usage_metadata", None)
        if usage:
            log.info(f"      usage_metadata : {usage}")
    except Exception as diag_err:
        log.warning(f"      (could not extract full diagnostics: {diag_err})")


def transcribe_with_gemini(wav_bytes: bytes, steam_id: str, map_name: str = None) -> list[Utterance]:
    """Upload WAV to Gemini Files API and transcribe.
    Retries up to MAX_GEMINI_RETRIES times on empty response."""
    wav_duration = get_wav_duration(wav_bytes)
    log.info(f"  Audio duration: {wav_duration:.1f}s ({wav_duration/60:.1f}min)")

    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.write(wav_bytes)
    tmp.close()

    try:
        log.info(f"  Uploading to Gemini Files API…")
        audio_file = client.files.upload(
            file=tmp.name,
            config=types.UploadFileConfig(mime_type="audio/wav"),
        )

        while audio_file.state.name == "PROCESSING":
            time.sleep(1)
            audio_file = client.files.get(name=audio_file.name)

        if audio_file.state.name == "FAILED":
            raise RuntimeError(f"Gemini file processing failed: {audio_file.state}")

        prompt     = build_transcription_prompt(map_name)
        last_error = None

        for attempt in range(1, MAX_GEMINI_RETRIES + 1):
            if attempt > 1:
                wait = 15 * (2 ** (attempt - 2))  # 15s, 30s
                log.warning(f"  Retrying transcription in {wait}s (attempt {attempt}/{MAX_GEMINI_RETRIES})…")
                time.sleep(wait)

            log.info(f"  Requesting transcription (gemini-2.5-pro, attempt {attempt}/{MAX_GEMINI_RETRIES})…")
            try:
                response = client.models.generate_content(
                    model="gemini-2.5-pro",
                    contents=[prompt, audio_file],
                    config=types.GenerateContentConfig(
                        temperature=0.0,
                        response_mime_type="application/json",
                    ),
                )
            except Exception as api_err:
                last_error = str(api_err)
                log.error(f"  Gemini API call failed (attempt {attempt}): {api_err}")
                continue

            # ── Empty / None response ──────────────────────────────────────────
            if not response.text or not response.text.strip():
                _log_gemini_diagnostics(response, attempt, steam_id)
                last_error = f"Empty response on attempt {attempt}"
                continue

            # ── Parse JSON ────────────────────────────────────────────────────
            try:
                text           = response.text.strip().replace("```json", "").replace("```", "").strip()
                raw_utterances = json.loads(text)
            except json.JSONDecodeError as parse_err:
                log.error(f"  JSON parse failed (attempt {attempt}): {parse_err}")
                log.error(f"  Raw response (first 500 chars): {response.text[:500]!r}")
                last_error = f"JSON parse error: {parse_err}"
                continue

            log.info(f"  Raw transcription: {len(raw_utterances)} utterances")

            # ── Scale + cleanup ───────────────────────────────────────────────
            raw_utterances = scale_timestamps(raw_utterances, wav_duration)
            raw_utterances = cleanup_transcript(raw_utterances)

            # ── Delete Gemini file ────────────────────────────────────────────
            try:
                client.files.delete(name=audio_file.name)
            except Exception:
                pass

            return [
                Utterance(
                    steam_id   = steam_id,
                    t_start    = u.get("t_start", 0.0),
                    t_end      = u.get("t_end", 0.0),
                    text       = u.get("text", ""),
                    confidence = u.get("confidence", 1.0),
                )
                for u in raw_utterances
                if u.get("text", "").strip()
            ]

        # All retries exhausted
        try:
            client.files.delete(name=audio_file.name)
        except Exception:
            pass

        raise RuntimeError(
            f"Transcription failed after {MAX_GEMINI_RETRIES} attempts for {steam_id}. Last error: {last_error}"
        )

    finally:
        os.unlink(tmp.name)


def cleanup_transcript(raw_utterances: list[dict]) -> list[dict]:
    """Post-processing pass to fix CS2 term errors and transcription artifacts using Gemini."""
    if not raw_utterances:
        return raw_utterances

    callout_list = ", ".join(f'"{c}"' for c in CS2_CALLOUTS)
    prompt = CLEANUP_PROMPT.format(
        callout_list    = callout_list,
        transcript_json = json.dumps(raw_utterances, ensure_ascii=False, indent=2),
    )

    log.info(f"  Running cleanup pass on {len(raw_utterances)} utterances…")
    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[prompt],
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )

        if not response.text or not response.text.strip():
            log.warning(f"  Cleanup pass returned empty — keeping original")
            return raw_utterances

        text    = response.text.strip().replace("```json", "").replace("```", "").strip()
        cleaned = json.loads(text)
        log.info(f"  Cleanup: {len(raw_utterances)} → {len(cleaned)} utterances")
        return cleaned

    except Exception as e:
        log.warning(f"  Cleanup pass failed ({e}) — keeping original transcript")
        return raw_utterances


# ─── S3 helpers ─────────────────────────────────────────────────────────────

def list_pending_sessions() -> list[str]:
    pending   = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match_prefix = prefix["Prefix"]
            try:
                obj      = s3.get_object(Bucket=S3_BUCKET, Key=f"{match_prefix}meta.json")
                meta     = json.loads(obj["Body"].read())
                match_id = meta.get("matchId", "")
                status   = meta.get("status", "")

                # Hard block: never re-queue sessions marked as permanently unprocessable
                if meta.get("blocked"):
                    log.debug(f"  blocked session skipped: {match_id}")
                    continue

                if status == "pending_transcription":
                    log.info(f"  queuing: {match_id}")
                    pending.append(match_id)
            except Exception:
                pass
    return pending


def check_stale_sessions():
    """Find sessions stuck in 'transcribing' with NO progress for >15 minutes.

    KEY FIX: We use `lastProgressAt` (updated after every player completes)
    rather than `statusUpdatedAt` (set once when transcribing starts).
    This prevents the stale checker from killing sessions that are actively
    transcribing long audio files — which previously caused concurrent runs
    that deleted audio mid-transcription.
    """
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match_prefix = prefix["Prefix"]
            try:
                obj  = s3.get_object(Bucket=S3_BUCKET, Key=f"{match_prefix}meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") != "transcribing":
                    continue

                # Use lastProgressAt if available — it's updated after every player
                # completes, so an active session will never appear stale here.
                # Fall back to statusUpdatedAt for sessions started before this fix.
                progress_at_str = meta.get("lastProgressAt") or meta.get("statusUpdatedAt")
                if progress_at_str:
                    progress_at = datetime.fromisoformat(progress_at_str.replace("Z", "+00:00"))
                else:
                    progress_at = obj["LastModified"]

                age = datetime.now(timezone.utc) - progress_at
                if age.total_seconds() < STALE_THRESHOLD_SECONDS:
                    continue

                match_id    = meta["matchId"]
                retry_count = meta.get("retryCount", 0)

                if retry_count >= MAX_RETRIES:
                    msg = f"No progress for {int(age.total_seconds())}s after {MAX_RETRIES} retries"
                    log.warning(f"⛔ Session {match_id}: {msg}")
                    set_status(match_id, "error_transcription", {"error": msg})
                    append_error_log(match_id, "stale_session", msg)
                    send_discord_alert(
                        title=f"⛔ Transcription permanently failed",
                        description=(
                            f"**Match:** `{match_id}`\n"
                            f"**Reason:** {msg}\n\n"
                            f"Use the retry button in the app or check `errors.json` in R2."
                        ),
                        color=0xFF0000,
                    )
                else:
                    log.warning(
                        f"🔄 Session {match_id} — no progress for {int(age.total_seconds())}s"
                        f" — resetting (retry {retry_count + 1}/{MAX_RETRIES})"
                    )
                    set_status(match_id, "pending_transcription", {"retryCount": retry_count + 1})

            except Exception:
                pass


def get_meta(match_id: str) -> dict:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
    return json.loads(obj["Body"].read())


def set_status(match_id: str, status: str, extra: dict = None):
    try:
        meta           = get_meta(match_id)
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
        log.info(f"  ✍️  Status set: {match_id} → {status}")
    except Exception as e:
        log.error(f"  ❌ set_status FAILED for {match_id} → {status}: {e}")
        raise


def r2_object_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except Exception:
        return False


def update_player_progress(match_id: str, steam_id: str, status: str,
                           utterance_count: int = 0, error: str = None) -> None:
    """Write per-player transcription progress into meta.json.

    Also updates lastProgressAt so the stale session checker knows this
    session is alive and should not be reset.
    """
    try:
        meta = get_meta(match_id)
        if "transcriptionProgress" not in meta:
            meta["transcriptionProgress"] = {}
        entry = {
            "status"    : status,
            "utterances": utterance_count,
            "updatedAt" : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if error:
            entry["error"] = error
        meta["transcriptionProgress"][steam_id] = entry
        # Keep lastProgressAt current so stale checker never fires on active sessions
        meta["lastProgressAt"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"matches/{match_id}/meta.json",
            Body=json.dumps(meta, indent=2),
            ContentType="application/json",
        )
    except Exception as e:
        log.warning(f"  Could not update player progress for {steam_id}: {e}")


# ─── Main processing ────────────────────────────────────────────────────────

def process_session(match_id: str):
    log.info(f"Processing match: {match_id}")
    set_status(match_id, "transcribing")
    meta     = get_meta(match_id)
    map_name = meta.get("map")

    # ── Resume from partial transcript if a previous run crashed ─────────────
    partial_key         = f"matches/{match_id}/transcript_partial.json"
    completed_steam_ids : set[str]  = set()
    all_utterances      : list[dict] = []

    if r2_object_exists(partial_key):
        try:
            obj     = s3.get_object(Bucket=S3_BUCKET, Key=partial_key)
            partial = json.loads(obj["Body"].read())
            all_utterances      = partial.get("utterances", [])
            completed_steam_ids = set(partial.get("completedSteamIds", []))
            log.info(
                f"  📂 Resuming from partial transcript: "
                f"{len(completed_steam_ids)}/{len(meta['players'])} players done, "
                f"{len(all_utterances)} utterances so far"
            )
        except Exception as e:
            log.warning(f"  Could not load partial transcript ({e}) — starting fresh")

    # ── Track audio keys safe to delete after transcript is written ───────────
    audio_keys_to_delete : list[str] = []
    player_errors        : list[str] = []

    # ── Per-player transcription ──────────────────────────────────────────────
    for player in meta["players"]:
        steam_id  = player["steamId"]
        audio_key = player["audioKey"]

        if steam_id in completed_steam_ids:
            log.info(f"  ✅ {steam_id} already transcribed — skipping")
            audio_keys_to_delete.append(audio_key)
            continue

        log.info(f"  Downloading audio for {steam_id} ({audio_key})…")
        update_player_progress(match_id, steam_id, "transcribing")

        try:
            audio_obj   = s3.get_object(Bucket=S3_BUCKET, Key=audio_key)
            audio_bytes = audio_obj["Body"].read()
            log.info(f"  Audio size: {len(audio_bytes) / 1024:.1f} KB")
        except Exception as e:
            msg = f"Failed to download audio: {e}"
            log.error(f"  {steam_id}: {msg}")
            update_player_progress(match_id, steam_id, "error_download", error=msg)
            append_error_log(match_id, "download", msg, steam_id=steam_id)
            player_errors.append(f"`{steam_id}`: download failed — {e}")
            continue

        if len(audio_bytes) < 1024:
            log.info(f"  Skipping {steam_id}: audio too short ({len(audio_bytes)} bytes)")
            update_player_progress(match_id, steam_id, "skipped_too_short")
            audio_keys_to_delete.append(audio_key)
            completed_steam_ids.add(steam_id)
            continue

        log.info(f"  Decoding Opus packets to WAV…")
        try:
            wav_bytes = opus_to_wav(audio_bytes)
        except Exception as e:
            msg = f"Opus decode failed: {e}"
            log.error(f"  {steam_id}: {msg}")
            update_player_progress(match_id, steam_id, "error_decode", error=msg)
            append_error_log(match_id, "decode", msg, steam_id=steam_id)
            player_errors.append(f"`{steam_id}`: decode failed — {e}")
            continue

        if not wav_bytes:
            log.info(f"  Skipping {steam_id}: no decodable audio")
            update_player_progress(match_id, steam_id, "skipped_no_audio")
            audio_keys_to_delete.append(audio_key)
            completed_steam_ids.add(steam_id)
            continue

        log.info(f"  WAV size: {len(wav_bytes) / 1024:.1f} KB")

        try:
            utterances = transcribe_with_gemini(wav_bytes, steam_id, map_name)
            log.info(f"  → {len(utterances)} utterances for {steam_id}")
        except Exception as e:
            msg = str(e)
            log.error(f"  Transcription failed for {steam_id}: {msg}")
            update_player_progress(match_id, steam_id, "error_transcription", error=msg)
            append_error_log(match_id, "transcription", msg, steam_id=steam_id)
            player_errors.append(f"`{steam_id}`: transcription failed — {msg}")
            # Do NOT delete audio on transcription failure — keep for debugging
            continue

        # ── Player succeeded ──────────────────────────────────────────────────
        all_utterances.extend([asdict(u) for u in utterances])
        completed_steam_ids.add(steam_id)
        audio_keys_to_delete.append(audio_key)

        update_player_progress(match_id, steam_id, "done", utterance_count=len(utterances))

        # ── Save partial transcript after every player (crash safety) ─────────
        try:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=partial_key,
                Body=json.dumps({
                    "matchId"           : match_id,
                    "completedSteamIds" : list(completed_steam_ids),
                    "utterances"        : all_utterances,
                    "savedAt"           : datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }, indent=2),
                ContentType="application/json",
            )
            log.info(f"  💾 Partial transcript saved ({len(completed_steam_ids)}/{len(meta['players'])} players)")
        except Exception as e:
            log.warning(f"  Could not save partial transcript: {e}")

    # ── KEY FIX: If ALL players failed, don't write empty transcript and advance.
    # Mark as error_transcription so the session is visible as failed in the app
    # and can be retried once the underlying issue (missing audio, API errors) is resolved.
    if len(completed_steam_ids) == 0:
        msg = f"All {len(meta['players'])} players failed — no audio could be transcribed"
        log.error(f"❌ {match_id}: {msg}")
        append_error_log(match_id, "all_players_failed", msg)
        # Write blocked=true to permanently prevent re-queuing, regardless of status resets
        set_status(match_id, "error_transcription", {"error": msg, "blocked": True})
        log.error(f"  🚫 Session {match_id} marked as blocked — will never be re-queued")
        send_discord_alert(
            title=f"❌ Transcription failed — no audio transcribed",
            description=(
                f"**Match:** `{match_id}`\n"
                f"**Map:** {meta.get('map', 'unknown')}\n"
                f"**Failed players:** {len(meta['players'])}/{len(meta['players'])}\n\n"
                + "\n".join(f"- {e}" for e in player_errors)
                + "\n\nCheck `errors.json` in R2 for full details."
            ),
            color=0xFF0000,
        )
        return

    # ── Send Discord alert if any (but not all) players failed ────────────────
    if player_errors:
        map_label = meta.get("map", "unknown map")
        send_discord_alert(
            title=f"⚠️ Transcription errors on {map_label}",
            description=(
                f"**Match:** `{match_id}`\n"
                f"**Map:** {map_label}\n"
                f"**Failed players ({len(player_errors)}/{len(meta['players'])}):**\n"
                + "\n".join(f"- {e}" for e in player_errors)
                + f"\n\n**Completed:** {len(completed_steam_ids)}/{len(meta['players'])} players, {len(all_utterances)} utterances\n"
                + "Check `errors.json` in R2 for full details. Use retry button in app if needed."
            ),
            color=0xFF8800,
        )

    # ── Sort utterances by time ───────────────────────────────────────────────
    all_utterances.sort(key=lambda u: u.get("t_start", 0))

    # ── Write final transcript.json ───────────────────────────────────────────
    transcript_key = f"matches/{match_id}/transcript.json"
    transcript = {
        "matchId"          : match_id,
        "recordingStartMs" : meta.get("recordingStartMs"),
        "utterances"       : all_utterances,
        "playerCount"      : len(meta["players"]),
        "completedPlayers" : len(completed_steam_ids),
        "totalUtterances"  : len(all_utterances),
        "provider"         : "gemini",
    }

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=transcript_key,
            Body=json.dumps(transcript, indent=2),
            ContentType="application/json",
        )
        log.info(f"  ✅ transcript.json written: {len(all_utterances)} utterances from {len(completed_steam_ids)}/{len(meta['players'])} players")
    except Exception as e:
        msg = f"Could not write transcript.json: {e}"
        log.error(f"  ❌ {msg}")
        append_error_log(match_id, "transcript_write", msg)
        set_status(match_id, "error_transcription", {"error": msg})
        send_discord_alert(
            title=f"❌ Transcription failed — could not write transcript",
            description=(
                f"**Match:** `{match_id}`\n"
                f"**Map:** {meta.get('map', 'unknown')}\n"
                f"**Error:** {e}\n\n"
                "Audio files have NOT been deleted. Use retry button in app."
            ),
            color=0xFF0000,
        )
        return

    # ── ONLY delete audio AFTER transcript.json is confirmed written ──────────
    for audio_key in audio_keys_to_delete:
        try:
            s3.delete_object(Bucket=S3_BUCKET, Key=audio_key)
            log.info(f"  🗑️  Deleted audio: {audio_key}")
        except Exception as e:
            log.warning(f"  Failed to delete audio {audio_key}: {e}")

    # ── Clean up partial transcript ───────────────────────────────────────────
    try:
        s3.delete_object(Bucket=S3_BUCKET, Key=partial_key)
    except Exception:
        pass

    # ── Advance to alignment ──────────────────────────────────────────────────
    set_status(match_id, "pending_alignment", {
        "transcriptKey": transcript_key,
        "transcribedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })

    log.info(
        f"✅ Transcription complete for {match_id}: "
        f"{len(all_utterances)} utterances from {len(completed_steam_ids)}/{len(meta['players'])} players"
    )


def main():
    log.info("🎙️ Transcription worker started (Gemini)")
    while True:
        try:
            check_stale_sessions()

            pending = list_pending_sessions()
            if pending:
                log.info(f"Found {len(pending)} pending sessions: {pending}")
                for match_id in pending:
                    try:
                        process_session(match_id)
                    except Exception as e:
                        msg = str(e)
                        log.error(f"Error processing {match_id}: {msg}")
                        append_error_log(match_id, "process_session", msg)
                        try:
                            set_status(match_id, "error_transcription", {"error": msg})
                        except Exception:
                            pass
                        try:
                            meta = get_meta(match_id)
                        except Exception:
                            meta = {}
                        send_discord_alert(
                            title=f"❌ Transcription session crashed",
                            description=(
                                f"**Match:** `{match_id}`\n"
                                f"**Map:** {meta.get('map', 'unknown')}\n"
                                f"**Error:** {msg}\n\n"
                                "Check Railway logs and `errors.json` in R2. Use retry button in app."
                            ),
                            color=0xFF0000,
                        )
            else:
                log.debug("No pending sessions, sleeping…")
        except Exception as e:
            log.error(f"Worker loop error: {e}")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
