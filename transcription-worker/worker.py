"""
Transcription Worker (AssemblyAI)
==================================
Polls S3 for sessions with status=pending_transcription.
Converts per-player raw PCM audio to WAV, submits to AssemblyAI,
polls until complete, and writes timestamped transcripts back to S3.

No GPU or heavy compute needed — AssemblyAI handles inference.
Free tier gives $50 in credits (~hundreds of hours of audio).

Deploy as a Railway worker. Even a 512MB instance is fine since
we're just doing I/O, PCM→WAV conversion, and HTTP calls.
"""

import io
import os
import json
import struct
import time
import logging
from dataclasses import dataclass, asdict

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

S3_BUCKET          = os.environ["S3_BUCKET"]
S3_ENDPOINT        = os.environ.get("S3_ENDPOINT")
ASSEMBLYAI_API_KEY = os.environ["ASSEMBLYAI_API_KEY"]
POLL_INTERVAL      = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
AAI_POLL_INTERVAL  = int(os.environ.get("AAI_POLL_INTERVAL_SECONDS", "5"))

SAMPLE_RATE = 48000
CHANNELS    = 2
BIT_DEPTH   = 16

AAI_BASE    = "https://api.assemblyai.com/v2"
AAI_HEADERS = {"authorization": ASSEMBLYAI_API_KEY, "content-type": "application/json"}

# CS2-specific vocabulary boost — helps AssemblyAI recognise callouts correctly
CS2_WORD_BOOST = [
    "B site", "A site", "catwalk", "mid", "long", "short", "banana",
    "connector", "CT spawn", "T spawn", "jungle", "palace", "apartments",
    "window", "boost", "one-tap", "AWP", "AK", "M4", "Molotov", "flash",
    "smoke", "HE", "defuse", "plant", "eco", "force buy", "full buy",
    "rush", "retake", "anchor", "entry", "lurk", "IGL", "rotate",
    "Heaven", "Hell", "pit", "arch", "library", "van", "truck",
]

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name=os.environ.get("AWS_REGION", "auto"),
)


# ─── Data types ───────────────────────────────────────────────────────────────

@dataclass
class Utterance:
    steam_id: str
    t_start: float   # seconds since recording start
    t_end: float
    text: str
    confidence: float


# ─── PCM → WAV helper ─────────────────────────────────────────────────────────

def pcm_to_wav(pcm_bytes: bytes, sample_rate: int = SAMPLE_RATE, channels: int = CHANNELS, bit_depth: int = BIT_DEPTH) -> bytes:
    """
    Wraps raw PCM bytes in a WAV container so AssemblyAI can ingest it.
    Discord outputs 48kHz stereo 16-bit PCM — we preserve that and let
    AssemblyAI handle the resampling on their end.
    """
    data_size    = len(pcm_bytes)
    byte_rate    = sample_rate * channels * (bit_depth // 8)
    block_align  = channels * (bit_depth // 8)

    buf = io.BytesIO()
    buf.write(b"RIFF")
    buf.write(struct.pack("<I", 36 + data_size))   # file size - 8
    buf.write(b"WAVE")
    buf.write(b"fmt ")
    buf.write(struct.pack("<I", 16))               # PCM chunk size
    buf.write(struct.pack("<H", 1))                # PCM format
    buf.write(struct.pack("<H", channels))
    buf.write(struct.pack("<I", sample_rate))
    buf.write(struct.pack("<I", byte_rate))
    buf.write(struct.pack("<H", block_align))
    buf.write(struct.pack("<H", bit_depth))
    buf.write(b"data")
    buf.write(struct.pack("<I", data_size))
    buf.write(pcm_bytes)

    return buf.getvalue()


# ─── AssemblyAI API helpers ───────────────────────────────────────────────────

def aai_upload(wav_bytes: bytes) -> str:
    """Upload audio to AssemblyAI's storage and return the upload URL."""
    resp = requests.post(
        f"{AAI_BASE}/upload",
        headers={"authorization": ASSEMBLYAI_API_KEY},
        data=wav_bytes,
    )
    resp.raise_for_status()
    return resp.json()["upload_url"]


def aai_submit(upload_url: str) -> str:
    """Submit a transcription job and return the transcript ID."""
    payload = {
        "audio_url": upload_url,
        "language_code": "en",
        "punctuate": True,
        "format_text": True,
        "word_boost": CS2_WORD_BOOST,
        "boost_param": "high",        # aggressively boost CS2 vocab
        "disfluencies": False,        # strip filler words (um, uh)
    }
    resp = requests.post(f"{AAI_BASE}/transcript", headers=AAI_HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["id"]


def aai_poll(transcript_id: str, max_wait_seconds: int = 600) -> dict:
    """Poll until transcript is complete or errored. Returns the full result.
    Raises TimeoutError if the job hasn't completed within max_wait_seconds.
    """
    url = f"{AAI_BASE}/transcript/{transcript_id}"
    waited = 0
    while True:
        resp = requests.get(url, headers=AAI_HEADERS)
        resp.raise_for_status()
        result = resp.json()
        status = result["status"]

        if status == "completed":
            return result
        elif status == "error":
            raise RuntimeError(f"AssemblyAI error for {transcript_id}: {result.get('error')}")
        elif waited >= max_wait_seconds:
            raise TimeoutError(f"AssemblyAI job {transcript_id} timed out after {max_wait_seconds}s (status={status})")
        else:
            log.debug(f"    [{transcript_id}] status={status}, waiting…")
            time.sleep(AAI_POLL_INTERVAL)
            waited += AAI_POLL_INTERVAL


def aai_to_utterances(steam_id: str, result: dict) -> list[Utterance]:
    """
    Convert an AssemblyAI transcript result into our Utterance format.
    We use the word-level timestamps to build utterance segments,
    grouping words into natural speech chunks using silence gaps.
    """
    words = result.get("words", [])
    if not words:
        # Fall back to full transcript as a single utterance
        if result.get("text"):
            return [Utterance(
                steam_id=steam_id,
                t_start=0.0,
                t_end=result.get("audio_duration", 0.0),
                text=result["text"],
                confidence=result.get("confidence", 1.0),
            )]
        return []

    utterances = []
    SILENCE_THRESHOLD_MS = 800  # gap > 800ms = new utterance

    current_words = [words[0]]
    for word in words[1:]:
        gap = word["start"] - current_words[-1]["end"]
        if gap > SILENCE_THRESHOLD_MS:
            # Flush current utterance
            utterances.append(_words_to_utterance(steam_id, current_words))
            current_words = [word]
        else:
            current_words.append(word)

    if current_words:
        utterances.append(_words_to_utterance(steam_id, current_words))

    return utterances


def _words_to_utterance(steam_id: str, words: list[dict]) -> Utterance:
    text = " ".join(w["text"] for w in words)
    confidence = sum(w.get("confidence", 1.0) for w in words) / len(words)
    return Utterance(
        steam_id=steam_id,
        t_start=words[0]["start"] / 1000.0,   # ms → seconds
        t_end=words[-1]["end"] / 1000.0,
        text=text,
        confidence=round(confidence, 3),
    )


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def list_pending_sessions() -> list[str]:
    """Return match IDs that need transcription.
    Includes 'pending_transcription' and also 'transcribing' sessions
    that were interrupted by a crash and need to be retried.
    """
    pending = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matches/", Delimiter="/"):
        for prefix in page.get("CommonPrefixes", []):
            match_prefix = prefix["Prefix"]
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{match_prefix}meta.json")
                meta = json.loads(obj["Body"].read())
                if meta.get("status") in ("pending_transcription", "transcribing"):
                    pending.append(meta["matchId"])
            except Exception:
                pass
    return pending


def get_meta(match_id: str) -> dict:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"matches/{match_id}/meta.json")
    return json.loads(obj["Body"].read())


def set_status(match_id: str, status: str, extra: dict = None):
    prefix = f"matches/{match_id}"
    meta = get_meta(match_id)
    meta["status"] = status
    if extra:
        meta.update(extra)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{prefix}/meta.json",
        Body=json.dumps(meta, indent=2),
        ContentType="application/json",
    )


# ─── Main processing ──────────────────────────────────────────────────────────

def process_session(match_id: str):
    log.info(f"Processing match: {match_id}")
    set_status(match_id, "transcribing")
    meta = get_meta(match_id)
    all_utterances = []

    for player in meta["players"]:
        steam_id  = player["steamId"]
        audio_key = player["audioKey"]

        try:
            # 1. Download raw PCM from S3
            log.info(f"  Downloading PCM for {steam_id}…")
            audio_obj  = s3.get_object(Bucket=S3_BUCKET, Key=audio_key)
            pcm_bytes  = audio_obj["Body"].read()

            if len(pcm_bytes) < SAMPLE_RATE * CHANNELS * (BIT_DEPTH // 8) * 0.5:
                log.info(f"  Skipping {steam_id}: audio too short")
                continue

            # 2. Wrap PCM in WAV container
            wav_bytes = pcm_to_wav(pcm_bytes)
            log.info(f"  WAV size: {len(wav_bytes) / 1024:.1f} KB")

            # 3. Upload to AssemblyAI
            log.info(f"  Uploading to AssemblyAI…")
            upload_url = aai_upload(wav_bytes)

            # 4. Submit transcription job
            transcript_id = aai_submit(upload_url)
            log.info(f"  Submitted: transcript_id={transcript_id}")

            # 5. Poll for result
            result = aai_poll(transcript_id)
            log.info(f"  Completed: {len(result.get('words', []))} words")

            # 6. Convert to our Utterance format
            utterances = aai_to_utterances(steam_id, result)
            log.info(f"  → {len(utterances)} utterances for {steam_id}")
            all_utterances.extend(utterances)

        except Exception as e:
            log.error(f"  Failed to transcribe {steam_id}: {e}")

    # Sort by t_start across all players
    all_utterances.sort(key=lambda u: u.t_start)

    # Upload merged transcript
    transcript = {
        "matchId":          match_id,
        "recordingStartMs": meta["recordingStartMs"],
        "utterances":       [asdict(u) for u in all_utterances],
        "playerCount":      len(meta["players"]),
        "totalUtterances":  len(all_utterances),
        "provider":         "assemblyai",
    }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"matches/{match_id}/transcript.json",
        Body=json.dumps(transcript, indent=2),
        ContentType="application/json",
    )

    set_status(match_id, "pending_alignment", {
        "transcriptKey": f"matches/{match_id}/transcript.json",
        "transcribedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    })

    log.info(f"✅ Transcription complete for {match_id}: {len(all_utterances)} utterances")


def main():
    log.info("🎙️ Transcription worker started (AssemblyAI)")
    while True:
        try:
            pending = list_pending_sessions()
            if pending:
                log.info(f"Found {len(pending)} pending sessions: {pending}")
                for match_id in pending:
                    try:
                        process_session(match_id)
                    except Exception as e:
                        log.error(f"Error processing {match_id}: {e}")
                        set_status(match_id, "error_transcription")
            else:
                log.debug("No pending sessions, sleeping…")
        except Exception as e:
            log.error(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
