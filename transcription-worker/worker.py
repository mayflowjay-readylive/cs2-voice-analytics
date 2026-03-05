"""
Transcription Worker (AssemblyAI)
==================================
Polls S3 for sessions with status=pending_transcription.
Uploads per-player OGG/Opus audio directly to AssemblyAI,
polls until complete, writes timestamped transcripts back to S3,
then deletes audio files from R2 (retention policy).
"""

import os
import json
import time
import logging
from dataclasses import dataclass, asdict

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

S3_BUCKET          = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT        = os.environ.get("S3_ENDPOINT")
ASSEMBLYAI_API_KEY = os.environ.get("ASSEMBLYAI_API_KEY", "")
POLL_INTERVAL      = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
AAI_POLL_INTERVAL  = int(os.environ.get("AAI_POLL_INTERVAL_SECONDS", "5"))

AAI_BASE    = "https://api.assemblyai.com/v2"
AAI_HEADERS = {"authorization": ASSEMBLYAI_API_KEY, "content-type": "application/json"}

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
    t_start: float
    t_end: float
    text: str
    confidence: float


# ─── AssemblyAI helpers ───────────────────────────────────────────────────────

def aai_upload(audio_bytes: bytes) -> str:
    resp = requests.post(
        f"{AAI_BASE}/upload",
        headers={"authorization": ASSEMBLYAI_API_KEY, "content-type": "audio/ogg"},
        data=audio_bytes,
    )
    resp.raise_for_status()
    return resp.json()["upload_url"]


def aai_submit(upload_url: str) -> str:
    payload = {
        "audio_url": upload_url,
        "language_code": "da",
    }
    resp = requests.post(f"{AAI_BASE}/transcript", headers=AAI_HEADERS, json=payload)
    if not resp.ok:
        log.error(f"  AssemblyAI submit error {resp.status_code}: {resp.text}")
    resp.raise_for_status()
    return resp.json()["id"]


def aai_poll(transcript_id: str, max_wait_seconds: int = 600) -> dict:
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
            raise TimeoutError(f"AssemblyAI job {transcript_id} timed out after {max_wait_seconds}s")
        log.debug(f"    [{transcript_id}] status={status}, waiting…")
        time.sleep(AAI_POLL_INTERVAL)
        waited += AAI_POLL_INTERVAL


def aai_to_utterances(steam_id: str, result: dict) -> list[Utterance]:
    words = result.get("words", [])
    if not words:
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
    SILENCE_THRESHOLD_MS = 800
    current_words = [words[0]]
    for word in words[1:]:
        gap = word["start"] - current_words[-1]["end"]
        if gap > SILENCE_THRESHOLD_MS:
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
        t_start=words[0]["start"] / 1000.0,
        t_end=words[-1]["end"] / 1000.0,
        text=text,
        confidence=round(confidence, 3),
    )


# ─── S3 helpers ───────────────────────────────────────────────────────────────

def list_pending_sessions() -> list[str]:
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
    meta = get_meta(match_id)
    meta["status"] = status
    if extra:
        meta.update(extra)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"matches/{match_id}/meta.json",
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
        transcribed_successfully = False

        try:
            log.info(f"  Downloading audio for {steam_id} ({audio_key})…")
            audio_obj   = s3.get_object(Bucket=S3_BUCKET, Key=audio_key)
            audio_bytes = audio_obj["Body"].read()
            log.info(f"  Audio size: {len(audio_bytes) / 1024:.1f} KB")

            if len(audio_bytes) < 1024:
                log.info(f"  Skipping {steam_id}: audio too short")
                transcribed_successfully = True  # nothing to retry
                continue

            log.info(f"  Uploading to AssemblyAI…")
            upload_url = aai_upload(audio_bytes)

            transcript_id = aai_submit(upload_url)
            log.info(f"  Submitted: transcript_id={transcript_id}")

            result = aai_poll(transcript_id)
            log.info(f"  Completed: {len(result.get('words', []))} words")

            utterances = aai_to_utterances(steam_id, result)
            log.info(f"  → {len(utterances)} utterances for {steam_id}")
            all_utterances.extend(utterances)
            transcribed_successfully = True

        except Exception as e:
            log.error(f"  Failed to transcribe {steam_id}: {e}")

        if transcribed_successfully:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=audio_key)
                log.info(f"  🗑️  Deleted audio: {audio_key}")
            except Exception as e:
                log.warning(f"  Failed to delete {audio_key}: {e}")

    all_utterances.sort(key=lambda u: u.t_start)

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
                        try:
                            set_status(match_id, "error_transcription")
                        except Exception:
                            pass
            else:
                log.debug("No pending sessions, sleeping…")
        except Exception as e:
            log.error(f"Worker loop error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
