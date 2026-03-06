"""
Transcription Worker (Gemini)
==============================
Polls S3 for sessions with status=pending_transcription.
Decodes per-player Opus packets to WAV, uploads to Gemini Files API,
transcribes in Danish, writes timestamped transcripts back to S3,
then deletes audio files from R2 (retention policy).
"""

import os
import json
import time
import logging
import struct
import wave
import tempfile
from dataclasses import dataclass, asdict

import boto3
import opuslib
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

S3_BUCKET     = os.environ.get("S3_BUCKET", "")
S3_ENDPOINT   = os.environ.get("S3_ENDPOINT")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
GEMINI_MODEL  = os.environ.get("GEMINI_MODEL", "gemini-2.0-flash")

client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

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


# ─── Audio decoding ───────────────────────────────────────────────────────────

def opus_to_wav(opus_bytes: bytes) -> bytes:
    """Decode length-prefixed raw Opus packets to WAV (48kHz stereo 16-bit)."""
    SAMPLE_RATE = 48000
    CHANNELS = 2
    FRAME_SIZE = 960  # 20ms at 48kHz

    decoder = opuslib.Decoder(SAMPLE_RATE, CHANNELS)
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


# ─── Gemini transcription ─────────────────────────────────────────────────────

TRANSCRIPTION_PROMPT = """This is a voice recording from a CS2 (Counter-Strike 2) match.
The player is speaking Danish, often mixing in English CS2 terms and callouts
(e.g. "B site", "AWP", "flash", "smoke", "banana", "catwalk", "CT", "T side").

Transcribe exactly what is said. Keep English CS2 terms as-is.
Return ONLY a JSON array of utterances with this exact schema, no preamble:
[
  {
    "t_start": <seconds as float>,
    "t_end": <seconds as float>,
    "text": "<transcribed text>",
    "confidence": <0.0-1.0>
  }
]

If there is silence or no speech, return an empty array: []
"""

def transcribe_with_gemini(wav_bytes: bytes, steam_id: str) -> list[Utterance]:
    """Upload WAV to Gemini Files API and transcribe."""
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.write(wav_bytes)
    tmp.close()

    try:
        log.info(f"  Uploading to Gemini Files API…")
        audio_file = client.files.upload(
            file=tmp.name,
            config=types.UploadFileConfig(mime_type="audio/wav"),
        )

        # Wait for file to be processed
        while audio_file.state.name == "PROCESSING":
            time.sleep(1)
            audio_file = client.files.get(name=audio_file.name)

        if audio_file.state.name == "FAILED":
            raise RuntimeError(f"Gemini file processing failed: {audio_file.state}")

        log.info(f"  Requesting transcription…")
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=[TRANSCRIPTION_PROMPT, audio_file],
            config=types.GenerateContentConfig(
                temperature=0.0,
                response_mime_type="application/json",
            ),
        )

        # Clean up uploaded file
        try:
            client.files.delete(name=audio_file.name)
        except Exception:
            pass

        text = response.text.strip().replace("```json", "").replace("```", "").strip()
        raw_utterances = json.loads(text)

        return [
            Utterance(
                steam_id=steam_id,
                t_start=u.get("t_start", 0.0),
                t_end=u.get("t_end", 0.0),
                text=u.get("text", ""),
                confidence=u.get("confidence", 1.0),
            )
            for u in raw_utterances
            if u.get("text", "").strip()
        ]

    finally:
        os.unlink(tmp.name)


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
                transcribed_successfully = True
                continue

            log.info(f"  Decoding Opus packets to WAV…")
            wav_bytes = opus_to_wav(audio_bytes)
            if not wav_bytes:
                log.info(f"  Skipping {steam_id}: no decodable audio")
                transcribed_successfully = True
                continue
            log.info(f"  WAV size: {len(wav_bytes) / 1024:.1f} KB")

            utterances = transcribe_with_gemini(wav_bytes, steam_id)
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
        "provider":         "gemini",
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
    log.info("🎙️ Transcription worker started (Gemini)")
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
