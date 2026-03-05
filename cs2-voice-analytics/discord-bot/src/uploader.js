import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { createReadStream } from "fs";

const s3 = new S3Client({
  region: process.env.AWS_REGION || "auto",
  endpoint: process.env.S3_ENDPOINT,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET = process.env.S3_BUCKET || "cs2-voice-analytics";

/**
 * Uploads all audio files + session metadata to S3/R2.
 *
 * Audio is uploaded first, meta.json last — the transcription worker
 * polls for meta.json with status=pending_transcription, so writing it
 * last ensures audio is already in place when the worker picks it up.
 *
 * Structure:
 *   matches/{matchId}/audio_{steamId}.pcm
 *   matches/{matchId}/meta.json        ← written last
 */
export async function uploadSession({ matchId, audioFiles, startedAt }) {
  const prefix = `matches/${matchId}`;

  // 1. Upload audio files first
  const uploadPromises = audioFiles.map(({ steamId, filePath }) =>
    s3.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: `${prefix}/audio_${steamId}.pcm`,
        Body: createReadStream(filePath),
        ContentType: "audio/pcm",
        Metadata: {
          matchId,
          steamId,
          sampleRate: "48000",
          channels: "2",
          bitDepth: "16",
        },
      })
    )
  );

  await Promise.all(uploadPromises);

  // 2. Upload meta.json only after all audio is safely stored
  const meta = {
    matchId,
    startedAt,
    recordingStartMs: startedAt,
    players: audioFiles.map(({ discordId, steamId }) => ({
      discordId,
      steamId,
      audioKey: `${prefix}/audio_${steamId}.pcm`,
    })),
    status: "pending_transcription",
    createdAt: new Date().toISOString(),
  };

  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${prefix}/meta.json`,
      Body: JSON.stringify(meta, null, 2),
      ContentType: "application/json",
    })
  );

  return { prefix, playerCount: audioFiles.length };
}
