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

// Sanitize strings for use in S3 metadata headers (ASCII printable only)
function sanitizeMetadata(value) {
  return String(value).replace(/[^\x20-\x7E]/g, "");
}

export async function uploadSession({ matchId, audioFiles, startedAt }) {
  const prefix = `matches/${matchId}`;

  const uploadPromises = audioFiles.map(({ steamId, filePath }) =>
    s3.send(new PutObjectCommand({
      Bucket: BUCKET,
      Key: `${prefix}/audio_${steamId}.opus`,
      Body: createReadStream(filePath),
      ContentType: "audio/opus",
      Metadata: {
        matchid: sanitizeMetadata(matchId),
        steamid: sanitizeMetadata(steamId),
        samplerate: "48000",
        channels: "2",
        codec: "opus",
      },
    }))
  );

  await Promise.all(uploadPromises);

  const meta = {
    matchId,
    startedAt,
    recordingStartMs: startedAt,
    players: audioFiles.map(({ discordId, steamId }) => ({
      discordId,
      steamId,
      audioKey: `${prefix}/audio_${steamId}.opus`,
    })),
    status: "pending_transcription",
    statusUpdatedAt: new Date().toISOString(),
    createdAt: new Date().toISOString(),
  };

  await s3.send(new PutObjectCommand({
    Bucket: BUCKET,
    Key: `${prefix}/meta.json`,
    Body: JSON.stringify(meta, null, 2),
    ContentType: "application/json",
  }));

  return { prefix, playerCount: audioFiles.length };
}
