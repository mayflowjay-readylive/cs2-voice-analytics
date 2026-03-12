import { createWriteStream, mkdirSync } from "fs";
import { join } from "path";
import os from "os";
import { EndBehaviorType } from "@discordjs/voice";

// Saves raw Opus packets in a simple length-prefixed binary format:
//   [4 bytes: packet length (uint32 LE)] [N bytes: opus packet]
// The worker decodes this using opuslib, then converts to WAV for transcription.
//
// IMPORTANT: This recorder injects silence frames during gaps in speech to preserve
// real-time alignment. Without this, Gemini's timestamps would be relative to
// compressed continuous speech, making round-level alignment impossible.

const DAVE_HANDSHAKE_DELAY_MS = 3000;
const OPUS_FRAME_DURATION_MS = 20; // Each Opus frame = 20ms
const SILENCE_FRAME = Buffer.from([0xf8, 0xff, 0xfe]); // Opus silence frame (stereo, 20ms)

export class SessionRecorder {
  constructor({ matchId, connection, voiceChannel, playerMap }) {
    this.matchId = matchId;
    this.connection = connection;
    this.voiceChannel = voiceChannel;
    this.playerMap = playerMap;
    this.tmpDir = join(os.tmpdir(), `cs2-match-${matchId}`);
    mkdirSync(this.tmpDir, { recursive: true });
    this.receivers = new Map();
    this.audioFiles = [];
    this.recordingStartTime = null; // Set when recording actually starts
  }

  start() {
    const receiver = this.connection.receiver;
    console.log(`🎧 Receiver attached, waiting for speaking events...`);
    console.log(`🔌 Connection state: ${this.connection.state.status}`);

    this.connection.on("error", (err) => {
      console.warn(`⚠️ Voice connection error (suppressed): ${err.message}`);
    });
    if (receiver.on) {
      receiver.on("error", (err) => {
        console.warn(`⚠️ Receiver error (suppressed): ${err.message}`);
      });
    }

    console.log(`⏳ Waiting ${DAVE_HANDSHAKE_DELAY_MS}ms for DAVE handshake to complete…`);
    setTimeout(() => {
      console.log(`✅ DAVE delay complete — now listening for speech`);
      this.recordingStartTime = Date.now();

      receiver.speaking.on("start", (userId) => {
        console.log(`🗣️ Speaking start detected for ${userId}`);

        if (this.receivers.has(userId)) {
          const existing = this.receivers.get(userId);
          if (!existing.audioStream.destroyed) {
            return;
          }
          console.log(`🔄 ${userId} reconnected — re-subscribing (appending to existing file)`);
          try { existing.writeStream.end(); } catch {}
          const existingFilePath = existing.filePath;
          const existingSteamId = existing.steamId;
          const existingLastPacketTime = existing.lastPacketTime;
          this.receivers.delete(userId);
          this._startUserRecording(userId, receiver, existingFilePath, existingSteamId, existingLastPacketTime);
          return;
        }

        this._startUserRecording(userId, receiver, null, null, null);
      });

      receiver.speaking.on("end", (userId) => {
        console.log(`🔇 Speaking end for ${userId}`);
        if (!this.receivers.has(userId)) {
          console.log(`🔄 Missed start for ${userId} during DAVE delay — subscribing now`);
          this._startUserRecording(userId, receiver, null, null, null);
        }
      });
    }, DAVE_HANDSHAKE_DELAY_MS);
  }

  _writePacket(writeStream, packet) {
    const lenBuf = Buffer.alloc(4);
    lenBuf.writeUInt32LE(packet.length, 0);
    writeStream.write(lenBuf);
    writeStream.write(packet);
  }

  _startUserRecording(userId, receiver, existingFilePath, existingSteamId, existingLastPacketTime) {
    const steamId = existingSteamId || this.playerMap[userId] || `discord_${userId}`;
    const filePath = existingFilePath || join(this.tmpDir, `audio_${steamId}.opus`);
    const isReconnect = !!existingFilePath;

    const audioStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });

    const writeStream = createWriteStream(filePath, { flags: isReconnect ? "a" : "w" });

    // Track timing for silence injection
    let lastPacketTime = existingLastPacketTime || Date.now();
    let isFirstPacket = !isReconnect;

    audioStream.on("data", (packet) => {
      const now = Date.now();

      if (isFirstPacket) {
        // For the first packet, inject silence from recording start to now
        // so this user's audio is aligned to the recording timeline
        const silenceMs = now - this.recordingStartTime;
        if (silenceMs > OPUS_FRAME_DURATION_MS) {
          const silenceFrames = Math.floor(silenceMs / OPUS_FRAME_DURATION_MS);
          // Cap at reasonable amount (max 10 minutes of leading silence = 30000 frames)
          const framesToWrite = Math.min(silenceFrames, 30000);
          for (let i = 0; i < framesToWrite; i++) {
            this._writePacket(writeStream, SILENCE_FRAME);
          }
          console.log(`🔇 Injected ${framesToWrite} silence frames (${(framesToWrite * OPUS_FRAME_DURATION_MS / 1000).toFixed(1)}s) for ${userId} leading gap`);
        }
        isFirstPacket = false;
        lastPacketTime = now;
      } else {
        // Inject silence frames for gaps between speech segments
        const gapMs = now - lastPacketTime;
        if (gapMs > OPUS_FRAME_DURATION_MS * 2) {
          // Subtract one frame duration since the current packet covers some time
          const silenceMs = gapMs - OPUS_FRAME_DURATION_MS;
          const silenceFrames = Math.floor(silenceMs / OPUS_FRAME_DURATION_MS);
          // Cap at 60 seconds of silence (3000 frames) — longer gaps are unusual
          const framesToWrite = Math.min(silenceFrames, 3000);
          if (framesToWrite > 5) {
            // Only inject if gap is meaningful (>100ms)
            for (let i = 0; i < framesToWrite; i++) {
              this._writePacket(writeStream, SILENCE_FRAME);
            }
          }
        }
      }

      // Write the actual audio packet
      this._writePacket(writeStream, packet);
      lastPacketTime = now;
    });

    audioStream.on("error", (err) => {
      console.warn(`⚠️ Audio stream error for ${userId} (suppressed): ${err.message}`);
    });

    this.receivers.set(userId, { filePath, audioStream, writeStream, steamId, lastPacketTime });
    const mode = isReconnect ? "RECONNECT/append" : "new";
    console.log(`🎤 Started recording user ${userId} (steam: ${steamId}, mode: ${mode}) → ${filePath}`);
  }

  async stop() {
    console.log(`⏹️ Stopping recording, receivers: ${this.receivers.size}`);
    const closePromises = [];

    for (const [userId, { filePath, audioStream, writeStream, steamId }] of this.receivers) {
      closePromises.push(
        new Promise((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error(`Timed out for ${userId}`)), 15_000);
          writeStream.once("finish", () => {
            clearTimeout(timeout);
            console.log(`✅ Closed recording for ${userId}`);
            this.audioFiles.push({ discordId: userId, steamId, filePath });
            resolve();
          });
          writeStream.once("error", (err) => { clearTimeout(timeout); reject(err); });
          audioStream.destroy();
          writeStream.end();
        })
      );
    }

    await Promise.all(closePromises);
    this.connection.destroy();
    return this.audioFiles;
  }
}
