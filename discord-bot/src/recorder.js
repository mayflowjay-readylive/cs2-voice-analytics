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
const OPUS_FRAME_DURATION_MS = 20;
const SILENCE_FRAME = Buffer.from([0xf8, 0xff, 0xfe]);

export class SessionRecorder {
  constructor({ matchId, connection, voiceChannel, playerMap, excludedDiscordIds }) {
    this.matchId = matchId;
    this.connection = connection;
    this.voiceChannel = voiceChannel;
    this.playerMap = playerMap;
    this.excludedDiscordIds = excludedDiscordIds || new Set();
    this.tmpDir = join(os.tmpdir(), `cs2-match-${matchId}`);
    mkdirSync(this.tmpDir, { recursive: true });
    this.receivers = new Map();
    this.audioFiles = [];
    this.recordingStartTime = null;
  }

  start() {
    const receiver = this.connection.receiver;
    console.log(`🎧 Receiver attached, waiting for speaking events...`);
    console.log(`🔌 Connection state: ${this.connection.state.status}`);
    if (this.excludedDiscordIds.size > 0) {
      console.log(`🚫 Excluded Discord IDs: ${[...this.excludedDiscordIds].join(", ")}`);
    }

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
        // Check exclusion list
        if (this.excludedDiscordIds.has(userId)) {
          return;
        }

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
        // Check exclusion list
        if (this.excludedDiscordIds.has(userId)) {
          return;
        }

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

    let lastPacketTime = existingLastPacketTime || Date.now();
    let isFirstPacket = !isReconnect;

    audioStream.on("data", (packet) => {
      const now = Date.now();

      if (isFirstPacket) {
        const silenceMs = now - this.recordingStartTime;
        if (silenceMs > OPUS_FRAME_DURATION_MS) {
          const silenceFrames = Math.floor(silenceMs / OPUS_FRAME_DURATION_MS);
          const framesToWrite = Math.min(silenceFrames, 30000);
          for (let i = 0; i < framesToWrite; i++) {
            this._writePacket(writeStream, SILENCE_FRAME);
          }
          console.log(`🔇 Injected ${framesToWrite} silence frames (${(framesToWrite * OPUS_FRAME_DURATION_MS / 1000).toFixed(1)}s) for ${userId} leading gap`);
        }
        isFirstPacket = false;
        lastPacketTime = now;
      } else {
        const gapMs = now - lastPacketTime;
        if (gapMs > OPUS_FRAME_DURATION_MS * 2) {
          const silenceMs = gapMs - OPUS_FRAME_DURATION_MS;
          const silenceFrames = Math.floor(silenceMs / OPUS_FRAME_DURATION_MS);
          const framesToWrite = Math.min(silenceFrames, 3000);
          if (framesToWrite > 5) {
            for (let i = 0; i < framesToWrite; i++) {
              this._writePacket(writeStream, SILENCE_FRAME);
            }
          }
        }
      }

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
