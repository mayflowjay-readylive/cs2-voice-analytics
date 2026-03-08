import { createWriteStream, mkdirSync } from "fs";
import { join } from "path";
import os from "os";
import { EndBehaviorType } from "@discordjs/voice";

// Saves raw Opus packets in a simple length-prefixed binary format:
//   [4 bytes: packet length (uint32 LE)] [N bytes: opus packet]
// The worker decodes this using opuslib, then converts to WAV for transcription.

const DAVE_HANDSHAKE_DELAY_MS = 3000; // Wait for DAVE E2EE negotiation to complete

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
  }

  start() {
    const receiver = this.connection.receiver;
    console.log(`🎧 Receiver attached, waiting for speaking events...`);
    console.log(`🔌 Connection state: ${this.connection.state.status}`);

    // Suppress DAVE E2EE decryption errors during handshake —
    // these fire when a user joins mid-session before DAVE negotiation completes.
    // Without these handlers the error propagates and crashes the process.
    this.connection.on("error", (err) => {
      console.warn(`⚠️ Voice connection error (suppressed): ${err.message}`);
    });
    if (receiver.on) {
      receiver.on("error", (err) => {
        console.warn(`⚠️ Receiver error (suppressed): ${err.message}`);
      });
    }

    // Delay attaching speaking listeners until DAVE handshake is complete.
    // This prevents dropped/errored packets from the first few seconds being
    // recorded as garbage, and avoids crashes from unencrypted handshake packets.
    console.log(`⏳ Waiting ${DAVE_HANDSHAKE_DELAY_MS}ms for DAVE handshake to complete…`);
    setTimeout(() => {
      console.log(`✅ DAVE delay complete — now listening for speech`);

      receiver.speaking.on("start", (userId) => {
        console.log(`🗣️ Speaking start detected for ${userId}`);

        // Check if we already have a receiver for this user
        if (this.receivers.has(userId)) {
          const existing = this.receivers.get(userId);
          if (!existing.audioStream.destroyed) {
            // Stream still active — normal speech resume after a pause, skip
            return;
          }
          // Stream is dead — user disconnected and reconnected mid-match.
          // Clean up the old write stream and re-subscribe, appending to the same file.
          console.log(`🔄 ${userId} reconnected — re-subscribing (appending to existing file)`);
          try { existing.writeStream.end(); } catch {}
          const existingFilePath = existing.filePath;
          const existingSteamId = existing.steamId;
          this.receivers.delete(userId);
          this._startUserRecording(userId, receiver, existingFilePath, existingSteamId);
          return;
        }

        this._startUserRecording(userId, receiver, null, null);
      });

      receiver.speaking.on("end", (userId) => {
        console.log(`🔇 Speaking end for ${userId}`);
        // If we see an end event for a user we're not tracking,
        // it means their start event fired during the DAVE delay.
        // Start recording them so we catch their next utterance.
        if (!this.receivers.has(userId)) {
          console.log(`🔄 Missed start for ${userId} during DAVE delay — subscribing now`);
          this._startUserRecording(userId, receiver, null, null);
        }
      });
    }, DAVE_HANDSHAKE_DELAY_MS);
  }

  _startUserRecording(userId, receiver, existingFilePath, existingSteamId) {
    const steamId = existingSteamId || this.playerMap[userId] || `discord_${userId}`;
    const filePath = existingFilePath || join(this.tmpDir, `audio_${steamId}.opus`);
    const isReconnect = !!existingFilePath;

    const audioStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });

    // Append mode if reconnecting, write mode if first time
    const writeStream = createWriteStream(filePath, { flags: isReconnect ? "a" : "w" });

    audioStream.on("data", (packet) => {
      const lenBuf = Buffer.alloc(4);
      lenBuf.writeUInt32LE(packet.length, 0);
      writeStream.write(lenBuf);
      writeStream.write(packet);
    });

    audioStream.on("error", (err) => {
      console.warn(`⚠️ Audio stream error for ${userId} (suppressed): ${err.message}`);
    });

    this.receivers.set(userId, { filePath, audioStream, writeStream, steamId });
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
