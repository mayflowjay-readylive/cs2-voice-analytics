import OpusScript from "opusscript";
import { createWriteStream, mkdirSync } from "fs";
import { join } from "path";
import { Transform } from "stream";
import os from "os";
import { EndBehaviorType } from "@discordjs/voice";

const SAMPLE_RATE = 48000;
const CHANNELS = 2;

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

    receiver.speaking.on("start", (userId) => {
      console.log(`🗣️ Speaking start detected for ${userId}`);
      if (this.receivers.has(userId)) return;
      this._startUserRecording(userId, receiver);
    });

    receiver.speaking.on("end", (userId) => {
      console.log(`🔇 Speaking end for ${userId}`);
    });
  }

  _startUserRecording(userId, receiver) {
    const steamId = this.playerMap[userId] || `discord_${userId}`;
    const filePath = join(this.tmpDir, `audio_${steamId}.pcm`);

    const audioStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });

    const encoder = new OpusScript(SAMPLE_RATE, CHANNELS, OpusScript.Application.AUDIO);

    const decoder = new Transform({
      transform(chunk, _enc, cb) {
        try {
          const pcm = encoder.decode(chunk);
          cb(null, pcm);
        } catch {
          cb();
        }
      },
    });

    const writeStream = createWriteStream(filePath);
    audioStream.pipe(decoder).pipe(writeStream);

    this.receivers.set(userId, { filePath, audioStream, decoder, writeStream, steamId });
    console.log(`🎤 Started recording user ${userId} (steam: ${steamId}) → ${filePath}`);
  }

  async stop() {
    console.log(`⏹️ Stopping recording, receivers: ${this.receivers.size}`);
    const closePromises = [];

    for (const [userId, { filePath, audioStream, decoder, writeStream, steamId }] of this.receivers) {
      closePromises.push(
        new Promise((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error(`Timed out waiting for writeStream for ${userId}`));
          }, 15_000);

          writeStream.once("finish", () => {
            clearTimeout(timeout);
            console.log(`✅ Closed recording for ${userId}`);
            this.audioFiles.push({ discordId: userId, steamId, filePath });
            resolve();
          });

          writeStream.once("error", (err) => {
            clearTimeout(timeout);
            reject(err);
          });

          audioStream.unpipe(decoder);
          audioStream.destroy();
          decoder.end();
        })
      );
    }

    await Promise.all(closePromises);
    this.connection.destroy();
    return this.audioFiles;
  }
}
