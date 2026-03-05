const OpusScript = require("opusscript");
const { createWriteStream, mkdirSync } = require("fs");
const { join } = require("path");
const { Transform } = require("stream");
const os = require("os");

const SAMPLE_RATE = 48000;
const CHANNELS = 2;

class SessionRecorder {
  constructor({ matchId, connection, voiceChannel, playerMap }) {
    this.matchId = matchId;
    this.connection = connection; // Eris VoiceConnection
    this.voiceChannel = voiceChannel;
    this.playerMap = playerMap; // discordId → steamId
    this.tmpDir = join(os.tmpdir(), `cs2-match-${matchId}`);
    mkdirSync(this.tmpDir, { recursive: true });
    this.receivers = new Map(); // discordId → { filePath, writeStream, decoder }
    this.audioFiles = []; // { discordId, steamId, filePath }
    this._onReceive = {};
  }

  start() {
    console.log(`🎧 Receiver attached, waiting for speaking events...`);
    console.log(`🔌 Connection state: ${this.connection.ready ? "ready" : "not ready"}`);

    this.connection.on("speak", (userID) => {
      console.log(`🗣️ Speaking detected for ${userID}`);
      if (!this.receivers.has(userID)) {
        this._startUserRecording(userID);
      }
    });
  }

  _startUserRecording(userID) {
    const steamId = this.playerMap[userID] || `discord_${userID}`;
    const filePath = join(this.tmpDir, `audio_${steamId}.pcm`);

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
    decoder.pipe(writeStream);

    const onReceive = (data) => {
      decoder.write(data);
    };

    this.connection.receive("opus", onReceive);
    this._onReceive[userID] = onReceive;

    this.receivers.set(userID, { filePath, decoder, writeStream, steamId });
    console.log(`🎤 Started recording user ${userID} (steam: ${steamId}) → ${filePath}`);
  }

  async stop() {
    console.log(`⏹️ Stopping recording, receivers: ${this.receivers.size}`);
    const closePromises = [];

    for (const [userID, { filePath, decoder, writeStream, steamId }] of this.receivers) {
      if (this._onReceive[userID]) {
        this.connection.removeListener("opus", this._onReceive[userID]);
      }
      closePromises.push(
        new Promise((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error(`Timed out waiting for writeStream for ${userID}`));
          }, 15_000);

          writeStream.once("finish", () => {
            clearTimeout(timeout);
            console.log(`✅ Closed recording for ${userID}`);
            this.audioFiles.push({ discordId: userID, steamId, filePath });
            resolve();
          });

          writeStream.once("error", (err) => {
            clearTimeout(timeout);
            reject(err);
          });

          decoder.end();
        })
      );
    }

    await Promise.all(closePromises);
    this.connection.stopPlaying();
    this.connection.disconnect();
    return this.audioFiles;
  }
}

module.exports = { SessionRecorder };
