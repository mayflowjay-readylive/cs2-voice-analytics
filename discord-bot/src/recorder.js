import { createWriteStream, mkdirSync } from "fs";
import { join } from "path";
import os from "os";
import { EndBehaviorType } from "@discordjs/voice";

// ─── Minimal OGG/Opus writer ──────────────────────────────────────────────────
// Wraps raw Opus packets in an OGG container without decoding to PCM.
// Output is standard .ogg that any transcription service can consume.

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let r = i;
    for (let j = 0; j < 8; j++) r = (r & 1) ? (r >>> 1) ^ 0xEDB88320 : r >>> 1;
    t[i] = r;
  }
  return t;
})();

function crc32ogg(buf) {
  let crc = 0;
  for (let i = 0; i < buf.length; i++) {
    crc = CRC_TABLE[(crc ^ buf[i]) & 0xFF] ^ (crc >>> 8);
  }
  return crc >>> 0;
}

function writeOggPage(serial, seq, granule, headerType, packets, stream) {
  const data = Buffer.concat(packets);
  const numSegments = Math.ceil(data.length / 255) || 1;
  const segTable = Buffer.alloc(numSegments);
  let remaining = data.length;
  for (let i = 0; i < numSegments; i++) {
    segTable[i] = Math.min(remaining, 255);
    remaining -= segTable[i];
  }

  const headerSize = 27 + numSegments;
  const page = Buffer.alloc(headerSize + data.length);
  page.write("OggS", 0, "ascii");
  page[4] = 0; // version
  page[5] = headerType;
  page.writeBigInt64LE(BigInt(granule), 6);
  page.writeUInt32LE(serial, 14);
  page.writeUInt32LE(seq, 18);
  page.writeUInt32LE(0, 22); // checksum placeholder
  page[26] = numSegments;
  segTable.copy(page, 27);
  data.copy(page, headerSize);

  const crc = crc32ogg(page);
  page.writeUInt32LE(crc, 22);
  stream.write(page);
}

function writeOpusOggHeader(serial, sampleRate, channels, stream) {
  // OpusHead
  const head = Buffer.alloc(19);
  head.write("OpusHead", 0, "ascii");
  head[8] = 1; // version
  head[9] = channels;
  head.writeUInt16LE(312, 10); // pre-skip
  head.writeUInt32LE(sampleRate, 12);
  head.writeInt16LE(0, 16); // output gain
  head[18] = 0; // channel map family
  writeOggPage(serial, 0, 0, 0x02, [head], stream);

  // OpusTags
  const vendorStr = "cs2-voice-bot";
  const tags = Buffer.alloc(16 + vendorStr.length);
  tags.write("OpusTags", 0, "ascii");
  tags.writeUInt32LE(vendorStr.length, 8);
  tags.write(vendorStr, 12, "ascii");
  tags.writeUInt32LE(0, 12 + vendorStr.length); // user comment list length
  writeOggPage(serial, 1, 0, 0x00, [tags], stream);
}

class OggOpusWriter {
  constructor(filePath, { sampleRate = 48000, channels = 2 } = {}) {
    this.serial = Math.floor(Math.random() * 0xFFFFFFFF);
    this.seq = 2; // 0 and 1 used by header pages
    this.granule = 0;
    this.stream = createWriteStream(filePath);
    writeOpusOggHeader(this.serial, sampleRate, channels, this.stream);
  }

  write(opusPacket) {
    // Each Opus frame at 48kHz is typically 20ms = 960 samples
    this.granule += 960;
    writeOggPage(this.serial, this.seq++, this.granule, 0x00, [opusPacket], this.stream);
  }

  end() {
    return new Promise((resolve, reject) => {
      // Write final page with EOS flag
      writeOggPage(this.serial, this.seq++, this.granule, 0x04, [Buffer.alloc(0)], this.stream);
      this.stream.once("finish", resolve);
      this.stream.once("error", reject);
      this.stream.end();
    });
  }
}

// ─── SessionRecorder ──────────────────────────────────────────────────────────

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
    const filePath = join(this.tmpDir, `audio_${steamId}.ogg`);

    const audioStream = receiver.subscribe(userId, {
      end: { behavior: EndBehaviorType.Manual },
    });

    const writer = new OggOpusWriter(filePath);

    audioStream.on("data", (packet) => {
      writer.write(packet);
    });

    this.receivers.set(userId, { filePath, audioStream, writer, steamId });
    console.log(`🎤 Started recording user ${userId} (steam: ${steamId}) → ${filePath}`);
  }

  async stop() {
    console.log(`⏹️ Stopping recording, receivers: ${this.receivers.size}`);
    const closePromises = [];

    for (const [userId, { filePath, audioStream, writer, steamId }] of this.receivers) {
      closePromises.push(
        (async () => {
          audioStream.destroy();
          await writer.end();
          console.log(`✅ Closed recording for ${userId}`);
          this.audioFiles.push({ discordId: userId, steamId, filePath });
        })()
      );
    }

    await Promise.all(closePromises);
    this.connection.destroy();
    return this.audioFiles;
  }
}
