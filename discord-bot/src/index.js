import "dotenv/config";
import { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } from "discord.js";
import { joinVoiceChannel, VoiceConnectionStatus, entersState, getVoiceConnection } from "@discordjs/voice";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { SessionRecorder } from "./recorder.js";
import { uploadSession } from "./uploader.js";
import { startGsiServer } from "./gsi.js";

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildVoiceStates,
    GatewayIntentBits.GuildMessages,
  ],
});

client.on("error", (err) => {
  console.error("Discord client error:", err.message);
});

client.ws.on('VOICE_SERVER_UPDATE', (data) => {
  console.log('🔊 VOICE_SERVER_UPDATE received for guild:', data.guild_id, 'endpoint:', data.endpoint);
});
client.ws.on('VOICE_STATE_UPDATE', (data) => {
  if (data.user_id === client.user?.id) {
    console.log('🎤 VOICE_STATE_UPDATE (self) received for guild:', data.guild_id, 'channel:', data.channel_id);
  }
});

const activeSessions = new Map();
const steamToDiscord = new Map();

// ─── Persist Steam→Discord links across restarts ──────────────────────────────

const LINKS_FILE = "/app/data/steam_links.json";

function loadLinks() {
  try {
    if (existsSync(LINKS_FILE)) {
      const data = JSON.parse(readFileSync(LINKS_FILE, "utf8"));
      for (const [steamId, discordId] of Object.entries(data)) {
        steamToDiscord.set(steamId, discordId);
      }
      console.log(`✅ Loaded ${steamToDiscord.size} Steam link(s)`);
    }
  } catch (err) {
    console.warn("Could not load links file:", err.message);
  }
}

function saveLinks() {
  try {
    const dir = "/app/data";
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    writeFileSync(LINKS_FILE, JSON.stringify(Object.fromEntries(steamToDiscord), null, 2));
  } catch (err) {
    console.warn("Could not save links file:", err.message);
  }
}

// ─── Shared helpers ───────────────────────────────────────────────────────────

async function startRecording({ guildId, voiceChannel, matchId, playerMap, startedAt }) {
  const connection = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId,
    adapterCreator: voiceChannel.guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: true,
  });

  try {
    await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
  } catch {
    console.warn("⚠️ Voice connection did not reach Ready state — continuing anyway");
  }

  const recorder = new SessionRecorder({ matchId, connection, voiceChannel, playerMap });
  recorder.start();

  activeSessions.set(guildId, { recorder, matchId, startedAt: startedAt ?? Date.now() });
  console.log(`🎙️ Recording started: match=${matchId}, channel=${voiceChannel.name}`);
}

async function cancelRecording(guildId) {
  const session = activeSessions.get(guildId);
  if (!session) return null;
  const { recorder, matchId } = session;
  for (const [, { audioStream, decoder, writeStream }] of recorder.receivers) {
    try { audioStream.unpipe(decoder); audioStream.destroy(); decoder.end(); writeStream.destroy(); } catch {}
  }
  try { recorder.connection.destroy(); } catch {}
  activeSessions.delete(guildId);
  console.log(`🗑️ Recording cancelled: match=${matchId}`);
  return { matchId };
}

async function stopRecording(guildId) {
  const session = activeSessions.get(guildId);
  if (!session) return null;
  const { recorder, matchId } = session;
  const audioFiles = await recorder.stop();
  activeSessions.delete(guildId);
  await uploadSession({ matchId, audioFiles, startedAt: session.startedAt });
  console.log(`✅ Upload complete: match=${matchId}, tracks=${audioFiles.length}`);
  return { matchId, audioFiles };
}

function findVoiceChannelForSteam(steamId) {
  const discordId = steamToDiscord.get(steamId);
  if (!discordId) return null;
  for (const guild of client.guilds.cache.values()) {
    for (const channel of guild.channels.cache.values()) {
      if (channel.isVoiceBased?.() && channel.members.has(discordId)) {
        return { guild, channel };
      }
    }
  }
  return null;
}

// ─── GSI auto-start / auto-stop ───────────────────────────────────────────────

startGsiServer({
  onMatchStart: async ({ matchId, steamId, startedAt }) => {
    if (!client.isReady()) return;
    const location = findVoiceChannelForSteam(steamId);
    if (!location) {
      console.log(`[GSI] ${steamId} not in any voice channel — skipping`);
      return;
    }
    const { guild, channel } = location;
    const guildId = guild.id;
    if (activeSessions.has(guildId)) {
      console.log(`[GSI] Already recording in guild ${guildId} — ignoring`);
      return;
    }
    const playerMap = { _source: "gsi" };
    for (const [discordId] of channel.members) {
      for (const [sid, did] of steamToDiscord) {
        if (did === discordId) playerMap[discordId] = sid;
      }
    }
    try {
      await startRecording({ guildId, voiceChannel: channel, matchId, playerMap, startedAt });
      guild.systemChannel?.send(`🎙️ Auto-recording started for match \`${matchId}\``);
    } catch (err) {
      console.error(`[GSI] Failed to auto-start:`, err.message);
    }
  },

  onMatchEnd: async ({ matchId, steamId }) => {
    if (!client.isReady()) return;
    const location = findVoiceChannelForSteam(steamId);
    if (!location) return;
    const guildId = location.guild.id;
    if (!activeSessions.has(guildId)) return;
    try {
      const result = await stopRecording(guildId);
      location.guild.systemChannel?.send(`✅ Match \`${result.matchId}\` recording saved. Transcription starting…`);
    } catch (err) {
      console.error(`[GSI] Failed to auto-stop:`, err.message);
    }
  },
});

// ─── Slash commands ───────────────────────────────────────────────────────────

const commands = [
  new SlashCommandBuilder()
    .setName("match")
    .setDescription("Control match recording")
    .addSubcommand((sub) =>
      sub
        .setName("start")
        .setDescription("Start recording a match")
        .addStringOption((opt) =>
          opt.setName("matchid").setDescription("Optional custom ID — leave blank to auto-generate").setRequired(false)
        )
        .addStringOption((opt) =>
          opt.setName("playermap").setDescription("steamid:discordid pairs, comma separated").setRequired(false)
        )
    )
    .addSubcommand((sub) => sub.setName("end").setDescription("Stop recording and upload the session"))
    .addSubcommand((sub) => sub.setName("cancel").setDescription("Stop recording and discard — nothing gets uploaded"))
    .addSubcommand((sub) => sub.setName("status").setDescription("Check current recording status")),

  new SlashCommandBuilder()
    .setName("link")
    .setDescription("Link your Discord account to your Steam ID")
    .addStringOption((opt) =>
      opt.setName("steamid").setDescription("Your 64-bit Steam ID (e.g. 76561198...)").setRequired(true)
    ),
];

// ─── Register commands ────────────────────────────────────────────────────────

client.once("ready", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);
  loadLinks();
  const rest = new REST().setToken(process.env.DISCORD_TOKEN);
  try {
    await rest.put(Routes.applicationCommands(client.user.id), {
      body: commands.map((c) => c.toJSON()),
    });
    console.log("✅ Slash commands registered");
  } catch (err) {
    console.error("Failed to register commands:", err.message);
  }
});

// ─── Interaction handler ──────────────────────────────────────────────────────

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  const age = Date.now() - interaction.createdTimestamp;
  if (age > 2500) {
    console.warn(`⚠️ Dropping stale interaction (${age}ms old) — already expired`);
    return;
  }

  console.log(`Interaction received: /${interaction.commandName} (age: ${age}ms)`);

  const { commandName, options, guildId, member } = interaction;

  const sub = commandName === "match" ? options.getSubcommand() : null;
  const ephemeral = commandName === "link" || sub === "cancel" || sub === "status";

  try {
    await interaction.deferReply({ flags: ephemeral ? 64 : 0 });
  } catch (err) {
    console.error(`Failed to defer interaction (age: ${Date.now() - interaction.createdTimestamp}ms):`, err.message);
    return;
  }

  try {
    // ── /link ─────────────────────────────────────────────────────────────────
    if (commandName === "link") {
      const steamId = options.getString("steamid");
      if (!/^7656119\d{10}$/.test(steamId)) {
        return interaction.editReply("❌ Invalid Steam ID format. Must be a 17-digit SteamID64.");
      }
      steamToDiscord.set(steamId, interaction.user.id);
      saveLinks();
      console.log(`Link: Discord ${interaction.user.id} → Steam ${steamId}`);
      return interaction.editReply(
        `✅ Linked your Discord account to Steam ID \`${steamId}\`.\nGSI auto-recording will now work for your matches.`
      );
    }

    if (commandName === "match") {

      // ── /match start ────────────────────────────────────────────────────────
      if (sub === "start") {
        if (activeSessions.has(guildId)) {
          return interaction.editReply("⚠️ A recording is already active. Use `/match end` first.");
        }
        const voiceChannel = member.voice?.channel;
        if (!voiceChannel) {
          return interaction.editReply("❌ You must be in a voice channel to start recording.");
        }

        const rawMatchId = options.getString("matchid");
        const matchId = rawMatchId || `manual-${Date.now()}`;
        const playerMapRaw = options.getString("playermap") || "";

        const playerMap = { _source: "manual" };
        for (const pair of playerMapRaw.split(",").filter(Boolean)) {
          const [sid, did] = pair.trim().split(":");
          if (sid && did) playerMap[did] = sid;
        }
        for (const [discordId] of voiceChannel.members) {
          for (const [sid, did] of steamToDiscord) {
            if (did === discordId && !playerMap[discordId]) playerMap[discordId] = sid;
          }
        }

        await startRecording({ guildId, voiceChannel, matchId, playerMap });
        return interaction.editReply(
          `🎙️ Recording started for match \`${matchId}\`\n` +
          `Channel: **${voiceChannel.name}** | Players present: ${voiceChannel.members.size}\n` +
          `Use \`/match end\` when the game is over.`
        );
      }

      // ── /match end ──────────────────────────────────────────────────────────
      else if (sub === "end") {
        if (!activeSessions.has(guildId)) {
          return interaction.editReply("❌ No active recording in this server.");
        }
        const result = await stopRecording(guildId);
        return interaction.editReply(
          `✅ Session uploaded for match \`${result.matchId}\`\n` +
          `${result.audioFiles.length} player tracks uploaded. Transcription will begin shortly.`
        );
      }

      // ── /match cancel ────────────────────────────────────────────────────────
      else if (sub === "cancel") {
        const orphan = getVoiceConnection(guildId);
        if (orphan) {
          orphan.destroy();
          activeSessions.delete(guildId);
          return interaction.editReply("🗑️ Bot disconnected.");
        }
        if (!activeSessions.has(guildId)) {
          return interaction.editReply("❌ No active recording in this server.");
        }
        await cancelRecording(guildId);
        return interaction.editReply("🗑️ Recording cancelled — nothing was saved.");
      }

      // ── /match status ────────────────────────────────────────────────────────
      else if (sub === "status") {
        const session = activeSessions.get(guildId);
        if (!session) {
          return interaction.editReply("📭 No active recording.");
        }
        const elapsed = Math.floor((Date.now() - session.startedAt) / 1000);
        const mins = Math.floor(elapsed / 60);
        const secs = elapsed % 60;
        return interaction.editReply(
          `🎙️ Recording active for match \`${session.matchId}\`\nElapsed: **${mins}m ${secs}s**`
        );
      }
    }
  } catch (err) {
    console.error("Interaction error:", err.message);
    try { await interaction.editReply("❌ Something went wrong: " + err.message); } catch {}
  }
});

loadLinks();
client.login(process.env.DISCORD_TOKEN);
