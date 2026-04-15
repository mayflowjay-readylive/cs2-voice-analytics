import "dotenv/config";
import { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } from "discord.js";
import { joinVoiceChannel, VoiceConnectionStatus, entersState, getVoiceConnection } from "@discordjs/voice";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { SessionRecorder } from "./recorder.js";
import { uploadSession } from "./uploader.js";
import { startGsiServer } from "./gsi.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

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

const activeSessions = new Map();
const pendingConnections = new Map();
const steamToDiscord = new Map();

// ─── GSI event queue ─────────────────────────────────────────────────────────

const pendingGsiEvents = [];
let clientReady = false;

function queueOrRun(handler, event) {
  if (clientReady) {
    handler(event);
  } else {
    pendingGsiEvents.push({ handler, event });
    console.log(`[GSI] Client not ready — queued ${event.type} event for ${event.steamId} (${pendingGsiEvents.length} in queue)`);
  }
}

function replayQueuedEvents() {
  if (pendingGsiEvents.length === 0) return;
  console.log(`[GSI] Replaying ${pendingGsiEvents.length} queued event(s)...`);
  const events = [...pendingGsiEvents];
  pendingGsiEvents.length = 0;
  for (const { handler, event } of events) {
    handler(event);
  }
}

// ─── Guild-level cooldown ────────────────────────────────────────────────────

const guildCooldowns = new Map();
const GUILD_COOLDOWN_MS = 120_000;

function isGuildInCooldown(guildId) {
  const lastStop = guildCooldowns.get(guildId);
  if (!lastStop) return false;
  const elapsed = Date.now() - lastStop;
  if (elapsed < GUILD_COOLDOWN_MS) {
    return elapsed;
  }
  guildCooldowns.delete(guildId);
  return false;
}

// ─── Persist Steam→Discord links ─────────────────────────────────────────────
// Store in discord-bot/data/ so it survives pm2 restarts on GCP.

const LINKS_DIR  = join(__dirname, "..", "data");
const LINKS_FILE = join(LINKS_DIR, "steam_links.json");

function loadLinks() {
  try {
    if (existsSync(LINKS_FILE)) {
      const data = JSON.parse(readFileSync(LINKS_FILE, "utf8"));
      for (const [steamId, discordId] of Object.entries(data)) {
        steamToDiscord.set(steamId, discordId);
      }
      console.log(`✅ Loaded ${steamToDiscord.size} Steam link(s) from ${LINKS_FILE}`);
    }
  } catch (err) {
    console.warn("Could not load links file:", err.message);
  }
}

function saveLinks() {
  try {
    if (!existsSync(LINKS_DIR)) mkdirSync(LINKS_DIR, { recursive: true });
    writeFileSync(LINKS_FILE, JSON.stringify(Object.fromEntries(steamToDiscord), null, 2));
  } catch (err) {
    console.warn("Could not save links file:", err.message);
  }
}

// ─── Voice API helpers ────────────────────────────────────────────────────────

const VOICE_API_URL = process.env.VOICE_API_URL || "https://voice-api-production-d2f7.up.railway.app";

async function isBotEnabled() {
  try {
    const resp = await fetch(`${VOICE_API_URL}/bot/enabled`);
    if (!resp.ok) return true;
    const data = await resp.json();
    return data.enabled !== false;
  } catch {
    return true;
  }
}

async function getExcludedDiscordIds() {
  try {
    const resp = await fetch(`${VOICE_API_URL}/bot/excluded-players`);
    if (!resp.ok) return new Set();
    const data = await resp.json();
    const players = data.players || [];
    return new Set(players.map(p => p.discordId).filter(Boolean));
  } catch {
    return new Set();
  }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

function buildPlayerMap(voiceChannel, source) {
  const playerMap = { _source: source };
  for (const [discordId] of voiceChannel.members) {
    for (const [sid, did] of steamToDiscord) {
      if (did === discordId && !playerMap[discordId]) playerMap[discordId] = sid;
    }
  }
  return playerMap;
}

async function preconnectVoice({ guildId, voiceChannel }) {
  const connection = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId,
    adapterCreator: voiceChannel.guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: true,
  });

  try {
    await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
    console.log(`✅ Voice connection ready (pre-connected during warmup)`);
  } catch {
    connection.destroy();
    throw new Error("Voice connection failed — could not reach Ready state within 30s. Check bot permissions in this channel.");
  }

  return connection;
}

async function startRecording({ guildId, voiceChannel, matchId, playerMap, startedAt, existingConnection, excludedDiscordIds }) {
  let connection = existingConnection;

  if (!connection) {
    connection = joinVoiceChannel({
      channelId: voiceChannel.id,
      guildId,
      adapterCreator: voiceChannel.guild.voiceAdapterCreator,
      selfDeaf: false,
      selfMute: true,
    });

    try {
      await entersState(connection, VoiceConnectionStatus.Ready, 30_000);
      console.log(`✅ Voice connection ready`);
    } catch {
      connection.destroy();
      throw new Error("Voice connection failed — could not reach Ready state within 30s. Check bot permissions in this channel.");
    }
  }

  const recorder = new SessionRecorder({ matchId, connection, voiceChannel, playerMap, excludedDiscordIds, steamToDiscord });
  recorder.start();

  activeSessions.set(guildId, { recorder, matchId, startedAt: startedAt ?? Date.now() });
  console.log(`🎙️ Recording started: match=${matchId}, channel=${voiceChannel.name}`);
}

async function stopRecording(guildId) {
  const session = activeSessions.get(guildId);
  if (!session) return null;
  const { recorder, matchId } = session;
  const audioFiles = await recorder.stop();
  activeSessions.delete(guildId);

  guildCooldowns.set(guildId, Date.now());
  console.log(`🛡️ Guild cooldown set for ${GUILD_COOLDOWN_MS / 1000}s`);

  await uploadSession({ matchId, audioFiles, startedAt: session.startedAt });
  console.log(`✅ Upload complete: match=${matchId}, tracks=${audioFiles.length}`);
  return { matchId, audioFiles };
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

// ─── GSI handlers ────────────────────────────────────────────────────────────

async function handleWarmup({ matchId, steamId }) {
  if (!(await isBotEnabled())) { console.log(`[GSI] Warmup: Bot disabled — skipping`); return; }
  const location = findVoiceChannelForSteam(steamId);
  if (!location) { console.log(`[GSI] Warmup: ${steamId} not in any voice channel — skipping`); return; }
  const { guild, channel } = location;
  const guildId = guild.id;

  const cooldownElapsed = isGuildInCooldown(guildId);
  if (cooldownElapsed !== false) {
    console.log(`[GSI] Warmup: Ignoring — guild cooldown active (${Math.round(cooldownElapsed / 1000)}s since last stop)`);
    return;
  }

  if (activeSessions.has(guildId)) { console.log(`[GSI] Warmup: Already recording — ignoring`); return; }
  if (pendingConnections.has(guildId)) { console.log(`[GSI] Warmup: Already pre-connected — ignoring`); return; }

  try {
    console.log(`[GSI] Warmup: Pre-connecting to ${channel.name}...`);
    const connection = await preconnectVoice({ guildId, voiceChannel: channel });
    const playerMap = buildPlayerMap(channel, "gsi");
    pendingConnections.set(guildId, { connection, voiceChannel: channel, playerMap });
    console.log(`[GSI] Warmup: Pre-connected to ${channel.name} — DAVE handshake complete, ready for match start`);
  } catch (err) {
    console.error(`[GSI] Warmup: Failed to pre-connect:`, err.message);
  }
}

async function handleMatchStart({ matchId, steamId, startedAt }) {
  if (!(await isBotEnabled())) { console.log(`[GSI] Live: Bot disabled — skipping`); return; }
  const location = findVoiceChannelForSteam(steamId);
  if (!location) { console.log(`[GSI] ${steamId} not in any voice channel — skipping`); return; }
  const { guild, channel } = location;
  const guildId = guild.id;

  const cooldownElapsed = isGuildInCooldown(guildId);
  if (cooldownElapsed !== false) {
    console.log(`[GSI] Live: Ignoring — guild cooldown active (${Math.round(cooldownElapsed / 1000)}s since last stop)`);
    if (pendingConnections.has(guildId)) {
      const pending = pendingConnections.get(guildId);
      pendingConnections.delete(guildId);
      try { pending.connection.destroy(); } catch {}
      console.log(`[GSI] Live: Cleaned up ghost pre-connection`);
    }
    return;
  }

  if (activeSessions.has(guildId)) { console.log(`[GSI] Already recording — ignoring`); return; }

  const pending = pendingConnections.get(guildId);
  pendingConnections.delete(guildId);

  let existingConnection = null;
  let playerMap;

  if (pending) {
    if (pending.connection.state.status === "ready") {
      console.log(`[GSI] Using pre-connected voice channel from warmup (connection still ready)`);
      existingConnection = pending.connection;
    } else {
      console.log(`[GSI] Pre-connected voice was ${pending.connection.state.status} — reconnecting`);
      try { pending.connection.destroy(); } catch {}
      existingConnection = null;
    }
    playerMap = buildPlayerMap(channel, "gsi");
  } else {
    console.log(`[GSI] No warmup pre-connection — connecting now`);
    playerMap = buildPlayerMap(channel, "gsi");
  }

  const excludedDiscordIds = await getExcludedDiscordIds();
  if (excludedDiscordIds.size > 0) {
    console.log(`[GSI] Excluding ${excludedDiscordIds.size} player(s) from recording`);
  }

  try {
    await startRecording({ guildId, voiceChannel: channel, matchId, playerMap, startedAt, existingConnection, excludedDiscordIds });
  } catch (err) {
    console.error(`[GSI] Failed to auto-start:`, err.message);
  }
}

async function handleMatchEnd({ matchId, steamId }) {
  const location = findVoiceChannelForSteam(steamId);
  if (!location) return;
  const guildId = location.guild.id;

  if (pendingConnections.has(guildId)) {
    const pending = pendingConnections.get(guildId);
    pendingConnections.delete(guildId);
    try { pending.connection.destroy(); } catch {}
    guildCooldowns.set(guildId, Date.now());
    console.log(`[GSI] Cleaned up pre-connection (match ended without going live) — cooldown set`);
    return;
  }

  if (!activeSessions.has(guildId)) return;
  try {
    await stopRecording(guildId);
  } catch (err) {
    console.error(`[GSI] Failed to auto-stop:`, err.message);
    guildCooldowns.set(guildId, Date.now());
  }
}

// ─── GSI server ──────────────────────────────────────────────────────────────

startGsiServer({
  onWarmup: (event) => queueOrRun(handleWarmup, { ...event, type: "warmup" }),
  onMatchStart: (event) => queueOrRun(handleMatchStart, { ...event, type: "matchStart" }),
  onMatchEnd: (event) => queueOrRun(handleMatchEnd, { ...event, type: "matchEnd" }),
});

// ─── Slash commands ───────────────────────────────────────────────────────────

const commands = [
  new SlashCommandBuilder()
    .setName("match")
    .setDescription("Control match recording")
    .addSubcommand((sub) =>
      sub.setName("start").setDescription("Start recording a match")
        .addStringOption((opt) => opt.setName("matchid").setDescription("Optional custom ID").setRequired(false))
        .addStringOption((opt) => opt.setName("playermap").setDescription("steamid:discordid pairs, comma separated").setRequired(false))
    )
    .addSubcommand((sub) => sub.setName("end").setDescription("Stop recording and upload"))
    .addSubcommand((sub) => sub.setName("cancel").setDescription("Stop and discard recording"))
    .addSubcommand((sub) => sub.setName("status").setDescription("Check recording status")),
  new SlashCommandBuilder()
    .setName("link")
    .setDescription("Link your Discord account to your Steam ID")
    .addStringOption((opt) => opt.setName("steamid").setDescription("Your 64-bit Steam ID").setRequired(true)),
];

// Use clientReady event (ready is deprecated in discord.js v15+)
client.once("clientReady", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);
  loadLinks();
  const rest = new REST().setToken(process.env.DISCORD_TOKEN);
  try {
    await rest.put(Routes.applicationCommands(client.user.id), { body: commands.map((c) => c.toJSON()) });
    console.log("✅ Slash commands registered");
  } catch (err) {
    console.error("Failed to register commands:", err.message);
  }

  clientReady = true;
  replayQueuedEvents();
});

// ─── Interactions ─────────────────────────────────────────────────────────────

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  const age = Date.now() - interaction.createdTimestamp;
  if (age > 2500) { console.warn(`⚠️ Dropping stale interaction (${age}ms)`); return; }

  console.log(`Interaction received: /${interaction.commandName} (age: ${age}ms)`);

  const { commandName, options, guildId, member } = interaction;
  const sub = commandName === "match" ? options.getSubcommand() : null;
  const ephemeral = commandName === "link" || sub === "cancel" || sub === "status";

  try {
    await interaction.deferReply({ flags: ephemeral ? 64 : 0 });
  } catch (err) {
    console.error(`Failed to defer interaction:`, err.message);
    return;
  }

  try {
    if (commandName === "link") {
      const steamId = options.getString("steamid");
      if (!/^7656119\d{10}$/.test(steamId)) {
        return interaction.editReply("❌ Invalid Steam ID format. Must be a 17-digit SteamID64.");
      }
      steamToDiscord.set(steamId, interaction.user.id);
      saveLinks();
      return interaction.editReply(`✅ Linked your Discord account to Steam ID \`${steamId}\`.`);
    }

    if (commandName === "match") {
      if (sub === "start") {
        if (activeSessions.has(guildId)) return interaction.editReply("⚠️ Already recording. Use `/match end` first.");
        const voiceChannel = member.voice?.channel;
        if (!voiceChannel) return interaction.editReply("❌ You must be in a voice channel to start recording.");

        if (pendingConnections.has(guildId)) {
          const pending = pendingConnections.get(guildId);
          pendingConnections.delete(guildId);
          try { pending.connection.destroy(); } catch {}
        }

        guildCooldowns.delete(guildId);

        const matchId = options.getString("matchid") || `manual-${Date.now()}`;
        const playerMap = buildPlayerMap(voiceChannel, "manual");
        const playerMapRaw = options.getString("playermap") || "";
        for (const pair of playerMapRaw.split(",").filter(Boolean)) {
          const [sid, did] = pair.trim().split(":");
          if (sid && did) playerMap[did] = sid;
        }

        const excludedDiscordIds = await getExcludedDiscordIds();
        if (excludedDiscordIds.size > 0) {
          console.log(`Excluding ${excludedDiscordIds.size} player(s) from recording`);
        }

        await startRecording({ guildId, voiceChannel, matchId, playerMap, excludedDiscordIds });

        const humanPlayerCount = voiceChannel.members.size - 1;
        return interaction.editReply(
          `🎙️ Recording started for match \`${matchId}\`\n` +
          `Channel: **${voiceChannel.name}** | Players: ${humanPlayerCount}\n` +
          `Use \`/match end\` when done.`
        );
      }

      else if (sub === "end") {
        if (!activeSessions.has(guildId)) return interaction.editReply("❌ No active recording.");
        const result = await stopRecording(guildId);
        return interaction.editReply(
          `✅ Session uploaded for match \`${result.matchId}\`\n` +
          `${result.audioFiles.length} player tracks uploaded. Transcription will begin shortly.`
        );
      }

      else if (sub === "cancel") {
        if (pendingConnections.has(guildId)) {
          const pending = pendingConnections.get(guildId);
          pendingConnections.delete(guildId);
          try { pending.connection.destroy(); } catch {}
          return interaction.editReply("🗑️ Bot disconnected (was pre-connected for warmup).");
        }
        const orphan = getVoiceConnection(guildId);
        if (orphan) { orphan.destroy(); activeSessions.delete(guildId); return interaction.editReply("🗑️ Bot disconnected."); }
        if (!activeSessions.has(guildId)) return interaction.editReply("❌ No active recording.");
        await cancelRecording(guildId);
        return interaction.editReply("🗑️ Recording cancelled — nothing was saved.");
      }

      else if (sub === "status") {
        if (pendingConnections.has(guildId)) {
          return interaction.editReply("⏳ Bot is pre-connected and waiting for match to go live (warmup phase).");
        }
        const session = activeSessions.get(guildId);
        if (!session) {
          const cooldownElapsed = isGuildInCooldown(guildId);
          if (cooldownElapsed !== false) {
            return interaction.editReply(`📭 No active recording. Guild cooldown active for ${Math.round((GUILD_COOLDOWN_MS - cooldownElapsed) / 1000)}s (prevents ghost recordings after match end).`);
          }
          return interaction.editReply("📭 No active recording.");
        }
        const elapsed = Math.floor((Date.now() - session.startedAt) / 1000);
        return interaction.editReply(`🎙️ Recording active for match \`${session.matchId}\`\nElapsed: **${Math.floor(elapsed/60)}m ${elapsed%60}s**`);
      }
    }
  } catch (err) {
    console.error("Interaction error:", err.message);
    try { await interaction.editReply("❌ Something went wrong: " + err.message); } catch {}
  }
});

loadLinks();
client.login(process.env.DISCORD_TOKEN);
