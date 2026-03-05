import "dotenv/config";
import { Client, GatewayIntentBits, REST, Routes, SlashCommandBuilder } from "discord.js";
import { joinVoiceChannel, VoiceConnectionStatus, entersState } from "@discordjs/voice";
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

// Active recording sessions keyed by guildId
const activeSessions = new Map();

// steamId → discordId mappings (populated via /link command)
// In production, persist this to a DB
const steamToDiscord = new Map();

// ─── Shared helpers ───────────────────────────────────────────────────────────

async function startRecording({ guildId, voiceChannel, matchId, playerMap, startedAt }) {
  const connection = joinVoiceChannel({
    channelId: voiceChannel.id,
    guildId,
    adapterCreator: voiceChannel.guild.voiceAdapterCreator,
    selfDeaf: false,
    selfMute: true,
  });

  await entersState(connection, VoiceConnectionStatus.Ready, 30_000);

  const recorder = new SessionRecorder({ matchId, connection, voiceChannel, playerMap });
  recorder.start();

  activeSessions.set(guildId, { recorder, matchId, startedAt: startedAt ?? Date.now() });
  console.log(`🎙️ Recording started: match=${matchId}, channel=${voiceChannel.name}, source=${playerMap._source ?? "manual"}`);
}

async function cancelRecording(guildId) {
  const session = activeSessions.get(guildId);
  if (!session) return null;

  const { recorder, matchId } = session;
  // Stop streams and destroy connection — but don't upload anything
  for (const [, { audioStream, decoder, writeStream }] of recorder.receivers) {
    audioStream.unpipe(decoder);
    audioStream.destroy();
    decoder.end();
    writeStream.destroy();
  }
  recorder.connection.destroy();
  activeSessions.delete(guildId);

  console.log(`🗑️ Recording cancelled and discarded: match=${matchId}`);
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

// Find the guild + voice channel that contains a given Steam ID
// Used by GSI auto-start to know where to join
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
    if (!client.isReady()) {
      console.log(`[GSI] Bot not ready yet — ignoring match start for ${matchId}`);
      return;
    }

    const location = findVoiceChannelForSteam(steamId);
    if (!location) {
      console.log(`[GSI] Match ${matchId} live but ${steamId} not found in any voice channel — skipping auto-start`);
      return;
    }

    const { guild, channel } = location;
    const guildId = guild.id;

    if (activeSessions.has(guildId)) {
      console.log(`[GSI] Already recording in guild ${guildId} — ignoring GSI start`);
      return;
    }

    // Build playerMap from all linked members currently in the channel
    const playerMap = { _source: "gsi" };
    for (const [discordId] of channel.members) {
      for (const [sid, did] of steamToDiscord) {
        if (did === discordId) playerMap[discordId] = sid;
      }
    }

    try {
      await startRecording({ guildId, voiceChannel: channel, matchId, playerMap, startedAt });

      // Notify the guild's system channel if available
      const systemChannel = guild.systemChannel;
      systemChannel?.send(`🎙️ Auto-recording started for match \`${matchId}\` (triggered by GSI)`);
    } catch (err) {
      console.error(`[GSI] Failed to auto-start recording:`, err);
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
      const systemChannel = location.guild.systemChannel;
      systemChannel?.send(`✅ Match \`${result.matchId}\` recording saved. Transcription starting…`);
    } catch (err) {
      console.error(`[GSI] Failed to auto-stop recording:`, err);
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
          opt
            .setName("matchid")
            .setDescription("Optional custom ID — leave blank to auto-generate")
            .setRequired(false)
        )
        .addStringOption((opt) =>
          opt
            .setName("playermap")
            .setDescription(
              "steamid:discordid pairs, comma separated. e.g. 76561198...:123456..."
            )
            .setRequired(false)
        )
    )
    .addSubcommand((sub) =>
      sub
        .setName("end")
        .setDescription("Stop recording and upload the session")
    )
    .addSubcommand((sub) =>
      sub
        .setName("cancel")
        .setDescription("Stop recording and discard — nothing gets uploaded or analysed")
    )
    .addSubcommand((sub) =>
      sub
        .setName("status")
        .setDescription("Check current recording status")
    ),

  new SlashCommandBuilder()
    .setName("link")
    .setDescription("Link your Discord account to your Steam ID")
    .addStringOption((opt) =>
      opt
        .setName("steamid")
        .setDescription("Your 64-bit Steam ID (e.g. 76561198...)")
        .setRequired(true)
    ),
];

// ─── Register commands ────────────────────────────────────────────────────────

client.once("ready", async () => {
  console.log(`✅ Logged in as ${client.user.tag}`);

  const rest = new REST().setToken(process.env.DISCORD_TOKEN);
  try {
    await rest.put(Routes.applicationCommands(client.user.id), {
      body: commands.map((c) => c.toJSON()),
    });
    console.log("✅ Slash commands registered");
  } catch (err) {
    console.error("Failed to register commands:", err);
  }
});

// ─── Interaction handler ──────────────────────────────────────────────────────

client.on("interactionCreate", async (interaction) => {
  if (!interaction.isChatInputCommand()) return;

  const { commandName, options, guildId, member } = interaction;

  // /link steamid:xxx
  if (commandName === "link") {
    const steamId = options.getString("steamid");
    if (!/^7656119\d{10}$/.test(steamId)) {
      return interaction.reply({ content: "❌ Invalid Steam ID format.", ephemeral: true });
    }
    steamToDiscord.set(steamId, interaction.user.id);
    console.log(`Link: Discord ${interaction.user.id} → Steam ${steamId}`);
    return interaction.reply({
      content: `✅ Linked your Discord account to Steam ID \`${steamId}\`.\nGSI auto-recording will now work for your matches.`,
      ephemeral: true,
    });
  }

  if (commandName === "match") {
    const sub = options.getSubcommand();

    // ── /match start ──────────────────────────────────────────────────────────
    if (sub === "start") {
      if (activeSessions.has(guildId)) {
        return interaction.reply({ content: "⚠️ A recording is already active. Use `/match end` first.", ephemeral: true });
      }

      const voiceChannel = member.voice?.channel;
      if (!voiceChannel) {
        return interaction.reply({ content: "❌ You must be in a voice channel to start recording.", ephemeral: true });
      }

      const rawMatchId = options.getString("matchid");
      const matchId = rawMatchId || `manual-${Date.now()}`;
      const playerMapRaw = options.getString("playermap") || "";

      // Parse optional steamid:discordid map, then merge with any /link mappings
      const playerMap = { _source: "manual" };
      for (const pair of playerMapRaw.split(",").filter(Boolean)) {
        const [steamId, discordId] = pair.trim().split(":");
        if (steamId && discordId) playerMap[discordId] = steamId;
      }
      // Also apply any previously /link-ed members in the channel
      for (const [discordId] of voiceChannel.members) {
        for (const [sid, did] of steamToDiscord) {
          if (did === discordId && !playerMap[discordId]) playerMap[discordId] = sid;
        }
      }

      await interaction.deferReply();

      try {
        await startRecording({ guildId, voiceChannel, matchId, playerMap });
        await interaction.editReply(
          `🎙️ Recording started for match \`${matchId}\`\n` +
          `Channel: **${voiceChannel.name}** | Players present: ${voiceChannel.members.size}\n` +
          `Use \`/match end\` when the game is over.`
        );
      } catch (err) {
        console.error("Failed to start recording:", err);
        await interaction.editReply("❌ Failed to join voice channel: " + err.message);
      }
    }

    // ── /match end ────────────────────────────────────────────────────────────
    else if (sub === "end") {
      if (!activeSessions.has(guildId)) {
        return interaction.reply({ content: "❌ No active recording in this server.", ephemeral: true });
      }

      await interaction.deferReply();

      try {
        const result = await stopRecording(guildId);
        await interaction.editReply(
          `✅ Session uploaded for match \`${result.matchId}\`\n` +
          `${result.audioFiles.length} player tracks uploaded. Transcription will begin shortly.`
        );
      } catch (err) {
        console.error("Failed to stop/upload:", err);
        await interaction.editReply("❌ Error during upload: " + err.message);
      }
    }

    // ── /match cancel ─────────────────────────────────────────────────────────
    else if (sub === "cancel") {
      // Even if there's no active session, try to disconnect any orphaned voice connection
      const { getVoiceConnection } = await import("@discordjs/voice");
      const orphan = getVoiceConnection(guildId);
      if (orphan) {
        orphan.destroy();
        return interaction.reply({ content: `🗑️ Bot disconnected.`, ephemeral: true });
      }
      if (!activeSessions.has(guildId)) {
        return interaction.reply({ content: "❌ No active recording in this server.", ephemeral: true });
      }
      await cancelRecording(guildId);
      interaction.reply({ content: `🗑️ Recording cancelled — nothing was saved.`, ephemeral: true });
    }

    // ── /match status ─────────────────────────────────────────────────────────
    else if (sub === "status") {
      const session = activeSessions.get(guildId);
      if (!session) {
        return interaction.reply({ content: "📭 No active recording.", ephemeral: true });
      }
      const elapsed = Math.floor((Date.now() - session.startedAt) / 1000);
      const mins = Math.floor(elapsed / 60);
      const secs = elapsed % 60;
      interaction.reply({
        content: `🎙️ Recording active for match \`${session.matchId}\`\nElapsed: **${mins}m ${secs}s**\nStarted via: ${activeSessions.get(guildId).recorder ? "manual or GSI" : "unknown"}`,
        ephemeral: true,
      });
    }
  }
});

client.login(process.env.DISCORD_TOKEN);
