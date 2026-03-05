require("dotenv").config();
const Eris = require("eris");
const { readFileSync, writeFileSync, existsSync, mkdirSync } = require("fs");
const { SessionRecorder } = require("./recorder.js");
const { uploadSession } = require("./uploader.js");
const { startGsiServer } = require("./gsi.js");

const bot = new Eris(process.env.DISCORD_TOKEN, {
  intents: ["guilds", "guildVoiceStates"],
  restMode: true,
});

const activeSessions = new Map();
const steamToDiscord = new Map();

// ─── Persist Steam→Discord links ─────────────────────────────────────────────

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

// ─── Helpers ──────────────────────────────────────────────────────────────────

async function startRecording({ guildId, voiceChannel, matchId, playerMap, startedAt }) {
  const connection = await bot.joinVoiceChannel(voiceChannel.id, {
    selfDeaf: false,
    selfMute: true,
  });

  console.log(`🔌 Voice connection state: ${connection.ready ? "ready" : "connecting..."}`);

  if (!connection.ready) {
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Voice connection timeout")), 30_000);
      connection.once("ready", () => { clearTimeout(timeout); resolve(); });
      connection.once("error", (err) => { clearTimeout(timeout); reject(err); });
    }).catch((err) => {
      console.warn(`⚠️ Voice connection did not reach ready state: ${err.message} — continuing anyway`);
    });
  }

  connection.receive("opus");

  const recorder = new SessionRecorder({ matchId, connection, voiceChannel, playerMap });
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
  await uploadSession({ matchId, audioFiles, startedAt: session.startedAt });
  console.log(`✅ Upload complete: match=${matchId}, tracks=${audioFiles.length}`);
  return { matchId, audioFiles };
}

async function cancelRecording(guildId) {
  const session = activeSessions.get(guildId);
  if (!session) return null;
  const { recorder, matchId } = session;
  try { recorder.connection.disconnect(); } catch {}
  activeSessions.delete(guildId);
  console.log(`🗑️ Recording cancelled: match=${matchId}`);
  return { matchId };
}

function findVoiceChannelForSteam(steamId) {
  const discordId = steamToDiscord.get(steamId);
  if (!discordId) return null;
  for (const guild of bot.guilds.values()) {
    for (const channel of guild.channels.values()) {
      if (channel.type === 2 && channel.voiceMembers?.has(discordId)) {
        return { guild, channel };
      }
    }
  }
  return null;
}

// ─── GSI ──────────────────────────────────────────────────────────────────────

startGsiServer({
  onMatchStart: async ({ matchId, steamId, startedAt }) => {
    if (!bot.ready) return;
    const location = findVoiceChannelForSteam(steamId);
    if (!location) { console.log(`[GSI] ${steamId} not in any voice channel — skipping`); return; }
    const { guild, channel } = location;
    const guildId = guild.id;
    if (activeSessions.has(guildId)) { console.log(`[GSI] Already recording — ignoring`); return; }
    const playerMap = { _source: "gsi" };
    for (const [did] of channel.voiceMembers) {
      for (const [sid, d] of steamToDiscord) {
        if (d === did) playerMap[did] = sid;
      }
    }
    try {
      await startRecording({ guildId, voiceChannel: channel, matchId, playerMap, startedAt });
    } catch (err) {
      console.error(`[GSI] Failed to auto-start:`, err.message);
    }
  },
  onMatchEnd: async ({ matchId, steamId }) => {
    if (!bot.ready) return;
    const location = findVoiceChannelForSteam(steamId);
    if (!location) return;
    const guildId = location.guild.id;
    if (!activeSessions.has(guildId)) return;
    try { await stopRecording(guildId); } catch (err) { console.error(`[GSI] Failed to auto-stop:`, err.message); }
  },
});

// ─── Slash commands ───────────────────────────────────────────────────────────

bot.on("ready", async () => {
  console.log(`✅ Logged in as ${bot.user.username}#${bot.user.discriminator}`);
  loadLinks();

  const commands = [
    {
      name: "match",
      description: "Control match recording",
      options: [
        {
          type: 1, name: "start", description: "Start recording a match",
          options: [
            { type: 3, name: "matchid", description: "Optional custom ID", required: false },
            { type: 3, name: "playermap", description: "steamid:discordid pairs, comma separated", required: false },
          ],
        },
        { type: 1, name: "end", description: "Stop recording and upload" },
        { type: 1, name: "cancel", description: "Stop and discard recording" },
        { type: 1, name: "status", description: "Check recording status" },
      ],
    },
    {
      name: "link",
      description: "Link your Discord account to your Steam ID",
      options: [
        { type: 3, name: "steamid", description: "Your 64-bit Steam ID", required: true },
      ],
    },
  ];

  try {
    await bot.bulkEditCommands(commands);
    console.log("✅ Slash commands registered");
  } catch (err) {
    console.error("Failed to register commands:", err.message);
  }
});

// ─── Interactions ─────────────────────────────────────────────────────────────

bot.on("interactionCreate", async (interaction) => {
  if (interaction.type !== 2) return;

  const age = Date.now() - interaction.createdAt;
  if (age > 2500) {
    console.warn(`⚠️ Dropping stale interaction (${age}ms)`);
    return;
  }

  console.log(`Interaction received: /${interaction.data.name} (age: ${age}ms)`);

  const commandName = interaction.data.name;
  const sub = interaction.data.options?.[0]?.name;
  const getOpt = (name) => interaction.data.options?.[0]?.options?.find(o => o.name === name)?.value
    ?? interaction.data.options?.find(o => o.name === name)?.value;

  const ephemeral = commandName === "link" || sub === "cancel" || sub === "status";

  try {
    await interaction.defer(ephemeral ? 64 : 0);
  } catch (err) {
    console.error(`Failed to defer interaction:`, err.message);
    return;
  }

  const guildId = interaction.guildID;

  try {
    if (commandName === "link") {
      const steamId = getOpt("steamid");
      if (!/^7656119\d{10}$/.test(steamId)) {
        return interaction.editOriginalMessage("❌ Invalid Steam ID format. Must be a 17-digit SteamID64.");
      }
      steamToDiscord.set(steamId, interaction.member.id);
      saveLinks();
      console.log(`Link: Discord ${interaction.member.id} → Steam ${steamId}`);
      return interaction.editOriginalMessage(`✅ Linked your Discord account to Steam ID \`${steamId}\`.`);
    }

    if (commandName === "match") {
      if (sub === "start") {
        if (activeSessions.has(guildId)) {
          return interaction.editOriginalMessage("⚠️ A recording is already active. Use `/match end` first.");
        }
        const member = interaction.member;
        const guild = bot.guilds.get(guildId);
        const voiceChannelId = guild?.members?.get(member.id)?.voiceState?.channelID;
        if (!voiceChannelId) {
          return interaction.editOriginalMessage("❌ You must be in a voice channel to start recording.");
        }
        const voiceChannel = guild.channels.get(voiceChannelId);

        const matchId = getOpt("matchid") || `manual-${Date.now()}`;
        const playerMapRaw = getOpt("playermap") || "";
        const playerMap = { _source: "manual" };
        for (const pair of playerMapRaw.split(",").filter(Boolean)) {
          const [sid, did] = pair.trim().split(":");
          if (sid && did) playerMap[did] = sid;
        }
        for (const [did] of voiceChannel.voiceMembers) {
          for (const [sid, d] of steamToDiscord) {
            if (d === did && !playerMap[did]) playerMap[did] = sid;
          }
        }

        await startRecording({ guildId, voiceChannel, matchId, playerMap });
        return interaction.editOriginalMessage(
          `🎙️ Recording started for match \`${matchId}\`\n` +
          `Channel: **${voiceChannel.name}** | Players: ${voiceChannel.voiceMembers.size}\n` +
          `Use \`/match end\` when done.`
        );
      }

      else if (sub === "end") {
        if (!activeSessions.has(guildId)) {
          return interaction.editOriginalMessage("❌ No active recording in this server.");
        }
        const result = await stopRecording(guildId);
        return interaction.editOriginalMessage(
          `✅ Session uploaded for match \`${result.matchId}\`\n` +
          `${result.audioFiles.length} player tracks uploaded. Transcription will begin shortly.`
        );
      }

      else if (sub === "cancel") {
        if (!activeSessions.has(guildId)) {
          return interaction.editOriginalMessage("❌ No active recording in this server.");
        }
        await cancelRecording(guildId);
        return interaction.editOriginalMessage("🗑️ Recording cancelled — nothing was saved.");
      }

      else if (sub === "status") {
        const session = activeSessions.get(guildId);
        if (!session) return interaction.editOriginalMessage("📭 No active recording.");
        const elapsed = Math.floor((Date.now() - session.startedAt) / 1000);
        return interaction.editOriginalMessage(
          `🎙️ Recording active for match \`${session.matchId}\`\nElapsed: **${Math.floor(elapsed/60)}m ${elapsed%60}s**`
        );
      }
    }
  } catch (err) {
    console.error("Interaction error:", err.message);
    try { await interaction.editOriginalMessage("❌ Something went wrong: " + err.message); } catch {}
  }
});

loadLinks();
bot.connect();
