# CS2 Voice Analytics — Setup Guide

Follow these steps in order. The whole process takes about 60–90 minutes the first time.

---

## Step 1 — Create your accounts

You need accounts on four services before touching any code. All are free to start.

**Cloudflare R2** (object storage — replaces S3)
- Go to cloudflare.com → sign up or log in
- In the dashboard, go to **R2 Object Storage** → **Create bucket**
- Name it `cs2-voice-analytics`
- Stay on the R2 page — you'll need credentials shortly

**AssemblyAI** (transcription)
- Go to assemblyai.com → sign up
- Copy your **API key** from the dashboard
- Free tier gives you $50 credit — no card required

**Google AI Studio** (AI analysis)
- Go to aistudio.google.com → sign in with Google
- Click **Get API key** → **Create API key**
- Copy it somewhere safe
- Gemini 2.0 Flash has a generous free tier (1,500 requests/day)

**Railway** (hosting all services)
- Go to railway.app → sign up with GitHub
- You likely already have this

---

## Step 2 — Set up Cloudflare R2 credentials

1. In the Cloudflare dashboard, go to **R2** → **Manage R2 API Tokens**
2. Click **Create API Token**
3. Give it **Object Read & Write** permissions, scoped to your `cs2-voice-analytics` bucket
4. Copy and save these three values:
   - **Access Key ID**
   - **Secret Access Key**
   - **Endpoint URL** — looks like `https://<account_id>.r2.cloudflarestorage.com`

---

## Step 3 — Create the Discord bot

1. Go to discord.com/developers/applications → **New Application**
2. Name it `CS2 Voice Analytics` (or whatever you like)
3. Go to **Bot** in the left sidebar
   - Click **Add Bot**
   - Under **Privileged Gateway Intents**, enable:
     - **Server Members Intent**
     - **Message Content Intent**
   - Copy the **Token** — you'll need this later
4. Go to **OAuth2** → **URL Generator**
   - Scopes: check `bot` and `applications.commands`
   - Bot Permissions: check `Connect`, `Speak`, `Send Messages`, `Use Slash Commands`
   - Copy the generated URL and open it in your browser to invite the bot to your Discord server
5. In your Discord server, make sure you have a voice channel players will use during matches (e.g. `#cs2-match`)

---

## Step 4 — Deploy the Discord bot to Railway

1. Push the `discord-bot/` folder to a GitHub repo (can be a monorepo with subdirectories)
2. In Railway → **New Project** → **Deploy from GitHub repo**
3. Select your repo. If it's a monorepo, set the **Root Directory** to `discord-bot/`
4. Railway will detect Node.js automatically. Set the **Start Command** to:
   ```
   node src/index.js
   ```
5. Go to the service's **Variables** tab and add:
   ```
   DISCORD_TOKEN          = <your bot token from Step 3>
   AWS_ACCESS_KEY_ID      = <R2 access key from Step 2>
   AWS_SECRET_ACCESS_KEY  = <R2 secret key from Step 2>
   S3_ENDPOINT            = <R2 endpoint URL from Step 2>
   S3_BUCKET              = cs2-voice-analytics
   AWS_REGION             = auto
   ```
6. Deploy. Check the logs — you should see:
   ```
   ✅ Logged in as CS2 Voice Analytics#1234
   ✅ Slash commands registered
   ✅ GSI server listening on port XXXX
   ```
7. Copy your Railway service's public URL (e.g. `https://cs2-bot-production.up.railway.app`) — you'll need it for the GSI config in Step 7

---

## Step 5 — Deploy the transcription worker to Railway

1. In Railway → **New Service** → **GitHub repo** (same repo)
2. Set **Root Directory** to `transcription-worker/`
3. Railway detects Python. Set **Start Command** to:
   ```
   python worker.py
   ```
4. Add variables:
   ```
   ASSEMBLYAI_API_KEY     = <your AssemblyAI key from Step 1>
   AWS_ACCESS_KEY_ID      = <R2 access key>
   AWS_SECRET_ACCESS_KEY  = <R2 secret key>
   S3_ENDPOINT            = <R2 endpoint URL>
   S3_BUCKET              = cs2-voice-analytics
   AWS_REGION             = auto
   POLL_INTERVAL_SECONDS  = 30
   ```
5. Deploy. Logs should show:
   ```
   🎙️ Transcription worker started (AssemblyAI)
   No pending sessions, sleeping…
   ```

---

## Step 6 — Deploy the alignment service to Railway

1. New Service → same repo, **Root Directory** = `alignment-service/`
2. Railway detects Go. Start Command:
   ```
   go run .
   ```
3. Add variables:
   ```
   AWS_ACCESS_KEY_ID      = <R2 access key>
   AWS_SECRET_ACCESS_KEY  = <R2 secret key>
   S3_ENDPOINT            = <R2 endpoint URL>
   S3_BUCKET              = cs2-voice-analytics
   AWS_REGION             = auto
   ```
4. Deploy. Logs should show:
   ```
   🔗 Alignment service started, polling every 30s
   ```

   > **Note:** The alignment service will error on any session until your existing Go demo parser also uploads a `demo_parsed.json` to `matches/{matchId}/demo_parsed.json` in R2. That's the handshake point between your existing pipeline and this new one. Make sure your demo parser writes the matchId-keyed file to R2 using the same bucket and same matchId that the Discord bot uses.

---

## Step 7 — Deploy the AI analysis worker to Railway

1. New Service → same repo, **Root Directory** = `ai-analysis/`
2. Start Command:
   ```
   python worker.py
   ```
3. Add variables:
   ```
   GEMINI_API_KEY         = <your Google AI Studio key from Step 1>
   AWS_ACCESS_KEY_ID      = <R2 access key>
   AWS_SECRET_ACCESS_KEY  = <R2 secret key>
   S3_ENDPOINT            = <R2 endpoint URL>
   S3_BUCKET              = cs2-voice-analytics
   AWS_REGION             = auto
   POLL_INTERVAL_SECONDS  = 30
   GEMINI_MODEL           = gemini-2.0-flash
   ```
4. Deploy. Logs should show:
   ```
   🤖 AI analysis worker started
   ```

---

## Step 8 — Set up GSI (one player, one time)

One player on your team needs to drop a config file in their CS2 folder. This is a one-time setup that makes recording start and stop automatically.

1. Take the file `discord-bot/gamestate_integration_cs2voice.cfg`
2. Open it and replace `<your-railway-bot-url>` with the Railway URL you copied in Step 4, e.g.:
   ```
   "uri" "https://cs2-bot-production.up.railway.app/gsi"
   ```
3. Place the file here:
   ```
   Steam/steamapps/common/Counter-Strike Global Offensive/game/csgo/cfg/
   gamestate_integration_cs2voice.cfg
   ```
   The full path on Windows is usually:
   ```
   C:\Program Files (x86)\Steam\steamapps\common\Counter-Strike Global Offensive\game\csgo\cfg\
   ```
4. Restart CS2 (the cfg is loaded at launch)

That's it — this player's game will now automatically POST to your Railway bot whenever a match starts or ends.

---

## Step 9 — Link Discord accounts to Steam IDs

Every player whose voice you want labelled by Steam ID needs to run this once in your Discord server:

```
/link steamid:76561198XXXXXXXXXX
```

Their 64-bit Steam ID can be found at steamid.io — just enter their Steam profile URL.

Players who haven't linked will still be recorded, but their audio will be labelled `discord_<discordId>` instead of their Steam ID, and won't map to demo data correctly.

---

## Step 10 — Test run

1. Get everyone into the Discord voice channel
2. Load into a CS2 match
3. Watch the Railway bot logs — when CS2 hits the live phase you should see:
   ```
   [GSI] steamId=76561198... phase: unknown → live
   [GSI] 🟢 Match live — triggering auto-start
   🎙️ Recording started: match=de_dust2-1234567890
   ```
4. Play a few rounds, then finish the match
5. Logs should show:
   ```
   [GSI] 🔴 Match over — triggering auto-stop
   ✅ Upload complete: match=de_dust2-1234567890, tracks=5
   ```
6. Switch to the transcription worker logs — within a minute you should see it pick up the session and start submitting to AssemblyAI
7. Once transcription completes, the alignment service picks it up (requires `demo_parsed.json` to be present — see Step 6 note)
8. Once aligned, the AI analysis worker runs and writes `analysis.json` to R2

You can verify the final output by checking R2 directly:
```
cs2-voice-analytics/
  matches/
    de_dust2-1234567890/
      meta.json              ← status should be "complete"
      audio_76561198....pcm
      transcript.json
      demo_parsed.json
      timeline_merged.json
      round_analyses.json
      analysis.json          ← final player scores and insights
```

---

## Fallback: manual recording

If the GSI player isn't in the match or you want to record without GSI, use the slash commands:

```
/match start matchid:de_dust2-1234567890
```
Run this when the match starts. Use `/match end` when it's over.

The `matchid` you provide here must match what your demo parser uses as the key when uploading `demo_parsed.json` — that's the join point between the two pipelines.

---

## Troubleshooting

**Bot doesn't join voice on match start**
- Check that the GSI player has run `/link` so their Steam ID maps to a Discord ID
- Check Railway bot logs for `[GSI]` lines — if you see the event arriving but no join, the player may not be in a voice channel yet when the event fires

**Transcription worker stuck**
- Sessions interrupted mid-transcription automatically retry on the next poll cycle
- Check AssemblyAI dashboard at app.assemblyai.com to see if jobs are queued or erroring

**Alignment fails with "load demo: get matches/.../demo_parsed.json: NoSuchKey"**
- Your existing demo parser hasn't uploaded the parsed demo yet for this matchId
- Make sure the demo parser uses the same matchId format and writes to the same R2 bucket

**Analysis JSON has `Player_1` instead of real Steam IDs**
- Some players haven't run `/link` — their aliases can't be resolved back to Steam IDs
- Run `/link` for all players and re-trigger analysis by setting `status=pending_analysis` in `meta.json` manually
