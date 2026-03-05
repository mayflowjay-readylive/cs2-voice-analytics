import http from "http";

/**
 * GSI Server
 *
 * CS2 POSTs game state updates to this HTTP server automatically
 * as the match progresses. We watch for map phase transitions to
 * auto-start and auto-stop recording.
 *
 * One player on the team needs the cfg file in their CS2 folder:
 *   Steam/steamapps/common/Counter-Strike Global Offensive/game/csgo/cfg/
 *   gamestate_integration_cs2voice.cfg
 *
 * That's a one-time setup. After that, everything is automatic.
 */

const PORT = process.env.PORT || 3000;

// Phase transitions we care about
const PHASE_LIVE     = "live";
const PHASE_GAMEOVER = "gameover";

// How long to wait after "live" before starting — gives the demo
// a moment to fully load so the tick clock is running. 0 is fine
// for most cases; increase if you notice consistent drift.
const START_DELAY_MS = 0;

export function startGsiServer({ onMatchStart, onMatchEnd }) {
  // Track last known phase per Steam ID to avoid duplicate triggers
  const lastPhase = new Map();

  const server = http.createServer((req, res) => {
    if (req.method !== "POST" || req.url !== "/gsi") {
      res.writeHead(404);
      return res.end();
    }

    let body = "";
    req.on("data", (chunk) => (body += chunk));
    req.on("end", () => {
      res.writeHead(200);
      res.end("OK");

      try {
        const state = JSON.parse(body);
        handleGsiEvent(state, lastPhase, onMatchStart, onMatchEnd);
      } catch (err) {
        console.error("[GSI] Failed to parse payload:", err.message);
      }
    });
  });

  server.listen(PORT, () => {
    console.log(`✅ GSI server listening on port ${PORT}`);
    console.log(`   CS2 cfg should point to: https://<your-railway-url>/gsi`);
  });

  return server;
}

function handleGsiEvent(state, lastPhase, onMatchStart, onMatchEnd) {
  const steamId  = state.provider?.steamid;
  const mapPhase = state.map?.phase;
  const matchId  = state.map?.matchid || generateMatchId(state);

  if (!steamId || !mapPhase) return;

  const prev = lastPhase.get(steamId);
  if (prev === mapPhase) return; // no change
  lastPhase.set(steamId, mapPhase);

  console.log(`[GSI] steamId=${steamId} phase: ${prev ?? "unknown"} → ${mapPhase}`);

  if (mapPhase === PHASE_LIVE && prev !== PHASE_LIVE) {
    const startedAt = Date.now() - START_DELAY_MS;
    console.log(`[GSI] 🟢 Match live — triggering auto-start (matchId=${matchId})`);
    setTimeout(() => {
      onMatchStart({
        matchId,
        steamId,       // the player who sent the GSI event
        startedAt,
        source: "gsi",
      });
    }, START_DELAY_MS);
  }

  if (mapPhase === PHASE_GAMEOVER && prev === PHASE_LIVE) {
    console.log(`[GSI] 🔴 Match over — triggering auto-stop (matchId=${matchId})`);
    onMatchEnd({ matchId, steamId, source: "gsi" });
  }
}

// Fallback match ID when CS2 doesn't provide one (e.g. community servers)
function generateMatchId(state) {
  const map    = state.map?.name   || "unknown";
  const ts     = Math.floor(Date.now() / 1000);
  return `${map}-${ts}`;
}
