import http from "http";
import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  CopyObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";

const PORT = process.env.PORT || 3000;
const PHASE_LIVE     = "live";
const PHASE_GAMEOVER = "gameover";
const START_DELAY_MS = 0;

const s3 = new S3Client({
  region: process.env.AWS_REGION || "auto",
  endpoint: process.env.S3_ENDPOINT,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const BUCKET = process.env.S3_BUCKET || "cs2-voice-analytics";

async function getR2Json(key) {
  const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: key });
  const resp = await s3.send(cmd);
  const body = await resp.Body.transformToString();
  return JSON.parse(body);
}

async function listR2Keys(prefix) {
  const cmd = new ListObjectsV2Command({ Bucket: BUCKET, Prefix: prefix });
  const resp = await s3.send(cmd);
  return (resp.Contents || []).map(o => o.Key);
}

async function renameSession(oldMatchId, newMatchId) {
  const oldPrefix = `matches/${oldMatchId}/`;
  const newPrefix = `matches/${newMatchId}/`;
  const keys = await listR2Keys(oldPrefix);
  if (keys.length === 0) throw new Error(`No objects found under ${oldPrefix}`);
  await Promise.all(keys.map(key => {
    const newKey = newPrefix + key.slice(oldPrefix.length);
    return s3.send(new CopyObjectCommand({ Bucket: BUCKET, CopySource: `${BUCKET}/${key}`, Key: newKey }));
  }));
  await Promise.all(keys.map(key => s3.send(new DeleteObjectCommand({ Bucket: BUCKET, Key: key }))));
  console.log(`✅ Renamed session ${oldMatchId} → ${newMatchId}`);
}

export function startGsiServer({ onMatchStart, onMatchEnd }) {
  const lastPhase = new Map();

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost`);

    if (req.method === "GET" && url.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ status: "ok" }));
    }

    const analysisMatch = url.pathname.match(/^\/analysis\/([^/]+)$/);
    if (req.method === "GET" && analysisMatch) {
      const matchId = decodeURIComponent(analysisMatch[1]);
      try {
        const data = await getR2Json(`matches/${matchId}/analysis.json`);
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
        return res.end(JSON.stringify(data));
      } catch (err) {
        res.writeHead(err.name === "NoSuchKey" ? 404 : 500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: err.message }));
      }
    }

    const roundsMatch = url.pathname.match(/^\/rounds\/([^/]+)$/);
    if (req.method === "GET" && roundsMatch) {
      const matchId = decodeURIComponent(roundsMatch[1]);
      try {
        const data = await getR2Json(`matches/${matchId}/round_analyses.json`);
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
        return res.end(JSON.stringify(data));
      } catch (err) {
        res.writeHead(err.name === "NoSuchKey" ? 404 : 500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: err.message }));
      }
    }

    if (req.method === "POST" && url.pathname === "/sessions/link") {
      let body = "";
      req.on("data", chunk => (body += chunk));
      req.on("end", async () => {
        try {
          const { matchDate, demoMatchId } = JSON.parse(body);
          if (!matchDate || !demoMatchId) {
            res.writeHead(400, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ error: "matchDate and demoMatchId required" }));
          }
          const targetTs = new Date(matchDate).getTime();
          const listCmd = new ListObjectsV2Command({ Bucket: BUCKET, Prefix: "matches/", Delimiter: "/" });
          const listResp = await s3.send(listCmd);
          const prefixes = (listResp.CommonPrefixes || []).map(p => p.Prefix);
          const sessions = (await Promise.all(
            prefixes.map(async prefix => {
              try { const meta = await getR2Json(`${prefix}meta.json`); return { matchId: meta.matchId, startedAt: meta.startedAt }; }
              catch { return null; }
            })
          )).filter(Boolean);
          const THREE_HOURS = 3 * 60 * 60 * 1000;
          const closest = sessions
            .filter(s => s.matchId !== demoMatchId)
            .map(s => ({ ...s, diff: Math.abs(s.startedAt - targetTs) }))
            .filter(s => s.diff < THREE_HOURS)
            .sort((a, b) => a.diff - b.diff)[0];
          if (!closest) {
            res.writeHead(404, { "Content-Type": "application/json" });
            return res.end(JSON.stringify({ error: "No matching voice session found within 3 hours of matchDate" }));
          }
          await renameSession(closest.matchId, demoMatchId);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ linked: true, oldMatchId: closest.matchId, newMatchId: demoMatchId, timeDiffSeconds: Math.round(closest.diff / 1000) }));
        } catch (err) {
          console.error("[link] Error:", err);
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: err.message }));
        }
      });
      return;
    }

    if (req.method === "POST" && url.pathname === "/gsi") {
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
      return;
    }

    res.writeHead(404);
    res.end();
  });

  server.listen(PORT, "::", () => {
    console.log(`✅ GSI + API server listening on port ${PORT}`);
  });

  return server;
}

function handleGsiEvent(state, lastPhase, onMatchStart, onMatchEnd) {
  const steamId  = state.provider?.steamid;
  const mapPhase = state.map?.phase;
  const matchId  = state.map?.matchid || generateMatchId(state);
  if (!steamId || !mapPhase) return;
  const prev = lastPhase.get(steamId);
  if (prev === mapPhase) return;
  lastPhase.set(steamId, mapPhase);
  console.log(`[GSI] steamId=${steamId} phase: ${prev ?? "unknown"} → ${mapPhase}`);
  if (mapPhase === PHASE_LIVE && prev !== PHASE_LIVE) {
    const startedAt = Date.now() - START_DELAY_MS;
    setTimeout(() => { onMatchStart({ matchId, steamId, startedAt, source: "gsi" }); }, START_DELAY_MS);
  }
  if (mapPhase === PHASE_GAMEOVER && prev === PHASE_LIVE) {
    onMatchEnd({ matchId, steamId, source: "gsi" });
  }
}

function generateMatchId(state) {
  const map = state.map?.name || "unknown";
  const ts  = Math.floor(Date.now() / 1000);
  return `${map}-${ts}`;
}
