const http = require("http");
const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;

const LAST_SESSION_FILE = path.join(__dirname, "lastSession.json");

// Load lastSession from disk so it survives server restarts / Render sleeps.
let lastSession = null;
try {
  if (fs.existsSync(LAST_SESSION_FILE)) {
    lastSession = JSON.parse(fs.readFileSync(LAST_SESSION_FILE, "utf8"));
  }
} catch (e) { /* ignore corrupt / missing file */ }

function saveLastSession() {
  try { fs.writeFileSync(LAST_SESSION_FILE, JSON.stringify(lastSession), "utf8"); } catch (e) {}
}

let game = {
  players: [],
  revealed: false,
  average: null,
  agreedPoints: null,
  round: 1,
  adminId: null,
  ticket: "",
  tickets: [],
  ticketIndex: 0,
  setupDone: false,
  sessionStartedAt: null,
  history: [],
  showSummary: false,
  cheeseGone: false,
};

function calcAverage(players) {
  const votes = players.filter((p) => p.vote !== null).map((p) => Number(p.vote));
  if (!votes.length) return null;
  return Math.round((votes.reduce((a, b) => a + b, 0) / votes.length) * 10) / 10;
}

const POINT_VALUES = [1, 2, 3, 5, 8, 13];
function autoAgreeIfExact(avg) {
  const n = Number(avg);
  return POINT_VALUES.includes(n) ? n : null;
}

function broadcast(wss, data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach((c) => { if (c.readyState === 1) c.send(msg); });
}

function broadcastState(wss) {
  // Clients who haven't joined the game yet (sitting on the join screen) must
  // never receive showSummary:true — it triggers the summary popup before they
  // have even entered their name.  Build both payloads once and pick per client.
  const now = Date.now();
  const full    = JSON.stringify({ type: "state", game, lastSession, serverNow: now });
  const noSumm  = game.showSummary
    ? JSON.stringify({ type: "state", game: { ...game, showSummary: false }, lastSession, serverNow: now })
    : full;
  wss.clients.forEach((c) => {
    if (c.readyState !== 1) return;
    const pid    = wsPlayerMap.get(c);
    const inGame = pid && game.players.some((p) => p.id === pid);
    c.send(inGame ? full : noSumm);
  });
}

// ── Server-driven animation scheduler ────────────────────────────────────
// Server fires precise 'animate' messages so all clients stay in sync.
// passIndex seeds the random choices so every client shows identical animations.
let animTimers = [];

function clearAnimTimers() {
  animTimers.forEach(clearTimeout);
  animTimers = [];
}

function scheduleAnimations(wss, sessionStartedAt) {
  clearAnimTimers();
  const now = Date.now();
  const elapsed = now - sessionStartedAt;

  function fireAt(targetMs, payload) {
    const delay = Math.max(0, targetMs - elapsed);
    const t = setTimeout(() => {
      if (payload.act === "mouse_steal") game.cheeseGone = true;
      broadcast(wss, { type: "animate", ...payload });
    }, delay);
    animTimers.push(t);
  }

  // Act 1: mouse steals cheese — T+30s
  if (elapsed < 30000) fireAt(30000, { act: "mouse_steal" });

  // Act 2: cat chases mouse — T+60s
  if (elapsed < 60000) fireAt(60000, { act: "cat_chase" });

  // Chase loop: every 30s from T+90s, seeded by passIndex
  const chaseBase = 90000;
  // Find first pass that hasn't fired yet
  const firstPass = elapsed < chaseBase ? 0 : Math.floor((elapsed - chaseBase) / 30000) + 1;
  for (let i = firstPass; i < firstPass + 200; i++) {
    const t = chaseBase + i * 30000;
    if (t - elapsed > 0) {
      fireAt(t, { act: "chase_pass", passIndex: i, dir: i % 2 === 0 ? "left" : "right" });
    }
  }
}

const server = http.createServer((req, res) => {
  const filePath = path.join(__dirname, "public", "index.html");
  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(500); res.end("Error loading app"); return; }
    res.writeHead(200, {
      "Content-Type": "text/html",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "Pragma": "no-cache",
      "Expires": "0",
    });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server });
const wsPlayerMap = new Map(); // ws  → playerId
const idToWs      = new Map(); // playerId → ws  (used for refresh deduplication)

// Grace-period timers: playerId → timeout handle
// When a WS closes we wait before actually removing the player,
// so mobile / tab-switch reconnects land back in the game seamlessly.
const disconnectTimers = new Map();
const DISCONNECT_GRACE_MS = 45_000; // 45 s — enough for a mobile reconnect

// Keep-alive ping so proxies / mobile radios don't kill idle connections.
setInterval(() => {
  wss.clients.forEach((c) => { if (c.readyState === 1) c.ping(); });
}, 25_000);

wss.on("connection", (ws) => {
  // Mask showSummary on the initial send — fresh visitors on the join screen
  // should never see the summary popup.  Players who rejoin mid-summary receive
  // the real state (showSummary: true) via broadcastState after their join
  // message, and get a targeted show_summary event on top of that.
  ws.send(JSON.stringify({ type: "state", game: { ...game, showSummary: false }, lastSession, serverNow: Date.now() }));
  ws.on("pong", () => {}); // browser auto-replies; just suppress unhandled events

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {

      case "join": {
        const playerId = msg.id;
        wsPlayerMap.set(ws, playerId);
        idToWs.set(playerId, ws);
        // Cancel any pending removal — this player reconnected in time
        if (disconnectTimers.has(playerId)) {
          clearTimeout(disconnectTimers.get(playerId));
          disconnectTimers.delete(playerId);
        }
        // Deduplicate: always remove any existing entry with the same name.
        // We no longer replace the client's fresh ID in the WS interceptor, so a refresh
        // will arrive with a new ID but the same name — the old entry must be cleared to
        // prevent ghost duplicates regardless of whether its connection is still open.
        if (msg.name) {
          game.players = game.players.filter((p) => {
            if (p.id === playerId) return false; // will be re-added below
            if (p.name !== msg.name) return true; // different name — keep
            if (p.id === game.adminId) game.adminId = null; // transfer admin to new join below
            return false; // same name — always remove old entry
          });
        }
        if (game.players.find((p) => p.id === playerId)) { broadcastState(wss); break; }
        game.players.push({ id: playerId, name: msg.name, vote: null });
        if (msg.wantAdmin) game.adminId = playerId;
        broadcastState(wss);
        // If the session summary is already showing, re-send the show_summary event
        // directly to the reconnecting client so the popup appears without needing
        // every other client to receive a redundant broadcast.
        if (game.showSummary) {
          ws.send(JSON.stringify({ type: "show_summary" }));
        }
        break;
      }

      case "setup_session": {
        if (msg.id !== game.adminId) break;
        const tickets = msg.tickets || [];
        game.tickets = tickets;
        game.ticketIndex = 0;
        game.ticket = tickets[0] || "";
        game.setupDone = true;
        game.sessionStartedAt = Date.now();
        game.showSummary = false;
        game.agreedPoints = null;
        game.round = 1;
        game.revealed = false;
        game.average = null;
        game.history = [];
        game.players = game.players.map(p => ({ ...p, vote: null }));
        broadcastState(wss);
        scheduleAnimations(wss, game.sessionStartedAt);
        break;
      }

      case "vote": {
        const p = game.players.find((p) => p.id === msg.id);
        if (p && !game.revealed && !game.showSummary && game.ticket?.label && msg.id !== game.adminId) {
          p.vote = Number(msg.vote);
          const nonAdminPlayers = game.players.filter((p) => p.id !== game.adminId);
          if (nonAdminPlayers.length > 0 && nonAdminPlayers.every((p) => p.vote !== null)) {
            game.revealed = true;
            game.average = calcAverage(nonAdminPlayers);
            game.agreedPoints = autoAgreeIfExact(game.average);
          }
          broadcastState(wss);
        }
        break;
      }

      case "reveal": {
        if (msg.id !== game.adminId) break;
        game.revealed = true;
        game.average = calcAverage(game.players.filter((p) => p.id !== game.adminId));
        game.agreedPoints = autoAgreeIfExact(game.average);
        broadcastState(wss);
        break;
      }

      case "new_round": {
        if (msg.id !== game.adminId) break;
        const agreedPts = game.agreedPoints ?? game.average;
        const historyEntry = game.revealed && agreedPts !== null
          ? { round: game.round, ticket: game.ticket?.label || "", ticketUrl: game.ticket?.url || "", points: agreedPts }
          : null;
        const nextIndex = (game.ticketIndex || 0) + 1;
        const nextTicket = (game.tickets || [])[nextIndex] || "";
        game.players = game.players.map((p) => ({ ...p, vote: null }));
        game.revealed = false;
        game.average = null;
        game.agreedPoints = null;
        game.round += 1;
        game.ticketIndex = nextIndex;
        game.ticket = nextTicket;
        if (historyEntry) game.history = [...(game.history || []), historyEntry];
        broadcastState(wss);
        break;
      }

      case "leave": {
        game.players = game.players.filter((p) => p.id !== msg.id);
        if (game.adminId === msg.id) game.adminId = null;
        broadcastState(wss);
        break;
      }

      case "reset_round": {
        if (msg.id !== game.adminId) break;
        game.players = game.players.map(p => ({ ...p, vote: null }));
        game.revealed = false;
        game.average = null;
        game.agreedPoints = null;
        broadcastState(wss);
        break;
      }

      case "skip_ticket": {
        if (msg.id !== game.adminId) break;
        const skipEntry = {
          round: game.round,
          ticket: game.ticket?.label || "",
          ticketUrl: game.ticket?.url || "",
          points: 0,
          skipped: true,
        };
        const skipNextIndex = (game.ticketIndex || 0) + 1;
        const skipNextTicket = (game.tickets || [])[skipNextIndex] || "";
        game.history = [...(game.history || []), skipEntry];
        game.players = game.players.map((p) => ({ ...p, vote: null }));
        game.revealed = false;
        game.average = null;
        game.agreedPoints = null;
        game.round += 1;
        game.ticketIndex = skipNextIndex;
        game.ticket = skipNextTicket;
        broadcastState(wss);
        break;
      }

      case "reset": {
        if (msg.id !== game.adminId) break;
        clearAnimTimers();
        game = {
          players: [],
          revealed: false, average: null, agreedPoints: null, round: 1, adminId: null,
          ticket: "", tickets: [], ticketIndex: 0, setupDone: false, sessionStartedAt: null, history: [], showSummary: false, cheeseGone: false,
        };
        broadcastState(wss);
        break;
      }

      case "new_session": {
        if (msg.id !== game.adminId) break;
        clearAnimTimers();
        // Keep all players connected, reset everything else back to setup
        game.players = game.players.map(p => ({ ...p, vote: null }));
        game.revealed = false;
        game.average = null;
        game.agreedPoints = null;
        game.round = 1;
        game.ticket = "";
        game.tickets = [];
        game.ticketIndex = 0;
        game.setupDone = false;
        game.sessionStartedAt = null;
        game.history = [];
        game.showSummary = false;
        game.cheeseGone = false;
        broadcastState(wss);
        break;
      }

      case "clear_session": {
        // No auth — anyone on join screen can clear a stale session before joining
        clearAnimTimers();
        game = {
          players: [],
          revealed: false, average: null, agreedPoints: null, round: 1, adminId: null,
          ticket: "", tickets: [], ticketIndex: 0, setupDone: false, sessionStartedAt: null, history: [], showSummary: false, cheeseGone: false,
        };
        broadcastState(wss);
        break;
      }

      case "save_history": {
        if (msg.id !== game.adminId) break;
        game.history = msg.history;
        broadcastState(wss);
        break;
      }

      case "show_summary": {
        if (msg.id !== game.adminId) break;
        // Include current ticket in history only if score was agreed.
        // If admin finishes mid-round (agreedPoints === null), exclude that round.
        if (game.agreedPoints !== null) {
          const already = (game.history || []).some(h => h.round === game.round);
          if (!already) {
            game.history = [...(game.history || []), { round: game.round, ticket: game.ticket?.label || "", ticketUrl: game.ticket?.url || "", points: game.agreedPoints }];
          }
        }
        // Clear any in-progress vote state so the frontend transitions cleanly to summary
        // (prevents a mid-vote partial state from blocking the summary popup).
        game.players  = game.players.map(p => ({ ...p, vote: null }));
        game.revealed = false;
        game.average  = null;
        game.showSummary = true;
        // Snapshot this session for the join screen and persist to disk
        lastSession = {
          history: game.history,
          attendees: game.players.map(p => p.name).filter(Boolean),
          completedAt: Date.now(),
        };
        saveLastSession();
        broadcastState(wss);
        broadcast(wss, { type: "show_summary" });
        break;
      }

      case "set_agreed": {
        if (msg.id !== game.adminId) break;
        const val = Number(msg.points);
        game.agreedPoints = isNaN(val) ? null : val;
        broadcastState(wss);
        break;
      }

      case "nudge": {
        if (msg.id !== game.adminId) break;
        broadcast(wss, { type: "nudge", name: msg.name, ts: Date.now() });
        break;
      }
    }
  });

  ws.on("close", () => {
    const playerId = wsPlayerMap.get(ws);
    wsPlayerMap.delete(ws);
    if (playerId) idToWs.delete(playerId);
    if (!playerId) return;
    // Don't remove immediately — give the client time to reconnect
    // (handles mobile background, tab switch, brief network drop).
    // Explicit "leave" messages still remove instantly (see case "leave").
    const timer = setTimeout(() => {
      disconnectTimers.delete(playerId);
      game.players = game.players.filter((p) => p.id !== playerId);
      if (game.adminId === playerId) game.adminId = null;
      broadcastState(wss);
    }, DISCONNECT_GRACE_MS);
    disconnectTimers.set(playerId, timer);
  });
});

server.listen(PORT, () => {
  console.log(`Purrfect Planning Poker running on http://localhost:${PORT}`);
});
