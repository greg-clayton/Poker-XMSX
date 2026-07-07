const http = require("http");
const fs   = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");

const PORT              = process.env.PORT || 3000;
const LAST_SESSION_FILE = path.join(__dirname, "lastSession.json");

let lastSession = null;
try {
  if (fs.existsSync(LAST_SESSION_FILE))
    lastSession = JSON.parse(fs.readFileSync(LAST_SESSION_FILE, "utf8"));
} catch (e) {}

function saveLastSession() {
  try { fs.writeFileSync(LAST_SESSION_FILE, JSON.stringify(lastSession), "utf8"); } catch (e) {}
}

function newGame() {
  return {
    players:       [],   // { id, name, sizeVote: null|string, confVote: null|string }
    revealed:      false,
    majoritySize:  null, // winning size string, or null if tied/none
    majorityConf:  null, // winning conf string, or null if tied/none
    sizeTie:       false,
    confTie:       false,
    agreedSize:    null, // admin-confirmed (or auto-agreed)
    agreedConf:    null,
    round:         1,
    adminId:       null,
    ticket:        "",
    tickets:       [],
    ticketIndex:   0,
    setupDone:     false,
    sessionStartedAt: null,
    history:       [],   // { round, ticket, ticketUrl, size, confidence, skipped? }
    showSummary:   false,
    cheeseGone:    false,
  };
}

let game = newGame();

/* ── Majority-vote calculation ─────────────────────────────────────────────── */
function calcMajority(votes) {
  // votes: array of non-null strings
  if (!votes.length) return { winner: null, isTie: false, counts: {} };
  const counts = {};
  votes.forEach(v => { counts[v] = (counts[v] || 0) + 1; });
  const max     = Math.max(...Object.values(counts));
  const winners = Object.keys(counts).filter(k => counts[k] === max);
  return { winner: winners.length === 1 ? winners[0] : null, isTie: winners.length > 1, counts };
}

function applyResults(nonAdminPlayers) {
  const complete = nonAdminPlayers.filter(p => p.sizeVote && p.confVote);
  if (!complete.length) return;
  const sr = calcMajority(complete.map(p => p.sizeVote));
  const cr = calcMajority(complete.map(p => p.confVote));
  game.majoritySize = sr.winner;
  game.sizeTie      = sr.isTie;
  game.majorityConf = cr.winner;
  game.confTie      = cr.isTie;
  if (!sr.isTie && !cr.isTie) {
    game.agreedSize = sr.winner;
    game.agreedConf = cr.winner;
  }
}

/* ── Broadcasting ──────────────────────────────────────────────────────────── */
function broadcast(wss, data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

function broadcastState(wss) {
  const now  = Date.now();
  const full = JSON.stringify({ type: "state", game, lastSession, serverNow: now });
  const safe = game.showSummary
    ? JSON.stringify({ type: "state", game: { ...game, showSummary: false }, lastSession, serverNow: now })
    : full;
  wss.clients.forEach(c => {
    if (c.readyState !== 1) return;
    const pid    = wsPlayerMap.get(c);
    const inGame = pid && game.players.some(p => p.id === pid);
    c.send(inGame ? full : safe);
  });
}

/* ── Animation scheduler ───────────────────────────────────────────────────── */
let animTimers = [];
function clearAnimTimers() { animTimers.forEach(clearTimeout); animTimers = []; }

function scheduleAnimations(wss, startedAt) {
  clearAnimTimers();
  const elapsed = Date.now() - startedAt;
  function fireAt(ms, payload) {
    const delay = Math.max(0, ms - elapsed);
    const t = setTimeout(() => {
      if (payload.act === "mouse_steal") game.cheeseGone = true;
      broadcast(wss, { type: "animate", ...payload });
    }, delay);
    animTimers.push(t);
  }
  if (elapsed < 30000) fireAt(30000, { act: "mouse_steal" });
  if (elapsed < 60000) fireAt(60000, { act: "cat_chase" });
  const base      = 90000;
  const firstPass = elapsed < base ? 0 : Math.floor((elapsed - base) / 30000) + 1;
  for (let i = firstPass; i < firstPass + 200; i++) {
    const t = base + i * 30000;
    if (t - elapsed > 0) fireAt(t, { act: "chase_pass", passIndex: i, dir: i % 2 === 0 ? "left" : "right" });
  }
}

/* ── HTTP + WebSocket server ───────────────────────────────────────────────── */
const server = http.createServer((req, res) => {
  fs.readFile(path.join(__dirname, "public", "index.html"), (err, data) => {
    if (err) { res.writeHead(500); res.end("Error loading app"); return; }
    res.writeHead(200, {
      "Content-Type":  "text/html",
      "Cache-Control": "no-store, no-cache, must-revalidate",
      "Pragma":        "no-cache",
      "Expires":       "0",
    });
    res.end(data);
  });
});

const wss         = new WebSocketServer({ server });
const wsPlayerMap = new Map(); // ws → playerId
const idToWs      = new Map(); // playerId → ws
const disconnectTimers = new Map();
const DISCONNECT_GRACE_MS = 45_000;

setInterval(() => { wss.clients.forEach(c => { if (c.readyState === 1) c.ping(); }); }, 25_000);

wss.on("connection", ws => {
  ws.send(JSON.stringify({ type: "state", game: { ...game, showSummary: false }, lastSession, serverNow: Date.now() }));
  ws.on("pong", () => {});

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // Observer-only request: send this client the current state so it can render
    // an out-of-band summary/banner (e.g. the unified app's login screen). Uses
    // the masked payload (showSummary suppressed) since the requester isn't in
    // the game.
    if (msg.type === "request_state") {
      ws.send(JSON.stringify({
        type: "state",
        game: { ...game, showSummary: false },
        lastSession,
        serverNow: Date.now(),
      }));
      return;
    }

    switch (msg.type) {

      case "join": {
        const pid = msg.id;
        wsPlayerMap.set(ws, pid);
        idToWs.set(pid, ws);
        if (disconnectTimers.has(pid)) { clearTimeout(disconnectTimers.get(pid)); disconnectTimers.delete(pid); }
        if (msg.name) {
          game.players = game.players.filter(p => {
            if (p.id === pid)         return false;
            if (p.name !== msg.name)  return true;
            if (p.id === game.adminId) game.adminId = pid; // transfer admin to reconnecting player
            return false;
          });
        }
        if (!game.players.find(p => p.id === pid))
          game.players.push({ id: pid, name: msg.name, sizeVote: null, confVote: null });
        if (msg.wantAdmin) game.adminId = pid;
        broadcastState(wss);
        if (game.showSummary) ws.send(JSON.stringify({ type: "show_summary" }));
        break;
      }

      case "setup_session": {
        if (msg.id !== game.adminId) break;
        game.tickets       = msg.tickets || [];
        game.ticketIndex   = 0;
        game.ticket        = game.tickets[0] || "";
        game.setupDone     = true;
        game.sessionStartedAt = Date.now();
        game.showSummary   = false;
        game.agreedSize    = null; game.agreedConf = null;
        game.majoritySize  = null; game.majorityConf = null;
        game.sizeTie       = false; game.confTie = false;
        game.round         = 1;    game.revealed = false;
        game.history       = [];
        game.cheeseGone    = false;
        game.players       = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        broadcastState(wss);
        scheduleAnimations(wss, game.sessionStartedAt);
        break;
      }

      case "vote": {
        const p = game.players.find(p => p.id === msg.id);
        if (!p || game.revealed || game.showSummary || !game.ticket?.label || msg.id === game.adminId) break;
        if (msg.size) p.sizeVote = msg.size;
        if (msg.conf) p.confVote = msg.conf;
        // Auto-reveal when all non-admin have both votes
        const nonAdmin = game.players.filter(p => p.id !== game.adminId);
        if (nonAdmin.length > 0 && nonAdmin.every(p => p.sizeVote && p.confVote)) {
          game.revealed = true;
          applyResults(nonAdmin);
        }
        broadcastState(wss);
        break;
      }

      case "reveal": {
        if (msg.id !== game.adminId) break;
        game.revealed = true;
        applyResults(game.players.filter(p => p.id !== game.adminId));
        broadcastState(wss);
        break;
      }

      case "new_round": {
        if (msg.id !== game.adminId) break;
        if (game.revealed && game.agreedSize && game.agreedConf) {
          game.history = [...game.history, {
            round: game.round, ticket: game.ticket?.label || "",
            ticketUrl: game.ticket?.url || "",
            size: game.agreedSize, confidence: game.agreedConf
          }];
        }
        const ni = (game.ticketIndex || 0) + 1;
        game.players      = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        game.revealed     = false;
        game.majoritySize = null; game.majorityConf = null;
        game.sizeTie      = false; game.confTie = false;
        game.agreedSize   = null; game.agreedConf = null;
        game.round       += 1;
        game.ticketIndex  = ni;
        game.ticket       = game.tickets[ni] || "";
        broadcastState(wss);
        break;
      }

      case "leave": {
        game.players = game.players.filter(p => p.id !== msg.id);
        if (game.adminId === msg.id) game.adminId = null;
        broadcastState(wss);
        break;
      }

      case "reset_round": {
        if (msg.id !== game.adminId) break;
        game.players      = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        game.revealed     = false;
        game.majoritySize = null; game.majorityConf = null;
        game.sizeTie      = false; game.confTie = false;
        game.agreedSize   = null; game.agreedConf = null;
        broadcastState(wss);
        break;
      }

      case "skip_ticket": {
        if (msg.id !== game.adminId) break;
        game.history = [...game.history, {
          round: game.round, ticket: game.ticket?.label || "",
          ticketUrl: game.ticket?.url || "",
          size: null, confidence: null, skipped: true
        }];
        const si = (game.ticketIndex || 0) + 1;
        game.players      = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        game.revealed     = false;
        game.majoritySize = null; game.majorityConf = null;
        game.sizeTie      = false; game.confTie = false;
        game.agreedSize   = null; game.agreedConf = null;
        game.round       += 1;
        game.ticketIndex  = si;
        game.ticket       = game.tickets[si] || "";
        broadcastState(wss);
        break;
      }

      case "reset": {
        if (msg.id !== game.adminId) break;
        clearAnimTimers(); game = newGame(); broadcastState(wss);
        break;
      }

      case "new_session": {
        if (msg.id !== game.adminId) break;
        clearAnimTimers();
        game.players      = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        game.revealed     = false;
        game.majoritySize = null; game.majorityConf = null;
        game.sizeTie      = false; game.confTie = false;
        game.agreedSize   = null; game.agreedConf = null;
        game.round        = 1;    game.ticket = ""; game.tickets = []; game.ticketIndex = 0;
        game.setupDone    = false; game.sessionStartedAt = null; game.history = [];
        game.showSummary  = false; game.cheeseGone = false;
        broadcastState(wss);
        break;
      }

      case "clear_session": {
        clearAnimTimers(); game = newGame(); broadcastState(wss);
        break;
      }

      case "show_summary": {
        if (msg.id !== game.adminId) break;
        if (game.agreedSize && game.agreedConf) {
          if (!(game.history || []).some(h => h.round === game.round))
            game.history = [...game.history, {
              round: game.round, ticket: game.ticket?.label || "",
              ticketUrl: game.ticket?.url || "",
              size: game.agreedSize, confidence: game.agreedConf
            }];
        }
        game.players      = game.players.map(p => ({ ...p, sizeVote: null, confVote: null }));
        game.revealed     = false;
        game.majoritySize = null; game.majorityConf = null;
        game.agreedSize   = null; game.agreedConf = null;
        game.showSummary  = true;
        lastSession = {
          history:     game.history,
          attendees:   game.players.map(p => p.name).filter(Boolean),
          completedAt: Date.now(),
        };
        saveLastSession();
        broadcastState(wss);
        broadcast(wss, { type: "show_summary" });
        break;
      }

      case "set_agreed": {
        if (msg.id !== game.adminId) break;
        if (msg.size) { game.agreedSize = msg.size; game.sizeTie = false; }
        if (msg.conf) { game.agreedConf = msg.conf; game.confTie = false; }
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
    const pid = wsPlayerMap.get(ws);
    wsPlayerMap.delete(ws);
    if (pid) idToWs.delete(pid);
    if (!pid) return;
    const timer = setTimeout(() => {
      disconnectTimers.delete(pid);
      game.players = game.players.filter(p => p.id !== pid);
      if (game.adminId === pid) game.adminId = null;
      broadcastState(wss);
    }, DISCONNECT_GRACE_MS);
    disconnectTimers.set(pid, timer);
  });
});

server.listen(PORT, () => console.log(`Purrfect Planning Poker - XMS X running on http://localhost:${PORT}`));
