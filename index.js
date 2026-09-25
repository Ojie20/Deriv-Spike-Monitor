/**
 * Deriv Synthetic Index Spike/Crash Monitor
 * 
**/
require('dotenv').config();
const WebSocket = require('ws');

// ─── CONFIG ────────────────────────────────────────────────────────────
const CONFIG = {
  // Symbol to watch. Common synthetic indices:
  //  R_10, R_25, R_50, R_75, R_100          (Volatility Indices)
  //  1HZ10V, 1HZ25V, ... 1HZ100V             (Volatility Indices (1s))
  //  BOOM300N, BOOM500, BOOM1000             (Boom Indices)
  //  CRASH300N, CRASH500, CRASH1000          (Crash Indices)
  SYMBOL: process.env.DERIV_SYMBOL || 'R_75',

  // Alert if price moves this % or more within WINDOW_SECONDS
  THRESHOLD_PERCENT: parseFloat(process.env.THRESHOLD_PERCENT || '0.5'),

  // Rolling window to measure the move over
  WINDOW_SECONDS: parseInt(process.env.WINDOW_SECONDS || '60', 10),

  // Minimum time between two alerts, so you're not spammed mid-move
  COOLDOWN_SECONDS: parseInt(process.env.COOLDOWN_SECONDS || '30', 10),

  NTFY_TOPIC: process.env.NTFY_TOPIC || 'YOUR_NTFY_TOPIC',

  NTFY_SERVER: process.env.NTFY_SERVER || 'https://ntfy.sh',
};
// ───────────────────────────────────────────────────────────────────────

const DERIV_WS_URL = 'wss://api.derivws.com/trading/v1/options/ws/public';

/** Rolling buffer of {time, price} ticks, pruned to WINDOW_SECONDS. */
let tickBuffer = [];
let lastAlertAt = 0;

function pruneBuffer(nowMs) {
  const cutoff = nowMs - CONFIG.WINDOW_SECONDS * 1000;
  tickBuffer = tickBuffer.filter((t) => t.time >= cutoff);
}

function checkForSpike(nowMs, currentPrice) {
  if (tickBuffer.length === 0) return;

  const oldest = tickBuffer[0];
  const pctChange = ((currentPrice - oldest.price) / oldest.price) * 100;

  const cooledDown = nowMs - lastAlertAt >= CONFIG.COOLDOWN_SECONDS * 1000;

  if (Math.abs(pctChange) >= CONFIG.THRESHOLD_PERCENT && cooledDown) {
    const isSpike = pctChange > 0;
    
    const title = `${isSpike ? 'SPIKE' : 'CRASH'} - ${CONFIG.SYMBOL}`;
    const body =
      `${pctChange.toFixed(2)}% in the last ${CONFIG.WINDOW_SECONDS}s\n` +
      `${oldest.price} → ${currentPrice}`;

    console.log(`[ALERT] ${title} | ${body.replace('\n', ' | ')}`);
    sendNtfyAlert(title, body, isSpike);
    lastAlertAt = nowMs;
  }
}

async function sendNtfyAlert(title, body, isSpike) {
  const url = `${CONFIG.NTFY_SERVER}/${CONFIG.NTFY_TOPIC}`;
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'text/plain; charset=utf-8',
        Title: title,
        Priority: 'high',
        Tags: isSpike ? 'chart_with_upwards_trend' : 'chart_with_downwards_trend',
      },
      body,
    });
    if (!res.ok) {
      console.error('ntfy send failed:', await res.text());
    }
  } catch (err) {
    console.error('ntfy send error:', err.message);
  }
}

function connect() {
  const ws = new WebSocket(DERIV_WS_URL);

  ws.on('open', () => {
    console.log(`Connected. Subscribing to ticks for ${CONFIG.SYMBOL}...`);
    ws.send(JSON.stringify({ ticks: CONFIG.SYMBOL, subscribe: 1 }));
  });

  ws.on('message', (data) => {
    const msg = JSON.parse(data);

    if (msg.error) {
      console.error('Deriv API error:', msg.error.message);
      return;
    }

    if (msg.msg_type === 'tick') {
      const { quote, epoch } = msg.tick;
      const nowMs = epoch * 1000;

      tickBuffer.push({ time: nowMs, price: quote });
      pruneBuffer(nowMs);
      checkForSpike(nowMs, quote);

      console.log(`${new Date(nowMs).toLocaleTimeString()}  ${CONFIG.SYMBOL} = ${quote}`);
    }
  });

  ws.on('close', () => {
    console.log('Connection closed. Reconnecting in 5s...');
    setTimeout(connect, 5000);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
  });
}

console.log(
  `Starting monitor: ${CONFIG.SYMBOL}, threshold ${CONFIG.THRESHOLD_PERCENT}% ` +
  `over ${CONFIG.WINDOW_SECONDS}s, cooldown ${CONFIG.COOLDOWN_SECONDS}s`
);
connect();

