const MAX_LINES = 500;
const FIELDS = ["wind_dir", "wind_speed", "humidity", "temperature", "air_pressure", "height"];

const els = {
    producers: document.getElementById("producers"),
    receivers: document.getElementById("receivers"),
    results: document.getElementById("results"),
    input: document.getElementById("input"),
    send: document.getElementById("send"),
    presets: document.getElementById("presets"),
    toggle: document.getElementById("toggle"),
};

let socket = null;
let running = false;
let loading = false;
let presetsLoaded = false;

function fmtTime(ts) {
    if (!ts) return "";
    return new Date(ts * 1000).toLocaleTimeString();
}

function parse(payload) {
    const parts = payload.split("|");
    if (parts.length !== FIELDS.length + 1) return null;
    const row = { timestamp: Number(parts[0]) };
    FIELDS.forEach((f, i) => (row[f] = Number(parts[i + 1])));
    return row;
}

function decode(payload) {
    const row = parse(payload);
    if (!row) return payload;
    return FIELDS.map((f) => `${f}=${row[f]}`).join("  ");
}

function append(target, { channel, payload, timestamp }, cls) {
    const log = els[target];
    const line = document.createElement("div");
    line.className = `line ${cls || ""}`;
    const isErr = typeof payload === "string" && payload.startsWith("ERR:");
    if (isErr) line.classList.add("err");
    line.innerHTML =
        `<span class="ts">${fmtTime(timestamp)}</span>` +
        `<span class="chan">${channel}</span>` +
        `<span class="msg">${isErr ? payload : decode(payload)}</span>`;
    log.appendChild(line);
    while (log.childElementCount > MAX_LINES) log.removeChild(log.firstChild);
    log.scrollTop = log.scrollHeight;
}

function renderToggle() {
    els.toggle.disabled = loading;
    els.toggle.className = `toggle ${loading ? "loading" : running ? "running" : "stopped"}`;
    els.toggle.innerHTML = loading ? '<span class="spinner"></span>' : running ? "stop" : "start";
}

function setRunning(state) {
    running = state;
    renderToggle();
}

function setLoading(state) {
    loading = state;
    renderToggle();
}

function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    socket = new WebSocket(`${proto}://${location.host}/ws`);
    socket.onclose = () => {
        setTimeout(connect, 1500);
    };
    socket.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.kind === "producer") append("producers", msg, "producer");
        else if (msg.kind === "receiver") append("receivers", msg, "receiver");
        else if (msg.kind === "query") {
            if (pending.has(msg.channel)) {
                setBtnLoading(pending.get(msg.channel), false);
                pending.delete(msg.channel);
            }
            const now = Math.floor(Date.now() / 1000);
            if (msg.replies.length === 0)
                append("results", { channel: msg.channel, payload: "ERR: no reply", timestamp: now });
            else msg.replies.forEach((r) => append("results", { ...r, timestamp: now }));
        } else if (msg.kind === "status") {
            setRunning(msg.payload === "running");
            if (running) loadPresets();
        }
    };
}

const pending = new Map();

function setBtnLoading(btn, on) {
    if (!btn) return;
    if (on) {
        btn.dataset.label = btn.textContent;
        btn.style.width = `${btn.offsetWidth}px`;
        btn.style.height = `${btn.offsetHeight}px`;
        btn.disabled = true;
        btn.classList.add("loading");
        btn.innerHTML = '<span class="spinner"></span>';
    } else {
        btn.disabled = false;
        btn.classList.remove("loading");
        btn.textContent = btn.dataset.label || btn.textContent;
        btn.style.width = "";
        btn.style.height = "";
    }
}

function runQuery(text, btn) {
    text = (text || "").trim();
    if (/^query /i.test(text)) text = text.slice(6).trim();
    if (!text || !socket || socket.readyState !== WebSocket.OPEN) return;
    if (btn && btn.disabled) return;
    setBtnLoading(btn, true);
    pending.set(text, btn);
    socket.send(text);
}

els.send.addEventListener("click", () => {
    runQuery(els.input.value, els.send);
    els.input.value = "";
});
els.input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        runQuery(els.input.value, els.send);
        els.input.value = "";
    }
});

document.querySelectorAll(".clear").forEach((b) =>
    b.addEventListener("click", () => (els[b.dataset.target].innerHTML = "")),
);

function addPreset(label, selector) {
    const b = document.createElement("button");
    b.className = "preset";
    b.textContent = label;
    b.title = selector;
    b.addEventListener("click", () => runQuery(selector, b));
    els.presets.appendChild(b);
}

async function loadPresets() {
    if (presetsLoaded) return;
    els.presets.innerHTML = "";
    addPreset("all sardegna", "sardegna/**");
    const cities = new Set();
    try {
        const res = await fetch("/api/channels");
        const data = await res.json();
        if (!data.channels.length) return;
        data.channels.forEach((c) => cities.add(c.split("/").slice(0, -1).join("/")));
        cities.forEach((city) => addPreset(`${city}/*`, `${city}/**`));
        cities.forEach((city) => addPreset(`stats ${city}`, `stats/${city}?window=60`));
        data.channels.forEach((c) => addPreset(c, c));
        presetsLoaded = true;
    } catch (e) {
        console.error("presets", e);
    }
}

els.toggle.addEventListener("click", async () => {
    if (loading) return;
    setLoading(true);
    try {
        const res = await fetch(running ? "/api/stop" : "/api/start", { method: "POST" });
        const data = await res.json();
        running = data.running;
        if (data.running) await loadPresets();
    } catch (e) {
        console.error("toggle", e);
    } finally {
        setLoading(false);
    }
});

async function initStatus() {
    try {
        const res = await fetch("/api/status");
        setRunning((await res.json()).running);
    } catch (e) {
        setRunning(false);
    }
}

initStatus();
loadPresets();
connect();
