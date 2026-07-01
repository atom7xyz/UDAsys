const MAX_LINES = 500;
const FIELDS = ["wind_dir", "wind_speed", "humidity", "temperature", "air_pressure", "height"];

const els = {
    status: document.getElementById("status"),
    producers: document.getElementById("producers"),
    consumers: document.getElementById("consumers"),
    results: document.getElementById("results"),
    input: document.getElementById("input"),
    send: document.getElementById("send"),
    presets: document.getElementById("presets"),
};

let socket = null;

function fmtTime(ts) {
    if (!ts) return "";
    return new Date(ts * 1000).toLocaleTimeString();
}

function decode(payload) {
    const parts = payload.split("|");
    if (parts.length !== FIELDS.length + 1) return payload;
    return FIELDS.map((f, i) => `${f}=${parts[i + 1]}`).join("  ");
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

function setStatus(online) {
    els.status.textContent = online ? "online" : "offline";
    els.status.className = `status ${online ? "online" : "offline"}`;
}

function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    socket = new WebSocket(`${proto}://${location.host}/ws`);
    socket.onopen = () => setStatus(true);
    socket.onclose = () => {
        setStatus(false);
        setTimeout(connect, 1500);
    };
    socket.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);
        if (msg.kind === "producer") append("producers", msg, "producer");
        else if (msg.kind === "consumer") append("consumers", msg, "consumer");
        else if (msg.kind === "query") {
            const now = Math.floor(Date.now() / 1000);
            if (msg.replies.length === 0)
                append("results", { channel: msg.channel, payload: "ERR: no reply", timestamp: now });
            else msg.replies.forEach((r) => append("results", { ...r, timestamp: now }));
        }
    };
}

function send(command) {
    const text = (command || els.input.value).trim();
    if (!text || !socket || socket.readyState !== WebSocket.OPEN) return;
    socket.send(text);
    if (!command) els.input.value = "";
}

els.send.addEventListener("click", () => send());
els.input.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        send();
    }
});

document.querySelectorAll(".clear").forEach((b) =>
    b.addEventListener("click", () => (els[b.dataset.target].innerHTML = "")),
);

function addPreset(label, selector) {
    const b = document.createElement("button");
    b.className = "preset";
    b.textContent = label;
    b.title = `query ${selector}`;
    b.addEventListener("click", () => send(selector));
    els.presets.appendChild(b);
}

async function loadPresets() {
    addPreset("all sardegna", "sardegna/**");
    const cities = new Set();
    try {
        const res = await fetch("/api/channels");
        const data = await res.json();
        data.channels.forEach((c) => {
            const city = c.split("/").slice(0, -1).join("/");
            cities.add(city);
        });
        cities.forEach((city) => addPreset(`${city}/*`, `${city}/**`));
        data.channels.forEach((c) => addPreset(c, c));
    } catch (e) {
        console.error("presets", e);
    }
}

loadPresets();
connect();
