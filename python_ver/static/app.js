const MAX_LINES = 500;
const MAX_POINTS = 200;
const FIELDS = ["wind_dir", "wind_speed", "humidity", "temperature", "air_pressure", "height"];

const els = {
    status: document.getElementById("status"),
    producers: document.getElementById("producers"),
    receivers: document.getElementById("receivers"),
    results: document.getElementById("results"),
    input: document.getElementById("input"),
    send: document.getElementById("send"),
    presets: document.getElementById("presets"),
    metric: document.getElementById("metric"),
    chart: document.getElementById("chart"),
    toggle: document.getElementById("toggle"),
};

let socket = null;
let running = false;
const history = {};

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

function setStatus(online) {
    els.status.textContent = online ? "online" : "offline";
    els.status.className = `status ${online ? "online" : "offline"}`;
}

function setRunning(state) {
    running = state;
    els.toggle.textContent = state ? "stop" : "start";
    els.toggle.className = `toggle ${state ? "running" : "stopped"}`;
}

const chart = echarts.init(els.chart);

function baseOption() {
    return {
        backgroundColor: "transparent",
        textStyle: { fontFamily: "monospace", color: "#7d8795" },
        tooltip: { trigger: "axis" },
        legend: { top: 0, textStyle: { color: "#7d8795" } },
        grid: { left: 55, right: 15, top: 30, bottom: 25 },
        xAxis: { type: "time", axisLabel: { color: "#7d8795" }, axisLine: { lineStyle: { color: "#2a313c" } } },
        yAxis: {
            type: "value",
            scale: true,
            axisLabel: { color: "#7d8795" },
            splitLine: { lineStyle: { color: "#2a313c" } },
        },
        series: [],
    };
}

function renderChart() {
    const metric = els.metric.value;
    const series = Object.keys(history)
        .sort()
        .map((channel) => ({
            name: channel,
            type: "line",
            showSymbol: false,
            data: history[channel].map((r) => [r.timestamp * 1000, r[metric]]),
        }));
    chart.setOption({ series }, { replaceMerge: ["series"] });
}

function record(channel, payload) {
    const row = parse(payload);
    if (!row) return;
    if (!history[channel]) history[channel] = [];
    const buf = history[channel];
    buf.push(row);
    while (buf.length > MAX_POINTS) buf.shift();
    renderChart();
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
        else if (msg.kind === "receiver") {
            append("receivers", msg, "receiver");
            record(msg.channel, msg.payload);
        } else if (msg.kind === "query") {
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
    b.title = selector;
    b.addEventListener("click", () => send(selector));
    els.presets.appendChild(b);
}

async function loadPresets() {
    els.presets.innerHTML = "";
    addPreset("all sardegna", "sardegna/**");
    const cities = new Set();
    try {
        const res = await fetch("/api/channels");
        const data = await res.json();
        data.channels.forEach((c) => cities.add(c.split("/").slice(0, -1).join("/")));
        cities.forEach((city) => addPreset(`${city}/*`, `${city}/**`));
        data.channels.forEach((c) => addPreset(c, c));
    } catch (e) {
        console.error("presets", e);
    }
}

function initMetrics() {
    FIELDS.forEach((f) => {
        const o = document.createElement("option");
        o.value = f;
        o.textContent = f;
        els.metric.appendChild(o);
    });
    els.metric.value = "temperature";
    els.metric.addEventListener("change", () => {
        chart.setOption(baseOption());
        renderChart();
    });
}

els.toggle.addEventListener("click", async () => {
    els.toggle.disabled = true;
    try {
        const res = await fetch(running ? "/api/stop" : "/api/start", { method: "POST" });
        const data = await res.json();
        setRunning(data.running);
        if (data.running) loadPresets();
    } finally {
        els.toggle.disabled = false;
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

initMetrics();
chart.setOption(baseOption());
window.addEventListener("resize", () => chart.resize());
initStatus();
loadPresets();
connect();
