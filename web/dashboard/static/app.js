// cache-boy dashboard. Plain JS, no dependencies, no external requests.
(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const TOKEN_KEY = "cacheboy.token";
  const NS_KEY = "cacheboy.ns";
  const HIST_KEY = "cacheboy.history";

  // ---------- storage (may be unavailable) ----------
  function sget(store, k) { try { return store.getItem(k); } catch (_) { return null; } }
  function sset(store, k, v) { try { v == null ? store.removeItem(k) : store.setItem(k, v); } catch (_) {} }
  const session = (() => { try { return window.sessionStorage; } catch (_) { return null; } })();
  const local = (() => { try { return window.localStorage; } catch (_) { return null; } })();

  const state = {
    token: session ? sget(session, TOKEN_KEY) : null,
    ns: session ? sget(session, NS_KEY) || "" : "",
    tab: "overview",
    prev: null,             // previous info sample {t, cmds}
    opsSeries: [],
    memSeries: [],
    scanCursor: "0",
    scanKeys: 0,
    selectedKey: null,
    ws: null,
    loginOpen: false,
  };

  // ---------- API ----------
  class AuthError extends Error {}

  async function api(path, opts) {
    opts = opts || {};
    const headers = Object.assign({}, opts.headers || {});
    if (state.token) headers["Authorization"] = "Bearer " + state.token;
    if (state.ns) headers["X-CacheBoy-Namespace"] = state.ns;
    let res;
    try {
      res = await fetch(path, Object.assign({}, opts, { headers, cache: "no-store" }));
    } catch (e) {
      setConn(false);
      throw e;
    }
    setConn(true);
    if (res.status === 401) {
      showLogin();
      throw new AuthError("authentication required");
    }
    let body = null;
    try { body = await res.json(); } catch (_) {}
    return { status: res.status, body };
  }

  async function command(args) {
    const r = await api("/v1/command", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(args),
    });
    return r.body || { error: "HTTP " + r.status };
  }

  function setConn(ok) {
    const d = $("conn-dot");
    d.className = "dot " + (ok ? "ok" : "bad");
    d.title = ok ? "connected" : "server unreachable";
  }

  // ---------- login ----------
  function showLogin(msg) {
    const dlg = $("login");
    $("login-error").textContent = msg || (state.token ? "Wrong password." : "");
    if (!state.loginOpen) {
      state.loginOpen = true;
      if (dlg.showModal) dlg.showModal(); else dlg.setAttribute("open", "");
      $("login-input").value = "";
      $("login-input").focus();
    }
  }

  $("login-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    state.token = $("login-input").value;
    sset(session, TOKEN_KEY, state.token);
    try {
      const r = await fetch("/v1/info", { headers: { Authorization: "Bearer " + state.token } });
      if (r.status === 401) {
        $("login-error").textContent = "Wrong password.";
        return;
      }
    } catch (_) {}
    state.loginOpen = false;
    $("login").close ? $("login").close() : $("login").removeAttribute("open");
    refreshAll();
  });
  $("login").addEventListener("cancel", (ev) => ev.preventDefault());

  // ---------- helpers ----------
  function el(tag, attrs, ...children) {
    const e = document.createElement(tag);
    if (attrs) for (const k in attrs) {
      if (k === "class") e.className = attrs[k];
      else if (k === "text") e.textContent = attrs[k];
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), attrs[k]);
      else e.setAttribute(k, attrs[k]);
    }
    for (const c of children) if (c != null) e.append(c);
    return e;
  }

  function toast(msg) {
    const t = $("toast");
    t.textContent = msg;
    t.hidden = false;
    clearTimeout(toast.timer);
    toast.timer = setTimeout(() => { t.hidden = true; }, 2600);
  }

  function fmtNum(n) {
    n = Number(n);
    if (!isFinite(n)) return "–";
    if (Math.abs(n) >= 1e9) return (n / 1e9).toFixed(2) + "B";
    if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + "M";
    if (Math.abs(n) >= 1e4) return (n / 1e3).toFixed(1) + "k";
    return String(Math.round(n * 10) / 10);
  }

  function fmtBytes(n) {
    n = Number(n);
    if (!isFinite(n)) return "–";
    const u = ["B", "KB", "MB", "GB", "TB"];
    let i = 0;
    while (Math.abs(n) >= 1024 && i < u.length - 1) { n /= 1024; i++; }
    return (i ? n.toFixed(1) : n) + " " + u[i];
  }

  function fmtDuration(sec) {
    sec = Math.max(0, Math.floor(Number(sec) || 0));
    const d = Math.floor(sec / 86400), h = Math.floor(sec % 86400 / 3600), m = Math.floor(sec % 3600 / 60), s = sec % 60;
    if (d) return d + "d " + h + "h";
    if (h) return h + "h " + m + "m";
    if (m) return m + "m " + s + "s";
    return s + "s";
  }

  function fmtTTL(ms) {
    if (ms == null || ms < 0) return null;
    if (ms < 1000) return ms + "ms";
    return fmtDuration(ms / 1000);
  }

  function fmtUs(us) {
    us = Number(us);
    if (us >= 1e6) return (us / 1e6).toFixed(2) + " s";
    if (us >= 1e3) return (us / 1e3).toFixed(2) + " ms";
    return (Number.isInteger(us) ? us : us.toFixed(2)) + " µs";
  }

  // redis-cli style argument splitting ("double" with escapes, 'single').
  function splitArgs(line) {
    const out = [];
    let i = 0;
    while (true) {
      while (i < line.length && /\s/.test(line[i])) i++;
      if (i >= line.length) return out;
      let cur = "";
      if (line[i] === '"') {
        i++;
        while (true) {
          if (i >= line.length) throw new Error("unbalanced quotes");
          let c = line[i];
          if (c === '"') { i++; break; }
          if (c === "\\" && i + 1 < line.length) {
            i++;
            const n = line[i];
            if (n === "n") c = "\n";
            else if (n === "r") c = "\r";
            else if (n === "t") c = "\t";
            else if (n === "x" && /^[0-9a-fA-F]{2}$/.test(line.substr(i + 1, 2))) {
              c = String.fromCharCode(parseInt(line.substr(i + 1, 2), 16));
              i += 2;
            } else c = n;
          }
          cur += c;
          i++;
        }
      } else if (line[i] === "'") {
        i++;
        while (true) {
          if (i >= line.length) throw new Error("unbalanced quotes");
          if (line[i] === "'") { i++; break; }
          if (line[i] === "\\" && line[i + 1] === "'") i++;
          cur += line[i++];
        }
      } else {
        while (i < line.length && !/\s/.test(line[i])) cur += line[i++];
      }
      out.push(cur);
    }
  }

  // ---------- tabs ----------
  $("tabs").addEventListener("click", (ev) => {
    const b = ev.target.closest("button[data-tab]");
    if (!b) return;
    selectTab(b.dataset.tab);
  });

  function selectTab(name) {
    if (!$("tab-" + name)) name = "overview";
    state.tab = name;
    try { history.replaceState(null, "", name === "overview" ? location.pathname : "#" + name); } catch (_) {}
    for (const b of document.querySelectorAll("#tabs button")) b.classList.toggle("active", b.dataset.tab === name);
    for (const s of document.querySelectorAll(".tab")) s.classList.toggle("active", s.id === "tab-" + name);
    if (name === "keys" && !state.scanKeys) scan(true);
    if (name === "console") $("console-input").focus();
    refreshTab();
  }

  // ---------- namespace ----------
  $("ns-input").value = state.ns;
  $("ns-input").addEventListener("change", () => {
    state.ns = $("ns-input").value.trim();
    if (state.ns === "default") state.ns = "";
    sset(session, NS_KEY, state.ns || null);
    state.selectedKey = null;
    $("key-view").replaceChildren(el("div", { class: "empty", text: "Select a key to see its value." }));
    if (state.tab === "keys") scan(true);
    if (state.ws) { stopEvents(); startEvents(); }
    toast(state.ns ? "Namespace: " + state.ns : "Default namespace");
  });

  // ---------- overview ----------
  function sparkline(canvas, data, color) {
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    if (!w || !h) return;
    if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
    }
    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    if (data.length < 2) return;
    const max = Math.max(...data) || 1;
    const min = Math.min(0, ...data);
    const n = 60;
    const step = w / (n - 1);
    const x0 = w - (data.length - 1) * step;
    const y = (v) => h - 3 - ((v - min) / (max - min || 1)) * (h - 6);
    ctx.beginPath();
    data.forEach((v, i) => { i ? ctx.lineTo(x0 + i * step, y(v)) : ctx.moveTo(x0, y(v)); });
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.6;
    ctx.lineJoin = "round";
    ctx.stroke();
    ctx.lineTo(x0 + (data.length - 1) * step, h);
    ctx.lineTo(x0, h);
    ctx.closePath();
    const g = ctx.createLinearGradient(0, 0, 0, h);
    g.addColorStop(0, color + "55");
    g.addColorStop(1, color + "00");
    ctx.fillStyle = g;
    ctx.fill();
  }

  function cssVar(name) {
    const v = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
    return /^#[0-9a-f]{6}$/i.test(v) ? v : "#3e63dd";
  }

  function pushSeries(arr, v) { arr.push(v); if (arr.length > 60) arr.shift(); }

  async function refreshOverview() {
    const r = await api("/v1/info");
    if (r.status !== 200 || !r.body) return;
    const info = r.body;
    const sv = info.server || {}, st = info.stats || {}, mem = info.memory || {}, cl = info.clients || {}, ks = info.keyspace || {};

    $("c-version").textContent = sv.cacheboy_version || "–";
    $("c-version-sub").textContent = "redis " + (sv.redis_version || "?") + " · " + (sv.redis_mode || "");
    $("c-uptime").textContent = fmtDuration(sv.uptime_in_seconds);
    $("c-uptime-sub").textContent = (sv.go_version || "") + " · " + (sv.os || "");

    const now = performance.now();
    const cmds = Number(st.total_commands_processed || 0);
    if (state.prev) {
      const dt = (now - state.prev.t) / 1000;
      const ops = dt > 0 ? Math.max(0, (cmds - state.prev.cmds) / dt) : 0;
      pushSeries(state.opsSeries, ops);
      $("c-ops").textContent = fmtNum(ops);
    }
    state.prev = { t: now, cmds };

    const used = Number(mem.used_memory || 0), maxm = Number(mem.maxmemory || 0);
    pushSeries(state.memSeries, used);
    $("c-mem").textContent = fmtBytes(used) + (maxm ? " / " + fmtBytes(maxm) : "");
    const pct = maxm ? Math.min(100, used / maxm * 100) : 0;
    const bar = $("c-mem-bar");
    bar.style.width = (maxm ? pct : 0) + "%";
    bar.classList.toggle("hot", pct > 85);
    bar.parentElement.title = maxm ? pct.toFixed(1) + "% of maxmemory (" + (mem.maxmemory_policy || "") + ")" : "no maxmemory limit";

    let keys = 0, expires = 0;
    const m = /keys=(\d+)(?:,expires=(\d+))?/.exec(ks.db0 || "");
    if (m) { keys = Number(m[1]); expires = Number(m[2] || 0); }
    $("c-keys").textContent = fmtNum(keys);
    $("c-keys-sub").textContent = "expired " + fmtNum(st.expired_keys || 0) + " · evicted " + fmtNum(st.evicted_keys || 0);

    const hits = Number(st.keyspace_hits || 0), misses = Number(st.keyspace_misses || 0);
    $("c-hit").textContent = hits + misses ? (hits / (hits + misses) * 100).toFixed(1) + "%" : "–";
    $("c-hit-sub").textContent = fmtNum(hits) + " hits · " + fmtNum(misses) + " misses";

    $("c-clients").textContent = fmtNum(cl.connected_clients || 0);
    $("c-clients-sub").textContent = (cl.blocked_clients || 0) + " blocked · " + (cl.pubsub_channels || 0) + " channels";

    sparkline($("spark-ops"), state.opsSeries, cssVar("--accent-2"));
    sparkline($("spark-mem"), state.memSeries, cssVar("--accent"));

    const kv = $("server-kv");
    const rows = [
      ["Role", (info.replication || {}).role],
      ["TCP port", sv.tcp_port], ["HTTP port", sv.http_port],
      ["Shards", sv.shards], ["CPU cores", sv.cpu_cores_used],
      ["Eviction policy", mem.maxmemory_policy],
      ["Heap", mem.used_memory_heap_human],
      ["Commands processed", st.total_commands_processed],
      ["Connections received", st.total_connections_received],
      ["Network in / out", fmtBytes(st.total_net_input_bytes) + " / " + fmtBytes(st.total_net_output_bytes)],
      ["Persistence", (info.persistence || {}).aof_enabled === "1" ? "AOF" : "snapshots / none"],
      ["Run ID", sv.run_id],
    ];
    kv.replaceChildren(...rows.filter((r) => r[1] != null && r[1] !== "").flatMap(([k, v]) => [el("dt", { text: k }), el("dd", { text: String(v) })]));
  }

  // ---------- keys ----------
  $("scan-form").addEventListener("submit", (ev) => { ev.preventDefault(); scan(true); });
  $("scan-more").addEventListener("click", () => scan(false));

  async function scan(reset) {
    if (reset) {
      state.scanCursor = "0";
      state.scanKeys = 0;
      $("keylist").replaceChildren();
    }
    const pattern = $("scan-pattern").value || "*";
    const type = $("scan-type").value;
    let cursor = state.scanCursor, found = 0, rounds = 0;
    // Shards can be empty or not match: keep scanning until we have a page.
    do {
      const q = new URLSearchParams({ pattern, cursor, count: "200", type });
      const r = await api("/v1/scan?" + q);
      if (r.status !== 200) { toast((r.body && r.body.error) || "scan failed"); return; }
      for (const k of r.body.keys) { addKeyRow(k); found++; }
      cursor = r.body.cursor;
      rounds++;
    } while (cursor !== "0" && found < 200 && rounds < 64);
    state.scanCursor = cursor;
    state.scanKeys += found;
    $("scan-count").textContent = state.scanKeys + (cursor === "0" ? "" : "+") + " keys";
    $("scan-more").hidden = cursor === "0";
    if (!state.scanKeys) $("keylist").append(el("li", { class: "empty", text: "No keys match." }));
  }

  function badges(type, ttl) {
    const out = [el("span", { class: "badge t-" + type, text: type })];
    const t = fmtTTL(ttl);
    if (t) out.push(el("span", { class: "badge ttl", title: "time to live", text: "⏱ " + t }));
    return out;
  }

  function addKeyRow(k) {
    const li = el("li", { title: k.key }, el("span", { class: "k", text: k.key }), ...badges(k.type, k.ttl_ms));
    li.addEventListener("click", () => {
      for (const x of document.querySelectorAll("#keylist li.sel")) x.classList.remove("sel");
      li.classList.add("sel");
      viewKey(k.key, li);
    });
    $("keylist").append(li);
  }

  async function viewKey(key, li) {
    state.selectedKey = key;
    const view = $("key-view");
    const r = await api("/v1/keys/" + encodeURIComponent(key));
    if (state.selectedKey !== key) return;
    if (r.status === 404) {
      view.replaceChildren(el("div", { class: "empty", text: "Key no longer exists." }));
      if (li) li.remove();
      return;
    }
    if (r.status !== 200) {
      view.replaceChildren(el("div", { class: "empty error", text: (r.body && r.body.error) || "HTTP " + r.status }));
      return;
    }
    const d = r.body;
    const text = typeof d.value === "string" ? prettyMaybeJSON(d.value) : JSON.stringify(d.value, null, 2);
    const del = el("button", { class: "danger", text: "Delete" });
    del.addEventListener("click", async () => {
      if (!confirm("Delete key " + key + "?")) return;
      const res = await api("/v1/keys/" + encodeURIComponent(key), { method: "DELETE" });
      if (res.status === 200) {
        toast("Deleted " + key);
        if (li) li.remove();
        view.replaceChildren(el("div", { class: "empty", text: "Key deleted." }));
      } else toast((res.body && res.body.error) || "delete failed");
    });
    const reload = el("button", { class: "ghost", text: "Reload" });
    reload.addEventListener("click", () => viewKey(key, li));
    const size = Array.isArray(d.value) ? d.value.length + " items" :
      (d.value && typeof d.value === "object") ? Object.keys(d.value).length + " fields" :
      typeof d.value === "string" ? d.value.length + " chars" : "";
    view.replaceChildren(
      el("div", { class: "kv-head" },
        el("h3", { text: d.key }),
        ...badges(d.type, d.ttl_ms),
        el("span", { class: "muted", text: size }),
        el("span", { class: "spacer" }),
        reload, del),
      el("pre", { class: "value", text }),
    );
  }

  function prettyMaybeJSON(s) {
    const t = s.trim();
    if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))) {
      try { return JSON.stringify(JSON.parse(t), null, 2); } catch (_) {}
    }
    return s;
  }

  // ---------- console ----------
  let history = [];
  try { history = JSON.parse(sget(local, HIST_KEY) || "[]") || []; } catch (_) {}
  let histPos = history.length;

  $("console-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const input = $("console-input");
    const line = input.value.trim();
    if (!line) return;
    if (history[history.length - 1] !== line) history.push(line);
    if (history.length > 200) history = history.slice(-200);
    sset(local, HIST_KEY, JSON.stringify(history));
    histPos = history.length;
    input.value = "";
    const out = $("console-out");
    out.append(el("div", { class: "cmd", text: "> " + line }));
    if (/^clear$/i.test(line)) { out.replaceChildren(); return; }
    let args;
    try { args = splitArgs(line); } catch (e) {
      out.append(el("pre", { class: "err", text: "(error) " + e.message }));
      return;
    }
    if (!args.length) return;
    try {
      const r = await command(args);
      if ("error" in r) out.append(el("pre", { class: "err", text: "(error) " + r.error }));
      else out.append(el("pre", { text: JSON.stringify(r.result, null, 2) }));
    } catch (e) {
      if (!(e instanceof AuthError)) out.append(el("pre", { class: "err", text: String(e) }));
    }
    out.scrollTop = out.scrollHeight;
  });

  $("console-input").addEventListener("keydown", (ev) => {
    if (ev.key === "ArrowUp" && histPos > 0) {
      histPos--;
      ev.target.value = history[histPos];
      ev.preventDefault();
    } else if (ev.key === "ArrowDown") {
      histPos = Math.min(history.length, histPos + 1);
      ev.target.value = history[histPos] || "";
      ev.preventDefault();
    }
  });

  // ---------- live events ----------
  $("events-toggle").addEventListener("click", () => (state.ws ? stopEvents() : startEvents()));
  $("events-clear").addEventListener("click", () => $("events-body").replaceChildren());

  function setEventsStatus(text) { $("events-status").textContent = text; }

  function startEvents() {
    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const q = new URLSearchParams();
    if (state.token) q.set("token", state.token);
    if (state.ns) q.set("namespace", state.ns);
    let ws;
    try { ws = new WebSocket(proto + "//" + location.host + "/v1/ws" + (q.toString() ? "?" + q : "")); }
    catch (e) { setEventsStatus("not available"); return; }
    state.ws = ws;
    $("events-toggle").textContent = "Stop";
    setEventsStatus("connecting…");
    const pattern = $("events-pattern").value || "*";
    ws.onopen = () => ws.send(JSON.stringify({ id: 1, cmd: ["KEYWATCH", pattern] }));
    ws.onmessage = (m) => {
      let msg;
      try { msg = JSON.parse(m.data); } catch (_) { return; }
      if (msg.id === 1) {
        if ("error" in msg) {
          setEventsStatus("not available (" + msg.error + ")");
          stopEvents(true);
        } else setEventsStatus("watching " + pattern);
        return;
      }
      if (msg.push) onPush(msg.push);
    };
    ws.onerror = () => setEventsStatus("not available");
    ws.onclose = (ev) => {
      if (state.ws === ws) {
        state.ws = null;
        $("events-toggle").textContent = "Start";
        if (!/not available/.test($("events-status").textContent)) setEventsStatus("disconnected" + (ev.code === 1006 ? " (auth or network error)" : ""));
      }
    };
  }

  function stopEvents(keepStatus) {
    const ws = state.ws;
    state.ws = null;
    $("events-toggle").textContent = "Start";
    if (!keepStatus) setEventsStatus("stopped");
    if (ws) try { ws.close(); } catch (_) {}
  }

  // Accepts ["keyevent", key, op], ["pmessage", pat, "__key__:k", op] and ["message", "__key__:k", op].
  function onPush(p) {
    if (!Array.isArray(p) || p.length < 3 || typeof p[p.length - 1] === "number") return;
    let key, op;
    if (p[0] === "keyevent") { key = p[1]; op = p[2]; }
    else if (p[0] === "pmessage" && p.length >= 4) { key = p[2]; op = p[3]; }
    else if (p[0] === "message") { key = p[1]; op = p[2]; }
    else return;
    if (typeof key === "string" && key.startsWith("__key__:")) key = key.slice(8);
    const body = $("events-body");
    const tr = el("tr", { class: "flash" },
      el("td", { class: "mono", text: new Date().toLocaleTimeString() }),
      el("td", {}, el("span", { class: "badge", text: String(op) })),
      el("td", { class: "mono", text: String(key) }));
    body.prepend(tr);
    while (body.children.length > 300) body.lastChild.remove();
  }

  // ---------- tables ----------
  function fillTable(id, rows, empty) {
    const body = $(id);
    if (!rows.length) {
      body.replaceChildren(el("tr", {}, el("td", { class: "empty", colspan: "12", text: empty })));
      return;
    }
    body.replaceChildren(...rows);
  }

  const td = (text, cls) => el("td", { class: cls || "", text: String(text) });

  async function refreshSlowlog() {
    const r = await api("/v1/slowlog?count=128");
    if (r.status !== 200) return fillTable("slowlog-body", [], (r.body && r.body.error) || "unavailable");
    fillTable("slowlog-body", r.body.map((s) => el("tr", {},
      td(s.id, "num"),
      td(new Date(s.time * 1000).toLocaleString()),
      td(fmtUs(s.duration_us), "num"),
      td(s.args.map((a) => (/[\s"]/.test(a) || a === "" ? JSON.stringify(a) : a)).join(" "), "mono"),
      td(s.client_addr + (s.client_name ? " (" + s.client_name + ")" : ""), "mono"))), "Slowlog is empty.");
  }

  async function refreshClients() {
    const r = await api("/v1/clients");
    if (r.status !== 200) return fillTable("clients-body", [], (r.body && r.body.error) || "unavailable");
    fillTable("clients-body", r.body.map((c) => el("tr", {},
      td(c.id, "num"), td(c.addr, "mono"), td(c.kind), td(c.name || "–"), td(c.user || "–"), td(c.namespace),
      td(fmtDuration(c.age_s), "num"), td(fmtDuration(c.idle_s), "num"), td(c.cmd || "–", "mono"))), "No clients.");
  }

  async function refreshCommands() {
    const r = await api("/v1/commandstats");
    if (r.status !== 200) return fillTable("commands-body", [], (r.body && r.body.error) || "unavailable");
    const rows = r.body.slice().sort((a, b) => b.calls - a.calls);
    fillTable("commands-body", rows.map((s) => el("tr", {},
      td(s.name, "mono"), td(s.calls.toLocaleString(), "num"), td(s.failed_calls.toLocaleString(), "num"),
      td(fmtUs(s.usec), "num"), td(fmtUs(s.usec_per_call), "num"))), "No commands yet.");
  }

  // ---------- refresh loop ----------
  async function refreshTab() {
    try {
      if (state.tab === "slowlog") await refreshSlowlog();
      else if (state.tab === "clients") await refreshClients();
      else if (state.tab === "commands") await refreshCommands();
    } catch (_) {}
  }

  async function refreshAll() {
    try { await refreshOverview(); } catch (_) {}
    await refreshTab();
  }

  let ticks = 0;
  setInterval(() => {
    if (state.loginOpen || document.hidden) return;
    refreshOverview().catch(() => {});
    if (++ticks % 2 === 0) refreshTab();
  }, 2000);
  window.addEventListener("resize", () => {
    sparkline($("spark-ops"), state.opsSeries, cssVar("--accent-2"));
    sparkline($("spark-mem"), state.memSeries, cssVar("--accent"));
  });

  if (location.hash.length > 1) selectTab(location.hash.slice(1));
  refreshAll();
})();
