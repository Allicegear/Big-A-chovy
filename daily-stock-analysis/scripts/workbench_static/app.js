/* A股 Web 工作台前端逻辑（无外部依赖） */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

async function fetchJSON(url, opts) {
  const resp = await fetch(url, opts);
  return resp.json();
}

async function fetchText(url) {
  const resp = await fetch(url);
  return resp.text();
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  }[c]));
}

/* ---- 极简 Markdown 渲染：标题/表格/引用/列表/分隔线/行内加粗代码 ---- */
function inline(s) {
  return esc(s)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+)`/g, "<code>$1</code>");
}

function renderMD(md) {
  const lines = md.split(/\r?\n/);
  const out = [];
  let i = 0;
  while (i < lines.length) {
    const line = lines[i];
    if (line.startsWith("|") && i + 1 < lines.length && /^\|[\s:|-]+\|?$/.test(lines[i + 1].trim())) {
      // GFM 表格
      const header = line.split("|").slice(1, -1).map((c) => c.trim());
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].startsWith("|")) {
        rows.push(lines[i].split("|").slice(1, -1).map((c) => c.trim()));
        i++;
      }
      let html = "<table><thead><tr>";
      for (const h of header) html += `<th>${inline(h)}</th>`;
      html += "</tr></thead><tbody>";
      for (const r of rows) {
        html += "<tr>";
        for (let c = 0; c < header.length; c++) {
          let cell = r[c] ?? "";
          // A股红涨绿跌：+x% 红、-x% 绿
          if (/^\+?\d/.test(cell) && cell.includes("%")) {
            const cls = cell.trim().startsWith("-") ? "down" : "up";
            cell = `<span class="${cls}">${inline(cell)}</span>`;
          } else {
            cell = inline(cell);
          }
          html += `<td>${cell}</td>`;
        }
        html += "</tr>";
      }
      html += "</tbody></table>";
      out.push(html);
      continue;
    }
    if (/^###\s/.test(line)) { out.push(`<h4>${inline(line.slice(4))}</h4>`); }
    else if (/^##\s/.test(line)) { out.push(`<h3>${inline(line.slice(3))}</h3>`); }
    else if (/^#\s/.test(line)) { out.push(`<h2>${inline(line.slice(2))}</h2>`); }
    else if (/^(-{3,}|\*{3,})$/.test(line.trim())) { out.push("<hr>"); }
    else if (/^>\s?/.test(line)) { out.push(`<blockquote>${inline(line.replace(/^>\s?/, ""))}</blockquote>`); }
    else if (/^[-*]\s/.test(line)) { out.push(`<li>${inline(line.slice(2))}</li>`); }
    else if (line.trim()) { out.push(`<p>${inline(line)}</p>`); }
    i++;
  }
  return out.join("\n");
}

function renderJSON(obj) {
  return `<pre>${esc(JSON.stringify(obj, null, 2))}</pre>`;
}

/* ---- 通用工具调用 ---- */
async function callTool(btn, outSel, fn) {
  const btnText = btn.textContent;
  btn.disabled = true;
  btn.textContent = "查询中...";
  const out = $(outSel);
  out.innerHTML = `<pre>查询中...</pre>`;
  try {
    const data = await fn();
    if (data && data.error) {
      out.innerHTML = `<pre class="err">${esc(JSON.stringify(data, null, 2))}</pre>`;
    } else {
      out.innerHTML = data && data._html ? data._html : renderJSON(data);
    }
  } catch (e) {
    out.innerHTML = `<pre class="err">请求失败: ${esc(e.message || e)}</pre>`;
  } finally {
    btn.disabled = false;
    btn.textContent = btnText;
  }
}

/* ================= Tab 切换 ================= */
$$(".tab-btn[data-tab]").forEach((btn) => {
  btn.addEventListener("click", () => {
    $$(".tab-btn").forEach((b) => b.classList.remove("active"));
    btn.classList.add("active");
    $$(".tab-panel").forEach((p) => p.classList.remove("active"));
    $(`#tab-${btn.dataset.tab}`).classList.add("active");
    if (btn.dataset.tab === "reports") loadReports();
    if (btn.dataset.tab === "config") loadConfig();
  });
});

/* ================= 筛选工作台 ================= */
let pollTimer = null;

$("#run-btn").addEventListener("click", async () => {
  const modes = [];
  if ($("#mode-strict").checked) modes.push("strict");
  if ($("#mode-low").checked) modes.push("low");
  if ($("#mode-watchlist").checked) modes.push("watchlist");
  if (!modes.length) { alert("请至少选择一个筛选模块"); return; }
  // 与看板顶部同名选项保持同一极性：勾选 = 执行检查/排名，取消 = 跳过。
  // 公告检查不在此列：它是框架一票否决门禁，服务端强制开启，没有关闭入口。
  const body = {
    modes,
    top: parseInt($("#top").value, 10) || 15,
    network_mode: $("#network-mode").value,
    skip_capital_ranking: !$("#rank-capital").checked,
  };
  try {
    const res = await fetchJSON("/api/wb/screen/run", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (res.status === "started") {
      SharedUI.notice($("#action-notice"), "");
      $("#screen-result").classList.add("hidden");
      $("#screen-summary").classList.add("hidden");
      startPolling();
    } else {
      // 被另一个入口占用：说清原因，不能点完没反应。
      $("#job-state").textContent = res.reason || "任务已在运行中";
      SharedUI.notice($("#action-notice"), res.reason || "有筛选任务正在运行，请稍后再试");
    }
  } catch (e) {
    $("#job-state").textContent = `启动失败: ${e.message || e}`;
    SharedUI.notice($("#action-notice"), `启动失败：${e.message || e}`, "bad");
  }
});

function startPolling() {
  $("#run-btn").disabled = true;
  $("#job-progress").classList.remove("hidden");
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(pollJob, 1500);
  pollJob();
}

async function pollJob() {
  let job;
  try { job = await fetchJSON("/api/wb/job"); } catch { return; }
  if (job.state === "running") {
    $("#job-state").textContent = `筛选中... 已用 ${job.elapsed ?? 0}s`;
    $("#job-progress-hint").style.display = "block";
    return;
  }
  clearInterval(pollTimer);
  pollTimer = null;
  $("#job-progress").classList.add("hidden");
  $("#job-progress-hint").style.display = "none";
  $("#run-btn").disabled = false;
  if (job.state === "error") {
    $("#job-state").innerHTML = `<span class="err-text">失败: ${esc(job.error)}</span>`;
    return;
  }
  $("#job-state").textContent = `完成，耗时 ${job.elapsed}s`;
  if (job.timestamp) {
    const degraded = job.degraded ? " <span class='down'>⚠️ 数据降级</span>" : "";
    $("#screen-summary").innerHTML =
      `<div>数据时间: <strong>${esc(job.timestamp)}</strong>${degraded} ` +
      `｜ 报告已保存: <code>${esc(job.md_path || "")}</code></div>`;
    $("#screen-summary").classList.remove("hidden");
  }
  try {
    const md = await fetchText("/api/wb/report");
    $("#screen-result").innerHTML = renderMD(md);
    $("#screen-result").classList.remove("hidden");
  } catch (e) {
    $("#screen-result").innerHTML = `<p class="err-text">报告获取失败: ${esc(e.message || e)}</p>`;
    $("#screen-result").classList.remove("hidden");
  }
}

/* ================= 报告库 ================= */
async function loadReports() {
  const data = await fetchJSON("/api/wb/reports");
  $("#reports-count").textContent = `共 ${data.count} 份`;
  const rows = (data.files || []).map((f) =>
    `<tr><td><a data-path="${esc(f.path)}">${esc(f.name)}</a></td>` +
    `<td>${esc(f.mtime)}</td><td>${(f.size / 1024).toFixed(1)} KB</td></tr>`).join("");
  $("#reports-table").innerHTML =
    `<table class="data"><thead><tr><th>文件名</th><th>修改时间</th><th>大小</th></tr></thead>` +
    `<tbody>${rows || '<tr><td colspan="3">暂无报告</td></tr>'}</tbody></table>`;
  $$("#reports-table a[data-path]").forEach((a) => {
    a.addEventListener("click", () => openReport(a.dataset.path, a.textContent));
  });
}
$("#reports-refresh").addEventListener("click", loadReports);

async function openReport(path, name) {
  const md = await fetchText(`/api/wb/md?path=${encodeURIComponent(path)}`);
  $("#reports-table").closest(".card").classList.add("hidden");
  $("#report-viewer").classList.remove("hidden");
  $("#report-title").textContent = name;
  $("#report-content").innerHTML = renderMD(md);
}
$("#report-back").addEventListener("click", () => {
  $("#report-viewer").classList.add("hidden");
  $("#reports-table").closest(".card").classList.remove("hidden");
});

/* ================= 工具箱 ================= */
$("#quote-btn").addEventListener("click", () => {
  callTool($("#quote-btn"), "#quote-out", () => {
    const codes = $("#quote-codes").value.trim();
    const m = $("#quote-minute").checked ? 1 : 0;
    const k = $("#quote-kline").checked ? 1 : 0;
    return fetchJSON(`/api/wb/quote?codes=${encodeURIComponent(codes)}&minute=${m}&kline=${k}`);
  });
});

$("#fin-btn").addEventListener("click", () => {
  callTool($("#fin-btn"), "#fin-out", () =>
    fetchJSON(`/api/wb/financials?code=${encodeURIComponent($("#fin-code").value.trim())}`));
});

/* ================= 证据核验舱 ================= */
const evidenceLabels = {
  ticks: "腾讯分笔",
  financials: "已披露财务",
  events: "隔夜事件",
  themes: "题材/主题",
  news: "个股新闻",
  interaction: "公司问答",
  dragon_tiger: "龙虎榜",
  commodity: "商品背景",
};

function evidenceLinks(value) {
  const links = [];
  const walk = (item) => {
    if (!item || links.length >= 6) return;
    if (Array.isArray(item)) { item.forEach(walk); return; }
    if (typeof item !== "object") return;
    Object.entries(item).forEach(([key, child]) => {
      if ((key === "url" || key === "source_url" || key === "link") && typeof child === "string" && /^https?:\/\//i.test(child)) {
        if (!links.some((x) => x === child)) links.push(child);
      } else walk(child);
    });
  };
  walk(value);
  return links;
}

function evidenceCard(topic, payload) {
  const status = payload?.status || (payload?.error ? "unavailable" : "ok");
  const statusClass = status === "ok" ? "evidence-ok" : (status === "empty" ? "evidence-empty" : "evidence-bad");
  const warnings = (payload?.warnings || []).map((w) => `<div class="evidence-warning">⚠ ${esc(w)}</div>`).join("");
  const links = evidenceLinks(payload).map((url) => `<a href="${esc(url)}" target="_blank" rel="noreferrer noopener">打开来源</a>`).join(" · ");
  const detail = payload?.data ?? payload;
  return `<article class="evidence-card-row ${statusClass}">
    <div class="evidence-row-head"><strong>${esc(evidenceLabels[topic] || topic)}</strong><span class="evidence-status">${esc(status)}</span><span class="evidence-source">${esc(payload?.source || "本地组合")}</span></div>
    <div class="evidence-meta">时点 ${esc(payload?.as_of || payload?.data_date || "-")} ${links ? `· ${links}` : ""}</div>
    ${payload?.error ? `<div class="evidence-error">${esc(payload.error.message || JSON.stringify(payload.error))}</div>` : ""}
    ${warnings}
    <details><summary>展开原始证据摘要</summary><pre>${esc(JSON.stringify(detail, null, 2))}</pre></details>
  </article>`;
}

async function fetchEvidenceTopic(topic, code, date, force) {
  const q = `code=${encodeURIComponent(code)}${date ? `&date=${encodeURIComponent(date)}` : ""}${force ? "&force=1" : ""}`;
  if (topic === "ticks") return fetchJSON(`/api/wb/ticks?${q}`);
  if (topic === "financials") return fetchJSON(`/api/wb/financials?code=${encodeURIComponent(code)}`);
  if (topic === "events") return fetchJSON(`/api/wb/events?${q}`);
  return fetchJSON(`/api/wb/context?${q}&topic=${encodeURIComponent(topic)}`);
}

$("#evidence-btn").addEventListener("click", async () => {
  const button = $("#evidence-btn");
  const code = $("#evidence-code").value.trim();
  const date = $("#evidence-date").value.trim();
  const force = $("#evidence-force").checked;
  const topics = $$('input[name="evidence-topic"]:checked').map((el) => el.value);
  const out = $("#evidence-out");
  if (!code) { out.innerHTML = `<div class="evidence-error">请先输入股票代码。</div>`; return; }
  if (!topics.length) { out.innerHTML = `<div class="evidence-error">至少选择一个证据主题。</div>`; return; }
  button.disabled = true;
  button.textContent = "核验中...";
  out.innerHTML = `<div class="evidence-loading">正在按主题独立查询，失败不会伪装为空…</div>`;
  const results = await Promise.all(topics.map(async (topic) => {
    try { return [topic, await fetchEvidenceTopic(topic, code, date, force)]; }
    catch (e) { return [topic, { status: "unavailable", error: { message: e.message || String(e) } }]; }
  }));
  out.innerHTML = results.map(([topic, payload]) => evidenceCard(topic, payload)).join("");
  button.disabled = false;
  button.textContent = "核验证据";
});

$("#calendar-btn").addEventListener("click", () => {
  const date = $("#evidence-date").value.trim() || new Date().toISOString().slice(0, 10);
  callTool($("#calendar-btn"), "#evidence-out", () => fetchJSON(`/api/wb/calendar?date=${encodeURIComponent(date)}&action=is_open`));
});

$("#sentiment-btn").addEventListener("click", () => {
  const date = $("#evidence-date").value.trim();
  callTool($("#sentiment-btn"), "#evidence-out", () => fetchJSON(`/api/wb/sentiment${date ? `?date=${encodeURIComponent(date)}` : ""}`));
});

$("#scan-btn").addEventListener("click", () => {
  callTool($("#scan-btn"), "#scan-out", () => {
    const date = $("#scan-date").value.trim();
    const latest = parseInt($("#scan-latest").value, 10) || 5;
    const q = date ? `date=${encodeURIComponent(date)}&latest=${latest}` : `latest=${latest}`;
    return fetchJSON(`/api/wb/scan?${q}`);
  });
});

$("#pos-btn").addEventListener("click", () => {
  callTool($("#pos-btn"), "#pos-out", () => {
    const date = $("#pos-date").value.trim();
    return fetchJSON(`/api/wb/position${date ? `?date=${encodeURIComponent(date)}` : ""}`);
  });
});

$("#t1-btn").addEventListener("click", () => {
  callTool($("#t1-btn"), "#t1-out", () =>
    fetchJSON(`/api/wb/verify_t1?date=${encodeURIComponent($("#t1-date").value.trim())}`));
});

$("#track-btn").addEventListener("click", () => {
  callTool($("#track-btn"), "#track-out", () => {
    const code = $("#track-code").value.trim();
    const date = $("#track-date").value.trim();
    return fetchJSON(`/api/wb/track?code=${encodeURIComponent(code)}${date ? `&date=${encodeURIComponent(date)}` : ""}`);
  });
});

/* ================= 参数配置 ================= */
let configState = null;

function selectedView() {
  const el = document.querySelector('input[name="neg-view"]:checked');
  return el ? el.value : "strict";
}

async function loadConfig() {
  let cfg;
  try {
    cfg = await fetchJSON("/api/config");
  } catch (e) {
    const box = $("#config-error");
    box.textContent = `配置读取失败：${e.message || e}`;
    box.classList.remove("hidden");
    return;
  }
  configState = cfg;
  const d = cfg.dashboard || {};
  $$('input[name="neg-view"]').forEach((el) => {
    el.checked = el.value === (d.negative_super_view || "strict");
  });
  $("#cfg-top").value = d.top != null ? d.top : 15;
  $("#cfg-interval").value = d.interval != null ? d.interval : 90;
  $("#cfg-network").value = d.network_mode || "auto";
  // 交易板范围：默认只选沪深主板（与历史基线一致）
  const savedBoards = Array.isArray(d.enabled_boards) && d.enabled_boards.length ? d.enabled_boards : ["main"];
  $("#cfg-board-main").checked = savedBoards.indexOf("main") !== -1;
  $("#cfg-board-chinext").checked = savedBoards.indexOf("chinext") !== -1;
  $("#cfg-board-star").checked = savedBoards.indexOf("star") !== -1;

  const snapRev = cfg.snapshot_revision;
  const snapView = cfg.snapshot_view === "observe" ? "观察开启" : (cfg.snapshot_view ? "严格展示" : "待生成");
  // 交易板范围：把「当前设置」与「当前快照实际范围」并排显示——两者可能不同，
  // 设置是对下一轮生效，快照是已经跑完那一轮的真实范围。
  const boardText = (list) => {
    const labels = { main: "沪深主板", chinext: "创业板", star: "科创板" };
    return Array.isArray(list) && list.length ? list.map((b) => labels[b] || b).join(" + ") : null;
  };
  const curBoards = boardText(d.enabled_boards) || "沪深主板";
  const snapBoards = cfg.snapshot_enabled_boards == null
    ? null
    : (boardText(cfg.snapshot_enabled_boards) || "（空）");
  // 快照口径：新快照带 board_scope_status（所选交易板统一参与正式筛选）；旧快照带
  // extended_board_observations（当时两板只进观察列表）。两者筛选方式不同，必须提示，
  // **不能**把旧快照的范围结论按当前设置重新解释。
  const legacyMethod = cfg.snapshot_screen_method === "legacy_extended_observation";
  const scopeStatus = cfg.snapshot_board_scope_status;
  const scopeText = scopeStatus == null
    ? (legacyMethod ? "旧口径（两板仅观察，不参与正式池）" : "范围未记录（该快照生成于交易板口径之前）")
    : scopeStatus !== "ok"
      ? `本轮结果不完整（${scopeStatus === "degraded" ? "行情降级" : "行情快照不完整"}）——无候选不等于未符合条件`
      : "数据完整";
  // 设置与快照范围不一致时（含旧口径快照），并排显示容易看漏，这里再明确提示一次。
  const curList = (d.enabled_boards || []).join(",");
  const snapList = (cfg.snapshot_enabled_boards || []).join(",");
  const methodWarn = legacyMethod
    ? `<div class="scope-warn">⚠️ 当前快照由旧口径生成：当时创业板/科创板只进观察列表，未参与正式池。当前设置已是「所选交易板统一参与正式筛选」，两者不可直接比较；如需同口径结果，请重新跑一轮筛选。</div>`
    : (cfg.snapshot_enabled_boards != null && curList !== snapList
        ? `<div class="scope-warn">⚠️ 当前设置的范围（${esc(curBoards)}）与当前快照实际范围（${esc(snapBoards)}）不同：设置改动只对下一轮筛选生效，看板仍显示旧快照。</div>`
        : "");

  $("#config-status").innerHTML =
    `<div>当前设置：<strong>v${cfg.revision}</strong>` +
    `${cfg.updated_at ? `（修改于 ${esc(cfg.updated_at)}）` : ""}` +
    `｜交易板范围：<strong>${esc(curBoards)}</strong></div>` +
    `<div>当前快照：<strong>${snapRev != null ? "v" + snapRev : "待生成"}</strong>` +
    `（观察模式：${esc(snapView)}｜实际范围：${snapBoards == null ? "范围未记录" : esc(snapBoards)}` +
    `｜口径：${esc(scopeText)}）</div>` +
    methodWarn +
    (cfg.pending
      ? `<div class="pending">新配置 v${cfg.revision} 待下一轮生效（当前快照仍按 v${snapRev} 的范围）</div>`
      : "");

  const err = $("#config-error");
  if (cfg.error) {
    err.textContent = `配置告警：${cfg.error}`;
    err.classList.remove("hidden");
  } else {
    err.classList.add("hidden");
    err.textContent = "";
  }

  // 影响数量仅基于有时间标记的完整快照计算
  $("#config-impact").textContent =
    cfg.affected_count == null || cfg.affected_status !== "ok"
      ? "影响数量：待下一轮筛选（当前没有可比的完整负超单快照数据）"
      : `影响数量：按最近完整快照，超大单为负标的 ${cfg.affected_count} 只`;
}

function selectedBoards() {
  const boards = [];
  if ($("#cfg-board-main").checked) boards.push("main");
  if ($("#cfg-board-chinext").checked) boards.push("chinext");
  if ($("#cfg-board-star").checked) boards.push("star");
  return boards;
}

function validateConfigInput() {
  const top = parseInt($("#cfg-top").value, 10);
  const interval = parseInt($("#cfg-interval").value, 10);
  if (!Number.isFinite(top) || top < 3 || top > 50) return "输出条数必须是 3~50 的整数";
  if (!Number.isFinite(interval) || interval < 10 || interval > 600) return "刷新间隔必须是 10~600 的整数";
  if (!selectedBoards().length) return "至少选择一个交易板";
  return null;
}

async function applyConfig() {
  const invalid = validateConfigInput();
  const state = $("#config-apply-state");
  if (invalid) {
    state.innerHTML = `<span class="err-text">${esc(invalid)}</span>`;
    return;
  }
  const body = {
    revision: configState ? configState.revision : undefined,
    dashboard: {
      negative_super_view: selectedView(),
      // 交易板范围必须随每次提交一起发出：整对象提交时漏传该字段会把它重置成默认（仅主板），
      // 用户改了别的配置却悄悄丢掉筛选范围。
      enabled_boards: selectedBoards(),
      top: parseInt($("#cfg-top").value, 10),
      interval: parseInt($("#cfg-interval").value, 10),
      network_mode: $("#cfg-network").value,
    },
  };
  const btn = $("#config-apply");
  btn.disabled = true;
  try {
    const resp = await fetch("/api/config/apply", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const res = await resp.json();
    if (res.status === "ok") {
      const running = configState && configState.running;
      state.textContent = running ? "已保存；正在筛选，新配置待下一轮生效" : "已保存，下一轮筛选生效";
      await loadConfig();
    } else {
      state.innerHTML = `<span class="err-text">${esc((res.errors || []).join("；"))}</span>`;
      await loadConfig();
    }
  } catch (e) {
    state.innerHTML = `<span class="err-text">应用失败：${esc(e.message || e)}</span>`;
  } finally {
    btn.disabled = false;
  }
}

$("#config-apply").addEventListener("click", applyConfig);

/* ================= 服务器状态与共用运行状态条 ================= */
async function pollSharedStatus() {
  const el = $("#server-status");
  let status;
  try {
    status = await SharedUI.fetchStatus();
  } catch (error) {
    el.textContent = "连接断开";
    el.className = "status-badge offline";
    return;
  }
  el.textContent = "服务正常";
  el.className = "status-badge online";
  // 数据时点/数据源/自动刷新/引擎占用/告警旗标：与实时看板同一实现、同一措辞
  SharedUI.render($("#shared-status"), status);

  const state = SharedUI.ownerState(status);
  const btn = $("#run-btn");
  if (!state.canStartScreening && !btn.dataset.busyLocked) {
    btn.dataset.busyLocked = "1";
    btn.disabled = true;
    btn.title = state.busyReason || "已有任务在运行，请稍后再试";
  } else if (state.canStartScreening && btn.dataset.busyLocked) {
    delete btn.dataset.busyLocked;
    btn.disabled = false;
    btn.title = "";
  }

  // 本次任务 vs 全局口径：把两个作用域并排说清，避免改错地方。
  const settings = status.settings || {};
  $("#dashboard-options-hint").textContent =
    `看板自动刷新当前：公告检查 强制开启 · 资金排名 ${settings.skip_capital_ranking ? "关" : "开"}`;
}
SharedUI.startPolling(pollSharedStatus, 5000);

setInterval(() => {
  $("#clock").textContent = new Date().toLocaleTimeString("zh-CN", { hour12: false });
}, 1000);

// 看板顶部的口径入口用 /workbench#config 直接落到参数配置页
if (window.location.hash === "#config") {
  const btn = document.querySelector('.tab-btn[data-tab="config"]');
  if (btn) btn.click();
}
