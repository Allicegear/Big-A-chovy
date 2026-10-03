/* 实时看板 / 筛选工作台 共用层：状态读取与渲染
 *
 * 设计约束（与 docs/web-workbench.md 的「并发与安全设计」一致）：
 *   - 两个入口共用一把筛选锁，同一时刻只有一个引擎在跑；状态条要能回答
 *     「现在是谁在跑」，所以占用判定只在这里实现一次。
 *   - 状态条的措辞是唯一的：不要在各页 app.js 里另写一套说法。
 *
 * 数据来源：/api/status。本线没有 screening_owner 字段，占用方由
 * is_running / is_prewarming 推导（看板自动刷新 / K线预热）。
 */
(function (global) {
  "use strict";

  var OWNER_TEXT = {
    dashboard: "看板自动刷新",
    prewarm: "K线预热",
  };

  function esc(value) {
    return String(value === null || value === undefined ? "" : value).replace(/[&<>"']/g, function (ch) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch];
    });
  }

  function text(value, fallback) {
    if (value === null || value === undefined || value === "") return fallback === undefined ? "-" : fallback;
    return String(value);
  }

  function clock(value) {
    var parts = text(value, "").split(" ");
    return parts.length > 1 ? parts[1] : text(value, "");
  }

  function duration(seconds) {
    var total = Number(seconds);
    if (!isFinite(total) || total < 0) return "";
    if (total < 60) return total + "s";
    return Math.floor(total / 60) + "m" + String(Math.floor(total % 60)).padStart(2, "0") + "s";
  }

  /** 占用状态：谁在跑，以及能否发起新任务。 */
  function ownerState(status) {
    var st = status || {};
    var owner = st.is_prewarming ? "prewarm" : (st.is_running ? "dashboard" : null);
    return {
      owner: owner,
      label: owner ? OWNER_TEXT[owner] || owner : null,
      running: Boolean(owner),
      busyReason: owner ? (OWNER_TEXT[owner] || owner) + "正在运行，请等它结束后再试" : null,
      canStartScreening: !owner,
    };
  }

  /** 自动刷新列。 */
  function autoRefreshState(status) {
    var st = status || {};
    var settings = st.settings || {};
    var state = ownerState(st);
    if (!settings.auto_refresh) return { text: "已关闭（仅手动触发）", tone: "" };
    if (state.owner === "prewarm") return { text: "等待K线预热完成", tone: "is-warn" };
    if (state.owner) return { text: "本轮进行中", tone: "" };
    if (st.is_trading_hours) {
      return { text: "开启 · 下轮 " + (clock(st.next_refresh_time) || "即将"), tone: "" };
    }
    if (st.next_is_trading_open) {
      return { text: "盘后待机 · 下次开盘 " + (clock(st.next_refresh_time) || "-"), tone: "" };
    }
    return { text: "盘后待机", tone: "" };
  }

  /** 引擎占用列。 */
  function occupancyState(status) {
    var st = status || {};
    var state = ownerState(st);
    if (!state.owner) return { text: "空闲", tone: "is-ok" };
    if (state.owner === "prewarm") {
      var progress = st.prewarm_progress || {};
      var failed = progress.failed ? "（" + progress.failed + " 失败）" : "";
      return { text: "K线预热 " + (progress.done || 0) + "/" + (progress.total || 0) + failed, tone: "is-warn" };
    }
    return { text: state.label + "（本轮进行中）", tone: "is-warn" };
  }

  /** 状态条右侧旗标：只列会影响判读的异常与门禁变化。 */
  function flagList(status) {
    var st = status || {};
    var settings = st.settings || {};
    var flags = [];
    if (st.data_mode === "degraded") flags.push({ text: "降级数据", tone: "bad", title: "行情源降级：部分字段缺失，不作为完整判定依据" });
    else if (st.data_mode === "snapshot") flags.push({ text: "最近快照", tone: "warn", title: "非交易时段或数据源异常，展示最近一次完整筛选结果" });
    if (st.market_fetch_complete === false) {
      flags.push({ text: "行情不完整（缺 " + ((st.failed_pages || []).length) + " 页）", tone: "warn", title: "东财部分分页失败，本轮为局部快照" });
    }
    if (st.em_in_cooldown) flags.push({ text: "东财冷却中", tone: "warn", title: "东财入口被限流，冷却结束前不参与请求轮换" });
    if (st.proxy_unavailable) flags.push({ text: "代理断开", tone: "bad", title: "本机代理不可用，行情可能取不到" });
    // 防呆：公告检查是一票否决门禁。判据是"这份快照实际有没有跳过公告检查"
    // （announcement_check_skipped，由结果 meta 记录、/api/status 暴露），
    // 不是"当前设置"——设置与快照执行口径是两回事，不能混为一谈。
    if (st.announcement_check_skipped) {
      flags.push({ text: "本快照公告检查已跳过", tone: "bad", title: "本快照公告检查已跳过，本轮结论不可作为真实仓依据" });
    }
    if (settings.skip_capital_ranking) flags.push({ text: "资金排名已关闭", tone: "warn", title: "未做资金排序，候选按其他条件排序" });
    // 交易板范围：同样跟随**快照**而不是当前设置。范围含扩展板时必须可见，
    // 便于把本轮结果与范围对应起来；旧快照没有该字段时如实标「范围未记录」。
    var boards = st.enabled_boards;
    var scopeNote = st.board_scope_note;
    if (boards === null || boards === undefined) {
      flags.push({ text: "范围未记录", tone: "warn", title: "该快照生成时还没有交易板范围口径（旧版本产物），无法追溯本轮实际筛选范围" });
    } else if (boards.length && !(boards.length === 1 && boards[0] === "main")) {
      flags.push({
        text: "范围：" + (st.enabled_boards_label || boards.join(" + ")),
        tone: "info",
        title: scopeNote || "本轮筛选的交易板范围；所选交易板统一参与正式筛选（同一门槛、同一排名、同一条状态机）",
      });
    }
    // 范围结论：必须能区分「范围内没有符合条件的候选」与「本轮数据不可用」。
    // 一律按**快照**的结论显示，不按当前设置推断。范围默认（仅主板）时也显示，
    // 否则用户会把“数据不可用”误读成“今天没候选”。
    if (st.board_scope_status && st.board_scope_status !== "ok") {
      flags.push({
        text: "本轮范围内结果不可用",
        tone: "bad",
        title: scopeNote || "行情降级或快照不完整：本轮没有候选**不等于**范围内没有符合条件的标的",
      });
    } else if (st.board_scope_status === "ok" && st.board_scope_candidates === 0) {
      flags.push({
        text: "范围内无符合条件的候选",
        tone: "info",
        title: scopeNote || "数据完整，本轮交易板范围内没有符合现有门槛的候选（属正常筛选结果，不是数据问题）",
      });
    }
    return flags;
  }

  function item(label, value, tone, title) {
    return '<div class="ss-item"><span class="ss-label">' + esc(label) + '</span>' +
      '<span class="ss-value' + (tone ? " " + tone : "") + '"' +
      (title ? ' title="' + esc(title) + '"' : "") + ">" + esc(value) + "</span></div>";
  }

  /**
   * 渲染状态条。两页调用方式一致，唯一差别是 variant:
   *   - "dashboard"：看板顶部（其后仍有涨跌/指数各行）
   *   - "workbench"：工作台顶部（同一槽位，措辞相同）
   */
  function render(element, status, options) {
    if (!element) return;
    var st = status || {};
    var opts = options || {};
    var refresh = autoRefreshState(st);
    var occupancy = occupancyState(st);
    var html = "";
    html += item("数据时点", text(st.data_timestamp), "", "最近一次筛选使用的行情时点；手动任务运行时会更新为本次时点");
    html += item("数据源", text(st.data_source), "", "最近一次筛选实际使用的行情源");
    html += item("自动刷新", refresh.text, refresh.tone);
    html += item("引擎占用", occupancy.text, occupancy.tone);
    var flags = flagList(st).map(function (flag) {
      return '<span class="ss-flag ' + flag.tone + '" title="' + esc(flag.title) + '">' + esc(flag.text) + "</span>";
    }).join("");
    html += '<div class="ss-flags">' + flags + "</div>";
    element.innerHTML = html;
    if (typeof opts.onRendered === "function") opts.onRendered(st, ownerState(st));
  }

  /** 统一的拒绝提示：动作被占用挡住时必须让用户看到原因。 */
  function notice(element, message, tone) {
    if (!element) return;
    if (!message) {
      element.classList.add("hidden");
      element.innerHTML = "";
      return;
    }
    element.className = "ss-notice" + (tone ? " " + tone : "");
    element.innerHTML = esc(message) + '<button type="button" aria-label="关闭提示">✕</button>';
    element.querySelector("button").addEventListener("click", function () {
      element.classList.add("hidden");
    });
  }

  function fetchStatus() {
    return fetch("/api/status").then(function (response) {
      if (!response.ok) throw new Error("HTTP " + response.status);
      return response.json();
    });
  }

  function startPolling(handler, intervalMs) {
    var timer = setInterval(handler, intervalMs);
    handler();
    return timer;
  }

  global.SharedUI = {
    render: render,
    notice: notice,
    fetchStatus: fetchStatus,
    startPolling: startPolling,
    ownerState: ownerState,
    autoRefreshState: autoRefreshState,
    occupancyState: occupancyState,
    flagList: flagList,
    esc: esc,
    duration: duration,
    clock: clock,
    OWNER_TEXT: OWNER_TEXT,
  };
})(window);
