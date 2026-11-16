// Avro 仪器事件格式升级平台 - 前端逻辑
const $ = (id) => document.getElementById(id);

async function api(path, opts = {}) {
  const r = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok || data.ok === false) {
    const err = new Error(data.error || `HTTP ${r.status}`);
    err.payload = data;
    throw err;
  }
  return data;
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}
function badge(status) {
  const map = {
    pending: "待确认 pending", confirmed: "已确认 confirmed",
    rejected: "已拒绝 rejected",
  };
  return `<span class="badge ${esc(status)}">${esc(map[status] || status)}</span>`;
}

async function refresh() {
  let s;
  try {
    s = await api("/api/status");
  } catch (e) {
    $("writer-state").innerHTML = `<span class="badge offline">后端不可达</span>`;
    return;
  }
  renderStates(s);
  renderSwitches(s);
  renderEvents(s);
  renderDecodes(s);
  renderHistory(s);
  renderCorrectionForm(s);
}

function renderStates(s) {
  const w = s.effective_writer;
  $("writer-state").innerHTML =
    `<div><b>v${w.version}</b> <span class="pill">fp ${esc(w.fingerprint.slice(0, 10))}</span></div>
     <pre>${esc(pretty(w.schema))}</pre>`;
  for (const [id, key] of [["reader-a", "analyzer-a"], ["reader-b", "analyzer-b"]]) {
    const a = s.analyzers[key];
    if (!a.reachable) {
      $(id).innerHTML = `<span class="badge offline">不可达：${esc(a.error || "")}</span>`;
      continue;
    }
    $(id).innerHTML =
      `<div><b>v${a.effective_version}</b> <span class="pill">${esc(a.analyzer_id)}</span></div>
       <pre>${esc(pretty(a.reader_schema))}</pre>`;
  }
}

function renderHistory(s) {
  const rows = s.writer_schemas.map((x) =>
    `<div class="state" style="margin-bottom:6px">
       <b>v${x.version}</b> <span class="pill">fp ${esc(x.fp.slice(0, 10))}</span>
       <span class="meta">${esc(x.created_at)}</span>
       <pre>${esc(pretty(x.schema))}</pre>
     </div>`).join("");
  $("schema-history").innerHTML = rows || "<div class='hint'>无</div>";
}

function switchRow(sw, isReader) {
  const target = isReader ? `目标：${esc(sw.analyzer_id)} · ` : "";
  return `<div class="switch-row">
      <div class="meta">${target}${esc(sw.switch_id)} · 版本 v${sw.base_version} → v${sw.version}
        · ${esc(sw.updated_at)} ${badge(sw.status)}</div>
      ${sw.last_error ? `<div class="err">${esc(sw.last_error)}</div>` : ""}
      <pre>${esc(pretty(sw.schema))}</pre>
    </div>`;
}

function renderSwitches(s) {
  $("writer-switches").innerHTML =
    s.writer_switches.map((x) => switchRow(x, false)).join("") ||
    "<div class='hint'>暂无切换记录</div>";
  $("reader-switches").innerHTML =
    s.reader_switches.map((x) => switchRow(x, true)).join("") ||
    "<div class='hint'>暂无切换记录</div>";
}

function renderEvents(s) {
  const tb = document.querySelector("#events-table tbody");
  tb.innerHTML = s.events.map((e) => `<tr>
      <td>${esc(e.id)}</td>
      <td>${esc(e.entity_id)}<br/>修订 v${e.revision}
        ${e.is_current ? '<span class="pill">当前</span>' : '<span class="pill">历史</span>'}</td>
      <td>v${e.writer_version}<div class="pill">${esc(e.writer_fp.slice(0, 8))}</div></td>
      <td><pre>${esc(pretty(e.payload))}</pre></td>
      <td>${esc(e.ingested_at)}</td>
      <td>
        <button onclick="decodeNow('${esc(e.id)}','analyzer-a')">A 解码</button>
        <button onclick="decodeNow('${esc(e.id)}','analyzer-b')">B 解码</button>
      </td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">尚无留存事件</td></tr>`;
}

function renderDecodes(s) {
  const tb = document.querySelector("#decodes-table tbody");
  tb.innerHTML = s.decode_results.map((d) => `<tr>
      <td>${esc(d.decoded_at)}</td>
      <td>${esc(d.event_id)}</td>
      <td>${esc(d.analyzer_id)}</td>
      <td>v${esc(d.reader_version)}</td>
      <td>${d.decoded_ok
        ? '<span class="badge ok">成功</span>'
        : '<span class="badge fail">失败</span>'}</td>
      <td><pre>${esc(d.error || pretty(d.datum))}</pre></td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">暂无解码结果</td></tr>`;
}

window.decodeNow = async function (eventId, analyzerId) {
  try {
    await api("/api/events/decode", {
      method: "POST",
      body: { event_id: eventId, analyzer_id: analyzerId },
    });
    await refresh();
  } catch (e) { alert("解码请求失败：" + e.message); }
};

function showMsg(id, text, ok) {
  const el = $(id);
  el.className = ok ? "msg-ok" : "msg-err";
  el.textContent = text;
}

// ---------------- 事件入仓 ----------------
async function ingest(body) {
  try {
    const r = await api("/api/events", { method: "POST", body });
    let msg = `事件已入仓：${r.event_id}（写入 v${r.writer_version}，${r.size_bytes} 字节 Avro）`;
    for (const d of r.decodes || []) {
      msg += d.error
        ? `\n[${d.analyzer_id}] 解码失败: ${d.error}`
        : `\n[${d.analyzer_id}] 解码${d.decoded_ok ? "成功" : "失败"} reader=v${d.reader_version}`;
    }
    const allOk = (r.decodes || []).every(
      (d) => !d.error && d.decoded_ok === true);
    showMsg("ingest-msg", msg, allOk);
    await refresh();
  } catch (e) {
    showMsg("ingest-msg", "入仓被拒：" + e.message, false);
  }
}

$("btn-ingest-simple").onclick = () => {
  const raw = $("ev-measurement").value.trim();
  const measurement = Number(raw);
  ingest({
    sample_id: $("ev-sample").value,
    project: $("ev-project").value,
    measurement: Number.isFinite(measurement) ? measurement : raw,
    decode_on: [
      ...($("ev-dec-a").checked ? ["analyzer-a"] : []),
      ...($("ev-dec-b").checked ? ["analyzer-b"] : []),
    ],
  });
};
$("btn-ingest-json").onclick = () => {
  try {
    const payload = JSON.parse($("ev-payload").value);
    ingest({
      payload,
      decode_on: [
        ...($("ev-dec-a").checked ? ["analyzer-a"] : []),
        ...($("ev-dec-b").checked ? ["analyzer-b"] : []),
      ],
    });
  } catch (e) {
    showMsg("ingest-msg", "JSON 解析失败：" + e.message, false);
  }
};

// ---------------- 事件更正 ----------------
// 载入表单时捕获“所见修订 + 所见写格式”，提交时原样带回；服务端据此拒绝
// 交错的另一笔更正或写格式切换。
let lastStatus = null;
let corrSeen = null;  // {entity_id, revision, writer_fp, writer_version}

function renderCorrectionForm(s) {
  lastStatus = s;
  const sel = $("corr-entity");
  const prev = corrSeen ? corrSeen.entity_id : sel.value;
  sel.innerHTML = (s.current_events || []).map((e) =>
    `<option value="${esc(e.entity_id)}">${esc(e.entity_id)} · 修订 v${e.revision} · ` +
    `${esc(e.id)} · 写格式 v${e.writer_version}</option>`).join("");
  if (!s.current_events || !s.current_events.length) {
    sel.innerHTML = `<option value="">（暂无当前有效事件）</option>`;
  }
  if (prev && [...sel.options].some((o) => o.value === prev)) sel.value = prev;

  if (corrSeen) {
    const w = s.effective_writer;
    const stale = w.fingerprint !== corrSeen.writer_fp;
    const cur = (s.current_events || []).find(
      (e) => e.entity_id === corrSeen.entity_id);
    const revStale = !cur || cur.revision !== corrSeen.revision;
    $("corr-seen").innerHTML =
      `所见：实体 ${esc(corrSeen.entity_id)} · 修订 v${corrSeen.revision} · ` +
      `写格式 v${corrSeen.writer_version} fp ${esc(corrSeen.writer_fp.slice(0, 8))}` +
      (stale ? `<br/><span class="err-text">⚠ 生效写格式已变为 v${w.version} ` +
               `fp ${esc(w.fingerprint.slice(0, 8))}，提交将被拒绝，请重新载入。</span>` : "") +
      (revStale ? `<br/><span class="err-text">⚠ 该实体当前修订已变化，提交将被拒绝，请重新载入。</span>` : "");
  }
  if (sel.value) loadChain(sel.value);
}

$("btn-corr-load").onclick = () => {
  const entityId = $("corr-entity").value;
  const cur = (lastStatus && lastStatus.current_events || []).find(
    (e) => e.entity_id === entityId);
  if (!cur) {
    showMsg("corr-msg", "请先在“当前有效事件”中选取一个事件。", false);
    return;
  }
  corrSeen = {
    entity_id: entityId,
    revision: cur.revision,
    writer_fp: lastStatus.effective_writer.fingerprint,
    writer_version: lastStatus.effective_writer.version,
  };
  $("corr-payload").value = pretty(cur.payload);
  showMsg("corr-msg",
    `已载入 ${cur.id}（修订 v${cur.revision}）；所见写格式 ` +
    `v${corrSeen.writer_version} fp ${corrSeen.writer_fp.slice(0, 8)}。`, true);
  renderCorrectionForm(lastStatus);
};

$("btn-corr-submit").onclick = async () => {
  if (!corrSeen) {
    return showMsg("corr-msg", "请先“载入更正表单”，再提交更正。", false);
  }
  let payload;
  try { payload = JSON.parse($("corr-payload").value); }
  catch (e) { return showMsg("corr-msg", "JSON 解析失败：" + e.message, false); }
  const reason = $("corr-reason").value.trim();
  if (!reason) return showMsg("corr-msg", "请填写更正原因。", false);
  try {
    const r = await api("/api/corrections", {
      method: "POST",
      body: {
        entity_id: corrSeen.entity_id,
        seen_revision: corrSeen.revision,
        seen_writer_fp: corrSeen.writer_fp,
        payload,
        reason,
        decode_on: [
          ...($("corr-dec-a").checked ? ["analyzer-a"] : []),
          ...($("corr-dec-b").checked ? ["analyzer-b"] : []),
        ],
      },
    });
    let msg = `更正已生效：${r.old_event_id} → ${r.new_event_id}（修订 v${r.revision}，` +
      `写格式 v${r.writer_version}，${r.size_bytes} 字节）；原事件字节与格式保留不变。`;
    for (const d of r.decodes || []) {
      msg += d.error
        ? `\n[${d.analyzer_id}] 解码失败: ${d.error}`
        : `\n[${d.analyzer_id}] 解码${d.decoded_ok ? "成功" : "失败"} reader=v${d.reader_version}`;
    }
    showMsg("corr-msg", msg, (r.decodes || []).every(
      (d) => !d.error && d.decoded_ok !== false));
    corrSeen = null;
    $("corr-reason").value = "";
    await refresh();
  } catch (e) {
    // 409 所见修订/写格式过期、422 编码失败：原当前事件不受影响
    showMsg("corr-msg", "更正被拒绝（原当前事件保持不变）：\n" + e.message, false);
    await refresh();
  }
};

async function loadChain(entityId) {
  let chain;
  try {
    chain = (await api(`/api/entities/${encodeURIComponent(entityId)}/revisions`)).chain;
  } catch (e) {
    $("corr-chain").innerHTML = `<div class="hint">修订链读取失败：${esc(e.message)}</div>`;
    return;
  }
  const revs = chain.revisions.slice().reverse().map((e) => `
    <div class="switch-row">
      <div class="meta">修订 v${e.revision}
        ${e.is_current ? '<span class="badge confirmed">当前</span>'
                       : '<span class="badge offline">历史</span>'}
        · ${esc(e.id)} · 写格式 v${e.writer_version}
        <span class="pill">fp ${esc(e.writer_fp.slice(0, 8))}</span> · ${esc(e.ingested_at)}</div>
      <pre>${esc(pretty(e.payload))}</pre>
      <button onclick="decodeNow('${esc(e.id)}','analyzer-a')">A 按原格式解码</button>
      <button onclick="decodeNow('${esc(e.id)}','analyzer-b')">B 按原格式解码</button>
    </div>`).join("");
  const cors = chain.corrections.map((c) => `
    <div class="meta">✎ ${esc(c.correction_id)} · 修订 v${c.seen_revision} → v${c.new_revision}
      · ${esc(c.old_event_id)} → ${esc(c.new_event_id)} · 写格式 v${c.writer_version}
      · ${esc(c.created_at)}<br/>原因：${esc(c.reason)}</div>`).join("");
  $("corr-chain").innerHTML =
    `<div class="meta">实体 ${esc(chain.entity_id)} · 当前修订 v${chain.current_revision}
       （${esc(chain.current_event_id)}）</div>` +
    (revs || '<div class="hint">无修订记录</div>') +
    (cors ? `<h3>更正记录</h3>${cors}` : "");
}

// ---------------- 格式切换 ----------------
$("btn-writer-switch").onclick = async () => {
  let schema;
  try { schema = JSON.parse($("writer-schema").value); }
  catch (e) { return showMsg("switch-msg", "Schema JSON 解析失败：" + e.message, false); }
  try {
    const r = await api("/api/switches/writer", {
      method: "POST", body: { schema },
    });
    showMsg("switch-msg",
      `写入切换 ${r.switch.switch_id} 状态：${r.switch.status}` +
      (r.switch.last_error ? "\n" + r.switch.last_error : ""),
      r.switch.status === "confirmed");
  } catch (e) { showMsg("switch-msg", "写入切换被阻止：\n" + e.message, false); }
  await refresh();
};

$("btn-reader-switch").onclick = async () => {
  let schema;
  try { schema = JSON.parse($("reader-schema").value); }
  catch (e) { return showMsg("switch-msg", "Schema JSON 解析失败：" + e.message, false); }
  try {
    const r = await api("/api/switches/reader", {
      method: "POST",
      body: { analyzer_id: $("reader-target").value, schema },
    });
    showMsg("switch-msg",
      `读取切换 ${r.switch.switch_id} 状态：${r.switch.status}` +
      (r.switch.last_error ? "\n" + r.switch.last_error : ""),
      r.switch.status === "confirmed");
  } catch (e) { showMsg("switch-msg", "读取切换被阻止：\n" + e.message, false); }
  await refresh();
};

// ---------------- 对账 ----------------
$("btn-reconcile").onclick = async () => {
  try {
    const r = await api("/api/reconcile", { method: "POST" });
    $("reconcile-msg").textContent =
      "对账完成：writer " + JSON.stringify(r.result.writer) +
      " / reader " + JSON.stringify(r.result.reader);
    $("reconcile-msg").className = "msg-ok";
  } catch (e) {
    $("reconcile-msg").textContent = "对账失败：" + e.message;
    $("reconcile-msg").className = "msg-err";
  }
  await refresh();
};

// ---------------- 故障注入 ----------------
$("btn-sim").onclick = async () => {
  try {
    const r = await api("/api/simulate", {
      method: "POST",
      body: {
        delay_ms: Number($("sim-delay").value || 0),
        drop_writer_switch: $("sim-drop-writer").checked,
        drop_reader_switch: $("sim-drop-reader").checked,
        analyzer_id: $("sim-analyzer").value || null,
        drop_analyzer_switch: $("sim-drop-sw").checked,
        drop_analyzer_decode: $("sim-drop-dec").checked,
      },
    });
    $("sim-msg").textContent = "已应用：" + JSON.stringify(r.simulation);
    $("sim-msg").className = "msg-ok";
  } catch (e) {
    $("sim-msg").textContent = "故障注入失败：" + e.message;
    $("sim-msg").className = "msg-err";
  }
};

// ---------------- 初始化：填入示例 schema ----------------
async function initForms() {
  const base = await api("/api/default-schema");
  const v2 = structuredClone(base);
  v2.fields.push({ name: "instrument_id", type: "string", default: "UNKNOWN" });
  v2.fields.push({ name: "quality_flag", type: ["null", "string"], default: null });
  $("writer-schema").value = pretty(v2);

  const readerV2 = structuredClone(base);
  readerV2.fields.push({ name: "temperature", type: "double", default: 0.0 });
  $("reader-schema").value = pretty(readerV2);
}

initForms();
refresh();
setInterval(refresh, 3000);
