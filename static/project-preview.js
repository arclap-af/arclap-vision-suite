/* By-project export preview UI hook.
 *
 * Drop-in helper. Two surfaces:
 *
 *   ProjectPreview.openScanReview(jobId)
 *      → Modal with "what was detected" after a scan finishes.
 *      → Shows N projects, file counts, date ranges, ts source mix,
 *        warnings, extension breakdown, duplicate basenames.
 *
 *   ProjectPreview.openExportPreview(jobId, ruleObject)
 *      → Modal with "what will be exported" — same shape but scoped to
 *        the post-filter survivors with proposed renamed filenames.
 *      → Has [Export N files into M folders] button that fires the
 *        actual /api/filter/{job}/export with mode=by_project.
 *
 * Loaded after shell-v2.js. Pure vanilla JS, no React/Vue.
 */
(function () {
  "use strict";

  const fmtNum = (n) => (n ?? 0).toLocaleString();

  function modal(html, onMount) {
    const back = document.createElement("div");
    back.className = "pp-modal-backdrop";
    back.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9000;" +
      "display:flex;align-items:center;justify-content:center;";
    const card = document.createElement("div");
    card.className = "pp-modal";
    card.style.cssText =
      "background:var(--surface,#fff);max-width:1100px;max-height:90vh;" +
      "width:92vw;overflow:auto;border-radius:8px;padding:20px;" +
      "box-shadow:0 10px 40px rgba(0,0,0,.3);";
    card.innerHTML = html;
    back.appendChild(card);
    document.body.appendChild(back);
    back.addEventListener("click", (e) => {
      if (e.target === back) back.remove();
    });
    if (onMount) onMount(card, () => back.remove());
    return { close: () => back.remove(), card };
  }

  function renderProject(p, withProposedNames) {
    const sources = Object.entries(p.ts_source_breakdown || {})
      .map(([k, v]) => `<span class="src-${k}" style="margin-right:8px">${k}:${v}</span>`)
      .join("");
    return `
      <details style="border-bottom:1px solid #eee;padding:8px 4px">
        <summary style="cursor:pointer;display:grid;grid-template-columns:240px 100px 240px 1fr;gap:12px;align-items:center">
          <strong>${p.name}</strong>
          <span style="color:#666">${fmtNum(p.n_files)} files</span>
          <span style="font-variant-numeric:tabular-nums;color:#666">
            ${p.earliest_str || "—"} → ${p.latest_str || "—"}
          </span>
          <span>${sources}${
      p.n_batches_detected > 1 ? ` · ${p.n_batches_detected} batches` : ""
    }</span>
        </summary>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:8px 12px">
          <div>
            <strong>${withProposedNames ? "First 5 (proposed name)" : "First 5"}:</strong>
            <ul style="margin:4px 0;padding-left:18px;font-size:12px">
              ${(p.first_5_filenames || []).map((n) => `<li>${n}</li>`).join("")}
            </ul>
          </div>
          <div>
            <strong>${withProposedNames ? "Last 5 (proposed name)" : "Last 5"}:</strong>
            <ul style="margin:4px 0;padding-left:18px;font-size:12px">
              ${(p.last_5_filenames || []).map((n) => `<li>${n}</li>`).join("")}
            </ul>
          </div>
        </div>
      </details>`;
  }

  function renderReview(d, withProposedNames, headerHtml, footerHtml) {
    const exts = Object.entries(d.extensions || {})
      .sort((a, b) => b[1] - a[1])
      .map(([k, v]) => `<span style="margin-right:12px">${k}: ${fmtNum(v)}</span>`)
      .join("");
    const warningsHtml = (d.warnings || []).length
      ? `<div style="background:#fff3cd;border:1px solid #ffeaa7;padding:10px;
                    border-radius:4px;margin:12px 0;">
           <strong>⚠ Warnings (${d.warnings.length})</strong>
           ${d.warnings
             .map((w) => `<div style="font-size:13px;margin-top:4px">• ${w}</div>`)
             .join("")}
         </div>`
      : "";
    return `
      ${headerHtml}
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:16px 0">
        <div style="background:#f5f5f5;padding:12px;border-radius:4px">
          <div style="font-size:12px;color:#666">Projects</div>
          <div style="font-size:24px;font-weight:600">${fmtNum(d.n_projects)}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px;border-radius:4px">
          <div style="font-size:12px;color:#666">Files</div>
          <div style="font-size:24px;font-weight:600">${fmtNum(d.n_files_total)}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px;border-radius:4px">
          <div style="font-size:12px;color:#666">No timestamp</div>
          <div style="font-size:24px;font-weight:600;color:${
            d.n_with_no_timestamp > 0 ? "#c33" : "#2a8b2a"
          }">${fmtNum(d.n_with_no_timestamp)}</div>
        </div>
        <div style="background:#f5f5f5;padding:12px;border-radius:4px">
          <div style="font-size:12px;color:#666">Duplicate names</div>
          <div style="font-size:24px;font-weight:600;color:${
            d.n_duplicate_basenames > 0 ? "#c33" : "#2a8b2a"
          }">${fmtNum(d.n_duplicate_basenames)}</div>
        </div>
      </div>
      <div style="font-size:12px;color:#666;margin:8px 0">
        Extensions: ${exts || "—"}
      </div>
      ${warningsHtml}
      <div style="margin-top:12px">
        ${(d.projects || []).map((p) => renderProject(p, withProposedNames)).join("")}
      </div>
      ${footerHtml}`;
  }

  async function fetchJSON(url, opts) {
    const r = await fetch(url, opts);
    if (!r.ok) throw new Error(`HTTP ${r.status}: ${await r.text()}`);
    return r.json();
  }

  async function openScanReview(jobId) {
    const m = modal(
      `<div>Loading scan review for ${jobId}…</div>`,
      async (card) => {
        try {
          const data = await fetchJSON(`/api/filter/${jobId}/scan-review`);
          card.innerHTML = renderReview(
            data,
            false,
            `<h2>Scan detection review</h2>
             <div style="color:#666">Job <code>${jobId}</code></div>`,
            `<div style="text-align:right;margin-top:16px">
               <button class="btn-secondary" id="pp-close">Close</button>
             </div>`
          );
          card.querySelector("#pp-close").addEventListener("click", () => m.close());
        } catch (e) {
          card.innerHTML = `<div style="color:#c33">Error: ${e.message}</div>`;
        }
      }
    );
  }

  async function openExportPreview(jobId, rule) {
    const m = modal(
      `<div>Building export preview…</div>`,
      async (card) => {
        try {
          const data = await fetchJSON(
            `/api/filter/${jobId}/export-preview`,
            {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(rule),
            }
          );
          card.innerHTML = renderReview(
            data,
            true,
            `<h2>Export tree preview</h2>
             <div style="color:#666">
               Will export <strong>${fmtNum(data.n_files_total)}</strong> files
               into <strong>${fmtNum(data.n_projects)}</strong> ARC-* folders
             </div>`,
            `<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:16px;
                        border-top:1px solid #eee;padding-top:16px">
               <button class="btn-secondary" id="pp-cancel">Cancel</button>
               <button class="btn-primary" id="pp-commit"
                 ${data.n_files_total === 0 ? "disabled" : ""}>
                 Export ${fmtNum(data.n_files_total)} files
               </button>
             </div>`
          );
          card.querySelector("#pp-cancel").addEventListener("click", () => m.close());
          card.querySelector("#pp-commit").addEventListener("click", async () => {
            card.querySelector("#pp-commit").disabled = true;
            card.querySelector("#pp-commit").textContent = "Exporting…";
            const exportReq = Object.assign({}, rule, {
              mode: "by_project",
              transfer_mode: "copy",
              rename_chronological: true,
              include_manifest: true,
              batch_separator: "flat",
            });
            try {
              const result = await fetchJSON(
                `/api/filter/${jobId}/export`,
                {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify(exportReq),
                }
              );
              card.innerHTML = `
                <h2>Export complete ✓</h2>
                <div>Wrote <strong>${fmtNum(result.n_files_exported)}</strong>
                files into <strong>${fmtNum(result.n_projects)}</strong> folders.</div>
                ${
                  result.n_errors > 0
                    ? `<div style="color:#c33">${result.n_errors} errors — see _manifest.csv</div>`
                    : ""
                }
                <div style="margin:12px 0;font-family:monospace;font-size:12px">
                  ${result.target}
                </div>
                <div style="text-align:right">
                  <button class="btn-primary" id="pp-done">Done</button>
                </div>`;
              card.querySelector("#pp-done").addEventListener("click", () => m.close());
            } catch (e) {
              card.innerHTML = `<div style="color:#c33">Export failed: ${e.message}</div>`;
            }
          });
        } catch (e) {
          card.innerHTML = `<div style="color:#c33">Error: ${e.message}</div>`;
        }
      }
    );
  }

  // Expose globally so any UI surface can call it
  window.ProjectPreview = { openScanReview, openExportPreview };
})();
