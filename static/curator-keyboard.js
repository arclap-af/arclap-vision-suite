/* Curator keyboard shortcuts + drag-to-select.
 * Loaded after shell-v2.js. Adds power-user navigation:
 *   J / →   next pick
 *   K / ←   previous pick
 *   Space   toggle current pick selection
 *   1..9    set class for current pick (when class strip is visible)
 *   A       select all visible
 *   I       invert selection
 *   D       discard selected
 *   Esc     clear selection
 *   ?       show help overlay
 *   Shift+click on a card  range-select from last to clicked
 */
(function () {
  "use strict";

  let cursor = -1;        // index of "current" card in the pp-grid
  let lastSelected = -1;  // for shift-range select
  let helpVisible = false;

  function $cards() {
    return Array.from(document.querySelectorAll(".pp-card, .pick-card"));
  }

  function focusCard(i) {
    const cards = $cards();
    if (!cards.length) return;
    cursor = Math.max(0, Math.min(i, cards.length - 1));
    cards[cursor].scrollIntoView({ block: "nearest", behavior: "smooth" });
    cards.forEach((c, idx) => c.classList.toggle("kb-cursor", idx === cursor));
  }

  function toggleSelected(i) {
    const cards = $cards();
    if (i < 0 || i >= cards.length) return;
    const c = cards[i];
    c.classList.toggle("pp-selected");
    lastSelected = i;
  }

  function rangeSelect(toIdx) {
    if (lastSelected < 0) return toggleSelected(toIdx);
    const cards = $cards();
    const [lo, hi] = lastSelected < toIdx ? [lastSelected, toIdx] : [toIdx, lastSelected];
    for (let i = lo; i <= hi; i++) cards[i].classList.add("pp-selected");
  }

  function selectedCards() {
    return Array.from(document.querySelectorAll(".pp-card.pp-selected, .pick-card.pp-selected"));
  }

  function showHelp() {
    if (document.getElementById("kb-help-overlay")) return;
    const ov = document.createElement("div");
    ov.id = "kb-help-overlay";
    ov.style.cssText =
      "position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:9999;" +
      "display:flex;align-items:center;justify-content:center;color:#fff;" +
      "font:14px/1.5 system-ui";
    ov.innerHTML = `
      <div style="background:#222;padding:24px 32px;border-radius:8px;max-width:480px">
        <h3 style="margin-top:0">Keyboard shortcuts</h3>
        <table style="width:100%;border-collapse:collapse">
          <tr><td><kbd>J</kbd> / <kbd>→</kbd></td><td>next pick</td></tr>
          <tr><td><kbd>K</kbd> / <kbd>←</kbd></td><td>previous pick</td></tr>
          <tr><td><kbd>Space</kbd></td><td>toggle selection</td></tr>
          <tr><td><kbd>1</kbd>…<kbd>9</kbd></td><td>set class</td></tr>
          <tr><td><kbd>A</kbd></td><td>select all</td></tr>
          <tr><td><kbd>I</kbd></td><td>invert</td></tr>
          <tr><td><kbd>D</kbd></td><td>discard selected</td></tr>
          <tr><td><kbd>Shift</kbd>+click</td><td>range select</td></tr>
          <tr><td><kbd>Esc</kbd></td><td>clear selection</td></tr>
          <tr><td><kbd>?</kbd></td><td>this help</td></tr>
        </table>
        <div style="margin-top:16px;text-align:right;opacity:.7">
          Press <kbd>Esc</kbd> or click outside to dismiss
        </div>
      </div>`;
    ov.addEventListener("click", () => ov.remove());
    document.body.appendChild(ov);
    helpVisible = true;
  }

  document.addEventListener("keydown", (e) => {
    // Skip when typing in inputs
    const t = e.target;
    if (t.matches("input, textarea, select, [contenteditable=true]")) return;
    if (e.ctrlKey || e.metaKey || e.altKey) return;

    const cards = $cards();
    if (!cards.length && e.key !== "?") return;

    switch (e.key) {
      case "j":
      case "ArrowRight":
        focusCard(cursor + 1);
        e.preventDefault();
        break;
      case "k":
      case "ArrowLeft":
        focusCard(cursor - 1);
        e.preventDefault();
        break;
      case " ":
        if (cursor >= 0) toggleSelected(cursor);
        e.preventDefault();
        break;
      case "a":
      case "A":
        cards.forEach((c) => c.classList.add("pp-selected"));
        lastSelected = cards.length - 1;
        e.preventDefault();
        break;
      case "i":
      case "I":
        cards.forEach((c) => c.classList.toggle("pp-selected"));
        e.preventDefault();
        break;
      case "d":
      case "D":
        selectedCards().forEach((c) => c.classList.add("pp-discarded"));
        e.preventDefault();
        break;
      case "Escape":
        if (helpVisible) {
          document.getElementById("kb-help-overlay")?.remove();
          helpVisible = false;
        } else {
          cards.forEach((c) => c.classList.remove("pp-selected"));
          lastSelected = -1;
        }
        e.preventDefault();
        break;
      case "?":
        showHelp();
        e.preventDefault();
        break;
      default:
        if (/^[1-9]$/.test(e.key) && cursor >= 0) {
          // Class assignment hook — fires a custom event the curator picks up
          const classIdx = parseInt(e.key, 10);
          cards[cursor].dispatchEvent(
            new CustomEvent("curator:set-class", { detail: { class_index: classIdx }, bubbles: true })
          );
          e.preventDefault();
        }
    }
  });

  // Shift+click range-select
  document.addEventListener(
    "click",
    (e) => {
      const card = e.target.closest(".pp-card, .pick-card");
      if (!card) return;
      if (!e.shiftKey) {
        const idx = $cards().indexOf(card);
        if (idx >= 0) lastSelected = idx;
        return;
      }
      const cards = $cards();
      const idx = cards.indexOf(card);
      if (idx < 0) return;
      rangeSelect(idx);
      e.preventDefault();
      e.stopPropagation();
    },
    true
  );
})();
