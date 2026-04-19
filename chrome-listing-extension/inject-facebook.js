/**
 * Runs in page MAIN world. Expects window.__FB_LISTING_EXT set by the popup right before this file loads.
 */
(function () {
  async function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  function isVisible(el) {
    if (!el || !(el instanceof Element)) return false;
    const r = el.getBoundingClientRect();
    if (r.width < 2 || r.height < 2) return false;
    const st = window.getComputedStyle(el);
    if (st.visibility === "hidden" || st.display === "none" || Number(st.opacity) === 0) return false;
    return true;
  }

  function nearestAriaLabel(el) {
    let n = el;
    for (let i = 0; i < 10 && n; i++) {
      const a = n.getAttribute && n.getAttribute("aria-label");
      if (a && a.trim()) return a.trim().toLowerCase();
      n = n.parentElement;
    }
    return "";
  }

  function isStrictlyPriceInput(el) {
    if (!(el instanceof HTMLInputElement)) return false;
    if (el.getAttribute("inputmode") === "decimal") return true;
    const ph = (el.getAttribute("placeholder") || "").trim().toLowerCase();
    if (ph.includes("$") && /\d/.test(ph)) return true;
    if (/\bprice\b/i.test(nearestAriaLabel(el))) return true;
    return false;
  }

  /** Blank line before Dimensions / Width / Weight when Shopify glues them to copy. */
  function formatDescriptionSpacing(s) {
    if (!s || typeof s !== "string") return s;
    const re = /\s+(Dimensions?:|Weight:|Width:\s*\d)/i;
    const m = s.match(re);
    if (!m || m.index == null) return s;
    const i = m.index;
    const before = s.slice(0, i).trimEnd();
    const after = s.slice(i).trimStart();
    if (/\n\n\s*$/.test(before)) return s;
    return `${before}\n\n${after}`;
  }

  /** Find Price input: walk from a node whose first-line text is "Price". */
  function findPriceInputByLabel(exclude) {
    const roots = document.querySelectorAll('[role="dialog"], [role="main"], form, [data-pagelet]');
    for (const root of roots) {
      for (const lab of root.querySelectorAll("span, label, div")) {
        const firstLine = (lab.textContent || "").trim().split("\n")[0].trim();
        if (!/^price$/i.test(firstLine)) continue;
        let cur = lab;
        for (let depth = 0; depth < 14 && cur; depth++) {
          for (const inp of cur.querySelectorAll(
            'input[type="text"], input[type="number"], input:not([type])'
          )) {
            if (!(inp instanceof HTMLInputElement)) continue;
            if (inp.type === "file" || inp.type === "search" || inp.type === "hidden") continue;
            if (!isVisible(inp) || exclude.has(inp)) continue;
            return inp;
          }
          cur = cur.parentElement;
        }
      }
    }
    return null;
  }

  /** If Facebook omits inputmode/$, use next plausible money line under title. */
  function findPriceInputFallback(exclude, titleBottom) {
    const inputs = [...document.querySelectorAll('input[type="text"], input[type="number"], input:not([type])')]
      .filter(isVisible)
      .filter((el) => !exclude.has(el))
      .filter((el) => !(el instanceof HTMLInputElement) || (el.type !== "file" && el.type !== "search" && el.type !== "hidden"))
      .filter((el) => !/\btitle\b/i.test(nearestAriaLabel(el)) || /\bprice\b/i.test(nearestAriaLabel(el)))
      .sort((a, b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);

    for (const el of inputs) {
      const top = el.getBoundingClientRect().top;
      if (titleBottom != null && top < titleBottom - 2) continue;
      if (isStrictlyPriceInput(el)) return el;
      const h = el.getBoundingClientRect().height;
      const aria = nearestAriaLabel(el);
      if (h > 4 && h < 52 && !/\bcategor|condition|search|location|city|zip\b/i.test(aria)) {
        if (/\bprice\b/i.test(aria)) return el;
        if (titleBottom != null && top > titleBottom + 2) return el;
      }
    }
    return null;
  }

  function setReactInput(el, value) {
    if (!el) return false;
    const tag = el.tagName;
    if (tag === "TEXTAREA" || tag === "INPUT") {
      const proto =
        tag === "TEXTAREA"
          ? window.HTMLTextAreaElement.prototype
          : window.HTMLInputElement.prototype;
      const desc = Object.getOwnPropertyDescriptor(proto, "value");
      if (desc && desc.set) desc.set.call(el, value);
      else el.value = value;
      el.dispatchEvent(new Event("input", { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
      el.dispatchEvent(new Event("blur", { bubbles: true }));
      return true;
    }
    if (el.isContentEditable) {
      el.focus();
      const sel = window.getSelection();
      const range = document.createRange();
      range.selectNodeContents(el);
      sel.removeAllRanges();
      sel.addRange(range);
      document.execCommand("insertText", false, value);
      el.dispatchEvent(
        new InputEvent("input", { bubbles: true, inputType: "insertText", data: value })
      );
      return true;
    }
    return false;
  }

  function collectFieldElements() {
    const set = new Set();
    for (const sel of [
      'input[type="text"]',
      "input:not([type])",
      'input[type="number"]',
      "textarea",
      '[contenteditable="true"]',
      '[role="textbox"]',
    ]) {
      for (const el of document.querySelectorAll(sel)) {
        if (el.closest('[data-pagelet*="Marketplace"]') || el.closest('[role="main"]') || document.body.contains(el)) {
          if (isVisible(el)) set.add(el);
        }
      }
    }
    return [...set];
  }

  function findMarketplaceField(kind, exclude) {
    if (kind === "price") {
      const byLabel = findPriceInputByLabel(exclude);
      if (byLabel) return byLabel;

      const strict = collectFieldElements().filter(isStrictlyPriceInput).filter((el) => !exclude.has(el));
      let best = null;
      let bestScore = -1;
      for (const el of strict) {
        const aria = nearestAriaLabel(el);
        let s = 0;
        if (/\bprice\b/i.test(aria)) s += 50;
        if (el.getAttribute("inputmode") === "decimal") s += 40;
        const ph = (el.getAttribute("placeholder") || "").toLowerCase();
        if (ph.includes("$")) s += 30;
        if (s > bestScore) {
          bestScore = s;
          best = el;
        }
      }
      if (best) return best;
      return null;
    }

    const candidates = collectFieldElements();

    const score = (el) => {
      if (exclude.has(el)) return -999;
      if (isStrictlyPriceInput(el)) return -999;
      const aria = nearestAriaLabel(el);
      const ph = ((el.getAttribute && el.getAttribute("placeholder")) || "").toLowerCase();

      if (kind === "title") {
        if (/\btitle\b/i.test(aria) && !/\bprice\b/i.test(aria)) return 100;
        if (aria === "title") return 110;
        if (/short.*title|title.*listing|name.*item/i.test(aria)) return 80;
        if (ph.includes("title") && !ph.includes("price")) return 70;
        if (el.tagName === "TEXTAREA" && ph.length === 0 && aria.includes("title")) return 65;
        return -1;
      }

      if (kind === "description") {
        if (/\bdescription\b/i.test(aria)) return 100;
        if (/describe|description|details.*item/i.test(aria)) return 85;
        if (ph.includes("describe") || ph.includes("escription")) return 75;
        if (el.tagName === "TEXTAREA" && el.getBoundingClientRect().height > 60) return 40;
        return -1;
      }

      return -1;
    };

    let best = null;
    let bestScore = -1;
    for (const el of candidates) {
      const s = score(el);
      if (s > bestScore) {
        bestScore = s;
        best = el;
      }
    }
    if (bestScore >= 40) return best;

    if (kind === "title") {
      const inputs = [...document.querySelectorAll('input[type="text"], input:not([type])')]
        .filter(isVisible)
        .filter((el) => !exclude.has(el))
        .filter((el) => !isStrictlyPriceInput(el))
        .filter((el) => !/\bprice\b/i.test(nearestAriaLabel(el)))
        .filter((el) => {
          const ph = (el.getAttribute("placeholder") || "").toLowerCase();
          return !ph.includes("price") && !ph.includes("$");
        })
        .sort((a, b) => a.getBoundingClientRect().top - b.getBoundingClientRect().top);
      const inComposer = inputs.find(
        (el) => el.closest('[href*="marketplace"]') || el.closest("form") || el.getBoundingClientRect().top > 80
      );
      if (inComposer) return inComposer;
      if (inputs[0]) return inputs[0];
    }

    if (kind === "description") {
      const textareas = [...document.querySelectorAll("textarea")].filter(isVisible);
      const tall = textareas.filter((t) => t.getBoundingClientRect().height > 72);
      for (const t of tall) {
        if (!exclude.has(t)) return t;
      }
    }

    return null;
  }

  /** Used – Good by default; Used – Fair if listing reads AS-IS. */
  function preferredConditionSubstrings(title, description) {
    const text = `${title || ""} ${description || ""}`.toLowerCase();
    const asIs =
      /\bas[-\s]?is\b/i.test(text) ||
      /\bsold\s+as\s+is\b/i.test(text) ||
      /\ball\s+sales\s+final\b/i.test(text) ||
      /\bno\s+warranty\b/i.test(text) ||
      /\bno\s+returns\b/i.test(text);
    if (asIs) return ["fair", "used - fair"];
    return ["good", "used - good"];
  }

  function normalizeConditionOptionText(s) {
    return (s || "")
      .toLowerCase()
      .replace(/\s+/g, " ")
      .replace(/[\u2013\u2014\u2212]/g, "-")
      .replace(/[–—]/g, "-")
      .trim();
  }

  /** Facebook often uses menuitemradio / listbox rows, not role=option. */
  function collectMenuLikeOptions() {
    const seen = new Set();
    const out = [];
    const add = (el) => {
      if (!(el instanceof Element) || !isVisible(el) || seen.has(el)) return;
      const t = (el.textContent || "").trim();
      if (t.length < 2 || t.length > 140) return;
      seen.add(el);
      out.push(el);
    };
    const selectors = [
      '[role="listbox"] [role="option"]',
      '[role="listbox"] [role="menuitemradio"]',
      '[role="listbox"] [role="menuitem"]',
      '[role="listbox"] [role="row"]',
      '[role="listbox"] li',
      '[role="menu"] [role="menuitemradio"]',
      '[role="menu"] [role="menuitem"]',
      '[role="dialog"] [role="option"]',
      '[role="dialog"] [role="menuitemradio"]',
      '[role="dialog"] [role="menuitem"]',
      '[aria-modal="true"] [role="menuitemradio"]',
      '[aria-modal="true"] [role="menuitem"]',
      '[aria-modal="true"] [role="option"]',
      '[role="dialog"] [role="listbox"] div[tabindex="0"]',
      '[role="grid"] [role="row"]',
    ];
    for (const sel of selectors) {
      try {
        document.querySelectorAll(sel).forEach((el) => add(el));
      } catch (_) {}
    }
    return out;
  }

  async function waitForMenuLikeOptions(maxMs) {
    const deadline = Date.now() + maxMs;
    let last = [];
    while (Date.now() < deadline) {
      const opts = collectMenuLikeOptions();
      if (opts.length >= 2) return opts;
      if (
        opts.length === 1 &&
        /used|new|fair|good|parts|refurb|like|open box/i.test(opts[0].textContent || "")
      ) {
        return opts;
      }
      last = opts;
      await sleep(100);
    }
    return last.length ? last : collectMenuLikeOptions();
  }

  function findConditionCombobox() {
    const boxes = [...document.querySelectorAll('[role="combobox"]')].filter(isVisible);
    for (const c of boxes) {
      const blob = (nearestAriaLabel(c) + " " + (c.textContent || "")).toLowerCase();
      if (/\bcondition\b/.test(blob)) return c;
    }
    for (const node of document.querySelectorAll("span, label, div")) {
      if ((node.textContent || "").trim().split("\n")[0].trim() === "Condition") {
        const root = node.closest("form") || node.parentElement?.parentElement || node.parentElement;
        if (root) {
          const cb = root.querySelector('[role="combobox"]');
          if (cb && isVisible(cb)) return cb;
        }
      }
    }
    for (const c of boxes) {
      const al = (c.getAttribute("aria-label") || "").toLowerCase();
      if (al.includes("condition")) return c;
    }
    return null;
  }

  async function selectConditionFromListing(ctx) {
    const { title, description } = ctx;
    const prefs = preferredConditionSubstrings(title, description);
    const trigger = findConditionCombobox();
    if (!trigger) {
      console.warn("Listing extension: condition combobox not found");
      return false;
    }
    trigger.focus();
    trigger.click();
    await sleep(200);
    trigger.dispatchEvent(
      new KeyboardEvent("keydown", { key: "ArrowDown", code: "ArrowDown", keyCode: 40, bubbles: true })
    );
    await sleep(200);

    let options = await waitForMenuLikeOptions(2800);
    if (!options.length) {
      trigger.click();
      await sleep(400);
      options = await waitForMenuLikeOptions(1200);
    }
    if (!options.length) {
      console.warn("Listing extension: no condition options (Facebook UI may have changed)");
      return false;
    }

    const norm = normalizeConditionOptionText;

    for (const pref of prefs) {
      const p = pref.toLowerCase();
      const hit = options.find((o) => {
        const t = norm(o.textContent);
        if (!t) return false;
        if (t.includes(p.replace(/\s+/g, " "))) return true;
        if (p === "good" && t.includes("used") && t.includes("good") && !t.includes("like")) return true;
        if (p === "fair" && t.includes("used") && t.includes("fair")) return true;
        if ((p.includes("like new") || p.includes("like-new")) && t.includes("like") && t.includes("new")) return true;
        if (p === "new" && /^new\b/i.test(t)) return true;
        if (p.includes("for parts") && (t.includes("parts") || t.includes("not working"))) return true;
        return false;
      });
      if (hit) {
        hit.click();
        console.log("Listing extension: condition set", {
          picked: hit.textContent?.trim(),
          prefs,
        });
        await sleep(350);
        return true;
      }
    }

    const asIs =
      /\bas[-\s]?is\b/i.test(`${title} ${description}`.toLowerCase()) ||
      /\bsold\s+as\s+is\b/i.test(`${title} ${description}`.toLowerCase());
    const fallback = options.find((o) => {
      const t = norm(o.textContent);
      if (asIs && (t.includes("like new") || (t.includes("used") && t.includes("like")))) return false;
      return /used\s*-\s*good|used\s*-\s*fair|^good\b|^fair\b/i.test(t);
    });
    if (fallback) {
      fallback.click();
      console.log("Listing extension: condition fallback", fallback.textContent?.trim());
      await sleep(350);
      return true;
    }

    if (options[0]) {
      options[0].click();
      console.log("Listing extension: condition first option", options[0].textContent?.trim());
    }
    return false;
  }

  function findFileInput() {
    const inputs = [...document.querySelectorAll('input[type="file"]')];
    const withAccept = inputs.filter(
      (i) => (i.getAttribute("accept") || "").toLowerCase().includes("image")
    );
    return withAccept[0] || inputs[0] || null;
  }

  function base64ToFile(base64, mime, filename) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return new File([bytes], filename, { type: mime || "image/jpeg" });
  }

  function setFilesOnInput(input, files) {
    if (!input || !files.length) return;
    const dt = new DataTransfer();
    for (const f of files) dt.items.add(f);
    input.files = dt.files;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  }

  async function run() {
    const listing = window.__FB_LISTING_EXT;
    try {
      delete window.__FB_LISTING_EXT;
    } catch (_) {}

    if (!listing || listing.platform !== "facebook") {
      console.warn("Listing extension: missing payload or wrong platform", listing);
      return;
    }

    let { title, price, description, images } = listing;
    description = formatDescriptionSpacing(description || "");

    console.log("Listing extension: autofill start", { title, price, description, imageCount: images?.length || 0 });

    const used = new Set();
    let titleBottom = null;

    for (let attempt = 0; attempt < 25; attempt++) {
      const titleEl = findMarketplaceField("title", used);
      if (titleEl && title) {
        setReactInput(titleEl, title);
        used.add(titleEl);
        titleBottom = titleEl.getBoundingClientRect().bottom;
        console.log("Listing extension: filled title", { aria: nearestAriaLabel(titleEl) });
        break;
      }
      await sleep(350);
    }

    await sleep(400);
    for (let attempt = 0; attempt < 22; attempt++) {
      let priceEl = findMarketplaceField("price", used);
      if (!priceEl) priceEl = findPriceInputFallback(used, titleBottom);
      if (priceEl && price != null && String(price).trim() !== "") {
        const p = String(price).replace(/[^\d.]/g, "");
        if (p) {
          priceEl.focus();
          setReactInput(priceEl, p);
          used.add(priceEl);
          console.log("Listing extension: filled price", { aria: nearestAriaLabel(priceEl), p });
        }
        break;
      }
      await sleep(280);
    }

    await sleep(250);
    if (description) {
      for (let attempt = 0; attempt < 18; attempt++) {
        const descEl = findMarketplaceField("description", used);
        if (descEl) {
          setReactInput(descEl, description);
          used.add(descEl);
          console.log("Listing extension: filled description");
          break;
        }
        await sleep(250);
      }
    }

    await sleep(400);
    await selectConditionFromListing({ title, description });

    await sleep(300);
    if (images && images.length) {
      const files = [];
      for (const img of images) {
        try {
          files.push(base64ToFile(img.base64, img.mime, img.name || "photo.jpg"));
        } catch (e) {
          console.warn("Listing extension: skip image", e);
        }
      }
      for (let attempt = 0; attempt < 15; attempt++) {
        const fi = findFileInput();
        if (fi) {
          setFilesOnInput(fi, files);
          console.log("Listing extension: attached", files.length, "image(s)");
          break;
        }
        await sleep(300);
      }
    }

    console.log("Listing extension: autofill done");
  }

  run().catch((e) => console.error("Listing extension: autofill error", e));
})();
