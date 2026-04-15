/**
 * Craigslist posting flow — reads window.__CL_LISTING_EXT set by popup.js (MAIN world).
 * Does not interact with Facebook listing globals.
 */
(function () {
  const listing = window.__CL_LISTING_EXT;

  if (!listing || listing.platform !== "craigslist") return;

  function setValue(el, val) {
    if (!el || !val) return;
    el.focus();
    el.value = val;
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function clickButtonByText(text) {
    const needle = String(text || "").toLowerCase();
    return [...document.querySelectorAll("button, input[type='submit'], input[type='button'], input")].find(
      (el) => {
        const label = (el.textContent || el.value || "").toLowerCase();
        return label.includes(needle);
      }
    );
  }

  /** Top row "city or neighborhood" — not `input[name="city"]` (that is often the disabled location block). */
  function setCityOrNeighborhood(val) {
    const v = String(val || "").trim();
    if (!v) return;
    const tryEls = [
      document.querySelector('input[name="GeographicArea"]'),
      document.querySelector("input#geographic_area"),
      document.querySelector('input[name="geographic_area"]'),
      document.querySelector('input[name="area"]'),
    ];
    for (const el of tryEls) {
      if (el && !el.disabled && !el.readOnly) {
        setValue(el, v);
        return;
      }
    }
    const lab = [...document.querySelectorAll("label")].find((l) =>
      /city\s+or\s+neighborhood/i.test((l.textContent || "").trim())
    );
    if (lab) {
      const fid = lab.getAttribute("for");
      if (fid) {
        const inp = document.getElementById(fid);
        if (inp && inp instanceof HTMLInputElement && !inp.disabled && !inp.readOnly) {
          setValue(inp, v);
        }
      }
    }
  }

  function firstEnabledInput(selector) {
    return [...document.querySelectorAll(selector)].find((el) => el && !el.disabled && !el.readOnly);
  }

  function normLabelText(el) {
    return String((el && el.textContent) || "")
      .replace(/\s+/g, " ")
      .trim();
  }

  /** Craigslist ties inputs to `<label for="id">`; names vary by category. */
  function inputForLabelRegex(re) {
    const lab = [...document.querySelectorAll("label")].find((l) => re.test(normLabelText(l)));
    if (!lab) return null;
    const id = lab.getAttribute("for");
    if (!id) return null;
    const el = document.getElementById(id);
    if (!el || el.disabled || el.readOnly) return null;
    if (el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement) return el;
    return null;
  }

  function setConditionSelect() {
    let sel = document.querySelector('select[name="condition"]');
    if (!sel) {
      const lab = [...document.querySelectorAll("label")].find((l) => /^condition\b/i.test(normLabelText(l)));
      if (lab) {
        const id = lab.getAttribute("for");
        if (id) {
          const el = document.getElementById(id);
          if (el && el instanceof HTMLSelectElement) sel = el;
        }
      }
    }
    if (!sel) return;
    const hay = `${listing.title || ""} ${listing.description || ""}`;
    const want = /\bas\s*is\b/i.test(hay) ? "fair" : listing.condition || "good";
    const wantLower = String(want).toLowerCase();
    const opts = [...sel.options];
    let opt =
      opts.find((o) => String(o.value || "").toLowerCase() === wantLower) ||
      opts.find((o) => normLabelText(o).toLowerCase() === wantLower) ||
      opts.find((o) => normLabelText(o).toLowerCase().includes(wantLower));
    if (!opt) return;
    sel.value = opt.value;
    sel.dispatchEvent(new Event("change", { bubbles: true }));
  }

  function setVendorMakeModel() {
    const v = String(listing.vendor || "").trim();
    if (!v) return;
    const makeEl =
      inputForLabelRegex(/make\s*\/\s*manufacturer/i) ||
      firstEnabledInput('input[name="sale_make"]') ||
      firstEnabledInput('input[name="make"]');
    if (makeEl) setValue(makeEl, v);
    const modelEl =
      inputForLabelRegex(/model\s*name/i) ||
      firstEnabledInput('input[name="sale_model"]') ||
      firstEnabledInput('input[name="model"]');
    if (modelEl) setValue(modelEl, v);
  }

  function setShowAddressAndFillStore() {
    let showCb =
      document.querySelector('input[type="checkbox"][name="show_address"]') ||
      document.querySelector('input[type="checkbox"][name="ShowAddress"]') ||
      document.querySelector("input#show_address");
    if (!showCb) {
      const lab = [...document.querySelectorAll("label")].find((l) => /show\s+address/i.test((l.textContent || "").trim()));
      if (lab) {
        const fid = lab.getAttribute("for");
        if (fid) {
          const inp = document.getElementById(fid);
          if (inp && inp instanceof HTMLInputElement && inp.type === "checkbox") showCb = inp;
        }
      }
    }
    if (showCb && !showCb.disabled) {
      showCb.checked = true;
      showCb.dispatchEvent(new Event("input", { bubbles: true }));
      showCb.dispatchEvent(new Event("change", { bubbles: true }));
    }

    const street = String(listing.storeStreet || "").trim();
    const cross = String(listing.storeCrossStreet || "").trim();
    const city = String(listing.storeCity || "").trim();
    const st = String(listing.storeState || "").trim();

    window.setTimeout(() => {
      const fieldsets = [...document.querySelectorAll("fieldset")];
      const locFs = fieldsets.find((fs) =>
        /location\s+info/i.test((fs.querySelector("legend")?.textContent || "").trim())
      );
      const root = locFs || document;

      const stEl =
        (locFs && [...locFs.querySelectorAll('input[name="street"]')].find((el) => !el.disabled && !el.readOnly)) ||
        firstEnabledInput('input[name="street"]');
      if (stEl && street) setValue(stEl, street);

      const xs =
        (locFs &&
          [...locFs.querySelectorAll('input[name="cross_street"],input[name="xstreet1"],input[name="intersection"]')].find(
            (el) => !el.disabled && !el.readOnly
          )) ||
        firstEnabledInput('input[name="cross_street"]') ||
        firstEnabledInput('input[name="xstreet1"]') ||
        firstEnabledInput('input[name="intersection"]');
      if (xs && cross) setValue(xs, cross);

      const cityEl =
        (locFs && [...locFs.querySelectorAll('input[name="city"]')].find((el) => !el.disabled && !el.readOnly)) ||
        firstEnabledInput('input[name="city"]');
      if (cityEl && city) setValue(cityEl, city);

      const reg =
        (locFs && locFs.querySelector('select[name="region"]')) || root.querySelector('select[name="region"]');
      if (reg && st) {
        const opt = [...reg.options].find((o) => (o.value || "").toUpperCase() === st.toUpperCase());
        if (opt) reg.value = opt.value;
        reg.dispatchEvent(new Event("change", { bubbles: true }));
      }
    }, 250);
  }

  function setDeliveryAvailableChecked() {
    let cb =
      document.querySelector('input[type="checkbox"][name="delivery"]') ||
      document.querySelector('input[type="checkbox"][name="OfferDelivery"]') ||
      document.querySelector("input#delivery");
    if (!cb) {
      const lab = [...document.querySelectorAll("label")].find((l) =>
        /delivery\s+available/i.test((l.textContent || "").trim())
      );
      if (lab) {
        const fid = lab.getAttribute("for");
        if (fid) {
          const inp = document.getElementById(fid);
          if (inp && inp instanceof HTMLInputElement && inp.type === "checkbox") cb = inp;
        }
      }
    }
    if (cb && !cb.disabled) {
      cb.checked = true;
      cb.dispatchEvent(new Event("input", { bubbles: true }));
      cb.dispatchEvent(new Event("change", { bubbles: true }));
    }
  }

  const isDetailsPage = document.querySelector('input[name="PostingTitle"]');
  const isMapPage = document.querySelector("#map") || document.querySelector(".map");
  const isImagePage = document.querySelector('input[type="file"]');
  const isPublishPage = document.body.innerText.toLowerCase().includes("unpublished draft");

  if (isDetailsPage) {
    setValue(document.querySelector('input[name="PostingTitle"]'), listing.title);
    setValue(document.querySelector('input[name="price"]'), listing.price);
    setValue(document.querySelector('textarea[name="PostingBody"]'), listing.description);
    setValue(document.querySelector('input[name="postal"]'), listing.zip);
    setCityOrNeighborhood(listing.neighborhood || listing.city);

    setVendorMakeModel();
    setConditionSelect();

    setDeliveryAvailableChecked();
    setShowAddressAndFillStore();

    console.log("Craigslist: Page 1 filled");
    return;
  }

  if (isMapPage) {
    console.log("Craigslist: Map page detected");

    setValue(document.querySelector('input[name="city"]'), listing.city);
    setValue(document.querySelector('input[name="postal"]'), listing.zip);

    setTimeout(() => {
      const btn =
        document.querySelector('button[type="submit"]') ||
        document.querySelector('input[type="submit"]') ||
        clickButtonByText("continue");

      if (btn) {
        console.log("Craigslist: Continue from map");
        btn.click();
      }
    }, 800);

    return;
  }

  if (isImagePage) {
    console.log("Craigslist: Image page");

    function base64ToFile(base64, mime, name) {
      const binary = atob(base64);
      const bytes = new Uint8Array(binary.length);
      for (let i = 0; i < binary.length; i++) {
        bytes[i] = binary.charCodeAt(i);
      }
      return new File([bytes], name, { type: mime });
    }

    function uploadImages(input, images) {
      const dt = new DataTransfer();
      images.forEach((img) => {
        dt.items.add(base64ToFile(img.base64, img.mime, img.name));
      });
      input.files = dt.files;
      input.dispatchEvent(new Event("change", { bubbles: true }));
    }

    const input = document.querySelector('input[type="file"]');

    if (input && listing.images?.length) {
      uploadImages(input, listing.images);
      console.log("Craigslist: Images uploaded");
    }

    setTimeout(() => {
      const doneBtn = clickButtonByText("done");
      if (doneBtn) doneBtn.click();
    }, 1000);

    return;
  }

  if (isPublishPage) {
    console.log("Craigslist: Publish page");

    const publishBtn = clickButtonByText("publish");

    if (publishBtn) {
      publishBtn.style.border = "3px solid red";
      publishBtn.style.backgroundColor = "#ffdddd";
    }

    return;
  }
})();
