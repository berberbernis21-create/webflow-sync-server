const API_SERVER = "https://webflow-sync-server.onrender.com";
const API_LISTING = `${API_SERVER}/api/listing?name=`;

/** First line of pickup footer (Facebook + Craigslist); matches store wayfinding. */
const PICKUP_LANDMARK_LINE =
  "Pickup is right by Scottsdale Quarter at Lost and Found Resale Interiors.";

/**
 * Marketplace/Craigslist pricing style: bump to next whole dollar only.
 * Examples: 39 -> 40, 44 -> 45, 1499 -> 1500.
 */
function roundUpMarketplacePrice(rawPrice) {
  const raw = String(rawPrice ?? "").trim();
  if (!raw) return "";
  const numeric = Number(raw.replace(/[^0-9.]/g, ""));
  if (!Number.isFinite(numeric) || numeric <= 0) return raw;
  const bumped = Math.floor(numeric) + 1;
  return String(bumped);
}

function showQuickLinks({ productUrl, storeUrl, handbagsShopUrl, isLuxury }) {
  const panel = document.getElementById("quickLinks");
  const qlProduct = document.getElementById("qlProduct");
  const qlHandbags = document.getElementById("qlHandbags");
  const qlStore = document.getElementById("qlStore");
  if (!panel || !qlProduct || !qlHandbags || !qlStore) return;
  qlProduct.href = productUrl || storeUrl;
  qlStore.href = storeUrl;
  qlHandbags.href = handbagsShopUrl;
  qlHandbags.style.display = isLuxury ? "block" : "none";
  panel.classList.add("visible");
}

function hideQuickLinks() {
  const panel = document.getElementById("quickLinks");
  if (panel) panel.classList.remove("visible");
}

const LUXURY_BRAND_PATTERN =
  /\b(gucci|chanel|prada|fendi|dior|ysl|saint laurent|louis vuitton|vuitton|lv|goyard|balenciaga|hermes|celine|bottega|bottega veneta|chloe|miu miu|valentino|versace|burberry|louboutin|michael kors|kate spade|tory burch|coach)\b/gi;

/** Furniture / lighting / art-world designer & maker names (Craigslist title only; reduces policy risk). */
const CRAIGSLIST_DESIGNER_BRAND_PATTERN =
  /\b(?:tribu|herman\s+miller|knoll|vitra|minotti|cassina|poliform|flexform|moroso|kartell|flos|artemide|moooi|dedon|gloster|b&b\s+italia|bb\s+italia|boconcept|ligne\s+roset|roche\s+bobois|poltrona\s+frau|gubi|hay\s+design|muuto|normann\s+copenhagen|fritz\s+hansen|carl\s+hansen|thonet|usm\s+haller|eames|saarinen|noguchi|bertoia|le\s+corbusier|mies\s+van\s+der\s+rohe|philippe\s+starck|patricia\s+urquiola|marcel\s+breuer|hans\s+j\.\s*wegner|wegner|arne\s+jacobsen|jacobsen|poul\s+kjaerholm|kjaerholm|finn\s+juhl|juhl|george\s+nelson|nelson\s+platform|warren\s+platner|platner|isamu\s+noguchi|tom\s+dixon|dixon|restoration\s+hardware|rh\s+modern|design\s+within\s+reach|\bdwr\b)\b/gi;

/**
 * Craigslist posting title: drop designer/artist/brand names (AI and platform sensitivity).
 * Keeps item type words (e.g. art, lounge chair, dining table).
 */
function sanitizeCraigslistTitle(rawTitle) {
  let t = String(rawTitle || "").trim();
  if (!t) return "";
  const byAttribution = /\s+by\s+(?!the\b|owner\b|seller\b|us\b|me\b|appointment\b)[A-Za-z][A-Za-z.'-]*(?:\s+[A-Za-z][A-Za-z.'-]*){0,5}\b/gi;
  t = t.replace(byAttribution, " ");
  t = t.replace(CRAIGSLIST_DESIGNER_BRAND_PATTERN, " ");
  t = t.replace(LUXURY_BRAND_PATTERN, " ");
  t = t
    .replace(/\(\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .replace(/^[\s\-,:;|]+|[\s\-,:;|]+$/g, "")
    .trim();
  if (!t || t.length < 4) return String(rawTitle || "").trim().slice(0, 70) || "Furniture / home item";
  return t.slice(0, 70).trim();
}

/**
 * Luxury title safety for Marketplace:
 * keep the core item wording, but strip explicit brand names/trademarks.
 */
function sanitizeLuxuryFacebookTitle(rawTitle) {
  const title = String(rawTitle || "").trim();
  if (!title) return "Luxury accessory";
  let cleaned = title
    .replace(LUXURY_BRAND_PATTERN, "")
    .replace(/\(\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .replace(/^[\s\-,:;|]+|[\s\-,:;|]+$/g, "")
    .trim();
  if (!cleaned || cleaned.length < 8) cleaned = "Luxury accessory";
  return cleaned.slice(0, 95).trim();
}

function sanitizeLuxuryFacebookDescription(rawText) {
  let cleaned = String(rawText || "")
    .replace(LUXURY_BRAND_PATTERN, "")
    .replace(/\(\s*\)/g, "")
    .replace(/\s{2,}/g, " ")
    .replace(/^[\s\-,:;|]+|[\s\-,:;|]+$/gm, "")
    .trim();

  const guaranteeLine =
    "Everything comes with an authenticity guarantee, and a certificate is available upon request.";
  if (!cleaned) return guaranteeLine;
  if (!/authenticity guarantee/i.test(cleaned)) {
    cleaned = `${cleaned} ${guaranteeLine}`;
  }
  return cleaned.trim();
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  const chunk = 8192;
  for (let i = 0; i < bytes.length; i += chunk) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
  }
  return btoa(binary);
}

async function fetchImagesForListing(imageUrls) {
  const images = [];
  const urls = (imageUrls || []).slice(0, 5);
  for (let i = 0; i < urls.length; i++) {
    const u = urls[i];
    try {
      const res = await fetch(u);
      if (!res.ok) continue;
      const blob = await res.blob();
      const buf = await blob.arrayBuffer();
      if (buf.byteLength > 4 * 1024 * 1024) continue;
      const mime = blob.type || "image/jpeg";
      const ext = mime.includes("png") ? "png" : mime.includes("webp") ? "webp" : "jpg";
      images.push({
        base64: arrayBufferToBase64(buf),
        mime,
        name: `listing_${i + 1}.${ext}`,
      });
    } catch (e) {
      console.warn("Listing extension (popup): image fetch failed", u, e);
    }
  }
  return images;
}

/** Which Craigslist posting UI is showing (MAIN world probe). */
async function detectCraigslistUiStep(tabId) {
  try {
    const [{ result }] = await chrome.scripting.executeScript({
      target: { tabId },
      world: "MAIN",
      func: () => {
        if (document.querySelector('input[name="PostingTitle"]')) return "details";
        if (document.querySelector("#map") || document.querySelector(".map")) return "map";
        const href = String(location.href || "").toLowerCase();
        if (/[?&]s=editimage\b/.test(href) || /[?&]s=editpic\b/.test(href)) return "images";

        const t = (document.body && document.body.innerText) || "";
        const tl = t.toLowerCase();
        const hasUploaderCopy =
          /\bmaximum\s+24\b/i.test(t) ||
          /\b0\s+images\s+of\s+a\s+maximum\s+24\b/i.test(tl) ||
          /\bdone with images\b/i.test(tl) ||
          /\bdrop image files here\b/i.test(tl);
        const hasUploaderChrome =
          document.querySelector("#uploader") ||
          document.querySelector('input[type="file"][name="file"]') ||
          document.querySelector('input[type="file"]') ||
          [...document.querySelectorAll("a")].some((a) =>
            /classic\s+image\s+uploader/i.test(String(a.textContent || "").replace(/\s+/g, " ").trim())
          );
        if (hasUploaderCopy && hasUploaderChrome) return "images";

        if (tl.includes("unpublished draft")) return "publish";
        return "unknown";
      },
    });
    return result || "unknown";
  } catch (e) {
    console.warn("Listing extension: Craigslist page probe failed", e);
    return "unknown";
  }
}

function craigslistUrlHintsImageStep(url) {
  const u = String(url || "").toLowerCase();
  return /[?&]s=editimage\b/.test(u) || /[?&]s=editpic\b/.test(u) || /[/?]editimage\b/.test(u);
}

/**
 * Natural Facebook body copy via server-side OpenAI (key never in extension).
 * Returns null if API missing, error, or empty — caller uses catalog description fallback.
 */
async function fetchFacebookListingNarrative(facts) {
  try {
    const res = await fetch(`${API_SERVER}/api/listing-blurb`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(facts),
    });
    const j = await res.json().catch(() => ({}));
    if (res.ok && j.text && String(j.text).trim()) {
      return String(j.text).trim();
    }
    console.warn("Listing extension: listing-blurb skipped", res.status, j?.error || j?.code || "");
  } catch (e) {
    console.warn("Listing extension: listing-blurb fetch error", e);
  }
  return null;
}

/** URLs, email, pickup, availability — appended after AI narrative (never hallucinated). */
function buildFacebookLinksFooter({ isLuxury, productUrl, storeUrl, handbagsShopUrl, pickupBlock }) {
  if (isLuxury) {
    return (
      "Item located here:\n\n" +
      `${productUrl}\n\n` +
      `Handbags: ${handbagsShopUrl}\nFurniture & home: ${storeUrl}\n\n` +
      'If the product page says "No Longer Available," it has sold.\n\n' +
      pickupBlock
    );
  }
  return (
    "Here is the item at our site. That page has all the information (photos, details, pricing, shipping and pickup).\n\n" +
    `${productUrl}\n\n` +
    `Shop the full store at ${storeUrl}. You can buy online or come in.\n\n` +
    'If the product page says "No Longer Available," it has sold.\n\n' +
    pickupBlock
  );
}

document.getElementById("start").addEventListener("click", async () => {
  hideQuickLinks();
  const name = document.getElementById("name").value.trim();
  const platform = document.getElementById("platform").value;

  if (!name) {
    console.warn("Listing extension: empty product name");
    return;
  }

  const url = API_LISTING + encodeURIComponent(name);

  let data;
  try {
    const res = await fetch(url);
    const text = await res.text();
    try {
      data = JSON.parse(text);
    } catch {
      console.error("Listing extension: not JSON", text);
      return;
    }
    if (!res.ok || data.error) {
      console.error("Listing extension: API error", res.status, data);
      return;
    }
    console.log("Listing extension (popup): API response", data);
  } catch (e) {
    console.error("Listing extension: fetch failed", e);
    return;
  }

  const storeUrl = "https://www.lostandfoundresale.com";
  const handbagsShopUrl = "https://www.lostandfoundhandbags.com";
  const productUrl = (data.productUrl && String(data.productUrl).trim()) || storeUrl;
  const isLuxury = data.vertical === "luxury";
  const marketplacePrice = roundUpMarketplacePrice(data.price);
  const craigslistTitle = sanitizeCraigslistTitle(data.title || "");
  const facebookTitle = isLuxury
    ? sanitizeLuxuryFacebookTitle(data.title || "")
    : String(data.title || "");
  showQuickLinks({ productUrl, storeUrl, handbagsShopUrl, isLuxury });

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.id) {
    console.error("Listing extension: no active tab");
    return;
  }

  if (platform === "craigslist") {
    const craigslistTabUrl = tab.url || "";
    if (!craigslistTabUrl.includes("craigslist.org")) {
      console.warn("Listing extension: open craigslist.org in the tab to autofill");
      return;
    }

    let uiStep = await detectCraigslistUiStep(tab.id);
    if (uiStep === "unknown" && craigslistUrlHintsImageStep(craigslistTabUrl)) {
      uiStep = "images";
    }

    if (uiStep === "images") {
      let images = [];
      if (Array.isArray(data.images) && data.images.length) {
        images = await fetchImagesForListing(data.images);
      }
      if (!images.length) {
        console.warn("Listing extension: no listing images from API; check product has photos");
        return;
      }
      const imagePayload = {
        platform: "craigslist",
        imagesOnly: true,
        images,
      };
      try {
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          func: (p) => {
            window.__CL_LISTING_EXT = p;
          },
          args: [imagePayload],
        });
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          world: "MAIN",
          files: ["inject-craigslist.js"],
        });
        console.log("Listing extension: Craigslist image step — attached", images.length, "image(s)");
      } catch (e) {
        console.error("Listing extension: Craigslist image inject failed", e);
      }
      return;
    }

    if (uiStep !== "details" && uiStep !== "map" && uiStep !== "publish") {
      console.warn(
        "Listing extension: open Craigslist posting form (title), map, or image upload page. On photos: click Start again."
      );
      return;
    }

    const titleAndDesc = `${data.title || ""} ${data.description || ""}`;
    const craigslistCondition = /\bas\s*is\b/i.test(titleAndDesc) ? "fair" : "good";

    const craigslistBlurbFacts = {
      title: craigslistTitle || data.title || "",
      price: marketplacePrice,
      vertical: data.vertical || "furniture",
      productDescription: data.description || "",
      pickupAddress: "15530 N Greenway Hayden Loop Suite 100, Scottsdale, AZ 85260",
      pickupHours: "MON - SAT 10-5, SUN 12-4",
      contactEmail: "info@lostandfoundresale.com",
      isLuxury,
      outputChannel: "craigslist",
    };
    const craigslistNarrative = await fetchFacebookListingNarrative(craigslistBlurbFacts);
    const catalogFallback = String(data.description || "").trim();
    let narrative =
      (craigslistNarrative && String(craigslistNarrative).trim()) ||
      catalogFallback ||
      "See photos. Pickup right by Scottsdale Quarter at Lost and Found Resale Interiors. Use the link at the bottom of this post for details and to contact us through the site, or stop in.";
    if (isLuxury) {
      narrative = sanitizeLuxuryFacebookDescription(narrative);
    }

    const pickupAddress = "15530 N Greenway Hayden Loop Suite 100, Scottsdale, AZ 85260";
    const pickupHours = "MON - SAT 10-5, SUN 12-4";
    const pickupBlock = `${PICKUP_LANDMARK_LINE}\n${pickupAddress}\nStore hours: ${pickupHours}`;
    const linksFooter = buildFacebookLinksFooter({
      isLuxury,
      productUrl,
      storeUrl,
      handbagsShopUrl,
      pickupBlock,
    });
    const craigslistDescription = `${narrative.trim()}\n\n---\n${linksFooter}`;

    const vendorRaw = String(data.vendor || "").trim();
    const vendor = vendorRaw && !/^unknown$/i.test(vendorRaw) ? vendorRaw : "";

    const payload = {
      platform: "craigslist",
      title: craigslistTitle || data.title || "",
      price: marketplacePrice,
      description: craigslistDescription,
      zip: "85251",
      city: "Scottsdale",
      /** Top-of-form "city or neighborhood" (not the same DOM field as location `city`). */
      neighborhood: "Scottsdale",
      condition: craigslistCondition,
      /** Shopify vendor (same text for CL make + model per store workflow). */
      vendor,
      /** Fetched again on image step (second Start); keeps first inject small. */
      images: [],
      /** Location info — after "show address" (same storefront as pickup footer). */
      storeStreet: "15530 N Greenway Hayden Loop Suite 100",
      storeCrossStreet: "",
      storeCity: "Scottsdale",
      storeState: "AZ",
    };

    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: "MAIN",
        func: (p) => {
          window.__CL_LISTING_EXT = p;
        },
        args: [payload],
      });
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        world: "MAIN",
        files: ["inject-craigslist.js"],
      });
    } catch (e) {
      console.error("Listing extension: Craigslist inject failed", e);
    }
    return;
  }

  if (platform !== "facebook") {
    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        func: (payload, plat) => {
          console.log("Listing extension (injected): data", payload);
          console.log("Listing extension (injected): platform", plat);
        },
        args: [data, platform],
      });
    } catch (e) {
      console.error("Listing extension: inject failed", e);
    }
    return;
  }

  const tabUrl = tab.url || "";
  if (!tabUrl.includes("facebook.com")) {
    console.warn("Listing extension: open facebook.com in the tab to autofill");
    return;
  }

  let images = [];
  if (Array.isArray(data.images) && data.images.length) {
    images = await fetchImagesForListing(data.images);
  }

  const pickupAddress = "15530 N Greenway Hayden Loop Suite 100, Scottsdale, AZ 85260";
  const pickupHours = "MON - SAT 10-5, SUN 12-4";
  const pickupBlock = `${PICKUP_LANDMARK_LINE}\n${pickupAddress}\nStore hours: ${pickupHours}`;

  const blurbFacts = {
    title: facebookTitle || "",
    sourceTitle: data.title || "",
    price: marketplacePrice,
    vertical: data.vertical || "furniture",
    productDescription: data.description || "",
    pickupAddress,
    pickupHours,
    contactEmail: "info@lostandfoundresale.com",
    isLuxury,
  };

  const aiNarrative = await fetchFacebookListingNarrative(blurbFacts);
  const catalogFallback = (data.description && String(data.description).trim()) || "";
  let narrative =
    aiNarrative ||
    catalogFallback ||
    "Thanks for looking — details are in the title and photos. Message us if you have questions.";
  if (isLuxury) {
    narrative = sanitizeLuxuryFacebookDescription(narrative);
  }

  if (aiNarrative) {
    console.log("Listing extension: using OpenAI listing narrative");
  }

  const linksFooter = buildFacebookLinksFooter({
    isLuxury,
    productUrl,
    storeUrl,
    handbagsShopUrl,
    pickupBlock,
  });
  const fullDescription = `${narrative}\n\n---\n${linksFooter}`;

  const payload = {
    platform: "facebook",
    title: facebookTitle || "",
    price: marketplacePrice,
    description: fullDescription,
    images,
  };

  try {
    await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      world: "MAIN",
      func: (p) => {
        window.__FB_LISTING_EXT = p;
      },
      args: [payload],
    });
    await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      world: "MAIN",
      files: ["inject-facebook.js"],
    });
  } catch (e) {
    console.error("Listing extension: inject failed", e);
  }
});

(function initCraigslistPhotoHint() {
  const platformEl = document.getElementById("platform");
  const hint = document.getElementById("craigslistPhotoHint");
  if (!platformEl || !hint) return;
  function sync() {
    hint.style.display = platformEl.value === "craigslist" ? "block" : "none";
  }
  platformEl.addEventListener("change", sync);
  sync();
})();
