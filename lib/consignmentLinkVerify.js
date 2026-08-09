/**
 * Live URL checks for consignment pricing comps.
 * Heuristic "verified" (host + price + item match) is not enough — links must resolve.
 */
import axios from "axios";

const DEFAULT_TIMEOUT_MS = Math.max(
  2000,
  Math.min(8000, parseInt(process.env.CONSIGNMENT_LINK_VERIFY_TIMEOUT_MS || "4500", 10) || 4500)
);
const MAX_CHECK = Math.max(
  4,
  Math.min(24, parseInt(process.env.CONSIGNMENT_LINK_VERIFY_MAX || "16", 10) || 16)
);

function safeHttpUrl(url) {
  const u = String(url || "").trim();
  return u.startsWith("http://") || u.startsWith("https://") ? u : null;
}

function isOkStatus(status) {
  const n = Number(status);
  return Number.isFinite(n) && n >= 200 && n < 400;
}

async function probeOnce(url, method) {
  const resp = await axios.request({
    method,
    url,
    timeout: DEFAULT_TIMEOUT_MS,
    maxRedirects: 5,
    validateStatus: () => true,
    // Some CDNs block HEAD or empty UA — keep a normal browser-ish GET.
    headers: {
      "User-Agent":
        "Mozilla/5.0 (compatible; LostFoundCompBot/1.0; +https://lostandfoundresale.com)",
      Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    },
    // Don't download huge bodies.
    responseType: method === "GET" ? "stream" : "text",
    maxContentLength: 64_000,
    maxBodyLength: 64_000,
  });

  if (resp.data && typeof resp.data.destroy === "function") {
    try {
      resp.data.destroy();
    } catch {
      /* ignore */
    }
  }

  return {
    ok: isOkStatus(resp.status),
    status: resp.status,
    finalUrl: String(resp.request?.res?.responseUrl || resp.headers?.location || url),
  };
}

/**
 * Returns true if the URL responds with a usable HTTP status.
 */
export async function isLinkLive(url) {
  const u = safeHttpUrl(url);
  if (!u) return false;
  try {
    const head = await probeOnce(u, "HEAD");
    if (head.ok) return true;
    // Many retail sites reject HEAD — fall back to GET.
    if (head.status === 405 || head.status === 403 || head.status === 501 || head.status >= 500) {
      const get = await probeOnce(u, "GET");
      return get.ok;
    }
    if (!head.ok && head.status !== 404 && head.status !== 410) {
      const get = await probeOnce(u, "GET");
      return get.ok;
    }
    return false;
  } catch {
    try {
      const get = await probeOnce(u, "GET");
      return get.ok;
    } catch {
      return false;
    }
  }
}

/**
 * Check unique URLs (capped) in parallel. Returns Set of live URLs (original form).
 */
export async function verifyLinksLive(urls, { max = MAX_CHECK } = {}) {
  const unique = [];
  const seen = new Set();
  for (const raw of urls || []) {
    const u = safeHttpUrl(raw);
    if (!u || seen.has(u)) continue;
    seen.add(u);
    unique.push(u);
    if (unique.length >= max) break;
  }

  const live = new Set();
  await Promise.all(
    unique.map(async (u) => {
      const ok = await isLinkLive(u);
      if (ok) live.add(u);
    })
  );
  return live;
}

/**
 * Drop search/source rows whose URLs do not resolve.
 * Prefer checking higher-ranked / priced candidates first.
 */
export async function filterRowsToLiveLinks(rows, { max = MAX_CHECK } = {}) {
  const list = Array.isArray(rows) ? [...rows] : [];
  list.sort((a, b) => {
    const ap = a?.priceHint != null || a?.price != null ? 1 : 0;
    const bp = b?.priceHint != null || b?.price != null ? 1 : 0;
    if (bp !== ap) return bp - ap;
    return (b?._rank || 0) - (a?._rank || 0);
  });

  const candidates = list
    .map((r) => safeHttpUrl(r?.url))
    .filter(Boolean);
  const live = await verifyLinksLive(candidates, { max });

  return list
    .filter((r) => {
      const u = safeHttpUrl(r?.url);
      if (!u) return false;
      // Only keep rows we confirmed live. Unchecked (beyond max) are dropped for comps.
      return live.has(u);
    })
    .map((r) => ({ ...r, linkVerified: true }));
}

/**
 * Filter analysis.sources to live URLs only (and require http URL).
 */
export async function filterSourcesToLiveLinks(sources, { max = MAX_CHECK } = {}) {
  const list = Array.isArray(sources) ? sources : [];
  const withUrl = list.filter((s) => safeHttpUrl(s?.url));
  const live = await verifyLinksLive(
    withUrl.map((s) => s.url),
    { max }
  );
  return list
    .filter((s) => {
      const u = safeHttpUrl(s?.url);
      if (!u) return false; // no URL → not a usable research link
      return live.has(u);
    })
    .map((s) => ({ ...s, linkVerified: true }));
}
