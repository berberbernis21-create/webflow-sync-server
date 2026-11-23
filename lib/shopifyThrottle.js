/**
 * One Shopify Admin budget for the whole process.
 * Bulk image syncs and listing writes share it, so they queue instead of racing.
 * Limit is about 2 requests/second with a burst of 2. A 429 waits for Retry-After
 * and tries again. This does not call Shopify by itself.
 */
import axios from "axios";
import { AsyncLocalStorage } from "node:async_hooks";

const PER_SECOND = 2;
const BURST = 2;
const MAX_429_ATTEMPTS = 5;
/**
 * An Intake edit (one listing write) goes ahead of bulk and Webflow sync calls.
 * After this many priority calls in a row, one waiting bulk call is served, so
 * the bulk work is delayed and never starved.
 */
const PRIORITY_RUN_MAX = 3;

let tokens = BURST;
let lastRefill = Date.now();
const waiters = [];
const priorityWaiters = [];
let priorityRun = 0;
let timer = null;
let installed = false;
const priorityContext = new AsyncLocalStorage();

/** Shopify calls made inside fn (and what it awaits or schedules) take the priority lane. */
export function runWithShopifyPriority(fn) {
  return priorityContext.run({ priority: true }, fn);
}

export function shopifyPriorityActive() {
  return Boolean(priorityContext.getStore()?.priority);
}

export function shopifyThrottleLimits() {
  return { perSecond: PER_SECOND, burst: BURST };
}

/** Start times (ms) if each call is instant. First `burst` run together, then one every 1/rate. */
export function planShopifyCallTimes(count, { perSecond = PER_SECOND, burst = BURST, startedAt = 0 } = {}) {
  const times = [];
  let available = burst;
  let clock = startedAt;
  const gap = 1000 / perSecond;
  const total = Math.max(0, Number(count) || 0);
  for (let i = 0; i < total; i += 1) {
    if (available >= 1) {
      times.push(clock);
      available -= 1;
      continue;
    }
    clock += gap;
    available += 1;
    times.push(clock);
    available -= 1;
  }
  return times;
}

function refill(now = Date.now()) {
  const elapsed = (now - lastRefill) / 1000;
  if (elapsed > 0) {
    tokens = Math.min(BURST, tokens + elapsed * PER_SECOND);
    lastRefill = now;
  }
}

export function resetShopifyThrottleForTests({ tokens: startTokens = BURST } = {}) {
  tokens = Math.max(0, Math.min(BURST, Number(startTokens)));
  lastRefill = Date.now();
  waiters.length = 0;
  priorityWaiters.length = 0;
  priorityRun = 0;
  if (timer) clearTimeout(timer);
  timer = null;
}

/** Next waiter: priority first, but a waiting bulk call gets a turn after PRIORITY_RUN_MAX. */
function nextWaiter() {
  const bulkWaiting = waiters.length > 0;
  if (priorityWaiters.length && (!bulkWaiting || priorityRun < PRIORITY_RUN_MAX)) {
    priorityRun = bulkWaiting ? priorityRun + 1 : 0;
    return priorityWaiters.shift();
  }
  priorityRun = 0;
  return waiters.shift();
}

function pump() {
  refill();
  while (tokens >= 1 && (waiters.length || priorityWaiters.length)) {
    tokens -= 1;
    const resolve = nextWaiter();
    resolve();
  }
  if (!waiters.length && !priorityWaiters.length) {
    if (timer) clearTimeout(timer);
    timer = null;
    return;
  }
  if (timer) return;
  const waitMs = Math.max(25, Math.ceil(((1 - tokens) / PER_SECOND) * 1000));
  timer = setTimeout(() => {
    timer = null;
    pump();
  }, waitMs);
}

/**
 * One slot of the shared Shopify budget. `priority` defaults to the calling
 * context (runWithShopifyPriority). The rate is the same for both lanes; only
 * the order of waiting calls changes.
 */
export function acquireShopifySlot({ priority = shopifyPriorityActive() } = {}) {
  refill();
  if (tokens >= 1 && !waiters.length && !priorityWaiters.length) {
    tokens -= 1;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    (priority ? priorityWaiters : waiters).push(resolve);
    pump();
  });
}

export function isShopifyAdminUrl(url) {
  return /myshopify\.com\/admin\//i.test(String(url || ""));
}

export function shopifyRetryDelayMs(response, attempt = 1) {
  const headers = response?.headers || {};
  const header = headers["retry-after"] ?? headers["Retry-After"];
  if (header != null && String(header).trim() !== "") {
    const seconds = Number(header);
    if (Number.isFinite(seconds) && seconds >= 0) return Math.min(seconds * 1000, 20000);
    const when = Date.parse(String(header));
    if (Number.isFinite(when)) return Math.min(Math.max(0, when - Date.now()), 20000);
  }
  const step = Math.max(1, Number(attempt) || 1);
  return Math.min(500 * 2 ** (step - 1), 8000);
}

function requestUrl(config) {
  return `${config?.baseURL || ""}${config?.url || ""}`;
}

/**
 * The first argument is the 429 response already in hand.
 * `requester` performs the next attempt and is called until it returns a non-429 or the cap is hit.
 */
export async function retryAfterShopify429(response, requester, { sleep, maxAttempts = MAX_429_ATTEMPTS } = {}) {
  const wait = sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  let current = response;
  let attempt = 0;
  while (current?.status === 429 && attempt < maxAttempts) {
    attempt += 1;
    await wait(shopifyRetryDelayMs(current, attempt));
    await acquireShopifySlot();
    current = await requester(current?.config || {});
  }
  return current;
}

async function retryAxios429(client, config, response, attempt) {
  const next = config || {};
  const seen = Number(next.__shopify429 || 0);
  if (seen >= MAX_429_ATTEMPTS) return null;
  next.__shopify429 = Math.max(seen, attempt);
  await new Promise((resolve) => setTimeout(resolve, shopifyRetryDelayMs(response, next.__shopify429)));
  return client.request(next);
}

export function installShopifyThrottle(client = axios) {
  if (installed || !client?.interceptors) return;
  installed = true;
  client.interceptors.request.use(async (config) => {
    if (isShopifyAdminUrl(requestUrl(config))) await acquireShopifySlot();
    return config;
  });
  client.interceptors.response.use(
    async (response) => {
      if (!isShopifyAdminUrl(requestUrl(response?.config)) || response?.status !== 429) return response;
      const retried = await retryAxios429(client, response.config, response, Number(response.config?.__shopify429 || 0) + 1);
      return retried || response;
    },
    async (error) => {
      const response = error?.response;
      const config = error?.config;
      if (!response || response.status !== 429 || !isShopifyAdminUrl(requestUrl(config))) {
        return Promise.reject(error);
      }
      const retried = await retryAxios429(client, config, response, Number(config?.__shopify429 || 0) + 1);
      if (!retried) return Promise.reject(error);
      return retried;
    }
  );
}
