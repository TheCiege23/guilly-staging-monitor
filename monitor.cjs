"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// lib/env/prisma-connection-guardrails.ts
function applyPrismaConnectionGuardrails(rawUrl) {
  try {
    const parsed = new URL(rawUrl);
    if (!["postgres:", "postgresql:"].includes(parsed.protocol)) return rawUrl;
    if (!parsed.searchParams.has("connection_limit")) parsed.searchParams.set("connection_limit", "5");
    if (!parsed.searchParams.has("pool_timeout")) parsed.searchParams.set("pool_timeout", "30");
    if (parsed.hostname.endsWith(".neon.tech") && !parsed.searchParams.has("statement_cache_size")) parsed.searchParams.set("statement_cache_size", "20");
    return parsed.toString();
  } catch {
    return rawUrl;
  }
}
var init_prisma_connection_guardrails = __esm({
  "lib/env/prisma-connection-guardrails.ts"() {
    "use strict";
  }
});

// lib/env/database-url.ts
function isDomRuntime() {
  if (typeof globalThis === "undefined") return false;
  const g4 = globalThis;
  return typeof g4.window !== "undefined" || typeof g4.document !== "undefined";
}
function normalizeDatabaseUrl(url) {
  try {
    const parsed = new URL(url);
    if (parsed.hostname.endsWith("pooler.supabase.com") && parsed.port === "6543") {
      if (!parsed.searchParams.has("pgbouncer")) {
        parsed.searchParams.set("pgbouncer", "true");
      }
      if (!parsed.searchParams.has("connection_limit")) {
        parsed.searchParams.set("connection_limit", "1");
      }
      return parsed.toString();
    }
    return url;
  } catch {
    return url;
  }
}
function hasSupportedPostgresScheme(url) {
  return /^(postgres|postgresql):\/\//i.test(url.trim());
}
function envRecord(env) {
  return env;
}
function resolveDatabaseUrl(env = process.env) {
  const e = envRecord(env);
  for (const key of DATABASE_URL_ENV_KEYS) {
    const raw = e[key];
    const value = typeof raw === "string" ? raw.trim() : "";
    if (!value) continue;
    if (!hasSupportedPostgresScheme(value)) continue;
    return normalizeDatabaseUrl(value);
  }
  return null;
}
function getDatabaseUrlOrThrow(env = process.env) {
  if (isDomRuntime()) {
    return NOOP_POSTGRES_URL;
  }
  if (process.env.NEXT_PHASE === "phase-production-build") {
    return NOOP_POSTGRES_URL;
  }
  const resolved = resolveDatabaseUrl(env);
  if (resolved) return resolved;
  const invalidSchemeKeys = [];
  const e = envRecord(env);
  for (const key of DATABASE_URL_ENV_KEYS) {
    const raw = e[key];
    if (raw == null || typeof raw !== "string") continue;
    const value = raw.trim();
    if (!value) continue;
    if (!hasSupportedPostgresScheme(value)) {
      invalidSchemeKeys.push(key);
    }
  }
  if (invalidSchemeKeys.length > 0) {
    throw new Error(
      `Invalid database URL: ${invalidSchemeKeys.join(", ")} must start with postgres:// or postgresql:// (Prisma does not use prisma:// Accelerate URLs as DATABASE_URL). Fix the value in Vercel and redeploy.`
    );
  }
  throw new Error(
    "DATABASE_URL is not set. Add it to your local environment and Vercel project settings. Use DATABASE_URL, or a provider alias such as POSTGRES_PRISMA_URL / POSTGRES_URL (Neon/Vercel). Set it for Production (and Preview if needed), use a postgres:// or postgresql:// URL, then redeploy so new serverless bundles pick up the variable."
  );
}
var DATABASE_URL_ENV_KEYS, NOOP_POSTGRES_URL;
var init_database_url = __esm({
  "lib/env/database-url.ts"() {
    "use strict";
    DATABASE_URL_ENV_KEYS = [
      "DATABASE_URL",
      "POSTGRES_PRISMA_URL",
      "POSTGRES_URL",
      /** Vercel Postgres / Neon: direct (non-pooled) URL; must be postgres:// — see playwright.config.ts parity */
      "POSTGRES_URL_NON_POOLING",
      "NEON_DATABASE_URL",
      "DIRECT_URL"
    ];
    NOOP_POSTGRES_URL = "postgresql://noop:noop@localhost:5432/noop";
  }
});

// lib/observability/dbTelemetry.ts
function operationName(operation) {
  return operation.model ? `${operation.model}.${operation.operation}` : operation.operation;
}
function failureLabel(error) {
  const code = error?.code;
  if (typeof code === "string" && /^P\d{4}$/.test(code)) return code;
  const name = error?.name;
  if (typeof name === "string" && /^[A-Za-z][A-Za-z0-9]{0,59}$/.test(name)) return name;
  return "Error";
}
function createDbObserver(api, config) {
  const statsByRoot = /* @__PURE__ */ new WeakMap();
  const maxErrorSpans = config.maxErrorSpansPerRoot ?? DEFAULT_MAX_ERROR_SPANS;
  function record2(parent, operation, startedMonotonic, startedEpoch, failure) {
    const durationMs = Math.max(0, config.monotonicMs() - startedMonotonic);
    const root = api.getRootSpan(parent);
    if (!root.isRecording()) return;
    const name = operationName(operation);
    const stats = statsByRoot.get(root) ?? {
      count: 0,
      totalMs: 0,
      maxMs: 0,
      slowest: "",
      errors: 0,
      slowSpans: 0,
      errorSpans: 0,
      errorOps: []
    };
    stats.count += 1;
    stats.totalMs += durationMs;
    const label = failure ? failureLabel(failure.error) : null;
    if (label) {
      stats.errors += 1;
      const op = `${name}:${label}`;
      if (!stats.errorOps.includes(op) && stats.errorOps.length < MAX_ERROR_OPS) stats.errorOps.push(op);
    }
    if (durationMs >= stats.maxMs) {
      stats.maxMs = durationMs;
      stats.slowest = name;
    }
    statsByRoot.set(root, stats);
    root.setAttributes({
      "af.db.count": stats.count,
      "af.db.ms": round1(stats.totalMs),
      "af.db.max_ms": round1(stats.maxMs),
      "af.db.slowest": stats.slowest,
      "af.db.errors": stats.errors,
      /*
       * Only once something has failed, so a clean request carries exactly the attributes it always
       * did. A string, so Sentry queries it by its bare name — unlike the numbers above, which need
       * `tags[name,number]`.
       */
      ...stats.errorOps.length > 0 ? { "af.db.error_ops": stats.errorOps.join(",") } : {}
    });
    const asError = label !== null && stats.errorSpans < maxErrorSpans;
    const asSlow = !asError && durationMs >= config.slowQueryMs && stats.slowSpans < config.maxSlowSpansPerRoot;
    if (!asError && !asSlow) return;
    if (asError) stats.errorSpans += 1;
    else stats.slowSpans += 1;
    const span = api.withActiveSpan(
      parent,
      () => api.startInactiveSpan({
        name,
        op: "db.prisma",
        startTime: startedEpoch / 1e3,
        onlyIfParent: true,
        attributes: {
          "db.system": "postgresql",
          "db.operation.name": operation.operation,
          ...operation.model ? { "db.collection.name": operation.model } : {},
          ...durationMs >= config.slowQueryMs ? { "af.db.slow": true } : {},
          ...label ? { "af.db.error": label } : {}
        }
      })
    );
    if (label) span.setStatus({ code: SPAN_STATUS_ERROR, message: "internal_error" });
    span.end((startedEpoch + durationMs) / 1e3);
  }
  return async function observeDbOperation2(operation, run) {
    let parent;
    try {
      parent = api.getActiveSpan();
    } catch {
      parent = void 0;
    }
    if (!parent) return run();
    const startedMonotonic = config.monotonicMs();
    const startedEpoch = config.epochMs();
    let failure = null;
    try {
      return await run();
    } catch (error) {
      failure = { error };
      throw error;
    } finally {
      try {
        record2(parent, operation, startedMonotonic, startedEpoch, failure);
      } catch {
      }
    }
  };
}
function positiveNumberFromEnv(name, fallback) {
  const raw = typeof process !== "undefined" ? process.env?.[name] : void 0;
  const parsed = raw == null || raw.trim() === "" ? Number.NaN : Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}
var Sentry, SPAN_STATUS_ERROR, MAX_ERROR_OPS, DEFAULT_MAX_ERROR_SPANS, round1, sentrySpanApi, observeDbOperation;
var init_dbTelemetry = __esm({
  "lib/observability/dbTelemetry.ts"() {
    "use strict";
    Sentry = __toESM(require("@sentry/nextjs"));
    SPAN_STATUS_ERROR = 2;
    MAX_ERROR_OPS = 8;
    DEFAULT_MAX_ERROR_SPANS = 10;
    round1 = (value) => Math.round(value * 10) / 10;
    sentrySpanApi = {
      getActiveSpan: () => Sentry.getActiveSpan(),
      getRootSpan: (span) => Sentry.getRootSpan(span),
      withActiveSpan: (span, callback) => Sentry.withActiveSpan(span, callback),
      startInactiveSpan: (options) => Sentry.startInactiveSpan(options)
    };
    observeDbOperation = createDbObserver(sentrySpanApi, {
      slowQueryMs: positiveNumberFromEnv("AF_TRACE_SLOW_QUERY_MS", 100),
      maxSlowSpansPerRoot: 20,
      monotonicMs: () => typeof performance !== "undefined" ? performance.now() : Date.now(),
      epochMs: () => Date.now()
    });
  }
});

// lib/prisma.ts
var prisma_exports = {};
__export(prisma_exports, {
  prisma: () => prisma
});
function isConnectionError(error) {
  if (error instanceof import_client.Prisma.PrismaClientInitializationError) return true;
  if (error instanceof import_client.Prisma.PrismaClientRustPanicError) return true;
  const err = error;
  const code = err?.code ?? "";
  const message = String(err?.message ?? "");
  const messageLower = message.toLowerCase();
  if (["P1001", "P1002", "P1008", "P1017", "P2024"].includes(code)) {
    return true;
  }
  if (messageLower.includes("terminating connection due to administrator command")) {
    return true;
  }
  if (messageLower.includes("can't reach database server")) {
    return true;
  }
  if (messageLower.includes("connection timed out")) {
    return true;
  }
  if (messageLower.includes("maxclientsinsessionmode") || messageLower.includes("too many clients")) {
    return true;
  }
  if (messageLower.includes("prepared statement") && (messageLower.includes("does not exist") || messageLower.includes("already exists"))) {
    return true;
  }
  if (messageLower.includes("bind message supplies") && messageLower.includes("prepared statement")) {
    return true;
  }
  return false;
}
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function isPostgresUrl(value) {
  return /^postgres(ql)?:\/\//i.test(value);
}
function isPrismaProtocolUrl(value) {
  return /^prisma(\+postgres)?:\/\//i.test(value.trim());
}
function syncPrismaEnvWithResolvedPostgresUrl(resolvedUrl) {
  if (!isPostgresUrl(resolvedUrl)) return;
  const prevDb = process.env.DATABASE_URL?.trim() ?? "";
  if (prevDb && !isPostgresUrl(prevDb)) {
    if (process.env.NODE_ENV === "development") {
      console.warn(
        "[Prisma] Replacing non-postgres DATABASE_URL in process.env with the resolved postgres URL (from lib/env/database-url.ts). Use postgresql:// for direct DB access; prisma:// is only for Accelerate."
      );
    }
  }
  process.env.DATABASE_URL = resolvedUrl;
  const prevDirect = process.env.DIRECT_URL?.trim() ?? "";
  if (!prevDirect || !isPostgresUrl(prevDirect) || isPrismaProtocolUrl(prevDirect)) {
    process.env.DIRECT_URL = resolvedUrl;
  }
}
function normalizePrismaEngineForDatabaseUrl(databaseUrl) {
  if (!isPostgresUrl(databaseUrl)) return;
  const engineType = process.env.PRISMA_CLIENT_ENGINE_TYPE?.trim().toLowerCase();
  if (engineType === "dataproxy" || engineType === "data-proxy" || engineType === "accelerate") {
    process.env.PRISMA_CLIENT_ENGINE_TYPE = "library";
  }
}
function createBuildPhaseStubClient() {
  const noopAsync = async () => null;
  const emptyArrayAsync = async () => [];
  const zeroAsync = async () => 0;
  const emptyObjectAsync = async () => ({});
  const modelHandler = {
    get(_target, prop) {
      const name = String(prop);
      if (name === "findMany" || name === "groupBy") return emptyArrayAsync;
      if (name === "count") return zeroAsync;
      if (name === "aggregate") return emptyObjectAsync;
      if (name === "findUnique" || name === "findFirst" || name === "findUniqueOrThrow" || name === "findFirstOrThrow") {
        return noopAsync;
      }
      return noopAsync;
    }
  };
  const stubClient = {};
  const handler = {
    get(_target, prop) {
      const name = String(prop);
      if (name === "$transaction") {
        return async (arg) => {
          if (typeof arg === "function") {
            return arg(stubProxy);
          }
          if (Array.isArray(arg)) return arg.map(() => null);
          return null;
        };
      }
      if (name === "$queryRaw" || name === "$queryRawUnsafe") return emptyArrayAsync;
      if (name === "$executeRaw" || name === "$executeRawUnsafe") return zeroAsync;
      if (name === "$connect" || name === "$disconnect") return async () => void 0;
      if (name === "$on" || name === "$use") return () => void 0;
      if (name === "$extends") return () => stubProxy;
      return new Proxy({}, modelHandler);
    }
  };
  const stubProxy = new Proxy(stubClient, handler);
  return stubProxy;
}
function createPrismaClient() {
  if (process.env.NEXT_PHASE === "phase-production-build") {
    return createBuildPhaseStubClient();
  }
  let databaseUrl;
  try {
    databaseUrl = applyPrismaConnectionGuardrails(getDatabaseUrlOrThrow());
  } catch (err) {
    if (isDomRuntime()) {
      databaseUrl = applyPrismaConnectionGuardrails(
        "postgresql://noop:noop@localhost:5432/noop"
      );
    } else {
      throw err;
    }
  }
  syncPrismaEnvWithResolvedPostgresUrl(databaseUrl);
  normalizePrismaEngineForDatabaseUrl(databaseUrl);
  const client = new import_client.PrismaClient({
    datasourceUrl: databaseUrl,
    log: process.env.NODE_ENV === "development" ? ["warn", "error"] : ["error"]
  });
  return client.$extends({
    query: {
      async $allOperations({
        model,
        operation,
        args,
        query
      }) {
        return observeDbOperation({ model, operation }, async () => {
          const isReadOperation = READ_OPERATIONS.has(operation);
          const retryCount = isReadOperation ? 3 : 1;
          for (let retryAttempt = 0; retryAttempt <= retryCount; retryAttempt++) {
            try {
              return await query(args);
            } catch (error) {
              const isLastAttempt = retryAttempt === retryCount;
              if (!isConnectionError(error) || isLastAttempt) {
                throw error;
              }
              const backoffMs = 150 * Math.pow(2, retryAttempt) + Math.floor(Math.random() * 50);
              if (process.env.NODE_ENV !== "production") {
                console.warn(
                  `[Prisma] Retrying ${operation} after transient connection error (retry ${retryAttempt + 1} of ${retryCount}, waiting ${backoffMs}ms)`
                );
              }
              await sleep(backoffMs);
            }
          }
          throw new Error("Prisma retry loop exited unexpectedly.");
        });
      }
    }
  });
}
var import_client, READ_OPERATIONS, globalForPrisma, prisma;
var init_prisma = __esm({
  "lib/prisma.ts"() {
    "use strict";
    init_prisma_connection_guardrails();
    import_client = require("@prisma/client");
    init_database_url();
    init_dbTelemetry();
    READ_OPERATIONS = /* @__PURE__ */ new Set([
      "findUnique",
      "findUniqueOrThrow",
      "findFirst",
      "findFirstOrThrow",
      "findMany",
      "count",
      "aggregate",
      "groupBy"
    ]);
    globalForPrisma = globalThis;
    prisma = typeof window !== "undefined" ? null : globalForPrisma.prisma ?? createPrismaClient();
    if (process.env.NODE_ENV !== "production" && typeof window === "undefined") {
      globalForPrisma.prisma = prisma;
    }
  }
});

// scripts/_audit-server-only-shim.cjs
var require_audit_server_only_shim = __commonJS({
  "scripts/_audit-server-only-shim.cjs"(exports2, module2) {
    "use strict";
    module2.exports = {};
  }
});

// lib/cfbd-fetch.ts
var CFBD_BASE_URL;
var init_cfbd_fetch = __esm({
  "lib/cfbd-fetch.ts"() {
    "use strict";
    CFBD_BASE_URL = "https://api.collegefootballdata.com";
  }
});

// lib/providers/espnUrls.ts
var ESPN_SITE_API_BASE;
var init_espnUrls = __esm({
  "lib/providers/espnUrls.ts"() {
    "use strict";
    ESPN_SITE_API_BASE = "https://site.web.api.espn.com/apis/site/v2/sports";
  }
});

// lib/scores/gameScoreProviders.ts
function normalizeSeasonType(raw) {
  if (raw == null) return null;
  if (typeof raw === "number") {
    return raw === 1 ? "pre" : raw === 2 ? "regular" : raw === 3 ? "post" : null;
  }
  const v = String(raw).trim().toLowerCase();
  if (!v) return null;
  if (v.includes("preseason") || v.includes("pre season") || v.includes("pre-season")) return "pre";
  if (v === "pre" || v === "1") return "pre";
  if (v.includes("postseason") || v.includes("post season") || v.includes("post-season")) return "post";
  if (v.includes("playoff") || v.includes("championship")) return "post";
  if (v === "post" || v === "3") return "post";
  if (v.includes("regular")) return "regular";
  if (v === "reg" || v === "2") return "regular";
  return null;
}
var import_server_only;
var init_gameScoreProviders = __esm({
  "lib/scores/gameScoreProviders.ts"() {
    "use strict";
    import_server_only = __toESM(require_audit_server_only_shim());
    init_cfbd_fetch();
    init_espnUrls();
  }
});

// lib/season-week/leagueSeasonWeek.ts
function resolveRegularSeasonEndWeek(shape) {
  return Math.min(shape.totalWeeks, Math.max(1, shape.playoffStartWeek - 1));
}
function phaseFor(fantasyWeek, shape) {
  if (fantasyWeek < 1) return "preseason";
  if (shape.lastPlayoffWeek != null && fantasyWeek > shape.lastPlayoffWeek) {
    return "season_complete";
  }
  return fantasyWeek >= shape.playoffStartWeek ? "playoffs" : "regular";
}
function mapSportWeekToLeagueWeek(sportWeek, shape) {
  if (!sportWeek.ok) return sportWeek;
  const anchor = Math.max(1, shape.anchorSportWeek ?? 1);
  const rawFantasyWeek = sportWeek.sportWeek - anchor + 1;
  if (rawFantasyWeek < 1 || sportWeek.state === "upcoming") {
    return {
      ok: true,
      fantasyWeek: 1,
      sportWeek: sportWeek.sportWeek,
      phase: "preseason",
      state: sportWeek.state,
      sportWeekComplete: false,
      slate: null,
      source: "schedule"
    };
  }
  return {
    ok: true,
    fantasyWeek: rawFantasyWeek,
    sportWeek: sportWeek.sportWeek,
    phase: phaseFor(rawFantasyWeek, shape),
    state: sportWeek.state,
    // `allFinal` alone is not enough: a slate whose rows are all final but whose
    // last kickoff is still in the future is a feed artifact, not a played week.
    sportWeekComplete: sportWeek.state === "played",
    slate: sportWeek.slate,
    source: "schedule"
  };
}
var init_leagueSeasonWeek = __esm({
  "lib/season-week/leagueSeasonWeek.ts"() {
    "use strict";
  }
});

// lib/sports/gameStatus.ts
function normalizeGameStatus(raw) {
  if (raw == null) return "unknown";
  const s = String(raw).trim().toLowerCase();
  if (s.length === 0) return "unknown";
  if (FINAL.has(s)) return "final";
  if (SCHEDULED.has(s)) return "scheduled";
  if (POSTPONED.has(s)) return "postponed";
  if (CANCELLED.has(s)) return "cancelled";
  if (LIVE_EXACT.has(s) || LIVE_PREFIX.test(s)) return "live";
  return "unknown";
}
var FINAL, SCHEDULED, POSTPONED, CANCELLED, LIVE_EXACT, LIVE_PREFIX;
var init_gameStatus = __esm({
  "lib/sports/gameStatus.ts"() {
    "use strict";
    FINAL = /* @__PURE__ */ new Set([
      "ft",
      "final",
      "aot",
      "ap",
      "aet",
      "pen",
      "match finished",
      "finished",
      "complete",
      "completed",
      "closed",
      "game over",
      "f",
      "f/ot",
      "awarded"
    ]);
    SCHEDULED = /* @__PURE__ */ new Set([
      "scheduled",
      "ns",
      "not started",
      "pre",
      "pregame",
      "upcoming",
      "tbd",
      "time to be defined"
    ]);
    POSTPONED = /* @__PURE__ */ new Set(["post", "pst", "postponed", "susp", "suspended", "delayed", "int"]);
    CANCELLED = /* @__PURE__ */ new Set(["canc", "cancelled", "canceled", "abd", "abandoned", "awd", "wo", "replaced"]);
    LIVE_EXACT = /* @__PURE__ */ new Set([
      "in_progress",
      "live",
      "inprogress",
      "ht",
      "halftime",
      "et",
      "bt",
      "brk",
      "break",
      "pen_live",
      "in play"
    ]);
    LIVE_PREFIX = /^(?:in|q|p|h|ot|otb)\d+$|^\d+h$/;
  }
});

// lib/live-scoring/types.ts
var init_types = __esm({
  "lib/live-scoring/types.ts"() {
    "use strict";
  }
});

// lib/live-scoring/cadence.ts
function normalizeLiveGameStatus(raw) {
  const key = String(raw ?? "").trim().toLowerCase().replace(/\s+/g, "_");
  const mapped = RAW_STATUS_MAP[key];
  if (mapped) return mapped;
  switch (normalizeGameStatus(raw)) {
    case "final":
      return "final";
    case "live":
      return "in_progress";
    case "postponed":
      return "postponed";
    // This vocabulary has no `cancelled`; the table already files `canceled`
    // under `postponed`, and both mean the same thing to a poller: stop.
    case "cancelled":
      return "postponed";
    case "scheduled":
      return "scheduled";
    default:
      return "scheduled";
  }
}
var RAW_STATUS_MAP;
var init_cadence = __esm({
  "lib/live-scoring/cadence.ts"() {
    "use strict";
    init_gameStatus();
    init_types();
    RAW_STATUS_MAP = {
      // not started
      scheduled: "scheduled",
      pre: "scheduled",
      pregame: "scheduled",
      upcoming: "scheduled",
      ns: "scheduled",
      not_started: "scheduled",
      tbd: "scheduled",
      status_scheduled: "scheduled",
      // running
      in_progress: "in_progress",
      inprogress: "in_progress",
      live: "in_progress",
      playing: "in_progress",
      status_in_progress: "in_progress",
      q1: "in_progress",
      q2: "in_progress",
      q3: "in_progress",
      q4: "in_progress",
      "1q": "in_progress",
      "2q": "in_progress",
      "3q": "in_progress",
      "4q": "in_progress",
      first_quarter: "in_progress",
      second_quarter: "in_progress",
      third_quarter: "in_progress",
      fourth_quarter: "in_progress",
      // halftime
      halftime: "halftime",
      half: "halftime",
      ht: "halftime",
      status_halftime: "halftime",
      // overtime
      overtime: "overtime",
      ot: "overtime",
      status_end_period_ot: "overtime",
      // final
      final: "final",
      "final/ot": "final",
      ft: "final",
      finished: "final",
      match_finished: "final",
      full_time: "final",
      // "After Over Time" (AOT) is TERMINAL — a game that ENDED in overtime, not one
      // still being played. Filing it under 'overtime' would mean it never reads as
      // final, which is the exact bug this block exists to close.
      after_over_time: "final",
      aot: "final",
      complete: "final",
      completed: "final",
      closed: "final",
      status_final: "final",
      // suspended / postponed
      suspended: "suspended",
      delayed: "suspended",
      status_suspended: "suspended",
      postponed: "postponed",
      ppd: "postponed",
      canceled: "postponed",
      cancelled: "postponed",
      status_postponed: "postponed"
    };
  }
});

// lib/security/redactSecrets.ts
function redactSecrets(input) {
  let text = typeof input === "string" ? input : input instanceof Error ? input.message : String(input);
  for (const [pattern, replacement] of RULES) {
    text = text.replace(pattern, replacement);
  }
  return text;
}
function redactAndCap(input, maxLength) {
  return redactSecrets(input).slice(0, Math.max(0, maxLength));
}
var RULES;
var init_redactSecrets = __esm({
  "lib/security/redactSecrets.ts"() {
    "use strict";
    RULES = [
      // 1. Credentials embedded in a URL's authority: postgres://user:password@host/db.
      //    First, because rule 5 would otherwise redact only a trailing fragment of the password.
      [/\b([a-z][a-z0-9+.-]*:\/\/)([^:@/\s]+):([^@/\s]+)@/gi, "$1$2:***@"],
      // 2. TheSportsDB puts its API key in a PATH SEGMENT, not a query string, so no generic
      //    key=value rule can ever catch it: .../api/v1/json/<KEY>/eventsday.php
      [/(thesportsdb\.com\/api\/v\d+\/json\/)[^/\s"']+/gi, "$1***"],
      // 3. Rolling Insights. Needs its own rule: `_` is a word character, so rule 5's `token` never
      //    matches inside `RSC_token`.
      //
      //    NO \b ANCHOR, here or in rule 5. A leading \b requires a non-word character before the
      //    match, so a credential concatenated straight onto preceding text — which is exactly what a
      //    truncated or joined log line looks like — slips through untouched. Over-redaction is safe;
      //    a miss is a leak. The cost is that `monkey=1` redacts as `monkey=***`, which is fine in a
      //    telemetry field.
      [/RSC_?token(["']?\s*[=:]\s*["']?)[^&\s"',;}]+/gi, "RSC_token$1***"],
      // 4. Authorization headers, in either header or prose form.
      [/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]{8,}/gi, "$1 ***"],
      // 5. Generic credential-bearing parameters, in query-string, header and JSON form alike. The
      //    quotes belong INSIDE the separator group: a JSON body reads `"client_secret":"v"`, so a
      //    separator of just `[=:]` never matches — the key's closing quote sits in the way.
      [
        /(api[_-]?key|apikey|access[_-]?token|auth[_-]?token|refresh[_-]?token|id[_-]?token|client[_-]?secret|session[_-]?token|token|secret|password|passwd|pwd|signature|sig|key)(["']?\s*[=:]\s*["']?)[^&\s"',;}]+/gi,
        "$1$2***"
      ],
      // 6. Shapes that are recognisable on their own, with no `key=` in front of them. These are the
      //    ones that leak when a provider echoes a credential back inside a message body.
      [/\bsk-ant-[A-Za-z0-9_-]+/g, "sk-ant-***"],
      //    The lookaheads keep this from re-redacting rule 6's own output: without them `sk-ant-***`
      //    is matched again on `sk-ant-` and becomes `sk-******`, which loses which vendor it was.
      [/\bsk-(?!ant-)(?!\*)[A-Za-z0-9_-]+/g, "sk-***"],
      // pre-existing behaviour, kept deliberately loose
      [/\bgithub_pat_[A-Za-z0-9_]{20,}/g, "github_pat_***"],
      [/\bghp_[A-Za-z0-9]{16,}/g, "ghp_***"],
      [/\bgh[opsu]_[A-Za-z0-9]{16,}/g, "gh_***"],
      [/\bnpg_[A-Za-z0-9]{12,}/g, "npg_***"],
      // Neon
      [/\bAKIA[0-9A-Z]{16}\b/g, "AKIA***"],
      // AWS access key id
      [/\bxox[baprs]-[A-Za-z0-9-]{10,}/g, "xox-***"],
      // Slack
      [/\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+/g, "<jwt>"]
    ];
  }
});

// lib/team-abbrev.ts
function normalizeTeamAbbrev(raw) {
  if (!raw) return null;
  const upper = raw.trim().toUpperCase();
  if (!upper) return null;
  if (CANONICAL_TEAMS[upper]) return upper;
  const alias = ALIAS_MAP[upper];
  if (alias) return alias;
  for (const [canonical, info] of Object.entries(CANONICAL_TEAMS)) {
    if (info.fullName.toLowerCase() === upper.toLowerCase() || info.mascot.toLowerCase() === upper.toLowerCase() || info.city.toLowerCase() === upper.toLowerCase()) {
      return canonical;
    }
  }
  return upper;
}
function getAllCanonicalTeams() {
  return Object.entries(CANONICAL_TEAMS).map(([abbrev, info]) => ({
    abbrev,
    ...info
  }));
}
function normalizePlayerName(name) {
  const base = name.trim().toLowerCase().replace(/[^a-z0-9\s'-]/gi, "").replace(/\s+/g, " ").trim();
  if (!base) return "";
  return base.split(" ").filter((token, index) => index === 0 || !GENERATIONAL_SUFFIX.test(token)).join(" ");
}
var CANONICAL_TEAMS, ALIAS_MAP, GENERATIONAL_SUFFIX;
var init_team_abbrev = __esm({
  "lib/team-abbrev.ts"() {
    "use strict";
    CANONICAL_TEAMS = {
      ARI: { canonical: "ARI", fullName: "Arizona Cardinals", city: "Arizona", mascot: "Cardinals" },
      ATL: { canonical: "ATL", fullName: "Atlanta Falcons", city: "Atlanta", mascot: "Falcons" },
      BAL: { canonical: "BAL", fullName: "Baltimore Ravens", city: "Baltimore", mascot: "Ravens" },
      BUF: { canonical: "BUF", fullName: "Buffalo Bills", city: "Buffalo", mascot: "Bills" },
      CAR: { canonical: "CAR", fullName: "Carolina Panthers", city: "Carolina", mascot: "Panthers" },
      CHI: { canonical: "CHI", fullName: "Chicago Bears", city: "Chicago", mascot: "Bears" },
      CIN: { canonical: "CIN", fullName: "Cincinnati Bengals", city: "Cincinnati", mascot: "Bengals" },
      CLE: { canonical: "CLE", fullName: "Cleveland Browns", city: "Cleveland", mascot: "Browns" },
      DAL: { canonical: "DAL", fullName: "Dallas Cowboys", city: "Dallas", mascot: "Cowboys" },
      DEN: { canonical: "DEN", fullName: "Denver Broncos", city: "Denver", mascot: "Broncos" },
      DET: { canonical: "DET", fullName: "Detroit Lions", city: "Detroit", mascot: "Lions" },
      GB: { canonical: "GB", fullName: "Green Bay Packers", city: "Green Bay", mascot: "Packers" },
      HOU: { canonical: "HOU", fullName: "Houston Texans", city: "Houston", mascot: "Texans" },
      IND: { canonical: "IND", fullName: "Indianapolis Colts", city: "Indianapolis", mascot: "Colts" },
      JAX: { canonical: "JAX", fullName: "Jacksonville Jaguars", city: "Jacksonville", mascot: "Jaguars" },
      KC: { canonical: "KC", fullName: "Kansas City Chiefs", city: "Kansas City", mascot: "Chiefs" },
      LAC: { canonical: "LAC", fullName: "Los Angeles Chargers", city: "Los Angeles", mascot: "Chargers" },
      LAR: { canonical: "LAR", fullName: "Los Angeles Rams", city: "Los Angeles", mascot: "Rams" },
      LV: { canonical: "LV", fullName: "Las Vegas Raiders", city: "Las Vegas", mascot: "Raiders" },
      MIA: { canonical: "MIA", fullName: "Miami Dolphins", city: "Miami", mascot: "Dolphins" },
      MIN: { canonical: "MIN", fullName: "Minnesota Vikings", city: "Minnesota", mascot: "Vikings" },
      NE: { canonical: "NE", fullName: "New England Patriots", city: "New England", mascot: "Patriots" },
      NO: { canonical: "NO", fullName: "New Orleans Saints", city: "New Orleans", mascot: "Saints" },
      NYG: { canonical: "NYG", fullName: "New York Giants", city: "New York", mascot: "Giants" },
      NYJ: { canonical: "NYJ", fullName: "New York Jets", city: "New York", mascot: "Jets" },
      PHI: { canonical: "PHI", fullName: "Philadelphia Eagles", city: "Philadelphia", mascot: "Eagles" },
      PIT: { canonical: "PIT", fullName: "Pittsburgh Steelers", city: "Pittsburgh", mascot: "Steelers" },
      SEA: { canonical: "SEA", fullName: "Seattle Seahawks", city: "Seattle", mascot: "Seahawks" },
      SF: { canonical: "SF", fullName: "San Francisco 49ers", city: "San Francisco", mascot: "49ers" },
      TB: { canonical: "TB", fullName: "Tampa Bay Buccaneers", city: "Tampa Bay", mascot: "Buccaneers" },
      TEN: { canonical: "TEN", fullName: "Tennessee Titans", city: "Tennessee", mascot: "Titans" },
      WAS: { canonical: "WAS", fullName: "Washington Commanders", city: "Washington", mascot: "Commanders" }
    };
    ALIAS_MAP = {
      JAC: "JAX",
      WSH: "WAS",
      GNB: "GB",
      GBP: "GB",
      KCC: "KC",
      NWE: "NE",
      SFO: "SF",
      TAM: "TB",
      TBB: "TB",
      NOR: "NO",
      SDG: "LAC",
      STL: "LAR",
      LA: "LAR",
      OAK: "LV",
      RAI: "LV",
      RAM: "LAR",
      CLT: "IND",
      RAV: "BAL",
      HTX: "HOU",
      CRD: "ARI",
      WFT: "WAS",
      WST: "WAS"
    };
    GENERATIONAL_SUFFIX = /^(jr|sr|ii|iii|iv|v)$/i;
  }
});

// lib/live/teamAbbreviation.ts
var init_teamAbbreviation = __esm({
  "lib/live/teamAbbreviation.ts"() {
    "use strict";
    init_team_abbrev();
  }
});

// lib/sport-scope.ts
function normalizeToSupportedSport(sport) {
  const raw = sport?.trim() ?? "";
  if (!raw) return DEFAULT_SPORT;
  const u = raw.toUpperCase().replace(/[\s-]+/g, "_");
  if (u === "NCAAFB" || u === "CFB" || u === "NCAAF" || u === "NCAA_FOOTBALL" || u === "COLLEGE_FOOTBALL" || u === "NCAA_FB" || u === "NCAA_F") {
    return "NCAAF";
  }
  if (u === "NCAABB" || u === "NCAA_BASKETBALL" || u === "NCAAM" || u === "NCAAB_ALL") {
    return "NCAAB";
  }
  if (u === "EURO" || u === "UEFA" || u === "EURO_SOCCER" || u === "EPL" || u === "PREMIER_LEAGUE" || u === "ENGLISH_PREMIER_LEAGUE" || u === "LALIGA" || u === "LA_LIGA" || u === "LA_LIGA_SPAIN" || u === "SERIEA" || u === "SERIE_A" || u === "ITALIAN_SERIE_A") {
    return "SOCCER";
  }
  if (SUPPORTED_SPORTS.includes(u)) return u;
  return DEFAULT_SPORT;
}
var SUPPORTED_SPORTS, DEFAULT_SPORT;
var init_sport_scope = __esm({
  "lib/sport-scope.ts"() {
    "use strict";
    SUPPORTED_SPORTS = [
      "NFL",
      "NBA",
      "NHL",
      "MLB",
      "NCAAF",
      "NCAAB",
      "SOCCER"
    ];
    DEFAULT_SPORT = SUPPORTED_SPORTS[0];
  }
});

// lib/workers/api-config.ts
function isRollingInsightsEnabledForSport(sport) {
  if (typeof sport === "string") {
    return toApiChainSport(sport) != null;
  }
  return true;
}
function isSupportedApiChainSport(value) {
  return SUPPORTED_SPORTS2.includes(value.toLowerCase());
}
function toApiChainSport(input) {
  if (!input) return null;
  const raw = String(input).trim();
  const lower = raw.toLowerCase();
  if (isSupportedApiChainSport(lower)) return lower;
  const map = {
    nfl: "nfl",
    nba: "nba",
    mlb: "mlb",
    nhl: "nhl",
    ncaab: "ncaab",
    ncaam: "ncaab",
    ncaabasketball: "ncaab",
    ncaa_basketball: "ncaab",
    ncaaf: "ncaaf",
    cfb: "ncaaf",
    ncaafb: "ncaaf",
    ncaa_football: "ncaaf",
    soccer: "soccer_euro",
    soccer_euro: "soccer_euro",
    euro: "soccer_euro",
    epl: "soccer_euro",
    /** MLS / NA soccer — distinct Rolling Insights / feed chain from European soccer. */
    mls: "soccer_mls",
    soccer_mls: "soccer_mls"
  };
  if (map[lower]) return map[lower];
  try {
    const normalized = normalizeToSupportedSport(raw);
    return legacySupportedSportToApiChain(normalized);
  } catch {
    return null;
  }
}
function legacySupportedSportToApiChain(sport) {
  const m = {
    NFL: "nfl",
    NBA: "nba",
    MLB: "mlb",
    NHL: "nhl",
    NCAAB: "ncaab",
    NCAAF: "ncaaf",
    SOCCER: "soccer_euro"
  };
  return m[sport];
}
function apiChainSportToDbSport(sport) {
  const m = {
    nfl: "NFL",
    nba: "NBA",
    mlb: "MLB",
    nhl: "NHL",
    ncaab: "NCAAB",
    ncaaf: "NCAAF",
    soccer_euro: "SOCCER",
    soccer_mls: "SOCCER"
  };
  return m[sport];
}
function envFlag(value, fallback) {
  if (value == null || value.trim() === "") return fallback;
  return /^(1|true|yes|on)$/i.test(value.trim());
}
function ttlSecondsForDataType(dataType) {
  const key = dataType;
  if (key in API_CHAIN_TTLS) return API_CHAIN_TTLS[key];
  if (dataType === "games") return API_CHAIN_TTLS.scores;
  return 900;
}
var SUPPORTED_SPORTS2, API_CHAIN_TTLS, ROLLING_INSIGHTS_SPORTS;
var init_api_config = __esm({
  "lib/workers/api-config.ts"() {
    "use strict";
    init_sport_scope();
    SUPPORTED_SPORTS2 = [
      "nfl",
      "nba",
      "nhl",
      "mlb",
      "ncaaf",
      "ncaab",
      "soccer_euro",
      /** MLS / North America soccer data chain (distinct from {@link soccer_euro}). */
      "soccer_mls"
    ];
    API_CHAIN_TTLS = {
      scores: 60,
      live_game: 30,
      injuries: 900,
      news: 300,
      trending: 600,
      players: 86400,
      teams: 86400,
      schedule: 3600,
      standings: 1800,
      projections: 3600,
      rankings: 3600,
      player_headshots: 604800,
      team_logos: 604800,
      roster: 3600,
      rolling_insights: 1800,
      adp: 21600
    };
    ROLLING_INSIGHTS_SPORTS = {
      NFL: envFlag(process.env.RI_NFL_ENABLED, true),
      NBA: envFlag(process.env.RI_NBA_ENABLED, false),
      MLB: envFlag(process.env.RI_MLB_ENABLED, false),
      NHL: envFlag(process.env.RI_NHL_ENABLED, false),
      NCAAF: envFlag(process.env.RI_NCAAF_ENABLED, false),
      NCAAB: envFlag(process.env.RI_NCAAB_ENABLED, false),
      SOCCER: envFlag(process.env.RI_SOCCER_ENABLED, false),
      Soccer: envFlag(process.env.RI_SOCCER_ENABLED, false)
    };
  }
});

// lib/workers/rate-limit-manager.ts
var rate_limit_manager_exports = {};
__export(rate_limit_manager_exports, {
  RATE_LIMITS: () => RATE_LIMITS,
  RateLimitManager: () => RateLimitManager,
  rateLimitManager: () => rateLimitManager
});
function normalizeProvider(provider) {
  const key = provider.trim().toLowerCase();
  return key in RATE_LIMITS ? key : null;
}
function startOfHour(date) {
  const next = new Date(date);
  next.setMinutes(0, 0, 0);
  return next;
}
function endOfHour(date) {
  return new Date(startOfHour(date).getTime() + 60 * 60 * 1e3);
}
function startOfDay(date) {
  const next = new Date(date);
  next.setHours(0, 0, 0, 0);
  return next;
}
function endOfDay(date) {
  return new Date(startOfDay(date).getTime() + 24 * 60 * 60 * 1e3);
}
var import_server_only2, RATE_LIMITS, RateLimitManager, rateLimitManager;
var init_rate_limit_manager = __esm({
  "lib/workers/rate-limit-manager.ts"() {
    "use strict";
    import_server_only2 = __toESM(require_audit_server_only_shim());
    init_prisma();
    RATE_LIMITS = {
      clearsports: { callsPerHour: 100, callsPerDay: 1e3 },
      // API-Sports Pro plan (status endpoint): 7,500/day. Keep hourly guard conservative.
      api_sports: { callsPerHour: 300, callsPerDay: 7500 },
      // Soccer integration (`lib/api-football.ts`) uses this provider key.
      api_football: { callsPerHour: 300, callsPerDay: 7500 },
      rolling_insights: { callsPerHour: 1e3, callsPerDay: 1e4 },
      thesportsdb: { callsPerHour: 6e3, callsPerDay: 1e5 },
      // premium: 100/min = 6000/hr (free is 30/min)
      cfbd: { callsPerHour: 100, callsPerDay: 1e3 },
      sleeper: { callsPerHour: 1e3, callsPerDay: 1e4 },
      yahoo: { callsPerHour: 200, callsPerDay: 2e3 },
      espn: { callsPerHour: 100, callsPerDay: 1e3 },
      mfl: { callsPerHour: 60, callsPerDay: 500 },
      fantrax: { callsPerHour: 60, callsPerDay: 500 },
      anthropic: { callsPerHour: 500, callsPerDay: 5e3 },
      openai: { callsPerHour: 200, callsPerDay: 2e3 },
      elevenlabs: { callsPerHour: 100, callsPerDay: 1e3 },
      resend: { callsPerHour: 100, callsPerDay: 1e3 },
      klipy: { callsPerHour: 120, callsPerDay: 1e3 },
      giphy: { callsPerHour: 120, callsPerDay: 1e3 },
      tenor: { callsPerHour: 120, callsPerDay: 1e3 }
    };
    RateLimitManager = class {
      async canCall(provider, endpoint) {
        const normalized = normalizeProvider(provider);
        if (!normalized) return true;
        try {
          const limits = RATE_LIMITS[normalized];
          const now = /* @__PURE__ */ new Date();
          const hourStart = startOfHour(now);
          const hourEnd = endOfHour(now);
          const dayStart = startOfDay(now);
          const dayEnd = endOfDay(now);
          const [hourlyUsage, dailyUsage] = await Promise.all([
            prisma.apiRateLimitRecord.aggregate({
              _sum: { callsMade: true },
              where: {
                provider: normalized,
                windowStart: hourStart,
                windowEnd: hourEnd
              }
            }),
            prisma.apiCallLogRecord.count({
              where: {
                provider: normalized,
                cached: false,
                calledAt: {
                  gte: dayStart,
                  lt: dayEnd
                }
              }
            })
          ]);
          const hourlyUsed = hourlyUsage._sum.callsMade ?? 0;
          return hourlyUsed < limits.callsPerHour && dailyUsage < limits.callsPerDay;
        } catch {
          return true;
        }
      }
      async recordCall(provider, endpoint, status, latencyMs, options) {
        const normalized = normalizeProvider(provider) ?? provider.trim().toLowerCase();
        const cached = Boolean(options?.cached);
        const now = /* @__PURE__ */ new Date();
        const hourStart = startOfHour(now);
        const hourEnd = endOfHour(now);
        const callsLimit = normalizeProvider(provider) ? RATE_LIMITS[normalized].callsPerHour : 0;
        try {
          await prisma.$transaction([
            prisma.apiCallLogRecord.create({
              data: {
                provider: normalized,
                endpoint,
                status,
                latencyMs,
                error: options?.error?.slice(0, 500) ?? null,
                cached,
                calledAt: now
              }
            }),
            prisma.apiRateLimitRecord.upsert({
              where: {
                uniq_api_rate_limits_window: {
                  provider: normalized,
                  endpoint,
                  windowStart: hourStart,
                  windowEnd: hourEnd
                }
              },
              update: {
                callsMade: cached ? void 0 : { increment: 1 },
                callsLimit
              },
              create: {
                provider: normalized,
                endpoint,
                callsMade: cached ? 0 : 1,
                callsLimit,
                windowStart: hourStart,
                windowEnd: hourEnd
              }
            })
          ]);
        } catch {
        }
      }
      async getUsage(provider) {
        const normalized = normalizeProvider(provider);
        const now = /* @__PURE__ */ new Date();
        const hourStart = startOfHour(now);
        const hourEnd = endOfHour(now);
        if (!normalized) {
          return { used: 0, limit: 0, resetAt: hourEnd };
        }
        try {
          const usage = await prisma.apiRateLimitRecord.aggregate({
            _sum: { callsMade: true },
            where: {
              provider: normalized,
              windowStart: hourStart,
              windowEnd: hourEnd
            }
          });
          return {
            used: usage._sum.callsMade ?? 0,
            limit: RATE_LIMITS[normalized].callsPerHour,
            resetAt: hourEnd
          };
        } catch {
          return {
            used: 0,
            limit: RATE_LIMITS[normalized].callsPerHour,
            resetAt: hourEnd
          };
        }
      }
      async getFallback(provider, dataType) {
        const normalizedProvider = provider.trim().toLowerCase();
        const normalizedType = dataType.trim().toLowerCase();
        switch (normalizedType) {
          case "teams":
            return prisma.sportsTeam.findMany({
              take: 100,
              orderBy: { updatedAt: "desc" }
            });
          case "players":
            return prisma.sportsPlayerRecord.findMany({
              take: 100,
              orderBy: { lastUpdated: "desc" }
            });
          case "player_headshots":
            return prisma.sportsPlayerRecord.findMany({
              where: {
                OR: [
                  { headshotUrl: { not: null } },
                  { logoUrl: { not: null } }
                ]
              },
              take: 100,
              orderBy: { lastUpdated: "desc" }
            });
          case "team_logos":
            return prisma.teamAsset.findMany({
              take: 100,
              orderBy: { lastUpdated: "desc" }
            });
          case "injuries":
          case "injury_reports":
            return prisma.injuryReportRecord.findMany({
              take: 100,
              orderBy: { reportDate: "desc" }
            });
          case "schedule":
          case "schedules":
          case "games":
            return prisma.gameSchedule.findMany({
              take: 100,
              orderBy: { startTime: "asc" }
            });
          case "adp":
            return prisma.adpDataRecord.findMany({
              take: 100,
              orderBy: [{ season: "desc" }, { week: "desc" }, { adp: "asc" }]
            });
          case "news":
            return prisma.playerNewsRecord.findMany({
              take: 100,
              orderBy: { publishedAt: "desc" }
            });
          default:
            return {
              provider: normalizedProvider,
              dataType: normalizedType,
              data: null
            };
        }
      }
    };
    rateLimitManager = new RateLimitManager();
  }
});

// lib/realtime-events/injuryFanoutPolicy.ts
var init_injuryFanoutPolicy = __esm({
  "lib/realtime-events/injuryFanoutPolicy.ts"() {
    "use strict";
  }
});

// lib/odds/normalizeApiSportsOdds.ts
var init_normalizeApiSportsOdds = __esm({
  "lib/odds/normalizeApiSportsOdds.ts"() {
    "use strict";
  }
});

// lib/api-sports.ts
function resolveLeagueId(sport) {
  return sport === "NCAAF" ? APISPORTS_LEAGUE_IDS.NCAAF : APISPORTS_LEAGUE_IDS.NFL;
}
function resolveDbSport(sport) {
  return sport === "NCAAF" ? "NCAAF" : "NFL";
}
function pushApiSportsDiagnostic(row) {
  apiSportsDiagnostics.push(row);
  if (apiSportsDiagnostics.length > MAX_DIAGNOSTIC_ROWS) {
    apiSportsDiagnostics.splice(0, apiSportsDiagnostics.length - MAX_DIAGNOSTIC_ROWS);
  }
}
function teamNameToAbbrev(name) {
  if (!name) return null;
  return normalizeTeamAbbrev(API_SPORTS_TEAM_MAP[name]) || normalizeTeamAbbrev(name) || null;
}
function enqueueRequest(fn) {
  const queued = requestQueue.then(() => fn(), () => fn());
  requestQueue = queued.then(() => {
  }, () => {
  });
  return queued;
}
function getApiSportsKey() {
  return process.env.APISPORTS_API_KEY || process.env.APISPORTS_KEY || process.env.API_SPORTS_KEY || process.env.SPORTS_API_KEY || null;
}
async function apiSportsFetchInternal(endpoint, params, opts) {
  const apiKey3 = getApiSportsKey();
  if (!apiKey3) {
    throw new Error("APISPORTS_API_KEY not configured (accepted aliases: APISPORTS_KEY, API_SPORTS_KEY, SPORTS_API_KEY)");
  }
  if (!opts?.bypassRateGuard && !await rateLimitManager.canCall("api_sports", endpoint)) {
    await rateLimitManager.recordCall("api_sports", endpoint, 429, 0, { cached: true, error: "rate_limit_guard" });
    const fallbackType = endpoint.includes("injur") ? "injuries" : endpoint.includes("game") ? "schedule" : endpoint.includes("player") ? "players" : "players";
    return await rateLimitManager.getFallback("api_sports", fallbackType);
  }
  if (Date.now() < ipBlockedUntil) {
    throw new Error("API-Sports IP blocked \u2014 skipping until cooldown expires");
  }
  if (Date.now() < minuteRateLimitResetAt) {
    const waitMs = minuteRateLimitResetAt - Date.now();
    console.log(`[API-Sports] Rate limit active, waiting ${Math.ceil(waitMs / 1e3)}s`);
    await new Promise((r) => setTimeout(r, waitMs));
  }
  const url = new URL(`${BASE_URL}/${endpoint}`);
  const paramCopy = {};
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      url.searchParams.set(k, v);
      paramCopy[k] = v;
    });
  }
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 3e4);
  try {
    const response = await fetch(url.toString(), {
      method: "GET",
      headers: {
        "x-apisports-key": apiKey3
      },
      signal: controller.signal
    });
    clearTimeout(timeout);
    if (!response.ok) {
      await rateLimitManager.recordCall("api_sports", endpoint, response.status, 0, { error: response.statusText });
      pushApiSportsDiagnostic({
        provider: "api_sports",
        endpoint,
        params: paramCopy,
        url: url.toString(),
        status: response.status,
        ok: false,
        error: `HTTP ${response.status} ${response.statusText}`,
        at: (/* @__PURE__ */ new Date()).toISOString()
      });
      throw new Error(`API-Sports request failed: ${response.status} ${response.statusText}`);
    }
    const remaining = response.headers.get("x-ratelimit-requests-remaining");
    if (remaining && parseInt(remaining) < 5) {
      console.warn(`[API-Sports] Low daily quota: ${remaining} requests remaining`);
    }
    const minuteRemaining = response.headers.get("X-RateLimit-Remaining");
    if (minuteRemaining) {
      const rem = parseInt(minuteRemaining);
      if (rem < 2) {
        minuteRateLimitResetAt = Date.now() + 62e3;
        console.warn(`[API-Sports] Per-minute rate limit nearly hit: ${rem} remaining, pausing new requests for 62s`);
      } else if (rem < 5) {
        await new Promise((r) => setTimeout(r, 3e3));
      }
    }
    const result = await response.json();
    await rateLimitManager.recordCall("api_sports", endpoint, response.status, 0);
    if (result.errors && Object.keys(result.errors).length > 0) {
      const errStr = JSON.stringify(result.errors);
      pushApiSportsDiagnostic({
        provider: "api_sports",
        endpoint,
        params: paramCopy,
        url: url.toString(),
        status: response.status,
        ok: false,
        error: errStr,
        at: (/* @__PURE__ */ new Date()).toISOString()
      });
      if (errStr.includes("IP is not allowed")) {
        console.warn("[API-Sports] IP blocked by API-Sports. Pausing requests for 1 hour.");
        ipBlockedUntil = Date.now() + IP_BLOCK_COOLDOWN_MS;
      }
      const apiError = new Error(`API-Sports error: ${errStr}`);
      if (result.errors && typeof result.errors === "object" && "plan" in result.errors) {
        Object.assign(apiError, { apiSportsPlanBlocked: true });
      }
      throw apiError;
    }
    pushApiSportsDiagnostic({
      provider: "api_sports",
      endpoint,
      params: paramCopy,
      url: url.toString(),
      status: response.status,
      ok: true,
      error: null,
      at: (/* @__PURE__ */ new Date()).toISOString()
    });
    return result.response;
  } catch (error) {
    clearTimeout(timeout);
    pushApiSportsDiagnostic({
      provider: "api_sports",
      endpoint,
      params: paramCopy,
      url: url.toString(),
      status: null,
      ok: false,
      error: error instanceof Error ? error.message : String(error),
      at: (/* @__PURE__ */ new Date()).toISOString()
    });
    await rateLimitManager.recordCall("api_sports", endpoint, 500, 0, {
      error: error instanceof Error ? error.message : String(error)
    });
    throw error;
  }
}
async function apiSportsFetch(endpoint, params, opts) {
  return enqueueRequest(() => apiSportsFetchInternal(endpoint, params, opts));
}
async function fetchAPISportsTeams(season, opts) {
  const currentSeason2 = season || getCurrentNFLSeasonForAPISports();
  const leagueId = resolveLeagueId(opts?.sport);
  const data = await apiSportsFetch("teams", {
    league: leagueId,
    season: currentSeason2
  });
  return data || [];
}
async function fetchAPISportsPlayers(teamId, season) {
  const data = await apiSportsFetch("players", {
    team: teamId,
    season
  });
  return data || [];
}
async function fetchAPISportsPlayerBySearch(search, _season, opts) {
  const sanitized = (search ?? "").replace(/[^A-Za-z0-9\s]/g, " ").replace(/\s+/g, " ").trim();
  if (!sanitized) return [];
  const data = await apiSportsFetch(
    "players",
    { search: sanitized },
    { bypassRateGuard: true }
  );
  const rows = data || [];
  if (rows.length === 0) return [];
  const dbSport = resolveDbSport(opts?.sport);
  const ids = rows.map((p) => String(p.id ?? p.player?.id ?? "")).filter(Boolean);
  const names = rows.map((p) => normalizePlayerName(String(p.name ?? p.player?.name ?? ""))).filter(Boolean);
  const [byIdRows, byNameRows] = await Promise.all([
    ids.length > 0 ? prisma.playerIdentityMap.findMany({
      where: { sport: dbSport, apiSportsId: { in: ids } },
      select: { apiSportsId: true, currentTeam: true }
    }) : Promise.resolve([]),
    names.length > 0 ? prisma.playerIdentityMap.findMany({
      where: { sport: dbSport, normalizedName: { in: names } },
      select: { normalizedName: true, currentTeam: true }
    }) : Promise.resolve([])
  ]);
  const teamById = /* @__PURE__ */ new Map();
  for (const row of byIdRows) {
    if (row.apiSportsId && row.currentTeam) teamById.set(String(row.apiSportsId), row.currentTeam);
  }
  const teamsByName = /* @__PURE__ */ new Map();
  for (const row of byNameRows) {
    if (!row.normalizedName || !row.currentTeam) continue;
    const s = teamsByName.get(row.normalizedName) ?? /* @__PURE__ */ new Set();
    s.add(row.currentTeam);
    teamsByName.set(row.normalizedName, s);
  }
  const teamByName = /* @__PURE__ */ new Map();
  for (const [name, teams] of teamsByName) {
    if (teams.size === 1) teamByName.set(name, [...teams][0]);
  }
  return rows.map((p) => {
    if (p.team?.name) return p;
    const id2 = String(p.id ?? p.player?.id ?? "");
    const normalizedName = normalizePlayerName(String(p.name ?? p.player?.name ?? ""));
    const mappedTeam = teamById.get(id2) || teamByName.get(normalizedName) || null;
    if (!mappedTeam) return p;
    return {
      ...p,
      team: {
        id: 0,
        name: mappedTeam,
        logo: null
      }
    };
  });
}
async function fetchAPISportsInjuries(season, opts) {
  const leagueId = resolveLeagueId(opts?.sport);
  const attempts = [
    { season, league: leagueId },
    void 0
  ];
  let lastError2 = null;
  for (const params of attempts) {
    try {
      const data = await apiSportsFetch("injuries", params);
      return data || [];
    } catch (error) {
      lastError2 = error;
    }
  }
  throw lastError2 instanceof Error ? lastError2 : new Error("Failed to fetch API-Sports injuries");
}
async function fetchAPISportsGames(season, opts) {
  const leagueId = resolveLeagueId(opts?.sport);
  const data = await apiSportsFetch("games", {
    league: leagueId,
    season
  });
  return data || [];
}
function getCurrentNFLSeasonForAPISports() {
  const now = /* @__PURE__ */ new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  return month >= 8 ? String(year) : String(year - 1);
}
var BASE_URL, APISPORTS_LEAGUE_IDS, apiSportsDiagnostics, MAX_DIAGNOSTIC_ROWS, API_SPORTS_TEAM_MAP, ipBlockedUntil, IP_BLOCK_COOLDOWN_MS, PLAN_BLOCK_COOLDOWN_MS, minuteRateLimitResetAt, requestQueue;
var init_api_sports = __esm({
  "lib/api-sports.ts"() {
    "use strict";
    init_prisma();
    init_team_abbrev();
    init_rate_limit_manager();
    init_injuryFanoutPolicy();
    init_gameScoreProviders();
    init_normalizeApiSportsOdds();
    BASE_URL = "https://v1.american-football.api-sports.io";
    APISPORTS_LEAGUE_IDS = {
      NFL: process.env.APISPORTS_NFL_LEAGUE_ID || "1",
      NCAAF: process.env.APISPORTS_NCAAF_LEAGUE_ID || "2"
    };
    apiSportsDiagnostics = [];
    MAX_DIAGNOSTIC_ROWS = 100;
    API_SPORTS_TEAM_MAP = {
      "Arizona Cardinals": "ARI",
      "Atlanta Falcons": "ATL",
      "Baltimore Ravens": "BAL",
      "Buffalo Bills": "BUF",
      "Carolina Panthers": "CAR",
      "Chicago Bears": "CHI",
      "Cincinnati Bengals": "CIN",
      "Cleveland Browns": "CLE",
      "Dallas Cowboys": "DAL",
      "Denver Broncos": "DEN",
      "Detroit Lions": "DET",
      "Green Bay Packers": "GB",
      "Houston Texans": "HOU",
      "Indianapolis Colts": "IND",
      "Jacksonville Jaguars": "JAX",
      "Kansas City Chiefs": "KC",
      "Las Vegas Raiders": "LV",
      "Los Angeles Chargers": "LAC",
      "Los Angeles Rams": "LAR",
      "Miami Dolphins": "MIA",
      "Minnesota Vikings": "MIN",
      "New England Patriots": "NE",
      "New Orleans Saints": "NO",
      "New York Giants": "NYG",
      "New York Jets": "NYJ",
      "Philadelphia Eagles": "PHI",
      "Pittsburgh Steelers": "PIT",
      "San Francisco 49ers": "SF",
      "Seattle Seahawks": "SEA",
      "Tampa Bay Buccaneers": "TB",
      "Tennessee Titans": "TEN",
      "Washington Commanders": "WAS"
    };
    ipBlockedUntil = 0;
    IP_BLOCK_COOLDOWN_MS = 60 * 60 * 1e3;
    PLAN_BLOCK_COOLDOWN_MS = 6 * 60 * 60 * 1e3;
    minuteRateLimitResetAt = 0;
    requestQueue = Promise.resolve();
  }
});

// lib/workers/providers/api-sports.ts
function toSeason(value) {
  if (typeof value === "string" && value.trim()) return value.trim();
  return getCurrentNFLSeasonForAPISports();
}
function toSearch(value) {
  return typeof value === "string" ? value.trim() : "";
}
function namesEqual(left, right) {
  return (left ?? "").trim().toLowerCase() === (right ?? "").trim().toLowerCase();
}
var teamIdOf, apiSportsProvider;
var init_api_sports2 = __esm({
  "lib/workers/providers/api-sports.ts"() {
    "use strict";
    init_api_sports();
    init_api_config();
    teamIdOf = (id2) => typeof id2 === "number" && Number.isFinite(id2) && id2 > 0 ? String(id2) : null;
    apiSportsProvider = {
      name: "api_sports",
      supports: ({ sport, dataType }) => ["nfl", "ncaaf"].includes(toApiChainSport(sport) ?? "") && [
        "teams",
        "players",
        "games",
        "schedule",
        "injuries",
        "player_headshots",
        "team_logos"
      ].includes(dataType),
      async fetch({ sport, dataType, query = {} }) {
        const chainSport = toApiChainSport(String(sport ?? "nfl")) ?? "nfl";
        if (chainSport !== "nfl" && chainSport !== "ncaaf") return null;
        const sportTag = chainSport === "ncaaf" ? "NCAAF" : "NFL";
        const season = toSeason(query.season);
        const search = toSearch(query.search ?? query.playerName);
        const teamId = typeof query.teamId === "string" ? query.teamId.trim() : "";
        const teamCode2 = typeof query.teamCode === "string" ? query.teamCode.trim().toUpperCase() : "";
        switch (dataType) {
          case "teams": {
            const teams = await fetchAPISportsTeams(season, { sport: sportTag });
            return teams.map((team) => ({
              id: String(team.id),
              name: team.name,
              shortName: teamNameToAbbrev(team.name) ?? team.name,
              city: team.city ?? null,
              logo: team.logo ?? null,
              source: "api_sports"
            }));
          }
          case "players": {
            const players = search ? await fetchAPISportsPlayerBySearch(search, season, { sport: sportTag }) : teamId ? await fetchAPISportsPlayers(teamId, season) : [];
            return players.map((player) => ({
              id: String(player.id),
              name: player.name,
              position: player.position ?? player.group ?? null,
              team: teamNameToAbbrev(player.team?.name ?? null),
              // Same placeholder-`0` guard as the games path below. `player.team` being truthy says
              // nothing about its id: an unassigned club arrives as `{ id: 0, name: … }`, and
              // `String(0)` is `"0"`, which reads as a real team to every consumer.
              teamId: teamIdOf(player.team?.id),
              number: player.number ?? null,
              height: player.height ?? null,
              weight: player.weight ? Number.parseInt(player.weight, 10) || null : null,
              college: player.college ?? null,
              status: null,
              imageUrl: player.image ?? null,
              source: "api_sports"
            }));
          }
          case "games":
          case "schedule": {
            const games = await fetchAPISportsGames(season, { sport: sportTag });
            return games.flatMap((game) => {
              const homeTeam = game.teams.home.name ? teamNameToAbbrev(game.teams.home.name) ?? game.teams.home.name : null;
              const awayTeam = game.teams.away.name ? teamNameToAbbrev(game.teams.away.name) ?? game.teams.away.name : null;
              if (!homeTeam || !awayTeam) return [];
              return [{
                id: String(game.game.id),
                homeTeam,
                awayTeam,
                /*
                 * 🛑 `0` IS NOT AN ID, AND `!= null` LETS IT THROUGH. API-Sports ships `id: 0` as a
                 * placeholder on unresolved fixtures — sometimes with the NAMES resolved — and
                 * `0 != null` is true, so this stringified it to `"0"`. `lib/workers/schedule-importer.ts`
                 * accepts any string and writes it to `GameSchedule.homeTeamId`, giving a team id that
                 * reads as real to anything joining on it. `writeApiSportsGames` in `lib/api-sports.ts`
                 * guards this with `teamIdOf`; this parallel writer was named in that comment as
                 * already handling it and did not.
                 */
                homeTeamId: teamIdOf(game.teams.home.id),
                awayTeamId: teamIdOf(game.teams.away.id),
                date: game.game.date.date,
                status: game.game.status.long,
                season: game.league.season,
                venue: game.game.venue?.name ?? null,
                homeScore: game.scores.home.total ?? null,
                awayScore: game.scores.away.total ?? null,
                source: "api_sports"
              }];
            });
          }
          case "injuries": {
            const injuries = await fetchAPISportsInjuries(season, { sport: sportTag });
            return injuries.map((injury) => ({
              externalId: String(injury.id),
              playerId: String(injury.player?.id ?? injury.id),
              playerName: injury.player?.name ?? "Unknown Player",
              team: teamNameToAbbrev(injury.team?.name ?? null) ?? injury.team?.name ?? "FA",
              status: injury.status ?? "questionable",
              bodyPart: injury.type ?? null,
              notes: injury.description ?? null,
              reportDate: injury.date ?? null,
              source: "api_sports"
            }));
          }
          case "player_headshots": {
            if (!search) return null;
            const players = await fetchAPISportsPlayerBySearch(search, season, { sport: sportTag });
            const matched = players.find((player) => {
              const sameName = namesEqual(player.name, search);
              const sameTeam = !teamCode2 || teamNameToAbbrev(player.team?.name ?? null) === teamCode2;
              return sameName && sameTeam;
            }) ?? players[0];
            if (!matched?.image) return null;
            return {
              playerId: String(matched.id),
              playerName: matched.name,
              teamCode: teamNameToAbbrev(matched.team?.name ?? null),
              headshotUrl: matched.image,
              headshotUrlSm: matched.image,
              headshotUrlLg: matched.image,
              headshotSource: "api_sports"
            };
          }
          case "team_logos": {
            const teams = await fetchAPISportsTeams();
            const matched = teams.find((team) => teamNameToAbbrev(team.name) === teamCode2) ?? teams[0];
            if (!matched?.logo) return null;
            return {
              teamCode: teamNameToAbbrev(matched.name) ?? matched.name,
              teamName: matched.name,
              logoUrl: matched.logo,
              logoUrlSm: matched.logo,
              logoUrlLg: matched.logo,
              logoSource: "api_sports"
            };
          }
          default:
            return null;
        }
      }
    };
  }
});

// lib/provider-config.ts
function trim(value) {
  return (value ?? "").trim();
}
function resolveFirstEnv(keys) {
  for (const key of keys) {
    const value = trim(process.env[key]);
    if (value) return { value, keyUsed: key };
  }
  return { value: "", keyUsed: null };
}
function normalizeBaseUrl(value, fallback) {
  const raw = trim(value) || fallback;
  return raw.replace(/\/+$/, "");
}
function getRollingInsightsEnabledSports() {
  return {
    NFL: ROLLING_INSIGHTS_SPORTS.NFL,
    NHL: ROLLING_INSIGHTS_SPORTS.NHL,
    NBA: ROLLING_INSIGHTS_SPORTS.NBA,
    MLB: ROLLING_INSIGHTS_SPORTS.MLB,
    NCAAF: ROLLING_INSIGHTS_SPORTS.NCAAF,
    NCAAB: ROLLING_INSIGHTS_SPORTS.NCAAB,
    SOCCER: ROLLING_INSIGHTS_SPORTS.SOCCER
  };
}
function getClearSportsConfigFromEnv() {
  const apiKey3 = resolveFirstEnv(CLEARSPORTS_KEY_KEYS);
  const baseUrlRaw = resolveFirstEnv(CLEARSPORTS_BASE_URL_KEYS);
  const baseFallback = apiKey3.value ? DEFAULT_CLEARSPORTS_BASE_URL : "";
  const baseUrl = normalizeBaseUrl(baseUrlRaw.value, baseFallback);
  if (!apiKey3.value || !baseUrl) return null;
  return {
    apiKey: apiKey3.value,
    baseUrl,
    keySource: apiKey3.keyUsed ?? CLEARSPORTS_KEY_KEYS[0]
  };
}
function getRollingInsightsConfigFromEnv() {
  const apiKey3 = resolveFirstEnv(ROLLING_INSIGHTS_API_KEY_KEYS);
  const clientId = resolveFirstEnv(ROLLING_INSIGHTS_CLIENT_ID_KEYS);
  const clientSecret = resolveFirstEnv(ROLLING_INSIGHTS_CLIENT_SECRET_KEYS);
  const clientId2 = resolveFirstEnv(ROLLING_INSIGHTS_CLIENT_ID2_KEYS);
  const clientSecret2 = resolveFirstEnv(ROLLING_INSIGHTS_CLIENT_SECRET2_KEYS);
  const baseUrl = normalizeBaseUrl(
    resolveFirstEnv(ROLLING_INSIGHTS_BASE_URL_KEYS).value,
    DEFAULT_ROLLING_INSIGHTS_BASE_URL
  );
  const enabledSports = getRollingInsightsEnabledSports();
  const hasPrimaryClientPair = !!clientId.value && !!clientSecret.value;
  const hasSecondaryClientPair = !!clientId2.value && !!clientSecret2.value;
  if (hasPrimaryClientPair || hasSecondaryClientPair) {
    const preferredKeySource = hasPrimaryClientPair ? clientId.keyUsed ?? ROLLING_INSIGHTS_CLIENT_ID_KEYS[0] : clientId2.keyUsed ?? ROLLING_INSIGHTS_CLIENT_ID2_KEYS[0];
    return {
      authMode: "client_credentials",
      baseUrl,
      keySource: preferredKeySource,
      enabledSports
    };
  }
  if (apiKey3.value) {
    return {
      authMode: "api_key",
      baseUrl,
      keySource: apiKey3.keyUsed ?? ROLLING_INSIGHTS_API_KEY_KEYS[0],
      enabledSports
    };
  }
  return null;
}
var CLEARSPORTS_KEY_KEYS, CLEARSPORTS_BASE_URL_KEYS, DEFAULT_CLEARSPORTS_BASE_URL, ROLLING_INSIGHTS_API_KEY_KEYS, ROLLING_INSIGHTS_CLIENT_ID_KEYS, ROLLING_INSIGHTS_CLIENT_SECRET_KEYS, ROLLING_INSIGHTS_CLIENT_ID2_KEYS, ROLLING_INSIGHTS_CLIENT_SECRET2_KEYS, ROLLING_INSIGHTS_BASE_URL_KEYS, DEFAULT_ROLLING_INSIGHTS_BASE_URL;
var init_provider_config = __esm({
  "lib/provider-config.ts"() {
    "use strict";
    init_api_config();
    CLEARSPORTS_KEY_KEYS = ["CLEARSPORTS_API_KEY", "CLEAR_SPORTS_API_KEY", "CLEARSPORTS_KEY"];
    CLEARSPORTS_BASE_URL_KEYS = [
      "CLEARSPORTS_API_BASE",
      "CLEAR_SPORTS_API_BASE",
      "CLEARSPORTS_BASE_URL",
      "CLEAR_SPORTS_BASE_URL"
    ];
    DEFAULT_CLEARSPORTS_BASE_URL = "https://api.clearsportsapi.com/api/v1";
    ROLLING_INSIGHTS_API_KEY_KEYS = ["ROLLING_INSIGHTS_API_KEY", "ROLLINGINSIGHTS_API_KEY"];
    ROLLING_INSIGHTS_CLIENT_ID_KEYS = ["ROLLING_INSIGHTS_CLIENT_ID"];
    ROLLING_INSIGHTS_CLIENT_SECRET_KEYS = ["ROLLING_INSIGHTS_CLIENT_SECRET"];
    ROLLING_INSIGHTS_CLIENT_ID2_KEYS = ["ROLLING_INSIGHTS_CLIENT_ID2"];
    ROLLING_INSIGHTS_CLIENT_SECRET2_KEYS = ["ROLLING_INSIGHTS_CLIENT_SECRET2"];
    ROLLING_INSIGHTS_BASE_URL_KEYS = ["ROLLING_INSIGHTS_BASE_URL", "ROLLING_INSIGHTS_API_BASE"];
    DEFAULT_ROLLING_INSIGHTS_BASE_URL = "https://rest.datafeeds.rolling-insights.com/api/v1";
  }
});

// lib/ai-orchestration/provider-utils.ts
function sanitizeProviderError(message) {
  if (message == null || typeof message !== "string") return "Provider error";
  const s = message.slice(0, 500).trim();
  if (!s) return "Provider error";
  const redacted = s.replace(/\b(sk-[a-zA-Z0-9-_]{20,})/gi, "[REDACTED]").replace(/\b(api[_-]?key|apikey)\s*[:=]\s*["']?[^"'\s]+/gi, "api_key=[REDACTED]");
  return redacted || "Provider error";
}
var init_provider_utils = __esm({
  "lib/ai-orchestration/provider-utils.ts"() {
    "use strict";
  }
});

// lib/admin/provider-status-service.ts
function trimFailures() {
  const byProvider = /* @__PURE__ */ new Map();
  for (const e of recentFailures) {
    const list = byProvider.get(e.provider) || [];
    list.push(e);
    byProvider.set(e.provider, list);
  }
  recentFailures.length = 0;
  for (const [, list] of byProvider) {
    const sorted = list.sort((a, b) => b.at - a.at).slice(0, MAX_FAILURES_PER_PROVIDER);
    recentFailures.push(...sorted);
  }
  recentFailures.sort((a, b) => b.at - a.at);
}
function recordProviderFailure(provider, error) {
  recentFailures.push({
    at: Date.now(),
    provider,
    error: error ? sanitizeProviderError(error) : void 0
  });
  trimFailures();
}
function recordProviderLatency(provider, latencyMs) {
  const arr = latencyByProvider[provider] || [];
  arr.push(latencyMs);
  if (arr.length > LATENCY_SAMPLES_PER_PROVIDER) arr.shift();
  latencyByProvider[provider] = arr;
}
function logDiagnosticsEvent(event, provider, detail) {
  if (process.env.NODE_ENV === "development" || event === "failure") {
    const msg = detail ? `[ProviderDiagnostics] ${event} ${provider} ${detail}` : `[ProviderDiagnostics] ${event} ${provider}`;
    if (event === "failure") console.warn(msg);
    else console.info(msg);
  }
}
var MAX_FAILURES_PER_PROVIDER, LATENCY_SAMPLES_PER_PROVIDER, recentFailures, latencyByProvider;
var init_provider_status_service = __esm({
  "lib/admin/provider-status-service.ts"() {
    "use strict";
    init_provider_utils();
    MAX_FAILURES_PER_PROVIDER = 50;
    LATENCY_SAMPLES_PER_PROVIDER = 20;
    recentFailures = [];
    latencyByProvider = {};
  }
});

// lib/provider-diagnostics.ts
var init_provider_diagnostics = __esm({
  "lib/provider-diagnostics.ts"() {
    "use strict";
    init_provider_status_service();
  }
});

// lib/clear-sports/client.ts
function getTimeoutMs() {
  const v = process.env.CLEARSPORTS_TIMEOUT_MS;
  if (v == null || v === "") return DEFAULT_TIMEOUT_MS;
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_TIMEOUT_MS;
}
function getMaxRetries() {
  const v = process.env.CLEARSPORTS_MAX_RETRIES;
  if (v == null || v === "") return DEFAULT_MAX_RETRIES;
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n >= 0 ? n : DEFAULT_MAX_RETRIES;
}
function getRateLimitPerMinute() {
  const v = process.env.CLEARSPORTS_RATE_LIMIT_PER_MINUTE;
  if (v == null || v === "") return DEFAULT_RATE_LIMIT_PER_MINUTE;
  const n = parseInt(v, 10);
  return Number.isFinite(n) && n > 0 ? n : DEFAULT_RATE_LIMIT_PER_MINUTE;
}
function safeLog(path, status, durationMs, extra) {
  const msg = extra ? `[ClearSports] ${path} ${status} ${durationMs}ms ${extra}` : `[ClearSports] ${path} ${status} ${durationMs}ms`;
  if (status >= 400) console.warn(msg);
  else if (process.env.NODE_ENV === "development") console.info(msg);
}
function checkRateLimit() {
  const limit = getRateLimitPerMinute();
  const now = Date.now();
  const windowMs = 6e4;
  if (now - rateLimitWindowStart >= windowMs) {
    rateLimitWindowStart = now;
    rateLimitCount = 0;
  }
  if (rateLimitCount >= limit) {
    const waitMs = rateLimitWindowStart + windowMs - now;
    if (waitMs > 0) {
      console.warn(`[ClearSports] Rate limit (${limit}/min), waiting ${Math.ceil(waitMs / 1e3)}s`);
      throw new Error("ClearSports rate limit \u2014 try again shortly");
    }
    rateLimitWindowStart = now;
    rateLimitCount = 0;
  }
  rateLimitCount++;
}
function inferFallbackType(path) {
  const lower = path.toLowerCase();
  if (lower.includes("news")) return "news";
  if (lower.includes("game")) return "schedule";
  if (lower.includes("projection") || lower.includes("ranking") || lower.includes("player") || lower.includes("trend")) {
    return "players";
  }
  return "players";
}
async function clearSportsFetch(path, params, options) {
  const cfg = getClearSportsConfigFromEnv();
  if (!cfg?.apiKey || !cfg?.baseUrl) return null;
  const timeoutMs = options?.timeoutMs ?? getTimeoutMs();
  const maxRetries = getMaxRetries();
  const url = new URL(`${cfg.baseUrl.replace(/\/+$/, "")}/${path.replace(/^\/+/, "")}`);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== void 0 && v !== null) url.searchParams.set(k, String(v));
    }
  }
  const pathLabel = path + (params && Object.keys(params).length ? `?${url.searchParams.toString().slice(0, 40)}` : "");
  if (!await rateLimitManager.canCall("clearsports", path)) {
    await rateLimitManager.recordCall("clearsports", path, 429, 0, {
      cached: true,
      error: "rate_limit_guard"
    });
    return await rateLimitManager.getFallback("clearsports", inferFallbackType(path));
  }
  let lastError2 = null;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      checkRateLimit();
    } catch (e) {
      return null;
    }
    const start = Date.now();
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url.toString(), {
        method: "GET",
        headers: {
          Authorization: `Bearer ${cfg.apiKey}`,
          "Content-Type": "application/json"
        },
        signal: controller.signal
      });
      clearTimeout(t);
      const durationMs = Date.now() - start;
      if (!res.ok) {
        await rateLimitManager.recordCall("clearsports", path, res.status, durationMs, {
          error: res.statusText
        });
        safeLog(pathLabel, res.status, durationMs, res.statusText);
        if (res.status === 429 && attempt < maxRetries) {
          await new Promise((r) => setTimeout(r, 1e3 * (attempt + 1)));
          continue;
        }
        recordProviderFailure("clearsports", `HTTP ${res.status} ${res.statusText}`);
        logDiagnosticsEvent("failure", "clearsports", `http_${res.status}`);
        return null;
      }
      const json = await res.json().catch(() => null);
      await rateLimitManager.recordCall("clearsports", path, res.status, durationMs);
      safeLog(pathLabel, res.status, durationMs);
      recordProviderLatency("clearsports", durationMs);
      logDiagnosticsEvent("latency", "clearsports", `${durationMs}ms`);
      return json;
    } catch (err) {
      clearTimeout(t);
      const durationMs = Date.now() - start;
      lastError2 = err instanceof Error ? err : new Error(String(err));
      await rateLimitManager.recordCall("clearsports", path, 500, durationMs, {
        error: lastError2.message
      });
      const isTimeout = lastError2.message?.toLowerCase().includes("abort") || lastError2.message?.toLowerCase().includes("timeout");
      const safeError2 = sanitizeProviderError(lastError2.message);
      safeLog(pathLabel, isTimeout ? 408 : 500, durationMs, isTimeout ? "timeout" : safeError2.slice(0, 50));
      if (attempt < maxRetries) {
        await new Promise((r) => setTimeout(r, 1e3 * (attempt + 1)));
      } else {
        recordProviderFailure("clearsports", isTimeout ? "request_timeout" : safeError2);
        logDiagnosticsEvent("failure", "clearsports", isTimeout ? "timeout" : safeError2.slice(0, 120));
        return null;
      }
    }
  }
  return null;
}
var DEFAULT_TIMEOUT_MS, DEFAULT_MAX_RETRIES, DEFAULT_RATE_LIMIT_PER_MINUTE, rateLimitCount, rateLimitWindowStart;
var init_client = __esm({
  "lib/clear-sports/client.ts"() {
    "use strict";
    init_provider_config();
    init_provider_diagnostics();
    init_provider_utils();
    init_rate_limit_manager();
    DEFAULT_TIMEOUT_MS = 15e3;
    DEFAULT_MAX_RETRIES = 2;
    DEFAULT_RATE_LIMIT_PER_MINUTE = 60;
    rateLimitCount = 0;
    rateLimitWindowStart = Date.now();
  }
});

// lib/provider-sync-logger.ts
async function recordProviderSync(ctx, stats) {
  const now = /* @__PURE__ */ new Date();
  const {
    provider,
    entityType,
    sport,
    key,
    fallbackProvider,
    sourcePriority
  } = ctx;
  const {
    recordsImported = 0,
    recordsUpdated = 0,
    recordsSkipped = 0,
    lastPayloadBytes = 0,
    error = null
  } = stats;
  const normalizedSport = sport ?? "GLOBAL";
  try {
    await prisma.providerSyncState.upsert({
      where: {
        provider_entityType_sport_key: {
          provider,
          entityType,
          sport: normalizedSport,
          key: key ?? null
        }
      },
      update: {
        lastCompletedAt: now,
        lastSuccessAt: error ? void 0 : now,
        lastErrorAt: error ? now : void 0,
        lastError: error,
        recordsImported,
        recordsUpdated,
        recordsSkipped,
        lastPayloadBytes,
        fallbackProvider: fallbackProvider ?? null,
        sourcePriority: sourcePriority ?? null
      },
      create: {
        provider,
        entityType,
        sport: normalizedSport,
        key: key ?? null,
        lastStartedAt: now,
        lastCompletedAt: now,
        lastSuccessAt: error ? null : now,
        lastErrorAt: error ? now : null,
        lastError: error,
        recordsImported,
        recordsUpdated,
        recordsSkipped,
        lastPayloadBytes,
        fallbackProvider: fallbackProvider ?? null,
        sourcePriority: sourcePriority ?? null
      }
    });
  } catch (e) {
    console.error("[provider-sync-logger] Failed to record sync state:", e);
  }
}
var init_provider_sync_logger = __esm({
  "lib/provider-sync-logger.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/clear-sports/sync.ts
var TTL_MS_BY_ENTITY;
var init_sync = __esm({
  "lib/clear-sports/sync.ts"() {
    "use strict";
    init_prisma();
    init_provider_sync_logger();
    init_team_abbrev();
    init_client();
    TTL_MS_BY_ENTITY = {
      teams: 24 * 60 * 60 * 1e3,
      games: 60 * 60 * 1e3,
      players: 24 * 60 * 60 * 1e3,
      injuries: 5 * 60 * 1e3,
      team_stats: 60 * 60 * 1e3,
      player_stats: 60 * 60 * 1e3,
      odds: 5 * 60 * 1e3,
      news: 15 * 60 * 1e3,
      predictions: 15 * 60 * 1e3,
      sportsbooks: 24 * 60 * 60 * 1e3,
      api_keys: 30 * 60 * 1e3
    };
  }
});

// lib/clear-sports/normalize.ts
var init_normalize = __esm({
  "lib/clear-sports/normalize.ts"() {
    "use strict";
    init_team_abbrev();
  }
});

// lib/clear-sports/tool-support.ts
var init_tool_support = __esm({
  "lib/clear-sports/tool-support.ts"() {
    "use strict";
  }
});

// lib/clear-sports/index.ts
function leagueCodeForSport(sport) {
  switch (sport) {
    case "NFL":
      return "nfl";
    case "NHL":
      return "nhl";
    case "NBA":
      return "nba";
    case "MLB":
      return "mlb";
    case "NCAAB":
      return "ncaab";
    case "NCAAF":
      return "ncaaf";
    case "SOCCER":
      return "epl";
    default:
      return sport.toLowerCase();
  }
}
function rowsFrom(json, key) {
  if (!json) return [];
  if (Array.isArray(json[key])) {
    return json[key];
  }
  return Array.isArray(json) ? json : [];
}
async function fetchClearSportsTeams(sport) {
  const league = leagueCodeForSport(sport);
  const json = await clearSportsFetch(`${league}/teams`);
  const rows = rowsFrom(json, "teams");
  return rows.map((t) => ({
    id: String(t.id ?? t.teamId ?? t.slug ?? ""),
    name: String(t.name ?? t.fullName ?? t.displayName ?? "Unknown"),
    shortName: t.abbrev ?? t.shortName ?? null,
    city: t.city ?? null,
    mascot: t.mascot ?? null,
    logo: t.logoUrl ?? t.logo ?? null
  })).filter((t) => t.id && t.name);
}
async function fetchClearSportsGames(sport, season) {
  const league = leagueCodeForSport(sport);
  const json = await clearSportsFetch(
    `${league}/games`,
    season ? { season } : void 0
  );
  const rows = rowsFrom(json, "games");
  return rows.map((g4) => ({
    id: String(g4.id ?? g4.gameId ?? ""),
    homeTeamId: String(g4.homeTeamId ?? g4.home?.id ?? ""),
    homeTeamAbbrev: normalizeTeamAbbrev(g4.homeTeamAbbrev || g4.home?.abbrv || g4.home?.code || null),
    awayTeamId: String(g4.awayTeamId ?? g4.away?.id ?? ""),
    awayTeamAbbrev: normalizeTeamAbbrev(g4.awayTeamAbbrev || g4.away?.abbrv || g4.away?.code || null),
    date: g4.date ?? g4.startTime ?? null,
    status: g4.status ?? null,
    season: g4.season ? String(g4.season) : season ?? null,
    venue: g4.venue?.name ?? g4.venue ?? null
  })).filter((g4) => g4.id && g4.homeTeamId && g4.awayTeamId);
}
async function fetchClearSportsRankings(sport, season) {
  const league = leagueCodeForSport(sport);
  const json = await clearSportsFetch(
    `${league}/rankings`,
    season ? { season } : void 0
  );
  return rowsFrom(json, "rankings").filter((r) => !!r && typeof r === "object");
}
async function fetchClearSportsProjections(sport, season) {
  const league = leagueCodeForSport(sport);
  const json = await clearSportsFetch(
    `${league}/projections`,
    season ? { season } : void 0
  );
  return rowsFrom(json, "projections").filter((r) => !!r && typeof r === "object");
}
async function fetchClearSportsNews(sport, limit = 20) {
  const json = await clearSportsFetch(
    "news",
    { sport, limit: Math.max(1, Math.min(100, limit)) }
  );
  return rowsFrom(json, "news").filter((r) => !!r && typeof r === "object");
}
var init_clear_sports = __esm({
  "lib/clear-sports/index.ts"() {
    "use strict";
    init_team_abbrev();
    init_client();
    init_client();
    init_sync();
    init_normalize();
    init_tool_support();
  }
});

// lib/clear-sports.ts
var init_clear_sports2 = __esm({
  "lib/clear-sports.ts"() {
    "use strict";
    init_clear_sports();
  }
});

// lib/workers/providers/clearsports.ts
function toClearSportsSport(params) {
  const chain = toApiChainSport(params.sport);
  if (!chain) return null;
  const map = {
    nfl: "NFL",
    nba: "NBA",
    mlb: "MLB",
    nhl: "NHL",
    ncaab: "NCAAB",
    ncaaf: "NCAAF",
    soccer_euro: "SOCCER",
    soccer_mls: "SOCCER"
  };
  return map[chain] ?? null;
}
function leagueCodeForSport2(sport) {
  switch (sport) {
    case "NFL":
      return "nfl";
    case "NHL":
      return "nhl";
    case "NBA":
      return "nba";
    case "MLB":
      return "mlb";
    case "NCAAB":
      return "ncaab";
    case "NCAAF":
      return "ncaaf";
    case "SOCCER":
      return "soccer";
    default:
      return sport.toLowerCase();
  }
}
function rowsFrom2(json, key) {
  if (!json) return [];
  if (Array.isArray(json[key])) {
    return json[key];
  }
  return Array.isArray(json) ? json : [];
}
function toSearch2(value) {
  return typeof value === "string" ? value.trim() : "";
}
function namesEqual2(left, right) {
  return (left ?? "").trim().toLowerCase() === (right ?? "").trim().toLowerCase();
}
async function fetchAllPlayers(sport, search) {
  const league = leagueCodeForSport2(sport);
  const json = await clearSportsFetch(
    `leagues/${league}/players`,
    search ? { q: search } : void 0
  );
  return rowsFrom2(json, "players").filter((row) => !!row && typeof row === "object");
}
var clearSportsProvider;
var init_clearsports = __esm({
  "lib/workers/providers/clearsports.ts"() {
    "use strict";
    init_clear_sports2();
    init_client();
    init_team_abbrev();
    init_api_config();
    clearSportsProvider = {
      name: "clearsports",
      supports: (params) => [
        "teams",
        "players",
        "games",
        "schedule",
        "news",
        "rankings",
        "projections",
        "player_headshots",
        "team_logos"
      ].includes(params.dataType) && toClearSportsSport(params) != null,
      async fetch(params) {
        const { dataType, query = {} } = params;
        const clearSport = toClearSportsSport(params);
        if (!clearSport) return null;
        const season = typeof query.season === "string" ? query.season : void 0;
        const search = toSearch2(query.search ?? query.playerName);
        const teamCode2 = normalizeTeamAbbrev(String(query.teamCode ?? query.team ?? ""));
        const teamName = typeof query.teamName === "string" ? query.teamName.trim() : "";
        switch (dataType) {
          case "teams": {
            const teams = await fetchClearSportsTeams(clearSport);
            return teams.map((team) => ({
              id: team.id,
              name: team.name,
              shortName: normalizeTeamAbbrev(team.shortName) ?? team.shortName,
              city: team.city ?? null,
              mascot: team.mascot ?? null,
              logo: team.logo ?? null,
              source: "clearsports"
            }));
          }
          case "players": {
            const players = await fetchAllPlayers(clearSport, search || void 0);
            return players.map((player) => ({
              id: String(player.id ?? player.playerId ?? player.slug ?? ""),
              name: String(player.name ?? player.fullName ?? player.displayName ?? "Unknown"),
              position: String(player.position ?? player.pos ?? "").trim() || null,
              team: normalizeTeamAbbrev(String(player.teamAbbrev ?? player.team ?? "")) ?? null,
              teamId: player.teamId ? String(player.teamId) : null,
              number: typeof player.number === "number" ? player.number : Number(player.jerseyNumber ?? NaN) || null,
              height: String(player.height ?? "").trim() || null,
              weight: Number(player.weight ?? NaN) || null,
              college: String(player.college ?? "").trim() || null,
              dob: String(player.dob ?? player.birthDate ?? "").trim() || null,
              status: String(player.status ?? "").trim() || null,
              imageUrl: String(player.imageUrl ?? player.headshot ?? "").trim() || null,
              source: "clearsports"
            })).filter((player) => player.id && player.name);
          }
          case "games":
          case "schedule": {
            const games = await fetchClearSportsGames(clearSport, season);
            return games.map((game) => ({
              id: game.id,
              homeTeam: game.homeTeamAbbrev ?? null,
              awayTeam: game.awayTeamAbbrev ?? null,
              homeTeamId: game.homeTeamId || null,
              awayTeamId: game.awayTeamId || null,
              date: game.date,
              status: game.status,
              season: game.season,
              venue: game.venue ?? null,
              source: "clearsports"
            }));
          }
          case "news":
            return fetchClearSportsNews(clearSport, Number(query.limit ?? 40) || 40);
          case "rankings":
            return fetchClearSportsRankings(clearSport, season);
          case "projections":
            return fetchClearSportsProjections(clearSport, season);
          case "player_headshots": {
            if (!search) return null;
            const players = await fetchAllPlayers(clearSport, search);
            const matched = players.find((player) => {
              const name = String(player.name ?? player.fullName ?? player.displayName ?? "");
              const sameName = namesEqual2(name, search);
              const sameTeam = !teamCode2 || normalizeTeamAbbrev(String(player.teamAbbrev ?? player.team ?? "")) === teamCode2;
              return sameName && sameTeam;
            }) ?? players[0];
            const imageUrl = String(matched?.imageUrl ?? matched?.headshot ?? "").trim();
            if (!imageUrl) return null;
            return {
              playerId: String(matched?.id ?? matched?.playerId ?? ""),
              playerName: String(matched?.name ?? matched?.fullName ?? matched?.displayName ?? search),
              teamCode: normalizeTeamAbbrev(String(matched?.teamAbbrev ?? matched?.team ?? "")) ?? null,
              headshotUrl: imageUrl,
              headshotUrlSm: imageUrl,
              headshotUrlLg: imageUrl,
              headshotSource: "clearsports"
            };
          }
          case "team_logos": {
            const teams = await fetchClearSportsTeams(clearSport);
            const matched = teams.find((team) => {
              const shortName = normalizeTeamAbbrev(team.shortName);
              return !!teamCode2 && shortName === teamCode2 || !!teamName && namesEqual2(team.name, teamName);
            }) ?? teams.find((team) => normalizeTeamAbbrev(team.shortName) === teamCode2);
            if (!matched?.logo) return null;
            return {
              teamCode: normalizeTeamAbbrev(matched.shortName) ?? matched.shortName ?? teamCode2,
              teamName: matched.name,
              logoUrl: matched.logo,
              logoUrlSm: matched.logo,
              logoUrlLg: matched.logo,
              logoSource: "clearsports"
            };
          }
          default:
            return null;
        }
      }
    };
  }
});

// lib/workers/providers/rolling-insights.ts
function nextCacheBuster() {
  riCacheBusterSeq += 1;
  return String(Date.now() + riCacheBusterSeq);
}
function vendorToday(dayOffset = 0) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(/* @__PURE__ */ new Date());
  if (dayOffset === 0) return parts;
  const shifted = /* @__PURE__ */ new Date(`${parts}T12:00:00Z`);
  shifted.setUTCDate(shifted.getUTCDate() + dayOffset);
  return shifted.toISOString().slice(0, 10);
}
function pathSegmentForDataType(dataType) {
  if (DATA_TYPE_PATH[dataType]) return DATA_TYPE_PATH[dataType];
  if (dataType === "games" || dataType === "live_game") return "scores";
  if (dataType === "player_headshots") return "players";
  if (dataType === "team_logos") return "teams";
  if (dataType === "rolling_insights") return "feed";
  if (dataType === "trending") return "trending";
  return dataType;
}
function extractPayload(json) {
  if (json == null) return null;
  if (Array.isArray(json)) return json;
  if (typeof json === "object") {
    const o = json;
    if (Array.isArray(o.data)) return o.data;
    if (o.data && typeof o.data === "object") {
      const nested = o.data;
      for (const value of Object.values(nested)) {
        if (Array.isArray(value)) return value;
      }
    }
    if (Array.isArray(o.results)) return o.results;
    if (Array.isArray(o.items)) return o.items;
  }
  return json;
}
function normalizeBaseUrl2(url) {
  return url.trim().replace(/\/+$/, "");
}
function splitEnvList(value) {
  if (!value) return [];
  return value.split(",").map((v) => normalizeBaseUrl2(v)).filter(Boolean);
}
function dedupe(values) {
  return Array.from(new Set(values));
}
function buildRestBaseCandidates(configBase) {
  const explicit = splitEnvList(process.env.ROLLING_INSIGHTS_REST_BASE_URL);
  const normalizedConfig = normalizeBaseUrl2(configBase);
  const derivedFromConfig = [normalizedConfig, `${normalizedConfig}/api/v1`];
  return dedupe([...explicit, ...derivedFromConfig, ...DEFAULT_RI_REST_BASES]);
}
function requestedYearFromQuery(query) {
  const raw = String(query.season ?? query.year ?? "").trim();
  if (/^\d{4}/.test(raw)) return raw.slice(0, 4);
  return String((/* @__PURE__ */ new Date()).getUTCFullYear());
}
function buildRestPathCandidates(dataSeg, chainSport, requestedYear = String((/* @__PURE__ */ new Date()).getUTCFullYear())) {
  const sportCodes = REST_SPORT_CODES[chainSport];
  const sportLower = SPORT_PATH[chainSport];
  const year = requestedYear;
  const today = vendorToday();
  const yesterday = vendorToday(-1);
  if (chainSport === "soccer_euro") {
    const soccerCode = "SOCCER";
    const byDataSeg = {
      players: [`player-info/${soccerCode}`],
      teams: [`team-info/${soccerCode}`],
      injuries: [`injuries/${soccerCode}`],
      schedule: [`schedule-season/${year}/${soccerCode}`, `schedule/${today}/${soccerCode}`],
      scores: [`live/${today}/${soccerCode}`, `live/${yesterday}/${soccerCode}`],
      standings: [`standings/${year}/${soccerCode}`],
      projections: [`player-stats/${year}/${soccerCode}`],
      adp: [`adp/${soccerCode}`],
      rosters: [`depth-charts/${soccerCode}`]
    };
    return dedupe([
      ...byDataSeg[dataSeg] ?? [],
      `${dataSeg}/${soccerCode}`,
      `${soccerCode}/${dataSeg}`
    ]);
  }
  if (chainSport === "soccer_mls") {
    const soccerCode = "MLS";
    const byDataSeg = {
      players: [`player-info/${soccerCode}`],
      teams: [`team-info/${soccerCode}`],
      injuries: [`injuries/${soccerCode}`],
      schedule: [`schedule-season/${year}/${soccerCode}`, `schedule/${today}/${soccerCode}`],
      scores: [`live/${today}/${soccerCode}`, `live/${yesterday}/${soccerCode}`],
      standings: [`standings/${year}/${soccerCode}`],
      projections: [`player-stats/${year}/${soccerCode}`],
      adp: [`adp/${soccerCode}`],
      rosters: [`depth-charts/${soccerCode}`]
    };
    return dedupe([
      ...byDataSeg[dataSeg] ?? [],
      `${dataSeg}/${soccerCode}`,
      `${soccerCode}/${dataSeg}`
    ]);
  }
  const pathBySportCode = (sportCode) => ({
    players: [`player-info/${sportCode}`],
    injuries: [`injuries/${sportCode}`],
    teams: [`team-info/${sportCode}`],
    /**
     * Rolling Insights has NO projections feed. Verified 2026-08-10 against the
     * official NFL docs (schedule, live feed, team info, team stats, player info,
     * player stats, injuries, depth charts, DK fantasy points, play-by-play) and
     * by probing every candidate below — only `player-stats` resolves, and it
     * returns HISTORICAL production, not a forecast.
     *
     * `player-stats` was previously the FIRST candidate here, which made the
     * `projections` dataType silently resolve to last season's actuals. It never
     * shipped bad numbers only because those rows carry no
     * projectedPoints/points/fpts field for persistProjectionRows() to read, and
     * because the current-season lookup 304s before kickoff. Both are accidents,
     * not safeguards.
     *
     * Do NOT re-add player-stats here. It is an INPUT to the AF projection engine
     * (see AF_PROJECTIONS_ENGINE_BRIEF.md), consumed under the `player_stats`
     * dataType and written to AFProjectionSnapshot after computation — never
     * echoed straight into a field the product labels "projected".
     */
    projections: [
      `projections/${year}/${sportCode}`,
      `projection-stats/${year}/${sportCode}`,
      `projected-stats/${year}/${sportCode}`,
      `fantasy-projections/${year}/${sportCode}`
    ],
    /** Historical production — the projection engine's base. Confirmed 2182 NFL rows for 2025. */
    player_stats: [`player-stats/${year}/${sportCode}`, `player-stats/${sportCode}`],
    adp: [
      `adp/${sportCode}`,
      `adp/${year}/${sportCode}`,
      `average-draft-position/${sportCode}`,
      `average-draft-position/${year}/${sportCode}`,
      `player-adp/${sportCode}`,
      `draft-adp/${sportCode}`
    ],
    scores: [`live/${today}/${sportCode}`, `live/${yesterday}/${sportCode}`],
    schedule: [
      `schedule-season/${year}/${sportCode}`,
      `schedule-week/${today}/${sportCode}`,
      `schedule/${today}/${sportCode}`
    ],
    rosters: [`depth-charts/${sportCode}`]
  });
  const allCandidates = [];
  for (const sportCode of sportCodes) {
    const byDataSeg = pathBySportCode(sportCode);
    allCandidates.push(
      ...byDataSeg[dataSeg] ?? [],
      `${dataSeg}/${sportCode}`,
      `${sportCode}/${dataSeg}`,
      `${dataSeg}/${sportLower}`
    );
  }
  return dedupe(allCandidates);
}
function collectRscTokenCandidates(accessToken) {
  return dedupe(
    [
      process.env.ROLLING_INSIGHTS_RSC_TOKEN?.trim(),
      process.env.ROLLING_INSIGHTS_RSC_TOKEN2?.trim(),
      process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim(),
      process.env.ROLLING_INSIGHTS_CLIENT_SECRET2?.trim(),
      accessToken
    ].filter(Boolean)
  );
}
async function fetchRiToken(clientId, clientSecret) {
  if (!clientId || !clientSecret) return null;
  try {
    const res = await fetch("https://datafeeds.rolling-insights.com/auth/token", {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: clientId,
        client_secret: clientSecret
      }),
      cache: "no-store",
      signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
    });
    if (!res.ok) return null;
    const json = await res.json();
    return json.access_token ?? json.token ?? json.rsc_token ?? json.RSC_token ?? null;
  } catch {
    return null;
  }
}
async function collectAllRiTokens(passedToken) {
  const [primary, secondary] = await Promise.all([
    fetchRiToken(
      process.env.ROLLING_INSIGHTS_CLIENT_ID?.trim() ?? "",
      process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim() ?? ""
    ),
    fetchRiToken(
      process.env.ROLLING_INSIGHTS_CLIENT_ID2?.trim() ?? "",
      process.env.ROLLING_INSIGHTS_CLIENT_SECRET2?.trim() ?? ""
    )
  ]);
  return dedupe([passedToken, primary, secondary].filter(Boolean));
}
async function fetchPlayersFromGraphql(sport, passedToken, configBase) {
  const queryName = RI_GRAPHQL_ROSTER_QUERY[sport];
  const queryBody = RI_GRAPHQL_QUERY_BODY[sport];
  if (!queryName || !queryBody) return null;
  const explicitGraphqlUrl = process.env.ROLLING_INSIGHTS_GRAPHQL_URL?.trim();
  const urls = dedupe(
    [explicitGraphqlUrl, `${normalizeBaseUrl2(configBase)}/graphql`, DEFAULT_RI_GRAPHQL_URL].filter(
      Boolean
    )
  );
  const tokens = await collectAllRiTokens(passedToken);
  for (const gqlUrl of urls) {
    for (const token of tokens) {
      try {
        const res = await fetch(gqlUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
            Accept: "application/json"
          },
          body: JSON.stringify({ query: queryBody }),
          cache: "no-store",
          signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
        });
        if (!res.ok) continue;
        const json = await res.json();
        if (json.errors?.length) continue;
        const players = json.data?.[queryName] ?? [];
        if (!players.length) continue;
        return players.map((p) => {
          const season = Array.isArray(p.regularSeason) ? p.regularSeason[0] : null;
          return {
            id: p.id,
            full_name: p.player,
            position: p.position,
            team_abbr: p.team?.abbrv ?? null,
            status: p.status,
            season_stats: {
              // NFL DraftKings fantasy points
              fantasy_points: season?.DK_fantasy_points ?? null,
              fantasy_points_per_game: season?.DK_fantasy_points_per_game ?? null,
              games_played: season?.games_played ?? season?.gamesPlayed ?? null,
              // MLB batting stats
              hr: season?.batting?.HR ?? null,
              rbi: season?.batting?.RBI ?? null,
              hits: season?.batting?.H ?? null,
              sb: season?.batting?.SB ?? null
            }
          };
        });
      } catch {
      }
    }
  }
  return null;
}
async function getClientCredentialsAccessToken(baseUrl) {
  const directRscToken = process.env.ROLLING_INSIGHTS_RSC_TOKEN?.trim();
  if (cachedAccessToken && cachedAccessToken.expiresAtMs > Date.now() + 6e4) {
    return cachedAccessToken.value;
  }
  const credentialPairs = [
    {
      clientId: process.env.ROLLING_INSIGHTS_CLIENT_ID?.trim() ?? "",
      clientSecret: process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim() ?? ""
    },
    {
      clientId: process.env.ROLLING_INSIGHTS_CLIENT_ID2?.trim() ?? "",
      clientSecret: process.env.ROLLING_INSIGHTS_CLIENT_SECRET2?.trim() ?? ""
    }
  ].filter((p) => p.clientId && p.clientSecret);
  if (!credentialPairs.length) {
    if (directRscToken) return directRscToken;
    throw new Error("Rolling Insights client credentials not configured");
  }
  const authEndpoints = dedupe([
    process.env.ROLLING_INSIGHTS_AUTH_URL?.trim() ?? "",
    "https://datafeeds.rolling-insights.com/auth/token",
    `${baseUrl}/auth/token`,
    "https://rest.datafeeds.rolling-insights.com/oauth/token"
  ].filter(Boolean));
  let lastStatus = null;
  for (const { clientId, clientSecret } of credentialPairs) {
    for (const endpoint of authEndpoints) {
      try {
        const tokenRes = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            grant_type: "client_credentials",
            client_id: clientId,
            client_secret: clientSecret
          }),
          cache: "no-store",
          signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
        });
        lastStatus = tokenRes.status;
        if (!tokenRes.ok) continue;
        const tokenJson = await tokenRes.json();
        const token = tokenJson.access_token ?? tokenJson.token ?? tokenJson.rsc_token ?? tokenJson.RSC_token;
        if (!token) continue;
        const expiresInSec = Number(tokenJson.expires_in ?? 3600);
        cachedAccessToken = {
          value: token,
          expiresAtMs: Date.now() + Math.max(expiresInSec, 300) * 1e3
        };
        return token;
      } catch {
      }
    }
  }
  if (directRscToken) return directRscToken;
  throw new Error(`Auth failed${lastStatus ? ` (${lastStatus})` : ""}`);
}
async function rollingInsightsProvider(params) {
  const config = getRollingInsightsConfigFromEnv();
  if (!config) {
    console.error("[rolling-insights] credentials not configured");
    return { data: null, error: "credentials not configured", fromCache: false };
  }
  const chainSport = toApiChainSport(params.sport);
  if (!chainSport) {
    return { data: null, error: "Unsupported sport", fromCache: false };
  }
  if ((chainSport === "soccer_euro" || chainSport === "soccer_mls") && String(params.dataType) === "players") {
    return {
      data: null,
      error: "RI soccer players unavailable",
      fromCache: false,
      source: "rolling_insights",
      latency: 0
    };
  }
  const base = config.baseUrl;
  const dataSeg = pathSegmentForDataType(String(params.dataType));
  const merged = { ...params.query ?? {}, ...params.options ?? {} };
  const started = Date.now();
  try {
    const headers = {
      Accept: "application/json",
      // transport.required_live_headers in contracts/rolling-insights/ENDPOINTS.yaml.
      // Mandatory on ALL endpoints, not just /live. These were missing entirely,
      // which is half of why this client saw 304s the compliant sibling does not.
      "Cache-Control": "no-cache, no-store",
      Pragma: "no-cache"
    };
    const hasClientCredentials = Boolean(process.env.ROLLING_INSIGHTS_CLIENT_ID?.trim()) && Boolean(process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim());
    if (config.authMode === "api_key") {
      const sportSeg = SPORT_PATH[chainSport];
      const url = new URL(`${base}/${sportSeg}/${dataSeg}`);
      Object.entries(merged).forEach(([k, v]) => {
        if (v == null) return;
        url.searchParams.set(k, String(v));
      });
      url.searchParams.set("_", nextCacheBuster());
      headers["x-api-key"] = process.env.ROLLING_INSIGHTS_API_KEY ?? "";
      const res = await fetch(url.toString(), {
        headers,
        cache: "no-store",
        signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
      });
      if (!res.ok) {
        const err2 = `HTTP ${res.status}`;
        if (!hasClientCredentials) {
          console.warn(`[rolling-insights] ${err2} ${chainSport}/${params.dataType}`);
          return { data: null, error: err2, fromCache: false };
        }
        console.warn(
          `[rolling-insights] ${err2} ${chainSport}/${params.dataType}; falling back to client_credentials`
        );
      } else {
        const json = await res.json();
        const data = extractPayload(json);
        return {
          data,
          fromCache: false,
          source: "rolling_insights",
          latency: Date.now() - started
        };
      }
    }
    const accessToken = await getClientCredentialsAccessToken(base);
    let rscTokenCandidates = collectRscTokenCandidates(accessToken);
    let restBases = buildRestBaseCandidates(base);
    const restPaths = buildRestPathCandidates(dataSeg, chainSport, requestedYearFromQuery(merged));
    if (chainSport === "soccer_euro" || chainSport === "soccer_mls") {
      restBases = restBases.slice(0, 1);
      rscTokenCandidates = rscTokenCandidates.slice(0, 2);
    }
    let lastHttpError = null;
    const probeBudgetMs = chainSport === "soccer_euro" || chainSport === "soccer_mls" ? RI_SOCCER_REST_PROBE_BUDGET_MS : RI_REST_PROBE_BUDGET_MS;
    const probeDeadlineAt = Date.now() + probeBudgetMs;
    restProbe: for (const restBase of restBases) {
      for (const restPath of restPaths) {
        for (const rscToken of rscTokenCandidates) {
          if (Date.now() >= probeDeadlineAt) {
            lastHttpError = `Probe timeout after ${probeBudgetMs}ms`;
            break restProbe;
          }
          const buildUrl = () => {
            const url = new URL(`${restBase}/${restPath}`);
            Object.entries(merged).forEach(([k, v]) => {
              if (v == null) return;
              url.searchParams.set(k, String(v));
            });
            url.searchParams.set("RSC_token", rscToken);
            url.searchParams.set("_", nextCacheBuster());
            return url.toString();
          };
          try {
            let res = await fetch(buildUrl(), {
              headers,
              cache: "no-store",
              signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
            });
            if (res.status === 304) {
              res = await fetch(buildUrl(), {
                headers,
                cache: "no-store",
                signal: AbortSignal.timeout(RI_REQUEST_TIMEOUT_MS)
              });
            }
            if (res.status === 304) {
              lastHttpError = "HTTP 304 (survived a cache-busted retry)";
              continue;
            }
            if (!res.ok) {
              lastHttpError = `HTTP ${res.status}`;
              continue;
            }
            const json = await res.json();
            const data = extractPayload(json);
            return {
              data,
              fromCache: false,
              source: "rolling_insights",
              latency: Date.now() - started
            };
          } catch {
          }
        }
      }
    }
    if (params.dataType === "players" && RI_GRAPHQL_ROSTER_QUERY[chainSport]) {
      const gqlPlayers = await fetchPlayersFromGraphql(chainSport, accessToken, base);
      if (Array.isArray(gqlPlayers) && gqlPlayers.length > 0) {
        return {
          data: gqlPlayers,
          fromCache: false,
          source: "rolling_insights",
          latency: Date.now() - started
        };
      }
    }
    const err = lastHttpError ?? "No successful Rolling Insights REST/GraphQL response";
    console.warn(`[rolling-insights] ${err} ${chainSport}/${params.dataType}`);
    return { data: null, error: err, fromCache: false };
  } catch (e) {
    console.warn(`[rolling-insights] fetch failed ${chainSport}/${params.dataType}:`, e);
    return { data: null, error: e instanceof Error ? e.message : "Request failed", fromCache: false };
  }
}
var DEFAULT_RI_GRAPHQL_URL, DEFAULT_RI_REST_BASES, SPORT_PATH, REST_SPORT_CODES, DATA_TYPE_PATH, riCacheBusterSeq, cachedAccessToken, RI_REQUEST_TIMEOUT_MS, RI_REST_PROBE_BUDGET_MS, RI_SOCCER_REST_PROBE_BUDGET_MS, RI_GRAPHQL_ROSTER_QUERY, RI_GRAPHQL_QUERY_BODY;
var init_rolling_insights = __esm({
  "lib/workers/providers/rolling-insights.ts"() {
    "use strict";
    init_api_config();
    init_provider_config();
    DEFAULT_RI_GRAPHQL_URL = "https://datafeeds.rolling-insights.com/graphql";
    DEFAULT_RI_REST_BASES = ["https://rest.datafeeds.rolling-insights.com/api/v1"];
    SPORT_PATH = {
      nfl: "nfl",
      mlb: "mlb",
      nhl: "nhl",
      nba: "nba",
      ncaab: "ncaab",
      ncaaf: "ncaaf",
      soccer_euro: "soccer/euro",
      soccer_mls: "soccer/mls"
    };
    REST_SPORT_CODES = {
      nfl: ["NFL"],
      mlb: ["MLB"],
      nhl: ["NHL"],
      nba: ["NBA"],
      ncaab: ["NCAABB", "NCAAB"],
      ncaaf: ["NCAAFB", "NCAAF"],
      soccer_euro: ["SOCCER", "SOCCER_EURO", "EURO_SOCCER"],
      soccer_mls: ["MLS", "SOCCER_MLS", "MLS_SOCCER"]
    };
    DATA_TYPE_PATH = {
      players: "players",
      teams: "teams",
      injuries: "injuries",
      news: "news",
      scores: "scores",
      schedule: "schedule",
      standings: "standings",
      projections: "projections",
      rankings: "rankings",
      adp: "adp",
      roster: "rosters"
    };
    riCacheBusterSeq = 0;
    cachedAccessToken = null;
    RI_REQUEST_TIMEOUT_MS = 1e4;
    RI_REST_PROBE_BUDGET_MS = 3e4;
    RI_SOCCER_REST_PROBE_BUDGET_MS = 12e3;
    RI_GRAPHQL_ROSTER_QUERY = {
      nfl: "nflRoster",
      mlb: "mlbRoster"
    };
    RI_GRAPHQL_QUERY_BODY = {
      nfl: `{
    nflRoster {
      id
      player
      position
      status
      team { abbrv }
      regularSeason {
        DK_fantasy_points
        DK_fantasy_points_per_game
        games_played
      }
    }
  }`,
      mlb: `{
    mlbRoster {
      id
      player
      position
      status
      team { abbrv }
      regularSeason {
        batting { HR RBI H SB }
      }
    }
  }`
    };
  }
});

// lib/sleeper-client.ts
function sleeperFetch(url, timeoutMs = SLEEPER_FETCH_TIMEOUT_MS) {
  return fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
}
async function getAllPlayers() {
  if (cachedPlayers && Date.now() - cacheTime < CACHE_TTL) {
    return cachedPlayers;
  }
  try {
    const response = await sleeperFetch(`${SLEEPER_API_BASE}/players/nfl`, SLEEPER_PLAYER_MAP_TIMEOUT_MS);
    if (!response.ok) return cachedPlayers || {};
    const data = await response.json();
    cachedPlayers = data;
    cacheTime = Date.now();
    return data;
  } catch {
    return cachedPlayers || {};
  }
}
async function getPlayersBySport(sport = "nfl") {
  if (sport === "nfl") {
    return getAllPlayers();
  }
  try {
    const response = await sleeperFetch(`${SLEEPER_API_BASE}/players/${sport}`, SLEEPER_PLAYER_MAP_TIMEOUT_MS);
    if (!response.ok) return {};
    return await response.json();
  } catch {
    return {};
  }
}
var SLEEPER_API_BASE, SLEEPER_FETCH_TIMEOUT_MS, SLEEPER_PLAYER_MAP_TIMEOUT_MS, cachedPlayers, cacheTime, CACHE_TTL, SLEEPER_PLAYERS_CACHE_TTL;
var init_sleeper_client = __esm({
  "lib/sleeper-client.ts"() {
    "use strict";
    SLEEPER_API_BASE = "https://api.sleeper.app/v1";
    SLEEPER_FETCH_TIMEOUT_MS = 12e3;
    SLEEPER_PLAYER_MAP_TIMEOUT_MS = 45e3;
    cachedPlayers = null;
    cacheTime = 0;
    CACHE_TTL = 24 * 60 * 60 * 1e3;
    SLEEPER_PLAYERS_CACHE_TTL = 6 * 60 * 60 * 1e3;
  }
});

// lib/workers/providers/sleeper-chain.ts
function namesClose(a, b) {
  return a.trim().toLowerCase() === b.trim().toLowerCase();
}
var sleeperChainProvider;
var init_sleeper_chain = __esm({
  "lib/workers/providers/sleeper-chain.ts"() {
    "use strict";
    init_sleeper_client();
    init_team_abbrev();
    init_api_config();
    sleeperChainProvider = {
      name: "sleeper",
      supports: ({ sport, dataType }) => {
        const chain = toApiChainSport(sport);
        if (chain !== "nfl") return false;
        return ["injuries", "players", "player_headshots", "team_logos"].includes(String(dataType));
      },
      async fetch({ dataType, query = {} }) {
        const searchRaw = typeof query.search === "string" ? query.search.trim() : "";
        const playerNameRaw = typeof query.playerName === "string" ? query.playerName.trim() : "";
        const search = searchRaw.toLowerCase();
        const playerName = playerNameRaw.toLowerCase();
        const teamCode2 = normalizeTeamAbbrev(String(query.teamCode ?? query.team ?? ""));
        const players = await getPlayersBySport("nfl");
        if (!players || typeof players !== "object") return null;
        if (String(dataType) === "team_logos") {
          const code = teamCode2 || normalizeTeamAbbrev(String(query.teamName ?? ""));
          if (!code) return null;
          const abbr = code.toLowerCase();
          const logoUrl = `https://sleepercdn.com/images/team_logos/nfl/${abbr}.png`;
          return {
            teamCode: code,
            teamName: String(query.teamName ?? code),
            logoUrl,
            logoUrlSm: logoUrl,
            logoUrlLg: logoUrl,
            logoSource: "sleeper"
          };
        }
        if (String(dataType) === "player_headshots") {
          const q = search || playerName;
          if (!q) return null;
          let best = null;
          for (const [id2, raw] of Object.entries(players)) {
            const p = raw;
            const name = String(p.full_name ?? `${p.first_name ?? ""} ${p.last_name ?? ""}`.trim());
            const nl = name.toLowerCase();
            if (!nl.includes(q) && !q.split(/\s+/).every((w) => w.length < 2 || nl.includes(w))) continue;
            const t = normalizeTeamAbbrev(String(p.team ?? ""));
            if (teamCode2 && t && !namesClose(t, teamCode2)) continue;
            const img = `https://sleepercdn.com/content/nfl/players/thumb/${id2}.jpg`;
            best = { id: id2, name, team: t, img };
            if (namesClose(name, searchRaw) || namesClose(name, playerNameRaw)) break;
          }
          if (!best) return null;
          return {
            playerId: best.id,
            playerName: best.name,
            teamCode: best.team,
            headshotUrl: best.img,
            headshotUrlSm: best.img,
            headshotUrlLg: best.img,
            headshotSource: "sleeper"
          };
        }
        if (String(dataType) === "injuries") {
          const rows = [];
          for (const [id2, raw] of Object.entries(players)) {
            const p = raw;
            const inj = p.injury_status;
            if (!inj || String(inj).trim() === "") continue;
            rows.push({
              playerId: id2,
              externalId: `sleeper:${id2}`,
              playerName: String(p.full_name ?? `${p.first_name ?? ""} ${p.last_name ?? ""}`.trim() ?? "Unknown"),
              team: normalizeTeamAbbrev(String(p.team ?? "")) ?? String(p.team ?? ""),
              status: String(inj),
              notes: p.injury_notes != null ? String(p.injury_notes) : null,
              reportDate: (/* @__PURE__ */ new Date()).toISOString(),
              source: "sleeper"
            });
          }
          return rows.slice(0, 800);
        }
        if (String(dataType) === "players") {
          const q = search || playerName;
          const out = [];
          for (const [id2, raw] of Object.entries(players)) {
            const p = raw;
            const name = String(p.full_name ?? `${p.first_name ?? ""} ${p.last_name ?? ""}`.trim());
            if (q && !name.toLowerCase().includes(q)) continue;
            out.push({
              id: id2,
              name,
              position: String(p.position ?? ""),
              team: normalizeTeamAbbrev(String(p.team ?? "")) ?? String(p.team ?? ""),
              status: p.injury_status != null ? String(p.injury_status) : null,
              imageUrl: p.avatar != null ? `https://sleepercdn.com/content/nfl/players/thumb/${id2}.jpg` : null,
              source: "sleeper"
            });
            if (out.length >= 250) break;
          }
          return out.length ? out : null;
        }
        return null;
      }
    };
  }
});

// lib/env/sports-media-keys.ts
function readTrimmedEnv(keys) {
  for (const key of keys) {
    const value = process.env[key];
    if (!value) continue;
    const trimmed = value.trim();
    if (trimmed) return trimmed;
  }
  return null;
}
function getTheSportsDbApiKey() {
  return readTrimmedEnv([
    "THESPORTSDB_API_KEY",
    "thesportsdb_api_key",
    "THEAUDIODB_API_KEY",
    "theaudiodb_api_key"
  ]);
}
function getTheSportsDbApiKeyOrFallback(fallbackKey = "3") {
  return getTheSportsDbApiKey() ?? fallbackKey;
}
var init_sports_media_keys = __esm({
  "lib/env/sports-media-keys.ts"() {
    "use strict";
  }
});

// lib/workers/providers/thesportsdb.ts
function apiKey() {
  return getTheSportsDbApiKeyOrFallback("123");
}
function leagueIdForSport(sport) {
  const chain = toApiChainSport(sport);
  if (chain === "soccer_mls") {
    const mls = process.env.THESPORTSDB_MLS_LEAGUE_ID?.trim();
    if (mls) return mls;
  }
  const key = chain ? apiChainSportToDbSport(chain) : sport.toUpperCase();
  return THESPORTSDB_LEAGUE_IDS[key] || "";
}
function toSearch3(value) {
  return typeof value === "string" ? value.trim() : "";
}
function namesEqual3(left, right) {
  return (left ?? "").trim().toLowerCase() === (right ?? "").trim().toLowerCase();
}
async function fetchTheSportsDb(path, params) {
  const url = new URL(`https://www.thesportsdb.com/api/v1/json/${apiKey()}/${path}`);
  Object.entries(params ?? {}).forEach(([key, value]) => {
    if (value) url.searchParams.set(key, value);
  });
  const response = await fetch(url.toString(), {
    method: "GET",
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(12e3)
  });
  if (!response.ok) return null;
  return await response.json();
}
function asRows(data, key) {
  if (!data) return [];
  const rows = data[key];
  return Array.isArray(rows) ? rows : [];
}
function resolvePlayerImage(row) {
  if (String(row.strCreativeCommons ?? "").trim().toLowerCase() !== "yes") return null;
  const options = [
    row.strCutout,
    row.strRender,
    row.strThumb,
    row.strFanart1
  ];
  for (const candidate of options) {
    if (typeof candidate === "string" && candidate.trim()) return candidate.trim();
  }
  return null;
}
function resolveTeamLogo(row) {
  const options = [
    row.strTeamBadge,
    row.strTeamLogo,
    row.strBadge,
    row.strLogo
  ];
  for (const candidate of options) {
    if (typeof candidate === "string" && candidate.trim()) return candidate.trim();
  }
  return null;
}
function resolveTeamCode(row) {
  return normalizeTeamAbbrev(String(row.strTeamShort ?? row.strTeamAlternate ?? "")) ?? null;
}
function matchesSport(row, sport) {
  const labels = {
    NFL: "American Football",
    NBA: "Basketball",
    NHL: "Ice Hockey",
    MLB: "Baseball",
    NCAAF: "American Football",
    NCAAB: "Basketball",
    SOCCER: "Soccer"
  };
  return namesEqual3(String(row.strSport ?? ""), labels[sport.toUpperCase()] ?? sport);
}
var THESPORTSDB_LEAGUE_IDS, theSportsDbProvider;
var init_thesportsdb = __esm({
  "lib/workers/providers/thesportsdb.ts"() {
    "use strict";
    init_team_abbrev();
    init_api_config();
    init_sports_media_keys();
    THESPORTSDB_LEAGUE_IDS = {
      NFL: "4391",
      NHL: "4380",
      NBA: "4387",
      MLB: "4424",
      NCAAB: process.env.THESPORTSDB_NCAAM_LEAGUE_ID || "4607",
      NCAAF: process.env.THESPORTSDB_NCAAF_LEAGUE_ID || "",
      SOCCER: process.env.THESPORTSDB_SOCCER_LEAGUE_ID || ""
    };
    theSportsDbProvider = {
      name: "thesportsdb",
      supports: ({ dataType }) => [
        "teams",
        "players",
        "games",
        "schedule",
        "player_headshots",
        "team_logos",
        "team_roster"
      ].includes(dataType),
      async fetch({ sport, dataType, query = {} }) {
        const leagueId = leagueIdForSport(sport);
        const search = toSearch3(query.search ?? query.playerName);
        const teamName = toSearch3(query.teamName);
        const teamCode2 = normalizeTeamAbbrev(String(query.teamCode ?? query.team ?? ""));
        switch (dataType) {
          case "teams": {
            if (!leagueId) return null;
            const leagueName = sport === "NFL" ? "NFL" : sport === "NBA" ? "NBA" : sport === "NHL" ? "NHL" : sport === "MLB" ? "MLB" : null;
            const data = leagueName ? await fetchTheSportsDb("search_all_teams.php", { l: leagueName }) : await fetchTheSportsDb("lookup_all_teams.php", { id: leagueId });
            const rows = asRows(data, "teams");
            return rows.map((team) => ({
              id: String(team.idTeam ?? ""),
              name: String(team.strTeam ?? ""),
              shortName: resolveTeamCode(team),
              city: String(team.strLocation ?? "").trim() || null,
              logo: resolveTeamLogo(team),
              source: "thesportsdb"
            })).filter((team) => team.id && team.name);
          }
          case "players": {
            if (!search) return null;
            const playerSearchParams = { p: search };
            if (teamName) playerSearchParams.t = teamName;
            else if (teamCode2) playerSearchParams.t = teamCode2;
            const data = await fetchTheSportsDb("searchplayers.php", playerSearchParams);
            const rows = asRows(data, "player");
            return rows.filter((player) => matchesSport(player, sport)).map((player) => ({
              id: String(player.idPlayer ?? ""),
              name: String(player.strPlayer ?? ""),
              position: String(player.strPosition ?? "").trim() || null,
              team: normalizeTeamAbbrev(String(player.strTeamShort ?? player.strTeam ?? "")) ?? null,
              teamId: player.idTeam ? String(player.idTeam) : null,
              height: String(player.strHeight ?? "").trim() || null,
              weight: String(player.strWeight ?? "").trim() || null,
              college: String(player.strCollege ?? "").trim() || null,
              imageUrl: resolvePlayerImage(player),
              source: "thesportsdb"
            })).filter((player) => player.id && player.name);
          }
          case "team_roster": {
            const teamId = toSearch3(query.teamId ?? query.id);
            if (!teamId) return null;
            const data = await fetchTheSportsDb("lookup_all_players.php", { id: teamId });
            const rows = asRows(data, "player");
            return rows.map((player) => ({
              id: String(player.idPlayer ?? ""),
              name: String(player.strPlayer ?? ""),
              position: String(player.strPosition ?? "").trim() || null,
              team: normalizeTeamAbbrev(String(player.strTeamShort ?? player.strTeam ?? "")) ?? null,
              teamId: player.idTeam ? String(player.idTeam) : null,
              height: String(player.strHeight ?? "").trim() || null,
              weight: String(player.strWeight ?? "").trim() || null,
              college: String(player.strCollege ?? "").trim() || null,
              imageUrl: resolvePlayerImage(player),
              source: "thesportsdb"
            })).filter((player) => player.id && player.name);
          }
          case "games":
          case "schedule": {
            if (!leagueId) return null;
            const data = await fetchTheSportsDb("eventsnextleague.php", { id: leagueId });
            const rows = asRows(data, "events");
            return rows.map((event) => ({
              id: String(event.idEvent ?? ""),
              homeTeam: normalizeTeamAbbrev(String(event.strHomeTeam ?? "")) ?? String(event.strHomeTeam ?? ""),
              awayTeam: normalizeTeamAbbrev(String(event.strAwayTeam ?? "")) ?? String(event.strAwayTeam ?? ""),
              date: String(event.dateEvent ?? event.strTimestamp ?? ""),
              status: String(event.strStatus ?? "scheduled"),
              season: String(event.strSeason ?? ""),
              venue: String(event.strVenue ?? "").trim() || null,
              source: "thesportsdb"
            })).filter((event) => event.id);
          }
          case "player_headshots": {
            if (!search) return null;
            const data = await fetchTheSportsDb("searchplayers.php", { p: search });
            const rows = asRows(data, "player");
            const matches2 = rows.filter((player) => {
              const sameName = namesEqual3(String(player.strPlayer ?? ""), search);
              const sameTeam = !teamCode2 || normalizeTeamAbbrev(String(player.strTeamShort ?? player.strTeam ?? "")) === teamCode2;
              return sameName && sameTeam && matchesSport(player, sport);
            });
            const matched = matches2.length === 1 ? matches2[0] : null;
            if (!matched?.idPlayer) return null;
            const full = asRows(await fetchTheSportsDb("lookupplayer.php", { id: String(matched.idPlayer) }), "players").find((p) => String(p.idPlayer) === String(matched.idPlayer) && matchesSport(p, sport));
            const imageUrl = full ? resolvePlayerImage(full) : null;
            if (!imageUrl) return null;
            return {
              playerId: String(matched?.idPlayer ?? ""),
              playerName: String(matched?.strPlayer ?? search),
              teamCode: normalizeTeamAbbrev(String(matched?.strTeamShort ?? matched?.strTeam ?? "")) ?? null,
              headshotUrl: imageUrl,
              headshotUrlSm: imageUrl,
              headshotUrlLg: imageUrl,
              headshotSource: "thesportsdb"
            };
          }
          case "team_logos": {
            const rows = teamName ? asRows(
              await fetchTheSportsDb("searchteams.php", { t: teamName }),
              "teams"
            ) : asRows(
              leagueId ? await fetchTheSportsDb("lookup_all_teams.php", { id: leagueId }) : null,
              "teams"
            );
            const matched = rows.find((team) => {
              const shortName = resolveTeamCode(team);
              return !!teamCode2 && shortName === teamCode2 || !!teamName && namesEqual3(String(team.strTeam ?? ""), teamName);
            });
            if (!matched || !matchesSport(matched, sport)) return null;
            const logoUrl = matched ? resolveTeamLogo(matched) : null;
            if (!logoUrl) return null;
            return {
              teamCode: resolveTeamCode(matched ?? {}) ?? teamCode2,
              teamName: String(matched?.strTeam ?? teamName ?? ""),
              logoUrl,
              logoUrlSm: logoUrl,
              logoUrlLg: logoUrl,
              logoSource: "thesportsdb"
            };
          }
          default:
            return null;
        }
      }
    };
  }
});

// lib/cfbd-env.ts
function normalizeKeyValue(raw) {
  const v = (raw ?? "").trim();
  if (v.length >= 2 && (v[0] === '"' && v.endsWith('"') || v[0] === "'" && v.endsWith("'"))) {
    return v.slice(1, -1).trim();
  }
  return v;
}
function getCfbdApiKey() {
  return normalizeKeyValue(process.env.CFBD_API_KEY) || normalizeKeyValue(process.env.CFBD_KEY) || normalizeKeyValue(process.env.COLLEGE_FOOTBALL_DATA_API_KEY) || "";
}
var init_cfbd_env = __esm({
  "lib/cfbd-env.ts"() {
    "use strict";
  }
});

// lib/workers/providers/cfbd.ts
function currentSeason() {
  return String((/* @__PURE__ */ new Date()).getFullYear());
}
function apiKey2() {
  return getCfbdApiKey();
}
async function cfbdFetch(path, params) {
  const key = apiKey2();
  if (!key) return null;
  const url = new URL(`${CFBD_BASE_URL}${path}`);
  Object.entries(params ?? {}).forEach(([param, value]) => {
    if (value) url.searchParams.set(param, value);
  });
  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${key}`,
      Accept: "application/json"
    }
  });
  if (!response.ok) return null;
  return await response.json();
}
function mapCfbdRosterToPlayerSeeds(rows) {
  if (!Array.isArray(rows)) return [];
  const seeds = [];
  for (const row of rows) {
    const first = String(row.firstName ?? row.first_name ?? "").trim();
    const last = String(row.lastName ?? row.last_name ?? "").trim();
    const name = `${first} ${last}`.trim();
    const position = String(row.position ?? "").trim().toUpperCase();
    if (!name || !FANTASY_POSITIONS.has(position)) continue;
    const team = String(row.team ?? "").trim();
    if (!team) continue;
    const externalId = String(row.id ?? "").trim();
    const jersey = Number(row.jersey);
    const classYear = Number(row.year ?? row.classYear);
    seeds.push({
      // A roster row without an id still has to be addressable, and name+team
      // is stable across refreshes for the same player at the same school.
      id: externalId || `${name}-${team}`,
      name,
      team,
      position,
      jersey: Number.isFinite(jersey) ? jersey : null,
      classYear: Number.isFinite(classYear) ? classYear : null,
      source: "cfbd"
    });
  }
  const byId = /* @__PURE__ */ new Map();
  for (const seed of seeds) byId.set(seed.id, seed);
  return [...byId.values()];
}
var FANTASY_POSITIONS, cfbdProvider;
var init_cfbd = __esm({
  "lib/workers/providers/cfbd.ts"() {
    "use strict";
    init_cfbd_fetch();
    init_api_config();
    init_cfbd_env();
    FANTASY_POSITIONS = /* @__PURE__ */ new Set(["QB", "RB", "FB", "WR", "TE", "K", "PK", "ATH"]);
    cfbdProvider = {
      name: "cfbd",
      supports: ({ sport, dataType }) => toApiChainSport(sport) === "ncaaf" && ["teams", "games", "schedule", "players"].includes(dataType),
      async fetch({ dataType, query = {} }) {
        const season = typeof query.season === "string" && query.season.trim() ? query.season.trim() : currentSeason();
        switch (dataType) {
          case "teams": {
            const rows = await cfbdFetch("/teams/fbs", { year: season });
            return (rows ?? []).map((team) => ({
              id: String(team.id ?? team.school ?? ""),
              name: String(team.school ?? ""),
              shortName: String(team.abbreviation ?? "").trim() || null,
              conference: String(team.conference ?? "").trim() || null,
              source: "cfbd"
            })).filter((team) => team.id && team.name);
          }
          case "players": {
            const rows = await cfbdFetch("/roster", {
              year: season,
              classification: "fbs"
            });
            return mapCfbdRosterToPlayerSeeds(rows);
          }
          case "games":
          case "schedule": {
            const rows = await cfbdFetch("/games", { year: season });
            return (rows ?? []).map((game) => ({
              id: String(game.id ?? `${game.home_team ?? ""}-${game.away_team ?? ""}-${game.start_date ?? ""}`),
              homeTeam: String(game.home_team ?? ""),
              awayTeam: String(game.away_team ?? ""),
              date: String(game.start_date ?? ""),
              status: String(game.completed ? "final" : "scheduled"),
              season,
              venue: String(game.venue ?? "").trim() || null,
              homeScore: typeof game.home_points === "number" ? game.home_points : null,
              awayScore: typeof game.away_points === "number" ? game.away_points : null,
              source: "cfbd"
            })).filter((game) => game.id);
          }
          default:
            return null;
        }
      }
    };
  }
});

// lib/workers/providers/espn.ts
async function fetchEspnJson(url) {
  try {
    const res = await fetch(url, {
      cache: "no-store",
      signal: AbortSignal.timeout(ESPN_TIMEOUT_MS)
    });
    if (!res.ok) return null;
    return await res.json();
  } catch {
    return null;
  }
}
function normalizeNewsArticles(json) {
  const articles = json?.articles;
  if (!Array.isArray(articles)) return [];
  return articles.map((a) => {
    const o = a;
    const links = o.links;
    return {
      id: o.id ?? null,
      headline: o.headline ?? o.title ?? null,
      description: o.description ?? null,
      published: o.published ?? null,
      url: links?.web?.href ?? null,
      source: "espn"
    };
  }).filter((a) => a.headline);
}
function normalizeInjuries(json) {
  const injuries = json?.injuries;
  if (!Array.isArray(injuries)) return [];
  const out = [];
  for (const teamBlock of injuries) {
    const block = teamBlock;
    const items = block.injuries;
    if (!Array.isArray(items)) continue;
    for (const item of items) {
      const i = item;
      const athlete = i.athlete;
      out.push({
        playerName: athlete?.displayName ?? null,
        playerId: athlete?.id ?? null,
        team: block.team?.abbreviation ?? null,
        status: i.status ?? null,
        type: i.type?.description ?? null,
        details: i.details?.detail ?? null,
        date: i.date ?? null,
        source: "espn"
      });
    }
  }
  return out;
}
var ESPN_SPORT_PATH, ESPN_TIMEOUT_MS, espnProvider;
var init_espn = __esm({
  "lib/workers/providers/espn.ts"() {
    "use strict";
    init_api_config();
    init_espnUrls();
    ESPN_SPORT_PATH = {
      nfl: { sport: "football", league: "nfl" },
      mlb: { sport: "baseball", league: "mlb" },
      nba: { sport: "basketball", league: "nba" },
      nhl: { sport: "hockey", league: "nhl" },
      ncaaf: { sport: "football", league: "college-football" },
      ncaab: { sport: "basketball", league: "mens-college-basketball" },
      soccer_mls: { sport: "soccer", league: "usa.1" },
      soccer_euro: { sport: "soccer", league: "eng.1" }
    };
    ESPN_TIMEOUT_MS = 8e3;
    espnProvider = {
      name: "espn",
      supports: ({ sport, dataType }) => {
        const cs = toApiChainSport(sport);
        if (!cs || !ESPN_SPORT_PATH[cs]) return false;
        return dataType === "news" || dataType === "injuries";
      },
      async fetch({ sport, dataType }) {
        const cs = toApiChainSport(sport);
        if (!cs) return null;
        const path = ESPN_SPORT_PATH[cs];
        if (!path) return null;
        const base = `${ESPN_SITE_API_BASE}/${path.sport}/${path.league}`;
        if (dataType === "news") {
          const json = await fetchEspnJson(`${base}/news`);
          const articles = normalizeNewsArticles(json);
          return articles.length ? articles : null;
        }
        if (dataType === "injuries") {
          const json = await fetchEspnJson(`${base}/injuries`);
          const items = normalizeInjuries(json);
          return items.length ? items : null;
        }
        return null;
      }
    };
  }
});

// lib/workers/sports-cache-persist.ts
function ttlMsForRow(kind) {
  if (kind === "news") return 24 * 60 * 60 * 1e3;
  if (kind === "injury") return 6 * 60 * 60 * 1e3;
  if (kind === "team") return 7 * 24 * 60 * 60 * 1e3;
  if (kind === "game") return 12 * 60 * 60 * 1e3;
  return 7 * 24 * 60 * 60 * 1e3;
}
async function persistNormalizedSportsRows(sport, dataType, data, source = DEFAULT_SOURCE) {
  const dbSport = apiChainSportToDbSport(sport);
  const expiresBase = new Date(Date.now() + ttlMsForRow("player"));
  if (dataType === "players" && Array.isArray(data)) {
    for (const raw of data) {
      if (!raw || typeof raw !== "object") continue;
      const player = raw;
      const id2 = String(player.id ?? player.playerId ?? "").trim();
      if (!id2) continue;
      const name = String(player.name ?? player.player ?? "").trim();
      if (!name) continue;
      await prisma.sportsPlayer.upsert({
        where: {
          sport_externalId_source: {
            sport: dbSport,
            externalId: id2,
            source
          }
        },
        update: {
          name,
          position: typeof player.position === "string" ? player.position : null,
          team: typeof player.team === "string" ? player.team : null,
          teamId: player.teamId != null ? String(player.teamId) : null,
          status: typeof player.status === "string" ? player.status : null,
          imageUrl: typeof player.imageUrl === "string" ? player.imageUrl : null,
          updatedAt: /* @__PURE__ */ new Date(),
          expiresAt: expiresBase
        },
        create: {
          sport: dbSport,
          externalId: id2,
          name,
          position: typeof player.position === "string" ? player.position : null,
          team: typeof player.team === "string" ? player.team : null,
          teamId: player.teamId != null ? String(player.teamId) : null,
          status: typeof player.status === "string" ? player.status : null,
          imageUrl: typeof player.imageUrl === "string" ? player.imageUrl : null,
          source,
          expiresAt: expiresBase
        }
      }).catch(() => {
      });
    }
    return;
  }
  if (dataType === "injuries" && Array.isArray(data)) {
    const exp = new Date(Date.now() + ttlMsForRow("injury"));
    for (const raw of data) {
      if (!raw || typeof raw !== "object") continue;
      const injury = raw;
      const externalId = String(
        injury.externalId ?? injury.playerId ?? `${injury.playerName ?? "unk"}:${injury.reportDate ?? ""}`
      ).slice(0, 180);
      const playerName = String(injury.playerName ?? injury.player ?? "Unknown");
      const reportDate = injury.reportDate ? new Date(String(injury.reportDate)) : /* @__PURE__ */ new Date();
      await prisma.sportsInjury.upsert({
        where: {
          sport_externalId_source: {
            sport: dbSport,
            externalId,
            source
          }
        },
        update: {
          status: typeof injury.status === "string" ? injury.status : null,
          description: typeof injury.notes === "string" ? injury.notes : typeof injury.bodyPart === "string" ? injury.bodyPart : null,
          date: reportDate,
          playerName,
          updatedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        },
        create: {
          sport: dbSport,
          externalId,
          playerName,
          playerId: injury.playerId != null ? String(injury.playerId) : null,
          team: typeof injury.team === "string" ? injury.team : null,
          status: typeof injury.status === "string" ? injury.status : null,
          description: typeof injury.notes === "string" ? injury.notes : typeof injury.bodyPart === "string" ? injury.bodyPart : null,
          date: reportDate,
          source,
          expiresAt: exp
        }
      }).catch(() => {
      });
    }
    return;
  }
  if (dataType === "news" && Array.isArray(data)) {
    const exp = new Date(Date.now() + ttlMsForRow("news"));
    for (const raw of data) {
      if (!raw || typeof raw !== "object") continue;
      const article = raw;
      const externalId = String(article.id ?? article.sourceId ?? article.headline ?? "").slice(0, 180);
      if (!externalId) continue;
      const title = String(article.title ?? article.headline ?? "").trim();
      if (!title) continue;
      const publishedAt = article.publishedAt ? new Date(String(article.publishedAt)) : article.date ? new Date(String(article.date)) : /* @__PURE__ */ new Date();
      await prisma.sportsNews.upsert({
        where: {
          sport_externalId_source: {
            sport: dbSport,
            externalId,
            source
          }
        },
        update: {
          title,
          sourceId: externalId,
          description: typeof article.description === "string" ? article.description : null,
          content: typeof article.body === "string" ? article.body : typeof article.content === "string" ? article.content : null,
          publishedAt,
          updatedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        },
        create: {
          sport: dbSport,
          externalId,
          sourceId: externalId,
          title,
          description: typeof article.description === "string" ? article.description : null,
          content: typeof article.body === "string" ? article.body : typeof article.content === "string" ? article.content : null,
          source,
          publishedAt,
          expiresAt: exp
        }
      }).catch(() => {
      });
    }
    return;
  }
  if (dataType === "teams" && Array.isArray(data)) {
    const exp = new Date(Date.now() + ttlMsForRow("team"));
    for (const raw of data) {
      if (!raw || typeof raw !== "object") continue;
      const team = raw;
      const externalId = String(team.id ?? team.teamId ?? team.externalId ?? "").trim();
      if (!externalId) continue;
      const name = String(team.name ?? team.team ?? "").trim();
      if (!name) continue;
      await prisma.sportsTeam.upsert({
        where: {
          sport_externalId_source: {
            sport: dbSport,
            externalId,
            source
          }
        },
        update: {
          name,
          shortName: typeof team.abbrv === "string" ? team.abbrv : typeof team.shortName === "string" ? team.shortName : null,
          city: typeof team.city === "string" ? team.city : null,
          logo: typeof team.logo === "string" ? team.logo : typeof team.img === "string" ? team.img : null,
          fetchedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        },
        create: {
          sport: dbSport,
          externalId,
          name,
          shortName: typeof team.abbrv === "string" ? team.abbrv : typeof team.shortName === "string" ? team.shortName : null,
          city: typeof team.city === "string" ? team.city : null,
          logo: typeof team.logo === "string" ? team.logo : typeof team.img === "string" ? team.img : null,
          source,
          fetchedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        }
      }).catch(() => {
      });
    }
    return;
  }
  if ((dataType === "schedule" || dataType === "scores") && Array.isArray(data)) {
    const exp = new Date(Date.now() + ttlMsForRow("game"));
    for (const raw of data) {
      if (!raw || typeof raw !== "object") continue;
      const game = raw;
      const externalId = String(game.gameId ?? game.id ?? game.externalId ?? "").trim();
      const homeTeam = String(game.homeTeam ?? game.home ?? "").trim();
      const awayTeam = String(game.awayTeam ?? game.away ?? "").trim();
      if (!externalId || !homeTeam || !awayTeam) continue;
      const seasonRaw = Number(game.season);
      const season = Number.isFinite(seasonRaw) ? Math.floor(seasonRaw) : (/* @__PURE__ */ new Date()).getUTCFullYear();
      const toScore = (value) => {
        if (value == null || value === "") return void 0;
        const n = Number(value);
        return Number.isFinite(n) ? Math.trunc(n) : void 0;
      };
      const homeScore = toScore(game.homeScore ?? game.homePoints ?? game.home_score);
      const awayScore = toScore(game.awayScore ?? game.awayPoints ?? game.away_score);
      const weekRaw = Number(game.week);
      const week = Number.isFinite(weekRaw) ? Math.floor(weekRaw) : void 0;
      const seasonType = typeof game.seasonType === "string" && game.seasonType.trim() ? game.seasonType.trim() : void 0;
      const scoring = {
        ...homeScore === void 0 ? {} : { homeScore },
        ...awayScore === void 0 ? {} : { awayScore },
        ...week === void 0 ? {} : { week },
        ...seasonType === void 0 ? {} : { seasonType }
      };
      await prisma.sportsGame.upsert({
        where: {
          sport_externalId_source: {
            sport: dbSport,
            externalId,
            source
          }
        },
        update: {
          homeTeam,
          awayTeam,
          status: typeof game.status === "string" ? game.status : null,
          startTime: typeof game.date === "string" ? new Date(game.date) : typeof game.startTime === "string" ? new Date(game.startTime) : null,
          venue: typeof game.venue === "string" ? game.venue : null,
          season,
          ...scoring,
          fetchedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        },
        create: {
          sport: dbSport,
          externalId,
          homeTeam,
          awayTeam,
          status: typeof game.status === "string" ? game.status : null,
          startTime: typeof game.date === "string" ? new Date(game.date) : typeof game.startTime === "string" ? new Date(game.startTime) : null,
          venue: typeof game.venue === "string" ? game.venue : null,
          season,
          source,
          ...scoring,
          fetchedAt: /* @__PURE__ */ new Date(),
          expiresAt: exp
        }
      }).catch(() => {
      });
    }
  }
}
var import_server_only3, DEFAULT_SOURCE;
var init_sports_cache_persist = __esm({
  "lib/workers/sports-cache-persist.ts"() {
    "use strict";
    import_server_only3 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_api_config();
    DEFAULT_SOURCE = "rolling_insights";
  }
});

// lib/scores/liveSourceSelection.ts
function sliceKey(row) {
  return row.season != null && row.week != null ? `${row.season}:${row.week}` : "";
}
function pickFreshestSourceRows(rows, now = Date.now()) {
  if (rows.length === 0) return rows;
  const slices = /* @__PURE__ */ new Map();
  for (const row of rows) {
    const key = sliceKey(row);
    const slice = slices.get(key);
    if (slice) slice.push(row);
    else slices.set(key, [row]);
  }
  if (slices.size === 1) return pickOneSource(rows, now);
  const keep = /* @__PURE__ */ new Set();
  for (const slice of slices.values()) {
    for (const row of pickOneSource(slice, now)) keep.add(row);
  }
  return rows.filter((row) => keep.has(row));
}
function pickOneSource(rows, now) {
  const bySource = /* @__PURE__ */ new Map();
  for (const row of rows) {
    const key = row.source ?? "";
    const stamp = row.fetchedAt ? row.fetchedAt.getTime() : 0;
    const entry = bySource.get(key);
    if (entry) {
      entry.rows.push(row);
      if (stamp > entry.newest) entry.newest = stamp;
    } else {
      bySource.set(key, { rows: [row], newest: stamp });
    }
  }
  const all = [...bySource.entries()];
  const live = all.filter(([, v]) => now - v.newest <= LIVE_SOURCE_DEAD_AFTER_MS);
  const available = live.length > 0 ? live : all;
  const rank = (source) => {
    const i = LIVE_SOURCE_PREFERENCE.indexOf(source);
    return i === -1 ? LIVE_SOURCE_PREFERENCE.length : i;
  };
  const unranked = LIVE_SOURCE_PREFERENCE.length;
  const rankedMax = available.reduce(
    (max, [source, v]) => rank(source) < unranked ? Math.max(max, v.rows.length) : max,
    0
  );
  const pool = rankedMax === 0 ? available : available.filter(
    ([source, v]) => rank(source) === unranked || v.rows.length >= LIVE_SOURCE_COVERAGE_FLOOR * rankedMax
  );
  const bucket = (newest) => Math.floor(Math.max(0, now - newest) / LIVE_SOURCE_STALENESS_BUCKET_MS);
  pool.sort(
    (a, b) => bucket(a[1].newest) - bucket(b[1].newest) || rank(a[0]) - rank(b[0]) || b[1].newest - a[1].newest
  );
  return pool[0][1].rows;
}
var LIVE_SOURCE_PREFERENCE, LIVE_SOURCE_DEAD_AFTER_MS, LIVE_SOURCE_STALENESS_BUCKET_MS, LIVE_SCORE_SOURCES, LIVE_SOURCE_COVERAGE_FLOOR;
var init_liveSourceSelection = __esm({
  "lib/scores/liveSourceSelection.ts"() {
    "use strict";
    LIVE_SOURCE_PREFERENCE = ["espn", "espn_live", "thesportsdb", "rolling_insights", "api_sports"];
    LIVE_SOURCE_DEAD_AFTER_MS = 6 * 60 * 60 * 1e3;
    LIVE_SOURCE_STALENESS_BUCKET_MS = 5 * 60 * 1e3;
    LIVE_SCORE_SOURCES = LIVE_SOURCE_PREFERENCE;
    LIVE_SOURCE_COVERAGE_FLOOR = 0.8;
  }
});

// lib/workers/api-chain.ts
function isPopulatedResult(value) {
  if (value == null) return false;
  if (Array.isArray(value)) return value.length > 0;
  if (typeof value === "object") return Object.keys(value).length > 0;
  return true;
}
function mergeQuery(params) {
  return { ...params.query ?? {}, ...params.options ?? {} };
}
function normalizeCacheValue(value) {
  if (value == null) return null;
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map((item) => normalizeCacheValue(item));
  if (typeof value === "object") {
    const input = value;
    return Object.keys(input).sort().reduce((acc, key) => {
      const normalized = normalizeCacheValue(input[key]);
      if (normalized !== void 0) acc[key] = normalized;
      return acc;
    }, {});
  }
  if (typeof value === "undefined") return void 0;
  return value;
}
function buildApiChainCacheKey(params) {
  const payload = normalizeCacheValue({
    query: params.query ?? {},
    options: params.options ?? {}
  });
  return `${params.sport}:${params.dataType}:${JSON.stringify(payload)}`;
}
async function tryRollingInsightsBlock(params, chainSport, dt, merged) {
  if (!isRollingInsightsEnabledForSport(chainSport)) return null;
  const startedRi = Date.now();
  try {
    const ri = await rollingInsightsProvider({
      ...params,
      sport: chainSport,
      dataType: dt,
      query: merged
    });
    if (isPopulatedResult(ri.data)) {
      return {
        data: ri.data,
        fromCache: false,
        source: "rolling_insights",
        latency: ri.latency ?? toLatency(startedRi),
        error: ri.error
      };
    }
  } catch (e) {
    console.warn(`[api-chain] Rolling Insights failed ${chainSport}/${dt}:`, e);
  }
  return null;
}
async function tryProviderBlock(provider, baseParams) {
  if (!provider.supports(baseParams)) return null;
  const startedAt = Date.now();
  try {
    const data = await provider.fetch(baseParams);
    if (isPopulatedResult(data)) {
      return {
        data,
        fromCache: false,
        source: provider.name,
        latency: toLatency(startedAt)
      };
    }
  } catch (e) {
    console.warn(`[api-chain] ${provider.name} failed:`, e);
  }
  return null;
}
function toLatency(startedAt) {
  return Math.max(0, Date.now() - startedAt);
}
function extractCachedPayload(raw) {
  if (raw == null) return null;
  if (Array.isArray(raw)) return raw;
  if (typeof raw === "object") {
    const o = raw;
    if (o.data != null && (Array.isArray(o.data) || typeof o.data === "object")) return o.data;
  }
  return raw;
}
function toPositiveInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}
function toIsoString(value) {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "string" && value.trim()) return value;
  return void 0;
}
function getFindManyModel(name) {
  const container = prisma;
  const candidate = container[name];
  if (!candidate || typeof candidate.findMany !== "function") return null;
  return candidate;
}
function freshOnly(now) {
  return { expiresAt: { gt: now } };
}
function measuredCacheAge(rows, nowMs) {
  let newest = 0;
  for (const row of rows) {
    const fetchedAt = row.fetchedAt;
    if (fetchedAt instanceof Date) newest = Math.max(newest, fetchedAt.getTime());
  }
  if (newest <= 0) return void 0;
  return Math.max(0, Math.floor((nowMs - newest) / 1e3));
}
function hasUnscopedDimension(mergedQuery) {
  return UNSCOPED_GAME_DIMENSIONS.some((key) => {
    const value = mergedQuery[key];
    return value != null && String(value).trim() !== "";
  });
}
async function readFromNormalizedTables(chainSport, dataType, mergedQuery) {
  const dbSport = apiChainSportToDbSport(chainSport);
  const limit = toPositiveInt(mergedQuery.limit, 200);
  const now = /* @__PURE__ */ new Date();
  const nowMs = now.getTime();
  const nameQ = typeof mergedQuery.playerName === "string" ? mergedQuery.playerName.trim() : "";
  const teamQ = typeof mergedQuery.team === "string" ? mergedQuery.team.trim() : typeof mergedQuery.teamAbbr === "string" ? mergedQuery.teamAbbr.trim() : "";
  const seasonNum = Number(mergedQuery.season);
  const weekNum = Number(mergedQuery.week);
  if (dataType === "players") {
    const model = getFindManyModel("sportsPlayer");
    if (!model) return null;
    const rows = await model.findMany({
      where: {
        sport: dbSport,
        ...freshOnly(now),
        ...nameQ ? { name: { contains: nameQ, mode: "insensitive" } } : {},
        ...teamQ ? { team: { equals: teamQ, mode: "insensitive" } } : {}
      },
      orderBy: [{ updatedAt: "desc" }],
      take: limit,
      select: {
        externalId: true,
        name: true,
        position: true,
        team: true,
        teamId: true,
        status: true,
        imageUrl: true,
        fetchedAt: true
      }
    });
    if (rows.length > 0) {
      return {
        data: rows.map((r) => ({
          id: r.externalId,
          name: r.name,
          position: r.position,
          team: r.team,
          teamId: r.teamId,
          status: r.status,
          imageUrl: r.imageUrl
        })),
        fromCache: true,
        source: "cache",
        cacheAge: measuredCacheAge(rows, nowMs)
      };
    }
    return null;
  }
  if (dataType === "teams") {
    const model = getFindManyModel("sportsTeam");
    if (!model) return null;
    const rows = await model.findMany({
      where: { sport: dbSport, ...freshOnly(now) },
      orderBy: [{ updatedAt: "desc" }],
      take: limit,
      select: {
        externalId: true,
        name: true,
        shortName: true,
        city: true,
        logo: true,
        fetchedAt: true
      }
    });
    if (rows.length > 0) {
      return {
        data: rows.map((r) => ({
          id: r.externalId,
          name: r.name,
          abbrv: r.shortName,
          city: r.city,
          logo: r.logo
        })),
        fromCache: true,
        source: "cache",
        cacheAge: measuredCacheAge(rows, nowMs)
      };
    }
    return null;
  }
  if (dataType === "injuries") {
    const model = getFindManyModel("sportsInjury");
    if (!model) return null;
    const rows = await model.findMany({
      where: {
        sport: dbSport,
        ...freshOnly(now),
        ...Number.isFinite(seasonNum) ? { season: Math.floor(seasonNum) } : {},
        ...Number.isFinite(weekNum) ? { week: Math.floor(weekNum) } : {},
        ...teamQ ? { team: { equals: teamQ, mode: "insensitive" } } : {},
        ...nameQ ? { playerName: { contains: nameQ, mode: "insensitive" } } : {}
      },
      orderBy: [{ date: "desc" }],
      take: limit,
      select: {
        externalId: true,
        playerId: true,
        playerName: true,
        team: true,
        status: true,
        description: true,
        date: true,
        fetchedAt: true
      }
    });
    if (rows.length > 0) {
      return {
        data: rows.map((r) => ({
          externalId: r.externalId,
          playerId: r.playerId,
          playerName: r.playerName,
          team: r.team,
          status: r.status,
          notes: r.description,
          reportDate: toIsoString(r.date)
        })),
        fromCache: true,
        source: "cache",
        cacheAge: measuredCacheAge(rows, nowMs)
      };
    }
    return null;
  }
  if (dataType === "news") {
    const model = getFindManyModel("sportsNews");
    if (!model) return null;
    const rows = await model.findMany({
      where: {
        sport: dbSport,
        ...freshOnly(now),
        ...teamQ ? { team: { equals: teamQ, mode: "insensitive" } } : {},
        ...nameQ ? { playerName: { contains: nameQ, mode: "insensitive" } } : {}
      },
      orderBy: [{ publishedAt: "desc" }],
      take: limit,
      select: {
        externalId: true,
        title: true,
        description: true,
        content: true,
        publishedAt: true,
        fetchedAt: true
      }
    });
    if (rows.length > 0) {
      return {
        data: rows.map((r) => ({
          id: r.externalId,
          title: r.title,
          description: r.description,
          content: r.content,
          publishedAt: toIsoString(r.publishedAt)
        })),
        fromCache: true,
        source: "cache",
        cacheAge: measuredCacheAge(rows, nowMs)
      };
    }
    return null;
  }
  if (dataType === "schedule" || dataType === "scores" || dataType === "games" || dataType === "live_game") {
    const model = getFindManyModel("sportsGame");
    if (!model) return null;
    if (hasUnscopedDimension(mergedQuery)) return null;
    const seasonTypeQ = typeof mergedQuery.seasonType === "string" ? mergedQuery.seasonType.trim() : "";
    const statusQ = typeof mergedQuery.status === "string" ? mergedQuery.status.trim() : "";
    const teamKey = teamQ ? normalizeTeamAbbrev(teamQ) || teamQ : "";
    const rows = await model.findMany({
      where: {
        sport: dbSport,
        ...freshOnly(now),
        /*
         * A live-score question reads only the ranked live feeds — the same LIVE_SCORE_SOURCES as
         * the scoreboard reader and Chimmy. A schedule question does not filter: `cfbd` is the
         * broadest schedule feed, and pickFreshestSourceRows only lets an unranked feed win when
         * every ranked feed is dead, which is what a schedule lookup should fall back to.
         */
        ...dataType === "scores" || dataType === "live_game" ? { source: { in: [...LIVE_SCORE_SOURCES] } } : {},
        ...Number.isFinite(seasonNum) ? { season: Math.floor(seasonNum) } : {},
        ...Number.isFinite(weekNum) ? { week: Math.floor(weekNum) } : {},
        ...seasonTypeQ ? { seasonType: seasonTypeQ } : {},
        ...statusQ ? { status: { equals: statusQ, mode: "insensitive" } } : {},
        ...teamKey ? {
          OR: [
            { homeTeam: { equals: teamKey, mode: "insensitive" } },
            { awayTeam: { equals: teamKey, mode: "insensitive" } }
          ]
        } : {}
      },
      orderBy: [{ startTime: "desc" }],
      /*
       * Over-read, because the dedup below happens in memory and discards whole
       * SOURCES. Taking `limit` here and then dropping every row that did not
       * come from the winning feed would return a fraction of what the caller
       * asked for — the result is sliced back to `limit` after the dedup.
       *
       * ⚠ A cross-source dedup by id is not available: `externalId` is the
       * PROVIDER's own id (`game.gameId` from Rolling Insights, `idEvent` from
       * TheSportsDB), so the same fixture carries a different id in each feed
       * and nothing matches them up. Choosing one feed wholesale is the only
       * dedup this table supports, which is why `pickFreshestSourceRows` works
       * the way it does.
       */
      take: Math.min(limit * 4, 1e3),
      select: {
        externalId: true,
        homeTeam: true,
        awayTeam: true,
        homeScore: true,
        awayScore: true,
        status: true,
        startTime: true,
        venue: true,
        week: true,
        seasonType: true,
        season: true,
        source: true,
        fetchedAt: true
      }
    });
    if (rows.length === 0) return null;
    const deduped = pickFreshestSourceRows(
      rows,
      nowMs
    ).slice(0, limit);
    const wantsScores = dataType === "scores" || dataType === "live_game";
    if (wantsScores && !deduped.some((r) => r.homeScore != null || r.awayScore != null)) {
      return null;
    }
    return {
      data: deduped.map((r) => ({
        id: r.externalId,
        gameId: r.externalId,
        homeTeam: r.homeTeam,
        awayTeam: r.awayTeam,
        homeScore: r.homeScore ?? null,
        awayScore: r.awayScore ?? null,
        status: r.status,
        date: toIsoString(r.startTime),
        venue: r.venue,
        week: r.week ?? null,
        seasonType: r.seasonType ?? null,
        season: r.season
      })),
      fromCache: true,
      source: "cache",
      cacheAge: measuredCacheAge(deduped, nowMs)
    };
  }
  return null;
}
async function saveToNormalizedTables(sport, dataType, data, source) {
  const chain = toApiChainSport(sport);
  if (!chain) return;
  const persistable = /* @__PURE__ */ new Set(["players", "injuries", "news", "teams", "schedule", "scores"]);
  if (!persistable.has(dataType)) return;
  await persistNormalizedSportsRows(chain, dataType, data, source);
}
async function fetchWithChain(params) {
  const chainSport = toApiChainSport(params.sport);
  if (!chainSport) {
    return { data: null, error: "Unsupported sport", fromCache: false };
  }
  const { dataType, forceRefresh } = params;
  const dt = String(dataType);
  const ttl = dt in API_CHAIN_TTLS ? API_CHAIN_TTLS[dt] : ttlSecondsForDataType(dt);
  const merged = mergeQuery(params);
  const cacheKey = buildApiChainCacheKey({
    sport: chainSport,
    dataType: dt,
    query: params.query,
    options: params.options
  });
  if (!forceRefresh) {
    try {
      const cached = await prisma.sportsDataCache.findUnique({
        where: { cacheKey }
      });
      if (cached?.data != null) {
        if (cached.expiresAt > /* @__PURE__ */ new Date()) {
          const inner = extractCachedPayload(cached.data);
          if (isPopulatedResult(inner)) {
            return {
              data: inner,
              fromCache: true,
              cacheAge: Math.floor((Date.now() - cached.createdAt.getTime()) / 1e3),
              source: "cache"
            };
          }
        }
      }
    } catch (e) {
      console.warn("[api-chain] cache lookup failed:", e);
    }
    try {
      const normalized = await readFromNormalizedTables(chainSport, dt, merged);
      if (normalized && isPopulatedResult(normalized.data)) {
        return normalized;
      }
    } catch (e) {
      console.warn("[api-chain] normalized lookup failed:", e);
    }
  }
  let result = null;
  const isImageDt = dt === "player_headshots" || dt === "team_logos";
  const skipGameLikeFallbacks = dt === "live_game" || dt === "games";
  const fallbackDt = dt === "scores" ? "games" : dt;
  const baseParams = { ...params, sport: chainSport, dataType: dt, query: merged };
  const fallbackParams = { ...baseParams, dataType: fallbackDt };
  if (isImageDt) {
    if (chainSport === "nfl") {
      result = await tryProviderBlock(theSportsDbProvider, baseParams) ?? await tryProviderBlock(sleeperChainProvider, baseParams) ?? await tryProviderBlock(apiSportsProvider, baseParams) ?? await tryProviderBlock(clearSportsProvider, baseParams) ?? await tryRollingInsightsBlock(params, chainSport, dt, merged);
    } else {
      result = await tryProviderBlock(clearSportsProvider, baseParams) ?? await tryProviderBlock(theSportsDbProvider, baseParams) ?? await tryProviderBlock(apiSportsProvider, baseParams) ?? await tryRollingInsightsBlock(params, chainSport, dt, merged) ?? await tryProviderBlock(sleeperChainProvider, baseParams);
    }
  } else {
    result = await tryRollingInsightsBlock(params, chainSport, dt, merged);
    if (!skipGameLikeFallbacks) {
      result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(theSportsDbProvider, fallbackParams) ?? result;
      result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(apiSportsProvider, fallbackParams) ?? result;
      result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(clearSportsProvider, fallbackParams) ?? result;
      result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(cfbdProvider, fallbackParams) ?? result;
    }
    result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(sleeperChainProvider, baseParams) ?? result;
    result = result && isPopulatedResult(result.data) ? result : await tryProviderBlock(espnProvider, baseParams) ?? result;
  }
  if (!result || !isPopulatedResult(result.data)) {
    return { data: null, error: result?.error ?? "All providers failed", fromCache: false };
  }
  const ok = result;
  const expiresAt = new Date(Date.now() + ttl * 1e3);
  try {
    await prisma.sportsDataCache.upsert({
      where: { cacheKey },
      update: { data: ok.data, expiresAt },
      create: {
        cacheKey,
        data: ok.data,
        expiresAt
      }
    });
  } catch (e) {
    console.error("[api-chain] cache save failed:", e);
  }
  await saveToNormalizedTables(chainSport, dt, ok.data, ok.source).catch(() => {
  });
  return {
    data: ok.data,
    fromCache: false,
    error: ok.error,
    cacheAge: ok.cacheAge,
    source: ok.source,
    latency: ok.latency
  };
}
var import_server_only4, UNSCOPED_GAME_DIMENSIONS, ApiChain, apiChain;
var init_api_chain = __esm({
  "lib/workers/api-chain.ts"() {
    "use strict";
    import_server_only4 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_api_config();
    init_api_sports2();
    init_clearsports();
    init_rolling_insights();
    init_sleeper_chain();
    init_thesportsdb();
    init_cfbd();
    init_espn();
    init_sports_cache_persist();
    init_liveSourceSelection();
    init_team_abbrev();
    UNSCOPED_GAME_DIMENSIONS = ["date", "dates", "day", "gameId", "gameID", "eventId"];
    ApiChain = class {
      async fetch(params) {
        const startedAt = Date.now();
        const attemptedSources = [];
        const chain = await fetchWithChain({
          sport: params.sport,
          dataType: params.dataType,
          query: params.query,
          options: params.options,
          forceRefresh: params.forceRefresh
        });
        if (chain.fromCache && chain.source === "cache") {
          return {
            data: chain.data,
            source: "cache",
            latency: 0,
            cached: true,
            attemptedSources
          };
        }
        if (chain.error && !chain.data) {
          return {
            data: null,
            source: "cache",
            latency: toLatency(startedAt),
            cached: false,
            attemptedSources,
            error: chain.error
          };
        }
        const src = chain.source ?? "rolling_insights";
        if (chain.source && chain.source !== "cache") {
          attemptedSources.push(chain.source);
        }
        return {
          data: chain.data,
          source: src,
          latency: chain.latency ?? toLatency(startedAt),
          cached: false,
          attemptedSources
        };
      }
    };
    apiChain = new ApiChain();
  }
});

// lib/live/espnGamePresentation.ts
var THREE_HOURS_MS, ESPN_DATE_FORMAT;
var init_espnGamePresentation = __esm({
  "lib/live/espnGamePresentation.ts"() {
    "use strict";
    THREE_HOURS_MS = 3 * 60 * 60 * 1e3;
    ESPN_DATE_FORMAT = new Intl.DateTimeFormat("en-CA", {
      timeZone: "America/New_York",
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    });
  }
});

// lib/live/espnGameSummary.ts
var init_espnGameSummary = __esm({
  "lib/live/espnGameSummary.ts"() {
    "use strict";
    init_espnGamePresentation();
  }
});

// lib/sport-teams/collegeTeamIdentity.ts
var init_collegeTeamIdentity = __esm({
  "lib/sport-teams/collegeTeamIdentity.ts"() {
    "use strict";
  }
});

// lib/sport-teams/collegeTeamIndexStore.ts
var MEMO_TTL_MS;
var init_collegeTeamIndexStore = __esm({
  "lib/sport-teams/collegeTeamIndexStore.ts"() {
    "use strict";
    init_prisma();
    init_collegeTeamIdentity();
    MEMO_TTL_MS = 15 * 60 * 1e3;
  }
});

// lib/sports-live-scores-service.ts
var import_server_only5, LIVE_SCORES_FRESHNESS_MS, EMPTY_LIVE_ONLY_BACKOFF_MS, LIVE_WINDOW_PAST_MS, LIVE_WINDOW_FUTURE_MS, ET_HOUR_MINUTE, ET_KICKOFF_CAPTION;
var init_sports_live_scores_service = __esm({
  "lib/sports-live-scores-service.ts"() {
    "use strict";
    import_server_only5 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_redactSecrets();
    init_teamAbbreviation();
    init_sport_scope();
    init_api_chain();
    init_api_config();
    init_espnUrls();
    init_espnGamePresentation();
    init_espnGameSummary();
    init_gameScoreProviders();
    init_liveSourceSelection();
    init_collegeTeamIndexStore();
    init_collegeTeamIdentity();
    init_liveSourceSelection();
    LIVE_SCORES_FRESHNESS_MS = 60 * 1e3;
    EMPTY_LIVE_ONLY_BACKOFF_MS = 10 * 60 * 1e3;
    LIVE_WINDOW_PAST_MS = 48 * 60 * 60 * 1e3;
    LIVE_WINDOW_FUTURE_MS = 21 * 24 * 60 * 60 * 1e3;
    ET_HOUR_MINUTE = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23"
    });
    ET_KICKOFF_CAPTION = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York",
      month: "numeric",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      timeZoneName: "short"
    });
  }
});

// lib/season-week/sportWeekSignal.ts
function toScheduleSportKey(sport) {
  const upper = String(sport ?? "").trim().toUpperCase();
  if (upper === "NCAAFB" || upper === "CFB" || upper === "COLLEGE_FOOTBALL") return "NCAAF";
  if (upper === "NCAABB") return "NCAAB";
  if (upper === "FOOTBALL") return "SOCCER";
  return upper;
}
function sportHasWeekSignal(sport) {
  return WEEK_SIGNAL_SPORTS.has(toScheduleSportKey(sport));
}
function isLiveStatus(raw) {
  const status = normalizeLiveGameStatus(raw);
  return status === "in_progress" || status === "halftime" || status === "overtime";
}
function isFinalStatus(raw) {
  return normalizeLiveGameStatus(raw) === "final";
}
function buildSlates(rows, now) {
  const byWeek = /* @__PURE__ */ new Map();
  for (const row of rows) {
    if (row.week == null || !Number.isFinite(row.week)) continue;
    if (row.startTime == null) continue;
    const seasonType = normalizeSeasonType(row.seasonType);
    const key = `${seasonType ?? "regular"}:${row.week}`;
    const bucket = byWeek.get(key);
    if (bucket) bucket.push(row);
    else byWeek.set(key, [row]);
  }
  const slates = [];
  for (const [key, bucket] of byWeek) {
    const deduped = pickFreshestSourceRows(bucket, now.getTime());
    if (deduped.length === 0) continue;
    const times = deduped.map((r) => r.startTime).filter((d) => d instanceof Date).map((d) => d.getTime());
    if (times.length === 0) continue;
    const liveCount = deduped.filter((r) => isLiveStatus(r.status)).length;
    const finalCount = deduped.filter((r) => isFinalStatus(r.status)).length;
    slates.push({
      week: Number(key.split(":")[1]),
      seasonType: key.split(":")[0] ?? "regular",
      gameCount: deduped.length,
      firstKickoffAt: new Date(Math.min(...times)),
      lastKickoffAt: new Date(Math.max(...times)),
      liveCount,
      finalCount,
      allFinal: finalCount === deduped.length
    });
  }
  return slates.sort((a, b) => a.week - b.week);
}
function stateFor(slate, now) {
  if (slate.liveCount > 0) return "live";
  if (now.getTime() < slate.firstKickoffAt.getTime()) return "upcoming";
  if (slate.allFinal && now.getTime() >= slate.lastKickoffAt.getTime()) return "played";
  return "between";
}
function relabelDailySportWeeks(rows, seasonStartUtc) {
  const DAY_MS = 864e5;
  const anchor = seasonStartUtc.getTime();
  const out = [];
  for (const row of rows) {
    const kickoff = row.startTime instanceof Date ? row.startTime : new Date(row.startTime);
    if (Number.isNaN(kickoff.getTime())) continue;
    if (kickoff.getTime() < anchor) continue;
    const week = Math.floor((kickoff.getTime() - anchor) / (7 * DAY_MS)) + 1;
    out.push({ ...row, week, seasonType: "regular" });
  }
  return out;
}
function resolveSportWeekFromSchedule(rows, now) {
  if (rows.length === 0) return { ok: false, reason: "NO_SCHEDULE_ROWS" };
  const slates = buildSlates(rows, now);
  if (slates.length === 0) return { ok: false, reason: "NO_DATED_ROWS" };
  const weeks = slates.map((s) => s.week);
  if (weeks.some((w) => w < 1 || w > MAX_PLAUSIBLE_SPORT_WEEK)) {
    return { ok: false, reason: "IMPLAUSIBLE_WEEKS" };
  }
  const nowMs = now.getTime();
  const live = slates.filter((s) => s.liveCount > 0);
  const started = slates.filter((s) => s.firstKickoffAt.getTime() <= nowMs);
  const current = (
    // Latest live week, so a Thursday game does not lose to a Sunday one.
    (live.length > 0 ? live[live.length - 1] : null) ?? (started.length > 0 ? started[started.length - 1] : null) ?? slates[0]
  );
  const index = slates.findIndex((s) => s.week === current.week);
  const next = index >= 0 && index + 1 < slates.length ? slates[index + 1].week : null;
  return {
    ok: true,
    sportWeek: current.week,
    seasonType: current.seasonType,
    state: stateFor(current, now),
    slate: current,
    nextSportWeek: next,
    source: "schedule"
  };
}
var WEEK_SIGNAL_SPORTS, MAX_PLAUSIBLE_SPORT_WEEK;
var init_sportWeekSignal = __esm({
  "lib/season-week/sportWeekSignal.ts"() {
    "use strict";
    init_cadence();
    init_gameScoreProviders();
    init_sports_live_scores_service();
    WEEK_SIGNAL_SPORTS = /* @__PURE__ */ new Set(["NFL", "NCAAF", "SOCCER"]);
    MAX_PLAUSIBLE_SPORT_WEEK = 60;
  }
});

// lib/season-week/dailySportSeasonStarts.ts
var dailySportSeasonStarts_exports = {};
__export(dailySportSeasonStarts_exports, {
  knownDailySportSeasons: () => knownDailySportSeasons,
  resolveDailySportSeasonStart: () => resolveDailySportSeasonStart
});
function resolveDailySportSeasonStart(sport, season) {
  const key = String(sport ?? "").trim().toUpperCase();
  const bySeason = REGULAR_SEASON_START_UTC[key];
  if (!bySeason) return null;
  const year = Number(season);
  if (!Number.isFinite(year)) return null;
  return bySeason[year] ?? null;
}
function knownDailySportSeasons(sport) {
  const key = String(sport ?? "").trim().toUpperCase();
  return Object.keys(REGULAR_SEASON_START_UTC[key] ?? {}).map(Number).sort((a, b) => a - b);
}
var REGULAR_SEASON_START_UTC;
var init_dailySportSeasonStarts = __esm({
  "lib/season-week/dailySportSeasonStarts.ts"() {
    "use strict";
    REGULAR_SEASON_START_UTC = {
      NHL: {
        // https://www.nhl.com/news/topic/catching-up-with/nhl-releases-full-2025-26-schedule
        2025: "2025-10-07T00:00:00.000Z",
        2026: "2026-09-29T00:00:00.000Z"
      },
      NBA: {
        // https://pr.nba.com/2025-26-nba-regular-season-schedule/
        2025: "2025-10-21T00:00:00.000Z",
        2026: "2026-10-20T00:00:00.000Z"
      },
      // Official Opening Night, including the standalone game before Opening Day.
      // https://www.mlb.com/news/mlb-2026-schedule-released
      MLB: {
        // https://www.mlb.com/news/mlb-2027-schedule-released — standalone Opening Night.
        2027: "2027-03-24T00:00:00.000Z",
        // Tokyo opener precedes domestic Opening Day.
        // https://www.mlb.com/press-release/press-release-mlb-announces-2025-regular-season-schedule
        2025: "2025-03-18T00:00:00.000Z",
        2026: "2026-03-25T00:00:00.000Z"
      },
      // The first 2026-27 NCAAB game in SportsGame (espn_live; measured 2026-09-24). Opening day is
      // the first Monday of November. Weeks run Monday-to-Sunday from here.
      NCAAB: {
        2026: "2026-11-02T00:00:00.000Z"
      }
    };
  }
});

// lib/season-week/seasonWeekService.ts
function requiresExplicitSeasonType(scheduleSport) {
  return scheduleSport !== "SOCCER";
}
async function resolveSportWeek(sport, seasonYear, deps = {}) {
  const db2 = deps.prisma ?? prisma;
  const now = deps.now ?? /* @__PURE__ */ new Date();
  const scheduleSport = toScheduleSportKey(sport);
  const dailyAnchor = sportHasWeekSignal(scheduleSport) ? null : resolveDailySportSeasonStart(scheduleSport, seasonYear);
  if (!sportHasWeekSignal(scheduleSport) && !dailyAnchor) {
    return { ok: false, reason: "NO_WEEK_SIGNAL" };
  }
  const rows = await db2.sportsGame.findMany({
    where: {
      sport: scheduleSport,
      // Daily seasons cross calendar years; the recorded opener supplies the
      // season boundary, independent of a provider's calendar-year label.
      ...dailyAnchor ? {} : { season: seasonYear },
      startTime: {
        gte: dailyAnchor ? new Date(dailyAnchor) : new Date(now.getTime() - LOOKBACK_MS),
        lte: dailyAnchor ? new Date(Math.min(now.getTime() + LOOKAHEAD_MS, new Date(dailyAnchor).getTime() + 365 * 864e5)) : new Date(now.getTime() + LOOKAHEAD_MS)
      }
    },
    select: {
      week: true,
      seasonType: true,
      startTime: true,
      status: true,
      source: true,
      fetchedAt: true
    }
  });
  if (dailyAnchor) {
    const relabelled = relabelDailySportWeeks(rows, new Date(dailyAnchor));
    return resolveSportWeekFromSchedule(relabelled, now);
  }
  const strict = requiresExplicitSeasonType(scheduleSport);
  const regularOnly = rows.filter((row) => {
    const type = normalizeSeasonType(row.seasonType);
    if (type === "regular") return true;
    return type == null && !strict;
  });
  if (rows.length > 0 && regularOnly.length === 0) {
    return { ok: false, reason: "NO_DATED_ROWS" };
  }
  return resolveSportWeekFromSchedule(regularOnly, now);
}
async function resolveSeasonWeekForRedraftSeason(seasonId, deps = {}) {
  const db2 = deps.prisma ?? prisma;
  const season = await db2.redraftSeason.findUnique({
    where: { id: seasonId },
    select: {
      sport: true,
      season: true,
      totalWeeks: true,
      playoffStartWeek: true
    }
  });
  if (!season) return { ok: false, reason: "SEASON_NOT_FOUND" };
  const lastPlayoffWeek = await resolveLastPlayoffWeek(db2, seasonId, season.playoffStartWeek);
  const sportWeek = await resolveSportWeek(season.sport, season.season, deps);
  return mapSportWeekToLeagueWeek(sportWeek, {
    sport: season.sport,
    seasonYear: season.season,
    totalWeeks: season.totalWeeks,
    playoffStartWeek: season.playoffStartWeek,
    lastPlayoffWeek: lastPlayoffWeek ?? void 0
  });
}
async function resolveLastPlayoffWeek(db2, seasonId, playoffStartWeek) {
  const round = await db2.redraftPlayoffRound.findFirst({
    where: { seasonId, roundNumber: { lt: 100 } },
    orderBy: { roundNumber: "desc" },
    select: { roundNumber: true }
  });
  if (round == null) return null;
  return playoffStartWeek + Math.max(1, round.roundNumber) - 1;
}
var LOOKAHEAD_MS, LOOKBACK_MS;
var init_seasonWeekService = __esm({
  "lib/season-week/seasonWeekService.ts"() {
    "use strict";
    init_prisma();
    init_gameScoreProviders();
    init_leagueSeasonWeek();
    init_sportWeekSignal();
    init_dailySportSeasonStarts();
    LOOKAHEAD_MS = 21 * 24 * 60 * 60 * 1e3;
    LOOKBACK_MS = 200 * 24 * 60 * 60 * 1e3;
  }
});

// lib/season-week/index.ts
var season_week_exports = {};
__export(season_week_exports, {
  MAX_PLAUSIBLE_SPORT_WEEK: () => MAX_PLAUSIBLE_SPORT_WEEK,
  mapSportWeekToLeagueWeek: () => mapSportWeekToLeagueWeek,
  resolveRegularSeasonEndWeek: () => resolveRegularSeasonEndWeek,
  resolveSeasonWeekForRedraftSeason: () => resolveSeasonWeekForRedraftSeason,
  resolveSportWeek: () => resolveSportWeek,
  resolveSportWeekFromSchedule: () => resolveSportWeekFromSchedule,
  sportHasWeekSignal: () => sportHasWeekSignal,
  toScheduleSportKey: () => toScheduleSportKey
});
var init_season_week = __esm({
  "lib/season-week/index.ts"() {
    "use strict";
    init_seasonWeekService();
    init_sportWeekSignal();
    init_leagueSeasonWeek();
  }
});

// lib/prisma-json.ts
function toPrismaJsonInput(value) {
  return value;
}
function toPrismaNullableJsonInput(value) {
  if (value === void 0) return void 0;
  if (value === null) return void 0;
  return value;
}
var init_prisma_json = __esm({
  "lib/prisma-json.ts"() {
    "use strict";
  }
});

// lib/events/types.ts
var DEFAULT_TENANT, EventValidationError;
var init_types2 = __esm({
  "lib/events/types.ts"() {
    "use strict";
    DEFAULT_TENANT = "allfantasy";
    EventValidationError = class extends Error {
      constructor(message) {
        super(message);
        this.name = "EventValidationError";
      }
    };
  }
});

// lib/events/envelope.ts
function isoOf(v, fallback) {
  if (!v) return fallback;
  const d = v instanceof Date ? v : new Date(v);
  return Number.isNaN(d.getTime()) ? fallback : d.toISOString();
}
function normalizeDomainEvent(input) {
  const now = (/* @__PURE__ */ new Date()).toISOString();
  const eventId = input.eventId ?? (0, import_node_crypto.randomUUID)();
  const { source: _s, correlationId: _c, causationId: _cz, ...extraMeta } = input.metadata ?? {};
  const event = {
    eventId,
    type: input.type,
    schemaVersion: input.schemaVersion ?? 1,
    occurredAt: isoOf(input.occurredAt, now),
    recordedAt: now,
    sport: input.sport ?? null,
    leagueConcept: input.leagueConcept ?? null,
    tenantId: input.tenantId ?? DEFAULT_TENANT,
    leagueId: input.leagueId ?? null,
    seasonId: input.seasonId ?? null,
    actor: { type: input.actor?.type ?? "system", id: input.actor?.id ?? null },
    period: input.period ?? null,
    subjects: input.subjects ?? [],
    payload: input.payload,
    metadata: {
      source: input.metadata?.source ?? "unknown",
      correlationId: input.metadata?.correlationId ?? null,
      causationId: input.metadata?.causationId ?? null,
      ...extraMeta
    },
    idempotencyKey: input.idempotencyKey ?? eventId
  };
  return Object.freeze(event);
}
var import_node_crypto, import_zod, eventActorSchema, eventPeriodSchema, eventSubjectSchema, eventMetadataSchema, domainEventEnvelopeSchema;
var init_envelope = __esm({
  "lib/events/envelope.ts"() {
    "use strict";
    import_node_crypto = require("node:crypto");
    import_zod = require("zod");
    init_types2();
    eventActorSchema = import_zod.z.object({
      type: import_zod.z.enum(["user", "commissioner", "system", "provider"]),
      id: import_zod.z.string().nullable().optional()
    });
    eventPeriodSchema = import_zod.z.object({
      kind: import_zod.z.enum(["week", "day", "gameday", "stage", "continuous", "none"]),
      index: import_zod.z.number().int().nullable().optional(),
      label: import_zod.z.string().nullable().optional()
    }).nullable();
    eventSubjectSchema = import_zod.z.object({
      kind: import_zod.z.string().min(1),
      id: import_zod.z.string().min(1),
      label: import_zod.z.string().nullable().optional()
    });
    eventMetadataSchema = import_zod.z.object({ source: import_zod.z.string().min(1) }).catchall(import_zod.z.unknown());
    domainEventEnvelopeSchema = import_zod.z.object({
      eventId: import_zod.z.string().min(1),
      type: import_zod.z.string().min(1).regex(/^[a-z0-9]+(?:[._][a-z0-9]+)*$/i, 'type must be dot/underscore namespaced (e.g. "competition.matchup.finalized")'),
      schemaVersion: import_zod.z.number().int().positive(),
      occurredAt: import_zod.z.string().min(1),
      recordedAt: import_zod.z.string().min(1),
      sport: import_zod.z.string().nullable(),
      leagueConcept: import_zod.z.string().nullable(),
      tenantId: import_zod.z.string().min(1),
      leagueId: import_zod.z.string().nullable(),
      seasonId: import_zod.z.string().nullable(),
      actor: eventActorSchema,
      period: eventPeriodSchema,
      subjects: import_zod.z.array(eventSubjectSchema),
      payload: import_zod.z.record(import_zod.z.unknown()),
      metadata: eventMetadataSchema,
      idempotencyKey: import_zod.z.string().min(1)
    });
  }
});

// lib/events/schemaRegistry.ts
function zodValidator(schema) {
  return (payload) => {
    const res = schema.safeParse(payload);
    if (res.success) return { ok: true };
    const error = res.error.issues.map((i) => `${i.path.join(".") || "(root)"}: ${i.message}`).join("; ");
    return { ok: false, error };
  };
}
var InMemoryEventSchemaRegistry;
var init_schemaRegistry = __esm({
  "lib/events/schemaRegistry.ts"() {
    "use strict";
    InMemoryEventSchemaRegistry = class {
      constructor() {
        /** type → version → validator */
        this.byType = /* @__PURE__ */ new Map();
      }
      register(type, version, validate) {
        if (!type) throw new Error("schema registry: type is required");
        if (!Number.isInteger(version) || version < 1) throw new Error(`schema registry: invalid version ${version} for ${type}`);
        const versions = this.byType.get(type) ?? /* @__PURE__ */ new Map();
        if (versions.has(version)) {
          throw new Error(`schema registry: ${type} v${version} already registered (versions are immutable; add a new version)`);
        }
        versions.set(version, validate);
        this.byType.set(type, versions);
      }
      has(type, version) {
        const versions = this.byType.get(type);
        if (!versions) return false;
        return version === void 0 ? versions.size > 0 : versions.has(version);
      }
      latestVersion(type) {
        const versions = this.byType.get(type);
        if (!versions || versions.size === 0) return void 0;
        return Math.max(...versions.keys());
      }
      validate(type, version, payload) {
        const validate = this.byType.get(type)?.get(version);
        if (!validate) return { ok: false, error: `no schema registered for ${type} v${version}` };
        return validate(payload);
      }
    };
  }
});

// lib/events/eventBus.ts
function matches(pattern, type) {
  if (pattern === "*" || pattern === type) return true;
  if (pattern.endsWith(".*")) {
    const prefix = pattern.slice(0, -2);
    return type === prefix || type.startsWith(prefix + ".");
  }
  return false;
}
var InProcessEventBus, g, inProcessEventBus;
var init_eventBus = __esm({
  "lib/events/eventBus.ts"() {
    "use strict";
    InProcessEventBus = class {
      constructor() {
        this.subs = /* @__PURE__ */ new Set();
      }
      subscribe(typePattern, handler) {
        const sub = { pattern: typePattern, handler };
        this.subs.add(sub);
        return () => {
          this.subs.delete(sub);
        };
      }
      async publish(event) {
        const targets = [...this.subs].filter((s) => matches(s.pattern, event.type));
        await Promise.all(
          targets.map(async (s) => {
            try {
              await s.handler(event);
            } catch {
            }
          })
        );
      }
    };
    g = globalThis;
    inProcessEventBus = g.__afEventBus ?? (g.__afEventBus = new InProcessEventBus());
  }
});

// lib/events/normalizer.ts
var EventNormalizer;
var init_normalizer = __esm({
  "lib/events/normalizer.ts"() {
    "use strict";
    init_envelope();
    init_types2();
    EventNormalizer = class {
      constructor(registry, opts = {}) {
        this.registry = registry;
        this.opts = opts;
      }
      normalize(input) {
        const event = normalizeDomainEvent(input);
        const envelope = domainEventEnvelopeSchema.safeParse(event);
        if (!envelope.success) {
          const detail = envelope.error.issues.map((i) => `${i.path.join(".") || "(root)"}: ${i.message}`).join("; ");
          throw new EventValidationError(`invalid event envelope for "${event.type}": ${detail}`);
        }
        if (this.registry.has(event.type, event.schemaVersion)) {
          const res = this.registry.validate(event.type, event.schemaVersion, event.payload);
          if (!res.ok) throw new EventValidationError(`invalid payload for ${event.type} v${event.schemaVersion}: ${res.error}`);
        } else if (this.opts.strict) {
          throw new EventValidationError(`no schema registered for ${event.type} v${event.schemaVersion} (strict mode)`);
        } else {
          return Object.freeze({
            ...event,
            metadata: { ...event.metadata, schemaUnregistered: true }
          });
        }
        return event;
      }
    };
  }
});

// lib/events/eventPublisher.ts
var EventPublisher;
var init_eventPublisher = __esm({
  "lib/events/eventPublisher.ts"() {
    "use strict";
    EventPublisher = class {
      constructor(normalizer, outbox) {
        this.normalizer = normalizer;
        this.outbox = outbox;
      }
      async publish(input, opts) {
        const event = this.normalizer.normalize(input);
        await this.outbox.enqueue(event, opts);
        return event;
      }
    };
  }
});

// lib/events/outboxRelay.ts
var import_node_crypto2, noopLogger, sleep2, OutboxRelay;
var init_outboxRelay = __esm({
  "lib/events/outboxRelay.ts"() {
    "use strict";
    import_node_crypto2 = require("node:crypto");
    noopLogger = () => {
    };
    sleep2 = (ms) => new Promise((r) => setTimeout(r, ms));
    OutboxRelay = class {
      constructor(store, opts = {}) {
        this.store = store;
        this.consumers = opts.consumers ?? [];
        this.bus = opts.bus;
        this.batchSize = opts.batchSize ?? 100;
        this.maxRetries = opts.maxRetries ?? 5;
        this.baseRetryMs = opts.baseRetryMs ?? 5e3;
        this.maxRetryMs = opts.maxRetryMs ?? 5 * 6e4;
        this.dryRun = opts.dryRun ?? false;
        this.workerId = opts.workerId ?? `relay-${(0, import_node_crypto2.randomUUID)().slice(0, 8)}`;
        this.claimTimeoutMs = opts.claimTimeoutMs ?? 6e4;
        this.logger = opts.logger ?? noopLogger;
        this.now = opts.now ?? (() => /* @__PURE__ */ new Date());
      }
      /** This relay's worker id (stamped on claimed rows). */
      get id() {
        return this.workerId;
      }
      backoffMs(attempts) {
        return Math.min(this.baseRetryMs * 2 ** Math.max(0, attempts - 1), this.maxRetryMs);
      }
      /** Process exactly one batch. Never throws for a single bad event. */
      async runOnce() {
        const now = this.now();
        const items = this.dryRun ? await this.store.claimPending(this.batchSize, now) : await this.store.claimBatch(this.workerId, { batchSize: this.batchSize, staleClaimMs: this.claimTimeoutMs, now });
        const summary = {
          fetched: items.length,
          dispatched: 0,
          retried: 0,
          deadLettered: 0,
          failed: 0,
          dryRun: this.dryRun,
          failures: []
        };
        for (const { event, attempts } of items) {
          if (this.dryRun) {
            this.logger("info", "relay dry-run: would dispatch", { workerId: this.workerId, eventId: event.eventId, type: event.type, attempts });
            continue;
          }
          try {
            for (const consumer of this.consumers) {
              await consumer.handle(event);
            }
            if (this.bus) {
              try {
                await this.bus.publish(event);
              } catch (busErr) {
                this.logger("warn", "relay bus fan-out failed (non-fatal)", {
                  workerId: this.workerId,
                  eventId: event.eventId,
                  error: busErr instanceof Error ? busErr.message : String(busErr)
                });
              }
            }
            await this.store.markDispatched(event.eventId);
            summary.dispatched += 1;
          } catch (err) {
            const error = err instanceof Error ? err.message : String(err);
            const nextAttempts = attempts + 1;
            if (nextAttempts >= this.maxRetries) {
              await this.store.markDead(event.eventId, error);
              summary.deadLettered += 1;
              this.logger("error", "relay dead-lettered event", { workerId: this.workerId, eventId: event.eventId, type: event.type, attempts: nextAttempts, error });
            } else {
              const nextAvailableAt = new Date(now.getTime() + this.backoffMs(nextAttempts));
              await this.store.markRetry(event.eventId, error, nextAvailableAt);
              summary.retried += 1;
              this.logger("warn", "relay retry scheduled", { workerId: this.workerId, eventId: event.eventId, attempts: nextAttempts, nextAvailableAt, error });
            }
            summary.failures.push({ eventId: event.eventId, error });
          }
        }
        summary.failed = summary.retried + summary.deadLettered;
        this.logger("info", "relay batch complete", {
          workerId: this.workerId,
          fetched: summary.fetched,
          dispatched: summary.dispatched,
          retried: summary.retried,
          deadLettered: summary.deadLettered,
          dryRun: summary.dryRun
        });
        return summary;
      }
      /** Back-compat alias for a single batch. */
      async dispatchPending() {
        return this.runOnce();
      }
      /** Drain in a loop until empty (or maxBatches / shouldStop). Dry-run does a single batch. */
      async run(opts = {}) {
        const maxBatches = this.dryRun ? 1 : opts.maxBatches ?? Number.POSITIVE_INFINITY;
        const stopWhenEmpty = opts.stopWhenEmpty ?? true;
        const agg = { fetched: 0, dispatched: 0, retried: 0, deadLettered: 0, failed: 0, dryRun: this.dryRun, failures: [] };
        let batches = 0;
        while (batches < maxBatches) {
          if (opts.shouldStop?.()) break;
          const s = await this.runOnce();
          agg.fetched += s.fetched;
          agg.dispatched += s.dispatched;
          agg.retried += s.retried;
          agg.deadLettered += s.deadLettered;
          agg.failures.push(...s.failures);
          batches += 1;
          if (s.fetched === 0 && stopWhenEmpty) break;
          if (opts.intervalMs && s.fetched > 0) await sleep2(opts.intervalMs);
        }
        agg.failed = agg.retried + agg.deadLettered;
        return agg;
      }
    };
  }
});

// lib/events/outboxStore.ts
function toRow(event) {
  return {
    eventId: event.eventId,
    type: event.type,
    schemaVersion: event.schemaVersion,
    occurredAt: new Date(event.occurredAt),
    sport: event.sport,
    leagueConcept: event.leagueConcept,
    tenantId: event.tenantId,
    leagueId: event.leagueId,
    seasonId: event.seasonId,
    actorType: event.actor.type,
    actorId: event.actor.id ?? null,
    source: event.metadata.source,
    correlationId: event.metadata.correlationId ?? null,
    causationId: event.metadata.causationId ?? null,
    idempotencyKey: event.idempotencyKey,
    payload: event.payload,
    metadata: event.metadata,
    period: event.period,
    subjects: event.subjects
  };
}
function rowToDomainEvent(row) {
  return Object.freeze({
    eventId: row.eventId,
    type: row.type,
    schemaVersion: row.schemaVersion,
    occurredAt: row.occurredAt.toISOString(),
    recordedAt: row.recordedAt.toISOString(),
    sport: row.sport,
    leagueConcept: row.leagueConcept,
    tenantId: row.tenantId,
    leagueId: row.leagueId,
    seasonId: row.seasonId,
    actor: { type: row.actorType, id: row.actorId },
    period: row.period ?? null,
    subjects: row.subjects ?? [],
    payload: row.payload ?? {},
    metadata: { source: row.source, ...row.metadata ?? {} },
    idempotencyKey: row.idempotencyKey
  });
}
var PrismaOutboxStore;
var init_outboxStore = __esm({
  "lib/events/outboxStore.ts"() {
    "use strict";
    PrismaOutboxStore = class {
      /**
       * @param client the base Prisma client (used for relay reads/marks).
       *   `enqueue` prefers `opts.tx` so writes join the caller's transaction.
       */
      constructor(client) {
        this.client = client;
      }
      db(opts) {
        return opts?.tx ?? this.client;
      }
      async enqueue(event, opts) {
        const db2 = this.db(opts);
        await db2.domainEvent.create({ data: toRow(event) });
        await db2.eventOutbox.create({
          data: { eventId: event.eventId, status: "pending", attempts: 0, availableAt: /* @__PURE__ */ new Date() }
        });
      }
      async fetchPending(limit, now = /* @__PURE__ */ new Date()) {
        return (await this.claimPending(limit, now)).map((i) => i.event);
      }
      /** Read-only peek of due rows (no claiming) — used for dry-run. */
      async claimPending(limit, now = /* @__PURE__ */ new Date()) {
        const due = await this.client.eventOutbox.findMany({
          where: { status: { in: ["pending", "retry"] }, availableAt: { lte: now } },
          orderBy: { createdAt: "asc" },
          take: limit
        });
        if (due.length === 0) return [];
        const ids = due.map((p) => p.eventId);
        const attemptsById = new Map(due.map((p) => [p.eventId, p.attempts]));
        const rows = await this.client.domainEvent.findMany({ where: { eventId: { in: ids } } });
        const byId = new Map(rows.map((r) => [r.eventId, r]));
        return ids.map((id2) => byId.get(id2)).filter((r) => Boolean(r)).map((r) => ({ event: rowToDomainEvent(r), attempts: attemptsById.get(r.eventId) ?? 0 }));
      }
      /**
       * Atomic claim via Postgres `FOR UPDATE SKIP LOCKED` + a CAS status flip to 'claimed'.
       * Two workers can never claim the same row. Picks due rows (pending/retry, availableAt<=now)
       * and stale claims (status='claimed', claimedAt older than staleClaimMs).
       */
      async claimBatch(workerId, opts) {
        const now = opts.now ?? /* @__PURE__ */ new Date();
        const staleThreshold = new Date(now.getTime() - opts.staleClaimMs);
        const sql = `
      UPDATE "event_outbox"
      SET status = 'claimed', "claimedBy" = $1, "claimedAt" = $2
      WHERE id IN (
        SELECT id FROM "event_outbox"
        WHERE ((status IN ('pending','retry') AND "availableAt" <= $2)
               OR (status = 'claimed' AND "claimedAt" IS NOT NULL AND "claimedAt" < $3))
        ORDER BY "createdAt" ASC
        LIMIT $4
        FOR UPDATE SKIP LOCKED
      )
      RETURNING "eventId", attempts`;
        const claimed = await this.client.$queryRawUnsafe(sql, workerId, now, staleThreshold, opts.batchSize);
        if (!claimed.length) return [];
        const ids = claimed.map((c) => c.eventId);
        const attemptsById = new Map(claimed.map((c) => [c.eventId, Number(c.attempts)]));
        const rows = await this.client.domainEvent.findMany({ where: { eventId: { in: ids } } });
        const byId = new Map(rows.map((r) => [r.eventId, r]));
        return ids.map((id2) => byId.get(id2)).filter((r) => Boolean(r)).map((r) => ({ event: rowToDomainEvent(r), attempts: attemptsById.get(r.eventId) ?? 0 }));
      }
      async markDispatched(eventId) {
        await this.client.eventOutbox.update({
          where: { eventId },
          data: { status: "dispatched", dispatchedAt: /* @__PURE__ */ new Date(), claimedBy: null, claimedAt: null }
        });
      }
      async markRetry(eventId, error, nextAvailableAt) {
        await this.client.eventOutbox.update({
          where: { eventId },
          data: { status: "retry", attempts: { increment: 1 }, lastError: error.slice(0, 1e3), availableAt: nextAvailableAt, claimedBy: null, claimedAt: null }
        });
      }
      /** @deprecated use markRetry */
      async markFailed(eventId, error, nextAvailableAt) {
        await this.markRetry(eventId, error, nextAvailableAt);
      }
      async markDead(eventId, error) {
        await this.client.eventOutbox.update({
          where: { eventId },
          data: { status: "dead", attempts: { increment: 1 }, lastError: error.slice(0, 1e3), claimedBy: null, claimedAt: null }
        });
      }
    };
  }
});

// lib/events/projections/auditFeed.ts
var init_auditFeed = __esm({
  "lib/events/projections/auditFeed.ts"() {
    "use strict";
    init_outboxStore();
  }
});

// lib/events/container.ts
function build(overrides = {}) {
  const registry = overrides.registry ?? new InMemoryEventSchemaRegistry();
  const normalizer = overrides.normalizer ?? new EventNormalizer(registry, { strict: overrides.strict ?? false });
  const bus = overrides.bus ?? inProcessEventBus;
  const outboxStore = overrides.outboxStore ?? new PrismaOutboxStore(prisma);
  const publisher = overrides.publisher ?? new EventPublisher(normalizer, outboxStore);
  const relay = overrides.relay ?? new OutboxRelay(outboxStore, { bus });
  return { registry, normalizer, bus, outboxStore, publisher, relay };
}
function getEventInfrastructure() {
  return g2.__afEventInfra ?? (g2.__afEventInfra = build());
}
var g2;
var init_container = __esm({
  "lib/events/container.ts"() {
    "use strict";
    init_prisma();
    init_eventBus();
    init_normalizer();
    init_eventPublisher();
    init_outboxRelay();
    init_schemaRegistry();
    init_outboxStore();
    g2 = globalThis;
  }
});

// lib/events/catalog.ts
function registerPlatformEventSchemas(registry) {
  for (const type of ALL_EVENT_TYPES) {
    const version = EVENT_SCHEMA_VERSION[type];
    if (!registry.has(type, version)) {
      registry.register(type, version, zodValidator(EVENT_PAYLOAD_SCHEMAS[type]));
    }
  }
}
var import_zod2, EVENT, id, optId, EVENT_PAYLOAD_SCHEMAS, ALL_EVENT_TYPES, EVENT_SCHEMA_VERSION;
var init_catalog = __esm({
  "lib/events/catalog.ts"() {
    "use strict";
    import_zod2 = require("zod");
    init_schemaRegistry();
    EVENT = {
      // League lifecycle
      LEAGUE_CREATED: "lifecycle.league.created",
      LEAGUE_ARCHIVED: "lifecycle.league.archived",
      SEASON_ACTIVATED: "lifecycle.season.activated",
      SEASON_COMPLETED: "lifecycle.season.completed",
      SCHEDULE_GENERATED: "lifecycle.schedule.generated",
      SEASON_SNAPSHOT_CREATED: "lifecycle.season.snapshot_created",
      LEAGUE_ENTERED_OFFSEASON: "lifecycle.league.entered_offseason",
      // Draft lifecycle + picks
      DRAFT_STARTED: "draft.session.started",
      DRAFT_PAUSED: "draft.session.paused",
      DRAFT_RESUMED: "draft.session.resumed",
      DRAFT_COMPLETED: "draft.session.completed",
      DRAFT_PICK_MADE: "draft.pick.made",
      // Roster + lineup
      ROSTER_PLAYER_ADDED: "roster.player.added",
      ROSTER_PLAYER_DROPPED: "roster.player.dropped",
      LINEUP_SET: "roster.lineup.set",
      LINEUP_LOCKED: "roster.lineup.locked",
      // Trades
      TRADE_PROPOSED: "transaction.trade.proposed",
      TRADE_COUNTERED: "transaction.trade.countered",
      TRADE_ACCEPTED: "transaction.trade.accepted",
      TRADE_DECLINED: "transaction.trade.declined",
      TRADE_REJECTED: "transaction.trade.rejected",
      TRADE_CANCELED: "transaction.trade.canceled",
      TRADE_VETOED: "transaction.trade.vetoed",
      TRADE_PROCESSED: "transaction.trade.processed",
      TRADE_EXPIRED: "transaction.trade.expired",
      // Waivers / free agency
      WAIVER_SUBMITTED: "transaction.waiver.submitted",
      WAIVER_CANCELED: "transaction.waiver.canceled",
      WAIVER_PROCESSED: "transaction.waiver.processed",
      WAIVER_WINDOW_PROCESSED: "transaction.waiver.window_processed",
      // Competition: matchups / scoring / standings / playoffs
      MATCHUP_CREATED: "competition.matchup.created",
      MATCHUP_UPDATED: "competition.matchup.updated",
      MATCHUP_FINALIZED: "competition.matchup.finalized",
      SCORE_UPDATED: "competition.score.updated",
      STANDINGS_UPDATED: "competition.standings.updated",
      PLAYOFF_BRACKET_GENERATED: "competition.playoff.bracket_generated",
      PLAYOFF_ADVANCED: "competition.playoff.advanced",
      CHAMPION_CROWNED: "competition.champion.crowned",
      // Governance / commissioner
      SETTINGS_CHANGED: "governance.settings.changed",
      COMMISSIONER_ACTION: "governance.commissioner.action",
      // User + chat activity
      USER_ACTIVITY: "user.activity.recorded",
      CHAT_MESSAGE_POSTED: "chat.message.posted",
      // Authentication
      AUTH_REGISTERED: "auth.user.registered",
      AUTH_SIGNED_IN: "auth.user.signed_in",
      // Subscription / entitlement
      SUBSCRIPTION_CHANGED: "billing.subscription.changed",
      ENTITLEMENT_CHANGED: "billing.entitlement.changed",
      // Ingestion — the trigger side of "one event system" (see lib/sports-os/reactions.ts).
      // These are emitted BY ingestion, never by a request path, and they are what makes an import
      // fan out to the projections, rankings and alerts it invalidates.
      INGEST_LEAGUE_STARTED: "ingest.league.started",
      INGEST_LEAGUE_COMPLETED: "ingest.league.completed",
      INGEST_LEAGUE_FAILED: "ingest.league.failed",
      INGEST_ROSTERS_REFRESHED: "ingest.rosters.refreshed",
      INGEST_SCORES_REFRESHED: "ingest.scores.refreshed",
      INGEST_PLAYER_VALUES_REFRESHED: "ingest.player_values.refreshed",
      INGEST_PROJECTIONS_REFRESHED: "ingest.projections.refreshed"
    };
    id = import_zod2.z.string().min(1);
    optId = import_zod2.z.string().min(1).optional();
    EVENT_PAYLOAD_SCHEMAS = {
      // ── League lifecycle ──
      [EVENT.LEAGUE_CREATED]: import_zod2.z.object({ leagueId: id, name: import_zod2.z.string().optional(), teamCount: import_zod2.z.number().int().nonnegative().optional() }),
      [EVENT.LEAGUE_ARCHIVED]: import_zod2.z.object({ leagueId: id }),
      [EVENT.SEASON_ACTIVATED]: import_zod2.z.object({ seasonId: id, season: import_zod2.z.number().int().optional() }),
      [EVENT.SEASON_COMPLETED]: import_zod2.z.object({ seasonId: id }),
      [EVENT.SCHEDULE_GENERATED]: import_zod2.z.object({ seasonId: id, regularSeasonWeeks: import_zod2.z.number().int().optional(), matchupCount: import_zod2.z.number().int().optional() }),
      [EVENT.SEASON_SNAPSHOT_CREATED]: import_zod2.z.object({ seasonId: id, snapshotId: id }),
      [EVENT.LEAGUE_ENTERED_OFFSEASON]: import_zod2.z.object({ seasonId: id, snapshotId: id }),
      // ── Draft ──
      [EVENT.DRAFT_STARTED]: import_zod2.z.object({ draftId: id }),
      [EVENT.DRAFT_PAUSED]: import_zod2.z.object({ draftId: id }),
      [EVENT.DRAFT_RESUMED]: import_zod2.z.object({ draftId: id }),
      [EVENT.DRAFT_COMPLETED]: import_zod2.z.object({ draftId: id, pickCount: import_zod2.z.number().int().optional() }),
      [EVENT.DRAFT_PICK_MADE]: import_zod2.z.object({
        draftId: id,
        rosterId: optId,
        playerId: optId,
        overall: import_zod2.z.number().int().optional(),
        round: import_zod2.z.number().int().optional(),
        isAuto: import_zod2.z.boolean().optional(),
        bidAmount: import_zod2.z.number().optional()
      }),
      // ── Roster / lineup ──
      [EVENT.ROSTER_PLAYER_ADDED]: import_zod2.z.object({ rosterId: id, playerId: id, via: import_zod2.z.string().optional() }),
      [EVENT.ROSTER_PLAYER_DROPPED]: import_zod2.z.object({ rosterId: id, playerId: id, via: import_zod2.z.string().optional() }),
      [EVENT.LINEUP_SET]: import_zod2.z.object({ rosterId: id, changeCount: import_zod2.z.number().int().optional() }),
      [EVENT.LINEUP_LOCKED]: import_zod2.z.object({ rosterId: id }),
      // ── Trades ──
      [EVENT.TRADE_PROPOSED]: import_zod2.z.object({ tradeId: id, proposerRosterId: optId, receiverRosterId: optId }),
      [EVENT.TRADE_COUNTERED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_ACCEPTED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_DECLINED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_REJECTED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_CANCELED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_VETOED]: import_zod2.z.object({ tradeId: id, byUserId: optId }),
      [EVENT.TRADE_PROCESSED]: import_zod2.z.object({ tradeId: id }),
      [EVENT.TRADE_EXPIRED]: import_zod2.z.object({ tradeId: id }),
      // ── Waivers ──
      [EVENT.WAIVER_SUBMITTED]: import_zod2.z.object({ claimId: id, rosterId: optId, addPlayerId: optId, dropPlayerId: optId, bid: import_zod2.z.number().optional() }),
      [EVENT.WAIVER_CANCELED]: import_zod2.z.object({ claimId: id }),
      [EVENT.WAIVER_PROCESSED]: import_zod2.z.object({ claimId: optId, rosterId: optId, result: import_zod2.z.string(), addPlayerId: optId, dropPlayerId: optId, bid: import_zod2.z.number().optional() }),
      [EVENT.WAIVER_WINDOW_PROCESSED]: import_zod2.z.object({ processed: import_zod2.z.number().int(), succeeded: import_zod2.z.number().int().optional(), failed: import_zod2.z.number().int().optional() }),
      // ── Competition ──
      [EVENT.MATCHUP_CREATED]: import_zod2.z.object({ matchupId: id }),
      [EVENT.MATCHUP_UPDATED]: import_zod2.z.object({ matchupId: id }),
      [EVENT.MATCHUP_FINALIZED]: import_zod2.z.object({ matchupId: id, homeScore: import_zod2.z.number().optional(), awayScore: import_zod2.z.number().optional(), winnerRosterId: optId }),
      [EVENT.SCORE_UPDATED]: import_zod2.z.object({ subjectId: id, subjectKind: import_zod2.z.string(), points: import_zod2.z.number().optional() }),
      [EVENT.STANDINGS_UPDATED]: import_zod2.z.object({ seasonId: id, changedRosterCount: import_zod2.z.number().int().optional() }),
      [EVENT.PLAYOFF_BRACKET_GENERATED]: import_zod2.z.object({ seasonId: id, playoffTeams: import_zod2.z.number().int().optional() }),
      [EVENT.PLAYOFF_ADVANCED]: import_zod2.z.object({ seasonId: id, round: import_zod2.z.number().int().optional(), advanced: import_zod2.z.number().int().optional() }),
      [EVENT.CHAMPION_CROWNED]: import_zod2.z.object({ seasonId: id, championRosterId: optId, championUserId: optId }),
      // ── Governance ──
      [EVENT.SETTINGS_CHANGED]: import_zod2.z.object({ leagueId: id, section: import_zod2.z.string().optional(), changedKeys: import_zod2.z.array(import_zod2.z.string()).optional() }),
      [EVENT.COMMISSIONER_ACTION]: import_zod2.z.object({ leagueId: id, action: import_zod2.z.string(), targetId: optId }),
      // ── User / chat ──
      [EVENT.USER_ACTIVITY]: import_zod2.z.object({ userId: id, action: import_zod2.z.string() }),
      [EVENT.CHAT_MESSAGE_POSTED]: import_zod2.z.object({ channelId: id, messageId: id, authorUserId: optId, scope: import_zod2.z.string().optional() }),
      // ── Auth ──
      [EVENT.AUTH_REGISTERED]: import_zod2.z.object({ userId: id }),
      [EVENT.AUTH_SIGNED_IN]: import_zod2.z.object({ userId: id, method: import_zod2.z.string().optional() }),
      // ── Billing ──
      [EVENT.SUBSCRIPTION_CHANGED]: import_zod2.z.object({ userId: id, status: import_zod2.z.string(), plan: import_zod2.z.string().optional() }),
      [EVENT.ENTITLEMENT_CHANGED]: import_zod2.z.object({ userId: id, feature: import_zod2.z.string(), granted: import_zod2.z.boolean() }),
      // -- Ingestion --
      // `provider` is the source name ('sleeper', 'mfl', 'rolling-insights'). NEVER a URL and never a
      // token-bearing one: Rolling Insights passes RSC_token as a query parameter, so a provider URL in
      // an event payload is a credential in the outbox (see CLAUDE.md, Credentials).
      [EVENT.INGEST_LEAGUE_STARTED]: import_zod2.z.object({ leagueId: id, provider: import_zod2.z.string(), mode: import_zod2.z.string().optional() }),
      [EVENT.INGEST_LEAGUE_COMPLETED]: import_zod2.z.object({
        leagueId: id,
        provider: import_zod2.z.string(),
        mode: import_zod2.z.string().optional(),
        durationMs: import_zod2.z.number().int().nonnegative().optional(),
        rosterCount: import_zod2.z.number().int().nonnegative().optional(),
        playerCount: import_zod2.z.number().int().nonnegative().optional()
      }),
      [EVENT.INGEST_LEAGUE_FAILED]: import_zod2.z.object({ leagueId: id, provider: import_zod2.z.string(), stage: import_zod2.z.string().optional() }),
      [EVENT.INGEST_ROSTERS_REFRESHED]: import_zod2.z.object({ leagueId: id, provider: import_zod2.z.string(), changedRosterCount: import_zod2.z.number().int().nonnegative().optional() }),
      [EVENT.INGEST_SCORES_REFRESHED]: import_zod2.z.object({ leagueId: optId, sport: import_zod2.z.string().optional(), period: import_zod2.z.number().int().optional(), changedCount: import_zod2.z.number().int().nonnegative().optional() }),
      [EVENT.INGEST_PLAYER_VALUES_REFRESHED]: import_zod2.z.object({ source: import_zod2.z.string(), playerCount: import_zod2.z.number().int().nonnegative().optional() }),
      [EVENT.INGEST_PROJECTIONS_REFRESHED]: import_zod2.z.object({ sport: import_zod2.z.string().optional(), period: import_zod2.z.number().int().optional(), playerCount: import_zod2.z.number().int().nonnegative().optional() })
    };
    ALL_EVENT_TYPES = Object.keys(EVENT_PAYLOAD_SCHEMAS);
    EVENT_SCHEMA_VERSION = Object.fromEntries(
      ALL_EVENT_TYPES.map((t) => [t, 1])
    );
  }
});

// lib/events/producers.ts
function getPlatformEvents() {
  if (g3.__afPlatformEvents) return g3.__afPlatformEvents;
  const infra = getEventInfrastructure();
  registerPlatformEventSchemas(infra.registry);
  g3.__afPlatformEvents = new PlatformEventProducer(infra.publisher);
  return g3.__afPlatformEvents;
}
var PlatformEventProducer, g3;
var init_producers = __esm({
  "lib/events/producers.ts"() {
    "use strict";
    init_container();
    init_catalog();
    PlatformEventProducer = class {
      constructor(publisher, registry) {
        this.publisher = publisher;
        if (registry) registerPlatformEventSchemas(registry);
      }
      build(type, args) {
        return {
          type,
          schemaVersion: EVENT_SCHEMA_VERSION[type],
          payload: args.payload,
          occurredAt: args.occurredAt,
          sport: args.sport ?? null,
          leagueConcept: args.leagueConcept ?? null,
          leagueId: args.leagueId ?? null,
          seasonId: args.seasonId ?? null,
          actor: args.actor,
          period: args.period ?? null,
          subjects: args.subjects ?? [],
          idempotencyKey: args.idempotencyKey,
          metadata: {
            ...args.metadata ?? {},
            source: args.source ?? "engine",
            correlationId: args.correlationId ?? null,
            causationId: args.causationId ?? null
          }
        };
      }
      /** Best-effort, post-commit emission. NEVER throws — safe to call from any business path. */
      async emit(type, args) {
        try {
          return await this.publisher.publish(this.build(type, args));
        } catch (err) {
          console.warn(`[events] best-effort emit failed for ${type}:`, err instanceof Error ? err.message : err);
          return null;
        }
      }
      /** Transactional emission — joins the caller's tx (atomic with the business write). Propagates errors. */
      async emitInTx(tx, type, args) {
        const opts = { tx };
        return this.publisher.publish(this.build(type, args), opts);
      }
    };
    g3 = globalThis;
  }
});

// lib/events/index.ts
var init_events = __esm({
  "lib/events/index.ts"() {
    "use strict";
    init_types2();
    init_envelope();
    init_schemaRegistry();
    init_eventBus();
    init_normalizer();
    init_eventPublisher();
    init_outboxRelay();
    init_auditFeed();
    init_outboxStore();
    init_container();
    init_catalog();
    init_producers();
  }
});

// lib/devy/scoringEligibilityEngine.ts
async function sumWeeklyPointsForPlayers(playerRows, week, season) {
  let pts = 0;
  for (const p of playerRows) {
    const row = await prisma.playerWeeklyScore.findUnique({
      where: {
        playerId_week_season_sport: {
          playerId: p.playerId,
          week,
          season,
          sport: p.sport
        }
      }
    });
    pts += row?.fantasyPts ?? 0;
  }
  return pts;
}
async function scoreFromRedraftRoster(rosterId, week, season) {
  const starters = await prisma.redraftRosterPlayer.findMany({
    where: {
      rosterId,
      droppedAt: null,
      slotType: { notIn: ["bench", "taxi", "devy"] }
    }
  });
  const bench = await prisma.redraftRosterPlayer.findMany({
    where: {
      rosterId,
      droppedAt: null,
      slotType: "bench"
    }
  });
  const taxi = await prisma.redraftRosterPlayer.findMany({
    where: {
      rosterId,
      droppedAt: null,
      slotType: "taxi"
    }
  });
  const officialScore = await sumWeeklyPointsForPlayers(starters, week, season);
  const displayBenchScore = await sumWeeklyPointsForPlayers(bench, week, season);
  const displayTaxiScore = await sumWeeklyPointsForPlayers(taxi, week, season);
  return {
    officialScore,
    displayBenchScore,
    displayTaxiScore,
    devyScore: 0
  };
}
async function calculateOfficialTeamScore(leagueId, rosterId, week, season) {
  const devyLeague = await prisma.devyLeague.findUnique({ where: { leagueId } });
  if (!devyLeague) {
    const starters2 = await prisma.redraftRosterPlayer.findMany({
      where: {
        rosterId,
        droppedAt: null,
        slotType: { notIn: ["bench", "taxi", "devy"] }
      }
    });
    const officialScore2 = await sumWeeklyPointsForPlayers(starters2, week, season);
    return { officialScore: officialScore2, displayBenchScore: 0, displayTaxiScore: 0, devyScore: 0 };
  }
  const states = await prisma.devyPlayerState.findMany({
    where: { leagueId, rosterId }
  });
  if (states.length === 0) {
    return scoreFromRedraftRoster(rosterId, week, season);
  }
  const starters = states.filter((s) => s.bucketState === "active_starter");
  const sportByPlayer = /* @__PURE__ */ new Map();
  const rp = await prisma.redraftRosterPlayer.findMany({
    where: { rosterId, droppedAt: null }
  });
  for (const row of rp) {
    sportByPlayer.set(row.playerId, row.sport);
  }
  async function sumBucket(bucket) {
    const ids = states.filter((s) => s.bucketState === bucket).map((s) => s.playerId);
    const rows = ids.map((playerId) => ({
      playerId,
      sport: sportByPlayer.get(playerId) ?? "NFL"
    }));
    return sumWeeklyPointsForPlayers(rows, week, season);
  }
  let officialScore = 0;
  for (const s of starters) {
    const sport = sportByPlayer.get(s.playerId) ?? "NFL";
    const row = await prisma.playerWeeklyScore.findUnique({
      where: {
        playerId_week_season_sport: {
          playerId: s.playerId,
          week,
          season,
          sport
        }
      }
    });
    officialScore += row?.fantasyPts ?? 0;
  }
  const displayBenchScore = await sumBucket("active_bench");
  const displayTaxiScore = await sumBucket("taxi");
  const devyScore = 0;
  return { officialScore, displayBenchScore, displayTaxiScore, devyScore };
}
async function leagueUsesDevyEngine(leagueId) {
  const row = await prisma.devyLeague.findUnique({ where: { leagueId }, select: { id: true } });
  return row != null;
}
var init_scoringEligibilityEngine = __esm({
  "lib/devy/scoringEligibilityEngine.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/c2c/scoringEngine.ts
async function leagueUsesC2CEngine(leagueId) {
  const row = await prisma.c2CLeague.findUnique({ where: { leagueId }, select: { id: true } });
  return !!row;
}
function getScoringEligibility(playerSide, bucketState, devyScoringEnabled) {
  if (bucketState === "campus_starter" && playerSide === "campus") return "counts_campus";
  if (bucketState === "canton_starter" && playerSide === "canton") return "counts_canton";
  if (bucketState === "devy" && devyScoringEnabled && playerSide === "campus") return "none";
  if (["bench", "taxi", "ir", "devy"].includes(bucketState)) return "display_only";
  return "display_only";
}
async function fantasyPtsForPlayer(playerId, sport, week, season) {
  const row = await prisma.playerWeeklyScore.findUnique({
    where: {
      playerId_week_season_sport: { playerId, week, season, sport }
    }
  });
  return row?.fantasyPts ?? 0;
}
function computeOfficialScore(cfg, campus, canton) {
  if (cfg.scoringMode === "weighted_combined") {
    return campus * cfg.campusScoreWeight + canton * cfg.cantonScoreWeight;
  }
  return campus + canton;
}
async function sumByBuckets(cfg, rows, week, season) {
  let campusStarterScore = 0;
  let cantonStarterScore = 0;
  let benchDisplayScore = 0;
  let taxiDisplayScore = 0;
  let devyDisplayScore = 0;
  for (const r of rows) {
    const elig = getScoringEligibility(r.playerSide, r.bucketState, cfg.devyScoringEnabled);
    const pts = await fantasyPtsForPlayer(r.playerId, r.sport, week, season);
    if (elig === "counts_campus") campusStarterScore += pts;
    else if (elig === "counts_canton") cantonStarterScore += pts;
    else if (elig === "display_only") {
      if (r.bucketState === "bench") benchDisplayScore += pts;
      else if (r.bucketState === "taxi") taxiDisplayScore += pts;
      else if (r.bucketState === "devy") devyDisplayScore += pts;
      else if (r.bucketState === "ir") benchDisplayScore += pts;
      else benchDisplayScore += pts;
    }
  }
  const officialTeamScore = computeOfficialScore(cfg, campusStarterScore, cantonStarterScore);
  return {
    campusStarterScore,
    cantonStarterScore,
    benchDisplayScore,
    taxiDisplayScore,
    devyDisplayScore,
    officialTeamScore
  };
}
async function calculateC2CTeamScore(leagueId, rosterId, matchupId, week, season) {
  const cfg = await prisma.c2CLeague.findUnique({ where: { leagueId } });
  if (!cfg) throw new Error("C2C league config not found");
  const rows = await prisma.c2CPlayerState.findMany({ where: { leagueId, rosterId } });
  const parts = await sumByBuckets(cfg, rows, week, season);
  return prisma.c2CMatchupScore.upsert({
    where: {
      leagueId_matchupId_rosterId: { leagueId, matchupId, rosterId }
    },
    create: {
      leagueId,
      matchupId,
      rosterId,
      week,
      season,
      campusStarterScore: parts.campusStarterScore,
      cantonStarterScore: parts.cantonStarterScore,
      benchDisplayScore: parts.benchDisplayScore,
      taxiDisplayScore: parts.taxiDisplayScore,
      devyDisplayScore: parts.devyDisplayScore,
      officialTeamScore: parts.officialTeamScore
    },
    update: {
      campusStarterScore: parts.campusStarterScore,
      cantonStarterScore: parts.cantonStarterScore,
      benchDisplayScore: parts.benchDisplayScore,
      taxiDisplayScore: parts.taxiDisplayScore,
      devyDisplayScore: parts.devyDisplayScore,
      officialTeamScore: parts.officialTeamScore
    }
  });
}
async function updateC2CMatchupScores(matchupId) {
  const m = await prisma.redraftMatchup.findFirst({
    where: { id: matchupId },
    include: { season: true }
  });
  if (!m || !m.awayRosterId) return;
  const cfg = await prisma.c2CLeague.findUnique({ where: { leagueId: m.leagueId } });
  if (!cfg) return;
  const week = m.week;
  const season = m.season.season;
  const home = await calculateC2CTeamScore(m.leagueId, m.homeRosterId, matchupId, week, season);
  const away = await calculateC2CTeamScore(m.leagueId, m.awayRosterId, matchupId, week, season);
  await prisma.redraftMatchup.update({
    where: { id: matchupId },
    data: {
      homeScore: home.officialTeamScore,
      awayScore: away.officialTeamScore,
      status: "active"
    }
  });
}
var init_scoringEngine = __esm({
  "lib/c2c/scoringEngine.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/sportConfig/configs/cricket.ts
var CRICKET_CONFIG;
var init_cricket = __esm({
  "lib/sportConfig/configs/cricket.ts"() {
    "use strict";
    CRICKET_CONFIG = {
      sport: "CRICKET",
      displayName: "Cricket",
      slug: "cricket",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "runs_bat", label: "Runs (batting)", defaultPoints: 1, isToggleable: false, group: "batting", sport: "CRICKET" },
        { key: "wickets_bowl", label: "Wickets", defaultPoints: 20, isToggleable: false, group: "bowling", sport: "CRICKET" },
        { key: "maidens", label: "Maidens", defaultPoints: 4, isToggleable: true, group: "bowling", sport: "CRICKET" },
        { key: "catches", label: "Catches", defaultPoints: 8, isToggleable: true, group: "fielding", sport: "CRICKET" },
        { key: "run_outs", label: "Run outs", defaultPoints: 12, isToggleable: true, group: "fielding", sport: "CRICKET" },
        { key: "stumpings", label: "Stumpings (WK)", defaultPoints: 15, isToggleable: true, group: "fielding", sport: "CRICKET" }
      ],
      scoringPresets: [{ name: "T20 baseline", categories: [] }],
      defaultRosterSlots: [
        { key: "BAT", label: "Batter", eligiblePositions: ["BAT"], defaultCount: 4, minCount: 3, maxCount: 6, isOptional: false },
        { key: "BOWL", label: "Bowler", eligiblePositions: ["BOWL"], defaultCount: 4, minCount: 3, maxCount: 6, isOptional: false },
        { key: "AR", label: "All-rounder", eligiblePositions: ["AR"], defaultCount: 2, minCount: 1, maxCount: 3, isOptional: true },
        { key: "WK", label: "Wicket-keeper", eligiblePositions: ["WK"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false }
      ],
      defaultBenchSlots: 3,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        BAT: ["BAT", "AR"],
        BOWL: ["BOWL", "AR"],
        AR: ["AR"],
        WK: ["WK"]
      },
      defaultSeasonWeeks: 20,
      defaultPlayoffStartWeek: 18,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [
        { key: "format", label: "Primary format", type: "select", defaultValue: "T20", options: [{ value: "T20", label: "T20" }, { value: "ODI", label: "ODI" }, { value: "TEST", label: "Test" }], section: "scoring" }
      ],
      aiMetadata: {
        scoringStyle: "match_format_dependent",
        lineupNotes: "T20 vs Test scoring differs materially \u2014 set format expectations in league rules."
      }
    };
  }
});

// lib/sportConfig/configs/golf.ts
var GOLF_CONFIG;
var init_golf = __esm({
  "lib/sportConfig/configs/golf.ts"() {
    "use strict";
    GOLF_CONFIG = {
      sport: "GOLF",
      displayName: "Golf",
      slug: "golf",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "strokes_vs_par", label: "Strokes vs Par (cumulative)", defaultPoints: -1, isToggleable: false, group: "scoring", sport: "GOLF" },
        { key: "top_10", label: "Top-10 Finish", defaultPoints: 4, isToggleable: true, group: "placement", sport: "GOLF" },
        { key: "top_20", label: "Top-20 Finish", defaultPoints: 2, isToggleable: true, group: "placement", sport: "GOLF" },
        { key: "cut_made", label: "Cut Made", defaultPoints: 2, isToggleable: true, group: "placement", sport: "GOLF" },
        { key: "eagles", label: "Eagles", defaultPoints: 5, isToggleable: true, group: "scoring", sport: "GOLF" },
        { key: "birdies", label: "Birdies", defaultPoints: 1, isToggleable: true, group: "scoring", sport: "GOLF" }
      ],
      scoringPresets: [{ name: "Tournament", categories: [] }],
      defaultRosterSlots: [
        { key: "GOLFER", label: "Golfer", eligiblePositions: ["GOLFER"], defaultCount: 6, minCount: 4, maxCount: 8, isOptional: false }
      ],
      defaultBenchSlots: 2,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: { GOLFER: ["GOLFER"] },
      defaultSeasonWeeks: 30,
      defaultPlayoffStartWeek: 28,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [
        { key: "rosterSize", label: "Golfers rostered", type: "number", defaultValue: 6, min: 4, max: 10, section: "roster" }
      ],
      aiMetadata: {
        scoringStyle: "event_points",
        lineupNotes: "Tournament weeks \u2014 not classic weekly NFL-style matchups."
      }
    };
  }
});

// lib/sportConfig/configs/horse_racing.ts
var HORSE_RACING_CONFIG;
var init_horse_racing = __esm({
  "lib/sportConfig/configs/horse_racing.ts"() {
    "use strict";
    HORSE_RACING_CONFIG = {
      sport: "HORSE_RACING",
      displayName: "Horse Racing",
      slug: "horse-racing",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "win", label: "Win", defaultPoints: 50, isToggleable: false, group: "finish", sport: "HORSE_RACING" },
        { key: "place", label: "Place", defaultPoints: 25, isToggleable: true, group: "finish", sport: "HORSE_RACING" },
        { key: "show", label: "Show", defaultPoints: 10, isToggleable: true, group: "finish", sport: "HORSE_RACING" },
        { key: "exacta", label: "Exacta (bonus)", defaultPoints: 30, isToggleable: true, group: "exotic", sport: "HORSE_RACING" }
      ],
      scoringPresets: [{ name: "Triple Crown lite", categories: [] }],
      defaultRosterSlots: [
        { key: "HORSE", label: "Horse", eligiblePositions: ["HORSE"], defaultCount: 6, minCount: 3, maxCount: 10, isOptional: false }
      ],
      defaultBenchSlots: 0,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: { HORSE: ["HORSE"] },
      defaultSeasonWeeks: 40,
      defaultPlayoffStartWeek: 36,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [{ key: "scratchedPolicy", label: "Scratched horse policy", type: "select", defaultValue: "bench", options: [{ value: "bench", label: "Move to bench" }, { value: "zero", label: "Score zero" }], section: "scoring" }],
      aiMetadata: {
        scoringStyle: "race_card",
        lineupNotes: "Post-time scratches require deterministic commissioner rules."
      }
    };
  }
});

// lib/mlb-scoring/MlbScoringPresets.ts
var MLB_HITTING_STAT_KEYS, MLB_PITCHING_STAT_KEYS, MLB_STAT_KEYS, MLB_AVAILABLE_SCORING_KEYS, MLB_STAT_LABELS, AF_DEFAULT, SLEEPER_COMPATIBLE, ESPN_DEFAULT, YAHOO_DEFAULT, PRESET_REGISTRY;
var init_MlbScoringPresets = __esm({
  "lib/mlb-scoring/MlbScoringPresets.ts"() {
    "use strict";
    MLB_HITTING_STAT_KEYS = [
      "plate_appearances",
      "at_bats",
      "runs",
      "singles",
      "doubles",
      "triples",
      "home_runs",
      "total_bases",
      "rbis",
      "walks",
      "intentional_walks",
      "hit_by_pitch",
      "strikeouts",
      "stolen_bases",
      "caught_stealing",
      "sacrifice_flies",
      "sacrifice_bunts",
      "ground_into_double_play",
      "cycle_bonus",
      "grand_slam_bonus",
      "game_winning_rbi_bonus"
    ];
    MLB_PITCHING_STAT_KEYS = [
      "outs_recorded",
      "innings_pitched",
      "wins",
      "losses",
      "saves",
      "holds",
      "save_opportunities",
      "blown_saves",
      "pitch_strikeouts",
      "walks_allowed",
      "hits_allowed",
      "earned_runs",
      "runs_allowed",
      "home_runs_allowed",
      "hit_batters",
      "quality_starts",
      "complete_games",
      "complete_game_shutouts",
      "no_hitters",
      "perfect_games",
      "pickoffs",
      "balks",
      "wild_pitches"
    ];
    MLB_STAT_KEYS = [...MLB_HITTING_STAT_KEYS, ...MLB_PITCHING_STAT_KEYS];
    MLB_AVAILABLE_SCORING_KEYS = /* @__PURE__ */ new Set([
      "at_bats",
      "runs",
      "singles",
      "doubles",
      "triples",
      "home_runs",
      "total_bases",
      "rbis",
      "walks",
      "intentional_walks",
      "hit_by_pitch",
      "strikeouts",
      "stolen_bases",
      "caught_stealing",
      "cycle_bonus",
      "outs_recorded",
      "innings_pitched",
      "wins",
      "losses",
      "saves",
      "holds",
      "blown_saves",
      "pitch_strikeouts",
      "walks_allowed",
      "hits_allowed",
      "earned_runs",
      "runs_allowed",
      "home_runs_allowed",
      "hit_batters",
      "quality_starts",
      "balks",
      "wild_pitches"
    ]);
    MLB_STAT_LABELS = {
      // Hitting
      plate_appearances: "Plate Appearances",
      at_bats: "At Bats",
      runs: "Runs",
      singles: "Singles",
      doubles: "Doubles",
      triples: "Triples",
      home_runs: "Home Runs",
      total_bases: "Total Bases",
      rbis: "RBIs",
      walks: "Walks (BB)",
      intentional_walks: "Intentional Walks",
      hit_by_pitch: "Hit By Pitch",
      strikeouts: "Strikeouts (K)",
      stolen_bases: "Stolen Bases",
      caught_stealing: "Caught Stealing",
      sacrifice_flies: "Sacrifice Flies",
      sacrifice_bunts: "Sacrifice Bunts",
      ground_into_double_play: "Ground Into Double Play",
      cycle_bonus: "Hit For Cycle Bonus",
      grand_slam_bonus: "Grand Slam Bonus",
      game_winning_rbi_bonus: "Game-Winning RBI Bonus",
      // Pitching
      outs_recorded: "Outs Recorded",
      innings_pitched: "Innings Pitched",
      wins: "Wins",
      losses: "Losses",
      saves: "Saves",
      holds: "Holds",
      save_opportunities: "Save Opportunities",
      blown_saves: "Blown Saves",
      pitch_strikeouts: "Strikeouts (K)",
      walks_allowed: "Walks Allowed",
      hits_allowed: "Hits Allowed",
      earned_runs: "Earned Runs",
      runs_allowed: "Runs Allowed",
      home_runs_allowed: "Home Runs Allowed",
      hit_batters: "Hit Batters",
      quality_starts: "Quality Starts",
      complete_games: "Complete Games",
      complete_game_shutouts: "Complete Game Shutouts",
      no_hitters: "No-Hitters",
      perfect_games: "Perfect Games",
      pickoffs: "Pickoffs",
      balks: "Balks",
      wild_pitches: "Wild Pitches"
    };
    AF_DEFAULT = {
      key: "af_default",
      label: "AllFantasy Default",
      source: "AF_DEFAULT",
      description: "Balanced MLB scoring optimized for AllFantasy league types including specialty formats.",
      rules: {
        // Hitting
        runs: 1,
        singles: 1,
        doubles: 2,
        triples: 3,
        home_runs: 4,
        rbis: 1,
        walks: 1,
        hit_by_pitch: 1,
        stolen_bases: 2,
        caught_stealing: -1,
        strikeouts: -1,
        cycle_bonus: 5,
        // Pitching
        innings_pitched: 3,
        pitch_strikeouts: 1,
        wins: 5,
        losses: -5,
        saves: 5,
        holds: 3,
        earned_runs: -2,
        hits_allowed: -1,
        walks_allowed: -1,
        hit_batters: -1,
        quality_starts: 3,
        blown_saves: -2
      }
    };
    SLEEPER_COMPATIBLE = {
      key: "sleeper_compatible",
      label: "Sleeper-Compatible",
      source: "PLATFORM_PRESET",
      description: "Sleeper-compatible MLB scoring. Note: Sleeper does not currently offer official fantasy baseball support. This is a compatibility baseline using AF stat structure.",
      warning: "Sleeper does not currently support fantasy baseball leagues. This preset uses a Sleeper-friendly stat structure but is not an official Sleeper preset. Specialty leagues are optimized for AllFantasy scoring.",
      rules: { ...AF_DEFAULT.rules }
    };
    ESPN_DEFAULT = {
      key: "espn_default",
      label: "ESPN Default",
      source: "PLATFORM_PRESET",
      description: "ESPN standard points-league scoring for MLB.",
      warning: "This scoring preset is based on ESPN's default format. Specialty leagues are optimized for AllFantasy scoring and may not score exactly as intended under this preset.",
      rules: {
        // Hitting
        total_bases: 1,
        runs: 1,
        rbis: 1,
        walks: 1,
        stolen_bases: 1,
        strikeouts: -1,
        // Pitching
        innings_pitched: 3,
        wins: 5,
        losses: -5,
        saves: 5,
        pitch_strikeouts: 1,
        earned_runs: -2,
        hits_allowed: -1,
        walks_allowed: -1
      }
    };
    YAHOO_DEFAULT = {
      key: "yahoo_default",
      label: "Yahoo Default",
      source: "PLATFORM_PRESET",
      description: "Yahoo-compatible head-to-head points baseline for MLB.",
      warning: "This scoring preset is based on Yahoo's default format. Specialty leagues are optimized for AllFantasy scoring and may not score exactly as intended under this preset.",
      rules: {
        // Hitting
        runs: 1,
        singles: 1,
        doubles: 2,
        triples: 3,
        home_runs: 4,
        rbis: 1,
        walks: 1,
        hit_by_pitch: 1,
        stolen_bases: 2,
        strikeouts: -0.5,
        // Pitching
        innings_pitched: 3,
        pitch_strikeouts: 1,
        wins: 5,
        saves: 5,
        holds: 2,
        earned_runs: -2,
        hits_allowed: -1,
        walks_allowed: -1,
        losses: -3,
        blown_saves: -1
      }
    };
    PRESET_REGISTRY = {
      af_default: AF_DEFAULT,
      sleeper_compatible: SLEEPER_COMPATIBLE,
      espn_default: ESPN_DEFAULT,
      yahoo_default: YAHOO_DEFAULT,
      custom: { key: "custom", label: "Custom", source: "CUSTOM", description: "Custom scoring values.", rules: { ...AF_DEFAULT.rules } }
    };
  }
});

// lib/mlb-scoring/scoringKeyBridge.ts
var MLB_UI_TO_ENGINE_KEYS;
var init_scoringKeyBridge = __esm({
  "lib/mlb-scoring/scoringKeyBridge.ts"() {
    "use strict";
    init_MlbScoringPresets();
    MLB_UI_TO_ENGINE_KEYS = Object.fromEntries(
      MLB_STAT_KEYS.map((key) => [key, {
        runs: "r",
        home_runs: "hr",
        rbis: "rbi",
        stolen_bases: "sb",
        total_bases: "tb",
        walks: "bb",
        innings_pitched: "ip",
        wins: "w",
        losses: "l",
        saves: "sv",
        pitch_strikeouts: "so",
        earned_runs: "er",
        quality_starts: "qs"
      }[key] ?? key])
    );
  }
});

// lib/sportConfig/configs/mlb.ts
var MLB_CONFIG;
var init_mlb = __esm({
  "lib/sportConfig/configs/mlb.ts"() {
    "use strict";
    init_MlbScoringPresets();
    init_scoringKeyBridge();
    MLB_CONFIG = {
      sport: "MLB",
      displayName: "MLB Baseball",
      slug: "mlb",
      defaultScoringSystem: "categories",
      lineupFrequency: "daily",
      hasBye: false,
      scoringCategories: [
        { key: "r", label: "Runs", defaultPoints: 1, isToggleable: false, group: "hitting", sport: "MLB" },
        { key: "hr", label: "Home Runs", defaultPoints: 4, isToggleable: false, group: "hitting", sport: "MLB" },
        { key: "rbi", label: "RBI", defaultPoints: 1, isToggleable: false, group: "hitting", sport: "MLB" },
        { key: "sb", label: "Stolen Bases", defaultPoints: 2, isToggleable: true, group: "hitting", sport: "MLB" },
        { key: "avg", label: "Batting Average", defaultPoints: 0, isToggleable: true, group: "hitting", sport: "MLB" },
        { key: "tb", label: "Total Bases", defaultPoints: 0.5, isToggleable: true, group: "hitting", sport: "MLB" },
        { key: "bb", label: "Walks (Batter)", defaultPoints: 0.5, isToggleable: true, group: "hitting", sport: "MLB" },
        { key: "w", label: "Win", defaultPoints: 5, isToggleable: false, group: "pitching", sport: "MLB" },
        { key: "sv", label: "Save", defaultPoints: 5, isToggleable: false, group: "pitching", sport: "MLB" },
        { key: "so", label: "Strikeouts", defaultPoints: 1, isToggleable: false, group: "pitching", sport: "MLB" },
        { key: "era", label: "ERA (category)", defaultPoints: -1, isToggleable: true, group: "pitching", sport: "MLB" },
        { key: "ip", label: "Innings Pitched", defaultPoints: 1, isToggleable: true, group: "pitching", sport: "MLB", unit: "per_inning" },
        { key: "qs", label: "Quality Start", defaultPoints: 4, isToggleable: true, group: "pitching", sport: "MLB" },
        { key: "l", label: "Loss", defaultPoints: -5, isToggleable: true, group: "pitching", sport: "MLB" },
        { key: "er", label: "Earned Runs", defaultPoints: -1, isToggleable: true, group: "pitching", sport: "MLB" },
        // Extra panel stats default to zero until a commissioner/preset assigns points.
        ...Object.entries(MLB_UI_TO_ENGINE_KEYS).filter(([, key]) => !["r", "hr", "rbi", "sb", "tb", "bb", "w", "sv", "so", "ip", "qs", "l", "er"].includes(key)).map(([uiKey, key]) => ({
          key,
          label: MLB_STAT_LABELS[uiKey] ?? uiKey,
          defaultPoints: 0,
          isToggleable: true,
          group: MLB_PITCHING_STAT_KEYS.includes(uiKey) ? "pitching" : "hitting",
          sport: "MLB"
        })),
        { key: "hits", label: "Hits", defaultPoints: 0, isToggleable: true, group: "hitting", sport: "MLB" }
      ],
      scoringPresets: [{ name: "Standard", categories: [] }],
      defaultRosterSlots: [
        { key: "C", label: "Catcher", eligiblePositions: ["C"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "1B", label: "First Base", eligiblePositions: ["1B"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "2B", label: "Second Base", eligiblePositions: ["2B"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "3B", label: "Third Base", eligiblePositions: ["3B"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "SS", label: "Shortstop", eligiblePositions: ["SS"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "OF", label: "Outfield", eligiblePositions: ["OF", "LF", "CF", "RF"], defaultCount: 3, minCount: 2, maxCount: 5, isOptional: false },
        { key: "UTIL", label: "Utility", eligiblePositions: ["1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF", "DH", "C"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: true },
        { key: "SP", label: "Starting Pitcher", eligiblePositions: ["SP", "P"], defaultCount: 4, minCount: 2, maxCount: 7, isOptional: false },
        { key: "RP", label: "Relief Pitcher", eligiblePositions: ["RP", "P"], defaultCount: 2, minCount: 0, maxCount: 5, isOptional: true },
        { key: "P", label: "Pitcher (SP/RP)", eligiblePositions: ["SP", "RP", "P"], defaultCount: 0, minCount: 0, maxCount: 4, isOptional: true }
      ],
      defaultBenchSlots: 5,
      defaultIRSlots: 2,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        C: ["C", "UTIL"],
        "1B": ["1B", "UTIL"],
        "2B": ["2B", "UTIL"],
        "3B": ["3B", "UTIL"],
        SS: ["SS", "UTIL"],
        OF: ["OF", "UTIL"],
        LF: ["OF", "UTIL"],
        CF: ["OF", "UTIL"],
        RF: ["OF", "UTIL"],
        SP: ["SP", "P"],
        RP: ["RP", "P"]
      },
      defaultSeasonWeeks: 23,
      defaultPlayoffStartWeek: 22,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "daily",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: true,
      supportsDailyLineups: true,
      commissionerSettings: [
        {
          key: "scoringMode",
          label: "Scoring Mode",
          type: "select",
          defaultValue: "categories",
          options: [
            { value: "categories", label: "Category League" },
            { value: "points", label: "Points League" }
          ],
          section: "scoring",
          locksAfterStart: true
        },
        { key: "ilSlots", label: "IL Slots", type: "number", defaultValue: 2, min: 0, max: 5, section: "roster" },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 5, min: 2, max: 10, section: "roster" },
        { key: "spCount", label: "SP Slots", type: "number", defaultValue: 4, min: 2, max: 7, section: "roster" },
        { key: "rpCount", label: "RP Slots", type: "number", defaultValue: 2, min: 0, max: 5, section: "roster" },
        {
          key: "seasonWeeks",
          label: "Regular Season Weeks",
          type: "number",
          defaultValue: 21,
          min: 15,
          max: 25,
          section: "schedule",
          locksAfterStart: true
        }
      ],
      aiMetadata: {
        scoringStyle: "categories",
        keyPositions: ["SP", "1B", "SS", "OF"],
        lineupNotes: "Daily lineups. Rain delays and postponements affect scoring windows.",
        pitcherNotes: "Streaming SPs is a core strategy. WHIP/ERA categories matter."
      }
    };
  }
});

// lib/sportConfig/configs/nascar.ts
var NASCAR_CONFIG;
var init_nascar = __esm({
  "lib/sportConfig/configs/nascar.ts"() {
    "use strict";
    NASCAR_CONFIG = {
      sport: "NASCAR",
      displayName: "NASCAR",
      slug: "nascar",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "finish_pos_pts", label: "Finish position points", defaultPoints: 1, isToggleable: false, group: "race", sport: "NASCAR" },
        { key: "stage_pts", label: "Stage points", defaultPoints: 1, isToggleable: true, group: "race", sport: "NASCAR" },
        { key: "laps_led", label: "Laps led", defaultPoints: 0.25, isToggleable: true, group: "race", sport: "NASCAR", unit: "per_yard" },
        { key: "top_10", label: "Top-10 bonus", defaultPoints: 2, isToggleable: true, group: "race", sport: "NASCAR" },
        { key: "pole", label: "Pole position", defaultPoints: 3, isToggleable: true, group: "qualifying", sport: "NASCAR" }
      ],
      scoringPresets: [{ name: "Cup Series", categories: [] }],
      defaultRosterSlots: [
        { key: "DRIVER", label: "Driver", eligiblePositions: ["DRIVER"], defaultCount: 5, minCount: 3, maxCount: 8, isOptional: false }
      ],
      defaultBenchSlots: 2,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: { DRIVER: ["DRIVER"] },
      defaultSeasonWeeks: 36,
      defaultPlayoffStartWeek: 28,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [{ key: "driverCount", label: "Active drivers", type: "number", defaultValue: 5, min: 3, max: 8, section: "roster" }],
      aiMetadata: {
        scoringStyle: "race_points",
        lineupNotes: "Race calendar \u2014 playoffs reset formats in real NASCAR."
      }
    };
  }
});

// lib/sportConfig/configs/nba.ts
var NBA_CONFIG;
var init_nba = __esm({
  "lib/sportConfig/configs/nba.ts"() {
    "use strict";
    NBA_CONFIG = {
      sport: "NBA",
      displayName: "NBA Basketball",
      slug: "nba",
      defaultScoringSystem: "categories",
      lineupFrequency: "daily",
      hasBye: false,
      scoringCategories: [
        { key: "pts", label: "Points", defaultPoints: 1, isToggleable: false, group: "offense", sport: "NBA" },
        { key: "reb", label: "Rebounds", defaultPoints: 1.2, isToggleable: false, group: "boards", sport: "NBA" },
        { key: "ast", label: "Assists", defaultPoints: 1.5, isToggleable: false, group: "playmaking", sport: "NBA" },
        { key: "stl", label: "Steals", defaultPoints: 3, isToggleable: true, group: "defense", sport: "NBA" },
        { key: "blk", label: "Blocks", defaultPoints: 3, isToggleable: true, group: "defense", sport: "NBA" },
        { key: "to", label: "Turnovers", defaultPoints: -1, isToggleable: true, group: "negative", sport: "NBA" },
        { key: "threes", label: "3-Pointers Made", defaultPoints: 0.5, isToggleable: true, group: "offense", sport: "NBA" },
        { key: "fgm", label: "FG Made", defaultPoints: 0, isToggleable: true, group: "shooting", sport: "NBA" },
        { key: "ftm", label: "FT Made", defaultPoints: 0, isToggleable: true, group: "shooting", sport: "NBA" },
        { key: "fga", label: "FG Attempted", defaultPoints: 0, isToggleable: true, group: "shooting", sport: "NBA" },
        { key: "fta", label: "FT Attempted", defaultPoints: 0, isToggleable: true, group: "shooting", sport: "NBA" },
        { key: "dbl_dbl", label: "Double-Double Bonus", defaultPoints: 1.5, isToggleable: true, group: "bonus", sport: "NBA", unit: "bonus" },
        { key: "trpl_dbl", label: "Triple-Double Bonus", defaultPoints: 3, isToggleable: true, group: "bonus", sport: "NBA", unit: "bonus" }
      ],
      scoringPresets: [
        { name: "Standard Points", categories: [] },
        { name: "Category League", categories: [] }
      ],
      defaultRosterSlots: [
        { key: "PG", label: "Point Guard", eligiblePositions: ["PG"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "SG", label: "Shooting Guard", eligiblePositions: ["SG"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "SF", label: "Small Forward", eligiblePositions: ["SF"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "PF", label: "Power Forward", eligiblePositions: ["PF"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "C", label: "Center", eligiblePositions: ["C"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "G", label: "Guard", eligiblePositions: ["PG", "SG"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: true },
        { key: "F", label: "Forward", eligiblePositions: ["SF", "PF"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: true },
        { key: "UTIL", label: "Utility", eligiblePositions: ["PG", "SG", "SF", "PF", "C"], defaultCount: 1, minCount: 0, maxCount: 3, isOptional: true }
      ],
      defaultBenchSlots: 4,
      defaultIRSlots: 2,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        PG: ["PG", "G", "UTIL"],
        SG: ["SG", "G", "UTIL"],
        SF: ["SF", "F", "UTIL"],
        PF: ["PF", "F", "UTIL"],
        C: ["C", "UTIL"]
      },
      defaultSeasonWeeks: 22,
      defaultPlayoffStartWeek: 20,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "daily",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: false,
      supportsC2C: true,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: true,
      supportsDailyLineups: true,
      commissionerSettings: [
        {
          key: "scoringMode",
          label: "Scoring Mode",
          type: "select",
          defaultValue: "points",
          options: [
            { value: "points", label: "Points League" },
            { value: "categories", label: "Category League (H2H)" }
          ],
          section: "scoring",
          locksAfterStart: true
        },
        { key: "enableDailyLineups", label: "Daily Lineup Changes", type: "toggle", defaultValue: true, section: "roster" },
        { key: "pgCount", label: "PG Slots", type: "number", defaultValue: 1, min: 1, max: 2, section: "roster" },
        { key: "sgCount", label: "SG Slots", type: "number", defaultValue: 1, min: 1, max: 2, section: "roster" },
        { key: "sfCount", label: "SF Slots", type: "number", defaultValue: 1, min: 1, max: 2, section: "roster" },
        { key: "pfCount", label: "PF Slots", type: "number", defaultValue: 1, min: 1, max: 2, section: "roster" },
        { key: "cCount", label: "C Slots", type: "number", defaultValue: 1, min: 1, max: 2, section: "roster" },
        { key: "utilCount", label: "Utility Slots", type: "number", defaultValue: 1, min: 0, max: 3, section: "roster" },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 4, min: 2, max: 10, section: "roster" },
        { key: "irSlots", label: "IR Slots", type: "number", defaultValue: 2, min: 0, max: 5, section: "roster" },
        {
          key: "seasonWeeks",
          label: "Regular Season Weeks",
          type: "number",
          defaultValue: 19,
          min: 12,
          max: 24,
          section: "schedule",
          locksAfterStart: true
        },
        {
          key: "playoffTeams",
          label: "Playoff Teams",
          type: "select",
          defaultValue: 4,
          options: [
            { value: 4, label: "4 teams" },
            { value: 6, label: "6 teams" }
          ],
          section: "schedule"
        }
      ],
      aiMetadata: {
        scoringStyle: "categories_or_points",
        keyPositions: ["PG", "SG", "SF", "PF", "C"],
        highValuePositions: ["C", "PG"],
        lineupNotes: "Daily lineup changes \u2014 games-played tracking matters",
        tradeNotes: "Category league trades require position balance analysis"
      }
    };
  }
});

// lib/sportConfig/configs/ncaaf.ts
var NCAAF_CONFIG;
var init_ncaaf = __esm({
  "lib/sportConfig/configs/ncaaf.ts"() {
    "use strict";
    NCAAF_CONFIG = {
      sport: "NCAAF",
      displayName: "NCAA Football",
      slug: "ncaaf",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "pass_yds", label: "Pass Yds", defaultPoints: 0.04, isToggleable: false, group: "passing", sport: "NCAAF", unit: "per_yard" },
        { key: "pass_td", label: "Pass TD", defaultPoints: 4, isToggleable: false, group: "passing", sport: "NCAAF" },
        { key: "pass_int", label: "Interception", defaultPoints: -2, isToggleable: true, group: "passing", sport: "NCAAF" },
        { key: "rush_yds", label: "Rush Yds", defaultPoints: 0.1, isToggleable: false, group: "rushing", sport: "NCAAF", unit: "per_yard" },
        { key: "rush_td", label: "Rush TD", defaultPoints: 6, isToggleable: false, group: "rushing", sport: "NCAAF" },
        { key: "rec", label: "Reception", defaultPoints: 1, isToggleable: true, group: "receiving", sport: "NCAAF" },
        { key: "rec_yds", label: "Rec Yds", defaultPoints: 0.1, isToggleable: false, group: "receiving", sport: "NCAAF", unit: "per_yard" },
        { key: "rec_td", label: "Rec TD", defaultPoints: 6, isToggleable: false, group: "receiving", sport: "NCAAF" },
        { key: "two_pt", label: "2-Point Conversion", defaultPoints: 2, isToggleable: true, group: "special", sport: "NCAAF" },
        { key: "fum_lost", label: "Fumble Lost", defaultPoints: -2, isToggleable: true, group: "special", sport: "NCAAF" },
        { key: "def_td", label: "Defense/Special Teams TD", defaultPoints: 6, isToggleable: true, group: "defense", sport: "NCAAF" },
        { key: "def_int", label: "Defense Interception", defaultPoints: 2, isToggleable: true, group: "defense", sport: "NCAAF" },
        { key: "def_fr", label: "Defense Fumble Recovery", defaultPoints: 2, isToggleable: true, group: "defense", sport: "NCAAF" },
        { key: "def_sack", label: "Defense Sack", defaultPoints: 1, isToggleable: true, group: "defense", sport: "NCAAF" }
      ],
      scoringPresets: [
        { name: "College Half PPR", categories: [] },
        { name: "College PPR", categories: [] },
        { name: "College Standard", categories: [] }
      ],
      defaultRosterSlots: [
        { key: "QB", label: "Quarterback", eligiblePositions: ["QB"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "RB", label: "Running Back", eligiblePositions: ["RB"], defaultCount: 1, minCount: 1, maxCount: 4, isOptional: false },
        { key: "WR", label: "Wide Receiver", eligiblePositions: ["WR"], defaultCount: 2, minCount: 1, maxCount: 4, isOptional: false },
        { key: "TE", label: "Tight End", eligiblePositions: ["TE"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: false },
        { key: "FLX", label: "Flex", eligiblePositions: ["RB", "WR", "TE"], defaultCount: 0, minCount: 0, maxCount: 2, isOptional: true },
        { key: "SF", label: "Superflex", eligiblePositions: ["QB", "RB", "WR", "TE"], defaultCount: 0, minCount: 0, maxCount: 1, isOptional: true },
        { key: "DEF", label: "Team Defense", eligiblePositions: ["DEF", "DST"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "K", label: "Kicker", eligiblePositions: ["K"], defaultCount: 0, minCount: 0, maxCount: 1, isOptional: true }
      ],
      defaultBenchSlots: 8,
      defaultIRSlots: 1,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        QB: ["QB", "SF", "SUPERFLEX", "SUPER_FLEX"],
        RB: ["RB", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        WR: ["WR", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        TE: ["TE", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        K: ["K"],
        DEF: ["DEF"],
        DST: ["DEF"]
      },
      defaultSeasonWeeks: 13,
      defaultPlayoffStartWeek: 12,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_player_kickoff",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: true,
      supportsC2C: true,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: true,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [
        { key: "seasonWeeks", label: "Regular Season Weeks", type: "number", defaultValue: 12, min: 8, max: 14, section: "schedule", locksAfterStart: true },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 8, min: 3, max: 12, section: "roster" }
      ],
      aiMetadata: {
        scoringStyle: "points",
        keyPositions: ["QB", "RB", "WR"],
        lineupNotes: "Shorter season than NFL; playoff alignment with conference championships."
      }
    };
  }
});

// lib/sportConfig/configs/ncaab.ts
var NCAAB_CONFIG;
var init_ncaab = __esm({
  "lib/sportConfig/configs/ncaab.ts"() {
    "use strict";
    NCAAB_CONFIG = {
      sport: "NCAAB",
      displayName: "NCAA Basketball",
      slug: "ncaab",
      defaultScoringSystem: "points",
      lineupFrequency: "daily",
      hasBye: false,
      scoringCategories: [
        { key: "pts", label: "Points", defaultPoints: 1, isToggleable: false, group: "offense", sport: "NCAAB" },
        { key: "reb", label: "Rebounds", defaultPoints: 1.2, isToggleable: false, group: "boards", sport: "NCAAB" },
        { key: "ast", label: "Assists", defaultPoints: 1.5, isToggleable: false, group: "playmaking", sport: "NCAAB" },
        { key: "stl", label: "Steals", defaultPoints: 3, isToggleable: true, group: "defense", sport: "NCAAB" },
        { key: "blk", label: "Blocks", defaultPoints: 3, isToggleable: true, group: "defense", sport: "NCAAB" },
        { key: "to", label: "Turnovers", defaultPoints: -1, isToggleable: true, group: "negative", sport: "NCAAB" },
        { key: "threes", label: "3-Pointers Made", defaultPoints: 0.5, isToggleable: true, group: "offense", sport: "NCAAB" }
      ],
      scoringPresets: [{ name: "Standard", categories: [] }],
      defaultRosterSlots: [
        { key: "PG", label: "Point Guard", eligiblePositions: ["PG"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "SG", label: "Shooting Guard", eligiblePositions: ["SG"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "SF", label: "Small Forward", eligiblePositions: ["SF"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "PF", label: "Power Forward", eligiblePositions: ["PF"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "C", label: "Center", eligiblePositions: ["C"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "UTIL", label: "Utility", eligiblePositions: ["PG", "SG", "SF", "PF", "C"], defaultCount: 2, minCount: 0, maxCount: 3, isOptional: true }
      ],
      defaultBenchSlots: 3,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        PG: ["PG", "UTIL"],
        SG: ["SG", "UTIL"],
        SF: ["SF", "UTIL"],
        PF: ["PF", "UTIL"],
        C: ["C", "UTIL"]
      },
      defaultSeasonWeeks: 18,
      defaultPlayoffStartWeek: 16,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "daily",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: false,
      supportsC2C: true,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: true,
      supportsDailyLineups: true,
      commissionerSettings: [
        { key: "marchMadnessMode", label: "March Madness bracket tie-in", type: "toggle", defaultValue: false, section: "schedule" },
        { key: "seasonWeeks", label: "Regular Season Weeks", type: "number", defaultValue: 16, min: 12, max: 22, section: "schedule", locksAfterStart: true }
      ],
      aiMetadata: {
        scoringStyle: "points",
        keyPositions: ["PG", "C"],
        lineupNotes: "Conference tournaments and March Madness create schedule spikes."
      }
    };
  }
});

// lib/sportConfig/configs/nfl.ts
function nflPreset(name, recPoints) {
  return {
    name,
    categories: NFL_SCORING_BASE.map((c) => ({
      ...c,
      points: c.key === "rec" ? recPoints : c.defaultPoints
    }))
  };
}
var NFL_SCORING_BASE, NFL_CONFIG;
var init_nfl = __esm({
  "lib/sportConfig/configs/nfl.ts"() {
    "use strict";
    NFL_SCORING_BASE = [
      { key: "pass_yds", label: "Passing Yards", defaultPoints: 0.04, isToggleable: false, group: "passing", sport: "NFL", unit: "per_yard" },
      { key: "pass_td", label: "Passing TD", defaultPoints: 4, isToggleable: false, group: "passing", sport: "NFL" },
      { key: "pass_int", label: "Interception Thrown", defaultPoints: -2, isToggleable: true, group: "passing", sport: "NFL" },
      {
        key: "pass_300_bonus",
        label: "300+ Yd Passing Bonus",
        defaultPoints: 3,
        isToggleable: true,
        group: "passing",
        sport: "NFL",
        unit: "bonus",
        minForBonus: 300
      },
      {
        key: "pass_400_bonus",
        label: "400+ Yd Passing Bonus",
        defaultPoints: 3,
        isToggleable: true,
        group: "passing",
        sport: "NFL",
        unit: "bonus",
        minForBonus: 400
      },
      { key: "rush_yds", label: "Rushing Yards", defaultPoints: 0.1, isToggleable: false, group: "rushing", sport: "NFL", unit: "per_yard" },
      { key: "rush_td", label: "Rushing TD", defaultPoints: 6, isToggleable: false, group: "rushing", sport: "NFL" },
      {
        key: "rush_100_bonus",
        label: "100+ Yd Rushing Bonus",
        defaultPoints: 3,
        isToggleable: true,
        group: "rushing",
        sport: "NFL",
        unit: "bonus",
        minForBonus: 100
      },
      { key: "rec", label: "Reception", defaultPoints: 1, isToggleable: true, group: "receiving", sport: "NFL" },
      { key: "rec_yds", label: "Receiving Yards", defaultPoints: 0.1, isToggleable: false, group: "receiving", sport: "NFL", unit: "per_yard" },
      { key: "rec_td", label: "Receiving TD", defaultPoints: 6, isToggleable: false, group: "receiving", sport: "NFL" },
      {
        key: "rec_100_bonus",
        label: "100+ Yd Receiving Bonus",
        defaultPoints: 3,
        isToggleable: true,
        group: "receiving",
        sport: "NFL",
        unit: "bonus",
        minForBonus: 100
      },
      {
        key: "te_premium",
        label: "TE Premium (extra per reception)",
        defaultPoints: 0.5,
        isToggleable: true,
        group: "receiving",
        sport: "NFL",
        requiresToggle: "TE_PREMIUM"
      },
      { key: "two_pt", label: "2-Point Conversion", defaultPoints: 2, isToggleable: true, group: "special", sport: "NFL" },
      { key: "fum_lost", label: "Fumble Lost", defaultPoints: -2, isToggleable: true, group: "special", sport: "NFL" },
      { key: "fumble_td", label: "Fumble Recovery TD", defaultPoints: 6, isToggleable: true, group: "special", sport: "NFL" },
      { key: "fg_0_39", label: "FG Made 0-39 Yds", defaultPoints: 3, isToggleable: true, group: "kicking", sport: "NFL" },
      { key: "fg_40_49", label: "FG Made 40-49 Yds", defaultPoints: 4, isToggleable: true, group: "kicking", sport: "NFL" },
      { key: "fg_50_plus", label: "FG Made 50+ Yds", defaultPoints: 5, isToggleable: true, group: "kicking", sport: "NFL" },
      { key: "fg_miss", label: "FG Missed", defaultPoints: -1, isToggleable: true, group: "kicking", sport: "NFL" },
      { key: "xp_made", label: "Extra Point Made", defaultPoints: 1, isToggleable: true, group: "kicking", sport: "NFL" },
      { key: "idp_solo", label: "Solo Tackle", defaultPoints: 1, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_assist", label: "Assisted Tackle", defaultPoints: 0.5, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_sack", label: "Sack", defaultPoints: 2, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_int", label: "Interception", defaultPoints: 6, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_pd", label: "Pass Deflection", defaultPoints: 1, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_ff", label: "Forced Fumble", defaultPoints: 3, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_fr", label: "Fumble Recovery", defaultPoints: 2, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_td", label: "Defensive TD", defaultPoints: 6, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_safety", label: "Safety", defaultPoints: 2, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_tfl", label: "Tackle For Loss", defaultPoints: 1, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      { key: "idp_qb_hit", label: "QB Hit", defaultPoints: 0.5, isToggleable: true, group: "idp", sport: "NFL", requiresToggle: "IDP" },
      // ── Team Defense / Special Teams (DST) ───────────────────────────────────
      // Always active (most leagues start a DEF), commissioner-overridable. These
      // score off team-defense stat keys (def_*) that only a team-defense roster row
      // carries, so offensive players are unaffected. See gap G8 in
      // docs/redraft-commissioner-scoring-contract.md.
      { key: "def_sack", label: "DST Sack", defaultPoints: 1, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_int", label: "DST Interception", defaultPoints: 2, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_fr", label: "DST Fumble Recovery", defaultPoints: 2, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_safety", label: "DST Safety", defaultPoints: 2, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_blk_kick", label: "DST Blocked Kick", defaultPoints: 2, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_td", label: "DST Defensive TD", defaultPoints: 6, isToggleable: true, group: "team_def", sport: "NFL" },
      { key: "def_st_td", label: "DST Special-Teams / Return TD", defaultPoints: 6, isToggleable: true, group: "team_def", sport: "NFL" },
      // Return yardage (G9) — 0 by default; commissioner enables a per-yard value.
      { key: "def_kr_yd", label: "DST Kick Return Yards", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "per_yard" },
      { key: "def_pr_yd", label: "DST Punt Return Yards", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "per_yard" },
      // Points-allowed tiers — the team-defense row carries a single
      // `def_points_allowed` value; exactly one tier matches it.
      { key: "def_pa_0", label: "DST Points Allowed: 0", defaultPoints: 10, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 0, tierMax: 0 },
      { key: "def_pa_1_6", label: "DST Points Allowed: 1\u20136", defaultPoints: 7, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 1, tierMax: 6 },
      { key: "def_pa_7_13", label: "DST Points Allowed: 7\u201313", defaultPoints: 4, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 7, tierMax: 13 },
      { key: "def_pa_14_20", label: "DST Points Allowed: 14\u201320", defaultPoints: 1, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 14, tierMax: 20 },
      { key: "def_pa_21_27", label: "DST Points Allowed: 21\u201327", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 21, tierMax: 27 },
      { key: "def_pa_28_34", label: "DST Points Allowed: 28\u201334", defaultPoints: -1, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 28, tierMax: 34 },
      { key: "def_pa_35_plus", label: "DST Points Allowed: 35+", defaultPoints: -4, isToggleable: true, group: "team_def", sport: "NFL", unit: "pa_tier", tierStatKey: "def_points_allowed", tierMin: 35 },
      // Yards-allowed tiers — optional (off by default: 0 pts), commissioner can
      // value them. Inert until a provider supplies `def_yds_allowed` (see G8 doc).
      { key: "def_ya_0_99", label: "DST Yards Allowed: <100", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 0, tierMax: 99 },
      { key: "def_ya_100_199", label: "DST Yards Allowed: 100\u2013199", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 100, tierMax: 199 },
      { key: "def_ya_200_299", label: "DST Yards Allowed: 200\u2013299", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 200, tierMax: 299 },
      { key: "def_ya_300_349", label: "DST Yards Allowed: 300\u2013349", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 300, tierMax: 349 },
      { key: "def_ya_350_399", label: "DST Yards Allowed: 350\u2013399", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 350, tierMax: 399 },
      { key: "def_ya_400_449", label: "DST Yards Allowed: 400\u2013449", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 400, tierMax: 449 },
      { key: "def_ya_450_plus", label: "DST Yards Allowed: 450+", defaultPoints: 0, isToggleable: true, group: "team_def", sport: "NFL", unit: "ya_tier", tierStatKey: "def_yds_allowed", tierMin: 450 }
    ];
    NFL_CONFIG = {
      sport: "NFL",
      displayName: "NFL Football",
      slug: "nfl",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: true,
      scoringCategories: NFL_SCORING_BASE,
      scoringPresets: [nflPreset("Half PPR", 0.5), nflPreset("PPR", 1), nflPreset("Standard", 0)],
      defaultRosterSlots: [
        { key: "QB", label: "Quarterback", eligiblePositions: ["QB"], defaultCount: 1, minCount: 1, maxCount: 3, isOptional: false },
        { key: "RB", label: "Running Back", eligiblePositions: ["RB"], defaultCount: 1, minCount: 1, maxCount: 4, isOptional: false },
        { key: "WR", label: "Wide Receiver", eligiblePositions: ["WR"], defaultCount: 2, minCount: 1, maxCount: 5, isOptional: false },
        { key: "TE", label: "Tight End", eligiblePositions: ["TE"], defaultCount: 1, minCount: 0, maxCount: 3, isOptional: false },
        { key: "FLX", label: "Flex (RB/WR/TE)", eligiblePositions: ["RB", "WR", "TE"], defaultCount: 0, minCount: 0, maxCount: 3, isOptional: true },
        {
          key: "SF",
          label: "Superflex (QB/RB/WR/TE)",
          eligiblePositions: ["QB", "RB", "WR", "TE"],
          defaultCount: 0,
          minCount: 0,
          maxCount: 2,
          isOptional: true,
          requiresToggle: "SUPERFLEX"
        },
        { key: "DEF", label: "Team Defense", eligiblePositions: ["DEF", "DST"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "K", label: "Kicker", eligiblePositions: ["K"], defaultCount: 0, minCount: 0, maxCount: 2, isOptional: true },
        {
          key: "IDP_DL",
          label: "Defensive Line",
          eligiblePositions: ["DE", "DT", "DL"],
          defaultCount: 2,
          minCount: 0,
          maxCount: 4,
          isOptional: true,
          requiresToggle: "IDP"
        },
        {
          key: "IDP_LB",
          label: "Linebacker",
          eligiblePositions: ["LB", "ILB", "OLB"],
          defaultCount: 2,
          minCount: 0,
          maxCount: 4,
          isOptional: true,
          requiresToggle: "IDP"
        },
        {
          key: "IDP_DB",
          label: "Defensive Back",
          eligiblePositions: ["CB", "S", "DB"],
          defaultCount: 2,
          minCount: 0,
          maxCount: 4,
          isOptional: true,
          requiresToggle: "IDP"
        },
        {
          key: "IDP_FLEX",
          label: "IDP Flex",
          eligiblePositions: ["DE", "DT", "DL", "LB", "ILB", "OLB", "CB", "S", "DB"],
          defaultCount: 1,
          minCount: 0,
          maxCount: 3,
          isOptional: true,
          requiresToggle: "IDP"
        }
      ],
      defaultBenchSlots: 6,
      defaultIRSlots: 1,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        QB: ["QB", "SF", "SUPERFLEX", "SUPER_FLEX"],
        RB: ["RB", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        WR: ["WR", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        TE: ["TE", "FLX", "FLEX", "SF", "SUPERFLEX", "SUPER_FLEX"],
        K: ["K"],
        DEF: ["DEF"],
        DST: ["DEF"],
        DE: ["IDP_DL", "IDP_FLEX"],
        DT: ["IDP_DL", "IDP_FLEX"],
        DL: ["IDP_DL", "IDP_FLEX"],
        LB: ["IDP_LB", "IDP_FLEX"],
        ILB: ["IDP_LB", "IDP_FLEX"],
        OLB: ["IDP_LB", "IDP_FLEX"],
        CB: ["IDP_DB", "IDP_FLEX"],
        S: ["IDP_DB", "IDP_FLEX"],
        DB: ["IDP_DB", "IDP_FLEX"]
      },
      defaultSeasonWeeks: 17,
      defaultPlayoffStartWeek: 15,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_player_kickoff",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: true,
      supportsC2C: true,
      supportsIDP: true,
      supportsSuperflex: true,
      supportsTEPremium: true,
      supportsPPR: true,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [
        {
          key: "scoringPreset",
          label: "Scoring Preset",
          type: "select",
          defaultValue: "PPR",
          options: [
            { value: "PPR", label: "PPR (1 pt per reception)" },
            { value: "HALF_PPR", label: "Half PPR (0.5 pts)" },
            { value: "STANDARD", label: "Standard (no reception pts)" },
            { value: "CUSTOM", label: "Custom" }
          ],
          section: "scoring",
          locksAfterStart: false
        },
        {
          key: "enableIDP",
          label: "Enable IDP",
          type: "toggle",
          defaultValue: false,
          section: "scoring",
          description: "Add individual defensive player scoring and roster slots"
        },
        { key: "enableSuperflex", label: "Enable Superflex", type: "toggle", defaultValue: false, section: "roster" },
        {
          key: "enableTEPremium",
          label: "Enable TE Premium",
          type: "toggle",
          defaultValue: false,
          section: "scoring",
          description: "Extra 0.5 pts per reception for Tight Ends"
        },
        { key: "qbCount", label: "QB Slots", type: "number", defaultValue: 1, min: 1, max: 3, section: "roster" },
        { key: "rbCount", label: "RB Slots", type: "number", defaultValue: 1, min: 1, max: 4, section: "roster" },
        { key: "wrCount", label: "WR Slots", type: "number", defaultValue: 2, min: 1, max: 5, section: "roster" },
        { key: "teCount", label: "TE Slots", type: "number", defaultValue: 1, min: 0, max: 3, section: "roster" },
        { key: "flexCount", label: "FLX Slots", type: "number", defaultValue: 0, min: 0, max: 3, section: "roster" },
        { key: "kCount", label: "K Slots", type: "number", defaultValue: 0, min: 0, max: 2, section: "roster" },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 6, min: 3, max: 15, section: "roster" },
        { key: "irSlots", label: "IR Slots", type: "number", defaultValue: 1, min: 0, max: 5, section: "roster" },
        {
          key: "idpDLCount",
          label: "DL Slots",
          type: "number",
          defaultValue: 2,
          min: 0,
          max: 4,
          section: "roster",
          requiresToggle: "enableIDP"
        },
        {
          key: "idpLBCount",
          label: "LB Slots",
          type: "number",
          defaultValue: 2,
          min: 0,
          max: 4,
          section: "roster",
          requiresToggle: "enableIDP"
        },
        {
          key: "idpDBCount",
          label: "DB Slots",
          type: "number",
          defaultValue: 2,
          min: 0,
          max: 4,
          section: "roster",
          requiresToggle: "enableIDP"
        },
        {
          key: "idpFlexCount",
          label: "IDP Flex Slots",
          type: "number",
          defaultValue: 1,
          min: 0,
          max: 3,
          section: "roster",
          requiresToggle: "enableIDP"
        },
        {
          key: "seasonWeeks",
          label: "Regular Season Weeks",
          type: "number",
          defaultValue: 14,
          min: 8,
          max: 17,
          section: "schedule",
          locksAfterStart: true
        },
        {
          key: "playoffStartWeek",
          label: "Playoff Start Week",
          type: "number",
          defaultValue: 15,
          min: 13,
          max: 17,
          section: "schedule"
        },
        {
          key: "playoffTeams",
          label: "Playoff Teams",
          type: "select",
          defaultValue: 4,
          options: [
            { value: 4, label: "4 teams" },
            { value: 6, label: "6 teams" },
            { value: 8, label: "8 teams" }
          ],
          section: "schedule"
        }
      ],
      aiMetadata: {
        scoringStyle: "points",
        keyPositions: ["QB", "RB", "WR", "TE"],
        highValuePositions: ["QB", "WR1"],
        scarcityPositions: ["TE", "QB_SF"],
        lineupNotes: "Players lock at individual game kickoff",
        playoffNote: "Playoffs typically weeks 15-17 in NFL",
        byeWeekImpact: "significant"
      }
    };
  }
});

// lib/sportConfig/configs/nhl.ts
var NHL_CONFIG;
var init_nhl = __esm({
  "lib/sportConfig/configs/nhl.ts"() {
    "use strict";
    NHL_CONFIG = {
      sport: "NHL",
      displayName: "NHL Hockey",
      slug: "nhl",
      defaultScoringSystem: "points",
      lineupFrequency: "daily",
      hasBye: false,
      scoringCategories: [
        { key: "g", label: "Goal", defaultPoints: 3, isToggleable: false, group: "offense", sport: "NHL" },
        { key: "a", label: "Assist", defaultPoints: 2, isToggleable: false, group: "offense", sport: "NHL" },
        { key: "plusminus", label: "+/-", defaultPoints: 0.5, isToggleable: true, group: "defense", sport: "NHL" },
        { key: "sog", label: "Shots on Goal", defaultPoints: 0.3, isToggleable: true, group: "offense", sport: "NHL" },
        { key: "ppp", label: "Power Play Point", defaultPoints: 0.5, isToggleable: true, group: "special_teams", sport: "NHL" },
        { key: "shp", label: "Short-Handed Point", defaultPoints: 1, isToggleable: true, group: "special_teams", sport: "NHL" },
        { key: "blks", label: "Blocked Shots", defaultPoints: 0.3, isToggleable: true, group: "defense", sport: "NHL" },
        { key: "hits", label: "Hits", defaultPoints: 0.2, isToggleable: true, group: "defense", sport: "NHL" },
        { key: "pim", label: "PIM", defaultPoints: -0.1, isToggleable: true, group: "negative", sport: "NHL" },
        { key: "g_win", label: "Win (G)", defaultPoints: 5, isToggleable: false, group: "goalie", sport: "NHL" },
        { key: "g_sv", label: "Save (G)", defaultPoints: 0.2, isToggleable: false, group: "goalie", sport: "NHL" },
        { key: "g_so", label: "Shutout (G)", defaultPoints: 5, isToggleable: true, group: "goalie", sport: "NHL" },
        { key: "g_ga", label: "Goal Against (G)", defaultPoints: -1, isToggleable: true, group: "goalie", sport: "NHL" },
        { key: "g_sa", label: "Shots Against (G)", defaultPoints: 0, isToggleable: true, group: "goalie", sport: "NHL" },
        { key: "g_seconds", label: "Time Played (G, seconds)", defaultPoints: 0, isToggleable: true, group: "goalie", sport: "NHL" }
      ],
      scoringPresets: [{ name: "Standard", categories: [] }],
      defaultRosterSlots: [
        { key: "C", label: "Center", eligiblePositions: ["C"], defaultCount: 2, minCount: 1, maxCount: 4, isOptional: false },
        { key: "LW", label: "Left Wing", eligiblePositions: ["LW"], defaultCount: 2, minCount: 1, maxCount: 4, isOptional: false },
        { key: "RW", label: "Right Wing", eligiblePositions: ["RW"], defaultCount: 2, minCount: 1, maxCount: 4, isOptional: false },
        { key: "D", label: "Defenseman", eligiblePositions: ["D"], defaultCount: 4, minCount: 2, maxCount: 6, isOptional: false },
        { key: "G", label: "Goalie", eligiblePositions: ["G"], defaultCount: 1, minCount: 1, maxCount: 3, isOptional: false },
        { key: "UTIL", label: "Utility (F/D)", eligiblePositions: ["C", "LW", "RW", "D"], defaultCount: 1, minCount: 0, maxCount: 2, isOptional: true }
      ],
      defaultBenchSlots: 5,
      defaultIRSlots: 2,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        C: ["C", "UTIL"],
        LW: ["LW", "UTIL"],
        RW: ["RW", "UTIL"],
        D: ["D", "UTIL"],
        G: ["G"]
      },
      defaultSeasonWeeks: 22,
      defaultPlayoffStartWeek: 20,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "daily",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: true,
      supportsDailyLineups: true,
      commissionerSettings: [
        { key: "cCount", label: "C Slots", type: "number", defaultValue: 2, min: 1, max: 4, section: "roster" },
        { key: "dCount", label: "D Slots", type: "number", defaultValue: 4, min: 2, max: 6, section: "roster" },
        { key: "gCount", label: "G Slots", type: "number", defaultValue: 1, min: 1, max: 3, section: "roster" },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 5, min: 2, max: 10, section: "roster" },
        {
          key: "seasonWeeks",
          label: "Regular Season Weeks",
          type: "number",
          defaultValue: 20,
          min: 14,
          max: 24,
          section: "schedule",
          locksAfterStart: true
        },
        {
          key: "playoffTeams",
          label: "Playoff Teams",
          type: "select",
          defaultValue: 4,
          options: [
            { value: 4, label: "4 teams" },
            { value: 6, label: "6 teams" }
          ],
          section: "schedule"
        }
      ],
      aiMetadata: {
        scoringStyle: "points_or_categories",
        keyPositions: ["LW", "C", "RW", "D", "G"],
        lineupNotes: "Daily lineups. Back-to-back game pairs often use backup goalies."
      }
    };
  }
});

// lib/sportConfig/configs/soccer.ts
var SOCCER_CONFIG;
var init_soccer = __esm({
  "lib/sportConfig/configs/soccer.ts"() {
    "use strict";
    SOCCER_CONFIG = {
      sport: "SOCCER",
      displayName: "Soccer",
      slug: "soccer",
      defaultScoringSystem: "points",
      lineupFrequency: "per_match",
      hasBye: false,
      scoringCategories: [
        { key: "goals", label: "Goals", defaultPoints: 6, isToggleable: false, group: "attack", sport: "SOCCER" },
        { key: "assists", label: "Assists", defaultPoints: 3, isToggleable: false, group: "attack", sport: "SOCCER" },
        { key: "clean_sheet_def", label: "Clean Sheet (DEF)", defaultPoints: 4, isToggleable: true, group: "defense", sport: "SOCCER" },
        { key: "clean_sheet_gk", label: "Clean Sheet (GK)", defaultPoints: 4, isToggleable: true, group: "goalie", sport: "SOCCER" },
        { key: "saves", label: "Saves", defaultPoints: 0.5, isToggleable: true, group: "goalie", sport: "SOCCER" },
        { key: "yellow_card", label: "Yellow Card", defaultPoints: -1, isToggleable: true, group: "discipline", sport: "SOCCER" },
        { key: "red_card", label: "Red Card", defaultPoints: -3, isToggleable: true, group: "discipline", sport: "SOCCER" },
        { key: "own_goal", label: "Own Goal", defaultPoints: -2, isToggleable: true, group: "negative", sport: "SOCCER" },
        { key: "pen_miss", label: "Penalty Miss", defaultPoints: -2, isToggleable: true, group: "negative", sport: "SOCCER" },
        { key: "pen_save", label: "Penalty Save (GK)", defaultPoints: 5, isToggleable: true, group: "goalie", sport: "SOCCER" }
      ],
      scoringPresets: [{ name: "FPL-style", categories: [] }],
      defaultRosterSlots: [
        { key: "GK", label: "Goalkeeper", eligiblePositions: ["GK"], defaultCount: 1, minCount: 1, maxCount: 2, isOptional: false },
        { key: "DEF", label: "Defenders", eligiblePositions: ["DEF", "D"], defaultCount: 4, minCount: 3, maxCount: 5, isOptional: false },
        { key: "MID", label: "Midfielders", eligiblePositions: ["MID", "M"], defaultCount: 4, minCount: 3, maxCount: 5, isOptional: false },
        { key: "FWD", label: "Forwards", eligiblePositions: ["FWD", "F"], defaultCount: 2, minCount: 1, maxCount: 3, isOptional: false }
      ],
      defaultBenchSlots: 4,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: {
        GK: ["GK"],
        DEF: ["DEF"],
        D: ["DEF"],
        MID: ["MID"],
        M: ["MID"],
        FWD: ["FWD"],
        F: ["FWD"]
      },
      defaultSeasonWeeks: 38,
      defaultPlayoffStartWeek: 35,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: true,
      supportsKeeper: true,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: true,
      commissionerSettings: [
        { key: "doubleGameweekMode", label: "Highlight double gameweeks", type: "toggle", defaultValue: true, section: "schedule" },
        { key: "benchSlots", label: "Bench Slots", type: "number", defaultValue: 4, min: 2, max: 8, section: "roster" }
      ],
      aiMetadata: {
        scoringStyle: "points",
        keyPositions: ["FWD", "MID", "GK"],
        lineupNotes: "Gameweek-based; double gameweeks and blanks drive strategy."
      }
    };
  }
});

// lib/sportConfig/configs/tennis.ts
var TENNIS_CONFIG;
var init_tennis = __esm({
  "lib/sportConfig/configs/tennis.ts"() {
    "use strict";
    TENNIS_CONFIG = {
      sport: "TENNIS",
      displayName: "Tennis",
      slug: "tennis",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "match_win", label: "Match Win", defaultPoints: 10, isToggleable: false, group: "match", sport: "TENNIS" },
        { key: "set_win", label: "Set Win", defaultPoints: 3, isToggleable: true, group: "match", sport: "TENNIS" },
        { key: "aces", label: "Aces", defaultPoints: 0.5, isToggleable: true, group: "serve", sport: "TENNIS" },
        { key: "dfs_saved", label: "Double faults (negative)", defaultPoints: -1, isToggleable: true, group: "serve", sport: "TENNIS" },
        { key: "breaks", label: "Service breaks", defaultPoints: 2, isToggleable: true, group: "return", sport: "TENNIS" }
      ],
      scoringPresets: [{ name: "ATP/WTA weekly", categories: [] }],
      defaultRosterSlots: [
        { key: "ATP", label: "ATP Player", eligiblePositions: ["ATP"], defaultCount: 3, minCount: 2, maxCount: 5, isOptional: false },
        { key: "WTA", label: "WTA Player", eligiblePositions: ["WTA"], defaultCount: 3, minCount: 2, maxCount: 5, isOptional: false }
      ],
      defaultBenchSlots: 2,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: { ATP: ["ATP"], WTA: ["WTA"] },
      defaultSeasonWeeks: 45,
      defaultPlayoffStartWeek: 42,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "per_event",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [
        { key: "grandSlamMultiplier", label: "Grand Slam multiplier", type: "number", defaultValue: 2, min: 1, max: 3, section: "scoring" }
      ],
      aiMetadata: {
        scoringStyle: "tournament_rounds",
        lineupNotes: "Surface and draw path matter \u2014 use deterministic bonus rules."
      }
    };
  }
});

// lib/sportConfig/configs/wwe.ts
var WWE_CONFIG;
var init_wwe = __esm({
  "lib/sportConfig/configs/wwe.ts"() {
    "use strict";
    WWE_CONFIG = {
      sport: "WWE",
      displayName: "WWE",
      slug: "wwe",
      defaultScoringSystem: "points",
      lineupFrequency: "weekly",
      hasBye: false,
      scoringCategories: [
        { key: "win_match", label: "Match Win", defaultPoints: 5, isToggleable: false, group: "match", sport: "WWE" },
        { key: "title_change", label: "Title Change (pick)", defaultPoints: 10, isToggleable: true, group: "story", sport: "WWE" },
        { key: "pinfall", label: "Pinfall / Submission", defaultPoints: 3, isToggleable: true, group: "match", sport: "WWE" },
        { key: "appearance", label: "TV Appearance", defaultPoints: 1, isToggleable: true, group: "misc", sport: "WWE" }
      ],
      scoringPresets: [{ name: "Standard", categories: [] }],
      defaultRosterSlots: [
        { key: "SUPERSTAR", label: "Superstar", eligiblePositions: ["SUPERSTAR"], defaultCount: 5, minCount: 3, maxCount: 8, isOptional: false }
      ],
      defaultBenchSlots: 2,
      defaultIRSlots: 0,
      defaultTaxiSlots: 0,
      defaultDevySlots: 0,
      positionEligibility: { SUPERSTAR: ["SUPERSTAR"] },
      defaultSeasonWeeks: 52,
      defaultPlayoffStartWeek: 48,
      defaultPlayoffTeams: 4,
      defaultMatchupPeriodDays: 7,
      lineupLockType: "weekly",
      supportsRedraft: true,
      supportsDynasty: false,
      supportsKeeper: false,
      supportsDevy: false,
      supportsC2C: false,
      supportsIDP: false,
      supportsSuperflex: false,
      supportsTEPremium: false,
      supportsPPR: false,
      supportsCategories: false,
      supportsDailyLineups: false,
      commissionerSettings: [{ key: "ppvWeight", label: "PPV event weight multiplier", type: "number", defaultValue: 2, min: 1, max: 4, section: "scoring" }],
      aiMetadata: {
        scoringStyle: "storyline_events",
        lineupNotes: "PPV-heavy calendar; kayfabe results still need deterministic scoring rules."
      }
    };
  }
});

// lib/sportConfig/index.ts
function resolveSportConfigKey(sport) {
  const u = sport.trim().toUpperCase();
  return SPORT_ALIASES[u] ?? u;
}
function getSportConfig(sport) {
  const key = resolveSportConfigKey(sport);
  const config = SPORT_CONFIGS[key];
  if (!config) throw new Error(`Unknown sport: ${sport}`);
  return config;
}
function tryGetSportConfig(sport) {
  try {
    return getSportConfig(sport);
  } catch {
    return null;
  }
}
function getScoringCategories(sport, activeToggles = []) {
  const config = getSportConfig(sport);
  return config.scoringCategories.filter((cat) => {
    if (!cat.requiresToggle) return true;
    return activeToggles.includes(cat.requiresToggle);
  });
}
function expandSportConfigToggles(activeToggles) {
  const s = new Set(activeToggles);
  if (s.has("IDP")) {
    s.add("enableIDP");
  }
  if (s.has("SUPERFLEX")) {
    s.add("enableSuperflex");
  }
  if (s.has("TE_PREMIUM")) {
    s.add("enableTEPremium");
  }
  return [...s];
}
var SPORT_ALIASES, SPORT_CONFIGS;
var init_sportConfig = __esm({
  "lib/sportConfig/index.ts"() {
    "use strict";
    init_cricket();
    init_golf();
    init_horse_racing();
    init_mlb();
    init_nascar();
    init_nba();
    init_ncaaf();
    init_ncaab();
    init_nfl();
    init_nhl();
    init_soccer();
    init_tennis();
    init_wwe();
    SPORT_ALIASES = {
      NCAAFB: "NCAAF",
      NCAABB: "NCAAB"
    };
    SPORT_CONFIGS = {
      NFL: NFL_CONFIG,
      NBA: NBA_CONFIG,
      MLB: MLB_CONFIG,
      NHL: NHL_CONFIG,
      NCAAF: NCAAF_CONFIG,
      NCAAB: NCAAB_CONFIG,
      SOCCER: SOCCER_CONFIG,
      GOLF: GOLF_CONFIG,
      NASCAR: NASCAR_CONFIG,
      WWE: WWE_CONFIG,
      CRICKET: CRICKET_CONFIG,
      HORSE_RACING: HORSE_RACING_CONFIG,
      TENNIS: TENNIS_CONFIG
    };
  }
});

// lib/nfl-scoring/scoringKeyBridge.ts
function bridgeTierKey(uiKey) {
  const pa = /^dst_pa_(.+)$/.exec(uiKey);
  if (pa) {
    const candidate = `def_pa_${pa[1]}`;
    return ENGINE_KEYS.has(candidate) ? candidate : null;
  }
  const ya = /^dst_ya_(.+)$/.exec(uiKey);
  if (ya) {
    const direct = `def_ya_${ya[1]}`;
    if (ENGINE_KEYS.has(direct)) return direct;
    if (/^(450|500|550)/.test(ya[1])) return "def_ya_450_plus";
    return null;
  }
  return null;
}
function bridgeScoringKey(uiKey) {
  if (STATIC_MAP[uiKey]) return STATIC_MAP[uiKey];
  const tier = bridgeTierKey(uiKey);
  if (tier) return tier;
  if (ENGINE_KEYS.has(uiKey)) return uiKey;
  return null;
}
function bridgeUiRulesToEngineCategoryPoints(uiRules) {
  const out = {};
  if (!uiRules || typeof uiRules !== "object") return out;
  for (const [uiKey, value] of Object.entries(uiRules)) {
    if (typeof value !== "number" || !Number.isFinite(value)) continue;
    const engineKey = bridgeScoringKey(uiKey);
    if (engineKey) out[engineKey] = value;
  }
  return out;
}
var ENGINE_KEYS, STATIC_MAP;
var init_scoringKeyBridge2 = __esm({
  "lib/nfl-scoring/scoringKeyBridge.ts"() {
    "use strict";
    init_sportConfig();
    ENGINE_KEYS = new Set(
      getScoringCategories("NFL", expandSportConfigToggles(["IDP", "SUPERFLEX", "TE_PREMIUM"])).map((c) => c.key)
    );
    STATIC_MAP = {
      // Passing
      passing_yards: "pass_yds",
      passing_td: "pass_td",
      interception_thrown: "pass_int",
      passing_2pt: "two_pt",
      // Rushing
      rushing_yards: "rush_yds",
      rushing_td: "rush_td",
      rushing_2pt: "two_pt",
      // Receiving
      reception: "rec",
      receiving_yards: "rec_yds",
      receiving_td: "rec_td",
      receiving_2pt: "two_pt",
      // Misc / fumbles (UI catalog uses `off_fumble_recovery_td`; accept aliases too)
      fumble_lost: "fum_lost",
      fumbles_lost: "fum_lost",
      off_fumble_recovery_td: "fumble_td",
      fumble_recovery_td: "fumble_td",
      // Yardage / passing bonuses
      three_hundred_yd_pass_bonus: "pass_300_bonus",
      four_hundred_yd_pass_bonus: "pass_400_bonus",
      one_hundred_yd_rush_bonus: "rush_100_bonus",
      one_hundred_yd_rec_bonus: "rec_100_bonus",
      // IDP — UI catalog keys → engine keys (engine uses short forms). The ones that
      // are already engine keys (idp_sack/idp_td/idp_safety/idp_qb_hit) resolve via
      // the identity fallback; these are the UI keys that DIFFER from the engine key.
      idp_solo_tackle: "idp_solo",
      idp_assisted_tackle: "idp_assist",
      idp_interception: "idp_int",
      idp_pass_defended: "idp_pd",
      idp_fumble_forced: "idp_ff",
      idp_fumble_recovery: "idp_fr",
      idp_tackle_for_loss: "idp_tfl",
      // Kicking (engine buckets FG 0-39 together; collapse the UI sub-buckets)
      fg_0_19: "fg_0_39",
      fg_20_29: "fg_0_39",
      fg_30_39: "fg_0_39",
      fg_40_49: "fg_40_49",
      fg_50_59: "fg_50_plus",
      fg_50_plus: "fg_50_plus",
      fg_60_plus: "fg_50_plus",
      pat_made: "xp_made",
      fg_missed_0_19: "fg_miss",
      fg_missed_20_29: "fg_miss",
      fg_missed_30_39: "fg_miss",
      fg_missed_40_49: "fg_miss",
      fg_missed_50_59: "fg_miss",
      // Team Defense — counting (accept both the prompt's names and the actual UI keys)
      dst_sack: "def_sack",
      dst_int: "def_int",
      dst_interception: "def_int",
      dst_fumble_recovery: "def_fr",
      dst_safety: "def_safety",
      dst_blocked_kick: "def_blk_kick",
      dst_td: "def_td",
      dst_defensive_td: "def_td",
      // Special teams / return TDs (engine has one return/ST-TD bucket)
      st_td: "def_st_td",
      dst_special_teams_td: "def_st_td",
      dst_kick_return_td: "def_st_td",
      dst_punt_return_td: "def_st_td",
      // Return yardage (G9) — UI Special-Teams keys → engine return-yard keys.
      st_kick_return_yards: "def_kr_yd",
      st_punt_return_yards: "def_pr_yd",
      dst_kick_return_yards: "def_kr_yd",
      dst_punt_return_yards: "def_pr_yd"
    };
  }
});

// lib/league-runtime/leagueRuntimeEvents.ts
var CANONICAL_LEAGUE_RUNTIME_EVENT_TYPES, CANONICAL_EVENT_TYPE_SET;
var init_leagueRuntimeEvents = __esm({
  "lib/league-runtime/leagueRuntimeEvents.ts"() {
    "use strict";
    CANONICAL_LEAGUE_RUNTIME_EVENT_TYPES = [
      "league.created",
      "league.updated",
      "settings.updated",
      "draft.scheduled",
      "draft.started",
      "draft.pick",
      "draft.pick.submitted",
      "draft.queue.selected",
      "draft.autopick",
      "draft.substitute_pick",
      "draft.player_drafted",
      "draft.paused",
      "draft.resumed",
      "draft.manager.disconnected",
      "draft.manager.reconnected",
      "draft.recommendation.viewed",
      "draft.trade_opportunity.generated",
      "draft.chat.message",
      "draft.chat.mirrored",
      "draft.completed",
      "draft.recap.generated",
      "roster.updated",
      "roster.player.added",
      "roster.player.dropped",
      "roster.player.started",
      "roster.player.benched",
      "roster.player.moved_to_ir",
      "roster.player.removed_from_ir",
      "roster.player.locked",
      "roster.player.unlocked",
      "lineup.submitted",
      "lineup.updated",
      "lineup.starter.changed",
      "schedule.generated",
      "schedule.regenerated",
      "schedule.locked",
      "schedule.week.opened",
      "schedule.week.completed",
      "matchup.created",
      "schedule.bye.assigned",
      "division.assigned",
      "waiver.period.opened",
      "waiver.period.closed",
      "waiver.claim.submitted",
      "waiver.claim.edited",
      "waiver.claim.cancelled",
      "waiver.processing.started",
      "waiver.claim.won",
      "waiver.claim.failed",
      "waiver.faab.deducted",
      "waiver.priority.updated",
      "waiver.free_agent.added",
      "waiver.processed",
      "waiver.transaction.recorded",
      "trade.proposed",
      "trade.countered",
      "trade.accepted",
      "trade.rejected",
      "trade.cancelled",
      "trade.expired",
      "trade.vetoed",
      "trade.league_vote.opened",
      "trade.league_vote.cast",
      "trade.league_vote.passed",
      "trade.league_vote.failed",
      "trade.executed",
      "trade.processed",
      "trade.roster.updated",
      "trade.transaction.recorded",
      "matchup.updated",
      "matchup.finalized",
      "scoring.period.opened",
      "scoring.player_stat.ingested",
      "scoring.fantasy_points.calculated",
      "scoring.team_score.updated",
      "scoring.matchup_score.updated",
      "scoring.updated",
      "scoring.stat_correction.applied",
      "standings.updated",
      "standings.recalculated",
      "playoffs.qualification.calculated",
      "playoffs.seeds.updated",
      "playoffs.bracket.generated",
      "playoffs.bracket.locked",
      "playoffs.round.opened",
      "playoffs.matchup.created",
      "playoffs.qualification_snapshot.updated",
      "playoffs.advancement",
      "playoffs.team.advanced",
      "playoffs.team.eliminated",
      "playoffs.reseeded",
      "playoffs.consolation.generated",
      "playoffs.championship.matchup.created",
      "playoffs.champion.crowned",
      "playoffs.final_standings.recorded",
      "season.completed",
      "player.data.refreshed",
      "player.status.changed",
      "player.injury.status_changed",
      "player.projection.updated",
      "player.team.changed",
      "player.data.fallback_used",
      "player.injury.updated",
      "player.news.updated",
      "commissioner.override",
      "commissioner.schedule_override",
      "commissioner.scoring_correction",
      "commissioner.waiver_override",
      "commissioner.trade_override",
      "commissioner.playoff_override",
      "commissioner.announcement.created",
      "league.chat.message",
      "league.chat.system_message",
      "lineup.illegal.flagged",
      "import.completed",
      "import.needs_review",
      "runtime.unknown"
    ];
    CANONICAL_EVENT_TYPE_SET = new Set(CANONICAL_LEAGUE_RUNTIME_EVENT_TYPES);
  }
});

// lib/scoring-runtime/nflStatNormalization.ts
function isRecord(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function asNumber(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : void 0;
  }
  return void 0;
}
function firstNumber(source, keys) {
  for (const key of keys) {
    const n = asNumber(source[key]);
    if (n !== void 0) return n;
  }
  return void 0;
}
function sumNumbers(source, keys) {
  let total = 0;
  let found = false;
  for (const key of keys) {
    const n = asNumber(source[key]);
    if (n !== void 0) {
      total += n;
      found = true;
    }
  }
  return found ? total : void 0;
}
function setIfNumber(out, key, value) {
  if (value !== void 0) out[key] = value;
}
function normalizeNflWeeklyStats(raw) {
  const source = isRecord(raw) && isRecord(raw.stats) ? raw.stats : raw;
  if (!isRecord(source)) return {};
  const out = {};
  setIfNumber(out, "pass_yds", firstNumber(source, ["pass_yds", "pass_yd", "passing_yards", "passingYards"]));
  setIfNumber(out, "pass_td", firstNumber(source, ["pass_td", "passing_td", "passing_touchdowns", "passingTouchdowns"]));
  setIfNumber(out, "pass_int", firstNumber(source, ["pass_int", "passing_int", "interception_thrown", "interceptions"]));
  setIfNumber(out, "rush_yds", firstNumber(source, ["rush_yds", "rush_yd", "rushing_yards", "rushingYards"]));
  setIfNumber(out, "rush_td", firstNumber(source, ["rush_td", "rushing_td", "rushing_touchdowns", "rushingTouchdowns"]));
  setIfNumber(out, "rec", firstNumber(source, ["rec", "receptions", "receiving_receptions"]));
  setIfNumber(out, "rec_yds", firstNumber(source, ["rec_yds", "rec_yd", "receiving_yards", "receivingYards"]));
  setIfNumber(out, "rec_td", firstNumber(source, ["rec_td", "receiving_td", "receiving_touchdowns", "receivingTouchdowns"]));
  setIfNumber(out, "two_pt", sumNumbers(source, ["two_pt", "pass_2pt", "rush_2pt", "rec_2pt"]));
  setIfNumber(out, "fum_lost", firstNumber(source, ["fum_lost", "fumbles_lost", "fumble_lost"]));
  setIfNumber(out, "fumble_td", firstNumber(source, ["fumble_td", "fum_rec_td"]));
  setIfNumber(out, "fg_0_39", sumNumbers(source, ["fg_0_39", "fgm_0_19", "fgm_20_29", "fgm_30_39"]));
  setIfNumber(out, "fg_40_49", firstNumber(source, ["fg_40_49", "fgm_40_49"]));
  setIfNumber(out, "fg_50_plus", firstNumber(source, ["fg_50_plus", "fgm_50p", "fgm_50_plus"]));
  setIfNumber(out, "fg_miss", firstNumber(source, ["fg_miss", "fgmiss", "fg_missed"]));
  setIfNumber(out, "xp_made", firstNumber(source, ["xp_made", "xpm", "pat_made"]));
  setIfNumber(out, "idp_solo", firstNumber(source, ["idp_solo", "tackle_solo", "solo_tkl"]));
  setIfNumber(out, "idp_assist", firstNumber(source, ["idp_assist", "tackle_ast", "assist_tkl"]));
  setIfNumber(out, "idp_sack", firstNumber(source, ["idp_sack", "sack"]));
  setIfNumber(out, "idp_int", firstNumber(source, ["idp_int", "def_int", "int"]));
  setIfNumber(out, "idp_pd", firstNumber(source, ["idp_pd", "pass_defended", "pd"]));
  setIfNumber(out, "idp_ff", firstNumber(source, ["idp_ff", "fum_forced", "ff"]));
  setIfNumber(out, "idp_fr", firstNumber(source, ["idp_fr", "fum_rec", "fr"]));
  setIfNumber(out, "idp_td", firstNumber(source, ["idp_td", "def_td"]));
  setIfNumber(out, "idp_safety", firstNumber(source, ["idp_safety", "safety"]));
  setIfNumber(out, "idp_tfl", firstNumber(source, ["idp_tfl", "tackle_loss", "tfl"]));
  setIfNumber(out, "idp_qb_hit", firstNumber(source, ["idp_qb_hit", "qb_hit", "qb_hits"]));
  return out;
}
function normalizeNflTeamDefenseWeeklyStats(raw) {
  const source = isRecord(raw) && isRecord(raw.stats) ? raw.stats : raw;
  if (!isRecord(source)) return {};
  const out = {};
  setIfNumber(out, "def_sack", firstNumber(source, ["def_sack", "sack", "sacks", "def_sacks"]));
  setIfNumber(out, "def_int", firstNumber(source, ["def_int", "interceptions", "int", "def_interceptions"]));
  setIfNumber(out, "def_fr", firstNumber(source, ["def_fr", "fum_rec", "fumble_recovery", "fumbles_recovered"]));
  setIfNumber(out, "def_safety", firstNumber(source, ["def_safety", "def_safe", "safe", "safety", "safeties"]));
  setIfNumber(out, "def_blk_kick", firstNumber(source, ["def_blk_kick", "blk_kick", "blocked_kick", "blocked_kicks"]));
  setIfNumber(out, "def_td", firstNumber(source, ["def_td", "defensive_td", "def_tds"]));
  setIfNumber(
    out,
    "def_st_td",
    sumNumbers(source, ["def_st_td", "st_td", "special_teams_td", "ret_td", "kr_td", "pr_td"])
  );
  setIfNumber(
    out,
    "def_points_allowed",
    firstNumber(source, ["def_points_allowed", "pts_allow", "points_allowed", "pa"])
  );
  setIfNumber(
    out,
    "def_yds_allowed",
    firstNumber(source, ["def_yds_allowed", "yds_allow", "yards_allowed", "total_yards_allowed"])
  );
  setIfNumber(out, "def_kr_yd", firstNumber(source, ["def_kr_yd", "kr_yd", "kick_return_yards", "kr_yards"]));
  setIfNumber(out, "def_pr_yd", firstNumber(source, ["def_pr_yd", "pr_yd", "punt_return_yards", "pr_yards"]));
  return out;
}
function isTeamDefenseRow(playerId, position) {
  const pos = String(position ?? "").trim().toUpperCase();
  if (pos === "DEF" || pos === "DST") return true;
  return String(playerId ?? "").toLowerCase().startsWith("nfl:def:");
}
function teamAbbrevFromDefPlayerId(playerId) {
  const match = String(playerId ?? "").match(/^nfl:def:(.+)$/i);
  return match ? match[1].trim().toUpperCase() : null;
}
function pointsAllowedFromGame(game, team) {
  const want = team.trim().toUpperCase();
  const home = String(game.homeTeam ?? "").trim().toUpperCase();
  const away = String(game.awayTeam ?? "").trim().toUpperCase();
  if (want === home) return typeof game.awayScore === "number" ? game.awayScore : null;
  if (want === away) return typeof game.homeScore === "number" ? game.homeScore : null;
  return null;
}
function rowWeek(row) {
  if (!isRecord(row)) return null;
  const direct = asNumber(row.week);
  if (direct !== void 0) return direct;
  if (isRecord(row.stats)) {
    const nested = asNumber(row.stats.week);
    if (nested !== void 0) return nested;
  }
  return null;
}
function findCachedWeekPayload(payload, week) {
  if (Array.isArray(payload)) {
    return payload.find((row) => rowWeek(row) === week) ?? null;
  }
  if (!isRecord(payload)) return null;
  const direct = payload[String(week)];
  if (direct) return direct;
  for (const key of ["gameLogs", "weeklyStats", "weeks", "stats"]) {
    const nested = payload[key];
    const found = findCachedWeekPayload(nested, week);
    if (found) return found;
  }
  return rowWeek(payload) === week ? payload : null;
}
var init_nflStatNormalization = __esm({
  "lib/scoring-runtime/nflStatNormalization.ts"() {
    "use strict";
  }
});

// lib/scoring-runtime/canonicalNflRedraftScoringRuntime.ts
function isNflRedraftScoringStarterSlot(slotType) {
  return !NON_SCORING_SLOTS.has(String(slotType ?? "").toLowerCase());
}
var NON_SCORING_SLOTS;
var init_canonicalNflRedraftScoringRuntime = __esm({
  "lib/scoring-runtime/canonicalNflRedraftScoringRuntime.ts"() {
    "use strict";
    init_leagueRuntimeEvents();
    init_scoringKeyBridge2();
    init_sportConfig();
    init_nflStatNormalization();
    NON_SCORING_SLOTS = /* @__PURE__ */ new Set(["bench", "bn", "ir", "taxi", "devy", "reserve"]);
  }
});

// lib/sport-defaults/types.ts
var init_types3 = __esm({
  "lib/sport-defaults/types.ts"() {
    "use strict";
  }
});

// lib/nba-roster/NbaRosterTemplates.ts
var NBA_ROSTER_SLOTS, NBA_SLOT_MAP;
var init_NbaRosterTemplates = __esm({
  "lib/nba-roster/NbaRosterTemplates.ts"() {
    "use strict";
    NBA_ROSTER_SLOTS = [
      // Standard positions
      { key: "PG", label: "Point Guard", shortLabel: "PG", color: "#f43f5e", category: "offense", eligiblePositions: ["PG"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "SG", label: "Shooting Guard", shortLabel: "SG", color: "#fb923c", category: "offense", eligiblePositions: ["SG"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "G", label: "Guard", shortLabel: "G", color: "#f59e0b", category: "flex", eligiblePositions: ["PG", "SG"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "SF", label: "Small Forward", shortLabel: "SF", color: "#14b8a6", category: "offense", eligiblePositions: ["SF"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "PF", label: "Power Forward", shortLabel: "PF", color: "#22d3ee", category: "offense", eligiblePositions: ["PF"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "F", label: "Forward", shortLabel: "F", color: "#34d399", category: "flex", eligiblePositions: ["SF", "PF"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "C", label: "Center", shortLabel: "C", color: "#818cf8", category: "offense", eligiblePositions: ["C"], defaultCount: 2, minCount: 0, maxCount: 4 },
      { key: "UTIL", label: "Utility", shortLabel: "UTIL", color: "#a78bfa", category: "flex", eligiblePositions: ["PG", "SG", "SF", "PF", "C"], defaultCount: 2, minCount: 0, maxCount: 6 },
      // Advanced flex
      { key: "FLEX_PG_SG", label: "Flex (PG/SG)", shortLabel: "PG/SG", color: "#e879f9", category: "flex", eligiblePositions: ["PG", "SG"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "FLEX_SF_PF", label: "Flex (SF/PF)", shortLabel: "SF/PF", color: "#e879f9", category: "flex", eligiblePositions: ["SF", "PF"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "FLEX_G_F", label: "Flex (G/F)", shortLabel: "G/F", color: "#e879f9", category: "flex", eligiblePositions: ["PG", "SG", "SF", "PF"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "FLEX_F_C", label: "Flex (F/C)", shortLabel: "F/C", color: "#e879f9", category: "flex", eligiblePositions: ["SF", "PF", "C"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "SUPER_UTIL", label: "Super Utility", shortLabel: "SU", color: "#ec4899", category: "flex", eligiblePositions: ["PG", "SG", "SF", "PF", "C"], defaultCount: 0, minCount: 0, maxCount: 3 },
      // Bench / Reserve
      { key: "BN", label: "Bench", shortLabel: "BN", color: "#4b5563", category: "bench", eligiblePositions: [], defaultCount: 3, minCount: 0, maxCount: 15 },
      { key: "IL", label: "Injured List", shortLabel: "IL", color: "#ef4444", category: "reserve", eligiblePositions: [], defaultCount: 2, minCount: 0, maxCount: 5 },
      { key: "IL_PLUS", label: "IL+", shortLabel: "IL+", color: "#dc2626", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "TAXI", label: "Taxi Squad", shortLabel: "TAXI", color: "#8b5cf6", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 8 },
      { key: "DEVY", label: "Devy Stash", shortLabel: "DEVY", color: "#7c3aed", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 12 },
      // C2C College
      { key: "C2C_G", label: "College Guard", shortLabel: "cG", color: "#f59e0b", category: "college", eligiblePositions: ["G", "PG", "SG"], defaultCount: 0, minCount: 0, maxCount: 4 },
      { key: "C2C_F", label: "College Forward", shortLabel: "cF", color: "#34d399", category: "college", eligiblePositions: ["F", "SF", "PF"], defaultCount: 0, minCount: 0, maxCount: 4 },
      { key: "C2C_C", label: "College Center", shortLabel: "cC", color: "#818cf8", category: "college", eligiblePositions: ["C"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "C2C_UTIL", label: "College Utility", shortLabel: "cUTIL", color: "#a78bfa", category: "college", eligiblePositions: ["G", "F", "C", "PG", "SG", "SF", "PF"], defaultCount: 0, minCount: 0, maxCount: 6 },
      { key: "C2C_BN", label: "College Bench", shortLabel: "cBN", color: "#4b5563", category: "college", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 15 },
      { key: "CAMPUS", label: "Campus / Rights", shortLabel: "CAMP", color: "#7c3aed", category: "college", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 12 }
    ];
    NBA_SLOT_MAP = new Map(NBA_ROSTER_SLOTS.map((s) => [s.key, s]));
  }
});

// lib/nhl-roster/NhlRosterTemplates.ts
var NHL_ROSTER_SLOTS, NHL_SLOT_MAP;
var init_NhlRosterTemplates = __esm({
  "lib/nhl-roster/NhlRosterTemplates.ts"() {
    "use strict";
    NHL_ROSTER_SLOTS = [
      // Skaters
      { key: "C", label: "Center", shortLabel: "C", color: "#f43f5e", category: "offense", eligiblePositions: ["C"], defaultCount: 2, minCount: 0, maxCount: 5 },
      { key: "LW", label: "Left Wing", shortLabel: "LW", color: "#14b8a6", category: "offense", eligiblePositions: ["LW"], defaultCount: 2, minCount: 0, maxCount: 5 },
      { key: "RW", label: "Right Wing", shortLabel: "RW", color: "#38bdf8", category: "offense", eligiblePositions: ["RW"], defaultCount: 2, minCount: 0, maxCount: 5 },
      { key: "W", label: "Winger", shortLabel: "W", color: "#22d3ee", category: "flex", eligiblePositions: ["LW", "RW"], defaultCount: 0, minCount: 0, maxCount: 4 },
      { key: "D", label: "Defenseman", shortLabel: "D", color: "#818cf8", category: "offense", eligiblePositions: ["D"], defaultCount: 4, minCount: 0, maxCount: 8 },
      { key: "UTIL", label: "Utility", shortLabel: "UTIL", color: "#a78bfa", category: "flex", eligiblePositions: ["C", "LW", "RW", "D"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "SKT", label: "Skater (Any)", shortLabel: "SKT", color: "#e879f9", category: "flex", eligiblePositions: ["C", "LW", "RW", "D"], defaultCount: 0, minCount: 0, maxCount: 4 },
      // Flex variants
      { key: "FLEX_CW", label: "Flex (C/W)", shortLabel: "C/W", color: "#e879f9", category: "flex", eligiblePositions: ["C", "LW", "RW"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "FLEX_LW_RW", label: "Flex (LW/RW)", shortLabel: "LW/RW", color: "#e879f9", category: "flex", eligiblePositions: ["LW", "RW"], defaultCount: 0, minCount: 0, maxCount: 3 },
      { key: "FLEX_FD", label: "Flex (F/D)", shortLabel: "F/D", color: "#e879f9", category: "flex", eligiblePositions: ["C", "LW", "RW", "D"], defaultCount: 0, minCount: 0, maxCount: 3 },
      // Goalie
      { key: "G", label: "Goalie", shortLabel: "G", color: "#f59e0b", category: "offense", eligiblePositions: ["G"], defaultCount: 2, minCount: 0, maxCount: 4 },
      // Bench / Reserve
      { key: "BN", label: "Bench", shortLabel: "BN", color: "#4b5563", category: "bench", eligiblePositions: [], defaultCount: 4, minCount: 0, maxCount: 15 },
      { key: "IR", label: "Injured Reserve", shortLabel: "IR", color: "#ef4444", category: "reserve", eligiblePositions: [], defaultCount: 2, minCount: 0, maxCount: 6 },
      { key: "IR_PLUS", label: "IR+", shortLabel: "IR+", color: "#dc2626", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 4 },
      { key: "TAXI", label: "Taxi Squad", shortLabel: "TAXI", color: "#8b5cf6", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 8 },
      { key: "PROSPECT", label: "Prospect Stash", shortLabel: "PRSP", color: "#7c3aed", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 12 },
      { key: "RIGHTS", label: "Rights / Development", shortLabel: "RTS", color: "#6d28d9", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 10 },
      { key: "RESERVE", label: "Reserve", shortLabel: "RSV", color: "#5b21b6", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 10 }
    ];
    NHL_SLOT_MAP = new Map(NHL_ROSTER_SLOTS.map((s) => [s.key, s]));
  }
});

// lib/mlb-roster/MlbRosterTemplates.ts
var MLB_ROSTER_SLOTS, MLB_SLOT_MAP;
var init_MlbRosterTemplates = __esm({
  "lib/mlb-roster/MlbRosterTemplates.ts"() {
    "use strict";
    MLB_ROSTER_SLOTS = [
      // Hitters
      { key: "C", label: "Catcher", shortLabel: "C", color: "#f43f5e", category: "offense", eligiblePositions: ["C"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "1B", label: "First Base", shortLabel: "1B", color: "#14b8a6", category: "offense", eligiblePositions: ["1B"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "2B", label: "Second Base", shortLabel: "2B", color: "#38bdf8", category: "offense", eligiblePositions: ["2B"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "3B", label: "Third Base", shortLabel: "3B", color: "#f59e0b", category: "offense", eligiblePositions: ["3B"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "SS", label: "Shortstop", shortLabel: "SS", color: "#a78bfa", category: "offense", eligiblePositions: ["SS"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "CI", label: "Corner Infielder", shortLabel: "CI", color: "#22d3ee", category: "flex", eligiblePositions: ["1B", "3B"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "MI", label: "Middle Infielder", shortLabel: "MI", color: "#818cf8", category: "flex", eligiblePositions: ["2B", "SS"], defaultCount: 1, minCount: 0, maxCount: 3 },
      { key: "OF", label: "Outfielder", shortLabel: "OF", color: "#34d399", category: "offense", eligiblePositions: ["OF", "LF", "CF", "RF"], defaultCount: 3, minCount: 0, maxCount: 7 },
      { key: "UTIL", label: "Utility", shortLabel: "UTIL", color: "#e879f9", category: "flex", eligiblePositions: ["C", "1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF", "DH"], defaultCount: 1, minCount: 0, maxCount: 4 },
      { key: "H", label: "Hitter (Any)", shortLabel: "H", color: "#ec4899", category: "flex", eligiblePositions: ["C", "1B", "2B", "3B", "SS", "OF", "LF", "CF", "RF", "DH"], defaultCount: 0, minCount: 0, maxCount: 4 },
      // Pitchers
      { key: "SP", label: "Starting Pitcher", shortLabel: "SP", color: "#ef4444", category: "offense", eligiblePositions: ["SP"], defaultCount: 2, minCount: 0, maxCount: 7 },
      { key: "RP", label: "Relief Pitcher", shortLabel: "RP", color: "#fb923c", category: "offense", eligiblePositions: ["RP"], defaultCount: 2, minCount: 0, maxCount: 5 },
      { key: "P", label: "Pitcher (Any)", shortLabel: "P", color: "#dc2626", category: "flex", eligiblePositions: ["SP", "RP"], defaultCount: 3, minCount: 0, maxCount: 7 },
      // Bench / Reserve
      { key: "BN", label: "Bench", shortLabel: "BN", color: "#4b5563", category: "bench", eligiblePositions: [], defaultCount: 6, minCount: 0, maxCount: 20 },
      { key: "IL", label: "Injured List", shortLabel: "IL", color: "#ef4444", category: "reserve", eligiblePositions: [], defaultCount: 2, minCount: 0, maxCount: 8 },
      { key: "IL_PLUS", label: "IL+ (60-day)", shortLabel: "IL+", color: "#dc2626", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 5 },
      { key: "NA", label: "Not Active", shortLabel: "NA", color: "#6b7280", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 10 },
      { key: "TAXI", label: "Taxi Squad", shortLabel: "TAXI", color: "#8b5cf6", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 10 },
      { key: "PROSPECT", label: "Prospect Stash", shortLabel: "PRSP", color: "#7c3aed", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 15 },
      { key: "RIGHTS", label: "Rights / Development", shortLabel: "RTS", color: "#6d28d9", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 15 },
      { key: "MINORS", label: "Minor League", shortLabel: "MiLB", color: "#5b21b6", category: "reserve", eligiblePositions: [], defaultCount: 0, minCount: 0, maxCount: 15 }
    ];
    MLB_SLOT_MAP = new Map(MLB_ROSTER_SLOTS.map((s) => [s.key, s]));
  }
});

// lib/category-scoring/NbaCategoryRegistry.ts
var NBA_CATEGORY_POINTS, NBA_CATEGORY_REBOUNDS, NBA_CATEGORY_ASSISTS, NBA_CATEGORY_STEALS, NBA_CATEGORY_BLOCKS, NBA_CATEGORY_TURNOVERS, NBA_CATEGORY_FG_PCT, NBA_CATEGORY_FT_PCT, NBA_CATEGORY_THREES_MADE, NBA_EIGHT_CAT, NBA_NINE_CAT;
var init_NbaCategoryRegistry = __esm({
  "lib/category-scoring/NbaCategoryRegistry.ts"() {
    "use strict";
    NBA_CATEGORY_POINTS = {
      id: "nba_pts",
      label: "PTS",
      description: "Points scored",
      direction: "higher",
      computation: { kind: "sum", statKey: "points_scored" }
    };
    NBA_CATEGORY_REBOUNDS = {
      id: "nba_reb",
      label: "REB",
      description: "Total rebounds",
      direction: "higher",
      computation: { kind: "sum", statKey: "rebound" }
    };
    NBA_CATEGORY_ASSISTS = {
      id: "nba_ast",
      label: "AST",
      direction: "higher",
      computation: { kind: "sum", statKey: "assist" }
    };
    NBA_CATEGORY_STEALS = {
      id: "nba_stl",
      label: "STL",
      direction: "higher",
      computation: { kind: "sum", statKey: "steal" }
    };
    NBA_CATEGORY_BLOCKS = {
      id: "nba_blk",
      label: "BLK",
      direction: "higher",
      computation: { kind: "sum", statKey: "block" }
    };
    NBA_CATEGORY_TURNOVERS = {
      id: "nba_to",
      label: "TO",
      description: "Turnovers \u2014 lower is better.",
      direction: "lower",
      computation: { kind: "sum", statKey: "turnover" }
    };
    NBA_CATEGORY_FG_PCT = {
      id: "nba_fg_pct",
      label: "FG%",
      description: "Field-goal percentage (made / attempted, team totals).",
      direction: "higher",
      computation: {
        kind: "ratio",
        numeratorStatKey: "field_goals_made",
        denominatorStatKey: "field_goals_attempted"
      }
    };
    NBA_CATEGORY_FT_PCT = {
      id: "nba_ft_pct",
      label: "FT%",
      description: "Free-throw percentage (made / attempted, team totals).",
      direction: "higher",
      computation: {
        kind: "ratio",
        numeratorStatKey: "free_throws_made",
        denominatorStatKey: "free_throws_attempted"
      }
    };
    NBA_CATEGORY_THREES_MADE = {
      id: "nba_3pm",
      label: "3PM",
      description: "Three-pointers made.",
      direction: "higher",
      computation: { kind: "sum", statKey: "three_point_made" }
    };
    NBA_EIGHT_CAT = Object.freeze([
      NBA_CATEGORY_POINTS,
      NBA_CATEGORY_REBOUNDS,
      NBA_CATEGORY_ASSISTS,
      NBA_CATEGORY_STEALS,
      NBA_CATEGORY_BLOCKS,
      NBA_CATEGORY_THREES_MADE,
      NBA_CATEGORY_FG_PCT,
      NBA_CATEGORY_FT_PCT
    ]);
    NBA_NINE_CAT = Object.freeze([
      NBA_CATEGORY_POINTS,
      NBA_CATEGORY_REBOUNDS,
      NBA_CATEGORY_ASSISTS,
      NBA_CATEGORY_STEALS,
      NBA_CATEGORY_BLOCKS,
      NBA_CATEGORY_TURNOVERS,
      NBA_CATEGORY_FG_PCT,
      NBA_CATEGORY_FT_PCT,
      NBA_CATEGORY_THREES_MADE
    ]);
  }
});

// lib/category-scoring/MultiSportCategoryRegistry.ts
var count, ratio, MLB_FIVE_BY_FIVE, NHL_TEN_CAT;
var init_MultiSportCategoryRegistry = __esm({
  "lib/category-scoring/MultiSportCategoryRegistry.ts"() {
    "use strict";
    count = (id2, label, statKey, direction = "higher") => ({ id: id2, label, direction, computation: { kind: "sum", statKey } });
    ratio = (id2, label, numeratorStatKey, denominatorStatKey, direction, multiplier = 1, additionalNumeratorStatKeys = []) => ({ id: id2, label, direction, computation: { kind: "ratio", numeratorStatKey, denominatorStatKey, multiplier, additionalNumeratorStatKeys } });
    MLB_FIVE_BY_FIVE = Object.freeze([
      count("mlb_r", "R", "r"),
      count("mlb_hr", "HR", "hr"),
      count("mlb_rbi", "RBI", "rbi"),
      count("mlb_sb", "SB", "sb"),
      ratio("mlb_avg", "AVG", "hits", "at_bats", "higher"),
      count("mlb_w", "W", "w"),
      count("mlb_sv", "SV", "sv"),
      count("mlb_k", "K", "so"),
      ratio("mlb_era", "ERA", "er", "outs_recorded", "lower", 27),
      ratio("mlb_whip", "WHIP", "hits_allowed", "outs_recorded", "lower", 3, ["walks_allowed"])
    ]);
    NHL_TEN_CAT = Object.freeze([
      count("nhl_g", "G", "g"),
      count("nhl_a", "A", "a"),
      count("nhl_pm", "+/-", "plusminus"),
      count("nhl_ppp", "PPP", "ppp"),
      count("nhl_sog", "SOG", "sog"),
      count("nhl_hits", "HIT", "hits"),
      count("nhl_w", "W", "g_win"),
      count("nhl_so", "SO", "g_so"),
      ratio("nhl_sv_pct", "SV%", "g_sv", "g_sa", "higher"),
      ratio("nhl_gaa", "GAA", "g_ga", "g_seconds", "lower", 3600)
    ]);
  }
});

// lib/category-scoring/types.ts
var init_types4 = __esm({
  "lib/category-scoring/types.ts"() {
    "use strict";
  }
});

// lib/category-scoring/CategoryMatchupResolver.ts
function stat(totals, key) {
  const value = totals[key] ?? totals[CANONICAL_ALIASES[key]];
  return typeof value === "number" && Number.isFinite(value) ? value : 0;
}
function computeCategoryValue(totals, category) {
  const comp = category.computation;
  if (comp.kind === "sum") {
    return stat(totals, comp.statKey);
  }
  const n = stat(totals, comp.numeratorStatKey) + (comp.additionalNumeratorStatKeys ?? []).reduce((n2, key) => n2 + stat(totals, key), 0);
  const d = stat(totals, comp.denominatorStatKey);
  if (d === 0) return 0;
  return (comp.multiplier ?? 1) * n / d;
}
function isCategoryEligible(totals, category) {
  const comp = category.computation;
  return comp.kind !== "ratio" || stat(totals, comp.denominatorStatKey) >= (comp.minimumDenominator ?? Number.MIN_VALUE);
}
function compareValues(aValue, bValue, direction) {
  if (aValue === bValue) return "tie";
  if (direction === "higher") return aValue > bValue ? "a" : "b";
  return aValue < bValue ? "a" : "b";
}
function resolveCategoryMatchup(aTotals, bTotals, categories) {
  const breakdown = [];
  let aWins = 0;
  let bWins = 0;
  let ties = 0;
  for (const cat of categories) {
    const aValue = computeCategoryValue(aTotals, cat);
    const bValue = computeCategoryValue(bTotals, cat);
    const aEligible = isCategoryEligible(aTotals, cat);
    const bEligible = isCategoryEligible(bTotals, cat);
    const winner = aEligible !== bEligible ? aEligible ? "a" : "b" : !aEligible ? "tie" : compareValues(aValue, bValue, cat.direction);
    if (winner === "a") aWins += 1;
    else if (winner === "b") bWins += 1;
    else ties += 1;
    breakdown.push({
      categoryId: cat.id,
      label: cat.label,
      aValue,
      bValue,
      winner
    });
  }
  return { categories: breakdown, aWins, bWins, ties };
}
function accumulateTeamTotals(playerStatMaps) {
  const out = {};
  for (const playerStats of playerStatMaps) {
    for (const key of Object.keys(playerStats)) {
      const v = playerStats[key];
      if (typeof v !== "number" || !Number.isFinite(v)) continue;
      out[key] = (out[key] ?? 0) + v;
    }
  }
  return out;
}
var CANONICAL_ALIASES;
var init_CategoryMatchupResolver = __esm({
  "lib/category-scoring/CategoryMatchupResolver.ts"() {
    "use strict";
    CANONICAL_ALIASES = {
      points_scored: "pts",
      rebound: "reb",
      assist: "ast",
      steal: "stl",
      block: "blk",
      turnover: "to",
      three_point_made: "threes",
      field_goals_made: "fgm",
      field_goals_attempted: "fga",
      free_throws_made: "ftm",
      free_throws_attempted: "fta"
    };
  }
});

// lib/category-scoring/index.ts
function getCategoryPresetDefinitions(presetId) {
  if (presetId === "nba_8cat") return NBA_EIGHT_CAT;
  if (presetId === "nba_9cat") return NBA_NINE_CAT;
  if (presetId === "mlb_5x5") return MLB_FIVE_BY_FIVE;
  if (presetId === "nhl_10cat") return NHL_TEN_CAT;
  return null;
}
var init_category_scoring = __esm({
  "lib/category-scoring/index.ts"() {
    "use strict";
    init_NbaCategoryRegistry();
    init_MultiSportCategoryRegistry();
    init_types4();
    init_NbaCategoryRegistry();
    init_CategoryMatchupResolver();
    init_MultiSportCategoryRegistry();
  }
});

// lib/category-scoring/leagueMode.ts
function resolveLeagueScoringMode(settings, sport) {
  const s = settings ?? {};
  const sc = s.scoringSettings ?? s.foundation_defaults?.scoring ?? {};
  const raw = s.scoring_mode ?? sc.scoringMode ?? s.sportConfig?.scoringMode ?? "points";
  const mode = raw === "categories" ? "h2h_category" : raw;
  if (!["points", "h2h_category", "roto"].includes(mode)) throw new Error("INVALID_SCORING_MODE");
  if (mode === "points") return { mode, categories: [], presetId: null };
  const presetId = s.category_preset_id ?? sc.categoryPresetId ?? s.sportConfig?.categoryPresetId;
  const categories = getCategoryPresetDefinitions(presetId);
  if (!categories || !String(presetId).startsWith(`${sport.toLowerCase()}_`)) throw new Error("INVALID_CATEGORY_PRESET");
  const minima = s.category_minimum_denominators ?? {};
  return { mode, presetId, categories: categories.map((c) => c.computation.kind === "ratio" && typeof minima[c.id] === "number" && Number.isFinite(minima[c.id]) && minima[c.id] > 0 ? { ...c, computation: { ...c.computation, minimumDenominator: minima[c.id] } } : c) };
}
var init_leagueMode = __esm({
  "lib/category-scoring/leagueMode.ts"() {
    "use strict";
    init_category_scoring();
  }
});

// lib/league-concepts/dynastyDefaults.ts
var init_dynastyDefaults = __esm({
  "lib/league-concepts/dynastyDefaults.ts"() {
    "use strict";
    init_NbaRosterTemplates();
    init_NhlRosterTemplates();
    init_MlbRosterTemplates();
    init_leagueMode();
  }
});

// lib/sport-defaults/sport-type-utils.ts
var init_sport_type_utils = __esm({
  "lib/sport-defaults/sport-type-utils.ts"() {
    "use strict";
    init_types3();
  }
});

// lib/sport-defaults/LeagueVariantRegistry.ts
var init_LeagueVariantRegistry = __esm({
  "lib/sport-defaults/LeagueVariantRegistry.ts"() {
    "use strict";
    init_sport_type_utils();
    init_sport_scope();
  }
});

// lib/sport-teams/SportTeamMetadataRegistry.ts
var init_SportTeamMetadataRegistry = __esm({
  "lib/sport-teams/SportTeamMetadataRegistry.ts"() {
    "use strict";
    init_team_abbrev();
  }
});

// lib/sport-defaults/SportDefaultsRegistry.ts
var init_SportDefaultsRegistry = __esm({
  "lib/sport-defaults/SportDefaultsRegistry.ts"() {
    "use strict";
    init_types3();
    init_dynastyDefaults();
    init_LeagueVariantRegistry();
    init_SportTeamMetadataRegistry();
    init_sport_scope();
  }
});

// lib/draft-defaults/DraftRoomConfigResolver.ts
var init_DraftRoomConfigResolver = __esm({
  "lib/draft-defaults/DraftRoomConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_SportDefaultsRegistry();
    init_sport_type_utils();
  }
});

// lib/draft-defaults/DraftUISettingsResolver.ts
var init_DraftUISettingsResolver = __esm({
  "lib/draft-defaults/DraftUISettingsResolver.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/multi-sport/sport-types.ts
var init_sport_types = __esm({
  "lib/multi-sport/sport-types.ts"() {
    "use strict";
  }
});

// lib/nfl-scoring/NflScoringCategories.ts
function buildFullNflDefaultConfig() {
  const config = {};
  for (const cat of NFL_SCORING_CATEGORIES) {
    for (const row of cat.rows) {
      config[row.key] = row.defaultValue;
    }
  }
  for (const row of NFL_PREMIUM_SCORING.rows) {
    config[row.key] = row.defaultValue;
  }
  return config;
}
var NFL_SCORING_CATEGORIES, NFL_PREMIUM_SCORING;
var init_NflScoringCategories = __esm({
  "lib/nfl-scoring/NflScoringCategories.ts"() {
    "use strict";
    NFL_SCORING_CATEGORIES = [
      {
        id: "passing",
        label: "Passing",
        rows: [
          { key: "passing_yards", label: "Passing Yards", helper: "1 point every 25 yards (0.04 per yard)", defaultValue: 0.04 },
          { key: "passing_td", label: "Passing TD", defaultValue: 4 },
          { key: "passing_first_down", label: "Passing 1st Down", defaultValue: 0 },
          { key: "passing_2pt", label: "2-Pt Conversion", defaultValue: 2 },
          { key: "interception_thrown", label: "Pass Intercepted", defaultValue: -1 },
          { key: "pick_6_thrown", label: "Pick 6 Thrown", defaultValue: 0 },
          { key: "completion", label: "Pass Completed", defaultValue: 0 },
          { key: "incomplete_pass", label: "Incomplete Pass", defaultValue: 0 },
          { key: "passing_attempt", label: "Pass Attempts", defaultValue: 0 },
          { key: "qb_sacked", label: "QB Sacked", defaultValue: 0 },
          { key: "forty_yd_completion_bonus", label: "40+ Yard Completion Bonus", defaultValue: 0 },
          { key: "forty_yd_pass_td_bonus", label: "40+ Yard Pass TD Bonus", defaultValue: 0 },
          { key: "fifty_yd_pass_td_bonus", label: "50+ Yard Pass TD Bonus", defaultValue: 0 }
        ]
      },
      {
        id: "rushing",
        label: "Rushing",
        rows: [
          { key: "rushing_yards", label: "Rushing Yards", helper: "1 point every 10 yards (0.10 per yard)", defaultValue: 0.1 },
          { key: "rushing_td", label: "Rushing TD", defaultValue: 6 },
          { key: "rushing_first_down", label: "Rushing 1st Down", defaultValue: 0 },
          { key: "rushing_2pt", label: "2-Pt Conversion", defaultValue: 2 },
          { key: "rush_attempt", label: "Rush Attempts", defaultValue: 0 },
          { key: "forty_yd_rush_bonus", label: "40+ Yard Rush Bonus", defaultValue: 0 },
          { key: "forty_yd_rush_td_bonus", label: "40+ Yard Rush TD Bonus", defaultValue: 0 },
          { key: "fifty_yd_rush_td_bonus", label: "50+ Yard Rush TD Bonus", defaultValue: 0 }
        ]
      },
      {
        id: "receiving",
        label: "Receiving",
        rows: [
          { key: "reception", label: "Reception", defaultValue: 0.5 },
          { key: "receiving_yards", label: "Receiving Yards", helper: "1 point every 10 yards (0.10 per yard)", defaultValue: 0.1 },
          { key: "receiving_td", label: "Receiving TD", defaultValue: 6 },
          { key: "receiving_first_down", label: "Receiving 1st Down", defaultValue: 0 },
          { key: "receiving_2pt", label: "2-Pt Conversion", defaultValue: 2 },
          { key: "rec_0_4_bonus", label: "0-4 Yard Reception Bonus", defaultValue: 0 },
          { key: "rec_5_9_bonus", label: "5-9 Yard Reception Bonus", defaultValue: 0 },
          { key: "rec_10_19_bonus", label: "10-19 Yard Reception Bonus", defaultValue: 0 },
          { key: "rec_20_29_bonus", label: "20-29 Yard Reception Bonus", defaultValue: 0 },
          { key: "rec_30_39_bonus", label: "30-39 Yard Reception Bonus", defaultValue: 0 },
          { key: "forty_yd_reception_bonus", label: "40+ Yard Reception Bonus", defaultValue: 0 },
          { key: "forty_yd_rec_td_bonus", label: "40+ Yard Reception TD Bonus", defaultValue: 0 },
          { key: "fifty_yd_rec_td_bonus", label: "50+ Yard Reception TD Bonus", defaultValue: 0 },
          { key: "rec_bonus_rb", label: "Reception Bonus - RB", defaultValue: 0 },
          { key: "rec_bonus_wr", label: "Reception Bonus - WR", defaultValue: 0 },
          { key: "rec_bonus_te", label: "Reception Bonus - TE", defaultValue: 0 }
        ]
      },
      {
        id: "kicking",
        label: "Kicking",
        rows: [
          { key: "fg_made", label: "FG Made", defaultValue: 0 },
          { key: "fg_0_19", label: "FG Made (0-19 yards)", defaultValue: 3 },
          { key: "fg_20_29", label: "FG Made (20-29 yards)", defaultValue: 3 },
          { key: "fg_30_39", label: "FG Made (30-39 yards)", defaultValue: 3 },
          { key: "fg_40_49", label: "FG Made (40-49 yards)", defaultValue: 4 },
          { key: "fg_50_59", label: "FG Made (50-59 yards)", defaultValue: 5 },
          { key: "fg_50_plus", label: "FG Made (50+ yards)", defaultValue: 5 },
          { key: "fg_60_plus", label: "FG Made (60+ yards)", defaultValue: 0 },
          { key: "fg_per_yard", label: "Points per FG yard", defaultValue: 0 },
          { key: "fg_per_yard_over_30", label: "Points per FG yard over 30", defaultValue: 0 },
          { key: "pat_made", label: "PAT Made", defaultValue: 1 },
          { key: "fg_missed", label: "FG Missed", defaultValue: 0 },
          { key: "fg_missed_0_19", label: "FG Missed (0-19 yards)", defaultValue: -1 },
          { key: "fg_missed_20_29", label: "FG Missed (20-29 yards)", defaultValue: -1 },
          { key: "fg_missed_30_39", label: "FG Missed (30-39 yards)", defaultValue: -1 },
          { key: "fg_missed_40_49", label: "FG Missed (40-49 yards)", defaultValue: 0 },
          { key: "fg_missed_50_59", label: "FG Missed (50-59 yards)", defaultValue: 0 },
          { key: "fg_missed_50_plus", label: "FG Missed (50+ yards)", defaultValue: 0 },
          { key: "fg_missed_60_plus", label: "FG Missed (60+ yards)", defaultValue: 0 },
          { key: "pat_missed", label: "PAT Missed", defaultValue: -1 }
        ]
      },
      {
        id: "team_defense",
        label: "Team Defense",
        rows: [
          { key: "dst_td", label: "Defense TD", defaultValue: 6 },
          { key: "dst_pa_0", label: "Points Allowed 0", defaultValue: 10 },
          { key: "dst_pa_1_6", label: "Points Allowed 1-6", defaultValue: 7 },
          { key: "dst_pa_7_13", label: "Points Allowed 7-13", defaultValue: 4 },
          { key: "dst_pa_14_20", label: "Points Allowed 14-20", defaultValue: 1 },
          { key: "dst_pa_21_27", label: "Points Allowed 21-27", defaultValue: 0 },
          { key: "dst_pa_28_34", label: "Points Allowed 28-34", defaultValue: -1 },
          { key: "dst_pa_35_plus", label: "Points Allowed 35+", defaultValue: -4 },
          { key: "dst_pts_per_pa", label: "Points Per Point Allowed", defaultValue: 0 },
          { key: "dst_ya_0_99", label: "Less Than 100 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_100_199", label: "100-199 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_200_299", label: "200-299 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_300_349", label: "300-349 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_350_399", label: "350-399 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_400_449", label: "400-449 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_450_499", label: "450-499 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_500_549", label: "500-549 Total Yards Allowed", defaultValue: 0 },
          { key: "dst_ya_550_plus", label: "550+ Total Yards Allowed", defaultValue: 0 },
          { key: "dst_pts_per_ya", label: "Points Per Yard Allowed", defaultValue: 0 },
          { key: "dst_three_and_out", label: "3 and Out", defaultValue: 0 },
          { key: "dst_fourth_down_stop", label: "4th Down Stop", defaultValue: 0 },
          { key: "dst_qb_hit", label: "Hit on QB", defaultValue: 0 },
          { key: "dst_sack", label: "Sacks", defaultValue: 1 },
          { key: "dst_sack_yards", label: "Sack Yards", defaultValue: 0 },
          { key: "dst_interception", label: "Interceptions", defaultValue: 2 },
          { key: "dst_int_return_yards", label: "INT Return Yards", defaultValue: 0 },
          { key: "dst_fumble_recovery", label: "Fumble Recovery", defaultValue: 2 },
          { key: "dst_fumble_return_yards", label: "Fumble Return Yards", defaultValue: 0 },
          { key: "dst_tackle_for_loss", label: "Tackle For Loss", defaultValue: 0 },
          { key: "dst_solo_tackle", label: "Solo Tackle", defaultValue: 0 },
          { key: "dst_tackle", label: "Tackle", defaultValue: 0 },
          { key: "dst_safety", label: "Safety", defaultValue: 2 },
          { key: "dst_forced_fumble", label: "Forced Fumble", defaultValue: 0 },
          { key: "dst_blocked_kick", label: "Blocked Kick", defaultValue: 2 },
          { key: "dst_forced_punt", label: "Forced Punt", defaultValue: 0 },
          { key: "dst_pass_defended", label: "Pass Defended", defaultValue: 0 },
          { key: "dst_2pt_return", label: "2-Pt Conversion Returns", defaultValue: 2 },
          { key: "dst_missed_fg_return_yards", label: "Missed FG Return Yards", defaultValue: 0 },
          { key: "dst_blocked_kick_return_yards", label: "Blocked Kick Return Yards", defaultValue: 0 }
        ]
      },
      {
        id: "special_teams",
        label: "Special Teams",
        rows: [
          { key: "st_td", label: "Special Teams TD", defaultValue: 0 },
          { key: "st_forced_fumble", label: "Special Teams Forced Fumble", defaultValue: 0 },
          { key: "st_fumble_recovery", label: "Special Teams Fumble Recovery", defaultValue: 0 },
          { key: "st_solo_tackle", label: "Special Teams Solo Tackle", defaultValue: 0 },
          { key: "st_punt_return_yards", label: "Punt Return Yards", defaultValue: 0 },
          { key: "st_kick_return_yards", label: "Kick Return Yards", defaultValue: 0 },
          { key: "st_player_td", label: "Special Teams Player TD", defaultValue: 0 },
          { key: "st_player_forced_fumble", label: "ST Player Forced Fumble", defaultValue: 0 },
          { key: "st_player_fumble_recovery", label: "ST Player Fumble Recovery", defaultValue: 0 },
          { key: "st_player_solo_tackle", label: "ST Player Solo Tackle", defaultValue: 0 },
          { key: "st_player_punt_return_yards", label: "Player Punt Return Yards", defaultValue: 0 },
          { key: "st_player_kick_return_yards", label: "Player Kick Return Yards", defaultValue: 0 }
        ]
      },
      {
        id: "misc",
        label: "Misc",
        rows: [
          { key: "fumble", label: "Fumble", defaultValue: 0 },
          { key: "fumble_lost", label: "Fumble Lost", defaultValue: -2 },
          { key: "off_fumble_recovery_td", label: "Fumble Recovery TD", defaultValue: 6 }
        ]
      },
      {
        id: "bonus",
        label: "Bonus",
        rows: [
          { key: "one_hundred_yd_rush_bonus", label: "100-199 Yard Rushing Game", defaultValue: 0 },
          { key: "two_hundred_yd_rush_bonus", label: "200+ Yard Rushing Game", defaultValue: 0 },
          { key: "one_hundred_yd_rec_bonus", label: "100-199 Yard Receiving Game", defaultValue: 0 },
          { key: "two_hundred_yd_rec_bonus", label: "200+ Yard Receiving Game", defaultValue: 0 },
          { key: "three_hundred_yd_pass_bonus", label: "300-399 Yard Passing Game", defaultValue: 0 },
          { key: "four_hundred_yd_pass_bonus", label: "400+ Yard Passing Game", defaultValue: 0 },
          { key: "one_hundred_combined_bonus", label: "100-199 Combined Rush + Rec Yards", defaultValue: 0 },
          { key: "two_hundred_combined_bonus", label: "200+ Combined Rush + Rec Yards", defaultValue: 0 },
          { key: "twenty_five_completions_bonus", label: "25+ Pass Completions", defaultValue: 0 },
          { key: "twenty_carries_bonus", label: "20+ Carries", defaultValue: 0 },
          { key: "first_down_bonus_rb", label: "1st Down Bonus - RB", defaultValue: 0 },
          { key: "first_down_bonus_wr", label: "1st Down Bonus - WR", defaultValue: 0 },
          { key: "first_down_bonus_te", label: "1st Down Bonus - TE", defaultValue: 0 },
          { key: "first_down_bonus_qb", label: "1st Down Bonus - QB", defaultValue: 0 }
        ]
      },
      {
        id: "idp",
        label: "IDP",
        rows: [
          { key: "idp_td", label: "IDP TD", defaultValue: 6 },
          { key: "idp_sack", label: "Sack", defaultValue: 3 },
          { key: "idp_sack_yards", label: "Sack Yards", defaultValue: 0 },
          { key: "idp_qb_hit", label: "Hit on QB", defaultValue: 1 },
          { key: "idp_tackle", label: "Tackle", defaultValue: 0 },
          { key: "idp_tackle_for_loss", label: "Tackle For Loss", defaultValue: 1 },
          { key: "idp_blocked_kick", label: "Blocked Punt, PAT or FG", defaultValue: 3 },
          { key: "idp_interception", label: "Interception", defaultValue: 4 },
          { key: "idp_int_return_yards", label: "INT Return Yards", defaultValue: 0 },
          { key: "idp_fumble_return_yards", label: "Fumble Return Yards", defaultValue: 0 },
          { key: "idp_fumble_forced", label: "Forced Fumble", defaultValue: 3 },
          { key: "idp_safety", label: "Safety", defaultValue: 2 },
          { key: "idp_assisted_tackle", label: "Assisted Tackle", defaultValue: 0.5 },
          { key: "idp_solo_tackle", label: "Solo Tackle", defaultValue: 1 },
          { key: "idp_pass_defended", label: "Pass Defended", defaultValue: 1 },
          { key: "idp_ten_tackle_bonus", label: "10+ Tackle Bonus", defaultValue: 0 },
          { key: "idp_two_sack_bonus", label: "2+ Sack Bonus", defaultValue: 0 },
          { key: "idp_three_pass_defended_bonus", label: "3+ Pass Defended Bonus", defaultValue: 0 },
          { key: "idp_fifty_yd_int_return_td_bonus", label: "50+ Yard INT Return TD Bonus", defaultValue: 0 }
        ]
      }
    ];
    NFL_PREMIUM_SCORING = {
      id: "premium",
      label: "Advanced",
      rows: [
        { key: "air_yards", label: "Air Yards", premium: true, defaultValue: 0 },
        { key: "completed_air_yards", label: "Completed Air Yards", premium: true, defaultValue: 0 },
        { key: "receiving_air_yards", label: "Receiving Air Yards", premium: true, defaultValue: 0 },
        { key: "passing_air_yards", label: "Passing Air Yards", premium: true, defaultValue: 0 }
      ]
    };
  }
});

// lib/ncaaf-scoring/NcaafScoringCategories.ts
function buildNcaafScoringDefaults() {
  const config = {};
  for (const cat of NCAAF_ALL_SCORING_CATEGORIES) {
    for (const row of cat.rows) {
      config[row.key] = row.defaultValue;
    }
  }
  return config;
}
var PASSING, RUSHING, RECEIVING, KICKING, TEAM_DEFENSE, SPECIAL_TEAMS, MISC, BONUS, IDP, NCAAF_PREMIUM_SCORING, NCAAF_SCORING_CATEGORIES, NCAAF_ALL_SCORING_CATEGORIES;
var init_NcaafScoringCategories = __esm({
  "lib/ncaaf-scoring/NcaafScoringCategories.ts"() {
    "use strict";
    PASSING = {
      id: "passing",
      label: "Passing",
      rows: [
        { key: "passing_yards", label: "Passing Yards", helper: "0.04 pts/yd (1 pt per 25 yds)", defaultValue: 0.04 },
        { key: "passing_td", label: "Passing TD", defaultValue: 4 },
        { key: "passing_first_down", label: "Passing 1st Down", defaultValue: 0 },
        { key: "passing_2pt", label: "2-Pt Conversion", defaultValue: 2 },
        { key: "interception_thrown", label: "Pass Intercepted", defaultValue: -1 },
        { key: "pick_6_thrown", label: "Pick 6 Thrown", defaultValue: 0 },
        { key: "completion", label: "Pass Completed", defaultValue: 0 },
        { key: "incomplete_pass", label: "Incomplete Pass", defaultValue: 0 },
        { key: "passing_attempt", label: "Pass Attempts", defaultValue: 0 },
        { key: "qb_sacked", label: "QB Sacked", defaultValue: 0 },
        { key: "forty_yd_completion_bonus", label: "40+ Yard Completion Bonus", defaultValue: 0 },
        { key: "forty_yd_pass_td_bonus", label: "40+ Yard Pass TD Bonus", defaultValue: 0 },
        { key: "fifty_yd_pass_td_bonus", label: "50+ Yard Pass TD Bonus", defaultValue: 0 },
        { key: "three_hundred_yd_pass_bonus", label: "300+ Passing Game Bonus", defaultValue: 0 },
        { key: "four_hundred_yd_pass_bonus", label: "400+ Passing Game Bonus", defaultValue: 0 },
        { key: "five_hundred_yd_pass_bonus", label: "500+ Passing Game Bonus", defaultValue: 0 }
      ]
    };
    RUSHING = {
      id: "rushing",
      label: "Rushing",
      rows: [
        { key: "rushing_yards", label: "Rushing Yards", helper: "0.10 pts/yd (1 pt per 10 yds)", defaultValue: 0.1 },
        { key: "rushing_td", label: "Rushing TD", defaultValue: 6 },
        { key: "rushing_first_down", label: "Rushing 1st Down", defaultValue: 0 },
        { key: "rushing_2pt", label: "2-Pt Conversion", defaultValue: 2 },
        { key: "rush_attempt", label: "Rush Attempts", defaultValue: 0 },
        { key: "forty_yd_rush_bonus", label: "40+ Yard Rush Bonus", defaultValue: 0 },
        { key: "forty_yd_rush_td_bonus", label: "40+ Yard Rush TD Bonus", defaultValue: 0 },
        { key: "fifty_yd_rush_td_bonus", label: "50+ Yard Rush TD Bonus", defaultValue: 0 },
        { key: "one_hundred_yd_rush_bonus", label: "100+ Rushing Game Bonus", defaultValue: 0 },
        { key: "two_hundred_yd_rush_bonus", label: "200+ Rushing Game Bonus", defaultValue: 0 },
        { key: "three_hundred_yd_rush_bonus", label: "300+ Rushing Game Bonus", defaultValue: 0 }
      ]
    };
    RECEIVING = {
      id: "receiving",
      label: "Receiving",
      rows: [
        { key: "reception", label: "Reception", defaultValue: 0.5 },
        { key: "receiving_yards", label: "Receiving Yards", helper: "0.10 pts/yd (1 pt per 10 yds)", defaultValue: 0.1 },
        { key: "receiving_td", label: "Receiving TD", defaultValue: 6 },
        { key: "receiving_first_down", label: "Receiving 1st Down", defaultValue: 0 },
        { key: "receiving_2pt", label: "2-Pt Conversion", defaultValue: 2 },
        { key: "rec_0_4_bonus", label: "0-4 Yard Reception Bonus", defaultValue: 0 },
        { key: "rec_5_9_bonus", label: "5-9 Yard Reception Bonus", defaultValue: 0 },
        { key: "rec_10_19_bonus", label: "10-19 Yard Reception Bonus", defaultValue: 0 },
        { key: "rec_20_29_bonus", label: "20-29 Yard Reception Bonus", defaultValue: 0 },
        { key: "rec_30_39_bonus", label: "30-39 Yard Reception Bonus", defaultValue: 0 },
        { key: "forty_yd_reception_bonus", label: "40+ Yard Reception Bonus", defaultValue: 0 },
        { key: "forty_yd_rec_td_bonus", label: "40+ Yard Reception TD Bonus", defaultValue: 0 },
        { key: "fifty_yd_rec_td_bonus", label: "50+ Yard Reception TD Bonus", defaultValue: 0 },
        { key: "rec_bonus_rb", label: "Reception Bonus - RB", defaultValue: 0 },
        { key: "rec_bonus_wr", label: "Reception Bonus - WR", defaultValue: 0 },
        { key: "rec_bonus_te", label: "Reception Bonus - TE", defaultValue: 0 },
        { key: "one_hundred_yd_rec_bonus", label: "100+ Receiving Game Bonus", defaultValue: 0 },
        { key: "two_hundred_yd_rec_bonus", label: "200+ Receiving Game Bonus", defaultValue: 0 },
        { key: "three_hundred_yd_rec_bonus", label: "300+ Receiving Game Bonus", defaultValue: 0 }
      ]
    };
    KICKING = {
      id: "kicking",
      label: "Kicking",
      rows: [
        { key: "fg_made", label: "FG Made", defaultValue: 0 },
        { key: "fg_0_19", label: "FG Made (0-19 yards)", defaultValue: 3 },
        { key: "fg_20_29", label: "FG Made (20-29 yards)", defaultValue: 3 },
        { key: "fg_30_39", label: "FG Made (30-39 yards)", defaultValue: 3 },
        { key: "fg_40_49", label: "FG Made (40-49 yards)", defaultValue: 4 },
        { key: "fg_50_59", label: "FG Made (50-59 yards)", defaultValue: 5 },
        { key: "fg_50_plus", label: "FG Made (50+ yards)", defaultValue: 5 },
        { key: "fg_60_plus", label: "FG Made (60+ yards)", defaultValue: 0 },
        { key: "fg_per_yard", label: "Points per FG yard", defaultValue: 0 },
        { key: "fg_per_yard_over_30", label: "Points per FG yard over 30", defaultValue: 0 },
        { key: "pat_made", label: "PAT Made", defaultValue: 1 },
        { key: "fg_missed", label: "FG Missed", defaultValue: 0 },
        { key: "fg_missed_0_19", label: "FG Missed (0-19 yards)", defaultValue: -1 },
        { key: "fg_missed_20_29", label: "FG Missed (20-29 yards)", defaultValue: -1 },
        { key: "fg_missed_30_39", label: "FG Missed (30-39 yards)", defaultValue: -1 },
        { key: "fg_missed_40_49", label: "FG Missed (40-49 yards)", defaultValue: 0 },
        { key: "fg_missed_50_59", label: "FG Missed (50-59 yards)", defaultValue: 0 },
        { key: "fg_missed_50_plus", label: "FG Missed (50+ yards)", defaultValue: 0 },
        { key: "fg_missed_60_plus", label: "FG Missed (60+ yards)", defaultValue: 0 },
        { key: "pat_missed", label: "PAT Missed", defaultValue: -1 }
      ]
    };
    TEAM_DEFENSE = {
      id: "team_defense",
      label: "Team Defense",
      rows: [
        { key: "dst_td", label: "Defense TD", defaultValue: 6 },
        { key: "dst_pa_0", label: "Points Allowed 0", defaultValue: 10 },
        { key: "dst_pa_1_6", label: "Points Allowed 1-6", defaultValue: 7 },
        { key: "dst_pa_7_13", label: "Points Allowed 7-13", defaultValue: 4 },
        { key: "dst_pa_14_20", label: "Points Allowed 14-20", defaultValue: 1 },
        { key: "dst_pa_21_27", label: "Points Allowed 21-27", defaultValue: 0 },
        { key: "dst_pa_28_34", label: "Points Allowed 28-34", defaultValue: -1 },
        { key: "dst_pa_35_plus", label: "Points Allowed 35+", defaultValue: -4 },
        { key: "dst_pts_per_pa", label: "Points Per Point Allowed", defaultValue: 0 },
        { key: "dst_ya_0_99", label: "Less Than 100 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_100_199", label: "100-199 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_200_299", label: "200-299 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_300_349", label: "300-349 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_350_399", label: "350-399 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_400_449", label: "400-449 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_450_499", label: "450-499 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_500_549", label: "500-549 Total Yards Allowed", defaultValue: 0 },
        { key: "dst_ya_550_plus", label: "550+ Total Yards Allowed", defaultValue: 0 },
        { key: "dst_pts_per_ya", label: "Points Per Yard Allowed", defaultValue: 0 },
        { key: "dst_three_and_out", label: "3 and Out", defaultValue: 0 },
        { key: "dst_fourth_down_stop", label: "4th Down Stop", defaultValue: 0 },
        { key: "dst_qb_hit", label: "Hit on QB", defaultValue: 0 },
        { key: "dst_sack", label: "Sacks", defaultValue: 1 },
        { key: "dst_sack_yards", label: "Sack Yards", defaultValue: 0 },
        { key: "dst_interception", label: "Interceptions", defaultValue: 2 },
        { key: "dst_int_return_yards", label: "INT Return Yards", defaultValue: 0 },
        { key: "dst_fumble_recovery", label: "Fumble Recovery", defaultValue: 2 },
        { key: "dst_fumble_return_yards", label: "Fumble Return Yards", defaultValue: 0 },
        { key: "dst_tackle_for_loss", label: "Tackle For Loss", defaultValue: 0 },
        { key: "dst_solo_tackle", label: "Solo Tackle", defaultValue: 0 },
        { key: "dst_tackle", label: "Tackle", defaultValue: 0 },
        { key: "dst_safety", label: "Safety", defaultValue: 2 },
        { key: "dst_forced_fumble", label: "Forced Fumble", defaultValue: 0 },
        { key: "dst_blocked_kick", label: "Blocked Kick", defaultValue: 2 },
        { key: "dst_forced_punt", label: "Forced Punt", defaultValue: 0 },
        { key: "dst_pass_defended", label: "Pass Defended", defaultValue: 0 },
        { key: "dst_2pt_return", label: "2-Pt Conversion Returns", defaultValue: 2 },
        { key: "dst_return_td", label: "Kick/Punt Return TD", defaultValue: 6 },
        { key: "dst_missed_fg_return_yards", label: "Missed FG Return Yards", defaultValue: 0 },
        { key: "dst_blocked_kick_return_yards", label: "Blocked Kick Return Yards", defaultValue: 0 }
      ]
    };
    SPECIAL_TEAMS = {
      id: "special_teams",
      label: "Special Teams",
      rows: [
        { key: "st_td", label: "Special Teams TD", defaultValue: 0 },
        { key: "st_forced_fumble", label: "Special Teams Forced Fumble", defaultValue: 0 },
        { key: "st_fumble_recovery", label: "Special Teams Fumble Recovery", defaultValue: 0 },
        { key: "st_solo_tackle", label: "Special Teams Solo Tackle", defaultValue: 0 },
        { key: "st_punt_return_yards", label: "Punt Return Yards", defaultValue: 0 },
        { key: "st_kick_return_yards", label: "Kick Return Yards", defaultValue: 0 },
        { key: "st_player_td", label: "Special Teams Player TD", defaultValue: 0 },
        { key: "st_player_forced_fumble", label: "Special Teams Player Forced Fumble", defaultValue: 0 },
        { key: "st_player_fumble_recovery", label: "Special Teams Player Fumble Recovery", defaultValue: 0 },
        { key: "st_player_solo_tackle", label: "Special Teams Player Solo Tackle", defaultValue: 0 },
        { key: "st_player_punt_return_yards", label: "Player Punt Return Yards", defaultValue: 0 },
        { key: "st_player_kick_return_yards", label: "Player Kick Return Yards", defaultValue: 0 }
      ]
    };
    MISC = {
      id: "misc",
      label: "Misc",
      rows: [
        { key: "fumble", label: "Fumble", defaultValue: 0 },
        { key: "fumble_lost", label: "Fumble Lost", defaultValue: -2 },
        { key: "off_fumble_recovery_td", label: "Fumble Recovery TD", defaultValue: 6 }
      ]
    };
    BONUS = {
      id: "bonus",
      label: "Bonus",
      rows: [
        { key: "one_hundred_combined_rush_bonus", label: "100-199 Combined Rush + Rec Yards", defaultValue: 0 },
        { key: "two_hundred_combined_rush_bonus", label: "200+ Combined Rush + Rec Yards", defaultValue: 0 },
        { key: "twenty_five_completions_bonus", label: "25+ Pass Completions", defaultValue: 0 },
        { key: "twenty_carries_bonus", label: "20+ Carries", defaultValue: 0 },
        { key: "first_down_bonus_rb", label: "1st Down Bonus - RB", defaultValue: 0 },
        { key: "first_down_bonus_wr", label: "1st Down Bonus - WR", defaultValue: 0 },
        { key: "first_down_bonus_te", label: "1st Down Bonus - TE", defaultValue: 0 },
        { key: "first_down_bonus_qb", label: "1st Down Bonus - QB", defaultValue: 0 }
      ]
    };
    IDP = {
      id: "idp",
      label: "IDP",
      rows: [
        { key: "idp_td", label: "IDP TD", defaultValue: 6 },
        { key: "idp_sack", label: "Sack", defaultValue: 3 },
        { key: "idp_sack_yards", label: "Sack Yards", defaultValue: 0 },
        { key: "idp_qb_hit", label: "Hit on QB", defaultValue: 1 },
        { key: "idp_tackle", label: "Tackle", defaultValue: 0 },
        { key: "idp_tackle_for_loss", label: "Tackle For Loss", defaultValue: 1 },
        { key: "idp_blocked_kick", label: "Blocked Punt, PAT or FG", defaultValue: 3 },
        { key: "idp_interception", label: "Interception", defaultValue: 4 },
        { key: "idp_int_return_yards", label: "INT Return Yards", defaultValue: 0 },
        { key: "idp_fumble_return_yards", label: "Fumble Return Yards", defaultValue: 0 },
        { key: "idp_fumble_forced", label: "Forced Fumble", defaultValue: 3 },
        { key: "idp_safety", label: "Safety", defaultValue: 2 },
        { key: "idp_assisted_tackle", label: "Assisted Tackle", defaultValue: 0.5 },
        { key: "idp_solo_tackle", label: "Solo Tackle", defaultValue: 1 },
        { key: "idp_pass_defended", label: "Pass Defended", defaultValue: 1 },
        { key: "idp_ten_tackle_bonus", label: "10+ Tackle Bonus", defaultValue: 0 },
        { key: "idp_two_sack_bonus", label: "2+ Sack Bonus", defaultValue: 0 },
        { key: "idp_three_pass_defended_bonus", label: "3+ Pass Defended Bonus", defaultValue: 0 },
        { key: "idp_fifty_yd_int_return_td_bonus", label: "50+ Yard INT Return TD Bonus", defaultValue: 0 }
      ]
    };
    NCAAF_PREMIUM_SCORING = {
      id: "advanced",
      label: "Advanced",
      rows: [
        { key: "air_yards", label: "Air Yards", premium: true, defaultValue: 0 },
        { key: "passing_air_yards", label: "Passing Air Yards", premium: true, defaultValue: 0 },
        { key: "receiving_air_yards", label: "Receiving Air Yards", premium: true, defaultValue: 0 },
        { key: "completed_air_yards", label: "Completed Air Yards", premium: true, defaultValue: 0 },
        { key: "explosive_play_bonus", label: "Explosive Play Bonus", premium: true, defaultValue: 0, helper: "20+ yard play bonus" },
        { key: "yards_after_contact", label: "Yards After Contact", premium: true, defaultValue: 0 },
        { key: "broken_tackles", label: "Broken Tackles", premium: true, defaultValue: 0 }
      ]
    };
    NCAAF_SCORING_CATEGORIES = [
      PASSING,
      RUSHING,
      RECEIVING,
      KICKING,
      TEAM_DEFENSE,
      SPECIAL_TEAMS,
      MISC,
      BONUS,
      IDP
    ];
    NCAAF_ALL_SCORING_CATEGORIES = [
      ...NCAAF_SCORING_CATEGORIES,
      NCAAF_PREMIUM_SCORING
    ];
  }
});

// lib/nba-scoring/NbaScoringCategories.ts
function buildFullNbaDefaultConfig() {
  const config = {};
  for (const cat of NBA_SCORING_CATEGORIES) {
    for (const row of cat.rows) {
      config[row.key] = row.defaultValue;
    }
  }
  for (const row of NBA_PREMIUM_SCORING.rows) {
    config[row.key] = row.defaultValue;
  }
  return config;
}
var NBA_SCORING_CATEGORIES, NBA_PREMIUM_SCORING;
var init_NbaScoringCategories = __esm({
  "lib/nba-scoring/NbaScoringCategories.ts"() {
    "use strict";
    NBA_SCORING_CATEGORIES = [
      {
        id: "general",
        label: "General",
        rows: [
          { key: "points_scored", label: "Points Scored", helper: "0.5 pts per point scored (AF default)", defaultValue: 0.5 },
          { key: "seconds_played", label: "Seconds Played", defaultValue: 0 },
          { key: "minutes_played", label: "Minutes Played", defaultValue: 0 },
          { key: "plus_minus", label: "Plus/Minus", defaultValue: 0 }
        ]
      },
      {
        id: "shooting",
        label: "Shooting",
        rows: [
          { key: "field_goals_made", label: "Field Goals Made", defaultValue: 0 },
          { key: "field_goals_attempted", label: "Field Goals Attempted", defaultValue: 0 },
          { key: "field_goals_missed", label: "Field Goals Missed", defaultValue: 0 },
          { key: "two_point_made", label: "2-Point Field Goals Made", defaultValue: 0 },
          { key: "two_point_attempted", label: "2-Point Field Goals Attempted", defaultValue: 0 },
          { key: "two_point_missed", label: "2-Point Field Goals Missed", defaultValue: 0 }
        ]
      },
      {
        id: "free_throws",
        label: "Free Throws",
        rows: [
          { key: "free_throws_made", label: "Free Throws Made", defaultValue: 0 },
          { key: "free_throws_attempted", label: "Free Throws Attempted", defaultValue: 0 },
          { key: "free_throws_missed", label: "Free Throws Missed", defaultValue: 0 }
        ]
      },
      {
        id: "three_point",
        label: "Three-Point",
        rows: [
          { key: "three_point_made", label: "3-Point Shots Made", helper: "0.5 bonus per made 3 (AF default)", defaultValue: 0.5 },
          { key: "three_point_attempted", label: "3-Point Shots Attempted", defaultValue: 0 },
          { key: "three_point_missed", label: "3-Point Shots Missed", defaultValue: 0 }
        ]
      },
      {
        id: "rebounds",
        label: "Rebounds",
        rows: [
          { key: "rebound", label: "Rebound", helper: "1 pt per total rebound (AF default)", defaultValue: 1 },
          { key: "offensive_rebound", label: "Offensive Rebound", defaultValue: 0 },
          { key: "defensive_rebound", label: "Defensive Rebound", defaultValue: 0 }
        ]
      },
      {
        id: "playmaking",
        label: "Playmaking",
        rows: [
          { key: "assist", label: "Assist", helper: "1 pt per assist (AF default)", defaultValue: 1 },
          { key: "turnover", label: "Turnover", defaultValue: -1 }
        ]
      },
      {
        id: "defense",
        label: "Defense",
        rows: [
          { key: "steal", label: "Steal", helper: "2 pts per steal (AF default)", defaultValue: 2 },
          { key: "block", label: "Block", helper: "2 pts per block (AF default)", defaultValue: 2 }
        ]
      },
      {
        id: "discipline",
        label: "Discipline",
        rows: [
          { key: "personal_foul", label: "Personal Foul", defaultValue: 0 },
          { key: "technical_foul", label: "Technical Foul", defaultValue: -2 },
          { key: "flagrant_foul", label: "Flagrant Foul", defaultValue: -2 }
        ]
      },
      {
        id: "bonuses",
        label: "Bonuses",
        rows: [
          { key: "double_double", label: "Double-Double", defaultValue: 1 },
          { key: "triple_double", label: "Triple-Double", defaultValue: 2 },
          { key: "forty_plus_points_bonus", label: "40+ Points Bonus", defaultValue: 2 },
          { key: "fifty_plus_points_bonus", label: "50+ Points Bonus", defaultValue: 2 },
          { key: "fifteen_plus_assists_bonus", label: "15+ Assists Bonus", defaultValue: 0 },
          { key: "twenty_plus_rebounds_bonus", label: "20+ Rebounds Bonus", defaultValue: 0 },
          { key: "ten_plus_fg_bonus", label: "10+ Made Field Goals Bonus", defaultValue: 0 },
          { key: "five_plus_threes_bonus", label: "5+ Made 3PT Bonus", defaultValue: 0 }
        ]
      }
    ];
    NBA_PREMIUM_SCORING = {
      id: "advanced",
      label: "Advanced",
      rows: [
        { key: "usage_rate_bonus", label: "Usage Rate Bonus", premium: true, helper: "Bonus based on usage rate thresholds", defaultValue: 0 },
        { key: "efficiency_bonus", label: "Efficiency Bonus", premium: true, helper: "Bonus based on game efficiency rating", defaultValue: 0 },
        { key: "true_shooting_bonus", label: "True Shooting Bonus", premium: true, helper: "Bonus for high true shooting percentage games", defaultValue: 0 },
        { key: "assist_turnover_bonus", label: "Assist-to-Turnover Bonus", premium: true, helper: "Bonus for high assist-to-turnover ratio games", defaultValue: 0 }
      ]
    };
  }
});

// lib/ncaab-scoring/NcaabScoringCategories.ts
function buildFullNcaabDefaultConfig() {
  const config = {};
  for (const cat of NCAAB_SCORING_CATEGORIES) {
    for (const row of cat.rows) config[row.key] = row.defaultValue;
  }
  for (const row of NCAAB_PREMIUM_SCORING.rows) config[row.key] = row.defaultValue;
  return config;
}
var NCAAB_SCORING_CATEGORIES, NCAAB_PREMIUM_SCORING;
var init_NcaabScoringCategories = __esm({
  "lib/ncaab-scoring/NcaabScoringCategories.ts"() {
    "use strict";
    NCAAB_SCORING_CATEGORIES = [
      {
        id: "general",
        label: "General",
        rows: [
          { key: "points_scored", label: "Points Scored", helper: "0.5 pts per point scored (AF default)", defaultValue: 0.5 },
          { key: "minutes_played", label: "Minutes Played", defaultValue: 0 },
          { key: "plus_minus", label: "Plus/Minus", defaultValue: 0 }
        ]
      },
      {
        id: "shooting",
        label: "Shooting",
        rows: [
          { key: "field_goals_made", label: "Field Goals Made", defaultValue: 0 },
          { key: "field_goals_attempted", label: "Field Goals Attempted", defaultValue: 0 },
          { key: "field_goals_missed", label: "Field Goals Missed", defaultValue: 0 },
          { key: "two_point_made", label: "2-Point Field Goals Made", defaultValue: 0 },
          { key: "two_point_attempted", label: "2-Point Field Goals Attempted", defaultValue: 0 },
          { key: "two_point_missed", label: "2-Point Field Goals Missed", defaultValue: 0 }
        ]
      },
      {
        id: "free_throws",
        label: "Free Throws",
        rows: [
          { key: "free_throws_made", label: "Free Throws Made", defaultValue: 0 },
          { key: "free_throws_attempted", label: "Free Throws Attempted", defaultValue: 0 },
          { key: "free_throws_missed", label: "Free Throws Missed", defaultValue: 0 }
        ]
      },
      {
        id: "three_point",
        label: "Three-Point",
        rows: [
          { key: "three_point_made", label: "3-Point Shots Made", helper: "0.5 bonus per made 3 (AF default)", defaultValue: 0.5 },
          { key: "three_point_attempted", label: "3-Point Shots Attempted", defaultValue: 0 },
          { key: "three_point_missed", label: "3-Point Shots Missed", defaultValue: 0 }
        ]
      },
      {
        id: "rebounds",
        label: "Rebounds",
        rows: [
          { key: "rebound", label: "Rebound", helper: "1 pt per total rebound (AF default)", defaultValue: 1 },
          { key: "offensive_rebound", label: "Offensive Rebound", defaultValue: 0 },
          { key: "defensive_rebound", label: "Defensive Rebound", defaultValue: 0 }
        ]
      },
      {
        id: "playmaking",
        label: "Playmaking",
        rows: [
          { key: "assist", label: "Assist", helper: "1 pt per assist (AF default)", defaultValue: 1 },
          { key: "turnover", label: "Turnover", defaultValue: -1 }
        ]
      },
      {
        id: "defense",
        label: "Defense",
        rows: [
          { key: "steal", label: "Steal", helper: "2 pts per steal (AF default)", defaultValue: 2 },
          { key: "block", label: "Block", helper: "2 pts per block (AF default)", defaultValue: 2 }
        ]
      },
      {
        id: "discipline",
        label: "Discipline",
        rows: [
          { key: "personal_foul", label: "Personal Foul", defaultValue: -0.5 },
          { key: "technical_foul", label: "Technical Foul", defaultValue: -2 },
          { key: "flagrant_foul", label: "Flagrant Foul", defaultValue: -2 }
        ]
      },
      {
        id: "bonuses",
        label: "Bonuses",
        rows: [
          { key: "double_double", label: "Double-Double", defaultValue: 1 },
          { key: "triple_double", label: "Triple-Double", helper: "Rare in college but supported", defaultValue: 2 },
          { key: "twenty_plus_points_bonus", label: "20+ Points Bonus", defaultValue: 1 },
          { key: "thirty_plus_points_bonus", label: "30+ Points Bonus", defaultValue: 2 },
          { key: "forty_plus_points_bonus", label: "40+ Points Bonus", defaultValue: 0 },
          { key: "ten_plus_assists_bonus", label: "10+ Assists Bonus", defaultValue: 0 },
          { key: "fifteen_plus_rebounds_bonus", label: "15+ Rebounds Bonus", defaultValue: 0 },
          { key: "ten_plus_rebounds_bonus", label: "10+ Rebounds Bonus", defaultValue: 0 },
          { key: "five_plus_threes_bonus", label: "5+ Made 3PT Bonus", defaultValue: 0 }
        ]
      }
    ];
    NCAAB_PREMIUM_SCORING = {
      id: "advanced",
      label: "Advanced",
      rows: [
        { key: "usage_rate_bonus", label: "Usage Rate Bonus", premium: true, helper: "Bonus based on usage rate thresholds", defaultValue: 0 },
        { key: "efficiency_bonus", label: "Efficiency Bonus", premium: true, helper: "Bonus based on game efficiency rating", defaultValue: 0 },
        { key: "tempo_adjusted_bonus", label: "Tempo-Adjusted Bonus", premium: true, helper: "Bonus adjusted for pace of play (college-specific)", defaultValue: 0 },
        { key: "assist_turnover_bonus", label: "Assist-to-Turnover Bonus", premium: true, helper: "Bonus for high assist-to-turnover ratio games", defaultValue: 0 }
      ]
    };
  }
});

// lib/mlb-scoring/MlbScoringCategories.ts
function buildMlbScoringDefaults() {
  const defaults = {};
  for (const cat of MLB_ALL_SCORING_CATEGORIES) {
    for (const row of cat.rows) {
      if (!(row.key in defaults)) defaults[row.key] = MLB_AVAILABLE_SCORING_KEYS.has(row.key) ? row.defaultValue : 0;
    }
  }
  return defaults;
}
var BATTING, POWER, DISCIPLINE, BASE_RUNNING, PITCHING, RESULTS, EFFICIENCY, CONTROL, BONUSES, MISC2, MLB_PREMIUM_SCORING, MLB_SCORING_CATEGORIES, MLB_ALL_SCORING_CATEGORIES;
var init_MlbScoringCategories = __esm({
  "lib/mlb-scoring/MlbScoringCategories.ts"() {
    "use strict";
    init_MlbScoringPresets();
    BATTING = {
      id: "batting",
      label: "Batting",
      group: "hitting",
      rows: [
        { key: "plate_appearances", label: "Plate Appearances", defaultValue: 0 },
        { key: "at_bats", label: "At Bats", defaultValue: 0 },
        { key: "runs", label: "Runs", defaultValue: 1 },
        { key: "singles", label: "Singles", defaultValue: 1 },
        { key: "doubles", label: "Doubles", defaultValue: 2 },
        { key: "triples", label: "Triples", defaultValue: 3 },
        { key: "home_runs", label: "Home Runs", defaultValue: 4 },
        { key: "total_bases", label: "Total Bases", helper: "Total bases accumulated (1B=1, 2B=2, 3B=3, HR=4)", defaultValue: 0 },
        { key: "rbis", label: "RBIs", defaultValue: 1 },
        { key: "sacrifice_flies", label: "Sacrifice Flies", defaultValue: 0 },
        { key: "sacrifice_bunts", label: "Sacrifice Bunts", defaultValue: 0 }
      ]
    };
    POWER = {
      id: "power",
      label: "Power",
      group: "hitting",
      rows: [
        { key: "grand_slam_bonus", label: "Grand Slam Bonus", helper: "Extra bonus per grand slam (on top of home_runs value)", defaultValue: 1 },
        { key: "cycle_bonus", label: "Hit For Cycle Bonus", defaultValue: 5 },
        { key: "game_winning_rbi_bonus", label: "Game-Winning RBI Bonus", defaultValue: 0 },
        { key: "two_hr_game_bonus", label: "2 HR Game Bonus", defaultValue: 0 },
        { key: "three_hr_game_bonus", label: "3 HR Game Bonus", defaultValue: 0 },
        { key: "five_rbi_game_bonus", label: "5 RBI Game Bonus", defaultValue: 0 },
        { key: "ten_tb_game_bonus", label: "10 Total Bases Game Bonus", defaultValue: 0 }
      ]
    };
    DISCIPLINE = {
      id: "discipline",
      label: "Discipline",
      group: "hitting",
      rows: [
        { key: "walks", label: "Walk (BB)", defaultValue: 1 },
        { key: "intentional_walks", label: "Intentional Walk", defaultValue: 0 },
        { key: "hit_by_pitch", label: "Hit By Pitch", defaultValue: 1 },
        { key: "strikeouts", label: "Strikeout (K)", defaultValue: -1 }
      ]
    };
    BASE_RUNNING = {
      id: "base_running",
      label: "Base Running",
      group: "hitting",
      rows: [
        { key: "stolen_bases", label: "Stolen Base", defaultValue: 2 },
        { key: "caught_stealing", label: "Caught Stealing", defaultValue: -1 },
        { key: "multi_steal_game_bonus", label: "Multi-Steal Game Bonus", defaultValue: 0 },
        { key: "ground_into_double_play", label: "Ground Into Double Play", defaultValue: 0 }
      ]
    };
    PITCHING = {
      id: "pitching",
      label: "Pitching",
      group: "pitching",
      rows: [
        { key: "outs_recorded", label: "Outs Recorded", helper: "Points per out recorded", defaultValue: 0 },
        { key: "innings_pitched", label: "Innings Pitched", helper: "Points per inning pitched", defaultValue: 3 },
        { key: "hits_allowed", label: "Hits Allowed", defaultValue: -1 },
        { key: "earned_runs", label: "Earned Runs", defaultValue: -2 },
        { key: "runs_allowed", label: "Runs Allowed", defaultValue: 0 },
        { key: "walks_allowed", label: "Walks Allowed", defaultValue: -1 },
        { key: "hit_batters", label: "Hit Batters", defaultValue: -1 },
        { key: "home_runs_allowed", label: "Home Runs Allowed", defaultValue: 0 },
        { key: "wild_pitches", label: "Wild Pitches", defaultValue: 0 },
        { key: "balks", label: "Balks", defaultValue: 0 },
        { key: "pickoffs", label: "Pickoffs", defaultValue: 0 }
      ]
    };
    RESULTS = {
      id: "results",
      label: "Results",
      group: "pitching",
      rows: [
        { key: "wins", label: "Win", defaultValue: 5 },
        { key: "losses", label: "Loss", defaultValue: -5 },
        { key: "saves", label: "Save", defaultValue: 5 },
        { key: "holds", label: "Hold", defaultValue: 3 },
        { key: "save_opportunities", label: "Save Opportunity", defaultValue: 0 },
        { key: "blown_saves", label: "Blown Save", defaultValue: -2 },
        { key: "complete_games", label: "Complete Game", defaultValue: 3 },
        { key: "complete_game_shutouts", label: "Shutout", defaultValue: 5 },
        { key: "no_hitters", label: "No-Hitter", defaultValue: 10 },
        { key: "perfect_games", label: "Perfect Game", defaultValue: 0 }
      ]
    };
    EFFICIENCY = {
      id: "efficiency",
      label: "Efficiency",
      group: "pitching",
      rows: [
        { key: "quality_starts", label: "Quality Start", defaultValue: 3 },
        { key: "pitcher_ten_k_game", label: "10+ Strikeout Game Bonus", defaultValue: 0 },
        { key: "pitcher_fifteen_k_game", label: "15+ Strikeout Game Bonus", defaultValue: 0 },
        { key: "cg_shutout_bonus", label: "CG Shutout Bonus", defaultValue: 0 },
        { key: "perfect_game_bonus", label: "Perfect Game Bonus", defaultValue: 0 }
      ]
    };
    CONTROL = {
      id: "control",
      label: "Control",
      group: "pitching",
      rows: [
        { key: "pitch_strikeouts", label: "Strikeouts (Pitcher)", defaultValue: 1 },
        { key: "walk_penalty", label: "Walk Penalty", helper: "Additional per-walk penalty on top of walks_allowed", defaultValue: 0 },
        { key: "k_bonus", label: "Strikeout Bonus", helper: '"K-boom" extra per strikeout after 8', defaultValue: 0 }
      ]
    };
    BONUSES = {
      id: "bonuses",
      label: "Bonuses",
      group: "global",
      rows: [
        { key: "two_hr_game_bonus", label: "2 HR Game", defaultValue: 0 },
        { key: "three_hr_game_bonus", label: "3 HR Game", defaultValue: 0 },
        { key: "five_rbi_game_bonus", label: "5 RBI Game", defaultValue: 0 },
        { key: "ten_tb_game_bonus", label: "10 Total Bases Game", defaultValue: 0 },
        { key: "multi_steal_game_bonus", label: "Multi-Steal Game", defaultValue: 0 },
        { key: "pitcher_ten_k_game", label: "10+ Strikeout Game", defaultValue: 0 },
        { key: "pitcher_fifteen_k_game", label: "15+ Strikeout Game", defaultValue: 0 },
        { key: "cg_shutout_bonus", label: "CG Shutout Bonus", defaultValue: 0 },
        { key: "perfect_game_bonus", label: "Perfect Game Bonus", defaultValue: 0 },
        { key: "cycle_bonus", label: "Hit For Cycle", defaultValue: 5 }
      ]
    };
    MISC2 = {
      id: "misc",
      label: "Misc",
      group: "global",
      rows: [
        { key: "fielding_error", label: "Error", defaultValue: 0 },
        { key: "ground_into_double_play", label: "Ground Into Double Play", defaultValue: 0 },
        { key: "pickoffs", label: "Pickoff", defaultValue: 0 },
        { key: "balks", label: "Balk", defaultValue: 0 },
        { key: "wild_pitches", label: "Wild Pitch", defaultValue: 0 }
      ]
    };
    MLB_PREMIUM_SCORING = {
      id: "advanced",
      label: "Advanced",
      group: "premium",
      rows: [
        { key: "ops_bonus", label: "OPS Bonus", premium: true, helper: "Bonus points per 0.100 OPS above .700", defaultValue: 0 },
        { key: "woba_bonus", label: "wOBA Bonus", premium: true, helper: "Weighted On-Base Average bonus points", defaultValue: 0 },
        { key: "wrc_plus_bonus", label: "wRC+ Bonus", premium: true, helper: "Weighted Runs Created+ bonus (100 = league avg)", defaultValue: 0 },
        { key: "fip_bonus", label: "FIP Bonus", premium: true, helper: "Fielding Independent Pitching efficiency bonus", defaultValue: 0 },
        { key: "xera_bonus", label: "xERA Bonus", premium: true, helper: "Expected ERA efficiency bonus", defaultValue: 0 },
        { key: "babip_bonus", label: "BABIP Bonus", premium: true, helper: "Batting Average on Balls In Play bonus", defaultValue: 0 },
        { key: "barrel_rate_bonus", label: "Barrel Rate Bonus", premium: true, helper: "Bonus per barrel (statcast exit velocity + angle)", defaultValue: 0 },
        { key: "exit_velocity_bonus", label: "Exit Velocity Bonus", premium: true, helper: "Bonus per 1 mph above 95 mph exit velocity", defaultValue: 0 },
        { key: "launch_angle_bonus", label: "Launch Angle Bonus", premium: true, helper: "Bonus for optimal launch angle (8\u201332\xB0)", defaultValue: 0 }
      ]
    };
    MLB_SCORING_CATEGORIES = [
      // Hitting
      BATTING,
      POWER,
      DISCIPLINE,
      BASE_RUNNING,
      // Pitching
      PITCHING,
      RESULTS,
      EFFICIENCY,
      CONTROL,
      // Global
      BONUSES,
      MISC2
    ];
    MLB_ALL_SCORING_CATEGORIES = [
      ...MLB_SCORING_CATEGORIES,
      MLB_PREMIUM_SCORING
    ];
  }
});

// lib/nhl-scoring/NhlScoringCategories.ts
function buildNhlScoringDefaults() {
  const defaults = {};
  for (const cat of NHL_ALL_SCORING_CATEGORIES) {
    for (const row of cat.rows) {
      if (!(row.key in defaults)) defaults[row.key] = row.defaultValue;
    }
  }
  return defaults;
}
var OFFENSE, DEFENSE, DISCIPLINE2, GOALIE_CORE, GOALIE_EFFICIENCY, GOALIE_BONUSES, BONUSES2, MISC3, NHL_PREMIUM_SCORING, NHL_SCORING_CATEGORIES, NHL_ALL_SCORING_CATEGORIES;
var init_NhlScoringCategories = __esm({
  "lib/nhl-scoring/NhlScoringCategories.ts"() {
    "use strict";
    OFFENSE = {
      id: "offense",
      label: "Offense",
      group: "skaters",
      rows: [
        { key: "goals", label: "Goal", defaultValue: 3 },
        { key: "assists", label: "Assist", defaultValue: 2 },
        { key: "points", label: "Point (combined)", helper: "Optional combined goal+assist stat", defaultValue: 0 },
        { key: "power_play_goals", label: "Power Play Goal", helper: "Bonus on top of goal value", defaultValue: 1 },
        { key: "power_play_assists", label: "Power Play Assist", helper: "Bonus on top of assist value", defaultValue: 0.5 },
        { key: "power_play_points", label: "Power Play Point", helper: "Use instead of individual PP goal/assist", defaultValue: 0 },
        { key: "short_handed_goals", label: "Short-Handed Goal", helper: "Bonus on top of goal value", defaultValue: 2 },
        { key: "short_handed_assists", label: "Short-Handed Assist", helper: "Bonus on top of assist value", defaultValue: 1 },
        { key: "short_handed_points", label: "Short-Handed Point", helper: "Use instead of individual SHG/SHA", defaultValue: 0 },
        { key: "game_winning_goals", label: "Game-Winning Goal", helper: "Bonus on top of goal value", defaultValue: 1 },
        { key: "overtime_goals", label: "Overtime Goal", defaultValue: 0 },
        { key: "shots_on_goal", label: "Shots on Goal", defaultValue: 0.5 },
        { key: "shooting_pct_bonus", label: "Shooting % Bonus", helper: "Bonus per point of shooting percentage above 10%", defaultValue: 0 },
        { key: "faceoff_wins", label: "Faceoff Wins", defaultValue: 0 },
        { key: "faceoff_losses", label: "Faceoff Losses", defaultValue: 0 }
      ]
    };
    DEFENSE = {
      id: "defense",
      label: "Defense",
      group: "skaters",
      rows: [
        { key: "blocked_shots", label: "Blocked Shots", defaultValue: 0.5 },
        { key: "hits", label: "Hits", defaultValue: 0.3 },
        { key: "takeaways", label: "Takeaways", defaultValue: 0.5 },
        { key: "giveaways", label: "Giveaways", defaultValue: -0.5 },
        { key: "plus_minus", label: "Plus/Minus", defaultValue: 1 }
      ]
    };
    DISCIPLINE2 = {
      id: "discipline",
      label: "Discipline",
      group: "skaters",
      rows: [
        { key: "penalty_minutes", label: "Penalty Minutes", helper: "Per penalty minute (negative recommended)", defaultValue: -0.25 },
        { key: "minor_penalty", label: "Minor Penalty", helper: "Per 2-minute minor", defaultValue: 0 },
        { key: "major_penalty", label: "Major Penalty", helper: "Per 5-minute major", defaultValue: 0 },
        { key: "misconduct", label: "Misconduct", helper: "Per 10-minute misconduct", defaultValue: 0 }
      ]
    };
    GOALIE_CORE = {
      id: "goalie_core",
      label: "Goalie Core",
      group: "goalies",
      rows: [
        { key: "goalie_wins", label: "Win", defaultValue: 5 },
        { key: "goalie_losses", label: "Loss", defaultValue: -3 },
        { key: "overtime_losses", label: "Overtime/Shootout Loss", defaultValue: 0 },
        { key: "saves", label: "Save", defaultValue: 0.2 },
        { key: "goals_against", label: "Goal Against", defaultValue: -1 },
        { key: "shots_against", label: "Shots Against", helper: "Use instead of separate saves/GA", defaultValue: 0 },
        { key: "empty_net_goals_against", label: "Empty Net Goal Against", defaultValue: 0 },
        { key: "goalie_goals", label: "Goalie Goal", defaultValue: 0 },
        { key: "goalie_assists", label: "Goalie Assist", defaultValue: 0 },
        { key: "goalie_penalty_minutes", label: "Goalie Penalty Minutes", defaultValue: 0 }
      ]
    };
    GOALIE_EFFICIENCY = {
      id: "goalie_efficiency",
      label: "Goalie Efficiency",
      group: "goalies",
      rows: [
        { key: "shutouts", label: "Shutout", defaultValue: 5 },
        { key: "save_pct_bonus", label: "Save % Bonus", helper: "Bonus per 0.010 save % above .900", defaultValue: 0 },
        { key: "gaa_bonus", label: "GAA Bonus", helper: "Bonus for GAA below 2.50", defaultValue: 0 },
        { key: "goalie_minutes_played", label: "Time on Ice (minutes)", helper: "Points per minute played", defaultValue: 0 }
      ]
    };
    GOALIE_BONUSES = {
      id: "goalie_bonuses",
      label: "Goalie Bonuses",
      group: "goalies",
      rows: [
        { key: "forty_save_game", label: "40+ Save Game", defaultValue: 2 },
        { key: "fifty_save_game", label: "50+ Save Game", defaultValue: 3 },
        { key: "overtime_win", label: "Overtime Win", defaultValue: 0 },
        { key: "shootout_win", label: "Shootout Win", defaultValue: 0 }
      ]
    };
    BONUSES2 = {
      id: "bonuses",
      label: "Bonuses",
      group: "global",
      rows: [
        { key: "hat_trick_bonus", label: "Hat Trick", defaultValue: 3 },
        { key: "gordie_howe_hat_trick_bonus", label: "Gordie Howe Hat Trick", helper: "Goal + Assist + Fight in same game", defaultValue: 0 },
        { key: "three_assist_game", label: "3+ Assist Game", defaultValue: 0 },
        { key: "five_point_game", label: "5+ Point Game", defaultValue: 0 },
        { key: "multi_goal_game", label: "Multi-Goal Game (2+)", defaultValue: 0 },
        { key: "multi_assist_game", label: "Multi-Assist Game (2+)", defaultValue: 0 },
        { key: "ten_shot_game", label: "10+ Shot Game", defaultValue: 0 },
        { key: "five_hit_game", label: "5+ Hit Game", defaultValue: 0 },
        { key: "five_block_game", label: "5+ Block Game", defaultValue: 0 },
        { key: "forty_save_game", label: "40+ Save Game", defaultValue: 2 },
        { key: "fifty_save_game", label: "50+ Save Game", defaultValue: 3 }
      ]
    };
    MISC3 = {
      id: "misc",
      label: "Misc",
      group: "global",
      rows: [
        { key: "empty_net_goal", label: "Empty Net Goal", helper: "Skater scores into empty net", defaultValue: 0 },
        { key: "shootout_goal", label: "Shootout Goal", defaultValue: 0 },
        { key: "shootout_miss", label: "Shootout Miss", defaultValue: 0 },
        { key: "overtime_goals", label: "Overtime Goal", defaultValue: 0 }
      ]
    };
    NHL_PREMIUM_SCORING = {
      id: "advanced",
      label: "Advanced",
      group: "premium",
      rows: [
        { key: "corsi_bonus", label: "Corsi Bonus", premium: true, helper: "Shot attempt differential bonus (CF%)", defaultValue: 0 },
        { key: "fenwick_bonus", label: "Fenwick Bonus", premium: true, helper: "Unblocked shot attempt differential (FF%)", defaultValue: 0 },
        { key: "xg_bonus", label: "Expected Goals (xG)", premium: true, helper: "Bonus per expected goal generated", defaultValue: 0 },
        { key: "xa_bonus", label: "Expected Assists (xA)", premium: true, helper: "Bonus per expected primary assist", defaultValue: 0 },
        { key: "high_danger_chances", label: "High Danger Chances", premium: true, helper: "Bonus per high-danger scoring chance", defaultValue: 0 },
        { key: "zone_starts_bonus", label: "Zone Starts Bonus", premium: true, helper: "Offensive zone start percentage bonus", defaultValue: 0 },
        { key: "goalie_gsaa", label: "Goalie GSAA", premium: true, helper: "Goals Saved Above Average bonus", defaultValue: 0 },
        { key: "goalie_xga", label: "Goalie xGA", premium: true, helper: "Expected Goals Against \u2014 bonus for outperforming", defaultValue: 0 }
      ]
    };
    NHL_SCORING_CATEGORIES = [
      // Skaters
      OFFENSE,
      DEFENSE,
      DISCIPLINE2,
      // Goalies
      GOALIE_CORE,
      GOALIE_EFFICIENCY,
      GOALIE_BONUSES,
      // Global
      BONUSES2,
      MISC3
    ];
    NHL_ALL_SCORING_CATEGORIES = [
      ...NHL_SCORING_CATEGORIES,
      NHL_PREMIUM_SCORING
    ];
  }
});

// lib/soccer-scoring/SoccerScoringCategories.ts
function buildSoccerDefaultRulesFromCategories() {
  return Object.fromEntries(
    SOCCER_SCORING_CATEGORIES.flatMap(
      (c) => c.rows.map((r) => [r.key, r.defaultValue])
    )
  );
}
var OUTFIELD_ATTACKING, GOALKEEPING, DISCIPLINE3, BONUSES3, MISC4, ADVANCED, SOCCER_SCORING_CATEGORIES, SOCCER_SCORING_CATEGORY_IDS, ALL_SOCCER_SCORING_KEYS;
var init_SoccerScoringCategories = __esm({
  "lib/soccer-scoring/SoccerScoringCategories.ts"() {
    "use strict";
    OUTFIELD_ATTACKING = {
      id: "outfield",
      label: "Outfield",
      group: "outfield",
      rows: [
        // ── Attacking ──────────────────────────────────────────────────────────
        { key: "goal", label: "Goal", defaultValue: 6 },
        { key: "assist", label: "Assist", defaultValue: 3 },
        { key: "shot_on_target", label: "Shot on Target", helper: "Excludes goals", defaultValue: 0.5 },
        { key: "shot", label: "Shot (any)", helper: "All shots taken", defaultValue: 0.2 },
        { key: "key_pass", label: "Key Pass", helper: "Pass leading to a shot", defaultValue: 0.5 },
        { key: "big_chance_created", label: "Big Chance Created", helper: "Clear goal-scoring opportunity created", defaultValue: 0 },
        { key: "through_ball", label: "Through Ball", defaultValue: 0 },
        { key: "dribble_success", label: "Successful Dribble", helper: "Completed take-on", defaultValue: 0 },
        { key: "cross_accurate", label: "Accurate Cross", defaultValue: 0 },
        { key: "penalty_scored", label: "Penalty Scored", helper: "On top of goal value", defaultValue: 6 },
        { key: "penalty_missed", label: "Penalty Missed", defaultValue: -2 },
        // ── Defending ──────────────────────────────────────────────────────────
        { key: "clean_sheet", label: "Clean Sheet (DEF/MID)", helper: "Outfield player 0 GA at 60+ min", defaultValue: 4 },
        { key: "goal_conceded", label: "Goal Conceded", helper: "Per goal while outfield player is on pitch", defaultValue: -1 },
        { key: "tackle_won", label: "Tackle Won", defaultValue: 0.3 },
        { key: "interception", label: "Interception", defaultValue: 0.3 },
        { key: "clearance", label: "Clearance", defaultValue: 0 },
        { key: "blocked_shot", label: "Blocked Shot", defaultValue: 0 },
        { key: "aerial_won", label: "Aerial Duel Won", defaultValue: 0 },
        { key: "own_goal", label: "Own Goal", defaultValue: -2 }
      ]
    };
    GOALKEEPING = {
      id: "goalkeeping",
      label: "Goalkeeping",
      group: "goalkeeping",
      rows: [
        { key: "gk_save", label: "Save", defaultValue: 0.5 },
        { key: "gk_penalty_save", label: "Penalty Save", defaultValue: 5 },
        { key: "gk_goals_against", label: "Goal Conceded (GK)", helper: "Per goal let in", defaultValue: -1 },
        { key: "gk_clean_sheet", label: "Clean Sheet (GK)", defaultValue: 4 },
        { key: "gk_high_claim", label: "High Claim", helper: "Claiming a high ball", defaultValue: 0 },
        { key: "gk_punch", label: "Punch / Claim", helper: "Successfully punching a cross", defaultValue: 0 },
        { key: "gk_save_inside_box", label: "Save Inside the Box", helper: "Difficult close-range save", defaultValue: 0 }
      ]
    };
    DISCIPLINE3 = {
      id: "discipline",
      label: "Discipline",
      group: "discipline",
      rows: [
        { key: "yellow_card", label: "Yellow Card", defaultValue: -1 },
        { key: "red_card", label: "Red Card", helper: "Automatic suspension", defaultValue: -3 },
        { key: "foul_committed", label: "Foul Committed", defaultValue: 0 },
        { key: "foul_drawn", label: "Foul Drawn", defaultValue: 0 },
        { key: "offside", label: "Offside", defaultValue: 0 }
      ]
    };
    BONUSES3 = {
      id: "bonuses",
      label: "Bonuses",
      group: "bonuses",
      rows: [
        { key: "hat_trick_bonus", label: "Hat Trick Bonus", helper: "Extra points for 3+ goals in a match", defaultValue: 3 },
        { key: "man_of_match", label: "Man of the Match", helper: "AFC MOTM award", defaultValue: 0 },
        { key: "rating_bonus_7plus", label: "Rating 7.0+ Bonus", helper: "Whoscored / Sofascore match rating \u22657.0", defaultValue: 0 },
        { key: "rating_bonus_8plus", label: "Rating 8.0+ Bonus", helper: "Whoscored / Sofascore match rating \u22658.0", defaultValue: 0 }
      ]
    };
    MISC4 = {
      id: "misc",
      label: "Misc",
      group: "misc",
      rows: [
        { key: "minutes_played", label: "Minutes Played", helper: "Per minute on pitch", defaultValue: 0.02 },
        { key: "appearance", label: "Appearance Bonus", helper: "Flat points for any appearance", defaultValue: 0 },
        { key: "sub_on", label: "Substitute On", helper: "Flat bonus for coming on as sub", defaultValue: 0 },
        { key: "sub_off", label: "Substitute Off", helper: "Flat modifier for being substituted off", defaultValue: 0 }
      ]
    };
    ADVANCED = {
      id: "advanced",
      label: "Advanced",
      group: "advanced",
      rows: [
        { key: "xg_bonus", label: "xG Bonus", helper: "Points per expected goal (xG) produced", premium: true, defaultValue: 0 },
        { key: "xa_bonus", label: "xA Bonus", helper: "Points per expected assist (xA)", premium: true, defaultValue: 0 },
        { key: "xgi_bonus", label: "xGI Bonus", helper: "Points per xG + xA (goal involvement)", premium: true, defaultValue: 0 },
        { key: "post_shot_xg", label: "Post-Shot xG", helper: "Shot quality metric \u2014 actual shot xG", premium: true, defaultValue: 0 },
        { key: "progressive_passes", label: "Progressive Pass", helper: "Pass that moves the ball significantly toward goal", premium: true, defaultValue: 0 },
        { key: "progressive_carries", label: "Progressive Carry", helper: "Ball carry significantly toward opponent goal", premium: true, defaultValue: 0 },
        { key: "shot_creating_actions", label: "Shot-Creating Action (SCA)", helper: "Any action leading directly to a shot", premium: true, defaultValue: 0 },
        { key: "gk_psxg", label: "GK Post-Shot xG Saved (PSxG)", helper: "GK performance vs. quality of shots faced", premium: true, defaultValue: 0 }
      ]
    };
    SOCCER_SCORING_CATEGORIES = [
      OUTFIELD_ATTACKING,
      GOALKEEPING,
      DISCIPLINE3,
      BONUSES3,
      MISC4,
      ADVANCED
    ];
    SOCCER_SCORING_CATEGORY_IDS = SOCCER_SCORING_CATEGORIES.map((c) => c.id);
    ALL_SOCCER_SCORING_KEYS = SOCCER_SCORING_CATEGORIES.flatMap(
      (c) => c.rows.map((r) => r.key)
    );
  }
});

// lib/league/scoring-stat-metadata.ts
function flattenCategories(categories) {
  const map = /* @__PURE__ */ new Map();
  categories.forEach((cat, categoryOrder) => {
    for (const row of cat.rows) {
      if (!map.has(row.key)) {
        map.set(row.key, {
          label: row.label,
          categoryOrder,
          categoryTitle: cat.label
        });
      }
    }
  });
  return map;
}
function templateStatKeyFromUiKey(sport, uiKey) {
  const u = sport.toUpperCase();
  const aliases = SCORING_STAT_KEY_ALIASES[u] ?? {};
  for (const [templateKey, ui] of Object.entries(aliases)) {
    if (ui === uiKey) return templateKey;
  }
  return uiKey;
}
var NFL_MAP, NCAAF_MAP, NBA_MAP, NCAAB_MAP, MLB_MAP, NHL_MAP, SOCCER_MAP, SCORING_STAT_KEY_ALIASES;
var init_scoring_stat_metadata = __esm({
  "lib/league/scoring-stat-metadata.ts"() {
    "use strict";
    init_NflScoringCategories();
    init_NcaafScoringCategories();
    init_NbaScoringCategories();
    init_NcaabScoringCategories();
    init_MlbScoringCategories();
    init_NhlScoringCategories();
    init_SoccerScoringCategories();
    NFL_MAP = flattenCategories([
      ...NFL_SCORING_CATEGORIES,
      NFL_PREMIUM_SCORING
    ]);
    NCAAF_MAP = flattenCategories(NCAAF_ALL_SCORING_CATEGORIES);
    NBA_MAP = flattenCategories([
      ...NBA_SCORING_CATEGORIES,
      NBA_PREMIUM_SCORING
    ]);
    NCAAB_MAP = flattenCategories([
      ...NCAAB_SCORING_CATEGORIES,
      NCAAB_PREMIUM_SCORING
    ]);
    MLB_MAP = flattenCategories(MLB_ALL_SCORING_CATEGORIES);
    NHL_MAP = flattenCategories(NHL_ALL_SCORING_CATEGORIES);
    SOCCER_MAP = flattenCategories(SOCCER_SCORING_CATEGORIES);
    SCORING_STAT_KEY_ALIASES = {
      NFL: {
        interception: "interception_thrown",
        receptions: "reception",
        dst_points_allowed_0: "dst_pa_0",
        dst_points_allowed_1_6: "dst_pa_1_6",
        dst_points_allowed_7_13: "dst_pa_7_13",
        dst_points_allowed_14_20: "dst_pa_14_20",
        dst_points_allowed_21_27: "dst_pa_21_27",
        dst_points_allowed_28_34: "dst_pa_28_34",
        dst_points_allowed_35_plus: "dst_pa_35_plus"
      },
      NCAAF: {
        interception: "interception_thrown",
        receptions: "reception",
        dst_points_allowed_0: "dst_pa_0",
        dst_points_allowed_1_6: "dst_pa_1_6",
        dst_points_allowed_7_13: "dst_pa_7_13",
        dst_points_allowed_14_20: "dst_pa_14_20",
        dst_points_allowed_21_27: "dst_pa_21_27",
        dst_points_allowed_28_34: "dst_pa_28_34",
        dst_points_allowed_35_plus: "dst_pa_35_plus"
      },
      NBA: {
        points: "points_scored",
        rebounds: "rebound",
        assists: "assist",
        steals: "steal",
        blocks: "block",
        turnovers: "turnover",
        three_pointers_made: "three_point_made"
      },
      NCAAB: {
        points: "points_scored",
        rebounds: "rebound",
        assists: "assist",
        steals: "steal",
        blocks: "block",
        turnovers: "turnover",
        three_pointers_made: "three_point_made"
      },
      MLB: {
        single: "singles",
        double: "doubles",
        triple: "triples",
        home_run: "home_runs",
        rbi: "rbis",
        run: "runs",
        walk: "walks",
        stolen_base: "stolen_bases",
        hit_by_pitch: "hit_by_pitch",
        strikeout: "strikeouts",
        innings_pitched: "innings_pitched",
        earned_runs: "earned_runs",
        strikeouts_pitched: "strikeouts_pitched",
        save: "saves",
        hold: "holds",
        win: "wins",
        loss: "losses",
        quality_start: "quality_starts"
      },
      NHL: {
        goal: "goals",
        assist: "assists",
        shot_on_goal: "shots_on_goal",
        blocked_shot: "blocked_shots",
        power_play_point: "power_play_points",
        short_handed_point: "short_handed_points",
        save: "saves",
        goal_allowed: "goals_against",
        win: "goalie_wins",
        loss: "goalie_losses",
        shutout: "shutouts"
      },
      SOCCER: {}
    };
  }
});

// lib/scoring-defaults/ScoringDefaultsRegistry.ts
function rule(statKey, pointsValue, multiplier = 1, enabled = true) {
  return { statKey, pointsValue, multiplier, enabled };
}
function mergeCategoryDefaults(base, sport, categoryDefaults) {
  const byKey = new Map(base.map((r) => [r.statKey, r]));
  for (const [uiKey, val] of Object.entries(categoryDefaults)) {
    const tk = templateStatKeyFromUiKey(sport, uiKey);
    if (!byKey.has(tk)) {
      byKey.set(tk, rule(tk, val));
    }
  }
  return Array.from(byKey.values());
}
var NFL_PPR, NFL_HALF_PPR, NFL_STANDARD, NFL_TE_PREMIUM, NFL_6PT_PASS_TD, NBA_POINTS, MLB_STANDARD, NHL_STANDARD, NCAAF_PPR, NCAAB_POINTS, SOCCER_STANDARD, NFL_IDP_RULES, NFL_IDP_TACKLE_HEAVY, NFL_IDP_BIG_PLAY, NFL_PPR_FULL, NFL_HALF_PPR_FULL, NFL_STANDARD_FULL, NFL_TE_PREMIUM_FULL, NFL_6PT_PASS_TD_FULL, NFL_IDP_RULES_FULL, NFL_IDP_TACKLE_HEAVY_FULL, NFL_IDP_BIG_PLAY_FULL, NCAAF_IDP_RULES_FULL, NCAAF_IDP_TACKLE_HEAVY_FULL, NCAAF_IDP_BIG_PLAY_FULL, NBA_POINTS_FULL, MLB_STANDARD_FULL, NHL_STANDARD_FULL, NCAAF_PPR_FULL, NCAAB_POINTS_FULL, SOCCER_STANDARD_FULL;
var init_ScoringDefaultsRegistry = __esm({
  "lib/scoring-defaults/ScoringDefaultsRegistry.ts"() {
    "use strict";
    init_sport_scope();
    init_scoring_stat_metadata();
    init_NflScoringCategories();
    init_NbaScoringCategories();
    init_MlbScoringCategories();
    init_NcaafScoringCategories();
    init_NcaabScoringCategories();
    init_NhlScoringCategories();
    init_SoccerScoringCategories();
    NFL_PPR = [
      rule("passing_yards", 0.04),
      rule("passing_td", 4),
      rule("interception", -2),
      rule("rushing_yards", 0.1),
      rule("rushing_td", 6),
      rule("receptions", 1),
      rule("receiving_yards", 0.1),
      rule("receiving_td", 6),
      rule("fumble_lost", -2),
      rule("two_pt_conversion", 2),
      rule("passing_2pt", 2),
      rule("rushing_2pt", 2),
      rule("receiving_2pt", 2),
      rule("fg_0_39", 3),
      rule("fg_40_49", 4),
      rule("fg_50_plus", 5),
      rule("pat_made", 1),
      rule("pat_missed", -1),
      rule("dst_sack", 1),
      rule("dst_interception", 2),
      rule("dst_fumble_recovery", 2),
      rule("dst_td", 6),
      rule("dst_safety", 2),
      rule("dst_blocked_kick", 2),
      rule("dst_return_td", 6),
      rule("dst_points_allowed_0", 10),
      rule("dst_points_allowed_1_6", 7),
      rule("dst_points_allowed_7_13", 4),
      rule("dst_points_allowed_14_20", 1),
      rule("dst_points_allowed_21_27", 0),
      rule("dst_points_allowed_28_34", -1),
      rule("dst_points_allowed_35_plus", -4)
    ];
    NFL_HALF_PPR = NFL_PPR.map(
      (r) => r.statKey === "receptions" ? { ...r, pointsValue: 0.5 } : r
    );
    NFL_STANDARD = NFL_PPR.map(
      (r) => r.statKey === "receptions" ? { ...r, pointsValue: 0, enabled: true } : r
    );
    NFL_TE_PREMIUM = NFL_PPR.map(
      (r) => r.statKey === "receptions" ? { ...r, pointsValue: 1.5 } : r
    );
    NFL_6PT_PASS_TD = NFL_PPR.map(
      (r) => r.statKey === "passing_td" ? { ...r, pointsValue: 6 } : r
    );
    NBA_POINTS = [
      rule("points", 1),
      rule("rebounds", 1.2),
      rule("assists", 1.5),
      rule("steals", 3),
      rule("blocks", 3),
      rule("turnovers", -1),
      rule("three_pointers_made", 0.5),
      rule("double_double", 1.5),
      rule("triple_double", 3)
    ];
    MLB_STANDARD = [
      rule("single", 1),
      rule("double", 2),
      rule("triple", 3),
      rule("home_run", 4),
      rule("rbi", 1),
      rule("run", 1),
      rule("walk", 1),
      rule("stolen_base", 2),
      rule("hit_by_pitch", 1),
      rule("strikeout", -0.5),
      rule("innings_pitched", 3),
      rule("earned_runs", -2),
      rule("strikeouts_pitched", 1),
      rule("save", 5),
      rule("hold", 4),
      rule("win", 5),
      rule("loss", -5),
      rule("quality_start", 4)
    ];
    NHL_STANDARD = [
      rule("goal", 3),
      rule("assist", 2),
      rule("shot_on_goal", 0.5),
      rule("blocked_shot", 0.5),
      rule("power_play_point", 1),
      rule("short_handed_point", 2),
      rule("save", 0.6),
      rule("goal_allowed", -3),
      rule("win", 5),
      rule("loss", -3),
      rule("shutout", 3)
    ];
    NCAAF_PPR = [
      ...NFL_PPR.filter(
        (r) => !r.statKey.startsWith("dst_") && !r.statKey.startsWith("fg_") && r.statKey !== "pat_made" && r.statKey !== "pat_missed"
      ),
      rule("fg_0_39", 3),
      rule("fg_40_49", 4),
      rule("fg_50_plus", 5),
      rule("pat_made", 1),
      rule("pat_missed", -1),
      rule("dst_sack", 1),
      rule("dst_interception", 2),
      rule("dst_fumble_recovery", 2),
      rule("dst_td", 6),
      rule("dst_safety", 2),
      rule("dst_blocked_kick", 2),
      rule("dst_return_td", 6),
      rule("dst_points_allowed_0", 10),
      rule("dst_points_allowed_1_6", 7),
      rule("dst_points_allowed_7_13", 4),
      rule("dst_points_allowed_14_20", 1),
      rule("dst_points_allowed_21_27", 0),
      rule("dst_points_allowed_28_34", -1),
      rule("dst_points_allowed_35_plus", -4)
    ];
    NCAAB_POINTS = [...NBA_POINTS];
    SOCCER_STANDARD = [
      rule("goal", 6),
      rule("assist", 3),
      rule("shot_on_target", 0.5),
      rule("shot", 0.2),
      rule("key_pass", 0.5),
      rule("clean_sheet", 4),
      rule("goal_conceded", -1),
      rule("goal_allowed", -1),
      rule("save", 0.5),
      rule("penalty_save", 5),
      rule("penalty_miss", -2),
      rule("yellow_card", -1),
      rule("red_card", -3),
      rule("own_goal", -2),
      rule("minutes_played", 0.02)
    ];
    NFL_IDP_RULES = [
      ...NFL_PPR,
      rule("idp_tackle_solo", 1),
      rule("idp_tackle_assist", 0.5),
      rule("idp_solo_tackle", 1),
      rule("idp_assist_tackle", 0.5),
      rule("idp_tackle_for_loss", 2),
      rule("idp_qb_hit", 1),
      rule("idp_sack", 4),
      rule("idp_interception", 3),
      rule("idp_pass_defended", 1),
      rule("idp_forced_fumble", 3),
      rule("idp_fumble_recovery", 2),
      rule("idp_td", 6),
      rule("idp_defensive_touchdown", 6),
      rule("idp_safety", 2),
      rule("idp_blocked_kick", 2)
    ];
    NFL_IDP_TACKLE_HEAVY = [
      ...NFL_PPR,
      rule("idp_solo_tackle", 1.5),
      rule("idp_assist_tackle", 0.75),
      rule("idp_tackle_solo", 1.5),
      rule("idp_tackle_assist", 0.75),
      rule("idp_tackle_for_loss", 1.5),
      rule("idp_qb_hit", 0.5),
      rule("idp_sack", 3),
      rule("idp_interception", 2),
      rule("idp_pass_defended", 0.5),
      rule("idp_forced_fumble", 2),
      rule("idp_fumble_recovery", 1),
      rule("idp_defensive_touchdown", 4),
      rule("idp_td", 4),
      rule("idp_safety", 1.5),
      rule("idp_blocked_kick", 1.5)
    ];
    NFL_IDP_BIG_PLAY = [
      ...NFL_PPR,
      rule("idp_solo_tackle", 0.5),
      rule("idp_assist_tackle", 0.25),
      rule("idp_tackle_solo", 0.5),
      rule("idp_tackle_assist", 0.25),
      rule("idp_tackle_for_loss", 3),
      rule("idp_qb_hit", 1.5),
      rule("idp_sack", 5),
      rule("idp_interception", 5),
      rule("idp_pass_defended", 1.5),
      rule("idp_forced_fumble", 4),
      rule("idp_fumble_recovery", 3),
      rule("idp_defensive_touchdown", 8),
      rule("idp_td", 8),
      rule("idp_safety", 4),
      rule("idp_blocked_kick", 3)
    ];
    NFL_PPR_FULL = mergeCategoryDefaults(NFL_PPR, "NFL", buildFullNflDefaultConfig());
    NFL_HALF_PPR_FULL = mergeCategoryDefaults(NFL_HALF_PPR, "NFL", buildFullNflDefaultConfig());
    NFL_STANDARD_FULL = mergeCategoryDefaults(NFL_STANDARD, "NFL", buildFullNflDefaultConfig());
    NFL_TE_PREMIUM_FULL = mergeCategoryDefaults(NFL_TE_PREMIUM, "NFL", buildFullNflDefaultConfig());
    NFL_6PT_PASS_TD_FULL = mergeCategoryDefaults(NFL_6PT_PASS_TD, "NFL", buildFullNflDefaultConfig());
    NFL_IDP_RULES_FULL = mergeCategoryDefaults(NFL_IDP_RULES, "NFL", buildFullNflDefaultConfig());
    NFL_IDP_TACKLE_HEAVY_FULL = mergeCategoryDefaults(NFL_IDP_TACKLE_HEAVY, "NFL", buildFullNflDefaultConfig());
    NFL_IDP_BIG_PLAY_FULL = mergeCategoryDefaults(NFL_IDP_BIG_PLAY, "NFL", buildFullNflDefaultConfig());
    NCAAF_IDP_RULES_FULL = mergeCategoryDefaults(NFL_IDP_RULES, "NCAAF", buildNcaafScoringDefaults());
    NCAAF_IDP_TACKLE_HEAVY_FULL = mergeCategoryDefaults(NFL_IDP_TACKLE_HEAVY, "NCAAF", buildNcaafScoringDefaults());
    NCAAF_IDP_BIG_PLAY_FULL = mergeCategoryDefaults(NFL_IDP_BIG_PLAY, "NCAAF", buildNcaafScoringDefaults());
    NBA_POINTS_FULL = mergeCategoryDefaults(NBA_POINTS, "NBA", buildFullNbaDefaultConfig());
    MLB_STANDARD_FULL = mergeCategoryDefaults(MLB_STANDARD, "MLB", buildMlbScoringDefaults());
    NHL_STANDARD_FULL = mergeCategoryDefaults(NHL_STANDARD, "NHL", buildNhlScoringDefaults());
    NCAAF_PPR_FULL = mergeCategoryDefaults(NCAAF_PPR, "NCAAF", buildNcaafScoringDefaults());
    NCAAB_POINTS_FULL = mergeCategoryDefaults(NCAAB_POINTS, "NCAAB", buildFullNcaabDefaultConfig());
    SOCCER_STANDARD_FULL = mergeCategoryDefaults(
      SOCCER_STANDARD,
      "SOCCER",
      buildSoccerDefaultRulesFromCategories()
    );
  }
});

// lib/scoring-defaults/ScoringKeyAliasResolver.ts
var init_ScoringKeyAliasResolver = __esm({
  "lib/scoring-defaults/ScoringKeyAliasResolver.ts"() {
    "use strict";
  }
});

// lib/multi-sport/ScoringTemplateResolver.ts
var init_ScoringTemplateResolver = __esm({
  "lib/multi-sport/ScoringTemplateResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_types();
    init_ScoringDefaultsRegistry();
    init_ScoringKeyAliasResolver();
  }
});

// lib/roster-defaults/RosterDefaultsRegistry.ts
var init_RosterDefaultsRegistry = __esm({
  "lib/roster-defaults/RosterDefaultsRegistry.ts"() {
    "use strict";
    init_SportDefaultsRegistry();
    init_sport_type_utils();
  }
});

// lib/roster-defaults/PositionEligibilityResolver.ts
var init_PositionEligibilityResolver = __esm({
  "lib/roster-defaults/PositionEligibilityResolver.ts"() {
    "use strict";
    init_RosterDefaultsRegistry();
    init_sport_type_utils();
    init_sport_scope();
  }
});

// lib/multi-sport/SportRegistry.ts
var init_SportRegistry = __esm({
  "lib/multi-sport/SportRegistry.ts"() {
    "use strict";
    init_sport_types();
    init_PositionEligibilityResolver();
    init_sport_scope();
  }
});

// lib/multi-sport/SportConfigResolver.ts
var init_SportConfigResolver = __esm({
  "lib/multi-sport/SportConfigResolver.ts"() {
    "use strict";
    init_SportRegistry();
    init_sport_types();
  }
});

// lib/multi-sport/MultiSportScoringResolver.ts
var init_MultiSportScoringResolver = __esm({
  "lib/multi-sport/MultiSportScoringResolver.ts"() {
    "use strict";
    init_ScoringTemplateResolver();
    init_SportConfigResolver();
    init_sport_scope();
  }
});

// lib/scoring-defaults/ScoringOverrideService.ts
var init_ScoringOverrideService = __esm({
  "lib/scoring-defaults/ScoringOverrideService.ts"() {
    "use strict";
    init_prisma();
    init_ScoringKeyAliasResolver();
  }
});

// lib/scoring-defaults/LeagueScoringConfigResolver.ts
var init_LeagueScoringConfigResolver = __esm({
  "lib/scoring-defaults/LeagueScoringConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_MultiSportScoringResolver();
    init_SportConfigResolver();
    init_ScoringTemplateResolver();
    init_ScoringOverrideService();
    init_ScoringKeyAliasResolver();
  }
});

// lib/waiver-defaults/WaiverProcessingConfigResolver.ts
var init_WaiverProcessingConfigResolver = __esm({
  "lib/waiver-defaults/WaiverProcessingConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_SportDefaultsRegistry();
    init_sport_type_utils();
  }
});

// lib/waiver-defaults/FAABConfigResolver.ts
var init_FAABConfigResolver = __esm({
  "lib/waiver-defaults/FAABConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_SportDefaultsRegistry();
    init_sport_type_utils();
  }
});

// lib/waiver-wire/waiver-engine-config.ts
var init_waiver_engine_config = __esm({
  "lib/waiver-wire/waiver-engine-config.ts"() {
    "use strict";
  }
});

// lib/waiver-wire/settings-service.ts
var init_settings_service = __esm({
  "lib/waiver-wire/settings-service.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_SportDefaultsRegistry();
    init_sport_type_utils();
    init_waiver_engine_config();
  }
});

// lib/waiver-defaults/WaiverConfigResolver.ts
var init_WaiverConfigResolver = __esm({
  "lib/waiver-defaults/WaiverConfigResolver.ts"() {
    "use strict";
    init_WaiverProcessingConfigResolver();
    init_FAABConfigResolver();
    init_settings_service();
  }
});

// lib/sport-defaults/DefaultPlayoffConfigResolver.ts
var NO_PLAYOFF_OVERLAY, VARIANT_OVERRIDES;
var init_DefaultPlayoffConfigResolver = __esm({
  "lib/sport-defaults/DefaultPlayoffConfigResolver.ts"() {
    "use strict";
    NO_PLAYOFF_OVERLAY = {
      playoff_team_count: 0,
      playoff_weeks: 0,
      first_round_byes: 0,
      playoff_start_week: null,
      bye_rules: null,
      total_rounds: 0,
      consolation_plays_for: "none",
      consolation_bracket_enabled: false,
      third_place_game_enabled: false,
      toilet_bowl_enabled: false,
      championship_length: 0,
      reseed_behavior: "fixed_bracket"
    };
    VARIANT_OVERRIDES = {
      NFL: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {},
        PPR: {},
        HALF_PPR: {},
        SUPERFLEX: {
          reseed_behavior: "reseed_after_round"
        },
        IDP: {
          tiebreaker_rules: ["points_for", "head_to_head", "points_against", "division_record"],
          reseed_behavior: "fixed_bracket"
        },
        DYNASTY_IDP: {
          tiebreaker_rules: ["points_for", "head_to_head", "points_against", "division_record"],
          reseed_behavior: "fixed_bracket"
        },
        DEVY_DYNASTY: {
          playoff_team_count: 8,
          playoff_weeks: 3,
          first_round_byes: 0,
          bye_rules: null,
          total_rounds: 3,
          reseed_behavior: "fixed_bracket"
        },
        MERGED_DEVY_C2C: {
          playoff_team_count: 8,
          playoff_weeks: 3,
          first_round_byes: 0,
          bye_rules: null,
          total_rounds: 3,
          reseed_behavior: "fixed_bracket"
        }
      },
      NBA: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {},
        DEVY_DYNASTY: {
          playoff_team_count: 8,
          playoff_weeks: 3,
          first_round_byes: 0,
          bye_rules: null,
          total_rounds: 3
        },
        MERGED_DEVY_C2C: {
          playoff_team_count: 8,
          playoff_weeks: 3,
          first_round_byes: 0,
          bye_rules: null,
          total_rounds: 3
        }
      },
      MLB: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {}
      },
      NHL: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {}
      },
      NCAAF: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {}
      },
      NCAAB: {
        NO_PLAYOFF: { ...NO_PLAYOFF_OVERLAY },
        STANDARD: {}
      },
      SOCCER: {
        STANDARD: {},
        NO_PLAYOFF: {
          ...NO_PLAYOFF_OVERLAY
        }
      }
    };
  }
});

// lib/playoff-defaults/PlayoffBracketConfigResolver.ts
var init_PlayoffBracketConfigResolver = __esm({
  "lib/playoff-defaults/PlayoffBracketConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_DefaultPlayoffConfigResolver();
    init_sport_type_utils();
  }
});

// lib/playoff-defaults/PlayoffSeedingResolver.ts
var init_PlayoffSeedingResolver = __esm({
  "lib/playoff-defaults/PlayoffSeedingResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_DefaultPlayoffConfigResolver();
    init_sport_type_utils();
  }
});

// lib/sport-defaults/DefaultScheduleConfigResolver.ts
var init_DefaultScheduleConfigResolver = __esm({
  "lib/sport-defaults/DefaultScheduleConfigResolver.ts"() {
    "use strict";
  }
});

// lib/sport-defaults/LeagueVariantResolver.ts
var init_LeagueVariantResolver = __esm({
  "lib/sport-defaults/LeagueVariantResolver.ts"() {
    "use strict";
    init_sport_type_utils();
    init_LeagueVariantRegistry();
    init_sport_scope();
  }
});

// lib/sport-defaults/LeagueDefaultSettingsService.ts
var init_LeagueDefaultSettingsService = __esm({
  "lib/sport-defaults/LeagueDefaultSettingsService.ts"() {
    "use strict";
    init_SportDefaultsRegistry();
    init_DefaultPlayoffConfigResolver();
    init_DefaultScheduleConfigResolver();
    init_sport_type_utils();
    init_LeagueVariantResolver();
  }
});

// lib/playoff-defaults/StandingsTiebreakerResolver.ts
var init_StandingsTiebreakerResolver = __esm({
  "lib/playoff-defaults/StandingsTiebreakerResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_LeagueDefaultSettingsService();
  }
});

// lib/league-runtime/leagueFormat.ts
function deriveLeagueFormat(league) {
  const raw = league.leagueType;
  return typeof raw === "string" && raw.trim() ? raw : league.isDynasty ? "dynasty" : "redraft";
}
var init_leagueFormat = __esm({
  "lib/league-runtime/leagueFormat.ts"() {
    "use strict";
  }
});

// lib/redraft-commissioner/controls.ts
function jsonRecord(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}
function isStandardRedraft(league) {
  const variant = String(league.leagueVariant ?? "").toLowerCase();
  const specialtyVariants = ["keeper", "dynasty", "devy", "c2c", "salary_cap", "salarycap", "best_ball", "bestball", "guillotine", "guilly", "survivor", "survivor_guillotine", "zombie", "big_brother", "bigbrother", "tournament"];
  return deriveLeagueFormat(league).toLowerCase() === "redraft" && !league.isDynasty && !league.guillotineMode && !league.survivorMode && !league.bestBallMode && !specialtyVariants.includes(variant);
}
var init_controls = __esm({
  "lib/redraft-commissioner/controls.ts"() {
    "use strict";
    init_leagueFormat();
  }
});

// lib/playoff-defaults/PlayoffConfigResolver.ts
var init_PlayoffConfigResolver = __esm({
  "lib/playoff-defaults/PlayoffConfigResolver.ts"() {
    "use strict";
    init_PlayoffBracketConfigResolver();
    init_PlayoffSeedingResolver();
    init_StandingsTiebreakerResolver();
    init_prisma();
    init_controls();
  }
});

// lib/schedule-defaults/schedule-settings-slice.ts
var init_schedule_settings_slice = __esm({
  "lib/schedule-defaults/schedule-settings-slice.ts"() {
    "use strict";
  }
});

// lib/schedule-defaults/MatchupCadenceResolver.ts
var init_MatchupCadenceResolver = __esm({
  "lib/schedule-defaults/MatchupCadenceResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_DefaultScheduleConfigResolver();
    init_schedule_settings_slice();
    init_sport_type_utils();
  }
});

// lib/schedule-defaults/ScoringWindowResolver.ts
var init_ScoringWindowResolver = __esm({
  "lib/schedule-defaults/ScoringWindowResolver.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_DefaultScheduleConfigResolver();
    init_sport_type_utils();
  }
});

// lib/schedule-defaults/LeagueScheduleGenerationService.ts
var init_LeagueScheduleGenerationService = __esm({
  "lib/schedule-defaults/LeagueScheduleGenerationService.ts"() {
    "use strict";
    init_prisma();
    init_sport_scope();
    init_DefaultScheduleConfigResolver();
    init_schedule_settings_slice();
    init_sport_type_utils();
  }
});

// lib/schedule-defaults/ScheduleConfigResolver.ts
var init_ScheduleConfigResolver = __esm({
  "lib/schedule-defaults/ScheduleConfigResolver.ts"() {
    "use strict";
    init_prisma();
    init_MatchupCadenceResolver();
    init_ScoringWindowResolver();
    init_LeagueScheduleGenerationService();
  }
});

// lib/league-runtime/canonicalLeagueRules.ts
var init_canonicalLeagueRules = __esm({
  "lib/league-runtime/canonicalLeagueRules.ts"() {
    "use strict";
    init_prisma();
    init_DraftRoomConfigResolver();
    init_DraftUISettingsResolver();
    init_LeagueScoringConfigResolver();
    init_WaiverConfigResolver();
    init_PlayoffConfigResolver();
    init_ScheduleConfigResolver();
    init_leagueFormat();
    init_controls();
  }
});

// lib/league-runtime/index.ts
var init_league_runtime = __esm({
  "lib/league-runtime/index.ts"() {
    "use strict";
    init_canonicalLeagueRules();
    init_leagueRuntimeEvents();
  }
});

// lib/redraft/sportConfig.ts
function getRedraftSportConfig(sport) {
  const key = sport.toLowerCase().replace(/[^a-z]/g, "");
  return REDRAFT_SPORT_CONFIGS[key] ?? REDRAFT_SPORT_CONFIGS.nfl;
}
var REDRAFT_SPORT_CONFIGS;
var init_sportConfig2 = __esm({
  "lib/redraft/sportConfig.ts"() {
    "use strict";
    REDRAFT_SPORT_CONFIGS = {
      nfl: {
        sport: "nfl",
        label: "NFL",
        starterSlots: ["QB", "RB", "WR", "WR", "TE", "DEF"],
        benchSlots: 6,
        irSlots: 1,
        totalRosterSize: 13,
        lineupFrequency: "weekly",
        defaultScoringFormat: "half_ppr",
        supportedScoringFormats: ["ppr", "half_ppr", "standard", "idp"],
        regularSeasonWeeks: 14,
        defaultPlayoffStartWeek: 15,
        defaultPlayoffTeams: 6,
        supportedPlayoffSizes: [4, 6, 8],
        defaultTeamCount: 12,
        defaultDraftRounds: 12,
        defaultDraftTimerSeconds: 90,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [3],
        // Wednesday
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 11,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "h2h_categories", "total_points"],
        scheduleNotes: [
          "NFL season: Weeks 1-18 (17 game weeks + 1 bye per team)",
          "Fantasy regular season typically Weeks 1-14, playoffs Weeks 15-17",
          "Thursday Night Football starts each week \u2014 early locks",
          "Monday Night Football ends each week \u2014 stat corrections Tuesday"
        ],
        flexPositions: {
          FLX: ["RB", "WR", "TE"],
          FLEX: ["RB", "WR", "TE"],
          SF: ["QB", "RB", "WR", "TE"],
          SUPERFLEX: ["QB", "RB", "WR", "TE"],
          SUPER_FLEX: ["QB", "RB", "WR", "TE"]
        }
      },
      nba: {
        sport: "nba",
        label: "NBA",
        starterSlots: ["PG", "SG", "SF", "PF", "C", "G", "F", "UTIL", "UTIL"],
        benchSlots: 4,
        irSlots: 2,
        totalRosterSize: 15,
        lineupFrequency: "daily",
        defaultScoringFormat: "points",
        supportedScoringFormats: ["points", "categories_9cat", "categories_8cat"],
        regularSeasonWeeks: 20,
        defaultPlayoffStartWeek: 21,
        defaultPlayoffTeams: 6,
        supportedPlayoffSizes: [4, 6, 8],
        defaultTeamCount: 12,
        defaultDraftRounds: 13,
        defaultDraftTimerSeconds: 60,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [1],
        // Monday
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 16,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "h2h_categories", "roto"],
        scheduleNotes: [
          "NBA uses daily lineups \u2014 managers set lineups each day",
          "Weekly scoring aggregates daily totals for matchup results",
          "All-Star break (mid-February) reduces slate for ~1 week",
          "Back-to-backs affect player availability \u2014 bench depth critical"
        ],
        flexPositions: {
          G: ["PG", "SG"],
          F: ["SF", "PF"],
          UTIL: ["PG", "SG", "SF", "PF", "C"]
        }
      },
      mlb: {
        sport: "mlb",
        label: "MLB",
        starterSlots: ["C", "1B", "2B", "3B", "SS", "OF", "OF", "OF", "UTIL", "SP", "SP", "RP", "RP"],
        benchSlots: 5,
        irSlots: 3,
        totalRosterSize: 21,
        lineupFrequency: "daily",
        defaultScoringFormat: "points",
        supportedScoringFormats: ["points", "categories_5x5", "categories_6x6"],
        regularSeasonWeeks: 21,
        defaultPlayoffStartWeek: 22,
        defaultPlayoffTeams: 6,
        supportedPlayoffSizes: [4, 6, 8],
        defaultTeamCount: 12,
        defaultDraftRounds: 20,
        defaultDraftTimerSeconds: 60,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [1],
        // Monday
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 17,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "h2h_categories", "roto"],
        scheduleNotes: [
          "MLB uses daily lineups \u2014 critical for pitcher streaming",
          "Weekly scoring aggregates daily totals",
          "All-Star break (~week 11) has reduced games",
          "Doubleheaders can spike daily scoring significantly",
          "September callups expand available player pool"
        ],
        flexPositions: {
          UTIL: ["C", "1B", "2B", "3B", "SS", "OF"]
        }
      },
      nhl: {
        sport: "nhl",
        label: "NHL",
        starterSlots: ["C", "C", "LW", "RW", "D", "D", "UTIL", "G", "G"],
        benchSlots: 4,
        irSlots: 2,
        totalRosterSize: 15,
        lineupFrequency: "daily",
        defaultScoringFormat: "points",
        supportedScoringFormats: ["points", "categories"],
        regularSeasonWeeks: 21,
        defaultPlayoffStartWeek: 22,
        defaultPlayoffTeams: 6,
        supportedPlayoffSizes: [4, 6, 8],
        defaultTeamCount: 12,
        defaultDraftRounds: 14,
        defaultDraftTimerSeconds: 60,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [1],
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 17,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "h2h_categories", "roto"],
        scheduleNotes: [
          "NHL uses daily lineups \u2014 goalie starts are critical decisions",
          "Overtime and shootout scoring counts",
          "Trade deadline shifts roster values significantly"
        ],
        flexPositions: {
          UTIL: ["C", "LW", "RW", "D"]
        }
      },
      ncaaf: {
        sport: "ncaaf",
        label: "College Football",
        starterSlots: ["QB", "RB", "WR", "WR", "TE", "DEF"],
        benchSlots: 8,
        irSlots: 1,
        totalRosterSize: 15,
        lineupFrequency: "weekly",
        defaultScoringFormat: "half_ppr",
        supportedScoringFormats: ["ppr", "half_ppr", "standard"],
        regularSeasonWeeks: 12,
        defaultPlayoffStartWeek: 13,
        defaultPlayoffTeams: 4,
        supportedPlayoffSizes: [4, 6],
        defaultTeamCount: 12,
        defaultDraftRounds: 14,
        defaultDraftTimerSeconds: 90,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [0],
        // Sunday
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 9,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "total_points"],
        scheduleNotes: [
          "Saturday-focused schedule with occasional weeknight games",
          "Bye weeks less uniform than NFL \u2014 check schedule carefully",
          "Conference championship weeks and bowl games extend season",
          "Shorter season means faster standings changes"
        ],
        flexPositions: {
          FLX: ["RB", "WR", "TE"],
          FLEX: ["RB", "WR", "TE"],
          SF: ["QB", "RB", "WR", "TE"],
          SUPERFLEX: ["QB", "RB", "WR", "TE"],
          SUPER_FLEX: ["QB", "RB", "WR", "TE"]
        }
      },
      ncaab: {
        sport: "ncaab",
        label: "College Basketball",
        starterSlots: ["PG", "SG", "SF", "PF", "C", "UTIL", "UTIL"],
        benchSlots: 4,
        irSlots: 1,
        totalRosterSize: 12,
        lineupFrequency: "daily",
        defaultScoringFormat: "points",
        supportedScoringFormats: ["points", "categories"],
        regularSeasonWeeks: 16,
        defaultPlayoffStartWeek: 17,
        defaultPlayoffTeams: 4,
        supportedPlayoffSizes: [4, 6],
        defaultTeamCount: 10,
        defaultDraftRounds: 12,
        defaultDraftTimerSeconds: 60,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [1],
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 12,
        tradeReviewPeriodHours: 24,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "h2h_categories"],
        scheduleNotes: [
          "College basketball has daily lineups",
          "Conference tournaments and March Madness create scheduling variance",
          "Eliminated tournament teams stop scoring \u2014 bench depth critical",
          "Smaller rosters mean each start slot is high-value"
        ],
        flexPositions: {
          UTIL: ["PG", "SG", "SF", "PF", "C"]
        }
      },
      soccer: {
        sport: "soccer",
        label: "Soccer",
        starterSlots: ["GK", "DEF", "DEF", "DEF", "MID", "MID", "MID", "FWD", "FWD", "FWD"],
        benchSlots: 5,
        irSlots: 1,
        totalRosterSize: 16,
        lineupFrequency: "weekly",
        defaultScoringFormat: "points",
        supportedScoringFormats: ["points"],
        regularSeasonWeeks: 34,
        defaultPlayoffStartWeek: 35,
        defaultPlayoffTeams: 4,
        supportedPlayoffSizes: [4, 6],
        defaultTeamCount: 10,
        defaultDraftRounds: 15,
        defaultDraftTimerSeconds: 90,
        defaultWaiverType: "faab",
        defaultFaabBudget: 100,
        defaultWaiverProcessDays: [2],
        // Tuesday
        defaultWaiverProcessTimeUtc: "10:00",
        defaultTradeDeadlineWeek: 28,
        tradeReviewPeriodHours: 48,
        defaultMatchupFormat: "h2h_points",
        supportedMatchupFormats: ["h2h_points", "total_points"],
        scheduleNotes: [
          "Matchweek-based scoring (Sat-Mon typically)",
          "International breaks create 2-week gaps",
          "Double gameweeks: some teams play twice \u2014 huge scoring potential",
          "Blank gameweeks: some teams don't play \u2014 bench depth critical",
          "Transfer windows affect player availability mid-season"
        ],
        flexPositions: {}
      }
    };
  }
});

// lib/redraft/lineupValidation.ts
function normalizeToken(input) {
  const value = String(input ?? "").trim().toUpperCase().replace(/\s+/g, "_");
  if (value === "D/ST" || value === "DEF/ST" || value === "DST" || value === "DEFENSE") return "DEF";
  if (value === "PK" || value === "KICKER") return "K";
  if (value === "FLEX") return "FLX";
  if (value === "SUPERFLEX" || value === "SUPER_FLEX") return "SF";
  if (value === "IL" || value === "IL+" || value === "DL") return "IR";
  if (value === "NA") return "RESERVE";
  if (value === "BN") return "BENCH";
  return value;
}
function redraftPlayerPositionTokens(position) {
  return String(position ?? "").split(/[,;]/).flatMap((part) => {
    const value = part.trim().toUpperCase();
    return ["D/ST", "DEF/ST"].includes(value) ? ["DEF"] : value.split("/").map(normalizeToken);
  }).filter(Boolean);
}
function allowedPositionsForSlot(sport, slotType) {
  const config = getRedraftSportConfig(sport);
  const slot = normalizeToken(slotType);
  const sportConfig = tryGetSportConfig(sport);
  if (sport.toUpperCase() === "MLB" && slot === "CI") return ["1B", "3B"];
  if (sport.toUpperCase() === "MLB" && slot === "MI") return ["2B", "SS"];
  const canonicalSlot = sportConfig?.defaultRosterSlots.find((s) => normalizeToken(s.key) === slot);
  if (canonicalSlot) {
    const aliases = Object.entries(sportConfig.positionEligibility).filter(([, slots]) => slots.some((eligibleSlot) => normalizeToken(eligibleSlot) === slot)).map(([position]) => normalizeToken(position));
    return [.../* @__PURE__ */ new Set([...canonicalSlot.eligiblePositions.map(normalizeToken), ...aliases])];
  }
  const flexPositions = config.flexPositions[slot];
  if (flexPositions?.length) return flexPositions.map(normalizeToken);
  return [slot];
}
var init_lineupValidation = __esm({
  "lib/redraft/lineupValidation.ts"() {
    "use strict";
    init_sportConfig2();
    init_sportConfig();
  }
});

// lib/redraft/rosterConfigResolver.ts
function isRecord2(v) {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}
function extractRawSlotMap(leagueSettings) {
  const s = isRecord2(leagueSettings) ? leagueSettings : {};
  const roster = isRecord2(s.roster) ? s.roster : null;
  const config = roster && isRecord2(roster.config) ? roster.config : null;
  const sections = config && Array.isArray(config.sections) ? config.sections : null;
  if (sections && sections.length > 0) {
    const merged = {};
    let found = false;
    for (const section of sections) {
      const slots = isRecord2(section) && isRecord2(section.slots) ? section.slots : null;
      if (!slots) continue;
      for (const [k, v] of Object.entries(slots)) {
        const n = Number(v);
        if (Number.isFinite(n)) {
          merged[k] = (merged[k] ?? 0) + n;
          found = true;
        }
      }
    }
    if (found) return merged;
  }
  for (const candidate of [s.starter_slots, isRecord2(s.roster) ? s.roster.starter_slots : null, isRecord2(s.roster) ? s.roster.starterSlots : null, isRecord2(s.sportConfig) ? s.sportConfig.starterSlots : null]) {
    if (isRecord2(candidate)) {
      const map = {};
      let found = false;
      for (const [k, v] of Object.entries(candidate)) {
        const n = Number(v);
        if (Number.isFinite(n)) {
          map[k] = n;
          found = true;
        }
      }
      if (found) return map;
    }
  }
  return null;
}
function defaultsFor(sport) {
  const cfg = getRedraftSportConfig(sport);
  const starterCapacities = /* @__PURE__ */ new Map();
  for (const slot of cfg.starterSlots) {
    const t = normalizeToken(slot);
    starterCapacities.set(t, (starterCapacities.get(t) ?? 0) + 1);
  }
  const benchSlots = Math.max(0, Math.floor(Number(cfg.benchSlots ?? 0)));
  const irSlots = Math.max(0, Math.floor(Number(cfg.irSlots ?? 0)));
  const taxiSlots = 0;
  const starters = [...starterCapacities.values()].reduce((a, b) => a + b, 0);
  return { starterCapacities, benchSlots, irSlots, taxiSlots, maxRosterSize: starters + benchSlots + irSlots + taxiSlots, source: "defaults" };
}
function resolveRedraftRosterConfig(sport, leagueSettings) {
  const raw = extractRawSlotMap(leagueSettings);
  if (!raw) return defaultsFor(sport);
  const starterCapacities = /* @__PURE__ */ new Map();
  let benchSlots = 0;
  let irSlots = 0;
  let taxiSlots = 0;
  let prospectSlots = 0;
  let inactiveSlots = 0;
  for (const [rawKey, rawCount] of Object.entries(raw)) {
    const count2 = Math.max(0, Math.floor(Number(rawCount ?? 0)));
    if (count2 <= 0) continue;
    const token = normalizeToken(rawKey);
    if (String(rawKey).trim().toUpperCase() === "NA") inactiveSlots += count2;
    else if (BENCH_TOKENS.has(token)) benchSlots += count2;
    else if (IR_TOKENS.has(token)) irSlots += count2;
    else if (TAXI_TOKENS.has(token)) taxiSlots += count2;
    else if (PROSPECT_TOKENS.has(token)) prospectSlots += count2;
    else starterCapacities.set(token, (starterCapacities.get(token) ?? 0) + count2);
  }
  const s = isRecord2(leagueSettings) ? leagueSettings : {};
  const roster = isRecord2(s.roster) ? s.roster : {};
  const sportConfig = isRecord2(s.sportConfig) ? s.sportConfig : {};
  const num2 = (...vals) => {
    for (const v of vals) {
      const n = Number(v);
      if (Number.isFinite(n) && n >= 0) return Math.floor(n);
    }
    return null;
  };
  if (benchSlots === 0) benchSlots = num2(roster.benchSlots, sportConfig.benchSlots) ?? 0;
  if (irSlots === 0) irSlots = num2(roster.irSlots, sportConfig.irSlots) ?? 0;
  if (taxiSlots === 0) taxiSlots = num2(roster.taxiSlots, sportConfig.taxiSlots) ?? 0;
  if (starterCapacities.size === 0) return defaultsFor(sport);
  const starters = [...starterCapacities.values()].reduce((a, b) => a + b, 0);
  return {
    starterCapacities,
    benchSlots,
    irSlots,
    taxiSlots,
    ...prospectSlots ? { prospectSlots } : {},
    ...inactiveSlots ? { inactiveSlots } : {},
    maxRosterSize: starters + benchSlots + irSlots + taxiSlots + prospectSlots + inactiveSlots,
    source: "commissioner"
  };
}
var BENCH_TOKENS, IR_TOKENS, TAXI_TOKENS, PROSPECT_TOKENS;
var init_rosterConfigResolver = __esm({
  "lib/redraft/rosterConfigResolver.ts"() {
    "use strict";
    init_sportConfig2();
    init_lineupValidation();
    BENCH_TOKENS = /* @__PURE__ */ new Set(["BENCH", "BN"]);
    IR_TOKENS = /* @__PURE__ */ new Set(["IR", "RESERVE"]);
    TAXI_TOKENS = /* @__PURE__ */ new Set(["TAXI", "DEVY"]);
    PROSPECT_TOKENS = /* @__PURE__ */ new Set(["PROSPECT", "RIGHTS", "MINORS"]);
  }
});

// lib/sports-data/easternGameDay.ts
function easternCalendarDay(instant) {
  if (!instant || Number.isNaN(instant.getTime())) return null;
  const parts = Object.fromEntries(easternParts.formatToParts(instant).map((p) => [p.type, p.value]));
  const y = Number(parts.year);
  const m = Number(parts.month);
  const d = Number(parts.day);
  if (!y || !m || !d) return null;
  return new Date(Date.UTC(y, m - 1, d));
}
function gameDayFromRiGameId(gameId) {
  const m = /^(\d{4})(\d{2})(\d{2})-/.exec(String(gameId ?? ""));
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  const d = Number(m[3]);
  const date = new Date(Date.UTC(y, mo - 1, d));
  return date.getUTCFullYear() === y && date.getUTCMonth() === mo - 1 && date.getUTCDate() === d ? date : null;
}
var EASTERN, easternParts;
var init_easternGameDay = __esm({
  "lib/sports-data/easternGameDay.ts"() {
    "use strict";
    EASTERN = "America/New_York";
    easternParts = new Intl.DateTimeFormat("en-US", {
      timeZone: EASTERN,
      year: "numeric",
      month: "2-digit",
      day: "2-digit"
    });
  }
});

// lib/scoring-runtime/dailySportStatNormalization.ts
function isRecord3(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function asNumber2(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const n = Number(value);
    return Number.isFinite(n) ? n : void 0;
  }
  return void 0;
}
function countDoubleDigitCategories(stats) {
  return DOUBLE_CATEGORIES.reduce((count2, key) => count2 + ((stats[key] ?? 0) >= 10 ? 1 : 0), 0);
}
function normalizeWithAliases(raw, aliases, componentSums = {}) {
  const source = isRecord3(raw) && isRecord3(raw.stats) ? raw.stats : raw;
  if (!isRecord3(source)) return { stats: {}, unmappedKeys: [] };
  const stats = {};
  const claimed = /* @__PURE__ */ new Set();
  for (const [canonical, spellings] of Object.entries(aliases)) {
    for (const spelling of spellings) {
      const value = asNumber2(source[spelling]);
      if (value !== void 0) {
        stats[canonical] = value;
        claimed.add(spelling);
        break;
      }
    }
  }
  if (stats.reb === void 0) {
    const off = asNumber2(source.oreb) ?? asNumber2(source.offensive_rebounds);
    const def = asNumber2(source.dreb) ?? asNumber2(source.defensive_rebounds);
    if (off !== void 0 || def !== void 0) {
      stats.reb = (off ?? 0) + (def ?? 0);
      for (const k of ["oreb", "offensive_rebounds", "dreb", "defensive_rebounds"]) {
        if (source[k] !== void 0) claimed.add(k);
      }
    }
  }
  for (const [canonical, parts] of Object.entries(componentSums)) {
    if (stats[canonical] !== void 0) continue;
    let total = 0;
    let found = false;
    for (const part of parts) {
      const value = asNumber2(source[part]);
      if (value !== void 0) {
        total += value;
        claimed.add(part);
        found = true;
      }
    }
    if (found) stats[canonical] = total;
  }
  const unmappedKeys = Object.keys(source).filter(
    (key) => !claimed.has(key) && asNumber2(source[key]) !== void 0
  );
  return { stats, unmappedKeys };
}
function normalizeNbaGameStats(raw) {
  const result = normalizeWithAliases(raw, NBA_STAT_ALIASES);
  if (Object.keys(result.stats).length === 0) return result;
  const doubles = countDoubleDigitCategories(result.stats);
  if (doubles >= 2) result.stats.dbl_dbl = 1;
  if (doubles >= 3) result.stats.trpl_dbl = 1;
  return result;
}
function normalizeNcaabGameStats(raw) {
  const result = normalizeWithAliases(raw, NCAAB_STAT_ALIASES);
  return { ...result, unmappedKeys: result.unmappedKeys.filter((k) => !NCAAB_KNOWN_UNSCORED.has(k)) };
}
function normalizeNhlGameStats(raw) {
  const result = normalizeWithAliases(raw, NHL_STAT_ALIASES, NHL_COMPONENT_SUMS);
  if (isRecord3(raw) && (raw.group === "goalies" || raw.position === "G")) {
    const source = isRecord3(raw.stats) ? raw.stats : raw;
    const time = raw.timeOnIce ?? source.time_on_ice;
    const match = typeof time === "string" ? /^(\d+):([0-5]\d)$/.exec(time) : null;
    if (match) result.stats.g_seconds = Number(match[1]) * 60 + Number(match[2]);
    else {
      const seconds = asNumber2(time);
      if (seconds !== void 0 && seconds >= 0) result.stats.g_seconds = seconds;
    }
  }
  return result;
}
function baseballInningsToOuts(value) {
  const ip = asNumber2(value);
  if (ip === void 0 || ip < 0) return void 0;
  const innings = Math.floor(ip);
  const fraction = (ip - innings) * 10;
  const outs = Math.round(fraction);
  if (Math.abs(fraction - outs) > 1e-6 || outs > 2) return void 0;
  return innings * 3 + outs;
}
function normalizeMlbGameStats(raw) {
  if (!isRecord3(raw) || !isRecord3(raw.stats)) return { stats: {}, unmappedKeys: [] };
  const source = raw.stats;
  const batting = raw.group === "batting";
  const pitching = raw.group === "pitching";
  if (!batting && !pitching) return { stats: {}, unmappedKeys: Object.keys(source) };
  const aliases = batting ? {
    r: ["R"],
    hr: ["HR"],
    rbi: ["RBI"],
    sb: ["SB"],
    bb: ["BB"],
    at_bats: ["AB"],
    hits: ["H"],
    singles: ["1B"],
    doubles: ["2B"],
    triples: ["3B"],
    strikeouts: ["SO"],
    caught_stealing: ["CS"],
    hit_by_pitch: ["HBP"],
    intentional_walks: ["IBB"]
  } : {
    w: ["W"],
    l: ["L"],
    sv: ["S"],
    so: ["K"],
    er: ["ER"],
    holds: ["HLD"],
    blown_saves: ["BS"],
    runs_allowed: ["R"],
    hits_allowed: ["H"],
    walks_allowed: ["BB"],
    home_runs_allowed: ["HR"],
    hit_batters: ["HBP"],
    balks: ["BK"],
    wild_pitches: ["WP"]
  };
  const result = normalizeWithAliases(source, aliases);
  if (batting) {
    const { singles, doubles, triples, hr } = result.stats;
    if ([singles, doubles, triples, hr].every((v) => v !== void 0)) {
      result.stats.tb = singles + 2 * doubles + 3 * triples + 4 * hr;
      result.stats.cycle_bonus = [singles, doubles, triples, hr].every((v) => v >= 1) ? 1 : 0;
    }
  } else {
    const outs = baseballInningsToOuts(source.IP);
    if (outs !== void 0) {
      result.stats.outs_recorded = outs;
      result.stats.ip = outs / 3;
      if (["SP", "S"].includes(String(raw.position ?? source.POS ?? "").toUpperCase()) && result.stats.er !== void 0) {
        result.stats.qs = outs >= 18 && result.stats.er <= 3 ? 1 : 0;
      }
      result.unmappedKeys = result.unmappedKeys.filter((key) => key !== "IP");
    }
  }
  const knownUnscored = batting ? ["E", "PO", "Outs", "BAT_ORD"] : ["1B", "2B", "3B", "PO", "SB", "CS", "IBB", "E", "pitches", "strikes"];
  result.unmappedKeys = result.unmappedKeys.filter((key) => !knownUnscored.includes(key));
  return result;
}
function aggregateWeeklyStats(rows, normalizer) {
  const stats = {};
  const unmapped = /* @__PURE__ */ new Set();
  let gamesCounted = 0;
  for (const row of rows) {
    const { stats: gameStats, unmappedKeys } = normalizer(row);
    for (const key of unmappedKeys) unmapped.add(key);
    if (Object.keys(gameStats).length === 0) continue;
    gamesCounted += 1;
    for (const [key, value] of Object.entries(gameStats)) {
      stats[key] = (stats[key] ?? 0) + value;
    }
  }
  return { stats, gamesCounted, unmappedKeys: [...unmapped].sort() };
}
function isDailyStatSport(sport) {
  return Object.hasOwn(DAILY_SPORT_NORMALIZERS, String(sport ?? "").trim().toUpperCase());
}
function weekWindowFromSeasonStart(seasonStartUtc, week) {
  if (seasonStartUtc == null) return null;
  const start = seasonStartUtc instanceof Date ? new Date(seasonStartUtc.getTime()) : new Date(seasonStartUtc);
  if (Number.isNaN(start.getTime())) return null;
  if (!Number.isFinite(week) || week < 1) return null;
  const DAY_MS = 864e5;
  const windowStart = new Date(start.getTime() + (week - 1) * 7 * DAY_MS);
  return { start: windowStart, end: new Date(windowStart.getTime() + 7 * DAY_MS) };
}
function getDailySportNormalizer(sport) {
  return DAILY_SPORT_NORMALIZERS[String(sport ?? "").trim().toUpperCase()] ?? null;
}
var NBA_STAT_ALIASES, NCAAB_STAT_ALIASES, NCAAB_KNOWN_UNSCORED, NHL_STAT_ALIASES, NHL_COMPONENT_SUMS, DOUBLE_CATEGORIES, DAILY_SPORT_NORMALIZERS;
var init_dailySportStatNormalization = __esm({
  "lib/scoring-runtime/dailySportStatNormalization.ts"() {
    "use strict";
    NBA_STAT_ALIASES = {
      pts: ["points", "pts", "PTS"],
      //                              measured: points
      reb: ["total_rebounds", "reb", "rebounds", "totReb", "REB"],
      // measured: total_rebounds
      ast: ["assists", "ast", "AST"],
      //                             measured: assists
      stl: ["steals", "stl", "STL"],
      //                              measured: steals
      blk: ["blocks", "blk", "blocked_shots", "BLK"],
      //             measured: blocks
      to: ["turnovers", "to", "tov", "turnover", "TO"],
      //           measured: turnovers
      // ⚠ `three_pointS_made`, not `three_pointERS_made`. The guessed spelling was
      // one letter out, which scores every made three as nothing.
      threes: ["three_points_made", "threes", "tpm", "fg3m", "3pm"],
      fgm: ["field_goals_made", "fgm", "FGM"],
      //                    measured: field_goals_made
      ftm: ["free_throws_made", "ftm", "FTM"],
      //                    measured: free_throws_made
      fga: ["field_goals_attempted", "fga", "FGA"],
      fta: ["free_throws_attempted", "fta", "FTA"]
    };
    NCAAB_STAT_ALIASES = {
      pts: NBA_STAT_ALIASES.pts,
      reb: NBA_STAT_ALIASES.reb,
      ast: NBA_STAT_ALIASES.ast,
      stl: NBA_STAT_ALIASES.stl,
      blk: NBA_STAT_ALIASES.blk,
      to: NBA_STAT_ALIASES.to,
      threes: NBA_STAT_ALIASES.threes
    };
    NCAAB_KNOWN_UNSCORED = /* @__PURE__ */ new Set([
      "player_id",
      "fouls",
      "minutes",
      "offensive_rebounds",
      "defensive_rebounds",
      "field_goals_made",
      "field_goals_attempted",
      "two_points_made",
      "two_points_attempted",
      "two_point_percentage",
      "three_points_attempted",
      "free_throws_made",
      "free_throws_attempted"
    ]);
    NHL_STAT_ALIASES = {
      g: ["goals", "g", "G"],
      //                                     measured: goals
      a: ["assists", "a", "A"],
      //                                   measured: assists
      plusminus: ["plus_minus", "plusminus", "plusMinus", "+/-"],
      //  measured: plus_minus
      sog: ["shots_on_goal", "sog", "shots", "SOG"],
      //               measured: shots_on_goal
      // ⚠ The feed carries NO *_points total for either special-teams stat, only
      // the goal and assist halves — summed below. The guessed single key matched
      // nothing at all.
      ppp: ["power_play_points", "ppp", "PPP"],
      shp: ["short_handed_points", "shp", "SHP"],
      blks: ["blocks", "blks", "blocked", "blocked_shots"],
      //        measured: blocks
      hits: ["hits", "HIT"],
      //                                      measured: hits
      pim: ["penalty_minutes", "pim", "PIM"],
      //                     measured: penalty_minutes
      // ⚠ Goalie keys are SINGULAR, and it is "allowed", not "against".
      g_win: ["win", "g_win", "wins", "goalie_wins", "W"],
      //         measured: win
      g_sv: ["saves", "g_sv", "goalie_saves", "SV"],
      //               measured: saves
      g_so: ["shutouts", "g_so", "goalie_shutouts", "SO"],
      //         measured: shutouts
      g_ga: ["goals_allowed", "g_ga", "goals_against", "GA"],
      //      measured: goals_allowed
      g_sa: ["shots_against", "g_sa"]
    };
    NHL_COMPONENT_SUMS = {
      ppp: ["power_play_goals", "power_play_assists"],
      shp: ["short_handed_goals", "short_handed_assists"]
    };
    DOUBLE_CATEGORIES = ["pts", "reb", "ast", "stl", "blk"];
    DAILY_SPORT_NORMALIZERS = {
      NBA: normalizeNbaGameStats,
      NHL: normalizeNhlGameStats,
      MLB: normalizeMlbGameStats,
      NCAAB: normalizeNcaabGameStats
    };
  }
});

// lib/redraft/dailyTeamIdentity.ts
function dailyTeamCode(sport, team, names) {
  const token = String(team ?? "").trim().toUpperCase();
  const code = names?.get(token) ?? token;
  return ALIASES[sport.toUpperCase()]?.[code] ?? code;
}
async function readDailyTeamNames(db2, sport) {
  const names = /* @__PURE__ */ new Map();
  const conflicts = /* @__PURE__ */ new Set();
  const rows = await db2.sportsTeam.findMany({ where: { sport }, select: { name: true, shortName: true } });
  for (const row of rows) {
    if (!row.shortName) continue;
    const name = row.name.trim().toUpperCase();
    const code = dailyTeamCode(sport, row.shortName);
    if (names.has(name) && names.get(name) !== code) conflicts.add(name);
    else names.set(name, code);
  }
  for (const name of conflicts) names.delete(name);
  return names;
}
var ALIASES;
var init_dailyTeamIdentity = __esm({
  "lib/redraft/dailyTeamIdentity.ts"() {
    "use strict";
    ALIASES = {
      NBA: { GS: "GSW", NY: "NYK", NO: "NOP", SA: "SAS", UTAH: "UTA", BRK: "BKN", PHO: "PHX" },
      NHL: { NJ: "NJD", LA: "LAK", SJ: "SJS", TB: "TBL", MON: "MTL", WAS: "WSH" },
      MLB: { CHW: "CWS", KCR: "KC", TBR: "TB", SDP: "SD", SFG: "SF", WAS: "WSH", OAK: "ATH" }
    };
  }
});

// lib/redraft/lineupLock.ts
function normalizeNflTeam(team) {
  const u = String(team ?? "").trim().toUpperCase();
  return NFL_TEAM_ALIASES[u] ?? NFL_FULL_TEAM_NAMES.get(u) ?? u;
}
var NFL_TEAM_ALIASES, NFL_FULL_TEAM_NAMES;
var init_lineupLock = __esm({
  "lib/redraft/lineupLock.ts"() {
    "use strict";
    init_easternGameDay();
    init_dailySportStatNormalization();
    init_dailySportSeasonStarts();
    init_gameStatus();
    init_dailyTeamIdentity();
    init_team_abbrev();
    NFL_TEAM_ALIASES = {
      JAC: "JAX",
      WSH: "WAS",
      WFT: "WAS",
      LA: "LAR",
      STL: "LAR",
      SD: "LAC",
      OAK: "LV",
      LVR: "LV",
      ARZ: "ARI",
      KCC: "KC",
      TBB: "TB",
      NOR: "NO",
      GBP: "GB",
      SFO: "SF",
      NEP: "NE"
    };
    NFL_FULL_TEAM_NAMES = new Map(getAllCanonicalTeams().map((team) => [team.fullName.toUpperCase(), team.abbrev]));
  }
});

// lib/redraft/medianGame.ts
var init_medianGame = __esm({
  "lib/redraft/medianGame.ts"() {
    "use strict";
  }
});

// lib/category-scoring/RotisserieResolver.ts
var init_RotisserieResolver = __esm({
  "lib/category-scoring/RotisserieResolver.ts"() {
    "use strict";
    init_CategoryMatchupResolver();
  }
});

// lib/redraft/standingsEngine.ts
var init_standingsEngine = __esm({
  "lib/redraft/standingsEngine.ts"() {
    "use strict";
    init_prisma();
    init_events();
    init_medianGame();
    init_leagueMode();
    init_RotisserieResolver();
  }
});

// lib/player-match/verifiedNameMatch.ts
function normalizeMatchName(name) {
  return String(name ?? "").toLowerCase().normalize("NFD").replace(/[̀-ͯ]/g, "").replace(/\s+(jr|sr|ii|iii|iv|v)\.?\s*$/, "").replace(/[^a-z0-9 ]/g, "").replace(/\s+/g, " ").trim();
}
var init_verifiedNameMatch = __esm({
  "lib/player-match/verifiedNameMatch.ts"() {
    "use strict";
  }
});

// lib/redraft/rosterGameLogIdBridge.ts
async function bridgeRosterIdsToGameLogIds(db2, sportKeys, rosterIds) {
  const toRoster = new Map(rosterIds.map((id2) => [id2, id2]));
  const numeric = rosterIds.filter((id2) => NUMERIC.test(id2));
  const ambiguous = [];
  let bridged = 0;
  const addAlias = (identityId, rosterId) => {
    const previous = toRoster.get(identityId);
    if (previous && previous !== rosterId) {
      ambiguous.push(previous, rosterId);
      return;
    }
    if (!previous) {
      toRoster.set(identityId, rosterId);
      bridged += 1;
    }
  };
  if (numeric.length > 0) {
    const [byRi, bySleeper] = await Promise.all([
      db2.playerIdentityMap.findMany({
        where: { sport: { in: sportKeys }, rollingInsightsId: { in: numeric } },
        select: { id: true, rollingInsightsId: true, sleeperId: true }
      }),
      db2.playerIdentityMap.findMany({
        where: { sport: { in: sportKeys }, sleeperId: { in: numeric } },
        select: { id: true, rollingInsightsId: true, sleeperId: true }
      })
    ]);
    const sleeperIds = new Set(bySleeper.map((r) => r.sleeperId).filter((v) => Boolean(v)));
    for (const row of byRi) {
      const rosterId = row.rollingInsightsId;
      if (!rosterId) continue;
      const clash = sleeperIds.has(rosterId) && bySleeper.some((s) => s.sleeperId === rosterId && s.id !== row.id);
      if (clash) {
        ambiguous.push(rosterId);
        continue;
      }
      addAlias(row.id, rosterId);
    }
  }
  const names = /* @__PURE__ */ new Map();
  for (const id2 of rosterIds.filter((id3) => id3.startsWith("name:"))) {
    const parts = id2.slice(5).split(":");
    if (parts.length >= 3) names.set(id2, parts.slice(0, -2).join(":"));
  }
  const sportsDbIds = rosterIds.filter((id2) => /^tsdb_\d+$/.test(id2));
  if (sportsDbIds.length && db2.sportsPlayer) for (const row of await db2.sportsPlayer.findMany({
    where: { sport: { in: sportKeys }, source: "thesportsdb", externalId: { in: sportsDbIds } },
    select: { externalId: true, name: true }
  })) names.set(row.externalId, row.name);
  if (names.size) {
    const identities = await db2.playerIdentityMap.findMany({
      where: {
        sport: { in: sportKeys },
        canonicalName: { in: [...new Set(names.values())], mode: "insensitive" }
      },
      select: { id: true, rollingInsightsId: true, sleeperId: true, canonicalName: true }
    });
    for (const [rosterId, name] of names) {
      const matches2 = identities.filter((row) => row.canonicalName && normalizeMatchName(row.canonicalName) === normalizeMatchName(name));
      if (matches2.length > 1) ambiguous.push(rosterId);
      else if (matches2[0]) addAlias(matches2[0].id, rosterId);
    }
  }
  return {
    gameLogIds: [...toRoster.keys()],
    rosterIdFor: (gameLogId) => toRoster.get(gameLogId),
    ambiguous: [...new Set(ambiguous)],
    bridged
  };
}
var NUMERIC;
var init_rosterGameLogIdBridge = __esm({
  "lib/redraft/rosterGameLogIdBridge.ts"() {
    "use strict";
    init_verifiedNameMatch();
    NUMERIC = /^\d+$/;
  }
});

// lib/sports-data/gameBoxCoverage.ts
function certifyFinalRiBox(sport, game, box, identities) {
  if (!["NBA", "NHL", "MLB"].includes(sport) || normalizeGameStatus(box.status) !== "final" || box.seasonType !== "regular" || !box.season || !box.gameDate || !box.lines.length) return null;
  const raw = record(game), full = record(raw?.full_box), players = record(raw?.player_box);
  const teams = [];
  for (const side of ["home_team", "away_team"]) {
    const team = record(full?.[side]), sideBox = record(players?.[side]), totals = record(team?.team_stats);
    if (!team || !sideBox || !totals || typeof team.abbrv !== "string" || !team.abbrv.trim() || number(team.score) == null) return null;
    teams.push(team.abbrv.trim());
    const buckets = sport === "NBA" ? [sideBox] : sport === "NHL" ? [record(sideBox.skaters), record(sideBox.goalies)] : [record(sideBox.batting), record(sideBox.pitching)];
    if (buckets.some((bucket) => !bucket || !Object.keys(bucket).length)) return null;
    for (const bucket of buckets) for (const [id2, entry] of Object.entries(bucket)) {
      const line = record(entry);
      if (!line || typeof line.player !== "string" || !line.player.trim() || !identities.has(id2) || [line.player_id, line.playerId, line.player_ID].some((value) => value != null && String(value) !== id2)) return null;
    }
    const primary = Object.values(buckets[0]).map(record);
    if (primary.some((line) => !line) || primary.length < (sport === "NBA" ? 5 : sport === "MLB" ? 9 : 5)) return null;
    const keys = sport === "NBA" ? ["field_goals_made", "field_goals_attempted", "free_throws_made", "free_throws_attempted", "three_points_made", "three_points_attempted", "assists", "steals", "blocks", "total_rebounds", "turnovers"] : sport === "NHL" ? ["shots_on_goal", "assists", "hits", "blocks"] : ["AB", "H", "R", "HR", "RBI", "BB", "SO", "SB"];
    for (const key of keys) {
      const expected = number(totals[key]), values = primary.map((line) => number(line[key]));
      if (expected == null || values.some((v) => v == null || v < 0) || values.reduce((a, v) => a + v, 0) !== expected) return null;
    }
    if (sport === "NHL") {
      const saves = Object.values(buckets[1]).map((entry) => number(record(entry)?.saves));
      if (number(totals.saves) == null || saves.some((n) => n == null) || saves.reduce((a, n) => a + n, 0) !== number(totals.saves)) return null;
    }
    if (sport === "NBA") {
      const seconds = primary.map((line) => /^\d+:\d{2}$/.test(String(line.minutes)) ? String(line.minutes).split(":").reduce((minutes, value) => minutes * 60 + Number(value), 0) : null);
      if (seconds.some((n) => n == null)) return null;
      const total = seconds.reduce((a, n) => a + n, 0);
      if (total < 14400 - 5 || Math.abs(total - 14400 - Math.round((total - 14400) / 1500) * 1500) > 5) return null;
    }
    if (sport === "MLB") {
      const other = record(full?.[side === "home_team" ? "away_team" : "home_team"]);
      const opponentTotals = record(other?.team_stats), innings = record(other?.quarter_scores);
      const pitchers = Object.values(buckets[1]).map((entry) => record(entry));
      for (const [pitcherKey, teamKey] of [["H", "H"], ["BB", "BB"], ["K", "SO"], ["R", "R"]]) {
        const values = pitchers.map((line) => number(line[pitcherKey])), expected = number(opponentTotals?.[teamKey]);
        if (expected == null || values.some((n) => n == null) || values.reduce((a, n) => a + n, 0) !== expected) return null;
      }
      if (!innings || !Object.keys(innings).length) return null;
      const inningRows = Object.entries(innings).sort(([a], [b]) => Number(a) - Number(b));
      if (inningRows.some(([key], i) => Number(key) !== i + 1)) return null;
      const played = inningRows.filter(([, runs]) => number(runs) != null).length;
      if (!played) return null;
      const outs = pitchers.map((line) => /^\d+(?:\.[012])?$/.test(String(line.IP)) ? Number(String(line.IP).split(".")[0]) * 3 + Number(String(line.IP).split(".")[1] ?? 0) : null);
      if (outs.some((n) => n == null)) return null;
      const totalOuts = outs.reduce((a, n) => a + n, 0);
      const awayInnings = Object.values(record(record(full?.away_team)?.quarter_scores) ?? {}).filter((runs) => number(runs) != null).length;
      const walkOffPossible = side === "away_team" && number(other?.score) > number(team.score) && played === awayInnings;
      if (walkOffPossible ? totalOuts < (played - 1) * 3 || totalOuts >= played * 3 : totalOuts !== played * 3) return null;
    }
  }
  if (teams[0] === teams[1] || box.lines.some((line) => !identities.has(line.providerPlayerId))) return null;
  return {
    sport,
    providerGameId: box.providerGameId,
    season: box.season,
    startTime: box.gameDate,
    homeTeam: teams[0],
    awayTeam: teams[1],
    participantIds: [...new Set(box.lines.map((line) => identities.get(line.providerPlayerId)))].sort()
  };
}
function boxCertifiesNoContribution(proof, game, canonicalPlayerId, normalizeTeam) {
  return proof.sport === game.sport && proof.season === game.season && proof.startTime instanceof Date && game.startTime instanceof Date && proof.startTime.getTime() === game.startTime.getTime() && !!normalizeTeam(proof.homeTeam) && !!normalizeTeam(proof.awayTeam) && normalizeTeam(proof.homeTeam) === normalizeTeam(game.homeTeam) && normalizeTeam(proof.awayTeam) === normalizeTeam(game.awayTeam) && Array.isArray(proof.participantIds) && proof.participantIds.length > 0 && !proof.participantIds.includes(canonicalPlayerId);
}
var record, number;
var init_gameBoxCoverage = __esm({
  "lib/sports-data/gameBoxCoverage.ts"() {
    "use strict";
    init_gameStatus();
    record = (v) => v && typeof v === "object" && !Array.isArray(v) ? v : null;
    number = (v) => (typeof v === "number" || typeof v === "string" && v.trim()) && Number.isFinite(Number(v)) ? Number(v) : null;
  }
});

// lib/sports-data/statCoverage.ts
var statCoverage_exports = {};
__export(statCoverage_exports, {
  STAT_CORRECTION_WINDOW_MS: () => STAT_CORRECTION_WINDOW_MS,
  coverageGameKey: () => coverageGameKey,
  dailyBoxCoverage: () => dailyBoxCoverage,
  pendingStatCorrectionDates: () => pendingStatCorrectionDates,
  readStatCoverage: () => readStatCoverage,
  recordNflWeeklyCoverage: () => recordNflWeeklyCoverage,
  statCoverageCorrectionsReady: () => statCoverageCorrectionsReady,
  writeStatCoverage: () => writeStatCoverage
});
function statCoverageCorrectionsReady(proof, now = /* @__PURE__ */ new Date()) {
  const first = proof.firstFinalObservedAt?.getTime(), fetched = proof.recordedAt?.getTime();
  return first != null && fetched != null && Number.isFinite(first) && Number.isFinite(fetched) && fetched >= first + STAT_CORRECTION_WINDOW_MS && fetched <= now.getTime();
}
async function pendingStatCorrectionDates(db2, sport, now = /* @__PURE__ */ new Date(), limit = 2) {
  if (!["NBA", "NHL", "MLB"].includes(sport)) return [];
  const rows = await db2.$queryRaw`SELECT
      to_char("startTime" AT TIME ZONE 'America/New_York', 'YYYY-MM-DD') AS day
    FROM sports_stat_coverage WHERE sport = ${sport} AND kind = 'game' AND source = 'rolling_insights_live'
      AND "firstFinalObservedAt" <= ${new Date(now.getTime() - STAT_CORRECTION_WINDOW_MS)}
      AND "recordedAt" < "firstFinalObservedAt" + interval '12 hours'
    GROUP BY day ORDER BY min("firstFinalObservedAt"), day LIMIT ${Math.max(1, Math.min(5, Math.floor(limit) || 2))}`;
  return rows.map((row) => row.day).filter((day) => /^\d{4}-\d{2}-\d{2}$/.test(day));
}
async function writeStatCoverage(tx, proof) {
  await tx.$executeRaw`INSERT INTO sports_stat_coverage
    (sport, source, "periodId", kind, "identitySpace", season, week, "startTime", "endTime", teams, "participantIds", "gameKeys")
    VALUES (${proof.sport}, ${proof.source}, ${proof.periodId}, ${proof.kind}, ${proof.identitySpace}, ${proof.season}, ${proof.week},
      ${proof.startTime}, ${proof.endTime}, ${proof.teams}::text[], ${proof.participantIds}::text[], ${proof.gameKeys}::text[])
    ON CONFLICT (sport, source, "periodId") DO UPDATE SET kind = EXCLUDED.kind, "identitySpace" = EXCLUDED."identitySpace",
      season = EXCLUDED.season, week = EXCLUDED.week, "startTime" = EXCLUDED."startTime", "endTime" = EXCLUDED."endTime",
      "firstFinalObservedAt" = CASE WHEN sports_stat_coverage."startTime" IS DISTINCT FROM EXCLUDED."startTime"
        OR sports_stat_coverage."gameKeys" IS DISTINCT FROM EXCLUDED."gameKeys" THEN now() ELSE sports_stat_coverage."firstFinalObservedAt" END,
      teams = EXCLUDED.teams, "participantIds" = EXCLUDED."participantIds", "gameKeys" = EXCLUDED."gameKeys", "recordedAt" = now()`;
}
function dailyBoxCoverage(proof) {
  return {
    sport: proof.sport,
    source: "rolling_insights_live",
    periodId: proof.providerGameId,
    kind: "game",
    identitySpace: "canonical",
    season: proof.season,
    week: null,
    startTime: proof.startTime,
    endTime: proof.startTime,
    teams: [proof.homeTeam, proof.awayTeam],
    participantIds: proof.participantIds,
    gameKeys: [coverageGameKey(proof, (value) => value)]
  };
}
async function recordNflWeeklyCoverage(db2, season, week, byId, now = /* @__PURE__ */ new Date()) {
  const games = pickFreshestSourceRows(await db2.sportsGame.findMany({ where: { sport: "NFL", season, week, seasonType: "regular" } })).filter((game) => !/pre|post|playoff/i.test(game.seasonType ?? "") && normalizeGameStatus(game.status) !== "cancelled");
  if (!games.length || games.some((game) => !game.startTime || normalizeGameStatus(game.status) !== "final")) return false;
  const times = games.map((game) => game.startTime.getTime());
  if (Math.max(...times) > now.getTime()) return false;
  const teams = [...new Set(games.flatMap((game) => [normalizeNflTeam(game.homeTeam), normalizeNflTeam(game.awayTeam)]))].sort();
  if (teams.some((team) => !team)) return false;
  const defenseTeams = /* @__PURE__ */ new Set();
  for (const [id2, entry] of byId) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return false;
    const raw = entry;
    if (!Object.values(raw).some((value) => typeof value === "number" && Number.isFinite(value))) return false;
    if (/^[A-Z]{2,3}$/.test(id2) && typeof raw.pts_allow === "number" && Number.isFinite(raw.pts_allow)) defenseTeams.add(normalizeNflTeam(id2));
  }
  if (teams.some((team) => !defenseTeams.has(team))) return false;
  await writeStatCoverage(db2, {
    sport: "NFL",
    source: "sleeper_weekly_stats",
    periodId: `${season}:${week}`,
    kind: "week",
    identitySpace: "sleeper",
    season,
    week,
    startTime: new Date(Math.min(...times)),
    endTime: new Date(Math.max(...times)),
    teams,
    participantIds: [...byId.keys()].sort(),
    gameKeys: games.map((game) => coverageGameKey(game, normalizeNflTeam)).sort()
  });
  return true;
}
async function readStatCoverage(db2, sport, season, week, start, end) {
  const rows = await db2.$queryRaw`SELECT sport, source, "periodId", kind, "identitySpace", season, week,
    "startTime", "endTime", teams, "participantIds", "gameKeys", "firstFinalObservedAt", "recordedAt" FROM sports_stat_coverage
    WHERE sport = ${sport} AND season = ${season} AND
      ((kind = 'game' AND "startTime" >= ${start} AND "startTime" < ${new Date(end.getTime() + 6 * 36e5)}) OR (kind = 'week' AND week = ${week}))`;
  return rows.filter((row) => row.sport === sport && row.season === season && Array.isArray(row.participantIds) && row.participantIds.length > 0 && Array.isArray(row.teams) && row.teams.length >= 2 && Array.isArray(row.gameKeys) && row.gameKeys.length > 0 && row.startTime instanceof Date && row.endTime instanceof Date);
}
var STAT_CORRECTION_WINDOW_MS, coverageGameKey;
var init_statCoverage = __esm({
  "lib/sports-data/statCoverage.ts"() {
    "use strict";
    init_gameStatus();
    init_liveSourceSelection();
    init_lineupLock();
    STAT_CORRECTION_WINDOW_MS = 12 * 36e5;
    coverageGameKey = (game, team) => JSON.stringify([team(game.homeTeam), team(game.awayTeam), game.startTime?.toISOString()]);
  }
});

// lib/redraft/lineupHistory.ts
var lineupHistory_exports = {};
__export(lineupHistory_exports, {
  historicalStartersAt: () => historicalStartersAt,
  loadHistoricalPeriod: () => loadHistoricalPeriod,
  readLineupHistory: () => readLineupHistory,
  usesLineupHistory: () => usesLineupHistory
});
function historicalStartersAt(events, instant, includeBench = false, includeReserves = false) {
  const states = /* @__PURE__ */ new Map();
  for (const event of events) {
    if (event.effectiveAtMs > instant) break;
    states.set(event.sourcePlayerRowId, event.afterState);
  }
  const active = [...states.values()].filter((p) => !!p && !p.droppedAt);
  const owners = /* @__PURE__ */ new Map();
  for (const player of active) {
    if (owners.has(player.playerId)) throw new Error("LINEUP_HISTORY_DUPLICATE_OWNERSHIP");
    owners.set(player.playerId, player.rosterId);
  }
  return active.filter((p) => !!p && !p.droppedAt && !!p.slotType && (includeReserves || !EXCLUDED.has(p.slotType.toUpperCase()) || includeBench && ["BENCH", "BN"].includes(p.slotType.toUpperCase())));
}
function usesLineupHistory(settings) {
  return !!settings && typeof settings === "object" && settings.lineup_history_version === 1;
}
async function readLineupHistory(db2, leagueId, seasonId, end) {
  const epochs = await db2.$queryRaw`
    SELECT "coveredFrom", "legacyBaseline" FROM redraft_lineup_history_epochs WHERE "leagueId" = ${leagueId}`;
  if (!epochs[0]) throw new Error("LINEUP_HISTORY_NOT_INITIALIZED");
  const events = await db2.$queryRaw`
    SELECT "sourcePlayerRowId", (extract(epoch from "effectiveAt") * 1000)::double precision AS "effectiveAtMs",
      sequence, "afterState" FROM redraft_player_state_events
    WHERE "leagueId" = ${leagueId} AND "seasonId" = ${seasonId} AND "effectiveAt" < ${end}
    ORDER BY "effectiveAt", sequence`;
  return { ...epochs[0], events };
}
async function loadHistoricalPeriod(db2, args) {
  const sport = args.sport.toUpperCase();
  const normalize = getDailySportNormalizer(sport);
  const window2 = normalize ? weekWindowFromSeasonStart(resolveDailySportSeasonStart(sport, args.season), args.week) : null;
  if (normalize && !window2) throw new Error("LINEUP_HISTORY_SEASON_START_UNKNOWN");
  const start = window2 ? new Date(window2.start) : new Date(Date.UTC(args.season, 0, 1));
  const end = window2 ? new Date(window2.end) : new Date(Date.UTC(args.season + 1, 0, 1));
  const history = await readLineupHistory(db2, args.leagueId, args.seasonId, end);
  const teamNames = normalize ? await readDailyTeamNames(db2, sport) : void 0;
  const teamCode2 = (team) => sport === "NFL" ? normalizeNflTeam(team) : dailyTeamCode(sport, team, teamNames);
  if (history.legacyBaseline && end <= history.coveredFrom) throw new Error("LINEUP_HISTORY_BEFORE_BASELINE");
  const games = await db2.sportsGame.findMany({ where: { sport, ...window2 ? { startTime: { gte: start, lt: new Date(end.getTime() + 6 * 36e5) } } : { season: args.season, week: args.week } } });
  const slate = pickFreshestSourceRows(games.map((g4) => ({ ...g4, week: args.week, season: args.season }))).filter((g4) => {
    if (!g4.startTime || normalizeGameStatus(g4.status) === "cancelled" || /postpon/i.test(g4.status ?? "")) return false;
    if (g4.seasonType && /pre|post|playoff/i.test(g4.seasonType)) return false;
    const day = easternCalendarDay(g4.startTime);
    return !window2 || !!day && day >= start && day < end;
  });
  const times = slate.map((g4) => g4.startTime.getTime());
  const firstKickoff = times.length ? Math.min(...times) : null;
  const settings = args.settings;
  const weekly = ["weekly", "weekly_first_game", "first_game", "first_game_of_week"].includes(String(settings?.sportConfig?.lineupLockType));
  const firstTeamGameOfDay = /* @__PURE__ */ new Map();
  for (const game of slate) {
    const day = easternCalendarDay(game.startTime).getTime();
    for (const team of [game.homeTeam, game.awayTeam]) {
      const key = `${day}:${teamCode2(team)}`;
      firstTeamGameOfDay.set(key, Math.min(firstTeamGameOfDay.get(key) ?? Infinity, game.startTime.getTime()));
    }
  }
  const statesAt = (kickoff, team) => {
    const day = easternCalendarDay(new Date(kickoff)).getTime();
    const dailyCutoff = normalize && team ? firstTeamGameOfDay.get(`${day}:${teamCode2(team)}`) : null;
    const instant = weekly && firstKickoff != null ? firstKickoff : dailyCutoff ?? kickoff;
    if (history.legacyBaseline && instant < history.coveredFrom.getTime()) throw new Error("LINEUP_HISTORY_BEFORE_BASELINE");
    return historicalStartersAt(history.events, instant, args.includeBench, args.includeReserves);
  };
  const eligible = /* @__PURE__ */ new Map();
  const expectedBoxes = /* @__PURE__ */ new Map();
  for (const game of slate) for (const gameTeam of [game.homeTeam, game.awayTeam]) for (const player of statesAt(game.startTime.getTime(), gameTeam)) {
    const team = teamCode2(player.team);
    if (team === teamCode2(gameTeam)) {
      eligible.set(`${player.rosterId}:${player.playerId}`, player);
      expectedBoxes.set(`${game.id}:${player.rosterId}:${player.playerId}`, { player, game });
    }
  }
  const contributions = [];
  const missing = /* @__PURE__ */ new Set();
  const proofs = await readStatCoverage(db2, sport, args.season, args.week, start, end);
  if (!normalize) {
    const unplaced = args.includeReserves ? [...new Map(times.flatMap((time) => statesAt(time)).filter((p) => !eligible.has(`${p.rosterId}:${p.playerId}`)).map((p) => [`${p.rosterId}:${p.playerId}`, p])).values()] : [];
    const ids = [...new Set([...eligible.values(), ...unplaced].map((p) => p.playerId))];
    const rows = await db2.playerWeeklyScore.findMany({ where: { sport, season: args.season, week: args.week, playerId: { in: ids } } });
    const gameKeys = slate.map((game) => coverageGameKey(game, normalizeNflTeam)).sort();
    const proof = proofs.find((row) => row.kind === "week" && row.identitySpace === "sleeper" && row.source === "sleeper_weekly_stats" && row.week === args.week && JSON.stringify(row.gameKeys) === JSON.stringify(gameKeys));
    const verified = proof ? new Set((await db2.playerIdentityMap.findMany({
      where: { sport, sleeperId: { in: ids } },
      select: { sleeperId: true }
    })).flatMap((row) => row.sleeperId ? [row.sleeperId] : [])) : /* @__PURE__ */ new Set();
    for (const player of unplaced) if (!(proof && verified.has(player.playerId) && !proof.participantIds.includes(player.playerId))) missing.add(player.playerId);
    for (const player of eligible.values()) {
      const row = rows.find((r) => r.playerId === player.playerId);
      if (row?.stats && typeof row.stats === "object" && !Array.isArray(row.stats) && Object.keys(row.stats).length)
        contributions.push({ player, stats: row.stats, gameId: `week:${args.week}` });
      else if (!(proof && verified.has(player.playerId) && !proof.participantIds.includes(player.playerId))) missing.add(player.playerId);
    }
  } else {
    const coveredBoxes = /* @__PURE__ */ new Set();
    const ids = [...new Set(history.events.flatMap((e) => e.afterState ? [e.afterState.playerId] : []))];
    const bridge = await bridgeRosterIdsToGameLogIds(db2, [sport], ids);
    if (bridge.ambiguous.length) throw new Error("LINEUP_HISTORY_AMBIGUOUS_PLAYER_ID");
    const rows = await db2.playerGameStat.findMany({ where: { sportType: sport, playerId: { in: bridge.gameLogIds }, gameDate: { gte: start, lt: end } } });
    for (const row of rows) {
      const raw = row.normalizedStatMap;
      if (raw?.seasonType === "pre" || raw?.seasonType === "post") continue;
      const candidates = slate.filter((g4) => g4.externalId === row.providerGameId || g4.externalId === row.gameId || row.gameDate && easternCalendarDay(g4.startTime)?.getTime() === row.gameDate.getTime() && (teamCode2(row.team) === teamCode2(g4.homeTeam) || teamCode2(row.team) === teamCode2(g4.awayTeam)));
      const storedTime = typeof raw?.gameStartTimeUtc === "string" ? Date.parse(raw.gameStartTimeUtc) : NaN;
      const kickoff = Number.isFinite(storedTime) ? storedTime : candidates.length === 1 ? candidates[0].startTime.getTime() : null;
      if (kickoff == null) throw new Error("LINEUP_HISTORY_GAME_TIME_UNRESOLVED");
      const exactGame = candidates.find((g4) => (g4.externalId === row.providerGameId || g4.externalId === row.gameId) && g4.startTime.getTime() === kickoff) ?? candidates.find((g4) => g4.startTime.getTime() === kickoff);
      if (!exactGame) throw new Error("LINEUP_HISTORY_GAME_UNRESOLVED");
      const playerId = bridge.rosterIdFor(row.playerId);
      for (const player of statesAt(kickoff, row.team).filter((p) => p.playerId === playerId)) {
        const boxKey = `${exactGame.id}:${player.rosterId}:${player.playerId}`;
        expectedBoxes.set(boxKey, { player, game: exactGame });
        if (sport === "MLB") {
          const pitchingSlot = ["P", "SP", "RP"].includes(player.slotType.toUpperCase());
          if (raw.group === "pitching" !== pitchingSlot) {
            coveredBoxes.add(boxKey);
            continue;
          }
        }
        const payload = row.statPayload;
        const stats = normalize({ ...raw, timeOnIce: raw.timeOnIce ?? payload?.time_on_ice }).stats;
        if (!Object.keys(stats).length) {
          missing.add(player.playerId);
          continue;
        }
        coveredBoxes.add(boxKey);
        eligible.set(`${player.rosterId}:${player.playerId}`, player);
        contributions.push({ player, stats, gameId: row.gameId });
      }
    }
    const gameProofs = proofs.filter((proof) => proof.kind === "game" && proof.identitySpace === "canonical" && proof.source === "rolling_insights_live");
    const canonicalByRoster = /* @__PURE__ */ new Map();
    if (gameProofs.length) for (const row of await db2.playerIdentityMap.findMany({ where: { sport, id: { in: bridge.gameLogIds } }, select: { id: true } })) {
      const rosterId = bridge.rosterIdFor(row.id);
      if (rosterId) canonicalByRoster.set(rosterId, row.id);
    }
    for (const [boxKey, { player, game }] of expectedBoxes) {
      if (coveredBoxes.has(boxKey)) continue;
      const canonicalId = canonicalByRoster.get(player.playerId);
      const matching = canonicalId ? gameProofs.filter((proof) => boxCertifiesNoContribution(
        {
          sport: proof.sport,
          providerGameId: proof.periodId,
          season: proof.season,
          startTime: proof.startTime,
          homeTeam: proof.teams[0],
          awayTeam: proof.teams[1],
          participantIds: proof.participantIds
        },
        game,
        canonicalId,
        teamCode2
      )) : [];
      if (matching.length !== 1) missing.add(player.playerId);
    }
  }
  const slateKeys = slate.map((game) => coverageGameKey(game, teamCode2)).sort();
  const correctionGames = [...new Map([...expectedBoxes.values()].map(({ game }) => [game.id, game])).values()];
  const correctionsReady = slate.length > 0 && (normalize ? correctionGames.every((game) => proofs.some((proof) => proof.kind === "game" && proof.identitySpace === "canonical" && proof.source === "rolling_insights_live" && proof.startTime.getTime() === game.startTime.getTime() && proof.gameKeys.some((key) => {
    try {
      const [home, away, time] = JSON.parse(key);
      return JSON.stringify([teamCode2(home), teamCode2(away), time]) === coverageGameKey(game, teamCode2);
    } catch {
      return false;
    }
  }) && statCoverageCorrectionsReady(proof))) : proofs.some((proof) => proof.kind === "week" && proof.identitySpace === "sleeper" && proof.source === "sleeper_weekly_stats" && proof.week === args.week && JSON.stringify(proof.gameKeys) === JSON.stringify(slateKeys) && statCoverageCorrectionsReady(proof)));
  return {
    contributions,
    eligible: [...eligible.values()],
    missingPlayerIds: [...missing],
    end,
    correctionsReady,
    slateFinal: correctionsReady && slate.length > 0 && slate.every((g4) => normalizeGameStatus(g4.status) === "final") && Math.max(...times) + 12 * 36e5 <= Date.now() && (!normalize || end.getTime() <= (easternCalendarDay(/* @__PURE__ */ new Date())?.getTime() ?? 0))
  };
}
var EXCLUDED;
var init_lineupHistory = __esm({
  "lib/redraft/lineupHistory.ts"() {
    "use strict";
    init_dailySportStatNormalization();
    init_dailySportSeasonStarts();
    init_rosterGameLogIdBridge();
    init_lineupLock();
    init_gameStatus();
    init_easternGameDay();
    init_liveSourceSelection();
    init_dailyTeamIdentity();
    init_gameBoxCoverage();
    init_statCoverage();
    EXCLUDED = /* @__PURE__ */ new Set(["BENCH", "BN", "IR", "TAXI", "DEVY", "RESERVE"]);
  }
});

// lib/providers/providerFallbackPolicy.ts
var init_providerFallbackPolicy = __esm({
  "lib/providers/providerFallbackPolicy.ts"() {
    "use strict";
    init_sport_scope();
  }
});

// lib/player-data/providerFallbackDiagnostics.ts
var init_providerFallbackDiagnostics = __esm({
  "lib/player-data/providerFallbackDiagnostics.ts"() {
    "use strict";
    init_providerFallbackPolicy();
  }
});

// lib/waiver-wire/roster-utils.ts
var init_roster_utils = __esm({
  "lib/waiver-wire/roster-utils.ts"() {
    "use strict";
  }
});

// lib/sport-teams/normalizeProviderPosition.ts
var init_normalizeProviderPosition = __esm({
  "lib/sport-teams/normalizeProviderPosition.ts"() {
    "use strict";
  }
});

// lib/draft-room/currentFantasyPoolPlayer.ts
var init_currentFantasyPoolPlayer = __esm({
  "lib/draft-room/currentFantasyPoolPlayer.ts"() {
    "use strict";
  }
});

// lib/redraft/teamDefenseIdentity.ts
var init_teamDefenseIdentity = __esm({
  "lib/redraft/teamDefenseIdentity.ts"() {
    "use strict";
    init_lineupLock();
  }
});

// lib/db-first-mode.ts
function readEnvBool(name, fallback = false) {
  const value = (process.env[name] ?? "").trim().toLowerCase();
  if (!value) return fallback;
  if (value === "1" || value === "true" || value === "yes" || value === "on") return true;
  if (value === "0" || value === "false" || value === "no" || value === "off") return false;
  return fallback;
}
function readEnvInt(name, fallback) {
  const raw = (process.env[name] ?? "").trim();
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) return fallback;
  return parsed;
}
var dbFirstMode;
var init_db_first_mode = __esm({
  "lib/db-first-mode.ts"() {
    "use strict";
    dbFirstMode = {
      useDbCacheOnly: readEnvBool("AF_USE_DB_CACHE_ONLY", false),
      disableLiveApiOnPageLoad: readEnvBool("AF_DISABLE_LIVE_API_ON_PAGE_LOAD", false),
      disableImageLookupOnPageLoad: readEnvBool("AF_DISABLE_IMAGE_LOOKUP_ON_PAGE_LOAD", false),
      disableAdpLiveMergeOnPageLoad: readEnvBool("AF_DISABLE_ADP_LIVE_MERGE_ON_PAGE_LOAD", false),
      disableStatsLiveMergeOnPageLoad: readEnvBool("AF_DISABLE_STATS_LIVE_MERGE_ON_PAGE_LOAD", false),
      mockAiCacheFirst: readEnvBool("AF_MOCK_AI_CACHE_FIRST", false),
      disableAiLiveCalls: readEnvBool("AF_DISABLE_AI_LIVE_CALLS", false),
      draftPoolCacheTtlSeconds: Math.max(1, readEnvInt("AF_DRAFT_POOL_CACHE_TTL_SECONDS", 300))
    };
  }
});

// lib/ttl-memo.ts
function createTtlMemo(options) {
  const store = /* @__PURE__ */ new Map();
  const enabled = options.enabled ?? defaultEnabled;
  const ttl = () => typeof options.ttlMs === "function" ? options.ttlMs() : options.ttlMs;
  return {
    get(key) {
      if (!enabled()) return void 0;
      const hit = store.get(key);
      if (!hit) return void 0;
      if (hit.expiresAt <= Date.now()) {
        store.delete(key);
        return void 0;
      }
      return hit.value;
    },
    set(key, value) {
      if (!enabled()) return;
      const now = Date.now();
      for (const [k, v] of store) if (v.expiresAt <= now) store.delete(k);
      store.delete(key);
      while (store.size >= options.maxEntries) {
        const oldest = store.keys().next().value;
        if (oldest === void 0) break;
        store.delete(oldest);
      }
      store.set(key, { value, expiresAt: now + ttl() });
    },
    clear() {
      store.clear();
    },
    get size() {
      return store.size;
    }
  };
}
var defaultEnabled;
var init_ttl_memo = __esm({
  "lib/ttl-memo.ts"() {
    "use strict";
    defaultEnabled = () => process.env.NODE_ENV !== "test" || process.env.AF_TTL_MEMO_IN_TESTS === "1";
  }
});

// lib/sport-teams/SportPlayerPoolResolver.ts
var poolTtlMs, playerPoolMemo, adpRankMemo;
var init_SportPlayerPoolResolver = __esm({
  "lib/sport-teams/SportPlayerPoolResolver.ts"() {
    "use strict";
    init_normalizeProviderPosition();
    init_currentFantasyPoolPlayer();
    init_prisma();
    init_SportConfigResolver();
    init_SportTeamMetadataRegistry();
    init_teamDefenseIdentity();
    init_db_first_mode();
    init_ttl_memo();
    poolTtlMs = () => dbFirstMode.draftPoolCacheTtlSeconds * 1e3;
    playerPoolMemo = createTtlMemo({ ttlMs: poolTtlMs, maxEntries: 64 });
    adpRankMemo = createTtlMemo({ ttlMs: poolTtlMs, maxEntries: 16 });
  }
});

// lib/player-media-urls.ts
var init_player_media_urls = __esm({
  "lib/player-media-urls.ts"() {
    "use strict";
    init_SportTeamMetadataRegistry();
  }
});

// lib/draft-sports-models/player-asset-resolver.ts
var FALLBACK_HEADSHOT_PLACEHOLDER, FALLBACK_TEAM_LOGO_PLACEHOLDER, CACHE_TTL_MS;
var init_player_asset_resolver = __esm({
  "lib/draft-sports-models/player-asset-resolver.ts"() {
    "use strict";
    init_SportTeamMetadataRegistry();
    init_player_media_urls();
    FALLBACK_HEADSHOT_PLACEHOLDER = `data:image/svg+xml;utf8,${encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96"><rect width="96" height="96" rx="48" fill="#1f2937"/><text x="50%" y="52%" dominant-baseline="middle" text-anchor="middle" fill="#cbd5e1" font-family="Arial, sans-serif" font-size="28" font-weight="700">AF</text></svg>'
    )}`;
    FALLBACK_TEAM_LOGO_PLACEHOLDER = `data:image/svg+xml;utf8,${encodeURIComponent(
      '<svg xmlns="http://www.w3.org/2000/svg" width="96" height="96" viewBox="0 0 96 96"><rect width="96" height="96" rx="14" fill="#0f172a"/><rect x="10" y="10" width="76" height="76" rx="10" fill="#1e293b"/><text x="50%" y="53%" dominant-baseline="middle" text-anchor="middle" fill="#93c5fd" font-family="Arial, sans-serif" font-size="24" font-weight="700">TM</text></svg>'
    )}`;
    CACHE_TTL_MS = 6 * 60 * 60 * 1e3;
  }
});

// lib/players/getTeamLogo.ts
var init_getTeamLogo = __esm({
  "lib/players/getTeamLogo.ts"() {
    "use strict";
    init_player_asset_resolver();
    init_sport_scope();
  }
});

// lib/player-media.ts
var CACHE_TTL_MS2;
var init_player_media = __esm({
  "lib/player-media.ts"() {
    "use strict";
    init_player_media_urls();
    init_player_media_urls();
    CACHE_TTL_MS2 = 6 * 60 * 60 * 1e3;
  }
});

// lib/providers/nflRookieSourcePolicy.ts
var init_nflRookieSourcePolicy = __esm({
  "lib/providers/nflRookieSourcePolicy.ts"() {
    "use strict";
  }
});

// lib/draft-room/collegeClass.ts
var init_collegeClass = __esm({
  "lib/draft-room/collegeClass.ts"() {
    "use strict";
  }
});

// lib/draft-room/classify-avatar-source.ts
var init_classify_avatar_source = __esm({
  "lib/draft-room/classify-avatar-source.ts"() {
    "use strict";
  }
});

// lib/providers/rollingInsightsSoccerLeague.ts
function compactKey(s) {
  return s.trim().replace(WS, "_").toUpperCase();
}
function normalizeSoccerLeague(input) {
  if (input == null) return null;
  const raw = String(input).trim();
  if (!raw) return null;
  const u = compactKey(raw);
  const lower = raw.toLowerCase();
  if (u === "EPL" || u === "PREMIER_LEAGUE" || u === "ENGLISH_PREMIER_LEAGUE" || lower.includes("premier league")) {
    return "EPL";
  }
  if (u === "LALIGA" || u === "LA_LIGA" || u === "LA_LIGA_SPAIN" || lower === "laliga" || lower.includes("la liga")) {
    return "LALIGA";
  }
  if (u === "SERIEA" || u === "SERIE_A" || u === "ITALIAN_SERIE_A" || lower.includes("serie a") || lower.includes("serie") && lower.includes("ital")) {
    return "SERIEA";
  }
  return null;
}
var WS;
var init_rollingInsightsSoccerLeague = __esm({
  "lib/providers/rollingInsightsSoccerLeague.ts"() {
    "use strict";
    WS = /\s+/g;
  }
});

// lib/player-data/sportSpecificPlayerFields.ts
var init_sportSpecificPlayerFields = __esm({
  "lib/player-data/sportSpecificPlayerFields.ts"() {
    "use strict";
  }
});

// lib/player-data/providerExperienceFields.ts
var init_providerExperienceFields = __esm({
  "lib/player-data/providerExperienceFields.ts"() {
    "use strict";
  }
});

// lib/player-data/playerExperience.ts
var init_playerExperience = __esm({
  "lib/player-data/playerExperience.ts"() {
    "use strict";
    init_sport_scope();
    init_providerExperienceFields();
    init_collegeClass();
    init_nflRookieSourcePolicy();
  }
});

// lib/player-data/unifiedPlayerProductView.ts
var init_unifiedPlayerProductView = __esm({
  "lib/player-data/unifiedPlayerProductView.ts"() {
    "use strict";
    init_nflRookieSourcePolicy();
    init_collegeClass();
    init_classify_avatar_source();
    init_rollingInsightsSoccerLeague();
    init_sportSpecificPlayerFields();
    init_playerExperience();
  }
});

// lib/draft-sports-models/normalize-draft-player.ts
var init_normalize_draft_player = __esm({
  "lib/draft-sports-models/normalize-draft-player.ts"() {
    "use strict";
    init_player_asset_resolver();
    init_sport_scope();
  }
});

// lib/player-data/normalizeProviderPlayer.ts
var init_normalizeProviderPlayer = __esm({
  "lib/player-data/normalizeProviderPlayer.ts"() {
    "use strict";
    init_normalize_draft_player();
    init_unifiedPlayerProductView();
    init_player_asset_resolver();
    init_rollingInsightsSoccerLeague();
  }
});

// lib/player-data/getPlayerDataForSurface.ts
var init_getPlayerDataForSurface = __esm({
  "lib/player-data/getPlayerDataForSurface.ts"() {
    "use strict";
    init_prisma();
    init_roster_utils();
    init_SportPlayerPoolResolver();
    init_getTeamLogo();
    init_player_media();
    init_unifiedPlayerProductView();
    init_normalizeProviderPlayer();
  }
});

// lib/player-data/getNormalizedPlayerData.ts
var init_getNormalizedPlayerData = __esm({
  "lib/player-data/getNormalizedPlayerData.ts"() {
    "use strict";
    init_providerFallbackDiagnostics();
    init_getPlayerDataForSurface();
  }
});

// lib/player-data/nflRedraftCanonicalPlayer.ts
var STALE_HOURS;
var init_nflRedraftCanonicalPlayer = __esm({
  "lib/player-data/nflRedraftCanonicalPlayer.ts"() {
    "use strict";
    init_getTeamLogo();
    init_team_abbrev();
    STALE_HOURS = {
      stats: 24 * 7,
      projections: 24 * 3,
      injury: 24 * 2,
      news: 24 * 3
    };
  }
});

// lib/player-data/nflRedraftPlayerMetadata.ts
var init_nflRedraftPlayerMetadata = __esm({
  "lib/player-data/nflRedraftPlayerMetadata.ts"() {
    "use strict";
    init_team_abbrev();
  }
});

// lib/nfl-provider/nflRedraftProviderFoundation.ts
var NFL_REDRAFT_RATE_LIMITS;
var init_nflRedraftProviderFoundation = __esm({
  "lib/nfl-provider/nflRedraftProviderFoundation.ts"() {
    "use strict";
    NFL_REDRAFT_RATE_LIMITS = {
      api_sports: { providerId: "api_sports", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      clearsports: { providerId: "clearsports", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      rolling_insights: { providerId: "rolling_insights", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      fantasycalc: { providerId: "fantasycalc", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      sportsdataio: { providerId: "sportsdataio", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      sleeper: { providerId: "sleeper", maxRequestsPerMinute: 900, burst: 50, retryBackoffMs: 5e3 },
      espn: { providerId: "espn", maxRequestsPerMinute: 120, burst: 20, retryBackoffMs: 1e4 },
      thesportsdb: { providerId: "thesportsdb", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 3e4 },
      openweather: { providerId: "openweather", maxRequestsPerMinute: 60, burst: 10, retryBackoffMs: 6e4 },
      deterministic: { providerId: "deterministic", maxRequestsPerMinute: Number.POSITIVE_INFINITY, burst: Number.POSITIVE_INFINITY, retryBackoffMs: 0 }
    };
  }
});

// lib/player-data/nflRedraftPlayerIntelligence.ts
var init_nflRedraftPlayerIntelligence = __esm({
  "lib/player-data/nflRedraftPlayerIntelligence.ts"() {
    "use strict";
    init_nflRedraftProviderFoundation();
  }
});

// lib/player-data/nflRedraftGameContext.ts
var init_nflRedraftGameContext = __esm({
  "lib/player-data/nflRedraftGameContext.ts"() {
    "use strict";
    init_nflRedraftProviderFoundation();
    init_team_abbrev();
  }
});

// lib/player-data/nflRedraftLiveScoringContext.ts
var init_nflRedraftLiveScoringContext = __esm({
  "lib/player-data/nflRedraftLiveScoringContext.ts"() {
    "use strict";
    init_nflRedraftProviderFoundation();
  }
});

// lib/player-data/serializeUnifiedPlayerForApi.ts
var init_serializeUnifiedPlayerForApi = __esm({
  "lib/player-data/serializeUnifiedPlayerForApi.ts"() {
    "use strict";
    init_getTeamLogo();
    init_teamDefenseIdentity();
    init_nflRedraftCanonicalPlayer();
    init_nflRedraftPlayerMetadata();
    init_nflRedraftPlayerIntelligence();
    init_nflRedraftGameContext();
    init_nflRedraftLiveScoringContext();
  }
});

// lib/scoring-runtime/resolveNflRedraftLiveScoringRuntime.ts
var init_resolveNflRedraftLiveScoringRuntime = __esm({
  "lib/scoring-runtime/resolveNflRedraftLiveScoringRuntime.ts"() {
    "use strict";
    init_prisma();
    init_prisma_json();
    init_league_runtime();
    init_rosterConfigResolver();
    init_lineupLock();
    init_lineupValidation();
    init_standingsEngine();
    init_lineupHistory();
    init_scoringEngine2();
    init_getNormalizedPlayerData();
    init_serializeUnifiedPlayerForApi();
    init_canonicalNflRedraftScoringRuntime();
  }
});

// lib/scoring-runtime/index.ts
var init_scoring_runtime = __esm({
  "lib/scoring-runtime/index.ts"() {
    "use strict";
    init_canonicalNflRedraftScoringRuntime();
    init_nflStatNormalization();
    init_resolveNflRedraftLiveScoringRuntime();
  }
});

// lib/sports-evidence/gates.ts
function isSportsDataEnabled(subsystem) {
  return process.env[ENV_KEY[subsystem]] === "true";
}
var import_server_only6, ENV_KEY;
var init_gates = __esm({
  "lib/sports-evidence/gates.ts"() {
    "use strict";
    import_server_only6 = __toESM(require_audit_server_only_shim());
    ENV_KEY = {
      lineup: "FANTASY_OS_SPORTS_DATA_LINEUP_ENABLED",
      waiver: "FANTASY_OS_SPORTS_DATA_WAIVER_ENABLED",
      trade: "FANTASY_OS_SPORTS_DATA_TRADE_ENABLED",
      draft: "FANTASY_OS_SPORTS_DATA_DRAFT_ENABLED",
      matchup: "FANTASY_OS_SPORTS_DATA_MATCHUP_ENABLED",
      // Phase 5E-g — Scoring is independently reversible from Matchup (added to the registry, disabled by default).
      scoring: "FANTASY_OS_SPORTS_DATA_SCORING_ENABLED",
      intelligence: "FANTASY_OS_SPORTS_DATA_INTELLIGENCE_ENABLED",
      coach: "FANTASY_OS_SPORTS_DATA_COACH_ENABLED",
      // Phase 5E-h — operator observability surface, independently reversible from customer intelligence.
      observability: "FANTASY_OS_SPORTS_DATA_OBSERVABILITY_ENABLED"
    };
  }
});

// lib/sports-data-gateway/runtime/checksum.ts
function snapshotChecksum(records) {
  const sorted = [...records].sort((a, b) => a.canonicalKey.localeCompare(b.canonicalKey));
  return sha256(sorted.map((r) => `${r.canonicalKey}:${r.contentHash}`).join("|"));
}
function sha256(s) {
  return import_node_crypto3.default.createHash("sha256").update(s).digest("hex");
}
var import_node_crypto3;
var init_checksum = __esm({
  "lib/sports-data-gateway/runtime/checksum.ts"() {
    "use strict";
    import_node_crypto3 = __toESM(require("node:crypto"));
  }
});

// lib/sports-data-gateway/runtime/snapshot.ts
function countSnapshot(draft) {
  return {
    recordCount: draft.records.length,
    resolvedCount: draft.records.filter((r) => r.resolutionStatus === "resolved").length,
    ambiguousCount: draft.records.filter((r) => r.resolutionStatus === "ambiguous").length,
    unresolvedCount: draft.records.filter((r) => r.resolutionStatus === "unresolved" || r.resolutionStatus === "conflicting").length,
    rejectedCount: draft.rejectedCount
  };
}
function canCertify(draft) {
  const reasons = [];
  if (draft.runPartial) reasons.push("run is partial");
  if (!draft.scopeComplete) reasons.push("scope is incomplete");
  if (!draft.generatedAt) reasons.push("missing provenance (generatedAt)");
  if (draft.records.some((r) => !r.schemaValid)) reasons.push("one or more records failed schema validation");
  const classified = draft.records.every((r) => ["resolved", "ambiguous", "unresolved", "conflicting"].includes(r.resolutionStatus));
  if (!classified) reasons.push("one or more identity outcomes are unclassified");
  if (draft.rejectedCount > 0 && draft.limitations.length === 0) reasons.push("unexplained rejects (no limitations recorded)");
  if (reasons.length > 0) return { certifiable: false, reasons };
  const checksum = snapshotChecksum(draft.records.map((r) => ({ canonicalKey: r.canonicalKey, contentHash: r.contentHash })));
  return { certifiable: true, checksum, counts: countSnapshot(draft) };
}
var init_snapshot = __esm({
  "lib/sports-data-gateway/runtime/snapshot.ts"() {
    "use strict";
    init_checksum();
  }
});

// lib/sports-data-gateway/runtime/store.ts
function gate() {
  if (process.env.FANTASY_OS_EXEC_ENABLED !== "true") return { ok: false, detail: "FANTASY_OS_EXEC_ENABLED != true" };
  const url = process.env.FANTASY_OS_EXEC_DATABASE_URL;
  if (!url) return { ok: false, detail: "FANTASY_OS_EXEC_DATABASE_URL not set" };
  return { ok: true, url };
}
async function getPool() {
  const g4 = gate();
  if (!g4.ok) throw new Error(`sports runtime store disabled: ${g4.detail}`);
  if (!poolPromise) {
    poolPromise = (async () => {
      const mod = await import("pg");
      const Pool = mod.Pool ?? mod.default?.Pool;
      if (!Pool) return null;
      return new Pool({ connectionString: g4.url, max: 3, statement_timeout: 15e3 });
    })();
  }
  const p = await poolPromise;
  if (!p) throw new Error("sports runtime store pool unavailable");
  return p;
}
var import_server_only7, poolPromise, SportsRuntimeStore;
var init_store = __esm({
  "lib/sports-data-gateway/runtime/store.ts"() {
    "use strict";
    import_server_only7 = __toESM(require_audit_server_only_shim());
    init_snapshot();
    poolPromise = null;
    SportsRuntimeStore = class {
      // ── SyncLock (leased; stale-lease recovery via expires_at) ────────────────────
      async acquire(key, leaseMs, now) {
        const pool = await getPool();
        const token = `${now.getTime()}-${Math.random().toString(36).slice(2, 10)}`;
        const expires = new Date(now.getTime() + leaseMs).toISOString();
        const res = await pool.query(
          `INSERT INTO sports_data.sync_lock (lock_key, token, expires_at, acquired_at)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (lock_key) DO UPDATE SET token=EXCLUDED.token, expires_at=EXCLUDED.expires_at, acquired_at=EXCLUDED.acquired_at
       WHERE sports_data.sync_lock.expires_at < $4
       RETURNING token`,
          [key, token, expires, now.toISOString()]
        );
        return res.rows[0] ? { acquired: true, token } : { acquired: false };
      }
      async release(key, token) {
        const pool = await getPool();
        await pool.query(`DELETE FROM sports_data.sync_lock WHERE lock_key=$1 AND token=$2`, [key, token]);
      }
      // ── SyncStore ─────────────────────────────────────────────────────────────────
      async getCheckpoint(runKey, scope) {
        const pool = await getPool();
        const r = await pool.query(`SELECT checkpoint FROM sports_data.sync_checkpoint WHERE run_key=$1 AND scope=$2`, [runKey, scope]);
        return r.rows[0]?.checkpoint ? String(r.rows[0].checkpoint) : null;
      }
      async saveCheckpoint(runKey, scope, checkpoint2) {
        const pool = await getPool();
        await pool.query(
          `INSERT INTO sports_data.sync_checkpoint (run_key, scope, checkpoint, updated_at) VALUES ($1,$2,$3,now())
       ON CONFLICT (run_key, scope) DO UPDATE SET checkpoint=EXCLUDED.checkpoint, updated_at=now()`,
          [runKey, scope, checkpoint2]
        );
      }
      async setLastSuccessfulSyncAt(runKey, iso) {
        const pool = await getPool();
        await pool.query(
          `INSERT INTO sports_data.sync_freshness (run_key, last_successful_sync_at, updated_at) VALUES ($1,$2,now())
       ON CONFLICT (run_key) DO UPDATE SET last_successful_sync_at=EXCLUDED.last_successful_sync_at, updated_at=now()`,
          [runKey, iso]
        );
      }
      async getLastSuccessfulSyncAt(runKey) {
        const pool = await getPool();
        const r = await pool.query(`SELECT last_successful_sync_at FROM sports_data.sync_freshness WHERE run_key=$1`, [runKey]);
        return r.rows[0]?.last_successful_sync_at ? String(r.rows[0].last_successful_sync_at) : null;
      }
      async recordRun(result) {
        const pool = await getPool();
        const a = result.accounting;
        await pool.query(
          `INSERT INTO sports_data.sync_run (run_id, run_key, sport, season_state, status, started_at, finished_at, checkpoint, request_attempts, logical_requests, retries, cache_hits, successful, not_found, permanent_failures, imported, unchanged, rejected, advanced_freshness, warnings)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20::jsonb)
       ON CONFLICT (run_id) DO NOTHING`,
          [result.runKey + ":" + result.startedAt, result.runKey, null, result.seasonState, result.status, result.startedAt, result.finishedAt, JSON.stringify(result.checkpoint), a.requestAttempts, a.logicalRequests, a.retries, a.cacheHits, a.successful, a.notFound, a.permanentFailures, a.imported, a.unchanged, a.rejected, result.advancedFreshness, JSON.stringify(result.warnings)]
        );
      }
      // ── Snapshot + event persistence ────────────────────────────────────────────
      /** Previous certified content hashes (canonicalKey → contentHash) for change detection. */
      async previousCertifiedHashes(sport, capability, scopeRef = null) {
        const pool = await getPool();
        const snap = await pool.query(
          `SELECT snapshot_id FROM sports_data.sports_snapshot WHERE sport=$1 AND capability=$2 AND scope_ref IS NOT DISTINCT FROM $3 AND status='certified' ORDER BY generated_at DESC LIMIT 1`,
          [sport, capability, scopeRef]
        );
        const id2 = snap.rows[0]?.snapshot_id ? String(snap.rows[0].snapshot_id) : null;
        const hashes = /* @__PURE__ */ new Map();
        if (id2) {
          const recs = await pool.query(`SELECT canonical_key, record->'__contentHash' AS h FROM sports_data.sports_snapshot_record WHERE snapshot_id=$1`, [id2]);
          for (const r of recs.rows) if (r.h) hashes.set(String(r.canonical_key), String(r.h).replace(/"/g, ""));
        }
        return { snapshotId: id2, hashes };
      }
      /** Persist a certifiable snapshot draft (append-only) + its records. Returns counts. Never updates in place. */
      async persistCertifiedSnapshot(draft) {
        const decision = canCertify(draft);
        if (!decision.certifiable) return { certified: false, reasons: decision.reasons };
        const pool = await getPool();
        const counts = decision.counts;
        await pool.query(
          `INSERT INTO sports_data.sports_snapshot (snapshot_id, version, sport, capability, scope_ref, provider, status, generated_at, source_updated_at, record_count, resolved_count, ambiguous_count, unresolved_count, rejected_count, checksum, previous_snapshot_id, limitations)
       VALUES ($1,$2,$3,$4,$5,$6,'certified',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb)
       ON CONFLICT (snapshot_id) DO NOTHING`,
          [draft.snapshotId, draft.version, draft.sport, draft.capability, draft.scopeRef ?? null, draft.provider, draft.generatedAt, draft.sourceUpdatedAt, counts.recordCount, counts.resolvedCount, counts.ambiguousCount, counts.unresolvedCount, counts.rejectedCount, decision.checksum, draft.previousSnapshotId, JSON.stringify(draft.limitations)]
        );
        for (const r of draft.records) {
          const stored = { ...r.record, __contentHash: r.contentHash };
          await pool.query(
            `INSERT INTO sports_data.sports_snapshot_record (snapshot_id, canonical_key, resolution_status, record) VALUES ($1,$2,$3,$4::jsonb)
         ON CONFLICT (snapshot_id, canonical_key) DO NOTHING`,
            [draft.snapshotId, r.canonicalKey, r.resolutionStatus, JSON.stringify(stored)]
          );
        }
        return { certified: true, checksum: decision.checksum };
      }
      /** Idempotent event insert (ON CONFLICT on deterministic event_id → no duplicates on rerun). */
      async insertEvents(events, meta) {
        if (events.length === 0) return 0;
        const pool = await getPool();
        let inserted = 0;
        for (const e of events) {
          const res = await pool.query(
            `INSERT INTO sports_data.sports_event (event_id, event_type, sport, entity_id, occurred_at, observed_at, provider, snapshot_version, payload, provenance)
         VALUES ($1,$2,$3,$4,$5,now(),$6,$7,$8::jsonb,$9::jsonb)
         ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
            [e.eventId, e.eventType, meta.sport, e.entityId, meta.occurredAt, meta.provider, meta.snapshotVersion, JSON.stringify(e.record), JSON.stringify({ provider: meta.provider, snapshotVersion: meta.snapshotVersion })]
          );
          if (res.rows[0]) inserted++;
        }
        return inserted;
      }
      /** Latest CERTIFIED snapshot metadata (never building/partial/rejected). Deterministic latest = newest generated_at. */
      async getCertifiedSnapshotMeta(sport, capability, scopeRef = null) {
        const pool = await getPool();
        const snap = await pool.query(
          `SELECT snapshot_id, version, checksum, provider, generated_at, source_updated_at, record_count, resolved_count, ambiguous_count, unresolved_count, rejected_count, limitations
       FROM sports_data.sports_snapshot WHERE sport=$1 AND capability=$2 AND scope_ref IS NOT DISTINCT FROM $3 AND status='certified' ORDER BY generated_at DESC LIMIT 1`,
          [sport, capability, scopeRef]
        );
        const r = snap.rows[0];
        if (!r) return null;
        return {
          snapshotId: String(r.snapshot_id),
          version: String(r.version),
          checksum: String(r.checksum),
          provider: String(r.provider),
          generatedAt: String(r.generated_at),
          sourceUpdatedAt: r.source_updated_at ? String(r.source_updated_at) : null,
          recordCount: Number(r.record_count),
          resolvedCount: Number(r.resolved_count),
          ambiguousCount: Number(r.ambiguous_count),
          unresolvedCount: Number(r.unresolved_count),
          rejectedCount: Number(r.rejected_count),
          limitations: r.limitations ?? []
        };
      }
      /** OS-consumer read: the latest certified snapshot's records (canonical, provider-neutral), deterministically ordered. */
      async getCertifiedRecords(sport, capability, scopeRef = null) {
        const pool = await getPool();
        const snap = await pool.query(
          `SELECT snapshot_id, version FROM sports_data.sports_snapshot WHERE sport=$1 AND capability=$2 AND scope_ref IS NOT DISTINCT FROM $3 AND status='certified' ORDER BY generated_at DESC LIMIT 1`,
          [sport, capability, scopeRef]
        );
        const id2 = snap.rows[0]?.snapshot_id ? String(snap.rows[0].snapshot_id) : null;
        if (!id2) return { snapshotId: null, version: null, records: [] };
        const recs = await pool.query(`SELECT record FROM sports_data.sports_snapshot_record WHERE snapshot_id=$1 ORDER BY canonical_key ASC`, [id2]);
        return { snapshotId: id2, version: snap.rows[0].version ? String(snap.rows[0].version) : null, records: recs.rows.map((r) => r.record) };
      }
    };
  }
});

// lib/sports-data-gateway/errors.ts
var init_errors = __esm({
  "lib/sports-data-gateway/errors.ts"() {
    "use strict";
  }
});

// lib/sports-data-gateway/adapter.ts
var init_adapter = __esm({
  "lib/sports-data-gateway/adapter.ts"() {
    "use strict";
    init_errors();
  }
});

// lib/sports-data-gateway/providers/espn.ts
var BASE;
var init_espn2 = __esm({
  "lib/sports-data-gateway/providers/espn.ts"() {
    "use strict";
    init_adapter();
    init_espnUrls();
    BASE = `${ESPN_SITE_API_BASE}/football/nfl`;
  }
});

// lib/sports-data-gateway/runtime/events.ts
var init_events2 = __esm({
  "lib/sports-data-gateway/runtime/events.ts"() {
    "use strict";
  }
});

// lib/sports-data-gateway/runtime/scheduleRuntime.ts
async function getCertifiedSchedule(store, season, week) {
  const { records } = await store.getCertifiedRecords("NFL", "games", `${season}-w${week ?? "x"}`).catch(() => ({ records: [] }));
  return records;
}
var import_server_only8;
var init_scheduleRuntime = __esm({
  "lib/sports-data-gateway/runtime/scheduleRuntime.ts"() {
    "use strict";
    import_server_only8 = __toESM(require_audit_server_only_shim());
    init_espn2();
    init_store();
    init_events2();
    init_snapshot();
  }
});

// lib/sports-data-gateway/runtime/freshnessPure.ts
function buildCertifiedFreshness(meta, now, staleMinutes = DEFAULT_CERTIFIED_STALE_MINUTES) {
  if (!meta) {
    return {
      generatedAt: now.toISOString(),
      lastSuccessfulSyncAt: null,
      sourceProviders: [],
      snapshotVersions: [],
      freshnessStatus: "unavailable",
      limitations: ["No certified snapshot available."]
    };
  }
  const ageMin = (now.getTime() - new Date(meta.generatedAt).getTime()) / 6e4;
  const status = Number.isNaN(ageMin) ? "unavailable" : ageMin <= staleMinutes ? "current" : "delayed";
  const limitations = [...meta.limitations];
  if (meta.unresolvedCount > 0) limitations.push(`${meta.unresolvedCount} unresolved identities in this snapshot`);
  if (meta.rejectedCount > 0) limitations.push(`${meta.rejectedCount} records rejected during normalization`);
  return {
    generatedAt: now.toISOString(),
    lastSuccessfulSyncAt: meta.generatedAt,
    sourceProviders: [meta.provider],
    snapshotVersions: [meta.version],
    freshnessStatus: status,
    limitations
  };
}
var DEFAULT_CERTIFIED_STALE_MINUTES;
var init_freshnessPure = __esm({
  "lib/sports-data-gateway/runtime/freshnessPure.ts"() {
    "use strict";
    DEFAULT_CERTIFIED_STALE_MINUTES = 60;
  }
});

// lib/sports-evidence/matchupIntegration.ts
var import_server_only9, MATCHUP_UNSUPPORTED, CertifiedMatchupIntegrationService;
var init_matchupIntegration = __esm({
  "lib/sports-evidence/matchupIntegration.ts"() {
    "use strict";
    import_server_only9 = __toESM(require_audit_server_only_shim());
    init_store();
    init_scheduleRuntime();
    init_freshnessPure();
    MATCHUP_UNSUPPORTED = {
      liveFantasyScore: "unavailable",
      projection: "unavailable",
      injury: "unavailable",
      winProbability: "unavailable",
      playerAvailability: "unavailable",
      inferredWinner: "unavailable",
      inferredPlayoffAdvancement: "unavailable"
    };
    CertifiedMatchupIntegrationService = class {
      constructor(store = new SportsRuntimeStore()) {
        this.store = store;
      }
      /** Informational certified game states for a matchup week (identity/status/finality/freshness). Never blocks. */
      async describeMatchupGameStates(input) {
        const now = input.now ?? /* @__PURE__ */ new Date();
        let games;
        let meta;
        try {
          games = await getCertifiedSchedule(this.store, input.season, input.week);
          meta = await this.store.getCertifiedSnapshotMeta("NFL", "games", `${input.season}-w${input.week ?? "x"}`);
        } catch {
          return { available: false, freshnessStatus: "unavailable", snapshotVersion: null, totalGames: 0, finalGames: 0, allGamesFinal: false, games: [], unsupported: MATCHUP_UNSUPPORTED };
        }
        if (!meta || games.length === 0) {
          return { available: false, freshnessStatus: "unavailable", snapshotVersion: null, totalGames: 0, finalGames: 0, allGamesFinal: false, games: [], unsupported: MATCHUP_UNSUPPORTED };
        }
        const freshness = buildCertifiedFreshness(meta, now);
        const mapped = games.map((g4) => ({
          canonicalGameId: g4.canonicalGameId,
          homeTeamId: g4.homeTeamId,
          awayTeamId: g4.awayTeamId,
          scheduledStart: g4.scheduledStart,
          status: g4.status,
          final: g4.status === "final"
        }));
        const finalGames = mapped.filter((g4) => g4.final).length;
        return {
          available: true,
          freshnessStatus: freshness.freshnessStatus,
          snapshotVersion: meta.version ?? null,
          totalGames: mapped.length,
          finalGames,
          allGamesFinal: mapped.length > 0 && finalGames === mapped.length,
          games: mapped,
          unsupported: MATCHUP_UNSUPPORTED
        };
      }
      /** Alias — informational certified matchup sports context (game-level). */
      async describeMatchupSportsContext(input) {
        return this.describeMatchupGameStates(input);
      }
      /**
       * Finality evidence. Certified game finality may only make finalization STRICTER — a final provider game
       * status alone must never finalize a fantasy matchup. `canSupportFinalization` is true only when certified
       * evidence is trustworthy AND every game is final; the existing finalization authority still decides.
       */
      async evaluateMatchupFinalityEvidence(input) {
        const ctx = await this.describeMatchupGameStates(input);
        const trustworthy = ctx.available && ctx.freshnessStatus === "current";
        const canSupportFinalization = trustworthy && ctx.allGamesFinal;
        return {
          certifiedAllGamesFinal: ctx.available && ctx.allGamesFinal,
          unresolvedGames: ctx.totalGames - ctx.finalGames,
          trustworthy,
          freshnessStatus: ctx.freshnessStatus,
          snapshotVersion: ctx.snapshotVersion,
          canSupportFinalization,
          reason: !ctx.available ? "certified games snapshot unavailable \u2014 existing finalization authority remains final" : !trustworthy ? `certified games ${ctx.freshnessStatus} \u2014 evidence not trustworthy enough to tighten finalization` : ctx.allGamesFinal ? "certified evidence: all games final (may support, never cause, finalization)" : `certified evidence: ${ctx.totalGames - ctx.finalGames} game(s) not final`
        };
      }
    };
  }
});

// lib/sports-evidence/scoringIntegration.ts
var import_server_only10, CertifiedScoringIntegrationService;
var init_scoringIntegration = __esm({
  "lib/sports-evidence/scoringIntegration.ts"() {
    "use strict";
    import_server_only10 = __toESM(require_audit_server_only_shim());
    init_matchupIntegration();
    CertifiedScoringIntegrationService = class {
      constructor(matchup = new CertifiedMatchupIntegrationService()) {
        this.matchup = matchup;
      }
      /** Certified GAME context for a scoring week (identity/status/finality/freshness). Never player stats. */
      async describeScoringGameContext(input) {
        return this.matchup.describeMatchupGameStates(input);
      }
      /** Honest report: certified player statistics are NOT implemented; existing stat inputs remain authoritative. */
      describeStatSourceAvailability() {
        return {
          certifiedPlayerStatistics: "certified-not-scoring-input",
          certifiedGameContext: "available",
          authoritativeStatInputs: ["PlayerGameLogCache", "PlayerWeeklyScore", "provider-normalized stat tables"],
          note: "Certified player-game statistics now exist (ESPN box scores, append-only certified snapshots) but are NOT yet a scoring input. Fantasy points are still computed solely by the existing scoring engine from its existing stat inputs; switching scoring to certified statistics is a later certification phase."
        };
      }
      /**
       * Finality guard for scoring. Returns whether certified GAME evidence can SUPPORT finalization proceeding
       * (all games final + trustworthy). It never finalizes on its own and never changes scores — a final provider
       * game status alone must not finalize a fantasy matchup. Fails open (supports=false → existing authority final).
       */
      async evaluateScoringFinalityEvidence(input) {
        const ev = await this.matchup.evaluateMatchupFinalityEvidence(input);
        return {
          certifiedGamesSupportFinalization: ev.canSupportFinalization,
          certifiedAllGamesFinal: ev.certifiedAllGamesFinal,
          unresolvedGames: ev.unresolvedGames,
          trustworthy: ev.trustworthy,
          freshnessStatus: ev.freshnessStatus,
          snapshotVersion: ev.snapshotVersion,
          reason: ev.reason
        };
      }
    };
  }
});

// lib/autocoach/bestBallShared.ts
function isBestBallLeague(leagueVariant, bestBallMode) {
  if (bestBallMode === true) return true;
  if (!leagueVariant) return false;
  const v = leagueVariant.toLowerCase();
  return BESTBALL_VARIANTS.has(v) || v.includes("best_ball") || v.includes("bestball");
}
var BESTBALL_VARIANTS;
var init_bestBallShared = __esm({
  "lib/autocoach/bestBallShared.ts"() {
    "use strict";
    BESTBALL_VARIANTS = /* @__PURE__ */ new Set(["best_ball", "bestball", "best-ball"]);
  }
});

// lib/lineup-optimizer/optimalLineup.ts
function expandSeats(slots) {
  const seats = [];
  const ordered = [...slots].sort(
    (a, b) => (a.slotOrder ?? 0) - (b.slotOrder ?? 0) || (a.slot < b.slot ? -1 : a.slot > b.slot ? 1 : 0)
  );
  for (const s of ordered) {
    const eligible = new Set(s.eligible.map((e) => e.trim().toUpperCase()));
    const any = eligible.has("*");
    for (let i = 0; i < Math.max(0, s.count); i += 1) {
      seats.push({ slot: s.slot, seat: i + 1, eligible, any });
    }
  }
  return seats;
}
function eligibleFor(player, seat) {
  if (seat.any) return true;
  for (const p of player.positions) {
    if (seat.eligible.has(p.trim().toUpperCase())) return true;
  }
  return false;
}
function computeOptimalLineup(input) {
  const seats = expandSeats(input.slots);
  const players = [...input.players].sort(
    (a, b) => b.points - a.points || (a.playerId < b.playerId ? -1 : a.playerId > b.playerId ? 1 : 0)
  );
  const seatsFor = players.map((p) => {
    const out = [];
    for (let s = 0; s < seats.length; s += 1) if (eligibleFor(p, seats[s])) out.push(s);
    return out;
  });
  const seatOwner = new Array(seats.length).fill(-1);
  function seat(playerIdx, visited) {
    for (const s of seatsFor[playerIdx]) {
      if (visited[s]) continue;
      visited[s] = true;
      if (seatOwner[s] === -1 || seat(seatOwner[s], visited)) {
        seatOwner[s] = playerIdx;
        return true;
      }
    }
    return false;
  }
  for (let i = 0; i < players.length; i += 1) {
    seat(i, new Array(seats.length).fill(false));
  }
  const assignments = [];
  const unfilledSlots = [];
  const used = /* @__PURE__ */ new Set();
  for (let s = 0; s < seats.length; s += 1) {
    const owner = seatOwner[s];
    if (owner === -1) {
      unfilledSlots.push({ slot: seats[s].slot, seat: seats[s].seat });
      continue;
    }
    const p = players[owner];
    used.add(p.playerId);
    assignments.push({
      slot: seats[s].slot,
      seat: seats[s].seat,
      playerId: p.playerId,
      playerName: p.playerName ?? null,
      points: p.points
    });
  }
  const total = assignments.reduce((sum, a) => sum + a.points, 0);
  const seatedPoints = new Set(assignments.map((a) => a.points));
  const tiedAlternativeExists = players.some(
    (p, i) => !used.has(p.playerId) && seatsFor[i].length > 0 && seatedPoints.has(p.points)
  );
  return {
    total: Math.round(total * 100) / 100,
    assignments,
    unfilledSlots,
    unusedPlayerIds: players.filter((p) => !used.has(p.playerId)).map((p) => p.playerId).sort(),
    provenance: {
      algorithm: OPTIMAL_LINEUP_ALGORITHM,
      seatCount: seats.length,
      playerCount: players.length,
      tiedAlternativeExists
    }
  };
}
var OPTIMAL_LINEUP_ALGORITHM;
var init_optimalLineup = __esm({
  "lib/lineup-optimizer/optimalLineup.ts"() {
    "use strict";
    OPTIMAL_LINEUP_ALGORITHM = "exact-transversal-matroid-v1";
  }
});

// lib/redraft/uiScoringStoreBridge.ts
function storeSavedByPerson(raw) {
  if (!raw || typeof raw !== "object") return false;
  const by = raw.lastUpdatedBy;
  return typeof by === "string" && by.trim().length > 0;
}
function bridgeUiRulesForSport(sport, rules) {
  const store = UI_SCORING_STORES[sport];
  if (!store) return {};
  const out = {};
  for (const [uiKey, engineKey] of Object.entries(store.keyMap)) {
    const value = Number(rules[uiKey]);
    if (rules[uiKey] != null && Number.isFinite(value)) out[engineKey] = value;
  }
  return out;
}
function bridgeSportUiScoringStore(sport, settings) {
  const store = UI_SCORING_STORES[sport];
  if (!store) return null;
  const s = settings && typeof settings === "object" ? settings : {};
  const raw = s[store.settingsKey];
  const rules = raw && typeof raw === "object" ? raw.rules : null;
  if (!rules || typeof rules !== "object") return {};
  const out = bridgeUiRulesForSport(sport, rules);
  if (sport === "MLB") {
    out.era = 0;
    out.avg = 0;
  }
  if (store.receptionFromPresetUntilSaved && !storeSavedByPerson(raw)) delete out.rec;
  return out;
}
var UI_SCORING_STORES;
var init_uiScoringStoreBridge = __esm({
  "lib/redraft/uiScoringStoreBridge.ts"() {
    "use strict";
    init_scoringKeyBridge();
    UI_SCORING_STORES = {
      MLB: { settingsKey: "mlb_scoring_config", keyMap: MLB_UI_TO_ENGINE_KEYS },
      NHL: {
        settingsKey: "nhl_scoring_config",
        keyMap: {
          goals: "g",
          assists: "a",
          plus_minus: "plusminus",
          shots_on_goal: "sog",
          power_play_points: "ppp",
          short_handed_points: "shp",
          blocked_shots: "blks",
          hits: "hits",
          penalty_minutes: "pim",
          goalie_wins: "g_win",
          saves: "g_sv",
          shutouts: "g_so",
          goals_against: "g_ga"
        }
      },
      NBA: {
        settingsKey: "nba_scoring_config",
        keyMap: {
          points_scored: "pts",
          rebound: "reb",
          assist: "ast",
          steal: "stl",
          block: "blk",
          turnover: "to",
          three_point_made: "threes",
          field_goals_made: "fgm",
          free_throws_made: "ftm",
          double_double: "dbl_dbl",
          triple_double: "trpl_dbl"
        }
      },
      NCAAB: {
        settingsKey: "ncaab_scoring_config",
        keyMap: {
          points_scored: "pts",
          rebound: "reb",
          assist: "ast",
          steal: "stl",
          block: "blk",
          turnover: "to",
          three_point_made: "threes"
        }
      },
      SOCCER: {
        settingsKey: "soccer_scoring_config",
        keyMap: {
          goal: "goals",
          assist: "assists",
          clean_sheet: "clean_sheet_def",
          gk_clean_sheet: "clean_sheet_gk",
          gk_save: "saves",
          yellow_card: "yellow_card",
          red_card: "red_card",
          own_goal: "own_goal",
          penalty_missed: "pen_miss",
          gk_penalty_save: "pen_save"
        }
      },
      NCAAF: {
        settingsKey: "ncaaf_scoring_config",
        receptionFromPresetUntilSaved: true,
        // The engine has one two-point category; when the panel's three differ, the last present
        // (receiving) wins — the same collapse the NFL bridge makes.
        keyMap: {
          passing_yards: "pass_yds",
          passing_td: "pass_td",
          interception_thrown: "pass_int",
          passing_2pt: "two_pt",
          rushing_yards: "rush_yds",
          rushing_td: "rush_td",
          rushing_2pt: "two_pt",
          reception: "rec",
          receiving_yards: "rec_yds",
          receiving_td: "rec_td",
          receiving_2pt: "two_pt",
          fumble_lost: "fum_lost",
          dst_td: "def_td",
          dst_interception: "def_int",
          dst_fumble_recovery: "def_fr",
          dst_sack: "def_sack"
        }
      }
    };
  }
});

// lib/redraft/playoffScoring.ts
var playoffScoring_exports = {};
__export(playoffScoring_exports, {
  advancePlayoffScoringPeriodIfReady: () => advancePlayoffScoringPeriodIfReady,
  aggregatePlayoffPeriodScores: () => aggregatePlayoffPeriodScores,
  ensurePlayoffScoringMatchups: () => ensurePlayoffScoringMatchups,
  syncPlayoffPeriodScores: () => syncPlayoffPeriodScores
});
async function ensurePlayoffScoringMatchups(seasonId) {
  const season = await prisma.redraftSeason.findUnique({
    where: { id: seasonId },
    include: { playoffRounds: { where: { status: "active" }, include: { matchups: true } } }
  });
  if (!season) return;
  for (const round of season.playoffRounds) {
    const legacyWeek = season.playoffStartWeek + (round.roundNumber >= 100 ? round.roundNumber - 100 : round.roundNumber) - 1;
    for (const matchup of round.matchups) {
      if (!matchup.homeRosterId || !matchup.awayRosterId || matchup.metadata?.commissionerOverride) continue;
      const metadata = matchup.metadata;
      const periods = Array.isArray(metadata.scoringPeriods) && metadata.scoringPeriods.length ? metadata.scoringPeriods.filter((v) => typeof v === "number" && Number.isInteger(v) && v > 0) : [legacyWeek];
      const managed = Array.isArray(metadata.scoringPeriods);
      const ids = periods.map((week) => !managed && periods.length === 1 ? `playoff-score:${matchup.id}` : `playoff-score:${matchup.id}:week:${week}`);
      for (const [index, week] of periods.entries()) {
        const id2 = ids[index];
        await prisma.redraftMatchup.upsert({ where: { id: id2 }, create: {
          id: id2,
          seasonId,
          leagueId: season.leagueId,
          week,
          type: "playoff",
          homeRosterId: matchup.homeRosterId,
          awayRosterId: matchup.awayRosterId
        }, update: {} });
      }
      await prisma.redraftPlayoffMatchup.update({ where: { id: matchup.id }, data: {
        metadata: toPrismaJsonInput({ ...metadata, scoringMatchupId: ids[0], scoringWeek: periods[0], scoringMatchupIds: ids })
      } });
    }
  }
}
async function advancePlayoffScoringPeriodIfReady(seasonId) {
  const season = await prisma.redraftSeason.findUnique({ where: { id: seasonId }, include: {
    playoffRounds: { where: { status: "active", roundNumber: { lt: 100 } }, include: { matchups: true } }
  } });
  if (!season) return false;
  const round = season.playoffRounds[0];
  if (!round) return false;
  const games = round.matchups.filter((m) => m.homeRosterId && m.awayRosterId);
  if (!games.length) return false;
  const periods = games[0].metadata.scoringPeriods;
  if (!Array.isArray(periods)) return false;
  const nextWeek = periods.find((v) => typeof v === "number" && v > season.currentWeek);
  if (typeof nextWeek !== "number") return false;
  const ids = games.map((game) => `playoff-score:${game.id}:week:${season.currentWeek}`);
  const scores = await prisma.redraftMatchup.findMany({ where: { id: { in: ids } }, select: { status: true } });
  if (scores.length !== games.length || scores.some((score) => score.status !== "final")) return false;
  const result = await prisma.redraftSeason.updateMany({ where: { id: seasonId, currentWeek: season.currentWeek, status: "playoffs" }, data: { currentWeek: nextWeek } });
  return result.count === 1;
}
function aggregatePlayoffPeriodScores(expectedIds, rows) {
  const unique = [...new Set(expectedIds)];
  const selected = unique.map((id2) => rows.find((row) => row.id === id2));
  const known = selected.filter((row) => Boolean(row));
  return {
    homeScore: known.reduce((sum, row) => sum + (row.homeScore ?? 0), 0),
    awayScore: known.reduce((sum, row) => sum + (row.awayScore ?? 0), 0),
    final: unique.length > 0 && known.length === unique.length && known.every((row) => row.status === "final" && typeof row.homeScore === "number" && Number.isFinite(row.homeScore) && typeof row.awayScore === "number" && Number.isFinite(row.awayScore))
  };
}
async function syncPlayoffPeriodScores(matchupId) {
  const matchup = await prisma.redraftPlayoffMatchup.findUnique({ where: { id: matchupId } });
  if (!matchup) return;
  const metadata = matchup.metadata;
  if (metadata.commissionerOverride === true) return;
  const ids = Array.isArray(metadata.scoringMatchupIds) ? metadata.scoringMatchupIds.filter((id2) => typeof id2 === "string") : [];
  if (!ids.length) return;
  const scores = await prisma.redraftMatchup.findMany({ where: { id: { in: ids } }, select: { id: true, homeScore: true, awayScore: true, status: true } });
  const aggregate = aggregatePlayoffPeriodScores(ids, scores);
  const correctedWinner = aggregate.homeScore > aggregate.awayScore ? matchup.homeRosterId : aggregate.awayScore > aggregate.homeScore ? matchup.awayRosterId : matchup.homeSeed != null && matchup.awaySeed != null && matchup.homeSeed !== matchup.awaySeed ? matchup.homeSeed < matchup.awaySeed ? matchup.homeRosterId : matchup.awayRosterId : null;
  const reviewRequired = Boolean(aggregate.final && matchup.winnerRosterId && correctedWinner !== matchup.winnerRosterId);
  await prisma.redraftPlayoffMatchup.updateMany({
    where: { id: matchupId, updatedAt: matchup.updatedAt },
    data: {
      homeScore: aggregate.homeScore,
      awayScore: aggregate.awayScore,
      status: aggregate.final ? "final" : "active",
      ...reviewRequired ? { metadata: toPrismaJsonInput({ ...metadata, correctionReviewRequired: true, previousWinnerRosterId: matchup.winnerRosterId, correctedWinnerRosterId: correctedWinner }) } : {}
    }
  });
}
var init_playoffScoring = __esm({
  "lib/redraft/playoffScoring.ts"() {
    "use strict";
    init_prisma();
    init_prisma_json();
  }
});

// lib/redraft/scoringEngine.ts
function calculateFantasyPoints(rawStats, statCategories, scoringOverrides) {
  const cats = statCategories;
  if (!Array.isArray(cats)) return 0;
  let sum = 0;
  for (const cat of cats) {
    const v = rawStats[cat.key] ?? 0;
    const mult = scoringOverrides?.[cat.key] ?? cat.points;
    sum += v * mult;
  }
  return sum;
}
function bonusBaseYardsKey(catKey) {
  if (catKey.includes("pass_") && catKey.includes("bonus")) return "pass_yds";
  if (catKey.includes("rush_") && catKey.includes("bonus")) return "rush_yds";
  if (catKey.includes("rec_") && catKey.includes("bonus")) return "rec_yds";
  return null;
}
function pointsForCategory(cat, rawStats) {
  const pts = cat.defaultPoints;
  if (cat.minForBonus != null) {
    const base = bonusBaseYardsKey(cat.key);
    if (!base) return 0;
    const yards = rawStats[base] ?? 0;
    return yards >= cat.minForBonus ? pts : 0;
  }
  if (cat.tierStatKey != null) {
    if (!(cat.tierStatKey in rawStats)) return 0;
    const value = rawStats[cat.tierStatKey];
    if (!Number.isFinite(value)) return 0;
    const min = cat.tierMin ?? Number.NEGATIVE_INFINITY;
    const max = cat.tierMax ?? Number.POSITIVE_INFINITY;
    return value >= min && value <= max ? pts : 0;
  }
  const raw = rawStats[cat.key] ?? 0;
  if (cat.unit === "per_yard" || cat.unit === "per_inning") {
    return raw * pts;
  }
  return raw * pts;
}
function applyScoringPresetToRecPoints(categories, preset, overrides) {
  if (overrides.rec != null) return categories;
  if (preset === "CUSTOM") return categories;
  const recPts = preset === "PPR" ? 1 : preset === "HALF_PPR" ? 0.5 : preset === "STANDARD" ? 0 : null;
  if (recPts === null) return categories;
  return categories.map((c) => c.key === "rec" ? { ...c, defaultPoints: recPts } : c);
}
function scoreStatsWithCategories(categories, rawStats, overrides = {}) {
  let sum = 0;
  for (const cat of categories) {
    const mult = overrides[cat.key] ?? cat.defaultPoints;
    sum += pointsForCategory({ ...cat, defaultPoints: mult }, rawStats);
  }
  return sum;
}
function isScoringStarterSlot(slotType) {
  return isNflRedraftScoringStarterSlot(normalizeToken(slotType));
}
function isBestBallCandidateSlot(slotType) {
  return !BEST_BALL_EXCLUDED_SLOTS.has(normalizeToken(slotType));
}
function countsTowardScore(slotType, bestBall) {
  return bestBall ? isBestBallCandidateSlot(slotType) : isScoringStarterSlot(slotType);
}
function leagueIsBestBall(league) {
  if (!league) return false;
  return isBestBallLeague(league.leagueVariant, league.bestBallMode) || isBestBallLeague(league.leagueType);
}
function buildBestBallSlots(sport, leagueSettings) {
  const config = resolveRedraftRosterConfig(sport, leagueSettings);
  const slots = [];
  let order = 0;
  for (const [token, count2] of config.starterCapacities) {
    slots.push({ slot: token, eligible: allowedPositionsForSlot(sport, token), count: count2, slotOrder: order++ });
  }
  return slots;
}
function playerPositions(position) {
  return redraftPlayerPositionTokens(position);
}
function readSportConfig(league) {
  const s = league.settings;
  const raw = s?.sportConfig;
  return raw && typeof raw === "object" && raw !== null ? raw : {};
}
function bridgeLegacyNflScoringConfig(league) {
  const s = league.settings;
  const legacy = s?.nfl_scoring_config;
  const rules = legacy?.rules;
  if (!rules || typeof rules !== "object") return {};
  return bridgeUiRulesToEngineCategoryPoints(rules);
}
function togglesFromSportConfig(sc) {
  const t = [];
  if (sc.enableIDP === true) t.push("IDP");
  if (sc.enableSuperflex === true) t.push("SUPERFLEX");
  if (sc.enableTEPremium === true) t.push("TE_PREMIUM");
  return t;
}
async function calculateScoreFromSportConfig(leagueId, _playerId, _week, rawStats, position) {
  const league = await prisma.league.findFirst({
    where: { id: leagueId },
    select: { sport: true, settings: true }
  });
  if (!league) return 0;
  return calculateScoreForLeague(league, rawStats, position);
}
function calculateScoreForLeague(league, rawStats, position) {
  const sport = resolveSportConfigKey(String(league.sport));
  const cfg = tryGetSportConfig(sport);
  if (!cfg) return calculateFantasyPoints(rawStats, [], void 0);
  const sc = readSportConfig(league);
  const toggles = togglesFromSportConfig(sc);
  const expanded = expandSportConfigToggles(toggles);
  let categories = getScoringCategories(cfg.sport, expanded);
  const preset = String(sc.scoringPreset ?? "PPR");
  let overrides = typeof sc.categoryPoints === "object" && sc.categoryPoints !== null ? sc.categoryPoints : {};
  if (Object.keys(overrides).length === 0) {
    overrides = bridgeSportUiScoringStore(cfg.sport, league.settings) ?? bridgeLegacyNflScoringConfig(league);
  }
  categories = applyScoringPresetToRecPoints(categories, preset, overrides);
  const effectiveStats = applyTePremiumStat(categories, rawStats, position);
  return scoreStatsWithCategories(categories, effectiveStats, overrides);
}
function applyTePremiumStat(categories, rawStats, position) {
  const isTe = String(position ?? "").trim().toUpperCase() === "TE";
  if (!isTe) return rawStats;
  if (!categories.some((c) => c.key === "te_premium")) return rawStats;
  const receptions = rawStats.rec ?? rawStats.receptions ?? 0;
  return { ...rawStats, te_premium: receptions };
}
async function scoreRosterStarters(args) {
  if (args.bestBall && !args.useDevyEngine) return scoreRosterBestBall({ ...args, bestBall: args.bestBall });
  if (args.useDevyEngine) {
    const r = await calculateOfficialTeamScore(args.leagueId, args.rosterId, args.week, args.seasonYear);
    return {
      rosterId: args.rosterId,
      starterCount: 1,
      scoredStarterCount: 1,
      points: r.officialScore,
      missingPlayerIds: [],
      allFinal: true
    };
  }
  const historyLeague = await prisma.league.findFirst({ where: { id: args.leagueId }, select: { sport: true, settings: true } });
  if (historyLeague && usesLineupHistory(historyLeague.settings)) {
    const season = await prisma.redraftSeason.findFirst({ where: { leagueId: args.leagueId, season: args.seasonYear } });
    if (!season) throw new Error("LINEUP_HISTORY_SEASON_NOT_FOUND");
    const period = await loadHistoricalPeriod(prisma, {
      leagueId: args.leagueId,
      seasonId: season.id,
      sport: String(historyLeague.sport),
      season: args.seasonYear,
      week: args.week,
      settings: historyLeague.settings,
      includeBench: args.benchOnly
    });
    const players = period.eligible.filter((p) => p.rosterId === args.rosterId && (!args.benchOnly || ["BENCH", "BN"].includes(p.slotType.toUpperCase())));
    const contributions = period.contributions.filter((c) => c.player.rosterId === args.rosterId && (!args.benchOnly || ["BENCH", "BN"].includes(c.player.slotType.toUpperCase())));
    const missingPlayerIds2 = players.filter((p) => period.missingPlayerIds.includes(p.playerId)).map((p) => p.playerId);
    const rows = await prisma.playerWeeklyScore.findMany({ where: {
      sport: String(historyLeague.sport),
      season: args.seasonYear,
      week: args.week,
      playerId: { in: players.map((p) => p.playerId) }
    } });
    let points = 0;
    for (const player of players) {
      const stats = accumulateTeamTotals(contributions.filter((c) => c.player.playerId === player.playerId).map((c) => c.stats));
      points += calculateScoreForLeague(historyLeague, stats, player.position);
    }
    return {
      rosterId: args.rosterId,
      starterCount: players.length,
      scoredStarterCount: players.length - missingPlayerIds2.length,
      points: Math.round(points * 100) / 100,
      missingPlayerIds: missingPlayerIds2,
      allFinal: period.slateFinal && rows.length === players.length && rows.every((r) => r.isFinalized),
      rawStats: accumulateTeamTotals(contributions.map((c) => c.stats)),
      historyVersion: 1
    };
  }
  const starters = await prisma.redraftRosterPlayer.findMany({
    where: {
      rosterId: args.rosterId,
      droppedAt: null
    }
  });
  const activeStarters = starters.filter((p) => args.benchOnly ? ["BENCH", "BN"].includes(p.slotType.toUpperCase()) : isScoringStarterSlot(p.slotType));
  let pts = 0;
  let scoredStarterCount = 0;
  let allFinal = true;
  const statMaps = [];
  const missingPlayerIds = [];
  for (const p of activeStarters) {
    const row = await prisma.playerWeeklyScore.findUnique({
      where: {
        playerId_week_season_sport: {
          playerId: p.playerId,
          week: args.week,
          season: args.seasonYear,
          sport: p.sport
        }
      }
    });
    if (!row) {
      missingPlayerIds.push(p.playerId);
      allFinal = false;
      continue;
    }
    pts += await calculateScoreFromSportConfig(
      args.leagueId,
      p.playerId,
      args.week,
      row.stats,
      p.position
    );
    statMaps.push(row.stats);
    scoredStarterCount += 1;
    if (!row.isFinalized) allFinal = false;
  }
  return {
    rosterId: args.rosterId,
    starterCount: activeStarters.length,
    scoredStarterCount,
    points: Math.round(pts * 100) / 100,
    missingPlayerIds,
    allFinal,
    rawStats: accumulateTeamTotals(statMaps)
  };
}
async function scoreRosterBestBall(args) {
  const players = await prisma.redraftRosterPlayer.findMany({
    where: { rosterId: args.rosterId, droppedAt: null }
  });
  const candidates = players.filter((p) => isBestBallCandidateSlot(p.slotType));
  let scored = 0;
  let allFinal = true;
  const missingPlayerIds = [];
  const inputs = [];
  for (const p of candidates) {
    const row = await prisma.playerWeeklyScore.findUnique({
      where: {
        playerId_week_season_sport: { playerId: p.playerId, week: args.week, season: args.seasonYear, sport: p.sport }
      }
    });
    if (!row) {
      missingPlayerIds.push(p.playerId);
      allFinal = false;
      continue;
    }
    const points = await calculateScoreFromSportConfig(
      args.leagueId,
      p.playerId,
      args.week,
      row.stats,
      p.position
    );
    inputs.push({ playerId: p.playerId, positions: playerPositions(p.position), points, playerName: p.playerName ?? null });
    scored += 1;
    if (!row.isFinalized) allFinal = false;
  }
  const lineup = computeOptimalLineup({ players: inputs, slots: args.bestBall.slots });
  const used = new Set(lineup.assignments.map((p) => p.playerId));
  const total = args.benchOnly ? inputs.filter((p) => !used.has(p.playerId)).reduce((sum, p) => sum + p.points, 0) : lineup.total;
  return {
    rosterId: args.rosterId,
    starterCount: candidates.length,
    scoredStarterCount: scored,
    points: Math.round(total * 100) / 100,
    missingPlayerIds,
    allFinal,
    bestBall: { assignments: lineup.assignments, unfilledSlots: lineup.unfilledSlots }
  };
}
async function loadBestBallContext(leagueId) {
  const league = await prisma.league.findFirst({
    where: { id: leagueId },
    select: { sport: true, settings: true, bestBallMode: true, leagueVariant: true, leagueType: true }
  });
  if (!league || !leagueIsBestBall(league)) return null;
  const sport = String(league.sport ?? "NFL");
  return { sport, slots: buildBestBallSlots(sport, league.settings) };
}
async function scoreRosterForWeek(args) {
  const useDevyEngine = await leagueUsesDevyEngine(args.leagueId);
  const bestBall = useDevyEngine ? null : await loadBestBallContext(args.leagueId);
  return scoreRosterStarters({ ...args, useDevyEngine, bestBall });
}
async function updateMatchupScores(matchupId) {
  const m = await prisma.redraftMatchup.findFirst({
    where: { id: matchupId },
    include: {
      homeRoster: { include: { players: true } },
      awayRoster: { include: { players: true } }
    }
  });
  if (!m || !m.homeRoster) return null;
  const leagueId = m.leagueId;
  const week = m.week;
  const homeRosterId = m.homeRosterId;
  const awayRosterId = m.awayRosterId;
  const seasonRowId = m.seasonId;
  if (await leagueUsesC2CEngine(leagueId)) {
    await updateC2CMatchupScores(matchupId);
    return null;
  }
  const season = await prisma.redraftSeason.findFirst({ where: { id: seasonRowId } });
  if (!season) return null;
  const seasonYear = season.season;
  const useDevyEngine = await leagueUsesDevyEngine(leagueId);
  const bestBall = useDevyEngine ? null : await loadBestBallContext(leagueId);
  let home;
  let away;
  try {
    home = await scoreRosterStarters({
      leagueId,
      rosterId: homeRosterId,
      week,
      seasonYear,
      useDevyEngine,
      bestBall
    });
    away = awayRosterId ? await scoreRosterStarters({
      leagueId,
      rosterId: awayRosterId,
      week,
      seasonYear,
      useDevyEngine,
      bestBall
    }) : { rosterId: "", starterCount: 0, scoredStarterCount: 0, points: 0, missingPlayerIds: [], allFinal: true, historyVersion: 1, rawStats: {} };
  } catch (error) {
    if (m.status === "final" && error instanceof Error && error.message === "LINEUP_HISTORY_BEFORE_BASELINE") return null;
    throw error;
  }
  const scoringLeague = await prisma.league.findFirst({ where: { id: leagueId }, select: { sport: true, settings: true } });
  const scoringMode = resolveLeagueScoringMode(scoringLeague?.settings, String(scoringLeague?.sport ?? season.sport ?? "NFL"));
  const categoryResult = scoringMode.mode === "points" || !awayRosterId ? null : resolveCategoryMatchup(home.rawStats ?? {}, away.rawStats ?? {}, scoringMode.categories);
  if (categoryResult) {
    home.points = categoryResult.aWins;
    away.points = categoryResult.bWins;
  }
  const missingPlayerIds = [...home.missingPlayerIds, ...away.missingPlayerIds];
  const isComplete = missingPlayerIds.length === 0 && (home.starterCount > 0 || home.historyVersion === 1) && (away.starterCount > 0 || away.historyVersion === 1) && home.scoredStarterCount === home.starterCount && away.scoredStarterCount === away.starterCount;
  const existingFinal = isComplete && home.allFinal && away.allFinal;
  let isFinal = existingFinal;
  if (existingFinal && isSportsDataEnabled("scoring") && String(season.sport ?? "NFL").toUpperCase() === "NFL") {
    try {
      const finality = await new CertifiedScoringIntegrationService().evaluateScoringFinalityEvidence({ season: String(seasonYear), week: String(week) });
      if (finality.trustworthy && !finality.certifiedAllGamesFinal) {
        isFinal = false;
        console.info("[scoring][sports-data] finalization withheld by certified evidence", { matchupId, week, unresolvedGames: finality.unresolvedGames, snapshot: finality.snapshotVersion, reason: finality.reason });
      }
    } catch {
      isFinal = existingFinal;
    }
  }
  await prisma.redraftMatchup.update({
    where: { id: matchupId },
    data: {
      homeScore: home.points,
      awayScore: away.points,
      status: isFinal ? "final" : "active",
      lineupSnapshots: toPrismaJsonInput({
        ...m.lineupSnapshots && typeof m.lineupSnapshots === "object" ? m.lineupSnapshots : {},
        redraftScoring: {
          scoredAt: (/* @__PURE__ */ new Date()).toISOString(),
          isComplete,
          home,
          away,
          missingPlayerIds,
          scoringMode: scoringMode.mode,
          categoryPresetId: scoringMode.presetId,
          categoryResult
        }
      })
    }
  });
  if (m.type === "playoff" && m.id.startsWith("playoff-score:")) {
    const playoffMatchupId = m.id.slice("playoff-score:".length).split(":week:")[0];
    if (m.id.includes(":week:")) {
      const { syncPlayoffPeriodScores: syncPlayoffPeriodScores2 } = await Promise.resolve().then(() => (init_playoffScoring(), playoffScoring_exports));
      await syncPlayoffPeriodScores2(playoffMatchupId);
    } else {
      await prisma.$executeRaw`UPDATE redraft_playoff_matchups
        SET "homeScore" = ${home.points}, "awayScore" = ${away.points}, status = ${isFinal ? "final" : "active"}, "updatedAt" = now()
        WHERE id = ${playoffMatchupId} AND (metadata->>'commissionerOverride') IS DISTINCT FROM 'true'`;
    }
  }
  const scoreChanged = home.points !== m.homeScore || away.points !== m.awayScore;
  if (isFinal) {
    const winnerRosterId = home.points > away.points ? homeRosterId : away.points > home.points ? awayRosterId : null;
    await getPlatformEvents().emit(EVENT.MATCHUP_FINALIZED, {
      leagueId,
      seasonId: seasonRowId,
      sport: season.sport ?? null,
      leagueConcept: "redraft",
      actor: { type: "system" },
      source: "engine:scoring",
      period: { kind: "week", index: week },
      idempotencyKey: `matchup.finalized:${matchupId}`,
      subjects: [{ kind: "matchup", id: matchupId }],
      payload: { matchupId, homeScore: home.points, awayScore: away.points, winnerRosterId: winnerRosterId ?? void 0 }
    });
  } else if (scoreChanged) {
    await getPlatformEvents().emit(EVENT.MATCHUP_UPDATED, {
      leagueId,
      seasonId: seasonRowId,
      sport: season.sport ?? null,
      leagueConcept: "redraft",
      actor: { type: "system" },
      source: "engine:scoring",
      period: { kind: "week", index: week },
      subjects: [{ kind: "matchup", id: matchupId }],
      payload: { matchupId }
    });
  }
  return {
    matchupId,
    week,
    homeScore: home.points,
    awayScore: away.points,
    isComplete,
    missingPlayerIds
  };
}
async function recalculateMatchupsForSeasonWeek(seasonId, week) {
  const matchups = await prisma.redraftMatchup.findMany({
    where: { seasonId, week },
    select: { id: true }
  });
  const summaries = [];
  let incomplete = 0;
  for (const matchup of matchups) {
    const summary = await updateMatchupScores(matchup.id);
    if (!summary) continue;
    summaries.push(summary);
    if (!summary.isComplete) incomplete += 1;
  }
  return { updated: summaries.length, incomplete, summaries };
}
var BEST_BALL_EXCLUDED_SLOTS;
var init_scoringEngine2 = __esm({
  "lib/redraft/scoringEngine.ts"() {
    "use strict";
    init_prisma();
    init_prisma_json();
    init_events();
    init_scoringEligibilityEngine();
    init_scoringEngine();
    init_sportConfig();
    init_scoringKeyBridge2();
    init_scoring_runtime();
    init_gates();
    init_scoringIntegration();
    init_bestBallShared();
    init_optimalLineup();
    init_rosterConfigResolver();
    init_lineupValidation();
    init_uiScoringStoreBridge();
    init_lineupHistory();
    init_category_scoring();
    init_category_scoring();
    init_leagueMode();
    BEST_BALL_EXCLUDED_SLOTS = /* @__PURE__ */ new Set(["IR", "RESERVE", "TAXI", "DEVY"]);
  }
});

// lib/sports-data/riSeasonType.ts
function classifyRiSeasonType(raw) {
  if (typeof raw !== "string") return null;
  const label = raw.trim();
  if (!label) return null;
  if (PRE.test(label)) return "pre";
  if (REGULAR.test(label)) return "regular";
  if (POST.test(label)) return "post";
  return null;
}
function resolveStoredSeasonType(row) {
  const map = row.normalizedStatMap;
  if (map && typeof map === "object" && !Array.isArray(map)) {
    const stored = map.seasonType;
    if (stored === "regular" || stored === "pre" || stored === "post") return stored;
  }
  if (!row.gameDate) return null;
  const start = resolveDailySportSeasonStart(row.sport, row.season);
  if (!start) return null;
  return row.gameDate.getTime() < new Date(start).getTime() ? "pre" : null;
}
var PRE, REGULAR, POST;
var init_riSeasonType = __esm({
  "lib/sports-data/riSeasonType.ts"() {
    "use strict";
    init_dailySportSeasonStarts();
    PRE = /\b(pre[\s-]?season|spring training|exhibition)\b/i;
    REGULAR = /^\s*regular(\s+season)?\s*$/i;
    POST = /\b(post[\s-]?season|playoffs?|wild\s*card|div(ision(al)?)?\s*(rd|round|series)|conf(erence)?\s*(champ|finals?)|championship|super bowl|world series|stanley cup|finals?|play[\s-]?in|alcs|nlcs|alds|nlds)\b/i;
  }
});

// lib/redraft/teamDefenseStatsIngest.ts
function buildTeamDefensePlayerId(teamAbbr) {
  return `nfl:def:${normalizeNflTeam(teamAbbr)}`;
}
var init_teamDefenseStatsIngest = __esm({
  "lib/redraft/teamDefenseStatsIngest.ts"() {
    "use strict";
    init_playerWeeklyScoreService();
    init_lineupLock();
  }
});

// lib/redraft/teamDefenseProvider.ts
async function fetchTeamDefenseSeasonUncached(team, season, seasonType, memoKey) {
  const endpoint = "team-defense:nfl:sleeper";
  const { rateLimitManager: rateLimitManager2 } = await Promise.resolve().then(() => (init_rate_limit_manager(), rate_limit_manager_exports));
  const canCall = await rateLimitManager2.canCall("sleeper", endpoint).catch(() => true);
  if (!canCall) return null;
  const url = `${SLEEPER_STATS_BASE}/${encodeURIComponent(team)}?season_type=${encodeURIComponent(seasonType)}&season=${encodeURIComponent(String(season))}&grouping=week`;
  const startedAt = Date.now();
  try {
    const res = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(1e4) });
    await rateLimitManager2.recordCall("sleeper", endpoint, res.status, Math.max(0, Date.now() - startedAt), { cached: false, error: res.ok ? null : `HTTP ${res.status}` }).catch(() => void 0);
    if (!res.ok) return null;
    const payload = await res.json();
    teamDefenseSeasonMemo.set(memoKey, payload);
    return payload;
  } catch {
    await rateLimitManager2.recordCall("sleeper", endpoint, 0, Math.max(0, Date.now() - startedAt), { cached: false, error: "fetch_failed" }).catch(() => void 0);
    return null;
  }
}
function extractSleeperWeekStats(payload, week) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
  const row = payload[String(week)];
  if (row && typeof row === "object" && !Array.isArray(row)) return row;
  return null;
}
var SLEEPER_STATS_BASE, teamDefenseSeasonMemo, teamDefenseInFlight, fetchSleeperTeamDefenseSeason;
var init_teamDefenseProvider = __esm({
  "lib/redraft/teamDefenseProvider.ts"() {
    "use strict";
    init_ttl_memo();
    init_lineupLock();
    init_teamDefenseStatsIngest();
    SLEEPER_STATS_BASE = "https://api.sleeper.com/stats/nfl/player";
    teamDefenseSeasonMemo = createTtlMemo({
      ttlMs: () => Number(process.env.AF_TEAM_DEFENSE_MEMO_MS ?? 4 * 60 * 1e3),
      maxEntries: 256
    });
    teamDefenseInFlight = /* @__PURE__ */ new Map();
    fetchSleeperTeamDefenseSeason = async (teamAbbr, season, seasonType) => {
      const team = normalizeNflTeam(teamAbbr);
      if (!team) return null;
      const memoKey = `${team}|${season}|${seasonType}`;
      const memoised = teamDefenseSeasonMemo.get(memoKey);
      if (memoised !== void 0) return memoised;
      const pending = teamDefenseInFlight.get(memoKey);
      if (pending) return pending;
      const request = fetchTeamDefenseSeasonUncached(team, season, seasonType, memoKey);
      teamDefenseInFlight.set(memoKey, request);
      try {
        return await request;
      } finally {
        teamDefenseInFlight.delete(memoKey);
      }
    };
  }
});

// lib/live-scoring/provider.ts
function resolveSeasonType(query) {
  return query.seasonType ?? "regular";
}
function teamDefenseTargets(query) {
  const playing = teamsInGames(query.games);
  if (query.teamAbbrs === void 0) return playing;
  const wanted = new Set(query.teamAbbrs.map((t) => String(t ?? "").trim().toUpperCase()).filter(Boolean));
  return playing.filter((t) => wanted.has(t));
}
function teamsInGames(games) {
  const set = /* @__PURE__ */ new Set();
  for (const g4 of games) {
    if (g4.homeTeam) set.add(g4.homeTeam.toUpperCase());
    if (g4.awayTeam) set.add(g4.awayTeam.toUpperCase());
  }
  return [...set];
}
var init_provider = __esm({
  "lib/live-scoring/provider.ts"() {
    "use strict";
    init_cadence();
  }
});

// lib/live-scoring/nflLiveStatsProvider.ts
var nflLiveStatsProvider_exports = {};
__export(nflLiveStatsProvider_exports, {
  NflLiveStatsProvider: () => NflLiveStatsProvider
});
function asPlayerStatMap(payload) {
  if (!payload || typeof payload !== "object") return null;
  const map = /* @__PURE__ */ new Map();
  if (Array.isArray(payload)) {
    for (const row of payload) {
      const id2 = row && typeof row === "object" ? String(row.player_id ?? "") : "";
      const stats = row && typeof row === "object" ? row.stats ?? row : row;
      if (id2) map.set(id2, stats);
    }
    return map;
  }
  for (const [id2, row] of Object.entries(payload)) {
    const stats = row && typeof row === "object" ? row.stats ?? row : row;
    map.set(id2, stats);
  }
  return map;
}
var NflLiveStatsProvider;
var init_nflLiveStatsProvider = __esm({
  "lib/live-scoring/nflLiveStatsProvider.ts"() {
    "use strict";
    init_teamDefenseProvider();
    init_nflStatNormalization();
    init_team_abbrev();
    init_teamDefenseStatsIngest();
    init_lineupLock();
    init_cadence();
    init_provider();
    init_statCoverage();
    NflLiveStatsProvider = class {
      constructor(prisma2) {
        this.prisma = prisma2;
      }
      normalizeGameStatus(raw) {
        return normalizeLiveGameStatus(raw);
      }
      async fetchActiveGames(query) {
        const seasonType = resolveSeasonType(query);
        const rows = await this.prisma.sportsGame.findMany({
          where: {
            sport: "NFL",
            season: query.season,
            week: query.week,
            ...seasonType === "regular" ? { OR: [{ seasonType: "regular" }, { seasonType: null }] } : { seasonType }
          },
          select: { externalId: true, homeTeam: true, awayTeam: true, status: true, startTime: true }
        });
        return rows.map((r) => ({
          gameId: r.externalId,
          homeTeam: normalizeNflTeam(r.homeTeam),
          awayTeam: normalizeNflTeam(r.awayTeam),
          status: this.normalizeGameStatus(r.status),
          startTime: r.startTime ?? null
        }));
      }
      async fetchTeamDefenseStatsForGames(query) {
        const out = /* @__PURE__ */ new Map();
        const seasonType = resolveSeasonType(query);
        const teams = teamDefenseTargets(query);
        for (const team of teams) {
          const payload = await fetchSleeperTeamDefenseSeason(team, query.season, seasonType).catch(() => null);
          if (payload == null) continue;
          const weekStats = extractSleeperWeekStats(payload, query.week);
          if (!weekStats) continue;
          const normalized = normalizeNflTeamDefenseWeeklyStats(weekStats);
          if (Object.keys(normalized).length === 0) continue;
          out.set(buildTeamDefensePlayerId(team), normalized);
        }
        return out;
      }
      async fetchPlayerStatsForGames(query) {
        const out = /* @__PURE__ */ new Map();
        if (query.playerIds.length === 0) return out;
        const seasonType = resolveSeasonType(query);
        const url = `https://api.sleeper.com/stats/nfl/${encodeURIComponent(query.season)}/${encodeURIComponent(query.week)}?season_type=${encodeURIComponent(seasonType)}`;
        let payload = null;
        try {
          const { rateLimitManager: rateLimitManager2 } = await Promise.resolve().then(() => (init_rate_limit_manager(), rate_limit_manager_exports));
          const endpoint = "stats/nfl/week";
          const canCall = await rateLimitManager2.canCall("sleeper", endpoint).catch(() => true);
          if (!canCall) return out;
          const res = await fetch(url, { headers: { accept: "application/json" } });
          await rateLimitManager2.recordCall("sleeper", endpoint, res.ok ? 1 : 0, 0, { cached: false }).catch(() => void 0);
          if (!res.ok) return out;
          payload = await res.json();
        } catch {
          return out;
        }
        const byId = asPlayerStatMap(payload);
        if (!byId) return out;
        const complete = seasonType === "regular" && await recordNflWeeklyCoverage(this.prisma, query.season, query.week, byId).catch(() => false);
        if (query.certifyAbsentPlayers && !complete) return out;
        const want = new Set(query.playerIds);
        const absent = query.certifyAbsentPlayers && complete ? new Set((await this.prisma.playerIdentityMap.findMany({
          where: { sport: "NFL", sleeperId: { in: [...want].filter((id2) => !byId.has(id2)) } },
          select: { sleeperId: true }
        })).flatMap((row) => row.sleeperId ? [row.sleeperId] : [])) : /* @__PURE__ */ new Set();
        for (const playerId of want) {
          const defense = teamAbbrevFromDefPlayerId(playerId);
          const raw = byId.get(defense ?? playerId);
          if (!raw) {
            if (absent.has(playerId) || query.certifyAbsentPlayers && complete && defense && getAllCanonicalTeams().some((team) => team.abbrev === defense)) out.set(playerId, {});
            continue;
          }
          const normalized = defense ? normalizeNflTeamDefenseWeeklyStats(raw) : normalizeNflWeeklyStats(raw);
          if (Object.keys(normalized).length > 0) out.set(playerId, normalized);
        }
        return out;
      }
    };
  }
});

// lib/redraft/playerWeeklyScoreService.ts
var playerWeeklyScoreService_exports = {};
__export(playerWeeklyScoreService_exports, {
  findCachedWeekPayload: () => findCachedWeekPayload,
  isTeamDefenseRow: () => isTeamDefenseRow,
  normalizeNflTeamDefenseWeeklyStats: () => normalizeNflTeamDefenseWeeklyStats,
  normalizeNflWeeklyStats: () => normalizeNflWeeklyStats,
  pointsAllowedFromGame: () => pointsAllowedFromGame,
  syncPlayerWeeklyScoresForRedraftSeason: () => syncPlayerWeeklyScoresForRedraftSeason,
  teamAbbrevFromDefPlayerId: () => teamAbbrevFromDefPlayerId
});
function candidateSportKeys(sport) {
  const upper = sport.toUpperCase();
  const lower = sport.toLowerCase();
  return upper === lower ? [upper] : [upper, lower];
}
async function recordScoreSyncAudit(summary, actorId) {
  try {
    await prisma.adminAuditLog?.create({
      data: {
        adminUserId: actorId,
        action: "redraft_score_sync",
        targetType: "redraft_season",
        targetId: summary.seasonId,
        details: summary
      }
    });
  } catch {
  }
}
async function syncPlayerWeeklyScoresForRedraftSeason(params) {
  const season = await prisma.redraftSeason.findFirst({
    where: params.seasonId ? { id: params.seasonId } : { leagueId: params.leagueId },
    orderBy: params.seasonId ? void 0 : { createdAt: "desc" }
  });
  if (!season) {
    throw new Error("Redraft season not found");
  }
  const week = Math.max(1, Number(params.week ?? season.currentWeek ?? 1) || 1);
  const sport = String(season.sport || "NFL").toUpperCase();
  const seasonYear = Number(season.season);
  const isDailySport = isDailyStatSport(sport);
  if (sport !== "NFL" && !isDailySport) {
    throw new Error(`Weekly stat sync supports NFL, NBA, NHL, MLB and NCAAB; ${sport} is not available yet.`);
  }
  const rosters = await prisma.redraftRoster.findMany({
    where: { seasonId: season.id, leagueId: season.leagueId },
    select: { id: true }
  });
  const rosterIds = rosters.map((r) => r.id);
  let rosterPlayers = rosterIds.length ? await prisma.redraftRosterPlayer.findMany({
    where: { rosterId: { in: rosterIds }, droppedAt: null },
    select: { playerId: true, sport: true, position: true, team: true }
  }) : [];
  const historyLeague = await prisma.league.findFirst({ where: { id: season.leagueId }, select: { settings: true } });
  if (usesLineupHistory(historyLeague?.settings)) {
    const history = await readLineupHistory(prisma, season.leagueId, season.id, new Date(Date.UTC(seasonYear + 2, 0, 1)));
    const byId = new Map(rosterPlayers.map((p) => [p.playerId, p]));
    for (const event of history.events) if (event.afterState) byId.set(event.afterState.playerId, event.afterState);
    rosterPlayers = [...byId.values()];
  }
  const playerIds = Array.from(new Set(rosterPlayers.map((p) => p.playerId).filter(Boolean)));
  const summary = {
    leagueId: season.leagueId,
    seasonId: season.id,
    sport,
    season: seasonYear,
    week,
    rosteredPlayers: playerIds.length,
    cacheRowsRead: 0,
    scoresUpserted: 0,
    missingCachePlayerIds: [],
    missingWeekPlayerIds: [],
    missingStatPlayerIds: [],
    weekStatsFromProvider: 0,
    warnings: []
  };
  if (!playerIds.length) {
    summary.warnings.push("No rostered players found for this redraft season.");
    await recordScoreSyncAudit(summary, params.actorId ?? "system");
    return summary;
  }
  const cachedRows = isDailySport ? [] : await prisma.playerGameLogCache.findMany({
    where: {
      playerId: { in: playerIds },
      season: String(seasonYear),
      seasonType: "regular",
      sport: { in: candidateSportKeys(sport) }
    }
  });
  summary.cacheRowsRead = cachedRows.length;
  const dailyRowsByPlayer = /* @__PURE__ */ new Map();
  if (isDailySport) {
    const seasonStartUtc = params.seasonStartUtc ?? resolveDailySportSeasonStart(sport, seasonYear);
    const window2 = weekWindowFromSeasonStart(seasonStartUtc, week);
    if (!window2) {
      const known = knownDailySportSeasons(sport);
      summary.warnings.push(
        `${sport} is a daily sport: a week is a date range, and scoring one needs the regular-season start date. None was supplied and none is recorded for season ${seasonYear}${known.length ? ` (recorded: ${known.join(", ")})` : ""}. Add it to \`lib/season-week/dailySportSeasonStarts.ts\`. No scores were written.`
      );
      await recordScoreSyncAudit(summary, params.actorId ?? "system");
      return summary;
    }
    const bridge = await bridgeRosterIdsToGameLogIds(prisma, candidateSportKeys(sport), playerIds);
    summary.gameLogIdsBridged = bridge.bridged;
    if (bridge.ambiguous.length) {
      summary.warnings.push(
        `${bridge.ambiguous.length} roster id(s) are both a Rolling Insights id and another player's Sleeper id in ${sport}; they were NOT matched to game logs rather than risk scoring the wrong player: ${bridge.ambiguous.slice(0, 5).join(", ")}.`
      );
    }
    const gameRows = await prisma.playerGameStat.findMany({
      where: {
        playerId: { in: bridge.gameLogIds },
        sportType: { in: candidateSportKeys(sport) },
        gameDate: { gte: window2.start, lt: window2.end }
      },
      select: { playerId: true, normalizedStatMap: true, sportType: true, season: true, gameDate: true }
    });
    for (const row of gameRows) {
      if (resolveStoredSeasonType({
        normalizedStatMap: row.normalizedStatMap,
        sport: row.sportType,
        season: row.season,
        gameDate: row.gameDate
      }) === "pre") {
        summary.preseasonRowsSkipped = (summary.preseasonRowsSkipped ?? 0) + 1;
        continue;
      }
      const rosterId = bridge.rosterIdFor(row.playerId);
      if (!rosterId) continue;
      const rows = dailyRowsByPlayer.get(rosterId) ?? [];
      rows.push(row.normalizedStatMap);
      dailyRowsByPlayer.set(rosterId, rows);
    }
    summary.cacheRowsRead = gameRows.length;
  }
  const cacheByPlayer = new Map(
    cachedRows.map((row) => [row.playerId, row])
  );
  const sportByPlayer = new Map(rosterPlayers.map((p) => [p.playerId, String(p.sport || sport).toUpperCase()]));
  const positionByPlayer = new Map(
    rosterPlayers.map((p) => [p.playerId, p.position ?? null])
  );
  const teamByPlayer = new Map(
    rosterPlayers.map((p) => [p.playerId, p.team ?? null])
  );
  const teamDefensePlayerIds = playerIds.filter((id2) => isTeamDefenseRow(id2, positionByPlayer.get(id2) ?? null));
  const gameByTeam = /* @__PURE__ */ new Map();
  if (teamDefensePlayerIds.length > 0) {
    const rawGames = await prisma.sportsGame.findMany({
      where: { sport: { in: candidateSportKeys(sport) }, season: seasonYear, week },
      select: { homeTeam: true, awayTeam: true, homeScore: true, awayScore: true, source: true, fetchedAt: true }
    });
    const games = pickFreshestSourceRows(rawGames);
    for (const g4 of games) {
      const home = String(g4.homeTeam ?? "").trim().toUpperCase();
      const away = String(g4.awayTeam ?? "").trim().toUpperCase();
      if (home) gameByTeam.set(home, g4);
      if (away) gameByTeam.set(away, g4);
    }
  }
  const cachedWeekStatsFor = (playerId) => {
    const cached = cacheByPlayer.get(playerId);
    const weekPayload = cached ? findCachedWeekPayload(cached.payload, week) : null;
    return weekPayload ? normalizeNflWeeklyStats(weekPayload) : {};
  };
  const weekStatsByPlayer = /* @__PURE__ */ new Map();
  if (!isDailySport && candidateSportKeys(sport).includes("NFL")) {
    const uncachedOffensiveIds = playerIds.filter(
      (id2) => !isTeamDefenseRow(id2, positionByPlayer.get(id2) ?? null) && (params.refreshNflStats || Object.keys(cachedWeekStatsFor(id2)).length === 0)
    );
    const requestedIds = params.refreshNflStats ? playerIds : uncachedOffensiveIds;
    if (requestedIds.length > 0) {
      const fetcher = params.fetchNflWeekStats ?? fetchNflWeekStatsFromLiveProvider;
      const fetched = await fetcher({ season: seasonYear, week, playerIds: requestedIds, ...params.refreshNflStats ? { requireComplete: true } : {} });
      if (params.refreshNflStats && requestedIds.some((id2) => !fetched.has(id2))) throw new Error("NFL_CORRECTION_REFRESH_INCOMPLETE");
      for (const [id2, stats] of fetched) {
        if (stats && (params.refreshNflStats || Object.keys(stats).length > 0)) weekStatsByPlayer.set(id2, stats);
      }
    }
  }
  const unmappedStatKeys = /* @__PURE__ */ new Set();
  for (const playerId of playerIds) {
    const position = positionByPlayer.get(playerId) ?? null;
    const playerSport = sportByPlayer.get(playerId) ?? sport;
    if (isTeamDefenseRow(playerId, position)) {
      const cached2 = cacheByPlayer.get(playerId);
      const weekPayload2 = cached2 ? findCachedWeekPayload(cached2.payload, week) : null;
      const stats2 = params.refreshNflStats ? { ...weekStatsByPlayer.get(playerId) } : weekPayload2 ? normalizeNflTeamDefenseWeeklyStats(weekPayload2) : {};
      if (stats2.def_points_allowed === void 0) {
        const team = teamAbbrevFromDefPlayerId(playerId) ?? String(teamByPlayer.get(playerId) ?? "").trim().toUpperCase();
        const game = team ? gameByTeam.get(team) : void 0;
        if (game) {
          const pa = pointsAllowedFromGame(game, team);
          if (pa !== null) stats2.def_points_allowed = pa;
        }
      }
      if (!Object.keys(stats2).length) {
        if (!cached2) summary.missingCachePlayerIds.push(playerId);
        else summary.missingWeekPlayerIds.push(playerId);
        continue;
      }
      const fantasyPts2 = await calculateScoreFromSportConfig(season.leagueId, playerId, week, stats2, position);
      await prisma.playerWeeklyScore.upsert({
        where: { playerId_week_season_sport: { playerId, week, season: seasonYear, sport: playerSport } },
        update: { stats: stats2, fantasyPts: fantasyPts2, isFinalized: false },
        create: { playerId, week, season: seasonYear, sport: playerSport, stats: stats2, fantasyPts: fantasyPts2, isFinalized: false }
      });
      summary.scoresUpserted += 1;
      continue;
    }
    if (isDailySport) {
      const dailyRows = dailyRowsByPlayer.get(playerId) ?? [];
      if (dailyRows.length === 0) {
        summary.missingCachePlayerIds.push(playerId);
        continue;
      }
      const normalizer = getDailySportNormalizer(sport);
      const aggregate = normalizer ? aggregateWeeklyStats(dailyRows, normalizer) : { stats: {}, gamesCounted: 0, unmappedKeys: [] };
      for (const key of aggregate.unmappedKeys) unmappedStatKeys.add(key);
      if (aggregate.gamesCounted === 0) {
        summary.missingWeekPlayerIds.push(playerId);
        continue;
      }
      if (!Object.keys(aggregate.stats).length) {
        summary.missingStatPlayerIds.push(playerId);
        continue;
      }
      const dailyPts = await calculateScoreFromSportConfig(
        season.leagueId,
        playerId,
        week,
        aggregate.stats,
        position
      );
      await prisma.playerWeeklyScore.upsert({
        where: { playerId_week_season_sport: { playerId, week, season: seasonYear, sport: playerSport } },
        update: { stats: aggregate.stats, fantasyPts: dailyPts, isFinalized: false },
        create: {
          playerId,
          week,
          season: seasonYear,
          sport: playerSport,
          stats: aggregate.stats,
          fantasyPts: dailyPts,
          isFinalized: false
        }
      });
      summary.scoresUpserted += 1;
      continue;
    }
    const cached = cacheByPlayer.get(playerId);
    const weekPayload = cached ? findCachedWeekPayload(cached.payload, week) : null;
    const cachedStats = cachedWeekStatsFor(playerId);
    const providerStats = weekStatsByPlayer.get(playerId) ?? {};
    const usedProvider = params.refreshNflStats || Object.keys(cachedStats).length === 0 && Object.keys(providerStats).length > 0;
    const stats = usedProvider ? providerStats : cachedStats;
    if (!Object.keys(stats).length && !(params.refreshNflStats && weekStatsByPlayer.has(playerId))) {
      if (!cached) summary.missingCachePlayerIds.push(playerId);
      else if (!weekPayload) summary.missingWeekPlayerIds.push(playerId);
      else summary.missingStatPlayerIds.push(playerId);
      continue;
    }
    if (usedProvider) summary.weekStatsFromProvider += 1;
    const fantasyPts = await calculateScoreFromSportConfig(season.leagueId, playerId, week, stats, positionByPlayer.get(playerId) ?? null);
    await prisma.playerWeeklyScore.upsert({
      where: {
        playerId_week_season_sport: {
          playerId,
          week,
          season: seasonYear,
          sport: playerSport
        }
      },
      update: {
        stats,
        fantasyPts,
        isFinalized: false
      },
      create: {
        playerId,
        week,
        season: seasonYear,
        sport: playerSport,
        stats,
        fantasyPts,
        isFinalized: false
      }
    });
    summary.scoresUpserted += 1;
  }
  if (summary.missingCachePlayerIds.length) {
    summary.warnings.push("Some rostered players do not have cached game logs. Run the provider/cache job before score sync.");
  }
  if (summary.missingWeekPlayerIds.length) {
    summary.warnings.push(`Some cached players do not have week ${week} rows yet.`);
  }
  if (summary.missingStatPlayerIds.length) {
    summary.warnings.push(`Some cached week rows did not contain recognized ${sport} stat keys.`);
  }
  if (unmappedStatKeys.size) {
    summary.warnings.push(
      `Unrecognized ${sport} provider stat keys (alias table may be wrong): ${[...unmappedStatKeys].sort().join(", ")}`
    );
  }
  await recordScoreSyncAudit(summary, params.actorId ?? "system");
  return summary;
}
var fetchNflWeekStatsFromLiveProvider;
var init_playerWeeklyScoreService = __esm({
  "lib/redraft/playerWeeklyScoreService.ts"() {
    "use strict";
    init_prisma();
    init_scoringEngine2();
    init_sports_live_scores_service();
    init_nflStatNormalization();
    init_nflStatNormalization();
    init_dailySportStatNormalization();
    init_dailySportSeasonStarts();
    init_riSeasonType();
    init_rosterGameLogIdBridge();
    init_lineupHistory();
    fetchNflWeekStatsFromLiveProvider = async ({ season, week, playerIds, requireComplete }) => {
      try {
        const { NflLiveStatsProvider: NflLiveStatsProvider2 } = await Promise.resolve().then(() => (init_nflLiveStatsProvider(), nflLiveStatsProvider_exports));
        const provider = new NflLiveStatsProvider2(prisma);
        return await provider.fetchPlayerStatsForGames({
          sport: "NFL",
          season,
          week,
          // The week-wide payload is keyed by player, not by game: the endpoint needs no slate, and
          // passing none is what lets this reconcile a week whose games are long finished.
          games: [],
          playerIds,
          certifyAbsentPlayers: requireComplete
        });
      } catch {
        return /* @__PURE__ */ new Map();
      }
    };
  }
});

// lib/guillotine/guillyRules.ts
function eliminationsForWeek(config, week, active) {
  const count2 = config.eliminationsByWeek?.[String(week)] ?? config.teamsPerChop;
  if (!Number.isInteger(count2) || count2 < 1)
    throw new Error("Eliminations must be a positive integer.");
  return Math.max(0, Math.min(count2, active - 1));
}
function guillySettings(raw) {
  const settings = raw ?? {};
  const rules = settings.guilly ?? {};
  return { eliminationsByWeek: rules.eliminationsByWeek ?? {}, waiverDelayHours: rules.waiverDelayHours ?? settings.guillotineWaiverDelay ?? 24 };
}
var init_guillyRules = __esm({
  "lib/guillotine/guillyRules.ts"() {
    "use strict";
  }
});

// lib/guillotine/constants.ts
var PUBLIC_PREFIX, GUILLOTINE_LEAGUE_IMAGE, GUILLOTINE_FIRST_ENTRY_VIDEO, GUILLOTINE_INTRO_VIDEO, DEFAULT_STAT_CORRECTION_HOURS, DEFAULT_DANGER_MARGIN_POINTS, TIEBREAK_BENCH_POINTS, TIEBREAK_SEASON_POINTS, TIEBREAK_PREVIOUS_PERIOD, TIEBREAK_DRAFT_SLOT, TIEBREAK_COMMISSIONER, TIEBREAK_RANDOM, DEFAULT_TIEBREAKER_ORDER, CORRECTION_AFTER_STAT, RELEASE_NEXT_WAIVER;
var init_constants = __esm({
  "lib/guillotine/constants.ts"() {
    "use strict";
    PUBLIC_PREFIX = process.env.NEXT_PUBLIC_APP_URL ?? "";
    GUILLOTINE_LEAGUE_IMAGE = process.env.GUILLOTINE_LEAGUE_IMAGE ?? `${PUBLIC_PREFIX}/guillotine/Guilly.png`;
    GUILLOTINE_FIRST_ENTRY_VIDEO = process.env.GUILLOTINE_FIRST_ENTRY_VIDEO ?? `${PUBLIC_PREFIX}/media/guilly/guilly-league-intro.mp4`;
    GUILLOTINE_INTRO_VIDEO = process.env.GUILLOTINE_INTRO_VIDEO ?? `${PUBLIC_PREFIX}/media/guilly/guilly-league-intro.mp4`;
    DEFAULT_STAT_CORRECTION_HOURS = 48;
    DEFAULT_DANGER_MARGIN_POINTS = 10;
    TIEBREAK_BENCH_POINTS = "bench_points";
    TIEBREAK_SEASON_POINTS = "season_points";
    TIEBREAK_PREVIOUS_PERIOD = "previous_period";
    TIEBREAK_DRAFT_SLOT = "draft_slot";
    TIEBREAK_COMMISSIONER = "commissioner";
    TIEBREAK_RANDOM = "random";
    DEFAULT_TIEBREAKER_ORDER = [
      TIEBREAK_BENCH_POINTS,
      TIEBREAK_SEASON_POINTS,
      TIEBREAK_PREVIOUS_PERIOD,
      TIEBREAK_DRAFT_SLOT,
      TIEBREAK_COMMISSIONER,
      TIEBREAK_RANDOM
    ];
    CORRECTION_AFTER_STAT = "after_stat_corrections";
    RELEASE_NEXT_WAIVER = "next_waiver_run";
  }
});

// lib/guillotine/GuillotineLeagueConfig.ts
var GuillotineLeagueConfig_exports = {};
__export(GuillotineLeagueConfig_exports, {
  DEFAULT_END_WEEK_BY_SPORT: () => DEFAULT_END_WEEK_BY_SPORT,
  SUPPORTED_SPORTS: () => SUPPORTED_SPORTS,
  getGuillotineConfig: () => getGuillotineConfig,
  isGuillotineLeague: () => isGuillotineLeague,
  upsertGuillotineConfig: () => upsertGuillotineConfig
});
function parseTiebreakerOrder(raw) {
  if (Array.isArray(raw) && raw.length > 0) {
    const allowed = /* @__PURE__ */ new Set([
      "bench_points",
      "season_points",
      "previous_period",
      "draft_slot",
      "commissioner",
      "random"
    ]);
    const out = raw.filter((s) => typeof s === "string" && allowed.has(s));
    if (out.length) return out;
  }
  return [...DEFAULT_TIEBREAKER_ORDER];
}
async function isGuillotineLeague(leagueId) {
  const config = await prisma.guillotineLeagueConfig.findUnique({
    where: { leagueId },
    select: { id: true }
  });
  if (config) return true;
  const league = await prisma.league.findUnique({
    where: { id: leagueId },
    select: { leagueVariant: true }
  });
  return league?.leagueVariant === GUILLOTINE_VARIANT;
}
async function getGuillotineConfig(leagueId) {
  const league = await prisma.league.findUnique({
    where: { id: leagueId },
    select: {
      id: true,
      sport: true,
      leagueVariant: true,
      settings: true
    }
  });
  if (!league) return null;
  const sport = normalizeToSupportedSport(league.sport);
  const row = await prisma.guillotineLeagueConfig.findUnique({
    where: { leagueId }
  });
  if (row) {
    return {
      leagueId: row.leagueId,
      sport,
      firstSeasonWeek: Number(league.settings?.redraft_start_week ?? 1),
      eliminationStartWeek: row.eliminationStartWeek,
      eliminationEndWeek: row.eliminationEndWeek,
      teamsPerChop: row.teamsPerChop,
      ...guillySettings(league.settings),
      correctionWindow: row.correctionWindow,
      customCutoffDayOfWeek: row.customCutoffDayOfWeek,
      customCutoffTimeUtc: row.customCutoffTimeUtc,
      statCorrectionHours: row.statCorrectionHours ?? DEFAULT_STAT_CORRECTION_HOURS,
      tiebreakerOrder: parseTiebreakerOrder(row.tiebreakerOrder),
      dangerMarginPoints: row.dangerMarginPoints ?? DEFAULT_DANGER_MARGIN_POINTS,
      rosterReleaseTiming: row.rosterReleaseTiming,
      commissionerOverride: row.commissionerOverride
    };
  }
  if (league.leagueVariant !== GUILLOTINE_VARIANT) return null;
  const settings = league.settings ?? {};
  const defaultEndWeek = DEFAULT_END_WEEK_BY_SPORT[sport] ?? 18;
  return {
    leagueId: league.id,
    sport,
    firstSeasonWeek: Number(league.settings?.redraft_start_week ?? 1),
    eliminationStartWeek: settings.eliminationStartWeek ?? 1,
    eliminationEndWeek: settings.eliminationEndWeek ?? defaultEndWeek,
    teamsPerChop: settings.teamsPerChop ?? 1,
    ...guillySettings(settings),
    correctionWindow: settings.correctionWindow ?? CORRECTION_AFTER_STAT,
    customCutoffDayOfWeek: settings.customCutoffDayOfWeek ?? null,
    customCutoffTimeUtc: settings.customCutoffTimeUtc ?? null,
    statCorrectionHours: settings.statCorrectionHours ?? DEFAULT_STAT_CORRECTION_HOURS,
    tiebreakerOrder: parseTiebreakerOrder(settings.tiebreakerOrder ?? DEFAULT_TIEBREAKER_ORDER),
    dangerMarginPoints: settings.dangerMarginPoints ?? DEFAULT_DANGER_MARGIN_POINTS,
    rosterReleaseTiming: settings.rosterReleaseTiming ?? RELEASE_NEXT_WAIVER,
    commissionerOverride: settings.commissionerOverride ?? true
  };
}
async function upsertGuillotineConfig(leagueId, input) {
  const league = await prisma.league.findUnique({
    where: { id: leagueId },
    select: { sport: true }
  });
  if (!league) return null;
  const sport = normalizeToSupportedSport(league.sport);
  const defaultEnd = DEFAULT_END_WEEK_BY_SPORT[sport] ?? 18;
  await prisma.guillotineLeagueConfig.upsert({
    where: { leagueId },
    create: {
      leagueId,
      eliminationStartWeek: input.eliminationStartWeek ?? 1,
      eliminationEndWeek: input.eliminationEndWeek ?? defaultEnd,
      teamsPerChop: input.teamsPerChop ?? 1,
      correctionWindow: input.correctionWindow ?? CORRECTION_AFTER_STAT,
      customCutoffDayOfWeek: input.customCutoffDayOfWeek ?? null,
      customCutoffTimeUtc: input.customCutoffTimeUtc ?? null,
      statCorrectionHours: input.statCorrectionHours ?? DEFAULT_STAT_CORRECTION_HOURS,
      tiebreakerOrder: toPrismaJsonInput(input.tiebreakerOrder ?? DEFAULT_TIEBREAKER_ORDER),
      dangerMarginPoints: input.dangerMarginPoints ?? DEFAULT_DANGER_MARGIN_POINTS,
      rosterReleaseTiming: input.rosterReleaseTiming ?? RELEASE_NEXT_WAIVER,
      commissionerOverride: input.commissionerOverride ?? true
    },
    update: {
      ...input.eliminationStartWeek !== void 0 && { eliminationStartWeek: input.eliminationStartWeek },
      ...input.eliminationEndWeek !== void 0 && { eliminationEndWeek: input.eliminationEndWeek },
      ...input.teamsPerChop !== void 0 && { teamsPerChop: input.teamsPerChop },
      ...input.correctionWindow !== void 0 && { correctionWindow: input.correctionWindow },
      ...input.customCutoffDayOfWeek !== void 0 && { customCutoffDayOfWeek: input.customCutoffDayOfWeek },
      ...input.customCutoffTimeUtc !== void 0 && { customCutoffTimeUtc: input.customCutoffTimeUtc },
      ...input.statCorrectionHours !== void 0 && { statCorrectionHours: input.statCorrectionHours },
      ...input.tiebreakerOrder !== void 0 && { tiebreakerOrder: toPrismaJsonInput(input.tiebreakerOrder) },
      ...input.dangerMarginPoints !== void 0 && { dangerMarginPoints: input.dangerMarginPoints },
      ...input.rosterReleaseTiming !== void 0 && { rosterReleaseTiming: input.rosterReleaseTiming },
      ...input.commissionerOverride !== void 0 && { commissionerOverride: input.commissionerOverride }
    }
  });
  return getGuillotineConfig(leagueId);
}
var GUILLOTINE_VARIANT, DEFAULT_END_WEEK_BY_SPORT;
var init_GuillotineLeagueConfig = __esm({
  "lib/guillotine/GuillotineLeagueConfig.ts"() {
    "use strict";
    init_prisma();
    init_guillyRules();
    init_prisma_json();
    init_sport_scope();
    init_constants();
    GUILLOTINE_VARIANT = "guillotine";
    DEFAULT_END_WEEK_BY_SPORT = {
      NFL: 18,
      NHL: 24,
      NBA: 24,
      MLB: 26,
      NCAAF: 14,
      NCAAB: 20,
      SOCCER: 38
    };
  }
});

// lib/draft-room/currentDraftSession.ts
var CURRENT_DRAFT_SESSION_ORDER;
var init_currentDraftSession = __esm({
  "lib/draft-room/currentDraftSession.ts"() {
    "use strict";
    CURRENT_DRAFT_SESSION_ORDER = [
      { createdAt: "desc" },
      { id: "desc" }
    ];
  }
});

// lib/guillotine/GuillotineWeekEvaluator.ts
function isPastCorrectionCutoff(args) {
  const now = args.now ?? /* @__PURE__ */ new Date();
  if (args.correctionWindow === "immediate") return true;
  if (args.correctionWindow === "after_stat_corrections" && args.statCorrectionHours != null) {
    const cutoff = new Date(args.periodEndedAt.getTime() + args.statCorrectionHours * 60 * 60 * 1e3);
    return now >= cutoff;
  }
  if (args.correctionWindow === "custom_cutoff") {
    if (args.customCutoffDayOfWeek != null && args.customCutoffTimeUtc) {
      const [h, m] = args.customCutoffTimeUtc.split(":").map(Number);
      const cutoff = new Date(args.periodEndedAt);
      cutoff.setUTCHours(h ?? 0, m ?? 0, 0, 0);
      const dayOffset = (args.customCutoffDayOfWeek - cutoff.getUTCDay() + 7) % 7;
      cutoff.setUTCDate(cutoff.getUTCDate() + dayOffset);
      if (cutoff < args.periodEndedAt) cutoff.setUTCDate(cutoff.getUTCDate() + 7);
      return now >= cutoff;
    }
    return true;
  }
  return true;
}
async function getDraftSlotByRoster(leagueId) {
  const session = await prisma.draftSession.findFirst({
    where: { leagueId },
    orderBy: CURRENT_DRAFT_SESSION_ORDER,
    select: { slotOrder: true }
  });
  const order = session?.slotOrder;
  if (!Array.isArray(order)) return /* @__PURE__ */ new Map();
  const map = /* @__PURE__ */ new Map();
  for (const o of order) {
    if (o?.rosterId != null && typeof o.slot === "number") map.set(String(o.rosterId), o.slot);
  }
  return map;
}
async function evaluateWeek(input) {
  const config = await getGuillotineConfig(input.leagueId);
  if (!config) return null;
  const { leagueId, weekOrPeriod, season = input.season ?? null } = input;
  const rosters = await prisma.roster.findMany({
    where: { leagueId },
    select: { id: true }
  });
  const rosterIds = rosters.map((r) => r.id);
  const chopped = await prisma.guillotineRosterState.findMany({
    where: { leagueId, choppedAt: { not: null } },
    select: { rosterId: true }
  });
  const alreadyChoppedRosterIds = chopped.map((c) => c.rosterId);
  const activeRosterIds = rosterIds.filter((id2) => !alreadyChoppedRosterIds.includes(id2));
  let scores;
  if (input.periodScores && input.periodScores.length > 0) {
    scores = input.periodScores.filter((s) => activeRosterIds.includes(s.rosterId));
  } else {
    const rows = await prisma.guillotinePeriodScore.findMany({
      where: { leagueId, weekOrPeriod },
      select: { rosterId: true, periodPoints: true, seasonPointsCumul: true }
    });
    const prevRows = await prisma.guillotinePeriodScore.findMany({
      where: { leagueId, weekOrPeriod: weekOrPeriod - 1 },
      select: { rosterId: true, periodPoints: true }
    });
    const prevByRoster = new Map(prevRows.map((r) => [r.rosterId, r.periodPoints]));
    scores = rows.filter((r) => activeRosterIds.includes(r.rosterId)).map((r) => ({
      rosterId: r.rosterId,
      periodPoints: r.periodPoints,
      seasonPointsCumul: r.seasonPointsCumul,
      previousPeriodPoints: prevByRoster.get(r.rosterId)
    }));
  }
  const pastCutoff = input.periodEndedAt ? isPastCorrectionCutoff({
    correctionWindow: config.correctionWindow,
    periodEndedAt: input.periodEndedAt,
    statCorrectionHours: config.statCorrectionHours,
    customCutoffDayOfWeek: config.customCutoffDayOfWeek,
    customCutoffTimeUtc: config.customCutoffTimeUtc
  }) : false;
  const orderedWorstFirst = [...scores].sort(
    (a, b) => a.periodPoints - b.periodPoints || a.seasonPointsCumul - b.seasonPointsCumul
  ).map((s) => s.rosterId);
  return {
    leagueId,
    weekOrPeriod,
    season: season ?? null,
    pastCutoff,
    activeRosterIds,
    scores,
    orderedWorstFirst,
    alreadyChoppedRosterIds
  };
}
async function savePeriodScores(args) {
  const { leagueId, weekOrPeriod, season, scores } = args;
  await prisma.$transaction(
    scores.map(
      (s) => prisma.guillotinePeriodScore.upsert({
        where: {
          leagueId_rosterId_weekOrPeriod: { leagueId, rosterId: s.rosterId, weekOrPeriod }
        },
        create: {
          leagueId,
          rosterId: s.rosterId,
          weekOrPeriod,
          season,
          periodPoints: s.periodPoints,
          seasonPointsCumul: s.seasonPointsCumul
        },
        update: {
          periodPoints: s.periodPoints,
          seasonPointsCumul: s.seasonPointsCumul
        }
      })
    )
  );
}
var init_GuillotineWeekEvaluator = __esm({
  "lib/guillotine/GuillotineWeekEvaluator.ts"() {
    "use strict";
    init_prisma();
    init_GuillotineLeagueConfig();
    init_currentDraftSession();
  }
});

// lib/league-import/importedRosterIdentity.ts
var init_importedRosterIdentity = __esm({
  "lib/league-import/importedRosterIdentity.ts"() {
    "use strict";
  }
});

// lib/redraft/redraftRosterIdentity.ts
var init_redraftRosterIdentity = __esm({
  "lib/redraft/redraftRosterIdentity.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/league-runtime/reconcileRosterRedraftLinks.ts
var import_server_only11;
var init_reconcileRosterRedraftLinks = __esm({
  "lib/league-runtime/reconcileRosterRedraftLinks.ts"() {
    "use strict";
    import_server_only11 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_importedRosterIdentity();
    init_redraftRosterIdentity();
  }
});

// lib/guillotine/GuillotineTiebreakResolver.ts
function resolveTiebreak(args) {
  const ordered = [...args.candidates].sort((a, b) => a.periodPoints - b.periodPoints || a.rosterId.localeCompare(b.rosterId));
  const n = Math.max(0, Math.min(args.teamsPerChop, ordered.length - 1));
  if (!n)
    return { choppedRosterIds: [], stepUsed: null, reason: "one survivor remains" };
  const boundary = ordered[n - 1].periodPoints;
  const fixed = ordered.filter((r) => r.periodPoints < boundary);
  let tied = ordered.filter((r) => r.periodPoints === boundary);
  let needed = n - fixed.length;
  let stepUsed = null;
  const chosen = [...fixed];
  if (tied.length === needed)
    return { choppedRosterIds: ordered.slice(0, n).map((r) => r.rosterId), stepUsed: null, reason: "lowest period scores" };
  for (const step of args.tiebreakerOrder) {
    if (tied.length <= needed)
      break;
    if (step === "commissioner") {
      const ids = new Set(args.commissionerChoppedRosterIds ?? []);
      const selection = tied.filter((r) => ids.has(r.rosterId));
      if (selection.length !== needed)
        return { choppedRosterIds: [], stepUsed: "commissioner", reason: "awaiting commissioner tiebreak decision" };
      chosen.push(...selection);
      needed = 0;
      stepUsed = step;
      break;
    }
    const value = (r) => {
      if (step === "bench_points")
        return r.benchPoints ?? 0;
      if (step === "season_points")
        return r.seasonPointsCumul;
      if (step === "previous_period")
        return r.previousPeriodPoints ?? 0;
      if (step === "draft_slot")
        return args.weekOrPeriod <= 1 ? -(args.draftSlotByRoster.get(r.rosterId) ?? 0) : 0;
      let hash = 2166136261;
      for (const c of String(args.weekOrPeriod) + ":" + r.rosterId)
        hash = Math.imul(hash ^ c.charCodeAt(0), 16777619);
      return hash >>> 0;
    };
    if (step === "draft_slot" && args.weekOrPeriod > 1)
      continue;
    tied.sort((a, b) => value(a) - value(b) || a.rosterId.localeCompare(b.rosterId));
    const cutoff = value(tied[needed - 1]);
    const lower = tied.filter((r) => value(r) < cutoff);
    if (lower.length || tied.some((r) => value(r) !== cutoff))
      stepUsed = step;
    chosen.push(...lower);
    needed -= lower.length;
    tied = tied.filter((r) => value(r) === cutoff);
  }
  if (needed > 0 && tied.length > needed)
    return { choppedRosterIds: [], stepUsed, reason: "unresolved tiebreak; commissioner decision required" };
  chosen.push(...tied.slice(0, needed));
  return { choppedRosterIds: chosen.map((r) => r.rosterId), stepUsed, reason: stepUsed ? "tiebreak: " + stepUsed : "lowest period scores" };
}
var init_GuillotineTiebreakResolver = __esm({
  "lib/guillotine/GuillotineTiebreakResolver.ts"() {
    "use strict";
  }
});

// lib/guillotine/GuillotineRosterReleaseEngine.ts
async function scheduleGuillyRosterRelease(tx, input) {
  const players = await tx.redraftRosterPlayer.findMany({ where: { rosterId: input.redraftRosterId, droppedAt: null } });
  for (const p of players)
    await tx.guillotineWaiverRelease.create({ data: { seasonId: input.seasonId, leagueId: input.leagueId, eliminatedRosterId: input.redraftRosterId, scoringPeriod: input.week, playerId: p.playerId, playerName: p.playerName, position: p.position, team: p.team, sport: p.sport, availableAt: input.availableAt, releaseStatus: "pending" } });
  await tx.redraftRosterPlayer.updateMany({ where: { rosterId: input.redraftRosterId, droppedAt: null }, data: { droppedAt: input.now } });
  await tx.roster.update({ where: { id: input.rosterId }, data: { playerData: [] } });
}
var init_GuillotineRosterReleaseEngine = __esm({
  "lib/guillotine/GuillotineRosterReleaseEngine.ts"() {
    "use strict";
    init_prisma();
    init_GuillotineLeagueConfig();
  }
});

// lib/league-chat/LeagueChatMessageService.ts
async function createLeagueChatMessage(leagueId, userId, message, options) {
  const source = typeof options.source === "string" && options.source.trim() ? options.source.trim() : options.source === null ? null : null;
  const created = await prisma.leagueChatMessage.create({
    data: {
      leagueId,
      userId,
      message,
      type: options.type ?? "text",
      imageUrl: options.imageUrl ?? null,
      metadata: toPrismaNullableJsonInput(options.metadata),
      source,
      discordMessageId: options.discordMessageId ?? null,
      sourceDiscord: options.sourceDiscord ?? false,
      isPrivate: options.isPrivate ?? false,
      visibleToUserId: options.visibleToUserId ?? null,
      messageSubtype: options.messageSubtype ?? null,
      mentionedUserIds: options.mentionedUserIds ?? [],
      globalBroadcastId: options.globalBroadcastId ?? null,
      parentMessageId: options.parentMessageId ?? null
    },
    include: includeUser
  });
  const withUser = created;
  const cm = created.metadata;
  const inboundName = typeof cm?.discordAuthorName === "string" ? cm.discordAuthorName : null;
  const inboundAvatar = typeof cm?.discordAuthorAvatarUrl === "string" ? cm.discordAuthorAvatarUrl : null;
  const out = {
    id: created.id,
    threadId: `league:${leagueId}`,
    parentMessageId: created.parentMessageId ?? null,
    channelSource: created.source ?? null,
    senderUserId: withUser.user?.id ?? created.userId,
    senderName: inboundName || withUser.user?.displayName || withUser.user?.email || "User",
    senderUsername: withUser.user?.username ?? null,
    senderAvatarUrl: inboundAvatar ?? withUser.user?.avatarUrl ?? null,
    senderAvatarPreset: withUser.user?.profile?.avatarPreset ?? null,
    messageType: created.type || "text",
    body: created.message || "",
    createdAt: created.createdAt.toISOString()
  };
  const cr = created;
  if (created.imageUrl || cr.metadata || cr.isPrivate || cr.messageSubtype || cr.globalBroadcastId) {
    const baseMeta = cr.metadata ?? {};
    out.metadata = {
      ...baseMeta,
      ...created.imageUrl && { imageUrl: created.imageUrl },
      lastActiveAt: created.createdAt.toISOString(),
      ...cr.isPrivate ? { isPrivate: true } : {},
      ...cr.visibleToUserId ? { visibleToUserId: cr.visibleToUserId } : {},
      ...cr.messageSubtype ? { messageSubtype: cr.messageSubtype } : {},
      ...cr.mentionedUserIds?.length ? { mentionedUserIds: cr.mentionedUserIds } : {},
      ...cr.globalBroadcastId ? { globalBroadcastId: cr.globalBroadcastId } : {}
    };
  }
  return out;
}
var includeUser;
var init_LeagueChatMessageService = __esm({
  "lib/league-chat/LeagueChatMessageService.ts"() {
    "use strict";
    init_prisma();
    init_prisma_json();
    includeUser = {
      user: {
        select: {
          id: true,
          username: true,
          displayName: true,
          email: true,
          avatarUrl: true,
          profile: { select: { avatarPreset: true } }
        }
      }
    };
  }
});

// lib/guillotine/guillotineChat.ts
async function postChopToLeagueChat(args) {
  const { leagueId, weekOrPeriod, choppedRosterIds, displayNames, userId } = args;
  const names = choppedRosterIds.map((id2) => displayNames[id2] ?? id2).join(", ");
  const message = choppedRosterIds.length > 1 ? `Week ${weekOrPeriod} \u2014 Chopped: ${names}. Their rosters have been released to waivers.` : `Week ${weekOrPeriod} \u2014 ${names} has been chopped. Their roster has been released to waivers.`;
  await createLeagueChatMessage(leagueId, userId, message, {
    type: "host_announcement",
    metadata: { senderIsHost: true, contentType: "guillotine_chop", guillotineChop: true, weekOrPeriod, choppedRosterIds }
  });
}
var init_guillotineChat = __esm({
  "lib/guillotine/guillotineChat.ts"() {
    "use strict";
    init_LeagueChatMessageService();
  }
});

// lib/guillotine/rosterDisplayNames.ts
async function resolveRosterDisplayNames(leagueId, rosterIds) {
  const ids = [...new Set(rosterIds.filter((id2) => typeof id2 === "string" && id2.length > 0))];
  const names = /* @__PURE__ */ new Map();
  if (ids.length === 0) return names;
  const rosters = await prisma.roster.findMany({
    where: { leagueId, id: { in: ids } },
    select: { id: true, platformUserId: true }
  });
  const ownerIds = [
    ...new Set(
      rosters.map((r) => r.platformUserId).filter((v) => typeof v === "string" && v.trim().length > 0)
    )
  ];
  if (ownerIds.length === 0) return names;
  const [teams, users] = await Promise.all([
    prisma.leagueTeam.findMany({
      where: { leagueId, externalId: { in: ownerIds } },
      select: { externalId: true, ownerName: true, teamName: true }
    }),
    prisma.appUser.findMany({
      where: { id: { in: ownerIds } },
      select: { id: true, displayName: true, username: true }
    })
  ]);
  const teamName = /* @__PURE__ */ new Map();
  for (const t of teams) {
    const name = t.ownerName?.trim() || t.teamName?.trim();
    if (name && !teamName.has(t.externalId)) teamName.set(t.externalId, name);
  }
  const userName = /* @__PURE__ */ new Map();
  for (const u of users) {
    const name = u.displayName?.trim() || (u.username?.trim() ? `@${u.username.trim()}` : "");
    if (name) userName.set(u.id, name);
  }
  for (const r of rosters) {
    const name = teamName.get(r.platformUserId) ?? userName.get(r.platformUserId);
    if (name) names.set(r.id, name);
  }
  return names;
}
var init_rosterDisplayNames = __esm({
  "lib/guillotine/rosterDisplayNames.ts"() {
    "use strict";
    init_prisma();
  }
});

// lib/guillotine/commissionerRules.ts
function readGuillotineControl(settings, seasonId) {
  const raw = settings?.[GUILLOTINE_COMMISSIONER_KEY];
  if (raw == null) return { control: null, error: null };
  const parsed = guillotineControlSchema.safeParse(raw);
  if (!parsed.success) return { control: null, error: "invalid_commissioner_controls" };
  if (parsed.data.seasonId !== seasonId) return { control: null, error: null };
  return { control: parsed.data, error: null };
}
function planGuillotineChop(policy, week, scores, legacyCount) {
  if (scores.some((n) => !Number.isFinite(n))) return { count: 0, hold: "invalid_scores" };
  if (policy?.finaleWeek != null && week > policy.finaleWeek) return { count: 0, hold: "past_automation_finale" };
  const count2 = policy?.finaleWeek != null ? Math.min(scores.length - 1, Math.max(1, Math.ceil((scores.length - 1) / (policy.finaleWeek - week + 1)))) : legacyCount;
  const ordered = [...scores].sort((a, b) => a - b);
  const margin = ordered[count2] - ordered[count2 - 1];
  return { count: count2, hold: policy?.reviewMargin != null && margin <= policy.reviewMargin ? "close_score_review_required" : null };
}
var import_zod3, GUILLOTINE_COMMISSIONER_KEY, guillotinePolicySchema, guillotineControlSchema;
var init_commissionerRules = __esm({
  "lib/guillotine/commissionerRules.ts"() {
    "use strict";
    import_zod3 = require("zod");
    GUILLOTINE_COMMISSIONER_KEY = "guillotineCommissioner";
    guillotinePolicySchema = import_zod3.z.object({
      finaleWeek: import_zod3.z.number().int().min(1).max(52).nullable(),
      reviewMargin: import_zod3.z.number().min(0).max(1e3).nullable(),
      releaseDelayHours: import_zod3.z.number().int().min(0).max(168)
    }).strict();
    guillotineControlSchema = import_zod3.z.object({
      version: import_zod3.z.literal(1),
      seasonId: import_zod3.z.string().min(1),
      paused: import_zod3.z.boolean(),
      policy: guillotinePolicySchema.nullable()
    }).strict();
  }
});

// lib/league/isNativeLeague.ts
function isNativePlatform(platform) {
  return NATIVE_PLATFORMS.has(String(platform ?? "").toLowerCase());
}
var NATIVE_PLATFORMS;
var init_isNativeLeague = __esm({
  "lib/league/isNativeLeague.ts"() {
    "use strict";
    NATIVE_PLATFORMS = /* @__PURE__ */ new Set([
      "allfantasy",
      "af",
      "manual",
      "native",
      /*
       * AF-created ADP seed fixtures (18 leagues in production). These are real `League` rows we
       * generated, not imports — there is no host platform holding their schedule — so the weekly
       * processor may pair and score them like any other native league.
       *
       * Listed explicitly rather than by prefix: `startsWith('allfantasy')` would silently adopt every
       * future `allfantasy_*` string, which is the denylist failure mode the header warns about,
       * pointing the other way. A new seed platform should be a deliberate line here.
       */
      "allfantasy_test_adp_seed",
      ""
    ]);
  }
});

// lib/guillotine/GuillotineEliminationEngine.ts
async function runElimination(input) {
  const config = await getGuillotineConfig(input.leagueId);
  if (!config)
    return null;
  const empty = (reason) => ({ leagueId: input.leagueId, weekOrPeriod: input.weekOrPeriod, choppedRosterIds: [], tiebreakStepUsed: null, reason });
  if (input.weekOrPeriod < config.eliminationStartWeek)
    return empty("before elimination start");
  if (config.eliminationEndWeek != null && input.weekOrPeriod > config.eliminationEndWeek)
    return empty("past elimination end");
  const slots = await getDraftSlotByRoster(input.leagueId);
  const result = await prisma.$transaction(async (tx) => {
    const season = await tx.guillotineSeason.findFirst({ where: { leagueId: input.leagueId, ...input.season != null ? { season: input.season } : {} }, orderBy: { season: "desc" } });
    if (!season?.redraftSeasonId)
      return empty("season_not_initialized");
    await tx.$queryRaw`SELECT id FROM redraft_seasons WHERE id = ${season.redraftSeasonId} FOR UPDATE`;
    const controlLeague = await tx.league.findUniqueOrThrow({ where: { id: input.leagueId }, select: { settings: true, platform: true, emergencyPaused: true } });
    const controls = readGuillotineControl(controlLeague.settings, season.redraftSeasonId);
    if (controls.error) return empty(controls.error);
    if (controls.control && (!isNativePlatform(controlLeague.platform) || controlLeague.emergencyPaused || controls.control.paused)) return empty("commissioner_automation_paused");
    const prior = await tx.guillotineEventLog.findFirst({ where: { leagueId: input.leagueId, eventType: "chop", metadata: { path: ["periodKey"], equals: season.id + ":" + input.weekOrPeriod } } });
    if (prior)
      return { ...empty("already_recorded"), audit: { recorded: false, reason: "already_recorded" } };
    const scoringSeason = await tx.redraftSeason.findUnique({ where: { id: season.redraftSeasonId } });
    if (scoringSeason?.status === "complete" || season.status === "complete")
      return empty("season_complete");
    const rosters = await tx.roster.findMany({ where: { leagueId: input.leagueId }, select: { id: true, redraftRosterId: true } });
    const active = await tx.redraftRoster.findMany({ where: { seasonId: season.redraftSeasonId, isEliminated: false } });
    if (active.length < 2)
      return empty(active.length === 1 ? "one survivor remains" : "no active teams; commissioner repair required");
    const currentConfig = await getGuillotineConfig(input.leagueId);
    if (!currentConfig)
      return empty("configuration_missing");
    const evaluation = await evaluateWeek(input);
    if (!evaluation?.pastCutoff)
      return empty("before stat correction cutoff");
    const activeIds = new Set(active.map((r) => r.id));
    const byId = new Map(rosters.map((r) => [r.id, r.redraftRosterId]));
    if (evaluation.scores.length !== active.length || evaluation.scores.some((s) => !byId.get(s.rosterId) || !activeIds.has(byId.get(s.rosterId))) || new Set(evaluation.scores.map((s) => s.rosterId)).size !== active.length || new Set(evaluation.scores.map((s) => byId.get(s.rosterId))).size !== active.length)
      return empty("roster_mapping_or_score_coverage_incomplete");
    const automation = planGuillotineChop(controls.control?.policy ?? null, input.weekOrPeriod, evaluation.scores.map((s) => s.periodPoints), eliminationsForWeek(currentConfig, input.weekOrPeriod, active.length));
    if (automation.hold) return empty(automation.hold);
    const decision = resolveTiebreak({ candidates: evaluation.scores, tiebreakerOrder: currentConfig.commissionerOverride ? currentConfig.tiebreakerOrder : currentConfig.tiebreakerOrder.filter((s) => s !== "commissioner"), teamsPerChop: automation.count, weekOrPeriod: input.weekOrPeriod, draftSlotByRoster: slots, commissionerChoppedRosterIds: currentConfig.commissionerOverride ? input.commissionerChoppedRosterIds : void 0 });
    if (!decision.choppedRosterIds.length)
      return { ...empty(decision.reason), tiebreakStepUsed: decision.stepUsed };
    const now = /* @__PURE__ */ new Date(), cut = new Set(decision.choppedRosterIds);
    const ordered = [...evaluation.scores].sort((a, b) => b.periodPoints - a.periodPoints);
    const safe = Math.min(...ordered.filter((s) => !cut.has(s.rosterId)).map((s) => s.periodPoints));
    const flagged = [];
    const availableAt = new Date(now.getTime() + (controls.control?.policy?.releaseDelayHours ?? (currentConfig.rosterReleaseTiming === "immediate" ? 0 : currentConfig.waiverDelayHours ?? 24)) * 36e5);
    for (const id2 of decision.choppedRosterIds) {
      const redraftId = byId.get(id2), meta = active.find((r) => r.id === redraftId), score = ordered.find((r) => r.rosterId === id2);
      await tx.guillotineRosterState.upsert({ where: { rosterId: id2 }, create: { leagueId: input.leagueId, rosterId: id2, choppedAt: now, choppedInPeriod: input.weekOrPeriod, choppedReason: decision.reason }, update: { choppedAt: now, choppedInPeriod: input.weekOrPeriod, choppedReason: decision.reason } });
      await tx.redraftRoster.update({ where: { id: redraftId }, data: { isEliminated: true } });
      flagged.push(redraftId);
      await scheduleGuillyRosterRelease(tx, { leagueId: input.leagueId, rosterId: id2, redraftRosterId: redraftId, seasonId: season.id, week: input.weekOrPeriod, now, availableAt });
      await tx.guillotineElimination.create({ data: { seasonId: season.id, leagueId: input.leagueId, eliminatedRosterId: redraftId, eliminatedTeamName: meta.teamName ?? meta.ownerName, eliminatedOwnerId: meta.ownerId, scoringPeriod: input.weekOrPeriod, finalScore: score.periodPoints, rankAmongActive: ordered.indexOf(score) + 1, marginBelowSafe: safe - score.periodPoints, wasTiebreaker: decision.stepUsed != null } });
    }
    for (let i = 0; i < ordered.length; i++) {
      const score = ordered[i], rosterId = byId.get(score.rosterId);
      const data = { totalScore: score.periodPoints, rankAmongActive: i + 1, teamsActiveThisPeriod: active.length, survivalStatus: cut.has(score.rosterId) ? "eliminated" : "survived", marginAboveChopLine: score.periodPoints - safe, wasInDangerZone: !cut.has(score.rosterId) && i >= Math.floor(active.length * 0.75) };
      await tx.guillotineSurvivalLog.upsert({ where: { seasonId_rosterId_scoringPeriod: { seasonId: season.id, rosterId, scoringPeriod: input.weekOrPeriod } }, create: { seasonId: season.id, leagueId: input.leagueId, rosterId, scoringPeriod: input.weekOrPeriod, ...data }, update: data });
    }
    const survivors = active.filter((r) => !flagged.includes(r.id)), complete = survivors.length === 1;
    await tx.guillotineSeason.update({ where: { id: season.id }, data: { currentScoringPeriod: input.weekOrPeriod, currentTeamsActive: survivors.length, status: complete ? "complete" : "active", isInFinalStage: complete, finalStageStartPeriod: complete ? input.weekOrPeriod : null } });
    if (complete) {
      const league = await tx.league.findUniqueOrThrow({ where: { id: input.leagueId }, select: { settings: true } });
      await tx.redraftSeason.update({ where: { id: season.redraftSeasonId }, data: { status: "complete" } });
      await tx.league.update({ where: { id: input.leagueId }, data: { lifecycleState: "completed", settings: toPrismaJsonInput({ ...league.settings ?? {}, guillyChampion: { season: season.season, seasonId: season.id, rosterId: survivors[0].id, displayName: survivors[0].teamName ?? survivors[0].ownerName, week: input.weekOrPeriod } }) } });
    }
    const metadata = { periodKey: season.id + ":" + input.weekOrPeriod, season: season.season, weekOrPeriod: input.weekOrPeriod, choppedRosterIds: decision.choppedRosterIds, availableAt: availableAt.toISOString(), championRosterId: complete ? survivors[0].id : null, tiebreakStepUsed: decision.stepUsed, reason: decision.reason, commissionerPolicy: controls.control?.policy ?? null };
    for (const eventType of ["chop", "chop_animation_trigger", "roster_released"])
      await tx.guillotineEventLog.create({ data: { leagueId: input.leagueId, eventType, metadata: toPrismaJsonInput(metadata) } });
    return { leagueId: input.leagueId, weekOrPeriod: input.weekOrPeriod, choppedRosterIds: decision.choppedRosterIds, tiebreakStepUsed: decision.stepUsed, reason: decision.reason, eliminationFlagged: { marked: flagged, unresolved: [] }, audit: { recorded: true, seasonId: season.id, eliminations: flagged.length, survivalRows: ordered.length, finalStageReached: complete } };
  }, { timeout: 3e4 });
  if (result.choppedRosterIds.length && !input.skipChat && input.systemUserId) {
    try {
      const names = await resolveRosterDisplayNames(input.leagueId, result.choppedRosterIds);
      await postChopToLeagueChat({ leagueId: input.leagueId, weekOrPeriod: input.weekOrPeriod, choppedRosterIds: result.choppedRosterIds, displayNames: Object.fromEntries(names), userId: input.systemUserId });
    } catch {
    }
  }
  return result;
}
var init_GuillotineEliminationEngine = __esm({
  "lib/guillotine/GuillotineEliminationEngine.ts"() {
    "use strict";
    init_prisma();
    init_prisma_json();
    init_reconcileRosterRedraftLinks();
    init_GuillotineLeagueConfig();
    init_GuillotineWeekEvaluator();
    init_GuillotineTiebreakResolver();
    init_guillyRules();
    init_GuillotineRosterReleaseEngine();
    init_guillotineChat();
    init_rosterDisplayNames();
    init_commissionerRules();
    init_isNativeLeague();
  }
});

// lib/guillotine/ensureGuillotineSeason.ts
async function ensureGuillotineSeason(input) {
  if (!await isGuillotineLeague(input.leagueId)) {
    return { ok: false, reason: "NOT_GUILLOTINE" };
  }
  const redraftSeason = await prisma.redraftSeason.findFirst({
    where: { id: input.redraftSeasonId, leagueId: input.leagueId },
    select: { id: true, sport: true, season: true, currentWeek: true }
  });
  if (!redraftSeason) return { ok: false, reason: "REDRAFT_SEASON_NOT_FOUND" };
  const existing = await prisma.guillotineSeason.findFirst({
    where: { redraftSeasonId: redraftSeason.id },
    select: { id: true }
  });
  if (existing) return { ok: true, created: false, seasonId: existing.id };
  const rosters = await prisma.redraftRoster.count({ where: { seasonId: redraftSeason.id } });
  if (rosters === 0) return { ok: false, reason: "NO_ROSTERS" };
  try {
    const created = await prisma.guillotineSeason.create({
      data: {
        leagueId: input.leagueId,
        redraftSeasonId: redraftSeason.id,
        sport: redraftSeason.sport,
        season: redraftSeason.season,
        status: "setup",
        totalTeamsStarted: rosters,
        currentTeamsActive: rosters,
        currentScoringPeriod: Math.max(0, (redraftSeason.currentWeek ?? 1) - 1)
      },
      select: { id: true }
    });
    return { ok: true, created: true, seasonId: created.id };
  } catch {
    const winner = await prisma.guillotineSeason.findFirst({
      where: { redraftSeasonId: redraftSeason.id },
      select: { id: true }
    });
    if (winner) return { ok: true, created: false, seasonId: winner.id };
    throw new Error("guillotine season create failed");
  }
}
var init_ensureGuillotineSeason = __esm({
  "lib/guillotine/ensureGuillotineSeason.ts"() {
    "use strict";
    init_prisma();
    init_GuillotineLeagueConfig();
  }
});

// lib/providers/rollingInsightsNflFieldMap.ts
var ROLLING_INSIGHTS_NFL_ENDPOINTS, ROLLING_INSIGHTS_NFL_PROFILE_FIELD_MAP, ROLLING_INSIGHTS_NFL_PLAYER_STATS_FIELD_MAP, ROLLING_INSIGHTS_NFL_LIVE_FIELD_MAP, ROLLING_INSIGHTS_NFL_INJURY_FIELD_MAP, ROLLING_INSIGHTS_NFL_DEPTH_CHART_FIELD_MAP, ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP, ROLLING_INSIGHTS_NFL_SCHEDULE_FIELD_MAP;
var init_rollingInsightsNflFieldMap = __esm({
  "lib/providers/rollingInsightsNflFieldMap.ts"() {
    "use strict";
    ROLLING_INSIGHTS_NFL_ENDPOINTS = {
      scheduleSeason: "/api/v1/schedule-season/<DATE>/NFL",
      scheduleWeek: "/api/v1/schedule-week/<DATE>/NFL",
      scheduleDay: "/api/v1/schedule/<DATE>/NFL",
      live: "/api/v1/live/<DATE>/NFL",
      teamInfo: "/api/v1/team-info/NFL",
      teamStats: "/api/v1/team-stats/<DATE>/NFL",
      playerInfo: "/api/v1/player-info/NFL",
      playerStats: "/api/v1/player-stats/<DATE>/NFL",
      injuries: "/api/v1/injuries/NFL",
      depthCharts: "/api/v1/depth-charts/NFL",
      playByPlay: "/api/v1/play-by-play/NFL"
    };
    ROLLING_INSIGHTS_NFL_PROFILE_FIELD_MAP = {
      providerPlayerId: "player_id",
      fullName: "player",
      teamName: "team",
      providerTeamId: "team_id",
      jerseyNumber: "number",
      status: "status",
      position: "position",
      positionCategory: "position_category",
      height: "height",
      weight: "weight",
      birthDateRaw: "age",
      college: "college",
      headshotImageId: "img",
      allStar: "all_star"
    };
    ROLLING_INSIGHTS_NFL_PLAYER_STATS_FIELD_MAP = {
      gamesPlayed: "regular_season.games_played",
      fantasyPoints: "regular_season.DK_fantasy_points",
      fantasyPointsPerGame: "regular_season.DK_fantasy_points_per_game",
      completions: "completions",
      passingAttempts: "passing_attempts",
      passingYards: "passing_yards",
      passingTouchdowns: "passing_touchdowns",
      passingInterceptions: "passing_interceptions",
      passerRating: "passer_rating",
      rushingAttempts: "rushing_attempts",
      rushingYards: "rushing_yards",
      rushingTouchdowns: "rushing_touchdowns",
      receptions: "receptions",
      receivingYards: "receiving_yards",
      receivingTouchdowns: "receiving_touchdowns",
      fumbles: "fumbles",
      fumblesLost: "fumbles_lost",
      tackles: "tackles",
      sacks: "sacks",
      interceptions: "interceptions",
      fieldGoalsAttempted: "field_goals_attempted",
      fieldGoalsMade: "field_goals_made",
      fieldGoalsLong: "field_goals_long",
      extraPointsAttempted: "extra_points_attempted",
      extraPointsMade: "extra_points_made",
      puntReturns: "punt_returns",
      puntReturnYards: "punt_return_yards",
      puntReturnTouchdowns: "punt_return_touchdowns",
      kickReturns: "kick_returns",
      kickReturnYards: "kick_return_yards",
      kickReturnTouchdowns: "kick_return_touchdowns"
    };
    ROLLING_INSIGHTS_NFL_LIVE_FIELD_MAP = {
      gameId: "game_ID",
      gameStatus: "game_status",
      currentGameState: "full_box.current",
      playerLiveStats: "player_box",
      teamLiveStatsHome: "full_box.home_team.team_stats",
      teamLiveStatsAway: "full_box.away_team.team_stats"
    };
    ROLLING_INSIGHTS_NFL_INJURY_FIELD_MAP = {
      providerPlayerId: "player_id",
      playerName: "player",
      injury: "injury",
      returnStatus: "returns",
      dateInjured: "date_injured",
      teamName: "team",
      providerTeamId: "team_id"
    };
    ROLLING_INSIGHTS_NFL_DEPTH_CHART_FIELD_MAP = {
      providerPlayerId: "id",
      playerName: "player",
      depthPositionKey: "<team>.<position_key>",
      depthRankKey: "<team>.<position_key>.<rank_key>",
      providerTeamId: "team_id"
    };
    ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP = {
      providerTeamId: "team_id",
      teamName: "team",
      abbreviation: "abbrv",
      mascot: "mascot",
      conference: "conf",
      city: "city",
      state: "state",
      arena: "arena",
      country: "country",
      field: "field",
      dome: "dome"
    };
    ROLLING_INSIGHTS_NFL_SCHEDULE_FIELD_MAP = {
      gameId: "game_ID",
      gameTime: "game_time",
      season: "season",
      seasonType: "season_type",
      week: "week",
      status: "status",
      homeTeam: "home_team",
      awayTeam: "away_team",
      homeTeamId: "home_team_ID",
      awayTeamId: "away_team_ID"
    };
  }
});

// lib/providers/rollingInsightsFieldMaps.ts
function getRollingInsightsSportCode(sport) {
  const raw = String(sport ?? "").trim().toUpperCase().replace(/[\s-]+/g, "_");
  if (raw === "NCAAF" || raw === "NCAAFB" || raw === "NCAA_FOOTBALL" || raw === "COLLEGE_FOOTBALL" || raw === "NCAA_FB" || raw === "NCAA_F" || raw === "CFB" || raw === "NCAAFOOTBALL") {
    return "NCAAFB";
  }
  if (raw === "EURO" || raw === "UEFA") return "SOCCER";
  if (normalizeSoccerLeague(raw) || normalizeSoccerLeague(raw.replace(/_/g, " "))) return "SOCCER";
  if (raw === "NCAABB") return "NCAABB";
  const su = normalizeToSupportedSport(sport);
  if (su === "NCAAB") return "NCAABB";
  if (su === "NCAAF") return "NCAAFB";
  if (su === "SOCCER") return "SOCCER";
  if (su === "NFL") return "NFL";
  if (su === "NBA") return "NBA";
  if (su === "MLB") return "MLB";
  if (su === "NHL") return "NHL";
  return "NFL";
}
function endpointsFor(code) {
  const base = (sp) => ({
    scheduleSeason: `/api/v1/schedule-season/<DATE>/${sp}`,
    scheduleWeek: `/api/v1/schedule-week/<DATE>/${sp}`,
    scheduleDay: `/api/v1/schedule/<DATE>/${sp}`,
    live: `/api/v1/live/<DATE>/${sp}`,
    teamInfo: `/api/v1/team-info/${sp}`,
    teamStats: `/api/v1/team-stats/<DATE>/${sp}`,
    playerInfo: `/api/v1/player-info/${sp}`,
    playerStats: `/api/v1/player-stats/<DATE>/${sp}`,
    injuries: `/api/v1/injuries/${sp}`,
    depthCharts: `/api/v1/depth-charts/${sp}`,
    playByPlay: `/api/v1/play-by-play/${sp}`
  });
  return base(code);
}
var NCAAFB_ENDPOINT_OVERRIDES, SOCCER_ENDPOINT_OVERRIDES, ROLLING_INSIGHTS_ENDPOINTS_BY_SPORT, NBA_LIVE_AND_BOX, NBA_SCHEDULE, NBA_LIVE_SHELL, NCAABB_PROFILE, NCAABB_SCHEDULE_EXTRA, MLB_BATTING, MLB_PITCHING, NHL_SKATERS, NHL_GOALIES, NCAAFB_PROFILE, NCAAFB_STATS, NCAAFB_LIVE_TOP, NCAAFB_LIVE_CURRENT, NCAAFB_LIVE_TEAM_SHELL, NCAAFB_LIVE_TEAM_STATS, NCAAFB_LIVE_PLAYER_BOX, NCAAFB_SCHEDULE, NCAAFB_TEAM_INFO, NCAAFB_TEAM_SEASON_STATS, SOCCER_PROFILE, SOCCER_SCHEDULE, SOCCER_TEAM_INFO, SOCCER_LIVE_TOP, SOCCER_LIVE_TEAM_SHELL, SOCCER_LIVE_TEAM_STATS, SOCCER_LIVE_PLAYER, SOCCER_LIVE_GOALKEEPER, SOCCER_TEAM_SEASON_STATS, ROLLING_INSIGHTS_FIELD_MAPS;
var init_rollingInsightsFieldMaps = __esm({
  "lib/providers/rollingInsightsFieldMaps.ts"() {
    "use strict";
    init_sport_scope();
    init_rollingInsightsSoccerLeague();
    init_rollingInsightsNflFieldMap();
    NCAAFB_ENDPOINT_OVERRIDES = {
      scheduleSeason: `/api/v1/schedule-season/<YYYY>/NCAAFB`,
      scheduleWeek: `/api/v1/schedule-week/<YYYY-MM-DD>/NCAAFB`,
      scheduleDay: `/api/v1/schedule/<YYYY-MM-DD>/NCAAFB`,
      teamStats: `/api/v1/team-stats/<YYYY>/NCAAFB`
    };
    SOCCER_ENDPOINT_OVERRIDES = {
      scheduleSeason: `/api/v1/schedule-season/<YYYY>/SOCCER`,
      scheduleWeek: `/api/v1/schedule-weekly/<YYYY-MM-DD>/SOCCER`,
      scheduleDay: `/api/v1/schedule-daily/<YYYY-MM-DD>/SOCCER`,
      scheduleDayAlias: `/api/v1/schedule/<YYYY-MM-DD>/SOCCER`,
      teamStats: `/api/v1/team-stats/<YYYY>/SOCCER`,
      live: `/api/v1/live/<YYYY-MM-DD>/SOCCER`,
      playerInfo: `/api/v1/player-info/SOCCER`,
      teamInfo: `/api/v1/team-info/SOCCER`
    };
    ROLLING_INSIGHTS_ENDPOINTS_BY_SPORT = {
      NFL: { ...ROLLING_INSIGHTS_NFL_ENDPOINTS },
      NBA: endpointsFor("NBA"),
      MLB: endpointsFor("MLB"),
      NHL: endpointsFor("NHL"),
      NCAABB: endpointsFor("NCAABB"),
      NCAAFB: { ...endpointsFor("NCAAFB"), ...NCAAFB_ENDPOINT_OVERRIDES },
      SOCCER: { ...endpointsFor("SOCCER"), ...SOCCER_ENDPOINT_OVERRIDES }
    };
    NBA_LIVE_AND_BOX = {
      fullName: "player",
      position: "position",
      status: "status",
      points: "points",
      assists: "assists",
      totalRebounds: "total_rebounds",
      defensiveRebounds: "defensive_rebounds",
      offensiveRebounds: "offensive_rebounds",
      steals: "steals",
      blocks: "blocks",
      turnovers: "turnovers",
      fouls: "fouls",
      minutes: "minutes",
      fieldGoalsMade: "field_goals_made",
      fieldGoalsAttempted: "field_goals_attempted",
      threePointsMade: "three_points_made",
      threePointsAttempted: "three_points_attempted",
      freeThrowsMade: "free_throws_made",
      freeThrowsAttempted: "free_throws_attempted",
      twoPointsMade: "two_points_made",
      twoPointsAttempted: "two_points_attempted",
      twoPointPercentage: "two_point_percentage"
    };
    NBA_SCHEDULE = {
      gameId: "game_ID",
      gameTime: "game_time",
      season: "season",
      seasonType: "season_type",
      status: "status",
      homeTeam: "home_team",
      awayTeam: "away_team",
      homeTeamId: "home_team_ID",
      awayTeamId: "away_team_ID",
      broadcast: "broadcast"
    };
    NBA_LIVE_SHELL = {
      currentQuarter: "full_box.current.Quarter",
      currentTimeRemaining: "full_box.current.TimeRemaining",
      playerBox: "player_box",
      teamStats: "team_stats"
    };
    NCAABB_PROFILE = {
      fullName: "player",
      position: "position",
      status: "status",
      starter: "starter"
    };
    NCAABB_SCHEDULE_EXTRA = {
      ...NBA_SCHEDULE,
      region: "region",
      round: "round",
      awayTeamSeed: "away_team_seed",
      homeTeamSeed: "home_team_seed",
      regionDisplayOrder: "region_display_order"
    };
    MLB_BATTING = {
      player: "player",
      status: "status",
      pos: "POS",
      positionCategory: "position_category",
      batOrder: "BAT_ORD",
      ab: "AB",
      h: "H",
      r: "R",
      rbi: "RBI",
      hr: "HR",
      bb: "BB",
      so: "SO",
      sb: "SB",
      cs: "CS",
      single: "1B",
      double: "2B",
      triple: "3B",
      hbp: "HBP",
      ibb: "IBB",
      e: "E"
    };
    MLB_PITCHING = {
      player: "player",
      status: "status",
      pos: "POS",
      positionCategory: "position_category",
      ip: "IP",
      h: "H",
      k: "K",
      bb: "BB",
      er: "ER",
      r: "R",
      hr: "HR",
      w: "W",
      l: "L",
      s: "S",
      hld: "HLD",
      bs: "BS",
      pitches: "pitches",
      strikes: "strikes"
    };
    NHL_SKATERS = {
      player: "player",
      status: "status",
      position: "position",
      positionCategory: "position_category",
      goals: "goals",
      assists: "assists",
      shotsOnGoal: "shots_on_goal",
      hits: "hits",
      blocks: "blocks",
      plusMinus: "plus_minus",
      penaltyMinutes: "penalty_minutes",
      powerPlayGoals: "power_play_goals",
      powerPlayAssists: "power_play_assists",
      faceoffsWon: "faceoffs_won",
      faceoffsLost: "faceoffs_lost",
      giveaways: "giveaways",
      takeaways: "takeaways",
      timeOnIce: "time_on_ice"
    };
    NHL_GOALIES = {
      player: "player",
      status: "status",
      position: "position",
      positionCategory: "position_category",
      saves: "saves",
      goalsAllowed: "goals_allowed",
      shotsAgainst: "shots_against",
      win: "win",
      loss: "loss",
      overtimeLoss: "overtime_loss",
      shutouts: "shutouts",
      timeOnIce: "time_on_ice"
    };
    NCAAFB_PROFILE = {
      providerPlayerId: "player_id",
      fullName: "player",
      teamName: "team",
      providerTeamId: "team_id",
      jerseyNumber: "number",
      status: "status",
      position: "position",
      positionCategory: "position_category",
      height: "height",
      weight: "weight",
      collegeClass: "class"
    };
    NCAAFB_STATS = {
      gamesPlayed: "regular_season.games_played",
      completions: "regular_season.completions",
      passingAttempts: "regular_season.passing_attempts",
      passingYards: "regular_season.passing_yards",
      passingTouchdowns: "regular_season.passing_touchdowns",
      passingInterceptions: "regular_season.passing_interceptions",
      passerRating: "regular_season.passer_rating",
      rushingAttempts: "regular_season.rushing_attempts",
      rushingYards: "regular_season.rushing_yards",
      rushingTouchdowns: "regular_season.rushing_touchdowns",
      rushingLong: "regular_season.rushing_long",
      receptions: "regular_season.receptions",
      receivingYards: "regular_season.receiving_yards",
      receivingTouchdowns: "regular_season.receiving_touchdowns",
      receivingLong: "regular_season.receiving_long",
      tackles: "regular_season.tackles",
      sacks: "regular_season.sacks",
      fumbles: "regular_season.fumbles",
      fumblesLost: "regular_season.fumbles_lost",
      kickReturns: "regular_season.kick_returns",
      kickReturnYards: "regular_season.kick_return_yards",
      kickReturnTouchdowns: "regular_season.kick_return_touchdowns",
      puntReturns: "regular_season.punt_returns",
      puntReturnYards: "regular_season.punt_return_yards",
      puntReturnTouchdowns: "regular_season.punt_return_touchdowns",
      twoPointConversionPassAttempts: "regular_season.two_point_conversion_pass_attempts",
      twoPointConversionRushAttempts: "regular_season.two_point_conversion_rush_attempts",
      twoPointConversionRushSucceeded: "regular_season.two_point_conversion_rush_succeeded"
    };
    NCAAFB_LIVE_TOP = {
      week: "week",
      sport: "sport",
      season: "season",
      status: "status",
      gameId: "game_ID",
      gameTime: "game_time",
      eventName: "event_name",
      gameStatus: "game_status",
      seasonType: "season_type",
      gameLocation: "game_location",
      awayTeamName: "away_team_name",
      homeTeamName: "home_team_name"
    };
    NCAAFB_LIVE_CURRENT = {
      down: "full_box.current.Down",
      possessionAbbr: "full_box.current.poss",
      field: "full_box.current.field",
      quarter: "full_box.current.Quarter",
      redZone: "full_box.current.RedZone",
      distance: "full_box.current.Distance",
      yardLine: "full_box.current.YardLine",
      possession: "full_box.current.Possession",
      timeRemaining: "full_box.current.TimeRemaining",
      yardLineTerritory: "full_box.current.YardLineTerritory"
    };
    NCAAFB_LIVE_TEAM_SHELL = {
      homeScore: "full_box.home_team.score",
      homeAbbrv: "full_box.home_team.abbrv",
      homeMascot: "full_box.home_team.mascot",
      homeRecord: "full_box.home_team.record",
      homeTeamId: "full_box.home_team.team_id",
      homeDivisionName: "full_box.home_team.division_name",
      homeQuarterScores: "full_box.home_team.quarter_scores",
      awayScore: "full_box.away_team.score",
      awayAbbrv: "full_box.away_team.abbrv",
      awayMascot: "full_box.away_team.mascot",
      awayRecord: "full_box.away_team.record",
      awayTeamId: "full_box.away_team.team_id",
      awayDivisionName: "full_box.away_team.division_name",
      awayQuarterScores: "full_box.away_team.quarter_scores"
    };
    NCAAFB_LIVE_TEAM_STATS = {
      sacks: "team_stats.sacks",
      safeties: "team_stats.safeties",
      penaltiesTotal: "team_stats.penalties.total",
      penaltiesYards: "team_stats.penalties.yards",
      turnovers: "team_stats.turnovers",
      firstDowns: "team_stats.first_downs",
      totalYards: "team_stats.total_yards",
      blockedKicks: "team_stats.blocked_kicks",
      blockedPunts: "team_stats.blocked_punts",
      kicksBlocked: "team_stats.kicks_blocked",
      passingYards: "team_stats.passing_yards",
      puntsBlocked: "team_stats.punts_blocked",
      rushingYards: "team_stats.rushing_yards",
      defenseTouchdowns: "team_stats.defense_touchdowns",
      defenseInterceptions: "team_stats.defense_interceptions",
      kickReturnTouchdowns: "team_stats.kick_return_touchdowns",
      puntReturnTouchdowns: "team_stats.punt_return_touchdowns",
      blockedKickTouchdowns: "team_stats.blocked_kick_touchdowns",
      blockedPuntTouchdowns: "team_stats.blocked_punt_touchdowns",
      interceptionTouchdowns: "team_stats.interception_touchdowns",
      fumbleReturnTouchdowns: "team_stats.fumble_return_touchdowns",
      defenseFumbleRecoveries: "team_stats.defense_fumble_recoveries",
      fieldGoalReturnTouchdowns: "team_stats.field_goal_return_touchdowns",
      twoPointConversionReturns: "team_stats.two_point_conversion_returns",
      twoPointConversionAttempts: "team_stats.two_point_conversion_attempts",
      twoPointConversionSucceeded: "team_stats.two_point_conversion_succeeded",
      pointsAgainstDefenseSpecialTeams: "team_stats.points_against_defense_special_teams"
    };
    NCAAFB_LIVE_PLAYER_BOX = {
      fullName: "player",
      status: "status",
      position: "position",
      positionCategory: "position_category",
      completions: "completions",
      passingAttempts: "passing_attempts",
      passingYards: "passing_yards",
      passingTouchdowns: "passing_touchdowns",
      passingInterceptions: "passing_interceptions",
      rushingAttempts: "rushing_attempts",
      rushingYards: "rushing_yards",
      rushingTouchdowns: "rushing_touchdowns",
      rushingLong: "rushing_long",
      receptions: "receptions",
      receivingYards: "receiving_yards",
      receivingTouchdowns: "receiving_touchdowns",
      receivingLong: "receiving_long",
      tackles: "tackles",
      sacks: "sacks",
      fumblesRecoveries: "fumbles_recoveries",
      fieldGoalsAttempted: "field_goals_attempted",
      fieldGoalsMade: "field_goals_made",
      fieldGoalsLong: "field_goals_long",
      fieldGoalDistances: "field_goal_distances",
      extraPointsAttempted: "extra_points_attempted",
      extraPointsMade: "extra_points_made",
      punts: "punts",
      inside20: "inside_20",
      puntsLong: "punts_long",
      puntingYards: "punting_yards",
      kickReturns: "kick_returns",
      kickReturnYards: "kick_return_yards",
      kickReturnLong: "kick_return_long",
      kickReturnTouchdowns: "kick_return_touchdowns",
      puntReturns: "punt_returns",
      puntReturnYards: "punt_return_yards",
      puntReturnLong: "punt_return_long",
      puntReturnTouchdowns: "punt_return_touchdowns"
    };
    NCAAFB_SCHEDULE = {
      awayTeam: "away_team",
      homeTeam: "home_team",
      awayTeamId: "away_team_ID",
      homeTeamId: "home_team_ID",
      gameId: "game_ID",
      gameTime: "game_time",
      seasonType: "season_type",
      week: "week",
      eventName: "event_name",
      season: "season",
      status: "status",
      arena: "arena",
      city: "city",
      state: "state",
      country: "country",
      latitude: "latitude",
      longitude: "longitude",
      field: "field",
      dome: "dome",
      postalCode: "postal_code"
    };
    NCAAFB_TEAM_INFO = {
      teamId: "team_id",
      teamName: "team",
      abbreviation: "abbrv",
      mascot: "mascot",
      rank: "rank",
      week: "week",
      conferenceId: "conf_ID",
      conference: "conf",
      city: "city",
      state: "state",
      arena: "arena",
      country: "country",
      latitude: "latitude",
      longitude: "longitude",
      field: "field",
      postalCode: "postal_code",
      dome: "dome"
    };
    NCAAFB_TEAM_SEASON_STATS = {
      sacks: "regular_season.sacks",
      points: "regular_season.points",
      safeties: "regular_season.safeties",
      gamesPlayed: "regular_season.games_played",
      penalties: "regular_season.penalties",
      penaltyYards: "regular_season.penalty_yards",
      turnovers: "regular_season.turnovers",
      firstDowns: "regular_season.first_downs",
      totalYards: "regular_season.total_yards",
      blockedKicks: "regular_season.blocked_kicks",
      blockedPunts: "regular_season.blocked_punts",
      kicksBlocked: "regular_season.kicks_blocked",
      puntsBlocked: "regular_season.punts_blocked",
      passingYards: "regular_season.passing_yards",
      rushingYards: "regular_season.rushing_yards",
      defenseTouchdowns: "regular_season.defense_touchdowns",
      defenseInterceptions: "regular_season.defense_interceptions",
      kickReturnTouchdowns: "regular_season.kick_return_touchdowns",
      puntReturnTouchdowns: "regular_season.punt_return_touchdowns",
      blockedKickTouchdowns: "regular_season.blocked_kick_touchdowns",
      blockedPuntTouchdowns: "regular_season.blocked_punt_touchdowns",
      interceptionTouchdowns: "regular_season.interception_touchdowns",
      fumbleReturnTouchdowns: "regular_season.fumble_return_touchdowns",
      defenseFumbleRecoveries: "regular_season.defense_fumble_recoveries",
      fieldGoalReturnTouchdowns: "regular_season.field_goal_return_touchdowns",
      twoPointConversionReturns: "regular_season.two_point_conversion_returns",
      twoPointConversionAttempts: "regular_season.two_point_conversion_attempts",
      twoPointConversionSucceeded: "regular_season.two_point_conversion_succeeded",
      pointsAgainstDefenseSpecialTeams: "regular_season.points_against_defense_special_teams"
    };
    SOCCER_PROFILE = {
      providerPlayerId: "player_id",
      fullName: "player",
      teamRaw: "team",
      providerTeamId: "team_id",
      jerseyNumber: "number",
      status: "status",
      position: "position",
      height: "height",
      weight: "weight",
      birthDateRaw: "age"
    };
    SOCCER_SCHEDULE = {
      awayTeam: "away_team",
      homeTeam: "home_team",
      awayTeamId: "away_team_ID",
      homeTeamId: "home_team_ID",
      gameId: "game_ID",
      gameTime: "game_time",
      seasonType: "season_type",
      season: "season",
      status: "status",
      city: "city",
      country: "country",
      postalCode: "postal_code",
      arena: "arena",
      field: "field",
      dome: "dome",
      latitude: "latitude",
      longitude: "longitude"
    };
    SOCCER_TEAM_INFO = {
      teamId: "team_id",
      teamName: "team",
      abbreviation: "abbrv",
      league: "league",
      city: "city",
      arena: "arena",
      country: "country",
      latitude: "latitude",
      longitude: "longitude",
      field: "field",
      postalCode: "postal_code",
      dome: "dome"
    };
    SOCCER_LIVE_TOP = {
      sport: "sport",
      gameStatus: "game_status",
      status: "status",
      awayTeamName: "away_team_name",
      homeTeamName: "home_team_name",
      gameId: "game_ID",
      gameTime: "game_time"
    };
    SOCCER_LIVE_TEAM_SHELL = {
      homeQuarterScores: "full_box.home_team.quarter_scores",
      homeScore: "full_box.home_team.score",
      awayQuarterScores: "full_box.away_team.quarter_scores",
      awayScore: "full_box.away_team.score"
    };
    SOCCER_LIVE_TEAM_STATS = {
      foulsCommitted: "team_stats.fouls_committed",
      foulsDrawn: "team_stats.fouls_drawn",
      saves: "team_stats.saves",
      offsides: "team_stats.offsides",
      redCards: "team_stats.red_cards",
      yellowCards: "team_stats.yellow_cards",
      corners: "team_stats.corners",
      shotsOnGoal: "team_stats.shots_on_goal",
      shotsAttempted: "team_stats.shots_attempted",
      freeKicksWon: "team_stats.free_kicks_won",
      freeKicksConceded: "team_stats.free_kicks_conceded",
      penaltyAttempts: "team_stats.penalty_attempts",
      penaltiesScored: "team_stats.penalties_scored",
      penaltiesFaced: "team_stats.penalties_faced",
      penaltiesConceded: "team_stats.penalties_conceded",
      cleanSheets: "team_stats.clean_sheets"
    };
    SOCCER_LIVE_PLAYER = {
      fullName: "player",
      position: "position",
      foulsCommitted: "fouls_committed",
      foulsDrawn: "fouls_drawn",
      minutesPlayed: "minutes_played",
      redCards: "red_cards",
      yellowCards: "yellow_cards",
      shotsOnGoal: "shots_on_goal",
      shotsAttempted: "shots_attempted",
      goals: "goals",
      assists: "assists",
      freeKicksWon: "free_kicks_won",
      penaltyAttempts: "penalty_attempts",
      penaltiesScored: "penalties_scored"
    };
    SOCCER_LIVE_GOALKEEPER = {
      goalsConceded: "goals_conceded",
      saves: "saves",
      cleanSheets: "clean_sheets",
      penaltiesSaved: "penalties_saved",
      penaltiesFaced: "penalties_faced"
    };
    SOCCER_TEAM_SEASON_STATS = {
      relegated: "relegated",
      drawsFromVendor: "regular_season.draws",
      tiesRaw: "regular_season.ties",
      wins: "regular_season.wins",
      saves: "regular_season.saves",
      losses: "regular_season.losses",
      corners: "regular_season.corners",
      offsides: "regular_season.offsides",
      redCards: "regular_season.red_cards",
      foulsDrawn: "regular_season.fouls_drawn",
      cleanSheets: "regular_season.clean_sheets",
      gamesPlayed: "regular_season.games_played",
      goalsScored: "regular_season.goals_scored",
      yellowCards: "regular_season.yellow_cards",
      shotsOnGoal: "regular_season.shots_on_goal",
      freeKicksWon: "regular_season.free_kicks_won",
      goalsConceded: "regular_season.goals_conceded",
      foulsCommitted: "regular_season.fouls_committed",
      penaltiesFaced: "regular_season.penalties_faced",
      shotsAttempted: "regular_season.shots_attempted",
      penaltiesScored: "regular_season.penalties_scored",
      penaltyAttempts: "regular_season.penalty_attempts",
      penaltiesConceded: "regular_season.penalties_conceded",
      freeKicksConceded: "regular_season.free_kicks_conceded"
    };
    ROLLING_INSIGHTS_FIELD_MAPS = {
      NFL: {
        profile: ROLLING_INSIGHTS_NFL_PROFILE_FIELD_MAP,
        player_stats: ROLLING_INSIGHTS_NFL_PLAYER_STATS_FIELD_MAP,
        live: ROLLING_INSIGHTS_NFL_LIVE_FIELD_MAP,
        injury: ROLLING_INSIGHTS_NFL_INJURY_FIELD_MAP,
        depth_chart: ROLLING_INSIGHTS_NFL_DEPTH_CHART_FIELD_MAP,
        team: ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP,
        schedule: ROLLING_INSIGHTS_NFL_SCHEDULE_FIELD_MAP
      },
      NBA: {
        live: { ...NBA_LIVE_AND_BOX, ...NBA_LIVE_SHELL },
        schedule: NBA_SCHEDULE,
        team: ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP
      },
      MLB: {
        live_batting: MLB_BATTING,
        live_pitching: MLB_PITCHING,
        schedule: {
          ...ROLLING_INSIGHTS_NFL_SCHEDULE_FIELD_MAP,
          awayPitcherPlayerId: "away_pitcher.player_id",
          homePitcherPlayerId: "home_pitcher.player_id"
        },
        team: ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP
      },
      NHL: {
        live_skaters: NHL_SKATERS,
        live_goalies: NHL_GOALIES,
        schedule: ROLLING_INSIGHTS_NFL_SCHEDULE_FIELD_MAP,
        team: ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP
      },
      NCAABB: {
        profile: NCAABB_PROFILE,
        live: { ...NBA_LIVE_AND_BOX, ...NBA_LIVE_SHELL, starter: "starter" },
        schedule: NCAABB_SCHEDULE_EXTRA,
        team: ROLLING_INSIGHTS_NFL_TEAM_FIELD_MAP
      },
      NCAAFB: {
        profile: NCAAFB_PROFILE,
        player_stats: NCAAFB_STATS,
        schedule: NCAAFB_SCHEDULE,
        team: NCAAFB_TEAM_INFO,
        ncaaf_live: NCAAFB_LIVE_TOP,
        ncaaf_live_current: NCAAFB_LIVE_CURRENT,
        ncaaf_live_team: NCAAFB_LIVE_TEAM_SHELL,
        ncaaf_live_team_stats: NCAAFB_LIVE_TEAM_STATS,
        ncaaf_live_player_box: NCAAFB_LIVE_PLAYER_BOX,
        ncaaf_team_season_stats: NCAAFB_TEAM_SEASON_STATS
      },
      SOCCER: {
        profile: SOCCER_PROFILE,
        schedule: SOCCER_SCHEDULE,
        team: SOCCER_TEAM_INFO,
        soccer_live: SOCCER_LIVE_TOP,
        soccer_live_team_shell: SOCCER_LIVE_TEAM_SHELL,
        soccer_live_team_stats: SOCCER_LIVE_TEAM_STATS,
        soccer_live_player: SOCCER_LIVE_PLAYER,
        soccer_live_goalkeeper: SOCCER_LIVE_GOALKEEPER,
        soccer_team_season_stats: SOCCER_TEAM_SEASON_STATS
      }
    };
  }
});

// lib/sports-data/rollingInsightsSupport.ts
function riSupports(endpoint, sport) {
  return SUPPORT[endpoint]?.[getRollingInsightsSportCode(sport)] === true;
}
function riSeasonArg(input, now = /* @__PURE__ */ new Date()) {
  if (input != null) {
    const s = String(input).trim();
    if (/^\d{4}/.test(s)) return s.slice(0, 4);
  }
  const y = now.getUTCFullYear();
  return String(now.getUTCMonth() + 1 >= 8 ? y : y - 1);
}
function resolveRiEnvelope(payload, opts) {
  if (payload == null) return [];
  if (Array.isArray(payload)) return payload;
  const root = payload;
  const data = root.data ?? root;
  if (Array.isArray(data)) return data;
  if (!data || typeof data !== "object") return [];
  const bag = data;
  const code = getRollingInsightsSportCode(opts.sport);
  const keys = [];
  if (code === "SOCCER") {
    const league = normalizeSoccerLeague(opts.league ?? "");
    if (league) keys.push(league);
    keys.push("EPL", "LALIGA", "SERIEA");
  }
  keys.push(code);
  for (const key of keys) {
    const v = bag[key];
    if (Array.isArray(v)) return v;
    if (v && typeof v === "object") return Object.values(v);
  }
  for (const v of Object.values(bag)) {
    if (Array.isArray(v)) return v;
  }
  return [];
}
var SUPPORT, RI_SOCCER_LEAGUES;
var init_rollingInsightsSupport = __esm({
  "lib/sports-data/rollingInsightsSupport.ts"() {
    "use strict";
    init_rollingInsightsFieldMaps();
    init_rollingInsightsSoccerLeague();
    SUPPORT = {
      schedule: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      schedule_week: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      schedule_season: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      live: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      play_by_play: { NFL: true, NBA: true, MLB: true, NHL: false, NCAABB: false, NCAAFB: false, SOCCER: false },
      team_info: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      team_stats: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      player_info: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: true },
      /** SOCCER has no player season stats — see `support_consequences` in the contract. */
      player_stats: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: true, NCAAFB: true, SOCCER: false },
      /** College has no injuries feed at all, and neither does soccer. */
      injuries: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: false, NCAAFB: false, SOCCER: false },
      depth_charts: { NFL: true, NBA: true, MLB: true, NHL: true, NCAABB: false, NCAAFB: false, SOCCER: false }
    };
    RI_SOCCER_LEAGUES = ["EPL", "LALIGA", "SERIEA"];
  }
});

// lib/workers/providers/rollingInsightsRest.ts
function riRestBase() {
  const raw = process.env.ROLLING_INSIGHTS_REST_BASE_URL?.trim() || process.env.ROLLING_INSIGHTS_REST_BASE?.trim() || process.env.ROLLING_INSIGHTS_BASE_URL?.trim() || process.env.ROLLING_INSIGHTS_API_BASE?.trim() || // db-first-exception: provider ingestion transport — the vendor base URL from
  // contracts/rolling-insights/ENDPOINTS.yaml meta.base_url, used only when every env spelling
  // above is unset. This module fetches and hands rows to ingestion writers; no read path
  // reaches it (see the caller census in the module header).
  "https://rest.datafeeds.rolling-insights.com/api/v1";
  const first = raw.split(",")[0].trim().replace(/\/+$/, "");
  return /\/api\/v\d+$/.test(first) ? first : `${first}/api/v1`;
}
function riCredentialsFor(sport) {
  const primary = process.env.ROLLING_INSIGHTS_RSC_TOKEN?.trim() || process.env.RSC_TOKEN?.trim() || process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim() || process.env.ROLLING_INSIGHTS_API_KEY?.trim() || process.env.ROLLING_INSIGHTS_KEY?.trim() || null;
  const secondary = process.env.ROLLING_INSIGHTS_RSC_TOKEN2?.trim() || process.env.ROLLING_INSIGHTS_CLIENT_SECRET2?.trim() || null;
  const primarySports = (process.env.ROLLING_INSIGHTS_PRIMARY_SPORTS?.trim() || "NFL").split(",").map((s) => getRollingInsightsSportCode(s.trim())).filter(Boolean);
  const prefersPrimary = primarySports.includes(getRollingInsightsSportCode(sport));
  const ordered = prefersPrimary ? [primary ? { token: primary, label: "primary" } : null, secondary ? { token: secondary, label: "secondary" } : null] : [secondary ? { token: secondary, label: "secondary" } : null, primary ? { token: primary, label: "primary" } : null];
  const seen = /* @__PURE__ */ new Set();
  return ordered.filter((c) => {
    if (!c || seen.has(c.token)) return false;
    seen.add(c.token);
    return true;
  });
}
function todayUtc(now = /* @__PURE__ */ new Date()) {
  return now.toISOString().slice(0, 10);
}
function buildPath(endpoint, req, code) {
  const season = riSeasonArg(req.season);
  const date = req.date?.trim() || todayUtc();
  switch (endpoint) {
    case "schedule":
      return `/schedule/${date}/${code}`;
    case "schedule_week":
      return `/schedule-week/${date}/${code}`;
    case "schedule_season":
      return `/schedule-season/${season}/${code}`;
    case "live":
      return `/live/${date}/${code}`;
    case "play_by_play":
      return `/play-by-play/${code}`;
    case "team_info":
      return `/team-info/${code}`;
    case "team_stats":
      return `/team-stats/${season}/${code}`;
    case "player_info":
      return `/player-info/${code}`;
    case "player_stats":
      return `/player-stats/${season}/${code}`;
    case "injuries":
      return `/injuries/${code}`;
    case "depth_charts":
      return `/depth-charts/${code}`;
    default:
      return null;
  }
}
async function riFetch(endpoint, req) {
  const code = getRollingInsightsSportCode(req.sport);
  if (!riSupports(endpoint, req.sport)) {
    return {
      ok: false,
      kind: "unsupported",
      error: `Rolling Insights does not document ${endpoint} for ${code}`
    };
  }
  const credentials = riCredentialsFor(req.sport);
  if (credentials.length === 0) {
    return { ok: false, kind: "no_token", error: "Rolling Insights RSC token not configured" };
  }
  let league = null;
  if (code === "SOCCER") {
    league = normalizeSoccerLeague(req.league ?? "");
    if (!league) {
      return {
        ok: false,
        kind: "bad_request",
        error: "SOCCER requires league (EPL | LALIGA | SERIEA) \u2014 soccer_league_param_required"
      };
    }
  }
  if (endpoint === "play_by_play" && !req.gameId?.trim()) {
    return { ok: false, kind: "bad_request", error: "play_by_play requires game_id" };
  }
  const path = buildPath(endpoint, req, code);
  if (!path) return { ok: false, kind: "bad_request", error: `no path for ${endpoint}` };
  const doFetch = req.fetchImpl ?? fetch;
  const now = req.now ?? Date.now;
  const timeoutMs = req.timeoutMs ?? DEFAULT_TIMEOUT_MS2;
  const attempt = async (credential) => {
    const url = new URL(`${riRestBase()}${path}`);
    url.searchParams.set("RSC_token", credential.token);
    if (league) url.searchParams.set("league", league);
    if (req.teamId) url.searchParams.set("team_id", req.teamId);
    if (req.playerId) url.searchParams.set("player_id", req.playerId);
    if (req.gameId) url.searchParams.set("game_id", req.gameId);
    url.searchParams.set("_", String(now()));
    try {
      const res = await doFetch(url.toString(), {
        headers: {
          Accept: "application/json",
          "Cache-Control": "no-cache, no-store",
          Pragma: "no-cache"
        },
        cache: "no-store",
        signal: AbortSignal.timeout(timeoutMs)
      });
      if (res.status === 304) {
        return { ok: false, kind: "not_modified", error: `HTTP 304 ${endpoint}/${code}` };
      }
      if (!res.ok) {
        return { ok: false, kind: "http", error: `HTTP ${res.status} ${endpoint}/${code}` };
      }
      return { ok: true, payload: await res.json(), status: res.status, credential: credential.label };
    } catch (e) {
      return { ok: false, kind: "network", error: redactSecrets(e instanceof Error ? e.message : String(e)) };
    }
  };
  let lastNotModified = null;
  for (const credential of credentials) {
    const first = await attempt(credential);
    if (first.ok) return first;
    if (first.kind !== "not_modified") {
      if (credentials.length === 1) return first;
      lastNotModified = lastNotModified ?? first;
      continue;
    }
    const second = await attempt(credential);
    if (second.ok) return second;
    if (second.ok === false && second.kind !== "not_modified") {
      lastNotModified = lastNotModified ?? second;
      continue;
    }
    lastNotModified = {
      ok: false,
      kind: "not_modified",
      error: `HTTP 304 after cache-busted retry ${endpoint}/${code} \u2014 treat as UNCHANGED, not empty`
    };
  }
  return lastNotModified ?? {
    ok: false,
    kind: "not_modified",
    error: `HTTP 304 on every configured credential ${endpoint}/${code} \u2014 UNCHANGED or not subscribed`
  };
}
async function riFetchRows(endpoint, req) {
  const out = await riFetch(endpoint, req);
  if (out.ok) {
    return {
      rows: resolveRiEnvelope(out.payload, { sport: req.sport, league: req.league }),
      notModified: false,
      unsupported: false,
      error: null,
      credential: out.credential
    };
  }
  return {
    rows: [],
    notModified: out.kind === "not_modified",
    unsupported: out.kind === "unsupported",
    error: redactSecrets(out.error)
  };
}
var import_server_only12, DEFAULT_TIMEOUT_MS2;
var init_rollingInsightsRest = __esm({
  "lib/workers/providers/rollingInsightsRest.ts"() {
    "use strict";
    import_server_only12 = __toESM(require_audit_server_only_shim());
    init_redactSecrets();
    init_rollingInsightsFieldMaps();
    init_rollingInsightsSoccerLeague();
    init_rollingInsightsSupport();
    DEFAULT_TIMEOUT_MS2 = 3e4;
  }
});

// lib/sports-data/riSeasonSchedule.ts
var riSeasonSchedule_exports = {};
__export(riSeasonSchedule_exports, {
  currentScheduleSeason: () => currentScheduleSeason,
  parseRiScheduleSeason: () => parseRiScheduleSeason,
  readRiScheduleWindow: () => readRiScheduleWindow,
  scheduleKeyPrefix: () => scheduleKeyPrefix,
  syncRiSeasonSchedule: () => syncRiSeasonSchedule
});
function parseRiScheduleSeason(rows) {
  const out = [];
  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    const g4 = row;
    const gameId = str(g4.game_ID ?? g4.game_id);
    if (!gameId) continue;
    const time = str(g4.game_time);
    const parsed = time ? new Date(time) : null;
    const instant = parsed && !Number.isNaN(parsed.getTime()) ? parsed : null;
    const day = gameDayFromRiGameId(gameId) ?? easternCalendarDay(instant);
    if (!day) continue;
    out.push({
      gameId,
      day: isoDay(day),
      startTime: instant ? instant.toISOString() : null,
      status: str(g4.status),
      seasonType: classifyRiSeasonType(g4.season_type),
      eventName: str(g4.event_name),
      replacedBy: str(g4.replaced_by)
    });
  }
  return out;
}
function scheduleKeyPrefix(sport, season) {
  return `${sport.toUpperCase()}:rischedule:${season}:`;
}
async function syncRiSeasonSchedule(opts) {
  const sport = opts.sport.toUpperCase();
  const now = opts.now ?? /* @__PURE__ */ new Date();
  const result = {
    sport,
    season: opts.season,
    fetched: false,
    notModified: false,
    unsupported: false,
    error: null,
    games: 0,
    days: 0,
    staleDaysRemoved: 0,
    statusCounts: {}
  };
  const { rows, notModified, unsupported: unsupported2, error } = await riFetchRows("schedule_season", {
    sport,
    season: opts.season,
    fetchImpl: opts.fetchImpl
  });
  if (notModified || unsupported2 || error) {
    result.notModified = notModified;
    result.unsupported = unsupported2;
    result.error = error;
    return result;
  }
  const games = parseRiScheduleSeason(rows);
  if (games.length === 0) {
    result.error = "schedule-season returned 200 with no parseable games \u2014 refusing to replace a stored schedule with nothing";
    return result;
  }
  result.fetched = true;
  result.games = games.length;
  const byDay = /* @__PURE__ */ new Map();
  for (const g4 of games) {
    byDay.set(g4.day, [...byDay.get(g4.day) ?? [], g4]);
    const k = g4.status ?? "(none)";
    result.statusCounts[k] = (result.statusCounts[k] ?? 0) + 1;
  }
  result.days = byDay.size;
  const prefix = scheduleKeyPrefix(sport, opts.season);
  const expiresAt = new Date(now.getTime() + KEEP_MS);
  const put = (cacheKey, data) => opts.db.sportsDataCache.upsert({
    where: { cacheKey },
    create: { cacheKey, data, expiresAt },
    update: { data, expiresAt }
  });
  for (const [day, list] of byDay) await put(`${prefix}${day}`, { games: list });
  const keep = [...byDay.keys()].map((d) => `${prefix}${d}`);
  const removed = await opts.db.sportsDataCache.deleteMany({
    where: { cacheKey: { startsWith: prefix, notIn: [...keep, `${prefix}meta`] } }
  });
  result.staleDaysRemoved = removed.count;
  const daysSorted = [...byDay.keys()].sort();
  await put(`${prefix}meta`, {
    syncedAt: now.toISOString(),
    games: games.length,
    firstDay: daysSorted[0],
    lastDay: daysSorted[daysSorted.length - 1],
    statusCounts: result.statusCounts
  });
  return result;
}
async function readRiScheduleWindow(db2, args) {
  const prefix = scheduleKeyPrefix(args.sport, args.season);
  const meta = await db2.sportsDataCache.findUnique({ where: { cacheKey: `${prefix}meta` } });
  if (!meta) return null;
  const keys = [];
  for (let t = args.window.start.getTime(); t < args.window.end.getTime(); t += 864e5) {
    keys.push(`${prefix}${isoDay(new Date(t))}`);
  }
  const rows = await db2.sportsDataCache.findMany({ where: { cacheKey: { in: keys } }, select: { cacheKey: true, data: true } });
  return rows.flatMap((r) => {
    const games = r.data?.games;
    return Array.isArray(games) ? games : [];
  });
}
function currentScheduleSeason(now = /* @__PURE__ */ new Date()) {
  return now.getUTCMonth() >= 7 ? now.getUTCFullYear() : now.getUTCFullYear() - 1;
}
var str, isoDay, KEEP_MS;
var init_riSeasonSchedule = __esm({
  "lib/sports-data/riSeasonSchedule.ts"() {
    "use strict";
    init_rollingInsightsRest();
    init_riSeasonType();
    init_easternGameDay();
    str = (v) => {
      if (v == null) return null;
      const t = String(v).trim();
      return t && t.toLowerCase() !== "null" ? t : null;
    };
    isoDay = (d) => d.toISOString().slice(0, 10);
    KEEP_MS = 400 * 864e5;
  }
});

// lib/season-week/standardSeasonScope.ts
function seasonSportToLeagueSport(sport) {
  const u = String(sport ?? "").trim().toUpperCase();
  if (u === "NCAAFB") return "NCAAF";
  if (u === "NCAABB") return "NCAAB";
  return u;
}
var init_standardSeasonScope = __esm({
  "lib/season-week/standardSeasonScope.ts"() {
    "use strict";
    init_sport_scope();
  }
});

// lib/jobs/types.ts
var QUEUE_NAMES;
var init_types5 = __esm({
  "lib/jobs/types.ts"() {
    "use strict";
    QUEUE_NAMES = {
      AI: "ai",
      NOTIFICATIONS: "notifications",
      SIMULATIONS: "simulations",
      DEVY: "devy",
      INTEGRITY: "integrity",
      AUTOCOACH_STATUS: "autocoach_status",
      /** Heavy league-engine work: waivers, scoring batches, automation, import resync (BullMQ). */
      LEAGUE_ENGINE: "league_engine"
    };
  }
});

// lib/queues/bullmq.ts
function isBuildTimeRuntimeDisabled() {
  return process.env.NEXT_PHASE === import_constants2.PHASE_PRODUCTION_BUILD;
}
function parseRedisPort(value) {
  if (!value?.trim()) return null;
  const port = Number(value.trim());
  return Number.isInteger(port) && port > 0 ? port : null;
}
function isPlausibleRedisUrl(value) {
  const v = value.trim();
  if (!v) return false;
  if (v.startsWith("/") && !v.startsWith("//")) return false;
  if (!/^rediss?:\/\//i.test(v)) return false;
  try {
    const u = new URL(v);
    return Boolean(u.hostname?.length);
  } catch {
    return false;
  }
}
function isPlausibleRedisHost(host) {
  const h = host.trim();
  if (!h || h === "/" || h === ".") return false;
  return true;
}
function getRedisUrl() {
  const value = process.env.REDIS_URL?.trim();
  if (!value) return null;
  if (!isPlausibleRedisUrl(value)) {
    console.warn(
      "[bullmq] REDIS_URL is set but not a valid redis(s):// URL with hostname; Redis disabled. Remove it or set a TCP URL (e.g. Upstash rediss://\u2026)."
    );
    return null;
  }
  return value;
}
function getRedisHost() {
  const value = process.env.REDIS_HOST?.trim();
  if (!value || !isPlausibleRedisHost(value)) return null;
  return value;
}
function getRedisPort() {
  return parseRedisPort(process.env.REDIS_PORT);
}
function getRedisOptions() {
  return {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    lazyConnect: true,
    connectTimeout: 8e3,
    retryStrategy: (times) => times > 2 ? null : Math.min(times * 200, 2e3)
  };
}
function isRedisConfigured() {
  if (isBuildTimeRuntimeDisabled()) {
    return false;
  }
  return Boolean(getRedisUrl() || getRedisHost() && getRedisPort());
}
function getRedisConnection() {
  if (isBuildTimeRuntimeDisabled()) {
    return null;
  }
  const redisUrl = getRedisUrl();
  if (redisUrl) {
    return {
      url: redisUrl,
      maxRetriesPerRequest: null,
      enableReadyCheck: false
    };
  }
  const host = getRedisHost();
  const port = getRedisPort();
  if (host && port) {
    return {
      host,
      port,
      maxRetriesPerRequest: null,
      enableReadyCheck: false
    };
  }
  return null;
}
function getRedisClient() {
  if (redisClient !== void 0) {
    return redisClient;
  }
  if (isBuildTimeRuntimeDisabled()) {
    redisClient = null;
    return redisClient;
  }
  const redisUrl = getRedisUrl();
  const host = getRedisHost();
  const port = getRedisPort();
  const options = getRedisOptions();
  if (redisUrl) {
    redisClient = new import_ioredis.default(redisUrl, options);
    redisClient.on("error", (err) => {
      console.warn("[redis]", err instanceof Error ? err.message : err);
    });
    return redisClient;
  }
  if (host && port) {
    redisClient = new import_ioredis.default(port, host, options);
    redisClient.on("error", (err) => {
      console.warn("[redis]", err instanceof Error ? err.message : err);
    });
    return redisClient;
  }
  redisClient = null;
  return redisClient;
}
function getSimulationQueue() {
  if (simulationQueueInstance !== void 0) {
    return simulationQueueInstance;
  }
  const connection = getRedisConnection();
  if (!connection) {
    simulationQueueInstance = null;
    return simulationQueueInstance;
  }
  simulationQueueInstance = new import_bullmq.Queue(QUEUE_NAME_SIMULATIONS, {
    connection
  });
  return simulationQueueInstance;
}
var import_server_only13, import_constants2, import_bullmq, import_ioredis, QUEUE_NAME_SIMULATIONS, redisClient, simulationQueueInstance, redis, simulationQueue;
var init_bullmq = __esm({
  "lib/queues/bullmq.ts"() {
    "use strict";
    import_server_only13 = __toESM(require_audit_server_only_shim());
    import_constants2 = require("next/constants");
    import_bullmq = require("bullmq");
    import_ioredis = __toESM(require("ioredis"));
    init_types5();
    QUEUE_NAME_SIMULATIONS = QUEUE_NAMES.SIMULATIONS;
    redis = getRedisClient();
    simulationQueue = getSimulationQueue();
  }
});

// lib/integrity/enqueueTankingScan.ts
async function enqueueTankingScan(leagueId, weekNumber, seasonId) {
  if (!isRedisConfigured()) return;
  const connection = getRedisConnection();
  if (!connection) return;
  const queue = new import_bullmq2.Queue(QUEUE_NAMES.INTEGRITY, { connection });
  try {
    await queue.add("tanking_scan_week", { type: "tanking_scan_week", leagueId, weekNumber, seasonId }, {
      jobId: `tanking-${leagueId}-${seasonId}-${weekNumber}`,
      attempts: 3,
      backoff: { type: "exponential", delay: 1e4 }
    });
  } finally {
    await queue.close();
  }
}
var import_server_only14, import_bullmq2;
var init_enqueueTankingScan = __esm({
  "lib/integrity/enqueueTankingScan.ts"() {
    "use strict";
    import_server_only14 = __toESM(require_audit_server_only_shim());
    import_bullmq2 = require("bullmq");
    init_types5();
    init_bullmq();
  }
});

// lib/redraft-commissioner/autopilot.ts
function validateAutopilotPolicy(raw) {
  const p = jsonRecord(raw);
  if (Object.keys(p).some((key) => !["correctionGraceHours", "seasonClosePacket", "prizePoolCents", "awards"].includes(key)) || !Number.isInteger(p.correctionGraceHours) || Number(p.correctionGraceHours) < 12 || Number(p.correctionGraceHours) > 72 || typeof p.seasonClosePacket !== "boolean" || !Number.isSafeInteger(p.prizePoolCents) || Number(p.prizePoolCents) < 0 || Number(p.prizePoolCents) > 1e8 || !Array.isArray(p.awards) || p.awards.length > AWARD_BASES.length) throw new Error("Choose a 12\u201372 hour correction wait and a valid prize worksheet (maximum $1,000,000).");
  const used = /* @__PURE__ */ new Set();
  const awards = p.awards.map((rawAward) => {
    const award = jsonRecord(rawAward);
    if (Object.keys(award).some((key) => !["basis", "basisPoints"].includes(key)) || !AWARD_BASES.includes(award.basis) || used.has(String(award.basis)) || !Number.isInteger(award.basisPoints) || Number(award.basisPoints) < 1 || Number(award.basisPoints) > 1e4) throw new Error("Each award needs a distinct supported result and a percentage between 0.01% and 100%.");
    used.add(String(award.basis));
    return { basis: award.basis, basisPoints: Number(award.basisPoints) };
  });
  if (awards.reduce((sum, award) => sum + award.basisPoints, 0) > 1e4) throw new Error("Award percentages cannot exceed 100% of the prize pool.");
  return { correctionGraceHours: Number(p.correctionGraceHours), seasonClosePacket: p.seasonClosePacket, prizePoolCents: Number(p.prizePoolCents), awards };
}
function savedAutopilot(settings, season) {
  const saved = jsonRecord(jsonRecord(settings)[REDRAFT_AUTOPILOT_KEY]);
  if (saved.version !== 1 || saved.season !== season || !Array.isArray(saved.graceSchedule)) return null;
  try {
    validateAutopilotPolicy(saved.policy);
  } catch {
    return null;
  }
  if (saved.graceSchedule.some((raw) => {
    const row = jsonRecord(raw);
    return !Number.isInteger(row.fromWeek) || Number(row.fromWeek) < 1 || !Number.isInteger(row.hours) || Number(row.hours) < 12 || Number(row.hours) > 72;
  })) return null;
  return saved;
}
function finalizationGraceFromSettings(settings, season, week, fallbackMs) {
  const saved = savedAutopilot(settings, season);
  if (!saved) return fallbackMs;
  const applicable = [...saved.graceSchedule].filter((row) => row.fromWeek <= week).sort((a, b) => b.fromWeek - a.fromWeek)[0];
  return applicable ? Math.max(fallbackMs, applicable.hours * 36e5) : fallbackMs;
}
var REDRAFT_AUTOPILOT_KEY, AWARD_BASES;
var init_autopilot = __esm({
  "lib/redraft-commissioner/autopilot.ts"() {
    "use strict";
    init_controls();
    REDRAFT_AUTOPILOT_KEY = "redraftAutopilot";
    AWARD_BASES = ["champion", "runner_up", "regular_season_points"];
  }
});

// lib/redraft/weekFinalizer.ts
function emptyResult(base, refusal, extra = {}) {
  return {
    ...base,
    finalized: false,
    alreadyFinal: false,
    refusal,
    slate: null,
    starters: 0,
    startersWithStats: 0,
    coverage: null,
    rowsFinalized: 0,
    zeroRowsWritten: 0,
    zeroedPlayerIds: [],
    matchupsFinal: 0,
    matchupsConsidered: 0,
    ...extra
  };
}
function isWeekFinalizerDisabled(env = process.env) {
  return String(env.REDRAFT_WEEK_FINALIZER_DISABLED ?? "").trim() === "1";
}
async function readWeekSlate(prisma2, args) {
  const windowed = args.dateWindow ?? null;
  if (windowed && (args.requireCompleteSchedule || RI_SCHEDULE_SLATE_SPORTS.includes(args.sport.toUpperCase()))) {
    const games = await readRiScheduleWindow(prisma2, { sport: args.sport, season: args.season, window: windowed });
    if (games == null) {
      return { games: 0, final: 0, unfinished: 0, cancelled: 0, lastStartTime: null, source: "rolling_insights_schedule (not synced)" };
    }
    const slateGames = games.filter(
      (g4) => args.seasonType === "regular" ? g4.seasonType === "regular" || g4.seasonType == null : g4.seasonType === "post"
    );
    const imported = args.requireCompleteSchedule ? await prisma2.sportsGame.findMany({
      where: { sport: args.sport, season: args.season, externalId: { in: slateGames.map((g4) => g4.gameId) } },
      select: { externalId: true, startTime: true, homeTeam: true, awayTeam: true, status: true }
    }) : null;
    const importedFinals = imported && new Set(imported.filter((g4) => g4.startTime && g4.homeTeam && g4.awayTeam && normalizeGameStatus(g4.status) === "final").map((g4) => g4.externalId));
    let final2 = 0;
    let cancelled2 = 0;
    let unfinished2 = 0;
    let lastStart2 = null;
    for (const g4 of slateGames) {
      const status = normalizeGameStatus(g4.status);
      if (status === "final" && (!importedFinals || importedFinals.has(g4.gameId))) final2 += 1;
      else if (status === "cancelled") cancelled2 += 1;
      else unfinished2 += 1;
      if (g4.startTime && (lastStart2 == null || g4.startTime > lastStart2)) lastStart2 = g4.startTime;
    }
    return { games: slateGames.length, final: final2, unfinished: unfinished2, cancelled: cancelled2, lastStartTime: lastStart2, source: "rolling_insights_schedule" };
  }
  const EASTERN_OVERSELECT_MS = 6 * 60 * 60 * 1e3;
  const rawRows = await prisma2.sportsGame.findMany({
    where: {
      sport: args.sport,
      season: args.season,
      ...windowed ? { startTime: { gte: windowed.start, lt: new Date(windowed.end.getTime() + EASTERN_OVERSELECT_MS) } } : { week: args.week },
      // Only ranked feeds may answer: an unranked one (cfbd donates kickoff times and has no
      // live status at all) would contribute permanent "unfinished" rows.
      source: { in: [...LIVE_SCORE_SOURCES] },
      /*
       * Same discriminator the live provider uses: preseason week 1 and regular week 1
       * share a number, and rows predating the column are overwhelmingly regular season,
       * so NULL joins the regular slate and never the postseason one.
       */
      ...args.seasonType === "regular" ? { OR: [{ seasonType: "regular" }, { seasonType: null }] } : { seasonType: args.seasonType }
    },
    select: { status: true, startTime: true, source: true, fetchedAt: true, season: true, week: true }
  });
  const inWindow = windowed ? rawRows.filter((row) => {
    const day = easternCalendarDay(row.startTime);
    return day != null && day >= windowed.start && day < windowed.end;
  }) : rawRows;
  const selectable = windowed ? inWindow.map((row) => ({ ...row, week: null })) : inWindow;
  const rows = pickFreshestSourceRows(selectable, (args.now ?? /* @__PURE__ */ new Date()).getTime());
  let final = 0;
  let cancelled = 0;
  let unfinished = 0;
  let lastStart = null;
  for (const row of rows) {
    const status = normalizeGameStatus(row.status);
    if (status === "final") final += 1;
    else if (status === "cancelled") cancelled += 1;
    else unfinished += 1;
    if (row.startTime && (lastStart == null || row.startTime > lastStart)) lastStart = row.startTime;
  }
  return {
    games: rows.length,
    final,
    unfinished,
    cancelled,
    lastStartTime: lastStart ? lastStart.toISOString() : null,
    source: rows[0]?.source ?? null
  };
}
async function finalizeRedraftWeek(params, deps = {}) {
  const prisma2 = deps.prisma ?? prisma;
  const now = deps.now?.() ?? /* @__PURE__ */ new Date();
  const recalc = deps.recalculateMatchups ?? recalculateMatchupsForSeasonWeek;
  let graceMs = params.graceMs ?? WEEK_FINALIZE_GRACE_MS;
  const coverageFloor = params.coverageFloor ?? WEEK_FINALIZE_COVERAGE_FLOOR;
  const seasonType = params.seasonType ?? "regular";
  const season = await prisma2.redraftSeason.findFirst({
    where: { id: params.seasonId },
    select: { id: true, leagueId: true, sport: true, season: true, league: { select: { settings: true, platform: true, leagueType: true, leagueVariant: true, isDynasty: true, guillotineMode: true, survivorMode: true, bestBallMode: true } } }
  });
  if (!season) {
    return emptyResult(
      { seasonId: params.seasonId, leagueId: "", sport: "", season: 0, week: params.week },
      "season_not_found"
    );
  }
  if (season.league && isNativePlatform(season.league.platform) && isStandardRedraft(season.league)) {
    graceMs = finalizationGraceFromSettings(season.league.settings, season.season, params.week, graceMs);
  }
  const sport = seasonSportToLeagueSport(season.sport ?? "NFL") || "NFL";
  const base = {
    seasonId: season.id,
    leagueId: season.leagueId,
    sport,
    season: season.season,
    week: params.week
  };
  if (isWeekFinalizerDisabled()) return emptyResult(base, "finalizer_disabled");
  const isDateWindowed = DATE_WINDOWED_SPORTS.includes(sport);
  if (!WEEK_KEYED_SPORTS.includes(sport) && !isDateWindowed) {
    return emptyResult(base, "sport_not_week_keyed");
  }
  let dateWindow = null;
  if (isDateWindowed) {
    const seasonStart = resolveDailySportSeasonStart(sport, season.season);
    const window2 = seasonStart ? weekWindowFromSeasonStart(seasonStart, params.week) : null;
    if (!window2) return emptyResult(base, "season_start_unknown");
    dateWindow = { start: new Date(window2.start), end: new Date(window2.end) };
    const currentDay = easternCalendarDay(now);
    if (!currentDay || currentDay < dateWindow.end) return emptyResult(base, "scoring_period_open");
  }
  const matchups = await prisma2.redraftMatchup.findMany({
    where: { seasonId: season.id, week: params.week },
    select: { id: true, status: true }
  });
  const matchupsConsidered = matchups.length;
  if (matchupsConsidered > 0 && matchups.every((m) => m.status === "final")) {
    return emptyResult(base, null, {
      alreadyFinal: true,
      matchupsConsidered,
      matchupsFinal: matchupsConsidered
    });
  }
  const slate = await readWeekSlate(prisma2, {
    sport,
    season: season.season,
    week: params.week,
    seasonType,
    now,
    dateWindow,
    requireCompleteSchedule: season.league?.guillotineMode === true || season.league?.leagueVariant?.toLowerCase() === "guillotine"
  });
  if (slate.games === 0) return emptyResult(base, "no_games_on_slate", { slate, matchupsConsidered });
  if (slate.unfinished > 0) return emptyResult(base, "games_not_final", { slate, matchupsConsidered });
  const lastStart = slate.lastStartTime ? new Date(slate.lastStartTime) : null;
  if (lastStart && now.getTime() < lastStart.getTime() + graceMs) {
    return emptyResult(base, "within_grace_period", { slate, matchupsConsidered });
  }
  const rosters = await prisma2.redraftRoster.findMany({
    where: { seasonId: season.id },
    select: { id: true }
  });
  const rosterIds = rosters.map((r) => r.id);
  let rosterPlayers = rosterIds.length ? await prisma2.redraftRosterPlayer.findMany({
    where: { rosterId: { in: rosterIds }, droppedAt: null },
    select: { playerId: true, sport: true, slotType: true }
  }) : [];
  const historyLeague = await prisma2.league.findFirst({ where: { id: season.leagueId }, select: { settings: true } });
  let historicalCoverage = null;
  if (usesLineupHistory(historyLeague?.settings)) {
    try {
      const period = await loadHistoricalPeriod(prisma2, {
        leagueId: season.leagueId,
        seasonId: season.id,
        sport,
        season: season.season,
        week: params.week,
        settings: historyLeague?.settings
      });
      rosterPlayers = period.eligible;
      historicalCoverage = {
        starters: period.eligible.length,
        covered: period.eligible.filter((player) => !period.missingPlayerIds.includes(player.playerId)).length
      };
      if (period.missingPlayerIds.length) return emptyResult(base, "stat_coverage_below_floor", {
        slate,
        matchupsConsidered,
        starters: historicalCoverage.starters,
        startersWithStats: historicalCoverage.covered,
        coverage: historicalCoverage.starters ? historicalCoverage.covered / historicalCoverage.starters : 0
      });
      if (!period.correctionsReady) return emptyResult(base, "stat_corrections_pending", { slate, matchupsConsidered });
    } catch {
      return emptyResult(base, "lineup_history_unavailable", { slate, matchupsConsidered });
    }
  }
  const bestBall = leagueIsBestBall(
    await prisma2.league.findFirst({
      where: { id: season.leagueId },
      select: { bestBallMode: true, leagueVariant: true, leagueType: true }
    })
  );
  const starters = /* @__PURE__ */ new Map();
  for (const p of rosterPlayers) {
    if (!countsTowardScore(p.slotType, bestBall)) continue;
    starters.set(`${p.sport}::${p.playerId}`, { playerId: p.playerId, sport: p.sport });
  }
  if (starters.size === 0) {
    return emptyResult(base, "no_starters", { slate, matchupsConsidered });
  }
  const bySport = /* @__PURE__ */ new Map();
  for (const s of starters.values()) {
    const list = bySport.get(s.sport) ?? [];
    list.push(s.playerId);
    bySport.set(s.sport, list);
  }
  const existing = /* @__PURE__ */ new Set();
  for (const [playerSport, playerIds] of bySport) {
    const rows = await prisma2.playerWeeklyScore.findMany({
      where: {
        week: params.week,
        season: season.season,
        sport: playerSport,
        playerId: { in: playerIds }
      },
      select: { playerId: true, sport: true }
    });
    for (const row of rows) existing.add(`${row.sport}::${row.playerId}`);
  }
  const startersWithStats = historicalCoverage ? starters.size : existing.size;
  const coverage = startersWithStats / starters.size;
  if (coverage < coverageFloor) {
    return emptyResult(base, "stat_coverage_below_floor", {
      slate,
      matchupsConsidered,
      starters: starters.size,
      startersWithStats,
      coverage
    });
  }
  const missing = [...starters.values()].filter((s) => !existing.has(`${s.sport}::${s.playerId}`));
  if (params.dryRun) {
    return {
      ...base,
      finalized: false,
      alreadyFinal: false,
      refusal: null,
      slate,
      starters: starters.size,
      startersWithStats,
      coverage,
      rowsFinalized: 0,
      zeroRowsWritten: 0,
      zeroedPlayerIds: missing.map((m) => m.playerId),
      matchupsFinal: 0,
      matchupsConsidered
    };
  }
  let zeroRowsWritten = 0;
  if (missing.length > 0) {
    const created = await prisma2.playerWeeklyScore.createMany({
      data: missing.map((m) => ({
        playerId: m.playerId,
        sport: m.sport,
        week: params.week,
        season: season.season,
        stats: {},
        fantasyPts: 0,
        isFinalized: true
      })),
      skipDuplicates: true
    });
    zeroRowsWritten = created.count;
  }
  let rowsFinalized = 0;
  for (const [playerSport, playerIds] of bySport) {
    const updated = await prisma2.playerWeeklyScore.updateMany({
      where: {
        week: params.week,
        season: season.season,
        sport: playerSport,
        playerId: { in: playerIds },
        isFinalized: false
      },
      data: { isFinalized: true }
    });
    rowsFinalized += updated.count;
  }
  await recalc(season.id, params.week);
  const after = await prisma2.redraftMatchup.findMany({
    where: { seasonId: season.id, week: params.week },
    select: { status: true }
  });
  const matchupsFinal = after.filter((m) => m.status === "final").length;
  if (matchupsFinal > 0 && matchupsFinal === after.length) {
    try {
      await (deps.enqueueTankingScan ?? enqueueTankingScan)(season.leagueId, params.week, season.id);
    } catch (error) {
      console.error("[weekFinalizer] Unable to queue tanking scan", { leagueId: season.leagueId, week: params.week, error });
    }
  }
  return {
    ...base,
    finalized: after.length > 0 && matchupsFinal === after.length,
    alreadyFinal: false,
    refusal: null,
    slate,
    starters: starters.size,
    startersWithStats,
    coverage,
    rowsFinalized,
    zeroRowsWritten,
    zeroedPlayerIds: missing.map((m) => m.playerId),
    matchupsFinal,
    matchupsConsidered: after.length
  };
}
var WEEK_FINALIZE_GRACE_MS, WEEK_FINALIZE_COVERAGE_FLOOR, WEEK_KEYED_SPORTS, DATE_WINDOWED_SPORTS, RI_SCHEDULE_SLATE_SPORTS;
var init_weekFinalizer = __esm({
  "lib/redraft/weekFinalizer.ts"() {
    "use strict";
    init_prisma();
    init_liveSourceSelection();
    init_gameStatus();
    init_riSeasonSchedule();
    init_dailySportStatNormalization();
    init_dailySportSeasonStarts();
    init_easternGameDay();
    init_scoringEngine2();
    init_standardSeasonScope();
    init_enqueueTankingScan();
    init_lineupHistory();
    init_autopilot();
    init_controls();
    init_isNativeLeague();
    WEEK_FINALIZE_GRACE_MS = 12 * 60 * 60 * 1e3;
    WEEK_FINALIZE_COVERAGE_FLOOR = 0.8;
    WEEK_KEYED_SPORTS = ["NFL", "NCAAF"];
    DATE_WINDOWED_SPORTS = ["NBA", "NHL", "MLB", "NCAAB"];
    RI_SCHEDULE_SLATE_SPORTS = ["NCAAB"];
  }
});

// lib/automation/locks.ts
var locks_exports = {};
__export(locks_exports, {
  acquireAutomationLock: () => acquireAutomationLock,
  releaseAutomationLock: () => releaseAutomationLock,
  renewAutomationLock: () => renewAutomationLock,
  withAutomationLock: () => withAutomationLock
});
async function upstashRedisCommand(args) {
  if (!REDIS_URL || !REDIS_TOKEN) return null;
  try {
    const res = await fetch(REDIS_URL, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${REDIS_TOKEN}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(args)
    });
    if (!res.ok) return null;
    const json = await res.json();
    return json.result ?? null;
  } catch {
    return null;
  }
}
async function acquireRedisLock(lockKey, owner, ttlSeconds) {
  if (!REDIS_URL || !REDIS_TOKEN) return null;
  const fullKey = `af:auto:${lockKey}`;
  const result = await upstashRedisCommand(["SET", fullKey, owner, "NX", "EX", ttlSeconds]);
  if (result === null) return null;
  return result === "OK";
}
async function releaseRedisLock(lockKey, owner) {
  const fullKey = `af:auto:${lockKey}`;
  const current = await upstashRedisCommand(["GET", fullKey]);
  if (typeof current === "string" && current === owner) {
    await upstashRedisCommand(["DEL", fullKey]);
  }
}
async function acquireAutomationLock(lockKey, options, db2 = singleton()) {
  const ttlMs = Math.max(1, options.ttlMs);
  const ttlSeconds = Math.max(1, Math.ceil(ttlMs / 1e3));
  if (REDIS_URL && REDIS_TOKEN) {
    const redisResult = await acquireRedisLock(lockKey, options.owner, ttlSeconds);
    if (redisResult === true) return { ok: true, backend: "redis" };
    if (redisResult === false) return { ok: false, reason: "Lock held (redis)" };
  }
  const expiresAt = new Date(Date.now() + ttlMs);
  try {
    const outcome = await db2.$transaction(async (tx) => {
      const existing = await tx.automationLock.findUnique({
        where: { lockKey }
      });
      const now = /* @__PURE__ */ new Date();
      if (existing && existing.expiresAt > now) {
        if (existing.owner === options.owner) {
          await tx.automationLock.update({
            where: { lockKey },
            data: {
              expiresAt,
              metadata: options.metadata ?? void 0
            }
          });
          return "acquired";
        }
        return "busy";
      }
      if (existing) {
        await tx.automationLock.delete({ where: { lockKey } });
      }
      await tx.automationLock.create({
        data: {
          lockKey,
          owner: options.owner,
          expiresAt,
          metadata: options.metadata ?? void 0
        }
      });
      return "acquired";
    });
    if (outcome === "busy") {
      return { ok: false, reason: "Lock held (postgres)" };
    }
    return { ok: true, backend: "postgres" };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, reason: `Postgres lock error: ${msg}` };
  }
}
async function renewAutomationLock(lockKey, options, db2 = singleton()) {
  const ttlMs = Math.max(1, options.ttlMs);
  if (REDIS_URL && REDIS_TOKEN) {
    const fullKey = `af:auto:${lockKey}`;
    const current = await upstashRedisCommand(["GET", fullKey]);
    if (current !== null) {
      if (current !== options.owner) return { ok: false, reason: "Lock owned by another (redis)" };
      const ttlSeconds = Math.max(1, Math.ceil(ttlMs / 1e3));
      await upstashRedisCommand(["SET", fullKey, options.owner, "XX", "EX", ttlSeconds]);
      return { ok: true, backend: "redis" };
    }
  }
  const expiresAt = new Date(Date.now() + ttlMs);
  try {
    const affected = await db2.automationLock.updateMany({
      where: { lockKey, owner: options.owner, expiresAt: { gt: /* @__PURE__ */ new Date() } },
      data: { expiresAt, metadata: options.metadata ?? void 0 }
    });
    if (affected.count === 1) return { ok: true, backend: "postgres" };
    return { ok: false, reason: "Lease lost (not owner or expired)" };
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return { ok: false, reason: `Postgres renew error: ${msg}` };
  }
}
async function releaseAutomationLock(lockKey, owner, db2 = singleton()) {
  if (REDIS_URL && REDIS_TOKEN) {
    await releaseRedisLock(lockKey, owner);
  }
  await db2.automationLock.deleteMany({
    where: { lockKey, owner }
  }).catch(() => void 0);
}
async function withAutomationLock(lockKey, options, fn, db2 = singleton()) {
  const acquired = await acquireAutomationLock(lockKey, options, db2);
  if (!acquired.ok) return acquired;
  try {
    const value = await fn();
    return { ok: true, value };
  } finally {
    await releaseAutomationLock(lockKey, options.owner, db2);
  }
}
var singleton, REDIS_URL, REDIS_TOKEN;
var init_locks = __esm({
  "lib/automation/locks.ts"() {
    "use strict";
    init_prisma();
    singleton = () => prisma;
    REDIS_URL = process.env.UPSTASH_REDIS_REST_URL;
    REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  }
});

// lib/guillotine/nativeGuillotineWeek.ts
var nativeGuillotineWeek_exports = {};
__export(nativeGuillotineWeek_exports, {
  runNativeGuillotineWeek: () => runNativeGuillotineWeek
});
function rosterIdForOwner(ownerId, rosters) {
  if (ownerId.startsWith("roster:")) {
    const id2 = ownerId.slice("roster:".length);
    return rosters.some((r) => r.id === id2) ? id2 : null;
  }
  return rosters.find((r) => r.platformUserId === ownerId)?.id ?? null;
}
async function runNativeGuillotineWeek(input, deps = {}) {
  const now = deps.now?.() ?? /* @__PURE__ */ new Date();
  const base = { seasonId: input.seasonId };
  const season = await prisma.redraftSeason.findFirst({
    where: { id: input.seasonId },
    select: { id: true, leagueId: true, sport: true, season: true, status: true, currentWeek: true, totalWeeks: true }
  });
  if (!season || !await isGuillotineLeague(season.leagueId)) return { ...base, outcome: "not_guillotine" };
  const config = await getGuillotineConfig(season.leagueId);
  if (!config) return { ...base, outcome: "not_guillotine" };
  if (input.currentFantasyWeek > season.currentWeek) {
    await prisma.redraftSeason.update({ where: { id: season.id }, data: { currentWeek: input.currentFantasyWeek } });
  }
  const gSeason = await ensureGuillotineSeason({ leagueId: season.leagueId, redraftSeasonId: season.id });
  const active = await prisma.redraftRoster.findMany({
    where: { seasonId: season.id, isEliminated: false },
    select: { id: true, ownerId: true }
  });
  if (active.length === 0) return { ...base, outcome: "refused", reason: "no_active_teams" };
  if (active.length === 1) return completeSeason(season.id, gSeason, base);
  const lastChop = await prisma.guillotineRosterState.findFirst({
    where: { leagueId: season.leagueId, choppedInPeriod: { not: null } },
    orderBy: { choppedInPeriod: "desc" },
    select: { choppedInPeriod: true }
  });
  const week = Math.max((lastChop?.choppedInPeriod ?? 0) + 1, config.eliminationStartWeek);
  if (config.eliminationEndWeek != null && week > config.eliminationEndWeek) {
    return { ...base, outcome: "past_elimination_window", week };
  }
  if (week > input.currentFantasyWeek || week === input.currentFantasyWeek && week < season.totalWeeks) return { ...base, outcome: "waiting", week, reason: "week_in_progress" };
  let sealed = await finalizeRedraftWeek({ seasonId: season.id, week });
  if (sealed.refusal === "stat_coverage_below_floor" && deps.syncWeekStats) {
    await deps.syncWeekStats({ seasonId: season.id, week });
    sealed = await finalizeRedraftWeek({ seasonId: season.id, week });
  }
  if (sealed.refusal) return { ...base, outcome: "waiting", week, reason: sealed.refusal };
  const lastStart = sealed.slate?.lastStartTime ?? await lastKickoff(season, week, now);
  if (!lastStart) return { ...base, outcome: "waiting", week, reason: "period_end_unknown" };
  const periodEndedAt = new Date(lastStart);
  const pastCutoff = isPastCorrectionCutoff({
    correctionWindow: config.correctionWindow,
    periodEndedAt,
    statCorrectionHours: config.statCorrectionHours,
    customCutoffDayOfWeek: config.customCutoffDayOfWeek,
    customCutoffTimeUtc: config.customCutoffTimeUtc,
    now
  });
  if (!pastCutoff) return { ...base, outcome: "waiting", week, reason: "stat_correction_window" };
  const lockKey = `guillotine-chop:${season.leagueId}:${week}`;
  const owner = `native-guillotine:${season.id}:${now.getTime()}`;
  const lock = await acquireAutomationLock(lockKey, { ttlMs: LOCK_TTL_MS, owner });
  if (!lock.ok) return { ...base, outcome: "locked", week, reason: lock.reason };
  try {
    const already = await prisma.guillotineRosterState.findFirst({
      where: { leagueId: season.leagueId, choppedInPeriod: week },
      select: { rosterId: true }
    });
    if (already) return { ...base, outcome: "already_chopped", week };
    const points = /* @__PURE__ */ new Map();
    const bench = /* @__PURE__ */ new Map();
    for (const r of active) {
      const s = await scoreRosterForWeek({ leagueId: season.leagueId, rosterId: r.id, week, seasonYear: season.season });
      if (s.missingPlayerIds?.length) return { ...base, outcome: "waiting", week, reason: "starter_stat_coverage_incomplete" };
      points.set(r.id, s.points);
      const b = await scoreRosterForWeek({ leagueId: season.leagueId, rosterId: r.id, week, seasonYear: season.season, benchOnly: true });
      if (b.missingPlayerIds?.length) return { ...base, outcome: "waiting", week, reason: "bench_stat_coverage_incomplete" };
      bench.set(r.id, b.points);
    }
    const rosters = await prisma.roster.findMany({
      where: { leagueId: season.leagueId },
      select: { id: true, platformUserId: true, redraftRosterId: true }
    });
    const chopped = new Set(
      (await prisma.guillotineRosterState.findMany({
        where: { leagueId: season.leagueId, choppedAt: { not: null } },
        select: { rosterId: true }
      })).map((c) => c.rosterId)
    );
    const rosterForRedraft = /* @__PURE__ */ new Map();
    for (const r of active) {
      const direct = rosters.find((x) => x.redraftRosterId === r.id)?.id;
      const byOwner = rosterIdForOwner(r.ownerId, rosters);
      const id2 = direct ?? byOwner;
      if (id2) {
        rosterForRedraft.set(r.id, id2);
        if (!direct) await prisma.roster.updateMany({ where: { id: id2, redraftRosterId: null }, data: { redraftRosterId: r.id } });
      }
    }
    const unplaced = active.filter((r) => !rosterForRedraft.has(r.id)).map((r) => r.id);
    const covered = new Set(rosterForRedraft.values());
    const unscored = rosters.filter((r) => !chopped.has(r.id) && !covered.has(r.id)).map((r) => r.id);
    if (unplaced.length > 0 || unscored.length > 0) {
      console.warn("[guillotine] chop refused: rosters do not line up", JSON.stringify({ leagueId: season.leagueId, week, unplaced, unscored }));
      return { ...base, outcome: "refused", week, reason: "roster_mapping_incomplete" };
    }
    const previous = await prisma.guillotinePeriodScore.findMany({
      where: { leagueId: season.leagueId, weekOrPeriod: { lt: week } },
      select: { rosterId: true, periodPoints: true, weekOrPeriod: true }
    });
    const cumulBefore = /* @__PURE__ */ new Map();
    for (const p of previous) cumulBefore.set(p.rosterId, (cumulBefore.get(p.rosterId) ?? 0) + p.periodPoints);
    const periodScores = active.map((r) => {
      const rosterId = rosterForRedraft.get(r.id);
      const periodPoints = points.get(r.id) ?? 0;
      return { rosterId, periodPoints, benchPoints: bench.get(r.id) ?? 0, previousPeriodPoints: previous.find((p) => p.rosterId === rosterId && p.weekOrPeriod === week - 1)?.periodPoints ?? 0, seasonPointsCumul: Math.round(((cumulBefore.get(rosterId) ?? 0) + periodPoints) * 100) / 100 };
    });
    await savePeriodScores({ leagueId: season.leagueId, weekOrPeriod: week, season: season.season, scores: periodScores });
    const owner2 = await prisma.league.findUnique({ where: { id: season.leagueId }, select: { userId: true } });
    const result = await runElimination({
      leagueId: season.leagueId,
      weekOrPeriod: week,
      season: season.season,
      periodEndedAt,
      periodScores,
      commissionerChoppedRosterIds: input.commissionerChoppedRosterIds,
      skipChat: !owner2?.userId,
      systemUserId: owner2?.userId ?? void 0
    });
    const choppedRedraft = result?.eliminationFlagged?.marked ?? [];
    if (!result || result.choppedRosterIds.length === 0) {
      return { ...base, outcome: "refused", week, reason: result?.reason ?? "engine_returned_nothing" };
    }
    return { ...base, outcome: "chopped", week, choppedRedraftRosterIds: choppedRedraft };
  } finally {
    await releaseAutomationLock(lockKey, owner).catch(() => void 0);
  }
}
async function completeSeason(seasonId, gSeason, base) {
  await prisma.redraftSeason.updateMany({ where: { id: seasonId, status: { not: "complete" } }, data: { status: "complete" } });
  if (gSeason.ok) {
    await prisma.guillotineSeason.updateMany({ where: { id: gSeason.seasonId, status: { not: "complete" } }, data: { status: "complete" } });
  }
  return { ...base, outcome: "season_complete" };
}
async function lastKickoff(season, week, now) {
  const sport = seasonSportToLeagueSport(season.sport);
  if (!WEEK_KEYED_SPORTS.includes(sport)) return null;
  const slate = await readWeekSlate(prisma, { sport, season: season.season, week, seasonType: "regular", now });
  return slate.lastStartTime;
}
var LOCK_TTL_MS;
var init_nativeGuillotineWeek = __esm({
  "lib/guillotine/nativeGuillotineWeek.ts"() {
    "use strict";
    init_prisma();
    init_GuillotineLeagueConfig();
    init_GuillotineWeekEvaluator();
    init_GuillotineEliminationEngine();
    init_ensureGuillotineSeason();
    init_weekFinalizer();
    init_scoringEngine2();
    init_standardSeasonScope();
    init_locks();
    LOCK_TTL_MS = 5 * 60 * 1e3;
  }
});

// lib/guillotine/guillyAcquisition.ts
async function assertGuillyAcquisition(db2, args) {
  const season = await db2.guillotineSeason.findFirst({ where: { leagueId: args.leagueId }, orderBy: { season: "desc" } });
  if (!season)
    return;
  if (season.status === "complete")
    throw new Error("The Guilly season is complete; roster moves are locked.");
  const legacy = await db2.roster.findFirst({ where: { leagueId: args.leagueId, OR: [{ id: args.rosterId }, { redraftRosterId: args.rosterId }] }, select: { id: true, redraftRosterId: true } });
  const roster = await db2.redraftRoster.findFirst({ where: { id: legacy?.redraftRosterId ?? args.rosterId, seasonId: season.redraftSeasonId ?? void 0 } });
  if (!roster || roster.isEliminated)
    throw new Error("Eliminated Guilly managers cannot acquire players.");
  const release = await db2.guillotineWaiverRelease.findFirst({ where: { seasonId: season.id, playerId: args.playerId, releaseStatus: "pending" }, orderBy: { availableAt: "desc" } });
  if (!release)
    return;
  if (args.freeAgent)
    throw new Error("This eliminated player must be acquired through Guilly waivers.");
  if (!args.submission && release.availableAt > (args.now ?? /* @__PURE__ */ new Date()))
    throw new Error("Guilly release window has not opened yet.");
}
async function guillyReleasePending(db2, leagueId, playerId, now = /* @__PURE__ */ new Date()) {
  return !!await db2.guillotineWaiverRelease.findFirst({ where: { leagueId, playerId, releaseStatus: "pending", availableAt: { gt: now }, season: { status: { not: "complete" } } }, select: { id: true } });
}
async function markGuillyReleaseClaimed(db2, leagueId, playerId, rosterId, bid) {
  await db2.guillotineWaiverRelease.updateMany({ where: { leagueId, playerId, releaseStatus: "pending", availableAt: { lte: /* @__PURE__ */ new Date() } }, data: { releaseStatus: "claimed", claimedByRosterId: rosterId, claimedAt: /* @__PURE__ */ new Date(), winningBid: bid } });
}
var init_guillyAcquisition = __esm({
  "lib/guillotine/guillyAcquisition.ts"() {
    "use strict";
  }
});

// lib/roster/LineupTemplateValidation.ts
function toPlayerId(raw) {
  if (typeof raw === "string") return raw.trim() || null;
  if (raw && typeof raw === "object") {
    const obj = raw;
    const id2 = obj.id ?? obj.player_id;
    if (typeof id2 === "string" && id2.trim()) return id2.trim();
  }
  return null;
}
function normalizeLineupSection(section) {
  if (!Array.isArray(section)) return [];
  const out = [];
  const seen = /* @__PURE__ */ new Set();
  for (const item of section) {
    const id2 = toPlayerId(item);
    if (!id2 || seen.has(id2)) continue;
    seen.add(id2);
    const obj = item && typeof item === "object" ? item : {};
    out.push({
      ...obj,
      id: id2,
      position: String(obj.position ?? "UTIL").toUpperCase()
    });
  }
  return out;
}
function getNormalizedLineupSections(playerData) {
  const data = playerData && typeof playerData === "object" && !Array.isArray(playerData) ? playerData : {};
  const rawSections = data.lineup_sections && typeof data.lineup_sections === "object" && !Array.isArray(data.lineup_sections) ? data.lineup_sections : {};
  return {
    starters: normalizeLineupSection(rawSections.starters),
    bench: normalizeLineupSection(rawSections.bench),
    ir: normalizeLineupSection(rawSections.ir),
    taxi: normalizeLineupSection(rawSections.taxi),
    devy: normalizeLineupSection(rawSections.devy)
  };
}
var init_LineupTemplateValidation = __esm({
  "lib/roster/LineupTemplateValidation.ts"() {
    "use strict";
  }
});

// lib/roster/buildPersistedRosterDataFromRosterState.ts
function toPlayerId2(raw) {
  if (typeof raw === "string") return raw.trim() || null;
  if (raw && typeof raw === "object") {
    const obj = raw;
    const id2 = obj.id ?? obj.player_id;
    if (typeof id2 === "string" && id2.trim()) return id2.trim();
  }
  return null;
}
function normalizeLineupSection2(section) {
  if (!Array.isArray(section)) return [];
  const out = [];
  const seen = /* @__PURE__ */ new Set();
  for (const item of section) {
    const id2 = toPlayerId2(item);
    if (!id2 || seen.has(id2)) continue;
    seen.add(id2);
    const obj = item && typeof item === "object" ? item : {};
    out.push({
      id: id2,
      name: String(obj.name ?? obj.full_name ?? id2),
      team: String(obj.team ?? obj.team_abbreviation ?? "\u2014"),
      position: String(obj.position ?? "UTIL").toUpperCase(),
      opponent: String(obj.opponent ?? "\u2014"),
      gameTime: String(obj.gameTime ?? obj.game_time ?? "\u2014"),
      projection: Number(obj.projection ?? 0) || 0,
      actual: obj.actual == null ? null : Number(obj.actual),
      status: String(obj.status ?? obj.injury_status ?? "healthy").toLowerCase()
    });
  }
  return out;
}
function buildPersistedRosterDataFromRosterState(rosterState, existingPlayerData) {
  const base = existingPlayerData && typeof existingPlayerData === "object" && !Array.isArray(existingPlayerData) ? existingPlayerData : {};
  const rawObj = rosterState && typeof rosterState === "object" && !Array.isArray(rosterState) ? rosterState : {};
  const cleanBase = { ...base };
  delete cleanBase.starter_slot_ids;
  delete cleanBase.starter_slot_types;
  const lineupSections = {
    starters: normalizeLineupSection2(rawObj.starters),
    bench: normalizeLineupSection2(rawObj.bench),
    ir: normalizeLineupSection2(rawObj.ir),
    taxi: normalizeLineupSection2(rawObj.taxi),
    devy: normalizeLineupSection2(rawObj.devy)
  };
  const allIds = [
    ...lineupSections.starters.map((p) => String(p.id)),
    ...lineupSections.bench.map((p) => String(p.id)),
    ...lineupSections.ir.map((p) => String(p.id)),
    ...lineupSections.taxi.map((p) => String(p.id)),
    ...lineupSections.devy.map((p) => String(p.id))
  ];
  const players = [...new Set(allIds)];
  return {
    ...cleanBase,
    ...Array.isArray(rawObj.starter_slot_ids) && Array.isArray(rawObj.starter_slot_types) ? { starter_slot_ids: rawObj.starter_slot_ids, starter_slot_types: rawObj.starter_slot_types } : {},
    players,
    starters: lineupSections.starters.map((p) => p.id),
    reserve: lineupSections.ir.map((p) => p.id),
    taxi: lineupSections.taxi.map((p) => p.id),
    devy: lineupSections.devy.map((p) => p.id),
    lineup_sections: lineupSections,
    lineup_updated_at: (/* @__PURE__ */ new Date()).toISOString()
  };
}
var init_buildPersistedRosterDataFromRosterState = __esm({
  "lib/roster/buildPersistedRosterDataFromRosterState.ts"() {
    "use strict";
  }
});

// lib/redraft/legacyLineupBridge.ts
async function readCanonicalLineupForLegacyRoster(db2, roster, displaySlots) {
  if (!roster.redraftRosterId) return roster.playerData;
  const players = await db2.redraftRosterPlayer.findMany({ where: { rosterId: roster.redraftRosterId, droppedAt: null }, orderBy: { addedAt: "asc" } });
  const league = await db2.league.findUnique({ where: { id: roster.leagueId }, select: { sport: true, settings: true } });
  const order = league ? [...resolveRedraftRosterConfig(String(league.sport), league.settings).starterCapacities.keys()] : [];
  players.sort((a, b) => {
    const rank = (slot) => {
      const i = order.indexOf(normalizeToken(slot));
      return i < 0 ? order.length : i;
    };
    return rank(a.slotType) - rank(b.slotType);
  });
  const old = getNormalizedLineupSections(roster.playerData);
  const details = new Map(Object.values(old).flat().map((p) => [String(p.id), p]));
  const sections = { starters: [], bench: [], ir: [], taxi: [], devy: [] };
  for (const player of players) {
    const slot = normalizeToken(player.slotType);
    const section = slot === "BN" || slot === "BENCH" ? "bench" : slot === "IR" || slot === "RESERVE" ? "ir" : slot === "TAXI" ? "taxi" : slot === "DEVY" ? "devy" : "starters";
    sections[section].push({ ...details.get(player.playerId), id: player.playerId, name: player.playerName, position: player.position, team: player.team });
  }
  const capacities = league ? resolveRedraftRosterConfig(String(league.sport), league.settings).starterCapacities : /* @__PURE__ */ new Map();
  const slots = displaySlots ?? [...capacities].flatMap(([slot, count2]) => Array.from({ length: count2 }, () => slot));
  const seated = /* @__PURE__ */ new Set();
  const ids = slots.map((slot) => {
    const player = players.find((p) => !seated.has(p.playerId) && normalizeToken(p.slotType) === normalizeToken(slot));
    if (player) seated.add(player.playerId);
    return player?.playerId ?? "";
  });
  return { ...buildPersistedRosterDataFromRosterState(sections, roster.playerData), starter_slot_ids: ids, starter_slot_types: slots };
}
var init_legacyLineupBridge = __esm({
  "lib/redraft/legacyLineupBridge.ts"() {
    "use strict";
    init_LineupTemplateValidation();
    init_buildPersistedRosterDataFromRosterState();
    init_lineupValidation();
    init_rosterConfigResolver();
    init_lineupLock();
  }
});

// lib/redraft/legacyWaiverBridge.ts
async function mirrorCanonicalRosters(tx, rosterIds) {
  for (const redraftRosterId of new Set(rosterIds)) {
    const legacy = await tx.roster.findFirst({ where: { redraftRosterId } });
    if (!legacy) continue;
    const playerData = await readCanonicalLineupForLegacyRoster(tx, legacy);
    const scoring = await tx.redraftRoster.findUnique({ where: { id: redraftRosterId } });
    await tx.roster.update({ where: { id: legacy.id }, data: {
      playerData: toPrismaJsonInput(playerData),
      ...scoring?.faabBalance != null ? { faabRemaining: scoring.faabBalance } : {}
    } });
  }
}
var init_legacyWaiverBridge = __esm({
  "lib/redraft/legacyWaiverBridge.ts"() {
    "use strict";
    init_guillyAcquisition();
    init_legacyLineupBridge();
    init_prisma_json();
  }
});

// lib/idp/capEngine.ts
function contractEndYear(rec) {
  return rec.contractStartYear + rec.contractYears - 1;
}
function isSalaryActiveInSeason(rec, season) {
  if (!ACTIVE_STATUSES.includes(rec.status)) return false;
  return rec.contractStartYear <= season && season <= contractEndYear(rec);
}
async function getTeamCapSummary(leagueId, rosterId, season, db2 = prisma) {
  const cfg = await db2.iDPCapConfig.findUnique({ where: { leagueId } });
  if (!cfg) {
    throw new Error("No IDP cap configuration for this league");
  }
  const salaryRows = await db2.iDPSalaryRecord.findMany({
    where: { leagueId, rosterId, status: { in: [...ACTIVE_STATUSES] } }
  });
  let activeSalary = 0;
  for (const r of salaryRows) {
    if (isSalaryActiveInSeason(r, season)) activeSalary += r.salary;
  }
  const deadRows = await db2.iDPDeadMoney.findMany({
    where: { leagueId, rosterId, season }
  });
  const deadMoney = deadRows.reduce((s, d) => s + d.currentYearDead, 0);
  const totalCap = cfg.totalCap;
  const totalCapUsed = activeSalary + deadMoney;
  const availableCap = totalCap - totalCapUsed;
  const holdbackReserved = cfg.inSeasonHoldbackEnabled ? totalCap * cfg.inSeasonHoldbackPct : 0;
  const effectiveSpendableCap = Math.max(0, availableCap - holdbackReserved);
  const capFloorRequired = cfg.capFloorEnabled && cfg.capFloor != null ? totalCap * cfg.capFloor : void 0;
  let isCapCompliant = totalCapUsed <= totalCap;
  if (isCapCompliant && capFloorRequired != null) {
    isCapCompliant = totalCapUsed >= capFloorRequired;
  }
  return {
    totalCap,
    activeSalary,
    deadMoney,
    totalCapUsed,
    availableCap,
    capFloorRequired,
    isCapCompliant,
    holdbackReserved,
    effectiveSpendableCap
  };
}
async function validateCapCompliance(leagueId, rosterId, proposedSalary, season) {
  const summary = await getTeamCapSummary(leagueId, rosterId, season);
  if (proposedSalary <= summary.effectiveSpendableCap) {
    return { compliant: true };
  }
  const overage = proposedSalary - summary.effectiveSpendableCap;
  return {
    compliant: false,
    overage,
    message: `Insufficient cap room (need ${proposedSalary.toFixed(2)}, have ${summary.effectiveSpendableCap.toFixed(2)} effective).`
  };
}
async function refreshCapProjections(leagueId, rosterId) {
  const cfg = await prisma.iDPCapConfig.findUnique({ where: { leagueId } });
  if (!cfg) return;
  const baseSeason = cfg.season;
  const salaryRows = await prisma.iDPSalaryRecord.findMany({
    where: { leagueId, rosterId, status: { in: [...ACTIVE_STATUSES] } }
  });
  const deadRows = await prisma.iDPDeadMoney.findMany({ where: { leagueId, rosterId } });
  for (let i = 0; i < 5; i++) {
    const projectionYear = baseSeason + i;
    let committedSalary = 0;
    for (const r of salaryRows) {
      if (isSalaryActiveInSeason(r, projectionYear)) committedSalary += r.salary;
    }
    let deadCapHits = 0;
    for (const d of deadRows) {
      if (d.season === projectionYear) deadCapHits += d.currentYearDead;
      else if (d.season < projectionYear && d.futureYearsDead > 0) {
        const span = Math.max(0, d.yearsRemainingAtCut - 1);
        if (span > 0) {
          const per = d.futureYearsDead / span;
          const y = projectionYear - d.season;
          if (y >= 1 && y <= span) deadCapHits += per;
        }
      }
    }
    const totalCapUsed = committedSalary + deadCapHits;
    const availableCap = cfg.totalCap - totalCapUsed;
    await prisma.iDPCapProjection.upsert({
      where: {
        leagueId_rosterId_projectionYear: { leagueId, rosterId, projectionYear }
      },
      create: {
        leagueId,
        rosterId,
        projectionYear,
        committedSalary,
        deadCapHits,
        totalCapUsed,
        availableCap
      },
      update: {
        committedSalary,
        deadCapHits,
        totalCapUsed,
        availableCap
      }
    });
  }
}
async function appendCapTransaction(input) {
  await prisma.iDPCapTransaction.create({
    data: {
      leagueId: input.leagueId,
      rosterId: input.rosterId,
      playerId: input.playerId,
      playerName: input.playerName,
      isDefensive: input.isDefensive,
      transactionType: input.transactionType,
      salary: input.salary,
      contractYears: input.contractYears,
      deadMoneyCreated: input.deadMoneyCreated ?? 0,
      capImpact: input.capImpact,
      notes: input.notes,
      season: input.season,
      week: input.week
    }
  });
}
async function assignDraftSalary(leagueId, rosterId, playerId, playerName, position, isDefensive, draftMethod, draftValue, contractYears, acquisitionMethod, season) {
  const cfg = await prisma.iDPCapConfig.findUnique({ where: { leagueId } });
  if (!cfg) throw new Error("No IDP cap configuration for this league");
  const capYear = season ?? cfg.season;
  const v = await validateCapCompliance(leagueId, rosterId, draftValue, capYear);
  if (!v.compliant) {
    throw new Error(v.message ?? "Cap compliance check failed");
  }
  const txType = acquisitionMethod === "waiver" ? "waiver_assign" : draftMethod === "auction" || acquisitionMethod === "auction" ? "auction_win" : draftMethod === "manual" ? "salary_correction" : "draft_assign";
  const record2 = await prisma.iDPSalaryRecord.create({
    data: {
      leagueId,
      rosterId,
      playerId,
      playerName,
      position,
      isDefensive,
      salary: draftValue,
      contractYears,
      yearsRemaining: contractYears,
      contractStartYear: capYear,
      status: "active",
      acquisitionMethod
    }
  });
  await appendCapTransaction({
    leagueId,
    rosterId,
    playerId,
    playerName,
    isDefensive,
    transactionType: txType,
    salary: draftValue,
    contractYears,
    capImpact: draftValue,
    season: capYear,
    notes: `assignDraftSalary draftMethod=${draftMethod}`
  });
  await refreshCapProjections(leagueId, rosterId);
  return record2;
}
async function assignIdpCapSalaryForWaiverClaim(leagueId, rosterId, playerId, playerName, position, isDefensive, bidAmount) {
  const cfg = await prisma.iDPCapConfig.findUnique({ where: { leagueId } });
  if (!cfg) return null;
  const salary = bidAmount != null && bidAmount > 0 ? bidAmount : 1;
  return assignDraftSalary(
    leagueId,
    rosterId,
    playerId,
    playerName,
    position,
    isDefensive,
    "waiver",
    salary,
    1,
    "waiver",
    cfg.season
  );
}
var ACTIVE_STATUSES;
var init_capEngine = __esm({
  "lib/idp/capEngine.ts"() {
    "use strict";
    init_prisma();
    ACTIVE_STATUSES = ["active", "franchise_tagged"];
  }
});

// lib/redraft/waiverEngine.ts
var waiverEngine_exports = {};
__export(waiverEngine_exports, {
  compareWaiverClaims: () => compareWaiverClaims,
  finalizeRedraftWaiverClaimIdpCap: () => finalizeRedraftWaiverClaimIdpCap,
  processWaiverWindow: () => processWaiverWindow,
  resetWaiverPriority: () => resetWaiverPriority
});
async function finalizeRedraftWaiverClaimIdpCap(opts) {
  await assignIdpCapSalaryForWaiverClaim(
    opts.leagueId,
    opts.rosterId,
    opts.addPlayerId,
    opts.addPlayerName,
    opts.position,
    opts.isDefensive,
    opts.bidAmount
  );
}
function isDefensivePosition(position) {
  return ["DE", "DT", "DL", "LB", "ILB", "OLB", "CB", "S", "DB"].includes(position.toUpperCase());
}
async function resolvePlayerMeta(addPlayerId, addPlayerName, sport) {
  const sportKeys = [sport.toUpperCase(), sport.toLowerCase()];
  const player = await prisma.sportsPlayer.findFirst({
    where: {
      sport: { in: sportKeys },
      OR: [{ externalId: addPlayerId }, { sleeperId: addPlayerId }, { id: addPlayerId }]
    },
    select: { name: true, position: true, team: true }
  }).catch(() => null);
  if (player) {
    return {
      playerName: player.name || addPlayerName,
      position: player.position || "UNK",
      team: player.team ?? null,
      warning: null
    };
  }
  const identity = await prisma.playerIdentityMap.findFirst({
    where: {
      sport: { in: sportKeys },
      OR: [
        { sleeperId: addPlayerId },
        { fantasyCalcId: addPlayerId },
        { rollingInsightsId: addPlayerId },
        { apiSportsId: addPlayerId },
        { espnId: addPlayerId },
        { clearSportsId: addPlayerId }
      ]
    },
    select: { canonicalName: true, position: true, currentTeam: true }
  }).catch(() => null);
  if (identity) {
    return {
      playerName: identity.canonicalName || addPlayerName,
      position: identity.position || "UNK",
      team: identity.currentTeam ?? null,
      warning: null
    };
  }
  return {
    playerName: addPlayerName,
    position: "UNK",
    team: null,
    warning: `No cached player metadata found for ${addPlayerId}; rostered with unknown position.`
  };
}
async function denyClaim(claimId, reason) {
  const changed = await prisma.redraftWaiverClaim.updateMany({
    where: { id: claimId, status: "pending" },
    data: { status: "denied", processedAt: /* @__PURE__ */ new Date(), denialReason: reason }
  });
  if (!changed.count) {
    const current = await prisma.redraftWaiverClaim.findUnique({ where: { id: claimId } });
    return { claimId, status: current?.status ?? "denied", reason: current?.denialReason ?? void 0 };
  }
  return { claimId, status: "denied", reason };
}
async function moveApprovedRosterToBack(seasonId, rosterId) {
  const rosters = await prisma.redraftRoster.findMany({
    where: { seasonId },
    select: { waiverPriority: true },
    orderBy: { waiverPriority: "desc" },
    take: 1
  });
  const maxPriority = rosters[0]?.waiverPriority ?? 0;
  await prisma.redraftRoster.update({
    where: { id: rosterId },
    data: { waiverPriority: maxPriority + 1 }
  });
}
async function runWaiverSettlement(callback) {
  const transaction = prisma.$transaction;
  if (typeof transaction === "function") {
    await transaction.call(prisma, callback);
    return;
  }
  await callback(prisma);
}
function compareWaiverClaims(a, b) {
  const bidA = a.bidAmount ?? 0;
  const bidB = b.bidAmount ?? 0;
  if (bidA !== bidB) return bidB - bidA;
  const prioA = a.priority ?? Number.MAX_SAFE_INTEGER;
  const prioB = b.priority ?? Number.MAX_SAFE_INTEGER;
  if (prioA !== prioB) return prioA - prioB;
  const tA = a.submittedAt.getTime();
  const tB = b.submittedAt.getTime();
  if (tA !== tB) return tA - tB;
  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0;
}
async function processWaiverWindow(leagueId, seasonId) {
  const season = await prisma.redraftSeason.findFirst({ where: { id: seasonId, leagueId } });
  if (!season || season.status === "complete") return [];
  const guilly = await prisma.guillotineSeason.findFirst({ where: { redraftSeasonId: season.id } });
  const claims = await prisma.redraftWaiverClaim.findMany({
    where: { leagueId, seasonId, status: "pending" },
    // Mirrors compareWaiverClaims: highest bid first (null bids last, treated as
    // 0), then lowest priority number, earliest submission, then stable id.
    orderBy: [
      { bidAmount: { sort: "desc", nulls: "last" } },
      { priority: "asc" },
      { submittedAt: "asc" },
      { id: "asc" }
    ]
  });
  const results = [];
  const acquiredPlayerIds = /* @__PURE__ */ new Set();
  for (const claim of claims) {
    const roster = await prisma.redraftRoster.findFirst({
      where: { id: claim.rosterId, seasonId, leagueId }
    });
    if (!roster) {
      results.push(await denyClaim(claim.id, "Roster not found for this season."));
      continue;
    }
    if (roster.isEliminated) {
      results.push(await denyClaim(claim.id, "This team has been eliminated."));
      continue;
    }
    if (guilly && await guillyReleasePending(prisma, leagueId, claim.addPlayerId)) continue;
    const bid = claim.bidAmount ?? 0;
    if (bid < 0) {
      results.push(await denyClaim(claim.id, "Invalid FAAB bid."));
      continue;
    }
    if (roster.faabBalance != null && bid > roster.faabBalance) {
      results.push(await denyClaim(claim.id, "Insufficient FAAB balance."));
      continue;
    }
    if (acquiredPlayerIds.has(claim.addPlayerId)) {
      results.push(await denyClaim(claim.id, "Another claim in this waiver run already won this player."));
      continue;
    }
    const existingActive = await prisma.redraftRosterPlayer.findFirst({
      where: {
        playerId: claim.addPlayerId,
        droppedAt: null,
        roster: { seasonId }
      },
      select: { rosterId: true }
    });
    if (existingActive) {
      const reason = existingActive.rosterId === claim.rosterId ? "Player is already on this roster." : "Player is already rostered in this season.";
      results.push(await denyClaim(claim.id, reason));
      continue;
    }
    const meta = await resolvePlayerMeta(claim.addPlayerId, claim.addPlayerName, season.sport || "NFL");
    try {
      await runWaiverSettlement(async (tx) => {
        await tx.$queryRaw`SELECT id FROM redraft_seasons WHERE id = ${seasonId} FOR UPDATE`;
        await tx.$queryRaw`SELECT id FROM redraft_rosters WHERE id = ${claim.rosterId} FOR UPDATE`;
        const freshClaim = await tx.redraftWaiverClaim.findUnique({ where: { id: claim.id } });
        if (!freshClaim || freshClaim.status !== "pending") throw new WaiverAlreadyProcessedError();
        const freshRoster = await tx.redraftRoster.findFirst({ where: { id: claim.rosterId, leagueId, seasonId } });
        if (!freshRoster || freshRoster.isEliminated) throw new WaiverSettlementRefusedError("Roster is unavailable or eliminated.");
        if (guilly) await assertGuillyAcquisition(tx, { leagueId, rosterId: claim.rosterId, playerId: claim.addPlayerId });
        const owned = await tx.redraftRosterPlayer.findFirst({ where: { playerId: claim.addPlayerId, droppedAt: null, roster: { seasonId } } });
        if (owned) throw new WaiverSettlementRefusedError("Player is already rostered in this season.");
        if (bid > 0 && freshRoster.faabBalance != null) {
          const paid = await tx.redraftRoster.updateMany({
            where: { id: claim.rosterId, faabBalance: { gte: bid } },
            data: { faabBalance: { decrement: bid } }
          });
          if (paid.count !== 1) throw new WaiverSettlementRefusedError("Insufficient FAAB balance.");
        }
        if (claim.dropPlayerId) {
          const dropResult = await tx.redraftRosterPlayer.updateMany({
            where: { rosterId: claim.rosterId, playerId: claim.dropPlayerId, droppedAt: null },
            data: { droppedAt: /* @__PURE__ */ new Date() }
          });
          if (dropResult.count === 0) throw new WaiverDropInactiveError();
        }
        await tx.redraftRosterPlayer.create({
          data: {
            rosterId: claim.rosterId,
            playerId: claim.addPlayerId,
            playerName: meta.playerName,
            position: meta.position,
            team: meta.team,
            sport: season.sport || "NFL",
            slotType: "bench",
            acquisitionType: "waiver"
          }
        });
        await tx.redraftWaiverClaim.update({
          where: { id: claim.id },
          data: {
            status: "approved",
            processedAt: /* @__PURE__ */ new Date(),
            denialReason: meta.warning
          }
        });
        if (guilly) await markGuillyReleaseClaimed(tx, leagueId, claim.addPlayerId, claim.rosterId, bid);
        await mirrorCanonicalRosters(tx, [claim.rosterId]);
      });
    } catch (err) {
      if (err instanceof WaiverAlreadyProcessedError) continue;
      if (err instanceof WaiverSettlementRefusedError) {
        results.push(await denyClaim(claim.id, err.message));
        continue;
      }
      if (err instanceof WaiverDropInactiveError) {
        results.push(await denyClaim(claim.id, "Drop player is not active on this roster."));
        continue;
      }
      console.error("[waiver-settlement] transaction failed", err);
      results.push(await denyClaim(claim.id, "Waiver settlement failed; no roster changes were applied."));
      continue;
    }
    await prisma.redraftLeagueTransaction.create({
      data: {
        leagueId,
        seasonId,
        rosterId: claim.rosterId,
        type: "waiver_claim_approved",
        metadata: {
          claimId: claim.id,
          addPlayerId: claim.addPlayerId,
          addPlayerName: meta.playerName,
          dropPlayerId: claim.dropPlayerId ?? null,
          bidAmount: claim.bidAmount ?? null,
          warning: meta.warning
        }
      }
    }).catch(() => null);
    await finalizeRedraftWaiverClaimIdpCap({
      leagueId,
      rosterId: claim.rosterId,
      addPlayerId: claim.addPlayerId,
      addPlayerName: meta.playerName,
      bidAmount: claim.bidAmount,
      position: meta.position,
      isDefensive: isDefensivePosition(meta.position)
    }).catch(() => null);
    acquiredPlayerIds.add(claim.addPlayerId);
    await moveApprovedRosterToBack(seasonId, claim.rosterId).catch(() => null);
    results.push({ claimId: claim.id, status: "approved", reason: meta.warning ?? void 0 });
  }
  const events = getPlatformEvents();
  for (const r of results) {
    await events.emit(EVENT.WAIVER_PROCESSED, {
      leagueId,
      seasonId,
      sport: season.sport ?? null,
      leagueConcept: "redraft",
      actor: { type: "system" },
      source: "engine:waiver",
      idempotencyKey: `waiver.processed:${r.claimId}`,
      subjects: [{ kind: "waiver_claim", id: r.claimId }],
      payload: { claimId: r.claimId, result: r.status }
    });
  }
  if (results.length > 0) {
    const claimIds = results.map((r) => r.claimId).sort();
    await events.emit(EVENT.WAIVER_WINDOW_PROCESSED, {
      leagueId,
      seasonId,
      sport: season.sport ?? null,
      leagueConcept: "redraft",
      actor: { type: "system" },
      source: "engine:waiver",
      idempotencyKey: `waiver.window:${seasonId}:${claimIds.length}:${claimIds[0]}:${claimIds[claimIds.length - 1]}`,
      payload: {
        processed: results.length,
        succeeded: results.filter((r) => r.status === "approved").length,
        failed: results.filter((r) => r.status !== "approved").length
      }
    });
  }
  return results;
}
async function resetWaiverPriority(seasonId) {
  const rosters = await prisma.redraftRoster.findMany({
    where: { seasonId },
    orderBy: [{ wins: "asc" }, { pointsFor: "asc" }]
  });
  let p = 1;
  for (const r of rosters) {
    await prisma.redraftRoster.update({
      where: { id: r.id },
      data: { waiverPriority: p }
    });
    p += 1;
  }
}
var WaiverDropInactiveError, WaiverAlreadyProcessedError, WaiverSettlementRefusedError;
var init_waiverEngine = __esm({
  "lib/redraft/waiverEngine.ts"() {
    "use strict";
    init_prisma();
    init_guillyAcquisition();
    init_legacyWaiverBridge();
    init_capEngine();
    init_events();
    WaiverDropInactiveError = class extends Error {
    };
    WaiverAlreadyProcessedError = class extends Error {
    };
    WaiverSettlementRefusedError = class extends Error {
    };
  }
});

// lib/media/imageUrl.ts
function toImageUrl(value) {
  if (typeof value !== "string") return null;
  const v = value.trim();
  if (!v) return null;
  if (/^https?:\/\//i.test(v)) return v;
  if (v.startsWith("//")) return v;
  if (v.startsWith("/") && !v.startsWith("//")) return v;
  if (/^data:image\//i.test(v)) return v;
  return null;
}
var init_imageUrl = __esm({
  "lib/media/imageUrl.ts"() {
    "use strict";
  }
});

// lib/rolling-insights.ts
var rolling_insights_exports = {};
__export(rolling_insights_exports, {
  fetchNFLDepthCharts: () => fetchNFLDepthCharts,
  fetchNFLPlayerStats: () => fetchNFLPlayerStats,
  fetchNFLRoster: () => fetchNFLRoster,
  fetchNFLRosterEnhanced: () => fetchNFLRosterEnhanced,
  fetchNFLSchedule: () => fetchNFLSchedule,
  fetchNFLTeams: () => fetchNFLTeams,
  fetchNFLTeamsFull: () => fetchNFLTeamsFull,
  getCurrentNFLSeason: () => getCurrentNFLSeason,
  getLastFetchNFLRosterTrace: () => getLastFetchNFLRosterTrace,
  isUsefulNFLRosterPayload: () => isUsefulNFLRosterPayload,
  normalizeRIDepthChartPlayers: () => normalizeRIDepthChartPlayers,
  rollingInsightsGraphqlQuery: () => rollingInsightsGraphqlQuery,
  runRollingInsightsHealthCheck: () => runRollingInsightsHealthCheck,
  searchNFLPlayer: () => searchNFLPlayer,
  syncNFLDepthChartsToDb: () => syncNFLDepthChartsToDb,
  syncNFLPlayersToDb: () => syncNFLPlayersToDb,
  syncNFLScheduleToDb: () => syncNFLScheduleToDb,
  syncNFLTeamStatsToDb: () => syncNFLTeamStatsToDb,
  syncNFLTeamsToDb: () => syncNFLTeamsToDb,
  testAuth: () => getAccessToken
});
function asObj(value) {
  if (!value || typeof value !== "object") return null;
  return value;
}
function asString(value) {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}
function asNumber3(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length) {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}
function normalizeRITeam(raw) {
  const obj = asObj(raw);
  if (!obj) return null;
  const id2 = asString(obj.id ?? obj.team_id ?? obj.teamId ?? obj.externalId);
  const teamName = asString(obj.team ?? obj.name ?? obj.full_name ?? obj.city_name);
  const abbrv = asString(obj.abbrv ?? obj.abbreviation ?? obj.team_abbr ?? obj.shortName);
  if (!id2 || !teamName || !abbrv) return null;
  return {
    id: id2,
    team: teamName,
    abbrv,
    mascot: asString(obj.mascot) ?? "",
    img: asString(obj.img ?? obj.logo ?? obj.logo_url)
  };
}
function normalizeRIPlayer(raw) {
  const obj = asObj(raw);
  if (!obj) return null;
  const id2 = asString(obj.id ?? obj.player_id ?? obj.playerId ?? obj.externalId);
  const player = asString(obj.player ?? obj.full_name ?? obj.name);
  if (!id2 || !player) return null;
  const teamObj = asObj(obj.team);
  const teamAbbrv = asString(obj.team_abbr ?? obj.teamAbbr ?? obj.team_abbreviation) ?? asString(teamObj?.abbrv ?? teamObj?.abbreviation);
  const teamId = asString(obj.team_id ?? obj.teamId) ?? asString(teamObj?.id);
  const teamName = asString(teamObj?.team ?? teamObj?.name ?? obj.team_name);
  const teamMascot = asString(teamObj?.mascot ?? obj.team_mascot);
  const seasonStatsObj = asObj(obj.season_stats) ?? asObj(obj.stats);
  const fantasyPoints = asNumber3(
    seasonStatsObj?.DK_fantasy_points ?? seasonStatsObj?.fantasy_points
  );
  const fantasyPointsPerGame = asNumber3(
    seasonStatsObj?.DK_fantasy_points_per_game ?? seasonStatsObj?.fantasy_points_per_game
  );
  const gamesPlayed = asNumber3(seasonStatsObj?.games_played ?? seasonStatsObj?.gamesPlayed);
  const period = asString(seasonStatsObj?.period) ?? getCurrentNFLSeason();
  const regularSeason = seasonStatsObj ? [
    {
      period,
      passing_yards: asNumber3(seasonStatsObj.passing_yards),
      passing_touchdowns: asNumber3(seasonStatsObj.passing_touchdowns),
      passing_attempts: asNumber3(seasonStatsObj.passing_attempts),
      completions: asNumber3(seasonStatsObj.completions),
      interceptions: asNumber3(seasonStatsObj.interceptions),
      passerRating: asNumber3(seasonStatsObj.passerRating),
      rushing_yards: asNumber3(seasonStatsObj.rushing_yards),
      rushing_touchdowns: asNumber3(seasonStatsObj.rushing_touchdowns),
      rushing_attempts: asNumber3(seasonStatsObj.rushing_attempts),
      receptions: asNumber3(seasonStatsObj.receptions),
      receiving_yards: asNumber3(seasonStatsObj.receiving_yards),
      receiving_touchdowns: asNumber3(seasonStatsObj.receiving_touchdowns),
      targets: asNumber3(seasonStatsObj.targets),
      sacks: asNumber3(seasonStatsObj.sacks),
      tackles: asNumber3(seasonStatsObj.tackles),
      fumbles: asNumber3(seasonStatsObj.fumbles),
      fumbles_lost: asNumber3(seasonStatsObj.fumbles_lost),
      DK_fantasy_points: fantasyPoints,
      DK_fantasy_points_per_game: fantasyPointsPerGame,
      games_played: gamesPlayed,
      snap_count_offense: asNumber3(seasonStatsObj.snap_count_offense),
      snap_count_defense: asNumber3(seasonStatsObj.snap_count_defense),
      field_goals_made: asNumber3(seasonStatsObj.field_goals_made),
      field_goals_attempted: asNumber3(seasonStatsObj.field_goals_attempted),
      extra_points_made: asNumber3(seasonStatsObj.extra_points_made),
      extra_points_attempted: asNumber3(seasonStatsObj.extra_points_attempted)
    }
  ] : [];
  return {
    id: id2,
    player,
    team: teamAbbrv || teamId || teamName ? {
      id: teamId ?? teamAbbrv ?? "UNK",
      team: teamName ?? teamAbbrv ?? "Unknown Team",
      abbrv: teamAbbrv ?? teamName ?? "UNK",
      mascot: teamMascot ?? ""
    } : null,
    number: asNumber3(obj.number),
    position: asString(obj.position),
    height: asString(obj.height),
    weight: asNumber3(obj.weight),
    college: asString(obj.college),
    dob: asString(obj.dob),
    // RI sends the literal `contact_support` for a missing headshot — see lib/media/imageUrl.ts.
    img: toImageUrl(obj.img ?? obj.image ?? obj.headshot_url),
    positionCategory: asString(obj.positionCategory ?? obj.position_category),
    status: asString(obj.status),
    DK_salary: asNumber3(obj.DK_salary ?? obj.dk_salary),
    regularSeason,
    postSeason: []
  };
}
function normalizeRISchedule(raw) {
  const obj = asObj(raw);
  if (!obj) return null;
  const gameId = asString(obj.gameId ?? obj.game_ID ?? obj.id ?? obj.game_id ?? obj.externalId);
  const awayTeam = asString(
    obj.awayTeam ?? obj.away_team ?? obj.away_team_abbr ?? obj.away ?? obj.away_name
  );
  const homeTeam = asString(
    obj.homeTeam ?? obj.home_team ?? obj.home_team_abbr ?? obj.home ?? obj.home_name
  );
  if (!gameId || !awayTeam || !homeTeam) return null;
  const venueObj = asObj(obj.venue);
  const venueArena = asString(venueObj?.arena ?? obj.venue_name ?? obj.venue);
  const venueCity = asString(venueObj?.city ?? obj.city);
  const venueState = asString(venueObj?.state ?? obj.state);
  const venueDomeRaw = venueObj?.dome;
  const venueDome = typeof venueDomeRaw === "boolean" ? venueDomeRaw : null;
  return {
    gameId,
    awayTeam,
    homeTeam,
    awayTeamId: asString(obj.awayTeamId ?? obj.away_team_ID ?? obj.away_team_id ?? obj.awayTeamID),
    homeTeamId: asString(obj.homeTeamId ?? obj.home_team_ID ?? obj.home_team_id ?? obj.homeTeamID),
    week: asNumber3(obj.week ?? obj.weekNumber ?? obj.week_number ?? obj.period),
    seasonType: classifyRiSeasonType(obj.seasonType ?? obj.season_type),
    date: asString(obj.date ?? obj.game_time ?? obj.start_time ?? obj.startTime) ?? (/* @__PURE__ */ new Date()).toISOString(),
    status: asString(obj.status ?? obj.game_status) ?? "scheduled",
    season: asString(obj.season) ?? getCurrentNFLSeason(),
    venue: venueArena || venueCity || venueState || venueDome !== null ? {
      arena: venueArena,
      city: venueCity,
      state: venueState,
      dome: venueDome
    } : null
  };
}
async function fetchRollingInsightsRest(dataType, options, mapFn) {
  const result = await rollingInsightsProvider({
    sport: "NFL",
    dataType,
    query: options
  });
  if (!Array.isArray(result.data)) return null;
  const normalized = [];
  for (const row of result.data) {
    const mapped = mapFn(row);
    if (mapped) normalized.push(mapped);
  }
  return normalized;
}
function getEnabledRollingInsightsSports() {
  return {
    NFL: ROLLING_INSIGHTS_SPORTS.NFL,
    NHL: ROLLING_INSIGHTS_SPORTS.NHL,
    NBA: ROLLING_INSIGHTS_SPORTS.NBA,
    MLB: ROLLING_INSIGHTS_SPORTS.MLB,
    NCAAF: ROLLING_INSIGHTS_SPORTS.NCAAF,
    NCAAB: ROLLING_INSIGHTS_SPORTS.NCAAB,
    SOCCER: ROLLING_INSIGHTS_SPORTS.SOCCER
  };
}
async function getAccessToken() {
  if (cachedToken && cachedToken.expiresAt > Date.now() + 6e4) {
    return cachedToken.accessToken;
  }
  const credentialPairs = [
    {
      clientId: process.env.ROLLING_INSIGHTS_CLIENT_ID?.trim() || "",
      clientSecret: process.env.ROLLING_INSIGHTS_CLIENT_SECRET?.trim() || ""
    },
    {
      clientId: process.env.ROLLING_INSIGHTS_CLIENT_ID2?.trim() || "",
      clientSecret: process.env.ROLLING_INSIGHTS_CLIENT_SECRET2?.trim() || ""
    }
  ].filter((pair) => pair.clientId && pair.clientSecret);
  const directApiKey = process.env.ROLLING_INSIGHTS_API_KEY?.trim();
  if (!credentialPairs.length) {
    if (directApiKey) {
      return directApiKey;
    }
    throw new Error("Rolling Insights credentials not configured");
  }
  let lastStatus = null;
  for (const { clientId, clientSecret } of credentialPairs) {
    const response = await fetch(AUTH_URL, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        grant_type: "client_credentials",
        client_id: clientId,
        client_secret: clientSecret
      })
    });
    lastStatus = response.status;
    if (!response.ok) {
      continue;
    }
    const data = await response.json();
    if (!data.access_token) {
      continue;
    }
    cachedToken = {
      accessToken: data.access_token,
      expiresAt: Date.now() + (data.expires_in || 3600) * 1e3
    };
    return cachedToken.accessToken;
  }
  if (directApiKey) {
    return directApiKey;
  }
  throw new Error(`Rolling Insights auth failed: ${lastStatus ?? "unknown"}`);
}
async function rollingInsightsGraphqlQuery(query, variables) {
  return graphqlQuery(query, variables);
}
async function graphqlQuery(query, variables) {
  const token = await getAccessToken();
  const response = await fetch(GRAPHQL_URL, {
    method: "POST",
    headers: {
      "Authorization": `Bearer ${token}`,
      "Content-Type": "application/json"
    },
    body: JSON.stringify({ query, variables })
  });
  if (!response.ok) {
    throw new Error(`Rolling Insights GraphQL request failed: ${response.status}`);
  }
  const result = await response.json();
  if (result.errors?.length) {
    console.error("[RollingInsights] GraphQL errors:", JSON.stringify(result.errors));
    throw new Error(`GraphQL error: ${result.errors[0].message}`);
  }
  return result.data;
}
function isUsefulNFLRosterPayload(players) {
  const total = Array.isArray(players) ? players.length : 0;
  if (!total) {
    return {
      useful: false,
      total: 0,
      withRealTeam: 0,
      withRegularSeason: 0,
      withFantasyPoints: 0,
      reason: "empty payload"
    };
  }
  let withRealTeam = 0;
  let withRegularSeason = 0;
  let withFantasyPoints = 0;
  for (const p of players) {
    const teamAbbr = (p.team?.abbrv ?? "").trim().toUpperCase();
    if (teamAbbr && teamAbbr !== "UNK" && teamAbbr !== "UNKNOWN" && teamAbbr !== "FA") {
      withRealTeam++;
    }
    const season = Array.isArray(p.regularSeason) ? p.regularSeason[0] : null;
    if (season) {
      withRegularSeason++;
      if (season.DK_fantasy_points != null) withFantasyPoints++;
    }
  }
  const teamRatio = withRealTeam / total;
  const useful = teamRatio >= 0.25;
  let reason = "ok";
  if (useful && withRegularSeason === 0) {
    reason = `useful (current rosters) but no regularSeason blocks \u2014 stats live on a separate endpoint`;
  } else if (!useful) {
    reason = `historical-shape: only ${withRealTeam}/${total} rows have a real team (need >=25%)`;
  }
  return { useful, total, withRealTeam, withRegularSeason, withFantasyPoints, reason };
}
function getLastFetchNFLRosterTrace() {
  return lastFetchNFLRosterTrace;
}
async function fetchNFLRoster(options) {
  const trace = {
    rest: { source: "rest", attempted: true, returned: 0 },
    graphql: { source: "graphql", attempted: false, returned: 0 },
    finalSource: "none"
  };
  const normalizedSeason = options.season && /^\d{4}$/.test(options.season.trim()) ? `${options.season.trim()}-${Number(options.season.trim()) + 1}` : options.season ?? getCurrentNFLSeason();
  const effectiveOptions = { ...options, season: normalizedSeason };
  const restRoster = await fetchRollingInsightsRest("players", effectiveOptions, normalizeRIPlayer);
  trace.rest.returned = Array.isArray(restRoster) ? restRoster.length : 0;
  if (Array.isArray(restRoster) && restRoster.length > 0) {
    const quality = isUsefulNFLRosterPayload(restRoster);
    trace.rest.quality = quality;
    if (quality.useful) {
      trace.finalSource = "rest";
      lastFetchNFLRosterTrace = trace;
      return restRoster;
    }
    console.warn(
      `[RollingInsights] REST NFL roster not useful (${quality.reason}); falling back to GraphQL`
    );
  }
  const args = [];
  if (effectiveOptions.season) args.push(`season: "${effectiveOptions.season}"`);
  if (effectiveOptions.playerName) args.push(`playerName: "${effectiveOptions.playerName}"`);
  if (effectiveOptions.teamId) args.push(`teamId: "${effectiveOptions.teamId}"`);
  if (effectiveOptions.limit) args.push(`limit: ${effectiveOptions.limit}`);
  const argsStr = args.length ? `(${args.join(", ")})` : "";
  const query = `{ nflRoster${argsStr} { ${PLAYER_FIELDS} } }`;
  trace.graphql.attempted = true;
  let gqlPlayers = [];
  try {
    const data = await graphqlQuery(query);
    gqlPlayers = (data.nflRoster ?? []).map((raw) => normalizeRIPlayer(raw) ?? raw).filter(Boolean);
  } catch (err) {
    console.warn(
      `[RollingInsights] GraphQL nflRoster failed:`,
      err instanceof Error ? err.message : err
    );
  }
  trace.graphql.returned = gqlPlayers.length;
  trace.graphql.quality = isUsefulNFLRosterPayload(gqlPlayers);
  trace.finalSource = gqlPlayers.length ? "graphql" : "none";
  lastFetchNFLRosterTrace = trace;
  return gqlPlayers;
}
async function fetchNFLTeams() {
  const restTeams = await fetchRollingInsightsRest("teams", {}, normalizeRITeam);
  if (Array.isArray(restTeams) && restTeams.length > 0) {
    return restTeams;
  }
  const query = `{ nflTeams { id team abbrv mascot img } }`;
  const data = await graphqlQuery(query);
  return data.nflTeams || [];
}
async function fetchNFLSchedule(options) {
  const restSchedule = await fetchRollingInsightsRest("schedule", options, normalizeRISchedule);
  if (Array.isArray(restSchedule) && restSchedule.length > 0) {
    return restSchedule;
  }
  const args = [];
  if (options.season) args.push(`season: "${options.season}"`);
  if (options.limit) args.push(`limit: ${options.limit}`);
  const argsStr = args.length ? `(${args.join(", ")})` : "";
  const query = `{ nflSchedules${argsStr} { gameId awayTeam homeTeam date status season venue { arena city state dome } } }`;
  const data = await graphqlQuery(query);
  return data.nflSchedules || [];
}
async function searchNFLPlayer(name) {
  return fetchNFLRoster({ playerName: name, limit: 10 });
}
function getCurrentNFLSeason() {
  const now = /* @__PURE__ */ new Date();
  const year = now.getFullYear();
  const month = now.getMonth() + 1;
  if (month >= 3) {
    return `${year}-${year + 1}`;
  }
  return `${year - 1}-${year}`;
}
async function syncNFLTeamsToDb() {
  const teams = await fetchNFLTeams();
  const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1e3);
  let synced = 0;
  for (const team of teams) {
    await prisma.sportsTeam.upsert({
      where: {
        sport_externalId_source: {
          sport: "NFL",
          externalId: team.id,
          source: "rolling_insights"
        }
      },
      update: {
        name: team.team,
        shortName: team.abbrv,
        city: team.team.replace(` ${team.mascot}`, ""),
        logo: team.img,
        fetchedAt: /* @__PURE__ */ new Date(),
        expiresAt
      },
      create: {
        sport: "NFL",
        externalId: team.id,
        name: team.team,
        shortName: team.abbrv,
        city: team.team.replace(` ${team.mascot}`, ""),
        logo: team.img,
        source: "rolling_insights",
        fetchedAt: /* @__PURE__ */ new Date(),
        expiresAt
      }
    });
    synced++;
  }
  return synced;
}
async function syncNFLPlayersToDb(options) {
  const season = options?.season || getCurrentNFLSeason();
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1e3);
  const teams = await fetchNFLTeams();
  let synced = 0;
  const TEAM_BATCH_SIZE = 4;
  for (let i = 0; i < teams.length; i += TEAM_BATCH_SIZE) {
    const batch = teams.slice(i, i + TEAM_BATCH_SIZE);
    const results = await Promise.allSettled(
      batch.map(async (team) => {
        let teamSynced = 0;
        try {
          const players = await fetchNFLRoster({ season, teamId: team.id });
          for (const player of players) {
            await prisma.sportsPlayer.upsert({
              where: {
                sport_externalId_source: {
                  sport: "NFL",
                  externalId: player.id,
                  source: "rolling_insights"
                }
              },
              update: {
                name: player.player,
                position: player.position,
                team: normalizeTeamAbbrev(player.team?.abbrv) || null,
                teamId: player.team?.id || null,
                number: player.number,
                height: player.height,
                weight: player.weight ? String(player.weight) : null,
                college: player.college,
                imageUrl: player.img || null,
                dob: player.dob || null,
                status: player.status || null,
                fetchedAt: /* @__PURE__ */ new Date(),
                expiresAt
              },
              create: {
                sport: "NFL",
                externalId: player.id,
                name: player.player,
                position: player.position,
                team: normalizeTeamAbbrev(player.team?.abbrv) || null,
                teamId: player.team?.id || null,
                number: player.number,
                height: player.height,
                weight: player.weight ? String(player.weight) : null,
                college: player.college,
                imageUrl: player.img || null,
                dob: player.dob || null,
                status: player.status || null,
                source: "rolling_insights",
                fetchedAt: /* @__PURE__ */ new Date(),
                expiresAt
              }
            });
            if (player.regularSeason?.length) {
              for (const stats of player.regularSeason) {
                await prisma.playerSeasonStats.upsert({
                  where: {
                    sport_playerId_season_seasonType_source: {
                      sport: "NFL",
                      playerId: player.id,
                      season: stats.period,
                      seasonType: "regular",
                      source: "rolling_insights"
                    }
                  },
                  update: {
                    playerName: player.player,
                    position: player.position,
                    team: player.team?.abbrv || null,
                    stats,
                    gamesPlayed: stats.games_played,
                    fantasyPoints: stats.DK_fantasy_points,
                    fantasyPointsPerGame: stats.DK_fantasy_points_per_game,
                    fetchedAt: /* @__PURE__ */ new Date(),
                    expiresAt
                  },
                  create: {
                    sport: "NFL",
                    playerId: player.id,
                    playerName: player.player,
                    season: stats.period,
                    seasonType: "regular",
                    position: player.position,
                    team: player.team?.abbrv || null,
                    stats,
                    gamesPlayed: stats.games_played,
                    fantasyPoints: stats.DK_fantasy_points,
                    fantasyPointsPerGame: stats.DK_fantasy_points_per_game,
                    source: "rolling_insights",
                    fetchedAt: /* @__PURE__ */ new Date(),
                    expiresAt
                  }
                });
              }
            }
            if (player.postSeason?.length) {
              for (const stats of player.postSeason) {
                await prisma.playerSeasonStats.upsert({
                  where: {
                    sport_playerId_season_seasonType_source: {
                      sport: "NFL",
                      playerId: player.id,
                      season: stats.period,
                      seasonType: "postseason",
                      source: "rolling_insights"
                    }
                  },
                  update: {
                    playerName: player.player,
                    position: player.position,
                    team: player.team?.abbrv || null,
                    stats,
                    gamesPlayed: stats.games_played,
                    fantasyPoints: stats.DK_fantasy_points,
                    fantasyPointsPerGame: stats.DK_fantasy_points_per_game,
                    fetchedAt: /* @__PURE__ */ new Date(),
                    expiresAt
                  },
                  create: {
                    sport: "NFL",
                    playerId: player.id,
                    playerName: player.player,
                    season: stats.period,
                    seasonType: "postseason",
                    position: player.position,
                    team: player.team?.abbrv || null,
                    stats,
                    gamesPlayed: stats.games_played,
                    fantasyPoints: stats.DK_fantasy_points,
                    fantasyPointsPerGame: stats.DK_fantasy_points_per_game,
                    source: "rolling_insights",
                    fetchedAt: /* @__PURE__ */ new Date(),
                    expiresAt
                  }
                });
              }
            }
            teamSynced++;
          }
        } catch (err) {
          console.error(`[RollingInsights] Failed to sync players for ${team.team}:`, err);
        }
        await recordProviderSync(
          { provider: "rolling_insights", entityType: "player", sport: "NFL", key: team.id, sourcePriority: 0 },
          { recordsImported: teamSynced }
        );
        return teamSynced;
      })
    );
    for (const result of results) {
      if (result.status === "fulfilled") synced += result.value;
    }
  }
  return synced;
}
async function syncNFLScheduleToDb(options) {
  const season = options?.season || getCurrentNFLSeason();
  const expiresAt = new Date(Date.now() + 12 * 60 * 60 * 1e3);
  const games = await fetchNFLSchedule({ season });
  let synced = 0;
  for (const game of games) {
    const period = {
      ...game.week != null ? { week: game.week } : {},
      ...game.seasonType ? { seasonType: game.seasonType } : {}
    };
    await prisma.sportsGame.upsert({
      where: {
        sport_externalId_source: {
          sport: "NFL",
          externalId: game.gameId,
          source: "rolling_insights"
        }
      },
      update: {
        ...period,
        homeTeam: game.homeTeam,
        awayTeam: game.awayTeam,
        status: game.status,
        startTime: game.date ? new Date(game.date) : null,
        venue: game.venue?.arena || null,
        season: parseInt(game.season.split("-")[0]),
        fetchedAt: /* @__PURE__ */ new Date(),
        expiresAt
      },
      create: {
        ...period,
        sport: "NFL",
        externalId: game.gameId,
        homeTeam: game.homeTeam,
        awayTeam: game.awayTeam,
        status: game.status,
        startTime: game.date ? new Date(game.date) : null,
        venue: game.venue?.arena || null,
        season: parseInt(game.season.split("-")[0]),
        source: "rolling_insights",
        fetchedAt: /* @__PURE__ */ new Date(),
        expiresAt
      }
    });
    synced++;
  }
  return synced;
}
function normalizeRIDepthChartPlayers(position, players) {
  if (!Array.isArray(players)) return [];
  const normalized = [];
  for (const raw of players) {
    const obj = asObj(raw);
    if (!obj) continue;
    const id2 = asString(obj.id ?? obj.player_id ?? obj.playerId);
    const player = asString(obj.player ?? obj.name ?? obj.full_name);
    if (!id2 || !player) continue;
    normalized.push({
      id: id2,
      player,
      position: asString(obj.position) ?? position,
      number: asNumber3(obj.number),
      status: asString(obj.status),
      img: toImageUrl(obj.img)
    });
  }
  return normalized;
}
async function fetchNFLDepthCharts(options) {
  const args = [];
  if (options?.teamName) args.push(`teamName: "${options.teamName}"`);
  if (options?.season) args.push(`season: "${options.season}"`);
  const argsStr = args.length ? `(${args.join(", ")})` : "";
  const query = `{
    nflTeams${argsStr} {
      id team abbrv
      rosterByPosition {
        ${DEPTH_CHART_FIELDS}
      }
    }
  }`;
  const data = await graphqlQuery(query);
  return (data.nflTeams || []).flatMap((t) => {
    if (!t) return [];
    const positions = {};
    if (t.rosterByPosition) {
      for (const [pos, players] of Object.entries(t.rosterByPosition)) {
        const normalizedPlayers = normalizeRIDepthChartPlayers(pos, players);
        if (normalizedPlayers.length > 0) positions[pos] = normalizedPlayers;
      }
    }
    return [{ team: t.team, teamId: t.id, abbrv: t.abbrv, positions }];
  });
}
async function fetchNFLTeamsFull(options) {
  const args = [];
  if (options?.teamName) args.push(`teamName: "${options.teamName}"`);
  if (options?.season) args.push(`season: "${options.season}"`);
  const argsStr = args.length ? `(${args.join(", ")})` : "";
  const query = `{
    nflTeams${argsStr} {
      id team abbrv mascot img
      conf city state arena dome
      bye { period value }
      injuries { injury player returns playerId date }
      record { period regular { wins losses ties } wildcard { wins losses ties } }
      regularSeason { ${TEAM_SEASON_STATS_FIELDS} }
      postSeason { ${TEAM_SEASON_STATS_FIELDS} }
    }
  }`;
  const data = await graphqlQuery(query);
  return (data.nflTeams || []).map((t) => ({
    id: t.id,
    team: t.team,
    abbrv: t.abbrv,
    mascot: t.mascot,
    img: t.img,
    conf: t.conf ?? null,
    city: t.city ?? null,
    state: t.state ?? null,
    arena: t.arena ?? null,
    dome: t.dome ?? null,
    bye: t.bye || [],
    injuries: t.injuries || [],
    record: t.record || [],
    regularSeason: (t.regularSeason || []).map(mapTeamSeasonStats),
    postSeason: (t.postSeason || []).map(mapTeamSeasonStats)
  }));
}
function mapTeamSeasonStats(s) {
  return {
    period: s.period,
    wins: s.wins ?? null,
    losses: s.losses ?? null,
    ties: s.ties ?? null,
    points: s.points ?? null,
    totalYards: s.total_yards ?? null,
    passingYards: s.passing_yards ?? null,
    rushingYards: s.rushing_yards ?? null,
    turnovers: s.turnovers ?? null,
    sacks: s.sacks ?? null,
    firstDowns: s.first_downs ?? null,
    penalties: s.penalties ?? null,
    penaltyYards: s.penalty_yards ?? null,
    gamesPlayed: s.games_played ?? null,
    DK_fantasy_points: s.DK_fantasy_points ?? null,
    DK_fantasy_points_per_game: s.DK_fantasy_points_per_game ?? null,
    passingTouchdowns: s.passing_touchdowns ?? null,
    rushingTouchdowns: s.rushing_touchdowns ?? null,
    defenseTouchdowns: s.defense_touchdowns ?? null,
    defenseInterceptions: s.defense_interceptions ?? null,
    totalPassingYardsAllowed: s.total_passing_yards_allowed ?? null,
    totalRushingYardsAllowed: s.total_rushing_yards_allowed ?? null,
    pointsAgainstDST: s.points_against_defense_special_teams ?? null
  };
}
async function fetchNFLRosterEnhanced(options) {
  const args = [];
  if (options.season) args.push(`season: "${options.season}"`);
  if (options.playerName) args.push(`playerName: "${options.playerName}"`);
  if (options.teamId) args.push(`teamId: "${options.teamId}"`);
  if (options.limit) args.push(`limit: ${options.limit}`);
  const argsStr = args.length ? `(${args.join(", ")})` : "";
  const query = `{ nflRoster${argsStr} { ${PLAYER_ENHANCED_FIELDS} } }`;
  const data = await graphqlQuery(query);
  return data.nflRoster || [];
}
async function syncNFLDepthChartsToDb(options) {
  const season = options?.season || getCurrentNFLSeason();
  const expiresAt = new Date(Date.now() + 12 * 60 * 60 * 1e3);
  let charts;
  try {
    charts = await fetchNFLDepthCharts({ season });
  } catch (error) {
    console.error("[RollingInsights] Failed to fetch depth charts:", error);
    return 0;
  }
  let synced = 0;
  for (const chart of charts) {
    const team = normalizeTeamAbbrev(chart.abbrv) || chart.abbrv;
    for (const [position, players] of Object.entries(chart.positions)) {
      if (!players.length) continue;
      try {
        await prisma.depthChart.upsert({
          where: {
            sport_team_position_source: {
              sport: "NFL",
              team,
              position,
              source: "rolling_insights"
            }
          },
          update: {
            teamId: chart.teamId,
            players,
            season,
            fetchedAt: /* @__PURE__ */ new Date(),
            expiresAt
          },
          create: {
            sport: "NFL",
            team,
            teamId: chart.teamId,
            position,
            players,
            source: "rolling_insights",
            season,
            fetchedAt: /* @__PURE__ */ new Date(),
            expiresAt
          }
        });
        synced++;
      } catch (err) {
        console.error(`[RollingInsights] Failed to sync depth chart ${team}/${position}:`, err);
      }
    }
  }
  console.log(`[RollingInsights] Synced ${synced} depth chart entries`);
  return synced;
}
async function syncNFLTeamStatsToDb(options) {
  const season = options?.season || getCurrentNFLSeason();
  const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1e3);
  let teams;
  try {
    teams = await fetchNFLTeamsFull({ season });
  } catch (error) {
    console.error("[RollingInsights] Failed to fetch team stats:", error);
    return 0;
  }
  let synced = 0;
  for (const team of teams) {
    const abbrev = normalizeTeamAbbrev(team.abbrv) || team.abbrv;
    if (team.conf || team.dome !== null || team.arena) {
      try {
        await prisma.sportsTeam.updateMany({
          where: { sport: "NFL", shortName: abbrev, source: "rolling_insights" },
          data: {
            conference: team.conf || void 0
          }
        });
      } catch {
      }
    }
    const allStats = [
      ...team.regularSeason.map((s) => ({ ...s, type: "regular" })),
      ...team.postSeason.map((s) => ({ ...s, type: "postseason" }))
    ];
    for (const stats of allStats) {
      try {
        await prisma.teamSeasonStats.upsert({
          where: {
            sport_team_season_seasonType_source: {
              sport: "NFL",
              team: abbrev,
              season: stats.period,
              seasonType: stats.type,
              source: "rolling_insights"
            }
          },
          update: {
            teamId: team.id,
            stats,
            wins: stats.wins,
            losses: stats.losses,
            ties: stats.ties,
            pointsFor: stats.points,
            totalYards: stats.totalYards,
            passingYards: stats.passingYards,
            rushingYards: stats.rushingYards,
            turnovers: stats.turnovers,
            sacks: stats.sacks,
            fantasyPoints: stats.DK_fantasy_points,
            gamesPlayed: stats.gamesPlayed,
            fetchedAt: /* @__PURE__ */ new Date(),
            expiresAt
          },
          create: {
            sport: "NFL",
            team: abbrev,
            teamId: team.id,
            season: stats.period,
            seasonType: stats.type,
            stats,
            wins: stats.wins,
            losses: stats.losses,
            ties: stats.ties,
            pointsFor: stats.points,
            totalYards: stats.totalYards,
            passingYards: stats.passingYards,
            rushingYards: stats.rushingYards,
            turnovers: stats.turnovers,
            sacks: stats.sacks,
            fantasyPoints: stats.DK_fantasy_points,
            gamesPlayed: stats.gamesPlayed,
            source: "rolling_insights",
            fetchedAt: /* @__PURE__ */ new Date(),
            expiresAt
          }
        });
        synced++;
      } catch (err) {
        console.error(`[RollingInsights] Failed to sync team stats ${abbrev}/${stats.period}:`, err);
      }
    }
  }
  console.log(`[RollingInsights] Synced ${synced} team season stats entries`);
  return synced;
}
async function runRollingInsightsHealthCheck() {
  const config = getRollingInsightsConfigFromEnv();
  const enabledSports = getEnabledRollingInsightsSports();
  if (!config) {
    return {
      configured: false,
      available: false,
      enabledSports
    };
  }
  const startedAt = Date.now();
  try {
    await graphqlQuery(
      "query RollingInsightsHealth { __schema { queryType { name } } }"
    );
    return {
      configured: true,
      available: true,
      latencyMs: Date.now() - startedAt,
      authMode: config.authMode,
      enabledSports
    };
  } catch (error) {
    return {
      configured: true,
      available: false,
      latencyMs: Date.now() - startedAt,
      error: error instanceof Error ? error.message : String(error),
      authMode: config.authMode,
      enabledSports
    };
  }
}
async function fetchNFLPlayerStats(options) {
  const seasonRaw = options.season ?? String((/* @__PURE__ */ new Date()).getUTCFullYear());
  const seasonForPath = seasonRaw.includes("-") ? seasonRaw.split("-")[0] : seasonRaw;
  const result = await rollingInsightsProvider({
    sport: "NFL",
    dataType: "player_stats",
    query: { season: seasonForPath }
  });
  if (!Array.isArray(result.data)) return [];
  return result.data.filter((r) => {
    if (!r || typeof r !== "object") return false;
    const o = r;
    return o.player_id != null && typeof o.player === "string" && o.player.length > 0;
  });
}
var cachedToken, ROLLING_INSIGHTS_BASE_URL, AUTH_URL, GRAPHQL_URL, PLAYER_FIELDS, lastFetchNFLRosterTrace, DEPTH_CHART_POSITIONS, DEPTH_CHART_FIELDS, TEAM_SEASON_STATS_FIELDS, PLAYER_ENHANCED_FIELDS;
var init_rolling_insights2 = __esm({
  "lib/rolling-insights.ts"() {
    "use strict";
    init_prisma();
    init_team_abbrev();
    init_provider_sync_logger();
    init_provider_config();
    init_api_config();
    init_rolling_insights();
    init_imageUrl();
    init_riSeasonType();
    cachedToken = null;
    ROLLING_INSIGHTS_BASE_URL = process.env.ROLLING_INSIGHTS_BASE_URL?.trim().replace(/\/+$/, "") || process.env.ROLLING_INSIGHTS_API_BASE?.trim().replace(/\/+$/, "") || "https://datafeeds.rolling-insights.com";
    AUTH_URL = `${ROLLING_INSIGHTS_BASE_URL}/auth/token`;
    GRAPHQL_URL = `${ROLLING_INSIGHTS_BASE_URL}/graphql`;
    PLAYER_FIELDS = `
  id player
  team { id team abbrv mascot }
  number position height weight college dob img
  positionCategory status DK_salary
  regularSeason {
    period passing_yards passing_touchdowns passing_attempts completions
    interceptions rushing_yards rushing_touchdowns rushing_attempts
    receptions receiving_yards receiving_touchdowns targets sacks tackles
    fumbles fumbles_lost DK_fantasy_points DK_fantasy_points_per_game
    games_played snap_count_offense snap_count_defense
    field_goals_made field_goals_attempted extra_points_made extra_points_attempted
  }
  postSeason {
    period passing_yards passing_touchdowns passing_attempts completions
    interceptions rushing_yards rushing_touchdowns rushing_attempts
    receptions receiving_yards receiving_touchdowns targets sacks tackles
    fumbles fumbles_lost DK_fantasy_points DK_fantasy_points_per_game
    games_played snap_count_offense snap_count_defense
    field_goals_made field_goals_attempted extra_points_made extra_points_attempted
  }
`;
    lastFetchNFLRosterTrace = null;
    DEPTH_CHART_POSITIONS = [
      "QB",
      "RB",
      "WR",
      "WR1",
      "WR2",
      "WR3",
      "TE",
      "K",
      "P",
      "LT",
      "LG",
      "C",
      "RG",
      "RT",
      "FB",
      "DE",
      "DT",
      "LB",
      "CB",
      "S",
      "SS",
      "FS",
      "EDGE",
      "ILB",
      "OLB",
      "NT",
      "DL",
      "KR",
      "PR",
      "LS"
    ];
    DEPTH_CHART_FIELDS = DEPTH_CHART_POSITIONS.map(
      (pos) => `${pos} { id player position number status img }`
    ).join("\n    ");
    TEAM_SEASON_STATS_FIELDS = `
  period
  wins losses ties
  points total_yards passing_yards rushing_yards turnovers sacks
  first_downs penalties penalty_yards games_played
  DK_fantasy_points DK_fantasy_points_per_game
  passing_touchdowns rushing_touchdowns defense_touchdowns
  defense_interceptions total_passing_yards_allowed total_rushing_yards_allowed
  points_against_defense_special_teams
  offense_touchdowns receiving_touchdowns special_team_touchdowns
  safeties blocked_kicks blocked_punts
  kick_return_touchdowns punt_return_touchdowns
  interception_touchdowns fumble_return_touchdowns defense_fumble_recoveries
`;
    PLAYER_ENHANCED_FIELDS = `
  id player
  team { id team abbrv mascot }
  number position height weight college dob img
  positionCategory status DK_salary
  injury { injury player returns date }
  formerTeams { period teamNames }
  allStar { period count }
  regularSeason {
    period passing_yards passing_touchdowns passing_attempts completions
    interceptions rushing_yards rushing_touchdowns rushing_attempts
    receptions receiving_yards receiving_touchdowns targets sacks tackles
    fumbles fumbles_lost DK_fantasy_points DK_fantasy_points_per_game
    games_played snap_count_offense snap_count_defense snap_count_special_teams
    field_goals_made field_goals_attempted extra_points_made extra_points_attempted
    punts punts_long punting_yards inside_20
    punt_returns punt_return_yards punt_return_touchdowns
    kick_return_touchdowns
  }
  postSeason {
    period passing_yards passing_touchdowns passing_attempts completions
    interceptions rushing_yards rushing_touchdowns rushing_attempts
    receptions receiving_yards receiving_touchdowns targets sacks tackles
    fumbles fumbles_lost DK_fantasy_points DK_fantasy_points_per_game
    games_played snap_count_offense snap_count_defense snap_count_special_teams
    field_goals_made field_goals_attempted extra_points_made extra_points_attempted
    punts punts_long punting_yards inside_20
    punt_returns punt_return_yards punt_return_touchdowns
    kick_return_touchdowns
  }
`;
  }
});

// lib/sports-data/rollingInsightsGameLogs.ts
var rollingInsightsGameLogs_exports = {};
__export(rollingInsightsGameLogs_exports, {
  dateRange: () => dateRange,
  ingestRollingInsightsGameLogs: () => ingestRollingInsightsGameLogs,
  normalizeRiGameBox: () => normalizeRiGameBox,
  recentDates: () => recentDates
});
function seasonStartYearLocal(v) {
  if (typeof v === "number") return Number.isFinite(v) ? v : null;
  const m = /^(\d{4})/.exec(String(v ?? "").trim());
  return m ? Number(m[1]) : null;
}
function asRecord(v) {
  return v && typeof v === "object" && !Array.isArray(v) ? v : null;
}
function teamCode(raw) {
  const s = str2(raw);
  if (!s) return null;
  return s.length <= TEAM_CODE_MAX ? s : s.slice(0, TEAM_CODE_MAX);
}
function normalizeRiGameBox(game) {
  const g4 = asRecord(game);
  if (!g4) return null;
  const providerGameId = str2(g4.game_ID ?? g4.game_id ?? g4.gameId);
  if (!providerGameId) return null;
  const fullBox = asRecord(g4.full_box);
  const playerBox = asRecord(g4.player_box) ?? asRecord(fullBox?.player_box);
  const sides = [
    { key: "home_team", other: "away_team" },
    { key: "away_team", other: "home_team" }
  ];
  const lines = [];
  for (const side of sides) {
    const teamNode = asRecord(fullBox?.[side.key]);
    const otherNode = asRecord(fullBox?.[side.other]);
    const team = teamCode(teamNode?.abbrv ?? teamNode?.abbreviation) ?? teamCode(g4[`${side.key}_name`]);
    const opponent = teamCode(otherNode?.abbrv ?? otherNode?.abbreviation) ?? teamCode(g4[`${side.other}_name`]);
    const sideBox = asRecord(playerBox?.[side.key]) ?? asRecord(teamNode?.player_box);
    if (!sideBox) continue;
    for (const [group, bucket] of Object.entries(sideBox)) {
      if (Array.isArray(bucket)) {
        for (const entry of bucket) {
          const p = asRecord(entry);
          if (!p) continue;
          const providerPlayerId = str2(p.player_id ?? p.playerId ?? p.player_ID);
          if (!providerPlayerId) continue;
          lines.push({
            providerPlayerId,
            playerName: str2(p.player ?? p.name),
            group,
            team,
            opponent,
            raw: p
          });
        }
        continue;
      }
      const byId = asRecord(bucket);
      if (!byId) continue;
      const looksGrouped = Object.values(byId).some((v) => asRecord(v) !== null);
      if (!looksGrouped) {
        const providerPlayerId = str2(byId.player_id ?? byId.playerId ?? byId.player_ID) ?? str2(group);
        if (!providerPlayerId) continue;
        lines.push({
          providerPlayerId,
          playerName: str2(byId.player ?? byId.name),
          group: "all",
          team,
          opponent,
          raw: { player_id: providerPlayerId, ...byId }
        });
        continue;
      }
      for (const [playerId, entry] of Object.entries(byId)) {
        const p = asRecord(entry);
        if (!p) continue;
        const providerPlayerId = str2(p.player_id ?? p.playerId ?? p.player_ID) ?? str2(playerId);
        if (!providerPlayerId) continue;
        lines.push({
          providerPlayerId,
          playerName: str2(p.player ?? p.name),
          group,
          team,
          opponent,
          // The id is the KEY, so it is folded into the stored payload — otherwise the row's own
          // provenance would be missing from `statPayload`.
          raw: { player_id: providerPlayerId, ...p }
        });
      }
    }
  }
  const gameTime = str2(g4.game_time ?? g4.gameTime);
  const parsedDate = gameTime ? new Date(gameTime) : null;
  return {
    providerGameId,
    // `/live` returns a hyphenated SPAN for sports whose season crosses a calendar year
    // ("2025-2026" for NBA and NHL, measured 2026-03-15). `num()` gives NaN -> null on
    // those, so this field was silently null for exactly the sports being added and the
    // caller fell back to a guessed season. The vendor contract settles the rule:
    // `season: { format: "YYYY", note: "Year season started." }`.
    // ⚠ DUPLICATES `lib/sports-data/seasonLabel.ts`, which is not on main yet (p1b, queued).
    //   Collapse into that helper once it lands — two implementations of one rule is the bug.
    season: seasonStartYearLocal(g4.season),
    // Football carries a real week; the daily sports do not, and 0 is this column's documented
    // "no week" value rather than a made-up round number.
    weekOrRound: num(g4.week) ?? 0,
    gameDate: parsedDate && !Number.isNaN(parsedDate.getTime()) ? parsedDate : null,
    // The vendor prints the Eastern day into game_ID; the kickoff's Eastern date is the fallback.
    gameDay: gameDayFromRiGameId(providerGameId) ?? easternCalendarDay(parsedDate && !Number.isNaN(parsedDate.getTime()) ? parsedDate : null),
    status: str2(g4.status ?? g4.game_status),
    // Preseason, regular season and playoffs all arrive through the same `/live` call. Dropping
    // this is how 619 NHL preseason rows landed in season 2026 unmarked — see riSeasonType.ts.
    seasonType: classifyRiSeasonType(g4.season_type ?? g4.seasonType),
    seasonTypeLabel: str2(g4.season_type ?? g4.seasonType),
    lines
  };
}
function normalizedMapFor(line, box) {
  const stats = {};
  for (const [key, value] of Object.entries(line.raw)) {
    const n = num(value);
    if (n != null) stats[key] = n;
  }
  return {
    group: line.group,
    gameStartTimeUtc: box.gameDate?.toISOString() ?? null,
    timeOnIce: str2(line.raw.time_on_ice) ?? null,
    position: str2(line.raw.position ?? line.raw.POS) ?? null,
    positionCategory: str2(line.raw.position_category) ?? null,
    status: str2(line.raw.status) ?? null,
    /** `regular` | `pre` | `post` | null. Readers resolve it via `resolveStoredSeasonType`. */
    seasonType: box.seasonType,
    seasonTypeLabel: box.seasonTypeLabel,
    stats,
    /**
     * ⚠ READ THIS BEFORE READING `fantasyPoints`. False means the row was never scored, and the
     * 0 in that column is the schema default, not a measurement. See the module header.
     */
    scored: false
  };
}
function leaguesFor(sport) {
  return getRollingInsightsSportCode(sport) === "SOCCER" ? [...RI_SOCCER_LEAGUES] : [void 0];
}
function dateRange(from, to) {
  const out = [];
  const start = /* @__PURE__ */ new Date(`${from}T00:00:00Z`);
  const end = /* @__PURE__ */ new Date(`${to}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return out;
  for (let d = start; d <= end; d = new Date(d.getTime() + 864e5)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}
async function ingestRollingInsightsGameLogs(opts) {
  const sport = opts.sport.trim().toUpperCase();
  const vendorCode = getRollingInsightsSportCode(sport);
  const result = {
    sport,
    datesRequested: opts.dates.length,
    datesFetched: 0,
    games: 0,
    lines: 0,
    written: 0,
    unresolved: 0,
    unrecognizedSeasonTypes: [],
    notModifiedDates: [],
    unsupported: false,
    errors: []
  };
  if (!riSupports("live", sport)) {
    result.unsupported = true;
    result.errors.push(`Rolling Insights documents no live feed for ${sport}`);
    return result;
  }
  const identityByRiId = /* @__PURE__ */ new Map();
  try {
    const rows = await prisma.playerIdentityMap.findMany({
      where: { sport, rollingInsightsId: { not: null } },
      select: { id: true, rollingInsightsId: true }
    });
    for (const r of rows) {
      if (r.rollingInsightsId) identityByRiId.set(r.rollingInsightsId, r.id);
    }
  } catch (e) {
    result.errors.push(`identity map load failed: ${e instanceof Error ? e.message : String(e)}`);
    return result;
  }
  if (identityByRiId.size === 0) {
    result.errors.push(
      "identity map has no rollingInsightsId rows for this sport \u2014 refusing to write provider-keyed game logs"
    );
    return result;
  }
  const now = opts.now ?? /* @__PURE__ */ new Date();
  const confidence = FIELD_CONFIDENCE[vendorCode] ?? 0.1;
  const fallbackSeason = now.getUTCMonth() + 1 >= 8 ? now.getUTCFullYear() : now.getUTCFullYear() - 1;
  for (const date of opts.dates) {
    if (opts.shouldStop?.()) break;
    for (const league of leaguesFor(sport)) {
      const { rows, notModified, error } = await riFetchRows("live", {
        sport,
        date,
        league,
        fetchImpl: opts.fetchImpl
      });
      if (notModified) {
        result.notModifiedDates.push(league ? `${date}:${league}` : date);
        continue;
      }
      if (error) {
        result.errors.push(`live ${date}${league ? `/${league}` : ""}: ${error}`);
        continue;
      }
      result.datesFetched += 1;
      for (const rawGame of rows) {
        const box = normalizeRiGameBox(rawGame);
        if (!box) continue;
        result.games += 1;
        result.lines += box.lines.length;
        if (box.seasonType === null && result.unrecognizedSeasonTypes.length < 10 && !result.unrecognizedSeasonTypes.includes(box.seasonTypeLabel ?? "(missing)")) {
          result.unrecognizedSeasonTypes.push(box.seasonTypeLabel ?? "(missing)");
        }
        let gameWritten = 0;
        const proof = certifyFinalRiBox(sport, rawGame, box, identityByRiId);
        try {
          await prisma.$transaction(async (tx) => {
            await tx.$executeRaw`SELECT pg_advisory_xact_lock(hashtextextended(${`${sport}:${box.providerGameId}`}, 0))`;
            if (!proof) await tx.$executeRaw`DELETE FROM sports_stat_coverage WHERE sport = ${sport} AND source = ${SOURCE} AND "periodId" = ${box.providerGameId}`;
            if (opts.persistSchedule && box.gameDate && box.seasonType) {
              const full = asRecord(asRecord(rawGame)?.full_box);
              const home = asRecord(full?.home_team), away = asRecord(full?.away_team);
              const homeTeam = str2(home?.abbrv), awayTeam = str2(away?.abbrv);
              if (!homeTeam || !awayTeam) throw new Error("LIVE_GAME_TEAM_IDENTITY_MISSING");
              const data = {
                homeTeam,
                awayTeam,
                homeScore: num(home?.score),
                awayScore: num(away?.score),
                status: box.status,
                startTime: box.gameDate,
                season: box.season ?? fallbackSeason,
                seasonType: box.seasonType === "regular" ? "regular" : box.seasonType,
                fetchedAt: now,
                expiresAt: new Date(now.getTime() + 12e4)
              };
              const scheduleSource = "rolling_insights";
              await tx.sportsGame.upsert({
                where: { sport_externalId_source: { sport, externalId: box.providerGameId, source: scheduleSource } },
                create: { sport, externalId: box.providerGameId, source: scheduleSource, ...data },
                update: data
              });
            }
            for (const line of box.lines) {
              const playerId = identityByRiId.get(line.providerPlayerId);
              if (!playerId) {
                result.unresolved += 1;
                continue;
              }
              const gameId = line.group === "all" ? box.providerGameId : `${box.providerGameId}:${line.group}`;
              const data = {
                providerPlayerId: line.providerPlayerId,
                providerGameId: box.providerGameId,
                season: box.season ?? fallbackSeason,
                weekOrRound: box.weekOrRound,
                statPayload: toPrismaJsonInput(line.raw),
                normalizedStatMap: toPrismaJsonInput(normalizedMapFor(line, box)),
                source: SOURCE,
                confidence,
                team: line.team,
                opponent: line.opponent,
                // The DAY, not the instant: `game_date` is a DATE, and the instant's UTC date is
                // tomorrow for every US night game (easternGameDay.ts has the measured damage).
                gameDate: box.gameDay,
                fetchedAt: now
              };
              await tx.playerGameStat.upsert({
                where: { playerId_sportType_gameId: { playerId, sportType: sport, gameId } },
                update: data,
                create: { playerId, sportType: sport, gameId, ...data }
              });
              gameWritten += 1;
            }
            if (proof) await writeStatCoverage(tx, dailyBoxCoverage(proof));
          }, { timeout: 3e4 });
          result.written += gameWritten;
        } catch (e) {
          if (result.errors.length < 10) result.errors.push(
            `game snapshot ${box.providerGameId} rolled back: ${e instanceof Error ? e.name : "write failed"}`
          );
        }
      }
    }
  }
  return result;
}
function recentDates(days, now = /* @__PURE__ */ new Date()) {
  const out = [];
  for (let i = 1; i <= days; i += 1) {
    out.push(new Date(now.getTime() - i * 864e5).toISOString().slice(0, 10));
  }
  return out;
}
var import_server_only15, SOURCE, FIELD_CONFIDENCE, TEAM_CODE_MAX, str2, num;
var init_rollingInsightsGameLogs = __esm({
  "lib/sports-data/rollingInsightsGameLogs.ts"() {
    "use strict";
    import_server_only15 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_prisma_json();
    init_rollingInsightsRest();
    init_rollingInsightsSupport();
    init_rollingInsightsFieldMaps();
    init_riSeasonType();
    init_easternGameDay();
    init_gameBoxCoverage();
    init_statCoverage();
    SOURCE = "rolling_insights_live";
    FIELD_CONFIDENCE = {
      NFL: 0.9,
      NBA: 0.4,
      MLB: 0.4,
      SOCCER: 0.2,
      NHL: 0.1,
      NCAAFB: 0.1,
      NCAABB: 0.1
    };
    TEAM_CODE_MAX = 8;
    str2 = (v) => {
      if (v == null) return null;
      const t = String(v).trim();
      return t.length > 0 && t.toLowerCase() !== "null" ? t : null;
    };
    num = (v) => {
      if (typeof v === "number") return Number.isFinite(v) ? v : null;
      if (typeof v === "string" && v.trim() !== "") {
        const n = Number(v);
        return Number.isFinite(n) ? n : null;
      }
      return null;
    };
  }
});

// lib/sports-reporting/riLiveGameLogAdapter.ts
async function fetchRiLiveGameLogRows(input) {
  const rows = [];
  const errors = [];
  const warnings = [];
  const dates = input.dates ?? recentDates(input.days ?? RI_LIVE_LOOKBACK_DAYS, input.now);
  const requested = requestedSeasonType(input.seasonType);
  let skippedOtherSeasonType = 0;
  for (const date of dates) {
    const out = await riFetchRows("live", { sport: input.sport, date });
    if (out.unsupported) {
      warnings.push(`${input.sport}: /live is not supported for this sport.`);
      break;
    }
    if (out.notModified) {
      warnings.push(`${input.sport} ${date}: unchanged/unknown (304 after retry) \u2014 left alone.`);
      continue;
    }
    if (out.error) {
      errors.push(`${input.sport} ${date}: ${out.error}`);
      continue;
    }
    for (const game of out.rows) {
      const box = normalizeRiGameBox(game);
      if (!box) continue;
      if (box.seasonType && requested && box.seasonType !== requested) {
        skippedOtherSeasonType += 1;
        continue;
      }
      for (const line of box.lines) {
        rows.push({
          provider: "rolling_insights",
          sport: input.sport,
          providerPlayerId: line.providerPlayerId,
          playerName: line.playerName,
          team: line.team,
          opponent: line.opponent,
          gameId: box.providerGameId,
          week: box.weekOrRound,
          // `normalizeRiGameBox` reduces the provider's hyphenated span ("2025-2026") to a
          // start year. It did NOT before this change — `num()` gave null on a span, so the
          // season was silently absent for exactly the two sports being added here.
          season: box.season == null ? "" : String(box.season),
          seasonType: input.seasonType,
          playedAt: box.gameDate ? box.gameDate.toISOString() : null,
          status: box.status,
          stats: line.raw,
          raw: line.raw
        });
      }
    }
  }
  if (skippedOtherSeasonType > 0) {
    warnings.push(
      `${input.sport}: skipped ${skippedOtherSeasonType} game(s) whose season_type is not ${input.seasonType}.`
    );
  }
  if (!rows.length && !errors.length && !warnings.length) {
    warnings.push(
      `${input.sport}: no games found across ${dates.length} date(s) \u2014 expected out of season.`
    );
  }
  return { rows, errors, warnings };
}
function requestedSeasonType(value) {
  const v = value.trim().toLowerCase();
  if (v === "regular") return "regular";
  if (v === "postseason" || v === "post" || v === "playoffs") return "post";
  if (v === "preseason" || v === "pre") return "pre";
  return null;
}
var import_server_only16, RI_LIVE_GAME_LOG_SPORTS, RI_LIVE_LOOKBACK_DAYS;
var init_riLiveGameLogAdapter = __esm({
  "lib/sports-reporting/riLiveGameLogAdapter.ts"() {
    "use strict";
    import_server_only16 = __toESM(require_audit_server_only_shim());
    init_rollingInsightsRest();
    init_rollingInsightsGameLogs();
    RI_LIVE_GAME_LOG_SPORTS = /* @__PURE__ */ new Set([
      "NBA",
      "NHL",
      "MLB",
      "NCAAB"
    ]);
    RI_LIVE_LOOKBACK_DAYS = 3;
  }
});

// lib/sports-reporting/PlayerGameLogImportService.ts
var PlayerGameLogImportService_exports = {};
__export(PlayerGameLogImportService_exports, {
  getPlayerGameLogHealthDashboard: () => getPlayerGameLogHealthDashboard,
  importPlayerGameLogs: () => importPlayerGameLogs,
  mergeGameLogPayload: () => mergeGameLogPayload,
  resolveProviderPlayerIdentity: () => resolveProviderPlayerIdentity,
  resolveProviderTeamIdentity: () => resolveProviderTeamIdentity
});
function isRecord4(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function asNumber4(value) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}
function normalizeSport(value) {
  const raw = String(value ?? "NFL").trim().toUpperCase().replace(/-/g, "_");
  if (raw === "WORLD_CUP" || raw === "WORLDCUP") return "WC_SOCCER";
  if (raw === "SOCCER_EURO" || raw === "SOCCER_MLS") return "SOCCER";
  return SPORTS.includes(raw) ? raw : "NFL";
}
function normalizeProvider2(value, sport) {
  const raw = String(value ?? "").trim().toLowerCase().replace(/-/g, "_");
  if (raw === "rollinginsights") return "rolling_insights";
  if (raw === "api_sports" || raw === "apisports") return "api_sports";
  if (raw === "api_football" || raw === "apifootball") return "api_football";
  if (raw === "clear_sports" || raw === "clearsports") return "clearsports";
  if (raw === "the_sports_db" || raw === "sportsdb" || raw === "thesportsdb") return "thesportsdb";
  if (raw === "cfbd") return "cfbd";
  if (raw === "espn") return "espn";
  if (raw === "cache") return "cache";
  if (raw === "sleeper") return "sleeper";
  if (sport === "NFL") return "sleeper";
  if (sport === "NCAAF") return "cache";
  if (RI_LIVE_GAME_LOG_SPORTS.has(sport)) return "rolling_insights";
  return "cache";
}
function toSeason2(value) {
  const raw = String(value ?? (/* @__PURE__ */ new Date()).getUTCFullYear()).trim();
  return /^\d{4}$/.test(raw) ? raw : String((/* @__PURE__ */ new Date()).getUTCFullYear());
}
function toSeasonType(value) {
  const raw = String(value ?? "regular").trim().toLowerCase().replace(/\s+/g, "_");
  if (raw === "postseason" || raw === "playoffs" || raw === "post") return "postseason";
  return raw || "regular";
}
function toWeeks(value) {
  const raw = Array.isArray(value) ? value : value == null ? [] : String(value).split(",");
  return Array.from(
    new Set(
      raw.map((week) => Number(week)).filter((week) => Number.isFinite(week) && week > 0).map((week) => Math.floor(week))
    )
  ).sort((a, b) => a - b);
}
function clampLimit(value) {
  const parsed = Number(value ?? DEFAULT_IMPORT_LIMIT);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_IMPORT_LIMIT;
  return Math.min(MAX_IMPORT_LIMIT, Math.floor(parsed));
}
function redactError(error) {
  return redactAndCap(error, 240);
}
function payloadLogs(payload) {
  if (Array.isArray(payload)) return payload.filter(isRecord4);
  if (!isRecord4(payload)) return [];
  for (const key of ["gameLogs", "weeklyStats", "weeks", "logs"]) {
    const value = payload[key];
    if (Array.isArray(value)) return value.filter(isRecord4);
  }
  return [];
}
function entryKey(row) {
  return `${row.week}:${row.gameId ?? "week"}:${row.sourceProvider}`;
}
function mergeGameLogPayload(input) {
  const importedAt = input.importedAt ?? /* @__PURE__ */ new Date();
  const map = /* @__PURE__ */ new Map();
  for (const row of payloadLogs(input.existingPayload)) map.set(entryKey(row), row);
  let imported = 0;
  let updated = 0;
  let duplicates = 0;
  let unchanged = 0;
  const seenIncoming = /* @__PURE__ */ new Set();
  for (const row of input.incomingRows) {
    const key = entryKey(row);
    if (seenIncoming.has(key)) {
      duplicates += 1;
      continue;
    }
    seenIncoming.add(key);
    const current = map.get(key);
    if (!current) {
      imported += 1;
      map.set(key, row);
      continue;
    }
    if (JSON.stringify(current.stats) !== JSON.stringify(row.stats) || JSON.stringify(current.raw) !== JSON.stringify(row.raw)) {
      updated += 1;
      map.set(key, row);
    } else {
      unchanged += 1;
    }
  }
  const gameLogs = Array.from(map.values()).sort((a, b) => a.week - b.week);
  return {
    payload: {
      version: 1,
      sport: input.sport,
      season: input.season,
      seasonType: input.seasonType,
      provider: input.provider,
      importedAt: importedAt.toISOString(),
      gameLogs
    },
    imported,
    updated,
    duplicates,
    unchanged
  };
}
function normalizeStats(source) {
  const stats = isRecord4(source.stats) ? source.stats : source;
  const out = {};
  for (const [key, value] of Object.entries(stats)) {
    if (value == null) continue;
    if (typeof value === "number" && Number.isFinite(value)) out[key] = value;
    else if (typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value))) out[key] = Number(value);
  }
  return out;
}
function weekFromRow(row, fallback) {
  return asNumber4(row.week) ?? asNumber4(row.weekNumber) ?? asNumber4(row.gameWeek) ?? fallback ?? null;
}
function normalizeProviderRows(input) {
  const rows = [];
  const rawRows = Array.isArray(input.rawPayload) ? input.rawPayload : isRecord4(input.rawPayload) ? Object.entries(input.rawPayload).map(([week, value]) => ({ week, ...isRecord4(value) ? value : { stats: value } })) : [];
  for (const raw of rawRows) {
    if (!isRecord4(raw)) continue;
    const stats = normalizeStats(raw);
    const week = weekFromRow(raw);
    if (!week || Object.keys(stats).length === 0) continue;
    rows.push({
      provider: input.provider,
      sport: input.sport,
      providerPlayerId: input.providerPlayerId,
      playerName: input.playerName,
      team: normalizeTeamAbbrev(input.team) ?? input.team ?? null,
      opponent: typeof raw.opponent === "string" ? raw.opponent : null,
      gameId: String(raw.game_id ?? raw.gameId ?? raw.eventId ?? "").trim() || null,
      week,
      season: input.season,
      seasonType: input.seasonType,
      playedAt: typeof raw.date === "string" ? raw.date : typeof raw.playedAt === "string" ? raw.playedAt : null,
      status: typeof raw.status === "string" ? raw.status : null,
      stats,
      raw
    });
  }
  return rows;
}
async function findTargetMetadata(providerPlayerId, sport) {
  const [identity, sportsPlayer] = await Promise.all([
    prisma.playerIdentityMap.findFirst({
      where: {
        sport,
        OR: [
          { sleeperId: providerPlayerId },
          { apiSportsId: providerPlayerId },
          { rollingInsightsId: providerPlayerId },
          { espnId: providerPlayerId },
          { clearSportsId: providerPlayerId }
        ]
      },
      select: { canonicalName: true, currentTeam: true }
    }).catch(() => null),
    prisma.sportsPlayer.findFirst({
      where: { sport, externalId: providerPlayerId },
      select: { name: true, team: true }
    }).catch(() => null)
  ]);
  return {
    playerName: identity?.canonicalName ?? sportsPlayer?.name ?? null,
    team: normalizeTeamAbbrev(identity?.currentTeam ?? sportsPlayer?.team) ?? identity?.currentTeam ?? sportsPlayer?.team ?? null
  };
}
async function resolveImportTargets(input) {
  if (input.playerIds.length > 0) {
    const rows = await Promise.all(
      input.playerIds.slice(0, input.limit).map(async (providerPlayerId) => ({
        providerPlayerId,
        ...await findTargetMetadata(providerPlayerId, input.sport)
      }))
    );
    return rows;
  }
  if (input.seasonId || input.leagueId) {
    const season = await prisma.redraftSeason.findFirst({
      where: input.seasonId ? { id: input.seasonId } : { leagueId: input.leagueId ?? void 0 },
      orderBy: input.seasonId ? void 0 : { createdAt: "desc" },
      select: { id: true, leagueId: true }
    });
    if (!season) return [];
    const rosters = await prisma.redraftRoster.findMany({
      where: { seasonId: season.id, leagueId: season.leagueId },
      select: { id: true }
    });
    const rosterIds = rosters.map((row) => row.id);
    const players = rosterIds.length ? await prisma.redraftRosterPlayer.findMany({
      where: { rosterId: { in: rosterIds }, droppedAt: null },
      select: { playerId: true, playerName: true, team: true },
      take: input.limit
    }) : [];
    return Array.from(new Map(players.map((row) => [row.playerId, row])).values()).map((row) => ({
      providerPlayerId: row.playerId,
      playerName: row.playerName,
      team: row.team
    }));
  }
  const providerField = PROVIDER_ID_FIELD[input.provider] ?? (input.sport === "NFL" ? "sleeperId" : null);
  if (providerField) {
    const rows = await prisma.playerIdentityMap.findMany({
      where: {
        sport: input.sport,
        [providerField]: { not: null }
      },
      select: {
        canonicalName: true,
        currentTeam: true,
        [providerField]: true
      },
      take: input.limit
    });
    return rows.flatMap((row) => {
      const id2 = String(row[providerField] ?? "").trim();
      return id2 ? [{
        providerPlayerId: id2,
        playerName: row.canonicalName,
        team: normalizeTeamAbbrev(row.currentTeam) ?? row.currentTeam
      }] : [];
    });
  }
  return [];
}
async function resolveProviderPlayerIdentity(input) {
  const providerField = PROVIDER_ID_FIELD[input.provider];
  if (providerField && input.providerPlayerId) {
    const exact = await prisma.playerIdentityMap.findFirst({
      where: { sport: input.sport, [providerField]: input.providerPlayerId },
      select: { id: true, sleeperId: true, canonicalName: true }
    });
    if (exact) {
      return {
        ok: true,
        cachePlayerId: exact.sleeperId ?? input.providerPlayerId,
        canonicalIdentityId: exact.id,
        matchType: "provider_id"
      };
    }
  }
  const sportsPlayer = await prisma.sportsPlayer.findFirst({
    where: {
      sport: input.sport,
      externalId: input.providerPlayerId,
      source: { equals: input.provider, mode: "insensitive" }
    },
    select: { id: true, externalId: true }
  }).catch(() => null);
  if (sportsPlayer) {
    return {
      ok: true,
      cachePlayerId: sportsPlayer.externalId,
      canonicalIdentityId: null,
      matchType: "sports_player"
    };
  }
  const normalizedName = normalizePlayerName(input.playerName ?? "");
  if (!normalizedName) return { ok: false, reason: "unmapped" };
  const nameMatches = await prisma.playerIdentityMap.findMany({
    where: {
      sport: input.sport,
      normalizedName,
      ...input.team ? { currentTeam: { equals: input.team, mode: "insensitive" } } : {}
    },
    select: { id: true, sleeperId: true, canonicalName: true },
    take: 3
  });
  if (nameMatches.length === 1) {
    const match = nameMatches[0];
    return {
      ok: true,
      cachePlayerId: match.sleeperId ?? input.providerPlayerId,
      canonicalIdentityId: match.id,
      matchType: "normalized_name"
    };
  }
  if (nameMatches.length > 1) {
    return { ok: false, reason: "ambiguous", candidates: nameMatches.map((row) => row.canonicalName) };
  }
  return { ok: false, reason: "unmapped" };
}
async function resolveProviderTeamIdentity(input) {
  const normalizedTeam = normalizeTeamAbbrev(input.team) ?? input.team?.trim() ?? "";
  if (!input.providerTeamId && !normalizedTeam) return { ok: false, reason: "unmapped" };
  if (input.providerTeamId) {
    const team = await prisma.sportsTeam.findFirst({
      where: {
        sport: input.sport,
        externalId: input.providerTeamId,
        source: { equals: input.provider, mode: "insensitive" }
      },
      select: { id: true, shortName: true, name: true }
    }).catch(() => null);
    if (team) {
      return {
        ok: true,
        teamCode: normalizeTeamAbbrev(team.shortName ?? team.name) ?? team.shortName ?? team.name,
        teamId: team.id,
        matchType: "provider_id"
      };
    }
  }
  if (!normalizedTeam) return { ok: false, reason: "unmapped" };
  const matches2 = await prisma.sportsTeam.findMany({
    where: {
      sport: input.sport,
      OR: [
        { shortName: { equals: normalizedTeam, mode: "insensitive" } },
        { name: { equals: normalizedTeam, mode: "insensitive" } },
        { externalId: { equals: normalizedTeam, mode: "insensitive" } }
      ]
    },
    select: { id: true, shortName: true, name: true },
    take: 3
  }).catch(() => []);
  if (matches2.length === 1) {
    const match = matches2[0];
    return {
      ok: true,
      teamCode: normalizeTeamAbbrev(match.shortName ?? match.name) ?? match.shortName ?? match.name,
      teamId: match.id,
      matchType: match.shortName?.toLowerCase() === normalizedTeam.toLowerCase() ? "team_code" : "team_name"
    };
  }
  if (matches2.length > 1) return { ok: false, reason: "ambiguous", candidates: matches2.map((row) => row.name) };
  return { ok: false, reason: "unmapped" };
}
async function fetchSleeperRawLogs(input) {
  const endpoint = "player-game-logs:nfl:sleeper";
  const canCall = await rateLimitManager.canCall("sleeper", endpoint);
  if (!canCall) return { payload: null, error: "Sleeper call budget exhausted for NFL game log import." };
  const url = `https://api.sleeper.com/stats/nfl/player/${encodeURIComponent(input.playerId)}?season_type=${encodeURIComponent(input.seasonType)}&season=${encodeURIComponent(input.season)}&grouping=week`;
  const startedAt = Date.now();
  try {
    const res = await fetch(url, { cache: "no-store", signal: AbortSignal.timeout(1e4) });
    const latency = Math.max(0, Date.now() - startedAt);
    await rateLimitManager.recordCall("sleeper", endpoint, res.status, latency, { cached: false, error: res.ok ? null : `HTTP ${res.status}` });
    if (!res.ok) return { payload: null, error: `Sleeper returned HTTP ${res.status} for player ${input.playerId}.` };
    return { payload: await res.json(), error: null };
  } catch (error) {
    await rateLimitManager.recordCall("sleeper", endpoint, 0, Math.max(0, Date.now() - startedAt), {
      cached: false,
      error: redactError(error)
    });
    return { payload: null, error: redactError(error) };
  }
}
function buildSleeperNflAdapter() {
  return {
    provider: "sleeper",
    sport: "NFL",
    status: "real",
    async fetchRows(input) {
      const rows = [];
      const errors = [];
      for (const target of input.targets) {
        const { payload, error } = await fetchSleeperRawLogs({
          playerId: target.providerPlayerId,
          season: input.season,
          seasonType: input.seasonType
        });
        if (error) {
          errors.push(error);
          continue;
        }
        rows.push(
          ...normalizeProviderRows({
            provider: "sleeper",
            sport: "NFL",
            providerPlayerId: target.providerPlayerId,
            playerName: target.playerName,
            team: target.team,
            season: input.season,
            seasonType: input.seasonType,
            rawPayload: payload
          }).filter((row) => !input.weeks.length || input.weeks.includes(row.week ?? -1))
        );
      }
      return { rows, errors, warnings: [] };
    }
  };
}
async function fetchCachedRowsForSport(input) {
  const sportKey = input.sport.toLowerCase();
  const cacheRows = await prisma.sportsDataCache.findMany({
    where: {
      OR: [
        { cacheKey: { contains: `${sportKey}:player_stats` } },
        { cacheKey: { contains: `${sportKey}:stats` } },
        { cacheKey: { contains: `${input.sport}:player_stats` } },
        { cacheKey: { contains: `${input.sport}:stats` } }
      ]
    },
    orderBy: { createdAt: "desc" },
    take: 20
  });
  const rows = [];
  for (const cacheRow of cacheRows) {
    const payload = isRecord4(cacheRow.data) && Array.isArray(cacheRow.data.data) ? cacheRow.data.data : cacheRow.data;
    const values = Array.isArray(payload) ? payload : isRecord4(payload) ? Object.values(payload) : [];
    for (const value of values) {
      if (!isRecord4(value)) continue;
      const providerPlayerId = String(value.playerId ?? value.player_id ?? value.id ?? value.externalId ?? "").trim();
      const playerName = String(value.playerName ?? value.player_name ?? value.name ?? "").trim();
      const week = weekFromRow(value);
      if (!providerPlayerId || !week) continue;
      rows.push({
        provider: input.provider,
        sport: input.sport,
        providerPlayerId,
        playerName,
        team: typeof value.team === "string" ? value.team : null,
        providerTeamId: typeof value.teamId === "string" ? value.teamId : null,
        opponent: typeof value.opponent === "string" ? value.opponent : null,
        gameId: String(value.gameId ?? value.game_id ?? value.eventId ?? "").trim() || null,
        week,
        season: input.season,
        seasonType: input.seasonType,
        playedAt: typeof value.date === "string" ? value.date : null,
        status: typeof value.status === "string" ? value.status : null,
        stats: normalizeStats(value),
        raw: value
      });
      if (rows.length >= input.limit * 20) break;
    }
  }
  return {
    rows: rows.filter((row) => !input.weeks.length || input.weeks.includes(row.week ?? -1)),
    errors: [],
    warnings: cacheRows.length ? [] : [`No cached ${input.sport} player stat payloads found in SportsDataCache.`]
  };
}
function getImportAdapter(sport, provider, limit, dates) {
  if (sport === "NFL" && provider === "sleeper") return buildSleeperNflAdapter();
  if (provider === "rolling_insights" && RI_LIVE_GAME_LOG_SPORTS.has(sport)) {
    return {
      provider,
      sport,
      status: "real",
      fetchRows: (input) => fetchRiLiveGameLogRows({ sport, seasonType: input.seasonType, days: RI_LIVE_LOOKBACK_DAYS, dates })
    };
  }
  if (provider === "cache" || sport === "NCAAF") {
    return {
      provider,
      sport,
      status: "cached_only",
      fetchRows: (input) => fetchCachedRowsForSport({
        sport,
        provider,
        season: input.season,
        seasonType: input.seasonType,
        weeks: input.weeks,
        limit
      })
    };
  }
  return {
    provider,
    sport,
    status: "scaffold",
    async fetchRows() {
      return {
        rows: [],
        errors: [],
        warnings: [`${provider} ${sport} player game-log import is scaffolded but not implemented yet.`]
      };
    }
  };
}
async function createSyncRun(input) {
  try {
    const row = await prisma.syncJobRun.create({
      data: {
        jobName: "player_game_log_import",
        jobScope: `${input.sport}:${input.provider}`,
        trigger: input.trigger,
        status: "running",
        metadata: toPrismaJsonInput(input.metadata)
      },
      select: { id: true }
    });
    return row.id;
  } catch {
    return null;
  }
}
async function completeSyncRun(input) {
  if (!input.id) return;
  try {
    const existing = await prisma.syncJobRun.findUnique({ where: { id: input.id }, select: { startedAt: true } });
    const durationMs = existing?.startedAt ? Math.max(0, Date.now() - existing.startedAt.getTime()) : null;
    await prisma.syncJobRun.update({
      where: { id: input.id },
      data: {
        status: input.status,
        rowsRead: input.rowsRead,
        rowsWritten: input.rowsWritten,
        rowsSkipped: input.rowsSkipped,
        metadata: toPrismaJsonInput(input.metadata),
        errorMessage: input.errorMessage ?? null,
        completedAt: /* @__PURE__ */ new Date(),
        durationMs
      }
    });
  } catch {
  }
}
async function importPlayerGameLogs(options) {
  const sport = normalizeSport(options.sport);
  const provider = normalizeProvider2(options.provider, sport);
  let season = toSeason2(options.season);
  const seasonType = toSeasonType(options.seasonType);
  const weeks = toWeeks(options.weeks);
  const limit = clampLimit(options.limit);
  const dryRun = options.dryRun === true;
  const trigger = options.trigger ?? "admin";
  const playerIds = Array.isArray(options.playerIds) ? options.playerIds.map((id2) => String(id2).trim()).filter(Boolean) : [];
  const syncJobRunId = await createSyncRun({
    sport,
    provider,
    trigger,
    metadata: { sport, provider, season, seasonType, weeks, dryRun, leagueId: options.leagueId ?? null }
  });
  const summary = {
    ok: true,
    dryRun,
    sport,
    provider,
    season,
    seasonType,
    weeks,
    targets: 0,
    rawRowsRead: 0,
    importedCount: 0,
    updatedCount: 0,
    skippedCount: 0,
    duplicateCount: 0,
    staleRows: 0,
    unmappedPlayers: [],
    unmappedTeams: [],
    providerErrors: [],
    warnings: [],
    syncJobRunId
  };
  try {
    const targets = await resolveImportTargets({
      sport,
      provider,
      playerIds,
      leagueId: options.leagueId,
      seasonId: options.seasonId,
      limit
    });
    summary.targets = targets.length;
    const adapter = getImportAdapter(sport, provider, limit, options.dates);
    if (adapter.status === "scaffold") summary.warnings.push(`${provider} adapter is scaffold-only for ${sport}.`);
    if (!targets.length && adapter.status !== "cached_only") {
      summary.warnings.push("No import targets found. Pass playerIds, leagueId, seasonId, or backfill PlayerIdentityMap first.");
      summary.skippedCount += 1;
    }
    if (dryRun) {
      await completeSyncRun({
        id: syncJobRunId,
        status: "success",
        rowsRead: 0,
        rowsWritten: 0,
        rowsSkipped: 0,
        metadata: summary
      });
      return summary;
    }
    const providerResult = await adapter.fetchRows({ targets, season, seasonType, weeks });
    summary.rawRowsRead = providerResult.rows.length;
    summary.providerErrors.push(...providerResult.errors);
    summary.warnings.push(...providerResult.warnings);
    if (options.season == null && provider === "rolling_insights" && RI_LIVE_GAME_LOG_SPORTS.has(sport)) {
      const reportedSeasons = [...new Set(providerResult.rows.map((row) => row.season).filter(Boolean))];
      if (reportedSeasons.length === 1) {
        season = reportedSeasons[0];
        summary.season = season;
      }
    }
    const grouped = /* @__PURE__ */ new Map();
    for (const row of providerResult.rows) {
      if (row.season && row.season !== season) {
        summary.skippedCount += 1;
        continue;
      }
      const player = await resolveProviderPlayerIdentity({
        sport,
        provider,
        providerPlayerId: row.providerPlayerId,
        playerName: row.playerName,
        team: row.team
      });
      if (!player.ok) {
        summary.unmappedPlayers.push({
          providerPlayerId: row.providerPlayerId,
          playerName: row.playerName,
          reason: player.reason
        });
        summary.skippedCount += 1;
        continue;
      }
      if (row.team || row.providerTeamId) {
        const team = await resolveProviderTeamIdentity({
          sport,
          provider,
          providerTeamId: row.providerTeamId,
          team: row.team
        });
        if (!team.ok) {
          summary.unmappedTeams.push({ team: row.team, providerTeamId: row.providerTeamId, reason: team.reason });
        }
      }
      const week = row.week ?? 0;
      if (!week && !RI_LIVE_GAME_LOG_SPORTS.has(sport) || Object.keys(row.stats).length === 0) {
        summary.skippedCount += 1;
        continue;
      }
      const normalized = {
        week,
        gameId: row.gameId ?? null,
        team: normalizeTeamAbbrev(row.team) ?? row.team ?? null,
        opponent: normalizeTeamAbbrev(row.opponent) ?? row.opponent ?? null,
        playedAt: row.playedAt ?? null,
        status: row.status ?? null,
        sourceProvider: row.provider,
        providerPlayerId: row.providerPlayerId,
        canonicalIdentityId: player.canonicalIdentityId,
        stats: row.stats,
        raw: row.raw
      };
      const current = grouped.get(player.cachePlayerId) ?? [];
      current.push(normalized);
      grouped.set(player.cachePlayerId, current);
    }
    const now = /* @__PURE__ */ new Date();
    const expiresAt = new Date(Date.now() + GAME_LOG_CACHE_TTL_MS);
    const playerIdsToWrite = Array.from(grouped.keys());
    const existingRows = playerIdsToWrite.length ? await prisma.playerGameLogCache.findMany({
      where: { playerId: { in: playerIdsToWrite }, sport, season, seasonType },
      select: { playerId: true, payload: true, expiresAt: true }
    }) : [];
    const existingByPlayer = new Map(existingRows.map((row) => [row.playerId, row]));
    summary.staleRows = existingRows.filter((row) => row.expiresAt <= now).length;
    for (const [cachePlayerId, incomingRows] of grouped) {
      const existing = existingByPlayer.get(cachePlayerId);
      const merged = mergeGameLogPayload({
        existingPayload: existing?.payload,
        incomingRows,
        provider,
        sport,
        season,
        seasonType,
        importedAt: now
      });
      summary.importedCount += merged.imported;
      summary.updatedCount += merged.updated;
      summary.duplicateCount += merged.duplicates;
      summary.skippedCount += merged.unchanged;
      await prisma.playerGameLogCache.upsert({
        where: {
          uniq_player_game_log_cache: {
            playerId: cachePlayerId,
            sport,
            season,
            seasonType
          }
        },
        update: {
          payload: toPrismaJsonInput(merged.payload),
          syncedAt: now,
          expiresAt
        },
        create: {
          playerId: cachePlayerId,
          sport,
          season,
          seasonType,
          payload: toPrismaJsonInput(merged.payload),
          syncedAt: now,
          expiresAt
        }
      });
    }
    await recordProviderSync(
      { provider, entityType: "player_game_logs", sport, key: `${season}:${seasonType}` },
      {
        recordsImported: summary.importedCount,
        recordsUpdated: summary.updatedCount,
        recordsSkipped: summary.skippedCount,
        error: summary.providerErrors[0] ?? null
      }
    );
    summary.ok = summary.providerErrors.length === 0;
    await completeSyncRun({
      id: syncJobRunId,
      status: summary.providerErrors.length ? summary.importedCount + summary.updatedCount > 0 ? "partial" : "failed" : "success",
      rowsRead: summary.rawRowsRead,
      rowsWritten: summary.importedCount + summary.updatedCount,
      rowsSkipped: summary.skippedCount,
      metadata: summary,
      errorMessage: summary.providerErrors[0] ?? null
    });
    return summary;
  } catch (error) {
    const message = redactError(error);
    summary.ok = false;
    summary.providerErrors.push(message);
    await recordProviderSync(
      { provider, entityType: "player_game_logs", sport, key: `${season}:${seasonType}` },
      { error: message }
    );
    await completeSyncRun({
      id: syncJobRunId,
      status: "failed",
      rowsRead: summary.rawRowsRead,
      rowsWritten: summary.importedCount + summary.updatedCount,
      rowsSkipped: summary.skippedCount,
      metadata: summary,
      errorMessage: message
    });
    return summary;
  }
}
function duplicateCountForPayload(payload) {
  const seen = /* @__PURE__ */ new Set();
  let duplicates = 0;
  for (const row of payloadLogs(payload)) {
    const key = entryKey(row);
    if (seen.has(key)) duplicates += 1;
    else seen.add(key);
  }
  return duplicates;
}
async function missingMappingCountForSport(sport) {
  const rows = await prisma.playerGameLogCache.findMany({
    where: { sport },
    select: { playerId: true },
    take: 500
  });
  let missing = 0;
  for (const row of rows) {
    const mapped = await prisma.playerIdentityMap.findFirst({
      where: {
        sport,
        OR: [
          { sleeperId: row.playerId },
          { apiSportsId: row.playerId },
          { rollingInsightsId: row.playerId },
          { clearSportsId: row.playerId },
          { espnId: row.playerId }
        ]
      },
      select: { id: true }
    }).catch(() => null);
    if (!mapped) missing += 1;
  }
  return missing;
}
async function healthRowForSport(sport) {
  const now = /* @__PURE__ */ new Date();
  const [totalCacheRows, staleRecords, latest, sampleRows, failedImports, syncStates, missingMappings] = await Promise.all([
    prisma.playerGameLogCache.count({ where: { sport } }).catch(() => 0),
    prisma.playerGameLogCache.count({ where: { sport, expiresAt: { lte: now } } }).catch(() => 0),
    prisma.playerGameLogCache.findFirst({
      where: { sport },
      orderBy: { syncedAt: "desc" },
      select: { syncedAt: true }
    }).catch(() => null),
    prisma.playerGameLogCache.findMany({
      where: { sport },
      orderBy: { syncedAt: "desc" },
      select: { payload: true },
      take: 500
    }).catch(() => []),
    prisma.syncJobRun.count({
      where: {
        jobName: "player_game_log_import",
        jobScope: { startsWith: `${sport}:` },
        status: "failed"
      }
    }).catch(() => 0),
    prisma.providerSyncState.findMany({
      where: { entityType: "player_game_logs", sport },
      orderBy: { updatedAt: "desc" },
      select: { provider: true, lastSuccessAt: true, lastErrorAt: true, lastError: true },
      take: 8
    }).catch(() => []),
    missingMappingCountForSport(sport).catch(() => 0)
  ]);
  let sampledGameLogs = 0;
  let latestWeekImported = null;
  let duplicateRecords = 0;
  for (const row of sampleRows) {
    const logs = payloadLogs(row.payload);
    sampledGameLogs += logs.length;
    duplicateRecords += duplicateCountForPayload(row.payload);
    for (const log of logs) {
      if (latestWeekImported == null || log.week > latestWeekImported) latestWeekImported = log.week;
    }
  }
  const topIssues = [
    totalCacheRows === 0 ? "No PlayerGameLogCache rows imported" : null,
    staleRecords > 0 ? `${staleRecords} stale cache row(s)` : null,
    missingMappings > 0 ? `${missingMappings} sampled player mapping gap(s)` : null,
    duplicateRecords > 0 ? `${duplicateRecords} duplicate sampled game-log row(s)` : null,
    failedImports > 0 ? `${failedImports} failed import run(s)` : null,
    !latest ? "No provider freshness timestamp" : null
  ].filter((issue) => Boolean(issue));
  return {
    sport,
    label: sport === "WC_SOCCER" ? "World Cup" : sport,
    totalCacheRows,
    sampledGameLogs,
    latestWeekImported,
    staleRecords,
    missingMappings,
    duplicateRecords,
    failedImports,
    providerFreshness: syncStates.map((row) => ({
      provider: row.provider,
      lastSuccessAt: row.lastSuccessAt?.toISOString() ?? null,
      lastErrorAt: row.lastErrorAt?.toISOString() ?? null,
      lastError: row.lastError?.slice(0, 180) ?? null
    })),
    topIssues
  };
}
async function getPlayerGameLogHealthDashboard(sports) {
  const requested = sports?.length ? sports.map(normalizeSport) : SPORTS;
  const uniqueSports = Array.from(new Set(requested));
  return {
    generatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    rows: await Promise.all(uniqueSports.map((sport) => healthRowForSport(sport)))
  };
}
var import_server_only17, GAME_LOG_CACHE_TTL_MS, DEFAULT_IMPORT_LIMIT, MAX_IMPORT_LIMIT, SPORTS, PROVIDER_ID_FIELD;
var init_PlayerGameLogImportService = __esm({
  "lib/sports-reporting/PlayerGameLogImportService.ts"() {
    "use strict";
    import_server_only17 = __toESM(require_audit_server_only_shim());
    init_prisma();
    init_redactSecrets();
    init_prisma_json();
    init_provider_sync_logger();
    init_team_abbrev();
    init_rate_limit_manager();
    init_riLiveGameLogAdapter();
    GAME_LOG_CACHE_TTL_MS = 6 * 60 * 60 * 1e3;
    DEFAULT_IMPORT_LIMIT = 50;
    MAX_IMPORT_LIMIT = 250;
    SPORTS = ["NFL", "NBA", "MLB", "NHL", "NCAAF", "NCAAB", "SOCCER", "WC_SOCCER"];
    PROVIDER_ID_FIELD = {
      sleeper: "sleeperId",
      rolling_insights: "rollingInsightsId",
      api_sports: "apiSportsId",
      api_football: "apiSportsId",
      clearsports: "clearSportsId",
      espn: "espnId"
    };
  }
});

// scripts/guilly-hosted-monitor.ts
var import_strict2 = __toESM(require("node:assert/strict"));
var import_node_http = require("node:http");
var import_node_crypto4 = require("node:crypto");

// tmp/guilly-hosted-fixture.json
var guilly_hosted_fixture_default = {
  tag: "guilly-auto-c8cb0059",
  databaseHost: "ep-winter-salad-ad34lce8-pooler.c-2.us-east-1.aws.neon.tech",
  leagues: {
    NFL: {
      leagueId: "de66f04b-ba49-4577-b709-6e04062fe692",
      seasonId: "cmvh1n8z00002j6h5eyvezl76",
      season: 2026,
      firstWeek: 7,
      realPlayers: true,
      historyCapturedAt: "2026-10-20T18:55:52.808Z"
    },
    NBA: {
      leagueId: "03b8eaba-efa7-4e1a-81e7-f3f90c1210d5",
      seasonId: "cmvh1n9vu001hj6h5q42jb1is",
      season: 2026,
      firstWeek: 2,
      realPlayers: true,
      historyCapturedAt: "2026-10-20T18:55:53.701Z"
    },
    NHL: {
      leagueId: "2e62583e-8f09-4192-b1bd-a4b742052fa0",
      seasonId: "cmvh1naj5002wj6h5fnohfl3v",
      season: 2026,
      firstWeek: 5,
      realPlayers: true,
      historyCapturedAt: "2026-10-20T18:55:54.523Z"
    },
    MLB: {
      leagueId: "97dada24-2036-4b36-8e4c-52b455a08978",
      seasonId: "cmvh1nb5w004bj6h558529crf",
      season: 2027,
      firstWeek: 1,
      realPlayers: true,
      historyCapturedAt: "2026-10-20T18:55:55.530Z"
    },
    "NFL-4": {
      leagueId: "81da7908-0e5b-4ebc-b3d7-25eaca07d133",
      seasonId: "cmvh3raj10002ci1blt1q5937",
      season: 2026,
      firstWeek: 7,
      initialTeams: 4,
      sport: "NFL",
      tag: "guilly-auto-7222c886",
      realPlayers: true,
      historyCapturedAt: "2026-10-20T19:55:00.432Z"
    },
    "NBA-4": {
      leagueId: "d7ef74b8-0a97-467f-8d2c-f14020b28f10",
      seasonId: "cmvh3rbce000tci1bbx4v37cf",
      season: 2026,
      firstWeek: 2,
      initialTeams: 4,
      sport: "NBA",
      tag: "guilly-auto-7222c886",
      realPlayers: true,
      historyCapturedAt: "2026-10-20T19:55:01.158Z"
    },
    "NHL-4": {
      leagueId: "803f3503-bbe3-43c1-aed8-15bc7381db29",
      seasonId: "cmvh3rbsx001kci1bx9opyx8i",
      season: 2026,
      firstWeek: 4,
      initialTeams: 4,
      sport: "NHL",
      tag: "guilly-auto-7222c886",
      realPlayers: true,
      historyCapturedAt: "2026-10-20T19:55:01.758Z"
    }
  }
};

// scripts/guilly-hosted-proof.ts
var import_strict = __toESM(require("node:assert/strict"));
function verifyPeriod(input) {
  const expected = Math.min(input.configuredCuts, input.activeBefore - 1);
  import_strict.default.equal(input.cutScores.length, expected, "Wrong cut count");
  import_strict.default.equal(input.scoredTeams, input.activeBefore, "Missing scored-team evidence");
  import_strict.default.equal(input.survivorScores.length, input.activeBefore - expected, "Wrong survivor count");
  import_strict.default.equal(input.journals, 1, "Duplicate or missing cut journal");
  import_strict.default.ok([...input.cutScores, ...input.survivorScores].every(Number.isFinite), "Nonfinite score");
  import_strict.default.ok(input.cutScores.every((cut) => input.survivorScores.every((safe) => cut <= safe)), "Higher scorer incorrectly eliminated");
}
function verifyCompletion(input) {
  const total = input.initialTeams ?? 8;
  import_strict.default.ok(Number.isInteger(total) && total >= 4, "Invalid starting team count");
  import_strict.default.equal(input.eliminated, total - 1);
  import_strict.default.equal(input.survivors, 1);
  import_strict.default.ok(input.complete && input.championMatches, "Winner not recorded");
  import_strict.default.equal(input.playoffBrackets, 0);
  if (input.requireDoubleChop !== false) import_strict.default.ok(input.periodCuts.includes(2), "Double chop not observed");
  import_strict.default.equal(input.periodCuts.reduce((a, b) => a + b, 0), total - 1, "Incomplete period evidence");
  import_strict.default.equal(input.verifiedWaivers, input.periodCuts.length - 1, "Missing survivor waiver evidence");
}

// scripts/guilly-hosted-monitor.ts
var STATE_KEY = "guilly:hosted-monitor:v1";
var FIVE_MINUTES = 3e5;
var fingerprint = (s) => (0, import_node_crypto4.createHash)("sha256").update(s).digest("hex").slice(0, 24);
function guard() {
  import_strict2.default.equal(process.env.GUILLY_HOSTED_STAGING, "true", "Dedicated staging mode required");
  import_strict2.default.equal(new URL(process.env.DATABASE_URL).hostname, guilly_hosted_fixture_default.databaseHost, "Staging database mismatch");
  import_strict2.default.equal(new URL(process.env.DIRECT_URL).hostname, guilly_hosted_fixture_default.databaseHost, "Direct database mismatch");
  import_strict2.default.equal([...new Set(Object.entries(guilly_hosted_fixture_default.leagues).map(([key, target]) => target.sport || key))].sort().join(","), "MLB,NBA,NFL,NHL");
  import_strict2.default.ok(process.env.RESEND_API_KEY && process.env.GUILLY_ALERT_TO && process.env.RESEND_FROM, "Alert delivery configuration required");
  import_strict2.default.ok(process.env.GUILLY_STATUS_TOKEN && process.env.GUILLY_STATUS_TOKEN.length >= 32, "Private status token required");
}
guard();
var bootedAt = Date.now();
var lastFinished = 0;
var running = false;
var lastError = null;
var checkpoint = { sports: {}, alerts: {}, lastFeedAt: 0, startedAt: (/* @__PURE__ */ new Date()).toISOString() };
var db;
var safeError = (error) => {
  let message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  for (const [key, value] of Object.entries(process.env))
    if (value && value.length > 8 && /SECRET|TOKEN|KEY|URL|PASSWORD/.test(key)) message = message.split(value).join("[redacted]");
  return message.replace(/postgres(?:ql)?:\/\/\S+/gi, "[database]").slice(0, 1e3);
};
async function save() {
  await db.sportsDataCache.upsert({
    where: { cacheKey: STATE_KEY },
    create: { cacheKey: STATE_KEY, data: checkpoint, expiresAt: /* @__PURE__ */ new Date("2035-01-01") },
    update: { data: checkpoint, expiresAt: /* @__PURE__ */ new Date("2035-01-01") }
  });
}
async function alert(kind, message) {
  if (process.env.GUILLY_MONITOR_VALIDATE === "1") {
    console.log(JSON.stringify({ event: "validation_alert_preview", kind, message }));
    return;
  }
  const key = fingerprint(kind + message);
  if (checkpoint.alerts[key]) return;
  const response = await fetch("https://api.resend.com/emails", {
    method: "POST",
    signal: AbortSignal.timeout(3e4),
    headers: {
      Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
      "Content-Type": "application/json",
      "Idempotency-Key": `guilly-${key}`
    },
    body: JSON.stringify({
      from: process.env.RESEND_FROM,
      to: [process.env.GUILLY_ALERT_TO],
      subject: `Guilly staging: ${kind}`,
      text: `${message}

Staging only. No production leagues were processed.
Monitor: ${process.env.GUILLY_DASHBOARD_URL || "Guilly staging validation"}
Time: ${(/* @__PURE__ */ new Date()).toISOString()}`
    })
  });
  if (!response.ok) throw Error(`Alert delivery failed: HTTP ${response.status}`);
  const receipt = await response.json();
  import_strict2.default.ok(receipt.id, "Email provider receipt missing");
  checkpoint.alerts[key] = (/* @__PURE__ */ new Date()).toISOString();
  checkpoint.deliveries ||= {};
  checkpoint.deliveries[key] = { id: receipt.id, kind, acceptedAt: checkpoint.alerts[key] };
  console.log(JSON.stringify({ event: "alert_accepted", kind }));
}
async function checkSport(sport, target, refresh, stateKey = sport) {
  const { resolveSeasonWeekForRedraftSeason: resolveSeasonWeekForRedraftSeason2 } = await Promise.resolve().then(() => (init_season_week(), season_week_exports));
  const { syncPlayerWeeklyScoresForRedraftSeason: syncPlayerWeeklyScoresForRedraftSeason2 } = await Promise.resolve().then(() => (init_playerWeeklyScoreService(), playerWeeklyScoreService_exports));
  const { runNativeGuillotineWeek: runNativeGuillotineWeek2 } = await Promise.resolve().then(() => (init_nativeGuillotineWeek(), nativeGuillotineWeek_exports));
  const { processWaiverWindow: processWaiverWindow2 } = await Promise.resolve().then(() => (init_waiverEngine(), waiverEngine_exports));
  const { readLineupHistory: readLineupHistory2 } = await Promise.resolve().then(() => (init_lineupHistory(), lineupHistory_exports));
  const { getGuillotineConfig: getGuillotineConfig2 } = await Promise.resolve().then(() => (init_GuillotineLeagueConfig(), GuillotineLeagueConfig_exports));
  const { leagueId, seasonId, season } = target;
  const league = await db.league.findUniqueOrThrow({ where: { id: leagueId } });
  import_strict2.default.ok(league.name?.includes(target.tag || guilly_hosted_fixture_default.tag), "QA league ownership mismatch");
  const shell = await db.redraftSeason.findUniqueOrThrow({ where: { id: seasonId } });
  import_strict2.default.equal(shell.leagueId, leagueId);
  import_strict2.default.equal(shell.season, season);
  import_strict2.default.equal(shell.sport, sport);
  const initialTeams = target.initialTeams ?? 8;
  import_strict2.default.equal(league.leagueSize, initialTeams, "Starting league size mismatch");
  import_strict2.default.equal(await db.redraftRoster.count({ where: { seasonId } }), initialTeams, "Roster count mismatch");
  const history = await readLineupHistory2(db, leagueId, seasonId, /* @__PURE__ */ new Date());
  import_strict2.default.equal(history.legacyBaseline, false);
  import_strict2.default.ok(history.events.length >= initialTeams * 2);
  const state = checkpoint.sports[stateKey] ||= { cuts: {}, waivers: {}, firstWeek: target.firstWeek, season };
  if (state.complete) return;
  const rules = await getGuillotineConfig2(leagueId);
  import_strict2.default.ok(rules);
  import_strict2.default.equal(rules.eliminationStartWeek, target.firstWeek);
  if (refresh) {
    if (sport === "NFL") {
      const { syncNFLScheduleToDb: syncNFLScheduleToDb2 } = await Promise.resolve().then(() => (init_rolling_insights2(), rolling_insights_exports));
      await syncNFLScheduleToDb2({ season: String(season) });
    } else {
      const { syncRiSeasonSchedule: syncRiSeasonSchedule2 } = await Promise.resolve().then(() => (init_riSeasonSchedule(), riSeasonSchedule_exports));
      const schedule = await syncRiSeasonSchedule2({ sport, season, db });
      state.schedule = schedule;
      if (schedule.error && !schedule.notModified) throw Error(`${sport}: ${schedule.error}`);
      if (season <= (/* @__PURE__ */ new Date()).getUTCFullYear()) {
        const { ingestRollingInsightsGameLogs: ingestRollingInsightsGameLogs2, recentDates: recentDates2 } = await Promise.resolve().then(() => (init_rollingInsightsGameLogs(), rollingInsightsGameLogs_exports));
        const { pendingStatCorrectionDates: pendingStatCorrectionDates2 } = await Promise.resolve().then(() => (init_statCoverage(), statCoverage_exports));
        const { importPlayerGameLogs: importPlayerGameLogs2 } = await Promise.resolve().then(() => (init_PlayerGameLogImportService(), PlayerGameLogImportService_exports));
        const dates = [.../* @__PURE__ */ new Set([...recentDates2(3), ...await pendingStatCorrectionDates2(db, sport)])];
        const boxes = await ingestRollingInsightsGameLogs2({ sport, dates, persistSchedule: true });
        const cache = await importPlayerGameLogs2({ sport, season, provider: "rolling_insights", dates, seasonType: "regular", trigger: "guilly-hosted-monitor" });
        state.feed = { checkedAt: (/* @__PURE__ */ new Date()).toISOString(), written: boxes.written, cached: cache.importedCount + cache.updatedCount };
        const errors = [...boxes.errors, ...cache.providerErrors].filter((e) => !/304/.test(e));
        if (errors.length) throw Error(`${sport} feed: ${errors.join("; ")}`);
      }
    }
  }
  await processWaiverWindow2(leagueId, seasonId);
  for (const [period, proof] of Object.entries(state.waivers)) {
    if (proof.verified) continue;
    const claims = await db.redraftWaiverClaim.findMany({ where: { id: { in: proof.ids }, leagueId } });
    if (claims.some((c) => c.status === "pending")) {
      if (Date.now() > Date.parse(proof.availableAt) + 6 * 36e5) throw Error(`${sport} period ${period}: waiver settlement overdue`);
      state.status = "waiver_window_pending";
      return;
    }
    const survivor = claims.find((c) => c.id === proof.ids[0]), eliminated = claims.find((c) => c.id === proof.ids[1]);
    import_strict2.default.equal(survivor?.status, "approved", `${sport}: survivor claim not awarded`);
    import_strict2.default.equal(eliminated?.status, "denied", `${sport}: eliminated claim not denied`);
    import_strict2.default.ok(await db.redraftRosterPlayer.findFirst({ where: { rosterId: survivor.rosterId, playerId: survivor.addPlayerId, droppedAt: null } }));
    const roster = await db.redraftRoster.findUniqueOrThrow({ where: { id: survivor.rosterId } });
    import_strict2.default.equal(roster.faabBalance, proof.faabBefore - 1, "FAAB charged incorrectly");
    await processWaiverWindow2(leagueId, seasonId);
    import_strict2.default.equal((await db.redraftRoster.findUniqueOrThrow({ where: { id: survivor.rosterId } })).faabBalance, roster.faabBalance, "Waiver retry double-charged");
    proof.verified = (/* @__PURE__ */ new Date()).toISOString();
  }
  const resolved = await resolveSeasonWeekForRedraftSeason2(seasonId);
  if (!resolved.ok || resolved.phase === "preseason") {
    state.status = resolved.ok ? "preseason" : resolved.reason;
    const { resolveDailySportSeasonStart: resolveDailySportSeasonStart2 } = await Promise.resolve().then(() => (init_dailySportSeasonStarts(), dailySportSeasonStarts_exports));
    const opener = resolveDailySportSeasonStart2(sport, season);
    const beforeOpener = opener && Date.now() < Date.parse(opener);
    if (season <= (/* @__PURE__ */ new Date()).getUTCFullYear() && !beforeOpener && !resolved.ok) throw Error(`${sport}: calendar unavailable: ${resolved.reason}`);
    return;
  }
  const sync = (week) => syncPlayerWeeklyScoresForRedraftSeason2({ seasonId, week, actorId: "system:guilly-hosted-monitor" });
  if (refresh) {
    await sync(resolved.fantasyWeek);
    const lastCut = await db.guillotineElimination.findFirst({ where: { leagueId }, orderBy: { scoringPeriod: "desc" } });
    const owed = Math.max(target.firstWeek, (lastCut?.scoringPeriod ?? 0) + 1);
    if (owed < resolved.fantasyWeek) await sync(owed);
  }
  const result = await runNativeGuillotineWeek2(
    { seasonId, currentFantasyWeek: resolved.fantasyWeek },
    { syncWeekStats: ({ week }) => sync(week) }
  );
  state.lastResult = result;
  state.status = result.outcome;
  if (result.outcome === "waiting" && result.reason !== "week_in_progress") {
    const same = state.waiting?.reason === result.reason && state.waiting?.week === result.week;
    if (!same) state.waiting = { reason: result.reason, week: result.week, since: (/* @__PURE__ */ new Date()).toISOString() };
    if (Date.now() - Date.parse(state.waiting.since) > 36 * 36e5) throw Error(`${sport} week ${result.week}: ${result.reason} for over 36 hours`);
  } else delete state.waiting;
  const cuts = await db.guillotineElimination.findMany({ where: { leagueId }, orderBy: { scoringPeriod: "asc" } });
  const survivors = await db.redraftRoster.findMany({ where: { seasonId, isEliminated: false }, orderBy: { id: "asc" } });
  const periods = [...new Set(cuts.map((c) => c.scoringPeriod))];
  for (const period of periods) {
    const eliminated = cuts.filter((c) => c.scoringPeriod === period);
    const before = initialTeams - cuts.filter((c) => c.scoringPeriod < period).length;
    const expected = Math.min(rules.eliminationsByWeek?.[period] ?? rules.teamsPerChop, before - 1);
    const survival = await db.guillotineSurvivalLog.findMany({ where: { leagueId, scoringPeriod: period } });
    const safe = survival.filter((x) => x.survivalStatus === "survived");
    verifyPeriod({
      activeBefore: before,
      configuredCuts: expected,
      cutScores: eliminated.map((x) => x.finalScore),
      survivorScores: safe.map((x) => x.totalScore),
      scoredTeams: survival.length,
      journals: await db.guillotineEventLog.count({ where: { leagueId, eventType: "chop", metadata: { path: ["weekOrPeriod"], equals: period } } })
    });
    for (const cut of eliminated) {
      import_strict2.default.ok(safe.every((s) => cut.finalScore <= s.totalScore), "Higher scorer incorrectly eliminated");
      import_strict2.default.equal(await db.redraftRosterPlayer.count({ where: { rosterId: cut.eliminatedRosterId, droppedAt: null } }), 0, "Eliminated roster still owns players");
    }
    import_strict2.default.equal(await db.guillotineEventLog.count({ where: { leagueId, eventType: "chop", metadata: { path: ["weekOrPeriod"], equals: period } } }), 1, "Duplicate or missing cut journal");
    state.cuts[period] = { count: eliminated.length, checkedAt: (/* @__PURE__ */ new Date()).toISOString(), scores: eliminated.map((c) => c.finalScore) };
    if (survivors.length > 1 && !state.waivers[period]) {
      const release = await db.guillotineWaiverRelease.findFirstOrThrow({ where: { leagueId, scoringPeriod: period } });
      const survivor = survivors[0];
      const drop = await db.redraftRosterPlayer.findFirstOrThrow({ where: { rosterId: survivor.id, droppedAt: null, slotType: { in: ["BENCH", "BN", "bench"] } } });
      const ids = [`guilly-monitor-survive-${fingerprint(leagueId + period)}`, `guilly-monitor-denied-${fingerprint(leagueId + period)}`];
      const proof = { ids, availableAt: release.availableAt.toISOString(), faabBefore: survivor.faabBalance };
      await db.$transaction(async (tx) => {
        for (const [index, rosterId] of [survivor.id, release.eliminatedRosterId].entries())
          await tx.redraftWaiverClaim.upsert({ where: { id: ids[index] }, update: {}, create: {
            id: ids[index],
            leagueId,
            seasonId,
            rosterId,
            addPlayerId: release.playerId,
            addPlayerName: release.playerName,
            dropPlayerId: index ? null : drop.playerId,
            bidAmount: index ? 99 : 1,
            priority: 1
          } });
        state.waivers[period] = proof;
        await tx.sportsDataCache.upsert({ where: { cacheKey: STATE_KEY }, create: { cacheKey: STATE_KEY, data: checkpoint, expiresAt: /* @__PURE__ */ new Date("2035-01-01") }, update: { data: checkpoint } });
      });
    }
  }
  if (survivors.length === 1) {
    const finalLeague = await db.league.findUniqueOrThrow({ where: { id: leagueId } });
    verifyCompletion({
      initialTeams,
      eliminated: cuts.length,
      survivors: survivors.length,
      complete: (await db.redraftSeason.findUniqueOrThrow({ where: { id: seasonId } })).status === "complete",
      championMatches: finalLeague.settings?.guillyChampion?.rosterId === survivors[0].id,
      playoffBrackets: await db.redraftPlayoffBracket.count({ where: { seasonId } }),
      periodCuts: Object.values(state.cuts).map((c) => c.count),
      verifiedWaivers: Object.values(state.waivers).filter((w) => w.verified).length
    });
    import_strict2.default.equal(Object.keys(state.waivers).length, periods.length - 1, "Missing survivor waiver periods");
    import_strict2.default.ok(Object.values(state.waivers).every((w) => w.verified), "Waivers not fully validated");
    state.complete = (/* @__PURE__ */ new Date()).toISOString();
    state.champion = survivors[0].id;
  }
}
async function tick() {
  if (running) return;
  running = true;
  const owner = (0, import_node_crypto4.randomUUID)();
  const { acquireAutomationLock: acquireAutomationLock2, releaseAutomationLock: releaseAutomationLock2 } = await Promise.resolve().then(() => (init_locks(), locks_exports));
  const lock = await acquireAutomationLock2(STATE_KEY, { ttlMs: 30 * 6e4, owner });
  if (!lock.ok) {
    running = false;
    console.log(JSON.stringify({ event: "tick_skipped", reason: "monitor_lease_held" }));
    if (process.env.GUILLY_MONITOR_ONCE === "1") throw Error("Smoke test did not run: monitor lease held");
    return;
  }
  try {
    const stored = await db.sportsDataCache.findUnique({ where: { cacheKey: STATE_KEY } });
    if (stored) checkpoint = stored.data;
    checkpoint.executor = {
      provider: process.env.GITHUB_ACTIONS === "true" ? "github-actions" : "local",
      runId: process.env.GITHUB_RUN_ID || null,
      sha: process.env.GITHUB_SHA || null
    };
    if (process.env.GUILLY_MONITOR_ALERT_SELFTEST === "1") {
      await alert("TEST failure notification", "TEST ONLY: Hosted Guilly failure email delivery verification. No actual league failure is asserted.");
      await save();
      await alert("TEST completion notification", "TEST ONLY: Hosted Guilly completion email delivery verification. Live validation is still pending; no sport is marked complete by this test.");
      await save();
    }
    const refresh = Date.now() - checkpoint.lastFeedAt >= 36e5;
    const failures = [];
    for (const [sport, target] of Object.entries(guilly_hosted_fixture_default.leagues)) {
      try {
        await checkSport(target.sport || sport, target, refresh, sport);
      } catch (error) {
        const message = safeError(error);
        failures.push({ sport, message });
        console.error(JSON.stringify({ event: "validation_failure", sport, message }));
        try {
          await alert("validation failure", `${sport}: ${message}`);
        } catch {
          console.error(JSON.stringify({ event: "alert_delivery_failed", sport }));
        }
      }
      await save();
    }
    if (refresh && !failures.length) checkpoint.lastFeedAt = Date.now();
    checkpoint.lastTick = (/* @__PURE__ */ new Date()).toISOString();
    checkpoint.failures = failures;
    const complete = Object.keys(guilly_hosted_fixture_default.leagues).every((key) => checkpoint.sports[key]?.complete);
    if (complete) await alert("all four sports validated", "NFL, NBA, NHL and MLB have live evidence of automatic cuts, double chop weeks, survivor waivers and one winner. Validation is complete.");
    await save();
    lastError = failures.length ? "validation_failure" : null;
    lastFinished = Date.now();
    console.log(JSON.stringify({
      event: "tick_complete",
      at: checkpoint.lastTick,
      failures: failures.length,
      complete,
      sports: Object.fromEntries(Object.entries(checkpoint.sports).map(([s, v]) => [s, { status: v.status, cuts: Object.keys(v.cuts).length, complete: !!v.complete }]))
    }));
  } finally {
    await releaseAutomationLock2(STATE_KEY, owner);
    running = false;
  }
}
async function main() {
  const { prisma: prisma2 } = await Promise.resolve().then(() => (init_prisma(), prisma_exports));
  db = prisma2;
  if (process.env.GUILLY_MONITOR_ONCE !== "1") (0, import_node_http.createServer)((req, res) => {
    const healthy = Date.now() - (lastFinished || bootedAt) < 30 * 6e4;
    if (req.url === "/healthz") {
      res.writeHead(healthy ? 200 : 503);
      res.end(JSON.stringify({ healthy, lastFinished, running, lastError }));
      return;
    }
    if (req.url === "/status" && req.headers.authorization === `Bearer ${process.env.GUILLY_STATUS_TOKEN}`) {
      res.setHeader("Content-Type", "application/json");
      res.end(JSON.stringify(checkpoint));
      return;
    }
    res.writeHead(404);
    res.end();
  }).listen(Number(process.env.PORT || 8080), "0.0.0.0");
  const run = async () => {
    try {
      await tick();
    } catch (error) {
      running = false;
      lastError = safeError(error);
      console.error(JSON.stringify({ event: "monitor_failure", message: lastError }));
      try {
        await alert("monitor failure", lastError);
        await save();
      } catch {
        console.error("Alert or checkpoint unavailable");
      }
    }
  };
  await run();
  if (process.env.GUILLY_MONITOR_ONCE === "1") {
    await db.$disconnect();
    if (lastError) process.exitCode = 1;
    return;
  }
  setInterval(run, FIVE_MINUTES);
  setInterval(() => {
    if (Date.now() - (lastFinished || bootedAt) > 30 * 6e4) {
      void alert("monitor stalled", "No completed monitor tick for 30 minutes; restarting the worker.").finally(() => process.exit(1));
    }
  }, 6e4);
}
main().catch((error) => {
  console.error(safeError(error));
  process.exitCode = 1;
});
