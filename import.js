#!/usr/bin/env node
/**
 * Import images into CollectiveAccess from a CSV.
 *
 * Each CSV row maps a server-side image path to a CA object.
 * The script creates a ca_object_representation for each row and links it
 * to the object identified by item_id (resolved via the CA find API).
 *
 * CSV columns: title, path, image_id, item_id
 *   path     — server-local file path already on the CA host,
 *              e.g. /var/www/html/import/photos_to_connect_YBZ/YBZ.0807.339.jpg
 *   image_id — becomes the representation idno; also used to detect duplicates
 *   item_id  — CA object idno to link the representation to
 *   title    — preferred label (optional)
 *
 * USAGE:
 *   node import.js --ftp <dir> [--server ynl|eretz] [options]
 *   node import.js --csv <file> [options]
 *
 * OPTIONS:
 *   --ftp DIR              FTP directory — connect to SFTP, list .jpg files, generate CSV, import
 *   --server NAME          SFTP server: ynl (default) or eretz
 *   --csv PATH             CSV file to import (alternative to --ftp)
 *   --start N              Skip the first N rows (start from row N)
 *   --limit N              Process at most N rows
 *   --concurrency N        Initial parallel rows in flight (default: 2)
 *   --max-concurrency N    Maximum concurrency the adaptive queue may reach (default: 10)
 *   --domain DOMAIN        CA host, e.g. ca.israelalbum.org.il (builds CA_BASE_URL)
 *   --resume               Skip rows already recorded in import.progress.json
 *   --verbose, -v          Log every API request/response
 *
 * ENV (loaded from .env in this directory, then process environment):
 *   CA_BASE_URL        Full API base URL including trailing slash
 *   CA_USER            API username (default: administrator)
 *   CA_PASSWORD        API password (required; prompted if TTY)
 *   CA_LOCALE          Locale string (default: he_IL)
 *   REL_TYPE_ID        representation→object relationship type id (default: 135)
 *
 * ADAPTIVE CONCURRENCY:
 *   The queue starts at --concurrency and adjusts between 1 and --max-concurrency
 *   automatically. It ramps up (+1) after 5 consecutive successes, and backs off
 *   (÷2) on HTTP 429/5xx or network errors. Failed rows caused by server overload
 *   are re-queued with a 5-second delay and not written to import.failed.csv —
 *   they will be retried automatically.
 *
 * SKIP LOGIC:
 *   A row is skipped if the target object already has one or more representations
 *   attached in CA. The image path in the CSV is assumed to already exist on the
 *   server (uploaded via SFTP separately).
 *
 * OUTPUT FILES (written alongside the CSV):
 *   import.log.csv             Every row: status (imported/skipped/error), reason, rep_id
 *   import.progress.json       Completed item_ids — used by --resume
 *   import.failed.csv          Rows that failed with non-overload errors (feed back with --csv)
 */

import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';
import readline from 'node:readline';

import { parse as parseCsv } from 'csv-parse/sync';
import * as dotenv from 'dotenv';
import SftpClient from 'ssh2-sftp-client';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '.env') });

// ─── SFTP server configs (credentials in .env) ───────────────────────────────

const SFTP_SERVERS = {
  ynl: {
    host:     process.env.SFTP_YNL_HOST,
    username: process.env.SFTP_YNL_USER,
    password: process.env.SFTP_YNL_PWD,
  },
  eretz: {
    host:     process.env.SFTP_ERETZ_HOST,
    username: process.env.SFTP_ERETZ_USER,
    password: process.env.SFTP_ERETZ_PWD,
  },
};

// ─── Helpers ───────────────────────────────────────────────────────────────

let verboseMode = false;
let progress    = null; // set during the import loop; routes log/warn through the bar

const log     = (...a) => progress ? progress.println(a.join(' ')) : console.log(...a);
const verbose = (...a) => { if (verboseMode) console.log('[V]', ...a); };
const warn    = (...a) => progress ? progress.println(a.join(' ')) : console.warn(...a);

// ─── Progress bar ───────────────────────────────────────────────────────────

class ProgressBar {
  constructor(total, getConcStatus) {
    this.total          = total;
    this.processed      = 0;
    this.worked         = 0;   // rows where actual API work was attempted (not skipped)
    this.startTime      = null;
    this.tty            = !!process.stdout.isTTY;
    this.width          = Math.max(20, Math.min(40, (process.stdout.columns || 80) - 60));
    this.getConcStatus  = getConcStatus || null;
  }

  // Called when real work was attempted (createRep or fetch error) — counts for timing.
  tick() {
    if (this.startTime === null) this.startTime = Date.now();
    this.processed++;
    this.worked++;
    if (this.tty) this._draw();
  }

  // Called for skipped rows (not found / already has media) — advances the bar but
  // does not affect the average time or ETA calculation.
  skip() {
    if (this.startTime === null) this.startTime = Date.now();
    this.processed++;
    if (this.tty) this._draw();
  }

  println(line) {
    if (this.tty) process.stdout.write('\r\x1b[K');
    console.log(line);
    if (this.tty && this.processed < this.total) this._draw();
  }

  finish() {
    if (this.tty) process.stdout.write('\r\x1b[K');
  }

  _draw() {
    const pct     = this.total > 0 ? this.processed / this.total : 0;
    const filled  = Math.round(this.width * pct);
    const bar     = '█'.repeat(filled) + '░'.repeat(this.width - filled);
    const elapsed    = this.startTime ? (Date.now() - this.startTime) / 1000 : 0;
    // imports/min counts only rows where real API work was done (not skips).
    // ETA uses total throughput (skips + imports) since remaining rows are a mix.
    const ipm        = elapsed > 0 && this.worked > 0 ? (this.worked / elapsed) * 60 : 0;
    const allRpm     = elapsed > 0 && this.processed > 0 ? (this.processed / elapsed) * 60 : 0;
    const eta        = allRpm > 0 ? (this.total - this.processed) / allRpm * 60 : 0;
    const rateStr    = ipm > 0 ? `${ipm.toFixed(1)}/min` : '  -  ';
    const etaStr     = eta > 0 ? this._fmt(eta) : '--:--';
    let line = `\r\x1b[K[${bar}] ${this.processed}/${this.total} (${Math.round(pct * 100)}%) | ${rateStr} | ETA ${etaStr}`;
    if (this.getConcStatus) {
      const { concurrency, direction } = this.getConcStatus();
      line += ` | conc=${concurrency}${direction}`;
    }
    process.stdout.write(line);
  }

  _fmt(secs) {
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.floor(secs % 60);
    return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  }
}

// ─── Adaptive Queue ─────────────────────────────────────────────────────────
//
// Replaces p-limit with a queue that dynamically adjusts concurrency using
// AIMD (Additive Increase, Multiplicative Decrease) — the same algorithm TCP
// uses for congestion control.
//
// Ramp-up  (+1): after 5 consecutive successes (no overload errors)
// Back-off (÷2): on HTTP 429/5xx or network error
// Retry        : overload failures are re-queued with a 5s delay (not failed to CSV)

class AdaptiveQueue {
  constructor({ min = 1, max = 5, initial = 2 } = {}) {
    this.min         = min;
    this.max         = max;
    this.concurrency = Math.max(min, Math.min(max, initial));
    this.active      = 0;
    this.queue       = [];
    this.successes   = 0;   // consecutive success counter for ramp-up
    this.direction   = '';  // last adjustment: '↑' | '↓' | ''
  }

  get status() {
    return { concurrency: this.concurrency, direction: this.direction };
  }

  // ── Internal helpers ──────────────────────────────────────────────────────

  _backOff() {
    this.successes = 0;
    this.concurrency = Math.max(this.min, Math.floor(this.concurrency / 2));
    this.direction = '↓';
  }

  _onSuccess() {
    this.successes++;
    // Ramp up after 5 consecutive successes (no overload errors)
    if (this.successes >= 5 && this.concurrency < this.max) {
      this.concurrency++;
      this.direction = '↑';
      this.successes = 0;
    } else if (this.successes > 2) {
      // Stable for a few rows — clear the stale arrow so it doesn't mislead
      this.direction = '';
    }
  }

  _drain() {
    while (this.active < this.concurrency && this.queue.length > 0) {
      const task = this.queue.shift();
      this.active++;
      this._execute(task);
    }
  }

  _execute({ fn, resolve, reject, retryDelay }) {
    fn().then(
      result => {
        if (result !== 'skip') this._onSuccess();
        this.active--;
        this._drain();
        resolve(result);
      },
      err => {
        if (err?.backOffOnly) {
          // Back off concurrency but don't retry (e.g. createRep may have partially succeeded)
          this._backOff();
          this.active--;
          this._drain();
          resolve(err.result);
        } else if (isOverloadErr(err)) {
          this._backOff();
          this.active--;
          this._drain();
          const delay = retryDelay || 5_000;
          setTimeout(() => {
            this.queue.unshift({ fn, resolve, reject, retryDelay });
            this._drain();
          }, delay);
        } else {
          this.successes = 0;
          this.active--;
          this._drain();
          reject(err);
        }
      }
    );
  }

  run(fn, { retryDelay } = {}) {
    return new Promise((resolve, reject) => {
      const task = { fn, resolve, reject, retryDelay };
      if (this.active < this.concurrency) {
        this.active++;
        this._execute(task);
      } else {
        this.queue.push(task);
      }
    });
  }
}

// ─── Error classification ──────────────────────────────────────────────────

function isNetworkErr(e) {
  return /fetch failed|ECONNREFUSED|ENOTFOUND|ETIMEDOUT|abort|502|503|504|500 Internal/.test(e.message || '');
}

// Overload = server-side congestion; safe to retry automatically
function isOverloadErr(e) {
  return /429/.test(e?.message || '') || isNetworkErr(e);
}

// Simple fixed-concurrency runner over a list of items.
async function runConcurrent(items, concurrency, fn) {
  let i = 0;
  await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (i < items.length) {
      const item = items[i++];
      await fn(item);
    }
  }));
}

// Headers are always pre-written at run start; this just appends data rows.
function appendRow(file, row) {
  const line = Object.values(row).map(v => {
    const s = String(v ?? '');
    return (s.includes(',') || s.includes('"') || s.includes('\n'))
      ? `"${s.replace(/"/g, '""')}"` : s;
  }).join(',');
  fs.appendFileSync(file, line + '\n', 'utf8');
}

async function promptPassword(prompt) {
  const rl   = readline.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
  const orig = rl._writeToOutput.bind(rl);
  let masking = false;
  rl._writeToOutput = (s) => {
    masking ? rl.output.write(`\r${prompt}${'*'.repeat(rl.line.length)}`) : orig(s);
  };
  return new Promise(resolve => {
    rl.question(prompt, val => { rl.output.write('\n'); rl.close(); resolve(val.trim()); });
    masking = true;
  });
}

// ─── Progress tracking ─────────────────────────────────────────────────────

function loadProgress(file) {
  if (!fs.existsSync(file)) return new Set();
  try {
    const { completed } = JSON.parse(fs.readFileSync(file, 'utf8'));
    return new Set(Array.isArray(completed) ? completed : []);
  } catch { return new Set(); }
}

function saveProgress(file, done) {
  fs.writeFileSync(file, JSON.stringify({
    timestamp: new Date().toISOString(),
    completed: [...done],
  }, null, 2));
}

// ─── CA API ────────────────────────────────────────────────────────────────

async function caFetch(url, options = {}) {
  verbose(`${options.method || 'GET'} ${url}`);
  if (options.body) verbose('body:', String(options.body).slice(0, 400));

  const controller = new AbortController();
  const timeout = options.method === 'PUT' ? 180_000 : 60_000; // PUTs need longer — server processes image
  const timer = setTimeout(() => controller.abort(), timeout);
  let res;
  try {
    res = await fetch(url, { ...options, signal: controller.signal });
  } catch (e) {
    clearTimeout(timer);
    if (/abort/i.test(e.message)) throw new Error(`${options.label || options.method || 'fetch'} timeout ${timeout / 1000}s`);
    throw e;
  }
  clearTimeout(timer);
  const text = await res.text();
  verbose(`→ ${res.status}`, text.slice(0, 400));

  if (!res.ok) throw new Error(`Error ${res.status}`);
  if (!text)   return null;
  return JSON.parse(text.replace(/[\x00-\x1f\x7f]/g, ''));
}

async function getToken(baseUrl, user, password) {
  const url  = new URL('auth/login', baseUrl).toString();
  const data = await caFetch(url, {
    label: 'login',
    headers: { Authorization: 'Basic ' + Buffer.from(`${user}:${password}`).toString('base64') },
  });
  if (!data?.ok) throw new Error(`Login failed: ${JSON.stringify(data)}`);
  return data.authToken;
}

/**
 * Fetches a CA object by idno.
 * Returns { objectId, mediaCount } or null if not found.
 *
 * Uses the find endpoint (search index) + a full-record fetch for media count.
 * The direct item endpoint (item/ca_objects/idno:…) returns empty on this CA instance,
 * so we skip it entirely to avoid a wasted round-trip per row.
 */
const countReps = reps => Array.isArray(reps) ? reps.length : Object.keys(reps).length;
const objectCache = new Map(); // idno → { objectId, mediaCount } | null

async function fetchObject(baseUrl, token, idno) {
  if (objectCache.has(idno)) return objectCache.get(idno);

  // Find endpoint (search index) — reliable across all CA instances
  const idnoLower = idno.toLowerCase();
  for (const q of [`ca_objects.idno:"${idno}"`, `ca_objects.idno:"${idnoLower}"`]) {
    const fu = new URL('find/ca_objects', baseUrl);
    fu.searchParams.set('authToken', token);
    fu.searchParams.set('q', q);
    const fd      = await caFetch(fu.toString(), { label: 'findObject' });
    const results = Array.isArray(fd?.results) ? fd.results : [];
    const match   = results.find(r => (r.idno || '').toLowerCase() === idno.toLowerCase());
    if (match) {
      const fid = match.object_id || match.id;
      // Fetch the full record to get media count
      const fu2 = new URL(`item/ca_objects/id/${fid}`, baseUrl);
      fu2.searchParams.set('authToken', token);
      const fd2    = await caFetch(fu2.toString(), { label: 'getObject' });
      const result = { objectId: fid, mediaCount: countReps(fd2?.representations ?? {}) };
      objectCache.set(idno, result);
      return result;
    }
  }

  objectCache.set(idno, null);
  return null;
}

async function createRep(baseUrl, token, locale, relTypeId, objectId, row) {
  const { title, path: mediaPath, image_id, item_id } = row;
  if (!mediaPath) throw new Error('Missing path');
  if (!item_id)   throw new Error('Missing item_id');
  const payload  = {
    intrinsic_fields: {
      media:   mediaPath,
      type_id: 'front',
      ...(image_id ? { idno: image_id } : {}),
    },
    ...(title ? { preferred_labels: [{ locale, name: String(title) }] } : {}),
    related: {
      ca_objects: [
        relTypeId
          ? { object_id: Number(objectId), type_id: relTypeId }
          : { object_id: Number(objectId) },
      ],
    },
  };

  const url = new URL('item/ca_object_representations', baseUrl);
  url.searchParams.set('authToken', token);
  const data = await caFetch(url.toString(), {
    label:   'createRep',
    method:  'PUT',
    headers: { 'Content-Type': 'application/json' },
    body:    JSON.stringify(payload),
  });
  if (!data?.ok) throw new Error(JSON.stringify(data));
  return data;
}

// ─── FTP → CSV ────────────────────────────────────────────────────────────

async function generateCsvFromFtp(ftpDir, serverName) {
  const conf = SFTP_SERVERS[serverName];
  if (!conf?.host) {
    console.error(
      `SFTP server '${serverName}' not configured.\n` +
      `Set SFTP_${serverName.toUpperCase()}_HOST / _USER / _PWD in .env`
    );
    process.exit(1);
  }

  const sftp = new SftpClient();
  try {
    log(`Connecting to SFTP '${serverName}' (${conf.host})...`);
    await sftp.connect({ host: conf.host, username: conf.username, password: conf.password });

    const remotePath = `outgoing/${ftpDir}`;
    log(`Listing ${remotePath}/...`);
    const listing = await sftp.list(remotePath);

    const jpgFiles = listing
      .filter(f => f.type === '-' && f.name.toLowerCase().endsWith('.jpg'))
      .map(f => f.name)
      .sort();

    if (jpgFiles.length === 0) {
      console.error(`No .jpg files found in ${remotePath}/`);
      process.exit(1);
    }

    const ftpFilesPath = `/var/www/html/import/${ftpDir}/`;
    const lines = ['title,path,image_id,item_id'];
    for (const name of jpgFiles) {
      const imageId = name.substring(0, name.lastIndexOf('.'));
      lines.push(`${name},${ftpFilesPath}${name},${imageId},${imageId}`);
    }

    const csvPath = path.resolve(`${ftpDir}.csv`);
    fs.writeFileSync(csvPath, lines.join('\n') + '\n', 'utf8');
    log(`Generated CSV: ${csvPath} (${jpgFiles.length} rows)`);
    return csvPath;
  } finally {
    await sftp.end();
  }
}

// ─── Main ──────────────────────────────────────────────────────────────────

async function main() {
  // ── CLI args ──
  const argv = process.argv.slice(2);
  let csvPath = null, limitRows = null, startRow = 0, concurrency = 2, maxConcurrency = 10;
  let domainArg = null, resumeMode = false;
  let ftpDir = null, serverName = 'ynl';

  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--csv')             { csvPath        = argv[++i]; continue; }
    if (a === '--limit')           { limitRows      = parseInt(argv[++i], 10); continue; }
    if (a === '--start')           { startRow       = parseInt(argv[++i], 10); continue; }
    if (a === '--concurrency')     { concurrency    = parseInt(argv[++i], 10); continue; }
    if (a === '--max-concurrency') { maxConcurrency = parseInt(argv[++i], 10); continue; }
    if (a === '--domain')          { domainArg      = argv[++i]; continue; }
    if (a === '--resume')          { resumeMode     = true; continue; }
    if (a === '--ftp')             { ftpDir         = argv[++i]; continue; }
    if (a === '--server')          { serverName     = argv[++i]; continue; }
    if (a === '--verbose' || a === '-v') { verboseMode = true; continue; }
    if (a.startsWith('--ftp='))            { ftpDir         = a.slice(6);  continue; }
    if (a.startsWith('--server='))         { serverName     = a.slice(9);  continue; }
    if (a.startsWith('--csv='))             { csvPath        = a.slice(6);  continue; }
    if (a.startsWith('--limit='))           { limitRows      = parseInt(a.slice(8),  10); continue; }
    if (a.startsWith('--start='))           { startRow       = parseInt(a.slice(8),  10); continue; }
    if (a.startsWith('--concurrency='))     { concurrency    = parseInt(a.slice(14), 10); continue; }
    if (a.startsWith('--max-concurrency=')) { maxConcurrency = parseInt(a.slice(19), 10); continue; }
    if (a.startsWith('--domain='))          { domainArg      = a.slice(9);  continue; }
    if (!a.startsWith('-'))                 { csvPath = a; continue; } // positional
    warn(`Unknown argument ignored: ${a}`);
  }

  if (ftpDir && csvPath) {
    console.error('Cannot use both --ftp and --csv');
    process.exit(1);
  }

  if (ftpDir) {
    if (!(serverName in SFTP_SERVERS)) {
      console.error(`Unknown server: ${serverName}. Valid choices: ${Object.keys(SFTP_SERVERS).join(', ')}`);
      process.exit(1);
    }
    csvPath = await generateCsvFromFtp(ftpDir, serverName);
  }

  if (!csvPath) {
    console.error(
      'Usage: node import.js (--ftp <dir> [--server ynl|eretz] | --csv <file.csv>)\n' +
      '       [--start N] [--limit N] [--concurrency N] [--max-concurrency N]\n' +
      '       [--domain host] [--resume] [-v]'
    );
    process.exit(1);
  }

  // ── Resolve credentials ──
  if (domainArg && !process.env.CA_BASE_URL) {
    const host = domainArg.replace(/^https?:\/\//, '').replace(/\/$/, '');
    process.env.CA_BASE_URL = `https://${host}/service.php/json/`;
  }

  const CA_BASE_URL = process.env.CA_BASE_URL;
  const CA_USER     = process.env.CA_USER || 'administrator';
  const CA_LOCALE   = process.env.CA_LOCALE   || 'he_IL';
  const REL_TYPE_ID = process.env.REL_TYPE_ID ? parseInt(process.env.REL_TYPE_ID, 10) : 135;
  let   CA_PASSWORD = process.env.CA_PASSWORD;

  if (!CA_BASE_URL) { console.error('Set CA_BASE_URL in .env or pass --domain <host>'); process.exit(1); }

  if (!CA_PASSWORD) {
    if (!process.stdin.isTTY) {
      console.error('CA_PASSWORD not set and stdin is not a TTY. Set it in .env');
      process.exit(1);
    }
    CA_PASSWORD = await promptPassword(`Password for ${CA_USER}: `);
  }

  // ── Load CSV ──
  const absCsv = path.resolve(csvPath);
  if (!fs.existsSync(absCsv)) { console.error(`File not found: ${absCsv}`); process.exit(1); }

  let rows = parseCsv(fs.readFileSync(absCsv, 'utf8'), {
    columns: true, trim: true, skip_empty_lines: true,
  });
  log(`Loaded ${rows.length} rows from ${absCsv}`);

  if (startRow > 0) {
    rows = rows.slice(startRow);
    log(`--start: skipping first ${startRow} rows (${rows.length} remaining)`);
  }

  if (limitRows > 0) {
    rows = rows.slice(0, limitRows);
    log(`--limit: processing ${rows.length} rows`);
  }

  // ── Resume ──
  const outDir       = path.dirname(absCsv);
  const progressFile = path.join(outDir, 'import.progress.json');
  const logFile      = path.join(outDir, 'import.log.csv');
  const failedFile   = path.join(outDir, 'import.failed.csv');
  const logHeader    = 'item_id,image_id,status,reason,rep_id';
  const csvHeader    = 'title,path,image_id,item_id';

  // Pre-write headers; appendRow() never needs to check for file existence
  fs.writeFileSync(logFile,    logHeader + '\n', 'utf8');
  fs.writeFileSync(failedFile, csvHeader + '\n', 'utf8');

  const done = resumeMode ? loadProgress(progressFile) : new Set();
  if (resumeMode && done.size > 0) log(`--resume: skipping ${done.size} already-completed rows`);

  const pending = rows.filter(r => !done.has(r.item_id));
  log(`Rows to process: ${pending.length}  (concurrency=${concurrency}, max=${maxConcurrency})`);
  if (pending.length === 0) { log('Nothing to do.'); return; }

  // ── Authenticate ──
  log(`\nAuthenticating with ${CA_BASE_URL}...`);
  let token = await getToken(CA_BASE_URL, CA_USER, CA_PASSWORD);
  log('OK');

  // ── Museum sanity check ──
  // The find API filters results by the logged-in user's museum. If the user is assigned
  // to the wrong museum they'll see zero results and all rows will be skipped as "not found".
  // Detect this early and bail with a clear message rather than silently wasting a run.
  {
    const checkUrl = new URL('find/ca_objects', CA_BASE_URL);
    checkUrl.searchParams.set('q', '*');
    checkUrl.searchParams.set('limit', '1');
    checkUrl.searchParams.set('authToken', token);
    const checkData = await caFetch(checkUrl.toString(), { label: 'museumCheck' });
    if (!checkData?.total) {
      console.error(`
ERROR: User '${CA_USER}' can see 0 objects in CollectiveAccess.
       This almost always means the user is assigned to the wrong museum.

       Fix: log in to CA and change user '${CA_USER}' to museum YBZ (23)
`);
      process.exit(1);
    }
    verbose(`Museum check OK — user can see ${checkData.total} objects`);
    log('');
  }

  // Helpers that close over `token` and refresh it on 401
  async function withTokenRefresh(fn) {
    try { return await fn(); }
    catch (e) {
      if (/401|Unauthorized/i.test(e.message)) {
        verbose('Token expired — refreshing');
        token = await getToken(CA_BASE_URL, CA_USER, CA_PASSWORD);
        return await fn();
      }
      throw e;
    }
  }

  // ── Graceful shutdown on Ctrl+C ──
  process.on('SIGINT', () => {
    progress?.finish();
    progress = null;
    console.log('\nInterrupted — saving progress...');
    saveProgress(progressFile, done);
    const resumeHint = ftpDir
      ? `node import.js --ftp "${ftpDir}" --server ${serverName} --resume`
      : `node import.js --csv "${csvPath}" --resume`;
    console.log(`${done.size}/${rows.length} rows saved. Resume with: ${resumeHint}`);
    process.exit(0);
  });

  // ── Process rows ──
  const queue      = new AdaptiveQueue({ min: 1, max: maxConcurrency, initial: concurrency });
  const inProgress = new Set(); // item_ids currently mid-upload; guards concurrent duplicate rows
  let imported = 0, skippedNoObject = 0, skippedHasMedia = 0, errors = 0;
  const importedItems = []; // { itemId, objectId } — for post-import verification
  progress = new ProgressBar(pending.length, () => queue.status);

  function logRow(item_id, image_id, status, reason, rep_id = '') {
    appendRow(logFile, { item_id, image_id: image_id ?? '', status, reason, rep_id });
  }

  function writeFailed(row) {
    appendRow(failedFile, { title: row.title ?? '', path: row.path ?? '', image_id: row.image_id ?? '', item_id: row.item_id ?? '' });
  }

  await Promise.all(
    pending.map((row, rowIdx) => {
      const n = rowIdx + 1;
      return queue.run(async () => {
        const idx = `[${n}/${pending.length}]`;

        // ── 1. Fetch object (id + media count) in one call ──
        let obj;
        try {
          obj = await withTokenRefresh(() => fetchObject(CA_BASE_URL, token, String(row.item_id)));
        } catch (e) {
          if (isOverloadErr(e)) { verbose(`${idx} [RETRY] fetch overload: ${e.message.slice(0, 80)}`); throw e; }
          warn(`${idx} [ERR] item_id=${row.item_id} → ${e.message}`);
          errors++;
          logRow(row.item_id, row.image_id, 'error', e.message);
          writeFailed(row);
          progress.tick();
          return;
        }

        if (obj == null) {
          log(`${idx} [SKIP] item_id=${row.item_id} — object not found in CA`);
          skippedNoObject++;
          done.add(row.item_id);
          logRow(row.item_id, row.image_id, 'skipped', 'object not found in CA');
          if (n % 10 === 0) saveProgress(progressFile, done);
          progress.skip();
          return 'skip';
        }

        // ── 2. Skip if already has media ──
        if (obj.mediaCount > 0) {
          log(`${idx} [SKIP] item_id=${row.item_id} — already has ${obj.mediaCount} image(s)`);
          skippedHasMedia++;
          done.add(row.item_id);
          logRow(row.item_id, row.image_id, 'skipped', `already has ${obj.mediaCount} image(s)`);
          if (n % 10 === 0) saveProgress(progressFile, done);
          progress.skip();
          return 'skip';
        }

        // ── 3. Attach the image ──
        // Guard against the same item_id appearing twice in the CSV (concurrent or sequential).
        // The check+add is synchronous so no concurrent task can slip through between them.
        const itemKey = String(row.item_id);
        if (inProgress.has(itemKey)) {
          log(`${idx} [SKIP] item_id=${row.item_id} — duplicate row, upload already in progress`);
          skippedHasMedia++;
          logRow(row.item_id, row.image_id, 'skipped', 'duplicate row');
          progress.skip();
          return 'skip';
        }
        inProgress.add(itemKey);
        try {
          const res = await withTokenRefresh(() =>
            createRep(CA_BASE_URL, token, CA_LOCALE, REL_TYPE_ID, obj.objectId, row)
          );
          const repId = res?.representation_id ?? res?.id ?? '?';
          log(`${idx} [OK] item_id=${row.item_id} "${row.title ?? ''}" → rep_id=${repId}`);
          imported++;
          importedItems.push({ itemId: String(row.item_id), objectId: obj.objectId });
          done.add(row.item_id);
          // Update cache so any later row for this item_id sees mediaCount > 0 and skips
          objectCache.set(itemKey, { objectId: obj.objectId, mediaCount: 1 });
          logRow(row.item_id, row.image_id, 'imported', '', repId);
          if (n % 10 === 0) saveProgress(progressFile, done);
        } catch (e) {
          // Never retry createRep — the server may still be processing the first
          // request even after a timeout/500. Retrying risks creating duplicates.
          // Instead, write to failed.csv — the next --resume run will re-check
          // and skip the item if the server did create the rep.
          warn(`${idx} [ERR] item_id=${row.item_id} → ${e.message}`);
          errors++;
          logRow(row.item_id, row.image_id, 'error', e.message);
          writeFailed(row);
          // Signal queue to back off concurrency (server is struggling)
          // but don't retry this task
          if (isOverloadErr(e)) throw { backOffOnly: true };
        }
        inProgress.delete(itemKey);
        progress.tick();
      });
    })
  );

  // ── Summary ──
  progress.finish();
  progress = null;
  saveProgress(progressFile, done);

  log('\n' + '─'.repeat(60));
  log(`Imported        : ${imported}`);
  log(`Skipped (no obj): ${skippedNoObject}  (item_id not found in CA)`);
  log(`Skipped (media) : ${skippedHasMedia}  (object already has images)`);
  log(`Errors          : ${errors}`);
  log(`Full log        : ${logFile}`);
  if (errors > 0) log(`Retry failed    : node import.js --csv "${failedFile}"  (${errors} rows)`);

  if (done.size === rows.length) {
    if (fs.existsSync(progressFile)) fs.unlinkSync(progressFile);
  }

  // ── Post-import verification ──
  // Re-fetch every item imported in this run (bypasses objectCache) and confirm
  // each has exactly one representation. Reports [MISSING] or [DUPLICATE] if not.
  if (importedItems.length > 0) {
    log(`\nVerifying ${importedItems.length} imported item(s)...`);
    const anomalies = [];
    let verified = 0;

    await runConcurrent(importedItems, 10, async ({ itemId, objectId }) => {
      try {
        const url = new URL(`item/ca_objects/id/${objectId}`, CA_BASE_URL);
        url.searchParams.set('authToken', token);
        const data = await withTokenRefresh(() => caFetch(url.toString(), { label: 'verify' }));
        const count = countReps(data?.representations ?? {});
        if (count !== 1) anomalies.push({ itemId, objectId, count });
      } catch (e) {
        anomalies.push({ itemId, objectId, count: -1, error: e.message });
      }
      verified++;
      if (verified % 50 === 0) log(`  verified ${verified}/${importedItems.length}...`);
    });

    if (anomalies.length === 0) {
      log(`Verification OK : all ${importedItems.length} items have exactly 1 representation`);
    } else {
      log(`Verification issues (${anomalies.length}):`);
      for (const { itemId, objectId, count, error } of anomalies) {
        if (error) {
          log(`  [ERROR]     item_id=${itemId} (object_id=${objectId}) — ${error}`);
        } else if (count === 0) {
          log(`  [MISSING]   item_id=${itemId} (object_id=${objectId}) — 0 representations`);
        } else {
          log(`  [DUPLICATE] item_id=${itemId} (object_id=${objectId}) — ${count} representations`);
        }
      }
    }
  }
}

main().catch(e => { console.error('\nFatal:', e.message); process.exit(1); });
