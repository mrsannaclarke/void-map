/***** CONFIG *****/
const SPREADSHEET_ID = '1Bdqi7a_3M10pkSNcGSnCpwdLaQ-7XhMQvQT-cewgGmw';
const SHEET_NAME = 'occupants';
const SHEET_RAW_NAME = 'bookings_raw';
const RAW_HEADER = [
  'ref', 'space_key', 'subject', 'from', 'date',
  'display', 'social', 'status', 'ticket',
  'lead_name', 'lead_email', 'lead_phone',
  'public_team_name', 'public_social',
  'group_address', 'booking_contact_email',
  'thread_id', 'message_id',
  'kv_json', 'updated_at'
];

// Label name candidates (try these in order; use the first that exists)
const L_CONFIRM_CANDIDATES = [
  '2026-admin-bookwhen---confirmations',
  '2026/Admin/Bookwhen / confirmations',
  '2026/Admin/Bookwhen/confirmations'
];
const L_PARSED_CANDIDATES = [
  '2026-admin-bookwhen---confirmations-bookwhen---parsed',
  '2026/Admin/Bookwhen / confirmations/Bookwhen/Parsed',
  '2026/Admin/Bookwhen/confirmations/Bookwhen/Parsed'
];

// Max threads per run
const PAGE_SIZE = 100;

// -----------------------------------------------------------
/***** MASTER CAPACITY + TICKET LOGIC (shared with site) *****/
// Base capacities by booth_type (not used to color here; front-end uses map JSON for type → color)
// Kept for future server-side fullness if desired.
const CAPACITY_BY_TYPE = {
  SDL: 1,
  MDL: 3,
  LDL: 4,
  XLDL: 8,
  STD10IN: 2,
  STD10TENT: 2,
  VND10TENT: 1,
  VND10OUT: 1,
  VND10IN: 1,
  VND10STREET: 1
};

// Per-space overrides and flags
const CAPACITY_OVERRIDES = {
  // e.g., '131': 4
};
const SPACE_FLAGS = {
  // Buy-out only test rooms (SSDL)
  '117': { buyoutOnly: true },
  '217': { buyoutOnly: true }
};

// Rooms that are used as sleepers (privacy: do not expose names/social publicly)
const SLEEPER_KEYS = new Set([
  '117','202','203','204','205','206','207','208','210','212','213','214','215','216',
  '217','218','219','220','221','222','223','224','231','233','235','239','244','247',
  '249','251','253'
]);

// Ticket patterns → slot cost and buyout flag
const TICKET_SLOTCOST_RULES = [
  { rx: /buy[-\s]?out.*full\s*booth/i, slots: null, buyout: true },      // Buy-out (Full Booth)
  { rx: /\bhalf\s*booth\s*\(4\s*artists?\)/i, slots: 4 },                 // Half Booth (4 Artists) — room 108
  { rx: /\bduo\s*\(?.*2\s*artist\s*slots?\)?/i, slots: 2 },               // Duo (2 Artist Slots)
  { rx: /\bvendor\s*booth\b/i, slots: 1 },                                // Vendor Booth
  { rx: /\bsingle\s*artist\s*slot\b/i, slots: 1 },                        // Single Artist Slot
  { rx: /.*/, slots: 1 }
];

function slotCostForTicketServer_(ticketName) {
  const name = String(ticketName || '').trim();
  for (const rule of TICKET_SLOTCOST_RULES) {
    if (rule.rx.test(name)) return { slots: rule.slots ?? 0, buyout: !!rule.buyout };
  }
  return { slots: 1, buyout: false };
}
// -----------------------------------------------------------
/***** ENTRY POINT *****/
function runParser() {
  // Prevent concurrent runs: try to acquire a script lock for up to 5 minutes
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5 * 60 * 1000)) {
    console.log('runParser: could not acquire lock, another run is in progress. Exiting.');
    return;
  }

  try {
    const sheet = getOrInitSheet_();
    const rawSheet = getOrInitRawSheet_();
    const rawsByRef = readExistingByRef_(rawSheet); // { ref -> rowIndex }
    const rawUpdates = []; // rows matching RAW_HEADER
    const confirmLabel = getOrCreateLabel_(L_CONFIRM_CANDIDATES);
    const threads = fetchThreads_();
    if (!threads.length) return;

    const parsedLabel = getOrCreateLabel_(L_PARSED_CANDIDATES);
    const rowsByKey = readExisting_(sheet); // { space_key -> rowIndex }

    const updates = []; // [ [space_key, display, social, status, ticket, lead_name, last_ref, updated_at] ]

    threads.forEach(thread => {
      try {
        const msgs = thread.getMessages();
        const msg = msgs[msgs.length - 1]; // newest
        const subj = msg.getSubject() || '';
        const bodyHtml = msg.getBody() || '';
        const bodyText = msg.getPlainBody() || '';

        const refCode  = extractRef_(subj);
        const rawKey   = extractSpaceKey(subj, bodyHtml, bodyText) || extractSpaceKey('', bodyHtml, bodyText);
        const spaceKey = toSiteKey_(rawKey);

        // normalize accidental whitespace in keys
        const _spaceKey = (spaceKey || '').trim();

        const table    = extractBookingFields_(bodyHtml, bodyText);
        const leadName = table.leadName || extractLeadNameFallback_(bodyText, bodyHtml);

        const display  = pickDisplay(table.publicTeamName, leadName);
        const social   = normalizeSocial(table.publicSocial);
        const ticket   = table.ticket || extractTicket_(bodyHtml, bodyText);

        // Auto-status: Buy-out implies full; buyoutOnly rooms full on any booking
        let status = '';
        if (/buy-?out/i.test(ticket)) status = 'full';
        const flags = SPACE_FLAGS[_spaceKey] || {};
        if (!status && flags.buyoutOnly && _spaceKey) {
          status = (display || social) ? 'full' : '';
        }

        const updatedAt = new Date();
        if (_spaceKey) {
          if (SLEEPER_KEYS.has(_spaceKey)) {
            // Privacy: mark occupied without exposing any PII in the public occupants sheet
            updates.push([_spaceKey, '', '', 'full', '', '', refCode, updatedAt]);
          } else {
            updates.push([_spaceKey, display, social, status, ticket, leadName, refCode, updatedAt]);
          }
        }

        // ----- Collect extended/raw fields for archival -----
        const fromHdr   = msg.getFrom() || '';
        const msgDate   = msg.getDate ? msg.getDate() : msg.getDateSent && msg.getDateSent();
        const leadEmail = extractLeadEmail_(bodyHtml, bodyText) || extractEmailNearLabel_(bodyHtml, 'Lead Artist');
        const leadPhone = extractLeadPhone_(bodyHtml, bodyText);
        const groupAddr = extractGroupAddress_(bodyHtml, bodyText);
        const bookingContactEmail = extractBookingContactEmail_(bodyHtml, bodyText);

        const kvPairs = extractAllKVPairs_(bodyHtml); // generic table scrape → {label: value}
        const kvJson  = JSON.stringify(kvPairs || {}, null, 0);

        const messageId = (msg.getId && msg.getId()) || '';
        const threadId  = (thread.getId && thread.getId()) || '';

        rawUpdates.push([
          refCode, _spaceKey, subj, fromHdr, msgDate || updatedAt,
          display, social, status, ticket,
          leadName, leadEmail, leadPhone,
          table.publicTeamName, table.publicSocial,
          groupAddr, bookingContactEmail,
          threadId, messageId,
          kvJson, updatedAt
        ]);

        thread.addLabel(parsedLabel);
        // Remove the original confirmations label after successful parse
        try { thread.removeLabel(confirmLabel); } catch (_) {}
      } catch (e) {
        console.warn('Parser error on thread:', thread.getFirstMessageSubject(), e);
      }
    });

    upsertRows_(sheet, rowsByKey, updates);
    upsertRawRows_(rawSheet, rawsByRef, rawUpdates);
  } finally {
    try { lock.releaseLock(); } catch (e) { /* ignore */ }
  }
}

function getOrInitRawSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(SHEET_RAW_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_RAW_NAME);
  const have = sh.getRange(1,1,1,RAW_HEADER.length).getValues()[0];
  const needs = RAW_HEADER.some((h,i)=> (have[i]||'') !== h);
  if (needs) {
    sh.clear();
    sh.getRange(1,1,1,RAW_HEADER.length).setValues([RAW_HEADER]);
  }
  return sh;
}
function readExistingByRef_(sheet) {
  const lastRow = sheet.getLastRow();
  const map = {};
  if (lastRow < 2) return map;
  const vals = sheet.getRange(2,1,lastRow-1,1).getValues(); // col A: ref
  vals.forEach((r, idx) => {
    const ref = String(r[0] || '').trim();
    if (ref) map[ref] = idx + 2;
  });
  return map;
}
function upsertRawRows_(sheet, rowsByRef, updates) {
  if (!updates.length) return;
  updates.forEach(row => {
    const ref = String(row[0] || '').trim();
    if (!ref) return;
    const rowIdx = rowsByRef[ref];
    if (rowIdx) {
      sheet.getRange(rowIdx, 1, 1, row.length).setValues([row]);
    } else {
      const next = sheet.getLastRow() + 1;
      sheet.getRange(next, 1, 1, row.length).setValues([row]);
      rowsByRef[ref] = next;
    }
  });
}

/***** LABELS & FETCH (label-object based) *****/
function getOrCreateLabel_(nameOrCandidates) {
  const candidates = Array.isArray(nameOrCandidates) ? nameOrCandidates : [nameOrCandidates];
  // 1) Try to find any existing label by exact candidate names
  for (let i = 0; i < candidates.length; i++) {
    const cand = (candidates[i] || '').trim();
    if (!cand) continue;
    const lbl = GmailApp.getUserLabelByName(cand);
    if (lbl) return lbl;
  }
  // 2) Try to create the first candidate; if conflict, try the next
  let lastError = null;
  for (let i = 0; i < candidates.length; i++) {
    const cand = (candidates[i] || '').trim();
    if (!cand) continue;
    try {
      return GmailApp.createLabel(cand);
    } catch (e) {
      lastError = e;
      // conflict? try next candidate before failing
    }
  }
  throw lastError || new Error('Unable to create or find label for candidates: ' + candidates.join(' | '));
}

/**
 * Fetch recent threads directly from the confirmations label (no search string).
 * Excludes threads already carrying the Parsed sublabel.
 * Applies a 30-day date cutoff using message timestamps.
 */
function fetchThreads_() {
  const confirmLabel = getOrCreateLabel_(L_CONFIRM_CANDIDATES);
  const parsedLabel  = getOrCreateLabel_(L_PARSED_CANDIDATES);

  // Page from the label
  let threads = confirmLabel.getThreads(0, PAGE_SIZE);

  // Exclude those already parsed
  const parsedName = parsedLabel.getName();
  threads = threads.filter(t => !t.getLabels().some(l => l.getName() === parsedName));

  // Enforce rolling window (last 30 days)
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 30);
  threads = threads.filter(t => t.getLastMessageDate() >= cutoff);

  return threads;
}

/***** SHEET HELPERS *****/
function getOrInitSheet_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  let sh = ss.getSheetByName(SHEET_NAME);
  if (!sh) sh = ss.insertSheet(SHEET_NAME);
  const header = ['space_key','display','social','status','ticket','lead_name','last_ref','updated_at'];
  const have = sh.getRange(1,1,1,header.length).getValues()[0];
  const needs = header.some((h,i)=> (have[i]||'') !== h);
  if (needs) {
    sh.clear();
    sh.getRange(1,1,1,header.length).setValues([header]);
  }
  return sh;
}
function readExisting_(sheet) {
  const lastRow = sheet.getLastRow();
  const map = {};
  if (lastRow < 2) return map;
  const vals = sheet.getRange(2,1,lastRow-1,1).getValues(); // col A: space_key
  vals.forEach((r, idx) => {
    const key = String(r[0] || '').trim();
    if (key) map[key] = idx + 2; // 1-based row index
  });
  return map;
}
function upsertRows_(sheet, rowsByKey, updates) {
  if (!updates.length) return;
  updates.forEach(row => {
    const [space_key] = row;
    if (!space_key) return;
    const rowIdx = rowsByKey[space_key];
    if (rowIdx) {
      sheet.getRange(rowIdx, 1, 1, row.length).setValues([row]);
    } else {
      const next = sheet.getLastRow() + 1;
      sheet.getRange(next, 1, 1, row.length).setValues([row]);
      rowsByKey[space_key] = next;
    }
  });
}

/***** EXTRACTION *****/
function extractRef_(subject) {
  // [Bookwhen] New booking. Ref: WYECW - …
  const m = String(subject||'').match(/\bRef:\s*([A-Z0-9]+)\b/i);
  return m ? m[1].toUpperCase() : '';
}

function extractTicket_(html, text) {
  const H = String(html || '').replace(/\s+/g, ' ');
  const T = String(text || '');
  // Prefer exact products you use
  const CANDIDATES = [
    /Buy[-\s]?out\s*\(Full\s*Booth\)/i,
    /Half\s*Booth\s*\(4\s*Artists\)/i,
    /Duo\s*\(2\s*Artist\s*Slots\)/i,
    /Single\s*Artist\s*Slot/i,
    /Vendor\s*Booth/i
  ];
  for (const rx of CANDIDATES) {
    let m = H.match(rx); if (m) return m[0].trim();
    m = T.match(rx);     if (m) return m[0].trim();
  }
  // Fallback: search within a Tickets section
  let m = H.match(/>Tickets?<\/?[^>]*>.*?<\/(?:td|div)>/i);
  if (m) {
    const block = m[0].replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
    const rxAny = /\b(Buy[-\s]?out\s*\(Full\s*Booth\)|Half\s*Booth\s*\(4\s*Artists\)|Duo\s*\(2\s*Artist\s*Slots\)|Single\s*Artist\s*Slot|Vendor\s*Booth)\b/i;
    const t = block.match(rxAny);
    if (t) return t[1].trim();
  }
  return '';
}

// tolerant to many subject variants you use
function extractSpaceKey(subject, bodyHtml, bodyText) {
  const norm = s => String(s || '')
    .replace(/[\u2012\u2013\u2014\u2212]/g, '-') // normalize en/em/minus dash to hyphen
    .replace(/\s+/g, ' ')
    .trim();

  const s = norm(subject);

  // Arium / Flex Lounge A#
  let m = s.match(/\b(?:Arium|Flex\s*Lounge)\s*A(\d{1,2})\b/i);
  if (m) return 'A' + m[1];

  // Dream Tent Vendor (letter code 1–2 chars: G, HH, II, FF)
  m = s.match(/\bDream\s*Tent\s*Vendor\s+([A-Z]{1,2})\b/i);
  if (m) return 'DTV-' + m[1].toUpperCase();

  // Dream Tent numbered
  m = s.match(/\bDream\s*Tent\s+(\d{1,2})\b/i);
  if (m) return 'DT' + m[1];

  // Street Fair vendors (numbered)
  m = s.match(/\bVendor\s+(\d{1,2})\s*-\s*Saturday\s*Street\s*Fair\b/i);
  if (m) return 'SF' + m[1];

  // Building vendors (letters only, 1–2 chars) for Jupiter Original/Next
  m = s.match(/\bVendor\s+([A-Z]{1,2})\s*-\s*Jupiter\s+Original\b/i);
  if (m) return 'VO-' + m[1].toUpperCase();

  m = s.match(/\bVendor\s+([A-Z]{1,2})\s*-\s*Jupiter\s+Next\b/i);
  if (m) return 'VN-' + m[1].toUpperCase();

  // Plain room numbers (Small/Medium/Large Deluxe lines)
  m = s.match(/\b(\d{3})\b/);
  if (m) return m[1];

  // ---- Fallbacks to body text if subject was odd ----
  const t = norm(bodyText);
  m = t.match(/\bRoom\s+(\d{3})\b/i);                 if (m) return m[1];
  m = t.match(/\bArium\s*A(\d{1,2})\b/i);             if (m) return 'A' + m[1];
  m = t.match(/\bFlex\s*Lounge\s*A(\d{1,2})\b/i);     if (m) return 'A' + m[1];
  m = t.match(/\bDream\s*Tent\s*Vendor\s+([A-Z]{1,2})\b/i);
  if (m) return 'DTV-' + m[1].toUpperCase();
  m = t.match(/\bDream\s*Tent\s+(\d{1,2})\b/i);       if (m) return 'DT' + m[1];
  m = t.match(/\bVendor\s+(\d{1,2})\b.*Street\s*Fair/i);
  if (m) return 'SF' + m[1];
  m = t.match(/\bVendor\s+([A-Z]{1,2})\s*-\s*Jupiter\s*Original\b/i);
  if (m) return 'VO-' + m[1].toLowerCase();
  m = t.match(/\bVendor\s+([A-Z]{1,2})\s*-\s*Jupiter\s*Next\b/i);
  if (m) return 'VN-' + m[1].toLowerCase();

  return '';
}

function toSiteKey_(k) {
  if (!k) return k;

  // 3-digit hotel rooms => "250", "254" (no underscore; matches CSV "shape name")
  if (/^\d{3}$/.test(k)) return k;

  // A-series (Arium / Flex Lounge) => "a1".."a13"
  if (/^A\d{1,2}$/.test(k)) return k.toLowerCase();

  // Dream Tent vendors => single/double letters a..g (DTV-G -> "g", DTV-HH -> "hh")
  if (/^DTV-[A-Z]{1,2}$/.test(k)) return k.slice(4).toLowerCase();

  // Dream Tent numbered => plain numbers "1".."6" to match CSV
  if (/^DT\d{1,2}$/.test(k)) return String(parseInt(k.slice(2), 10));

  // Street Fair vendors => "sf1".."sf11"
  if (/^SF\d{1,2}$/.test(k)) return k.toLowerCase();

  // Jupiter Original/Next vendors:
  // - Original (outdoor) uses single letters "h".."n" in CSV/SVG
  // - Next (indoor) uses double letters "aa".."ii"
  if (/^(VO|VN)-[A-Z]{1,2}$/.test(k)) return k.replace(/^(VO|VN)-/, '').toLowerCase();

  return k;
}

// Parse the “booking details” table
function extractBookingFields_(html, text) {
  const out = {
    publicTeamName: '',
    publicSocial: '',
    leadName: '',
    ticket: ''
  };
  const H = String(html || '');
  const T = String(text || '');

  // Lead name (robust): prefer the human name preceding an email, else fallback
  out.leadName = extractLeadName_(H, T);

  // Public Team/Group/Shop Name
  out.publicTeamName = extractPublicTeamName_(H, T);

  // Public Instagram (preferred) or website
  out.publicSocial   = extractPublicSocial_(H, T);

  // Tickets block (best-effort)
  out.ticket = extractTicket_(H, T);

  return out;
}

// Grab the value to the right of a label (table or text)
function captureRightOfLabel_(src, labelRx) {
  if (!src) return '';
  // HTML table cell pattern
  let m = src.match(new RegExp(labelRx.source + '.*?</td>\\s*<td[^>]*>(.*?)</td>', 'i'));
  if (m) return stripTags_(m[1]).trim();

  // Generic text “Label    value”
  const s = stripTags_(src).replace(/\r/g,'').split('\n');
  for (let i=0;i<s.length;i++) {
    if (labelRx.test(s[i])) {
      for (let j=i+1;j<s.length;j++) {
        const v = s[j].trim();
        if (v) return v;
      }
    }
  }
  return '';
}

function stripTags_(html) {
  return String(html || '').replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
}

function pickDisplay(publicName, leadName) {
  const bad = /^(public|team\/?group\/?shop\s*name|phone\s*number|\-+|n\/a|\s*)$/i;
  const pn = String(publicName || '').trim();
  if (pn && !bad.test(pn)) return pn;
  const ln = String(leadName || '').trim();
  if (ln && !bad.test(ln)) return ln;
  return '';
}

function normalizeSocial(raw) {
  let s = String(raw || '').trim();
  if (!s) return '';
  if (/^(public|instagram|website|n\/a|\-+)$/i.test(s)) return '';

  const lower = s.toLowerCase();

  // Instagram URLs → @handle
  let m = lower.match(/(?:https?:\/\/)?(?:www\.)?instagram\.com\/([a-z0-9._]+)/i);
  if (m) return '@' + m[1];

  // "instagram anatomytattoo"
  m = lower.match(/\binstagram\b[^@a-z0-9._-]*([a-z0-9._]+)/i);
  if (m) return '@' + m[1];

  // Raw handle with/without @
  m = lower.match(/^@?([a-z0-9._]{2,})$/i);
  if (m) return '@' + m[1];

  // Otherwise, return just the domain if it's a URL
  const domain = s.replace(/^https?:\/\//i,'').replace(/^www\./i,'').split(/[\/?#]/)[0];
  if (domain.includes('.')) return domain;

  return '';
}

function extractLeadNameFallback_(text, html) {
  // “Booking contact … Name” fallback
  const T = String(text || '');
  let m = T.match(/Booking\s+contact.*?\b([A-Z][A-Za-z'.\-]+\s+[A-Z][A-Za-z'.\-]+)\b/i);
  if (m) return m[1].trim();

  // Try any bold-like name line in HTML
  const H = String(html || '');
  m = H.replace(/\s+/g,' ').match(/>\s*([A-Z][A-Za-z'.\-]+\s+[A-Z][A-Za-z'.\-]+)\s*<\/(?:strong|b)>/i);
  if (m) return m[1].trim();

  return '';
}

/***** NEW HELPERS *****/
function extractLeadName_(html, text) {
  const H = String(html || '').replace(/\s+/g,' ');
  const T = String(text || '');

  // 1) Try "Booking contact:" line → Name before an email address
  let m = H.replace(/<[^>]+>/g,' ').match(/\b([A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+)+)\b\s+[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/);
  if (m && !/Void\s*Tattoo\s*Fest/i.test(m[1])) return m[1].trim();

  // 2) Try text body variant
  m = T.match(/\b([A-Z][\w'.-]+(?:\s+[A-Z][\w'.-]+)+)\b\s+[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b/);
  if (m && !/Void\s*Tattoo\s*Fest/i.test(m[1])) return m[1].trim();

  // 3) Fallback: first two-word capitalized sequence near "Lead Artist"
  m = H.match(/Lead\s+Artist[^<]*?\b([A-Z][\w'.-]+)\s+([A-Z][\w'.-]+)\b/);
  if (m) return (m[1] + ' ' + m[2]).trim();

  return '';
}

function extractPublicTeamName_(html, text) {
  // Require the full label phrase on the left cell
  const rowRx = /<td[^>]*>\s*\(Public\)\s*Team\/?Group\/?Shop\s*Name\s*<\/td>\s*<td[^>]*>(.*?)<\/td>/i;
  let m = String(html || '').match(rowRx);
  if (m) return stripTags_(m[1]).trim();

  // Text fallback: exact phrase, capture same-line or next non-empty line
  const lines = String(text || '').split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    if (/\(Public\)\s*Team\/?Group\/?Shop\s*Name/i.test(lines[i])) {
      const same = lines[i].replace(/\(Public\)\s*Team\/?Group\/?Shop\s*Name/i,'').trim();
      if (same) return same;
      for (let j=i+1; j<lines.length; j++) {
        const v = lines[j].trim();
        if (v) return v;
      }
    }
  }
  return '';
}

function extractPublicSocial_(html, text) {
  const rowRx = /<td[^>]*>\s*\(Public\)\s*Instagram\s*\(preferred\)\s*or\s*website\s*<\/td>\s*<td[^>]*>(.*?)<\/td>/i;
  let m = String(html || '').match(rowRx);
  if (m) return stripTags_(m[1]).trim();

  const lines = String(text || '').split(/\r?\n/);
  for (let i = 0; i < lines.length; i++) {
    if (/\(Public\)\s*Instagram/i.test(lines[i]) && /or\s*website/i.test(lines[i])) {
      const same = lines[i].replace(/\(Public\)\s*Instagram.*?or\s*website/i,'').trim();
      if (same) return same;
      for (let j=i+1; j<lines.length; j++) {
        const v = lines[j].trim();
        if (v) return v;
      }
    }
  }
  return '';
}

/***** OPTIONAL DIAGNOSTICS *****/
function debugByLabelObjects() {
  const confirmLabel = getOrCreateLabel_(L_CONFIRM_CANDIDATES);
  const parsedLabel  = getOrCreateLabel_(L_PARSED_CANDIDATES);
  const threads = confirmLabel.getThreads(0, 10);
  console.log('Confirm label name:', confirmLabel.getName(), 'Threads:', threads.length);
  threads.forEach((t,i) => {
    const subj = t.getFirstMessageSubject();
    const hasParsed = t.getLabels().some(l => l.getName() === parsedLabel.getName());
    console.log(`${i+1}.`, subj, 'parsed?', hasParsed, 'last:', t.getLastMessageDate());
  });
}
/***** NEW HELPERS *****/
function extractLeadEmail_(html, text) {
  const H = String(html || '').replace(/<[^>]+>/g,' ');
  const m = H.match(/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/);
  return m ? m[0] : '';
}
function extractEmailNearLabel_(html, leftLabel) {
  const rx = new RegExp(leftLabel.replace(/[.*+?^${}()|[\]\\]/g,'\\$&') + '.*?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,})','i');
  const H = String(html || '').replace(/\s+/g,' ');
  const m = H.match(rx);
  return m ? m[1] : '';
}
function extractLeadPhone_(html, text) {
  const H = String(html || '');
  // look for the exact left cell then the right cell value
  let m = H.match(/<td[^>]*>\s*Lead\s+Artist\s+Phone\s+Number\s*<\/td>\s*<td[^>]*>(.*?)<\/td>/i);
  if (m) return stripTags_(m[1]);
  // fallback: any phone-like pattern
  const T = String(text || '');
  m = T.match(/(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
  return m ? m[0] : '';
}
function extractGroupAddress_(html, text) {
  let m = String(html || '').match(/<td[^>]*>\s*Lead\s+Artist\s+Business\s+or\s+Group\s+Address\s*<\/td>\s*<td[^>]*>(.*?)<\/td>/i);
  if (m) return stripTags_(m[1]);
  const T = String(text || '');
  const lines = T.split(/\r?\n/);
  for (let i=0;i<lines.length;i++) {
    if (/Lead\s+Artist\s+Business\s+or\s+Group\s+Address/i.test(lines[i])) {
      const same = lines[i].replace(/Lead\s+Artist\s+Business\s+or\s+Group\s+Address/i,'').trim();
      if (same) return same;
      for (let j=i+1;j<lines.length;j++) {
        const v = lines[j].trim();
        if (v) return v;
      }
    }
  }
  return '';
}
function extractBookingContactEmail_(html, text) {
  // “Booking contact:” row (email)
  let m = String(html || '').match(/Booking\s+contact[^<]*<a[^>]*href="mailto:([^"]+)"/i);
  if (m) return m[1].trim();
  const T = String(text || '');
  m = T.match(/Booking\s+contact.*?([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})/i);
  return m ? m[1] : '';
}
function extractAllKVPairs_(html) {
  const out = {};
  const H = String(html || '');
  // Walk all table rows; capture left <td> as key, right <td> as value
  const rowRx = /<tr[^>]*>\s*<td[^>]*>(.*?)<\/td>\s*<td[^>]*>(.*?)<\/td>\s*<\/tr>/ig;
  let m;
  while ((m = rowRx.exec(H)) !== null) {
    const key = stripTags_(m[1]).replace(/\s+/g,' ').trim();
    const val = stripTags_(m[2]).trim();
    if (key) out[key] = val;
  }
  return out;
}

/***** SCHEDULER: run parser every hour *****/
function installTimeTrigger() {
  // Remove existing clock triggers for runParser to avoid duplicates
  const triggers = ScriptApp.getProjectTriggers();
  triggers
    .filter(t => t.getHandlerFunction && t.getHandlerFunction() === 'runParser')
    .forEach(t => {
      try { ScriptApp.deleteTrigger(t); } catch (_) {}
    });
  // Create a fresh 1-hour trigger
  ScriptApp.newTrigger('runParser')
    .timeBased()
    .everyHours(1)
    .create();
}

/***** PUBLIC JSON FEED (minimal, privacy-safe) *****/
// Deploy as Web App: Deploy → New deployment → Web app
// - Execute as: Me
// - Who has access: Anyone with the link
function doGet(e) {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  const raw = ss.getSheetByName(SHEET_RAW_NAME);
  const occ = ss.getSheetByName(SHEET_NAME);

  // If we don't have raw yet, fall back to occupants minimal
  if (!raw) {
    const fallback = occ ? buildMinimalFromOcc_(occ) : { spaces: {}, updated_at: new Date().toISOString() };
    return ContentService.createTextOutput(JSON.stringify(fallback)).setMimeType(ContentService.MimeType.JSON);
  }

  const vals = raw.getDataRange().getValues(); // RAW_HEADER + rows
  if (!vals || vals.length < 2) {
    const empty = { spaces: {}, updated_at: new Date().toISOString() };
    return ContentService.createTextOutput(JSON.stringify(empty)).setMimeType(ContentService.MimeType.JSON);
  }

  const header = vals[0];
  const idx = {
    ref: header.indexOf('ref'),
    space_key: header.indexOf('space_key'),
    display: header.indexOf('display'),
    social: header.indexOf('social'),
    status: header.indexOf('status'),
    ticket: header.indexOf('ticket'),
    updated_at: header.indexOf('updated_at')
  };

  const groupsBySpace = {};
  const metaBySpace = {}; // hasBuyout, lastUpdated

  for (let i = 1; i < vals.length; i++) {
    const r = vals[i];
    const key = String(r[idx.space_key] || '').trim();
    if (!key) continue;

    const display = String(r[idx.display] || '').trim();
    const social  = String(r[idx.social]  || '').trim();
    const ticket  = String(r[idx.ticket]  || '').trim();
    const status  = String(r[idx.status]  || '').trim();
    const updated = r[idx.updated_at] instanceof Date ? r[idx.updated_at] : new Date(r[idx.updated_at] || new Date());

    const isSleeper = SLEEPER_KEYS.has(key);

    if (!metaBySpace[key]) metaBySpace[key] = { hasBuyout: false, lastUpdated: updated };
    if (updated > metaBySpace[key].lastUpdated) metaBySpace[key].lastUpdated = updated;

    if (isSleeper) {
      // Any RAW row implies occupied; do not collect groups (PII)
      metaBySpace[key].hasBuyout = true; // force full
      continue;
    }

    if (!groupsBySpace[key]) groupsBySpace[key] = [];

    // Track buyout for non-sleepers
    const { buyout } = slotCostForTicketServer_(ticket);
    metaBySpace[key].hasBuyout = metaBySpace[key].hasBuyout || buyout;

    // Push group (dedupe loose by display+social+ticket)
    const arr = groupsBySpace[key];
    const exists = arr.some(g => g.name === display && g.social === social && g.ticket === ticket);
    if (!exists) arr.push({ name: display, social: social, ticket: ticket });
  }

  // Build response
  const spaces = {};
  const allKeys = new Set([...Object.keys(groupsBySpace), ...Object.keys(metaBySpace)]);
  allKeys.forEach(key => {
    const isSleeper = SLEEPER_KEYS.has(key);
    const groups = groupsBySpace[key] || [];
    const flags = SPACE_FLAGS[key] || {};
    const hasBuyout = !!(metaBySpace[key] && metaBySpace[key].hasBuyout);
    const any = groups.length > 0;

    if (isSleeper) {
      // Privacy: status only, no name/social/groups
      const occupied = hasBuyout; // set above when any RAW row exists for sleeper
      spaces[key] = { status: occupied ? 'full' : '' };
      return;
    }

    // Status rules for non-sleepers
    let status = '';
    if ((flags.buyoutOnly && any) || hasBuyout) status = 'full';

    const name   = groups[0] && groups[0].name   || '';
    const social = groups[0] && groups[0].social || '';

    spaces[key] = { status, name, social, groups };
  });

  const resp = {
    spaces,
    updated_at: new Date().toISOString()
  };

  return ContentService.createTextOutput(JSON.stringify(resp))
    .setMimeType(ContentService.MimeType.JSON);
}

function buildMinimalFromOcc_(sh) {
  const vals = sh.getDataRange().getValues();
  if (!vals || vals.length < 2) return { spaces: {}, updated_at: new Date().toISOString() };
  const header = vals[0];
  const idx = {
    space_key: header.indexOf('space_key'),
    display: header.indexOf('display'),
    social: header.indexOf('social'),
    status: header.indexOf('status')
  };
  const spaces = {};
  for (let i = 1; i < vals.length; i++) {
    const r = vals[i];
    const key = String(r[idx.space_key] || '').trim();
    if (!key) continue;
    const name   = String(r[idx.display] || '').trim();
    const social = String(r[idx.social] || '').trim();
    const status = String(r[idx.status] || '').trim();
    spaces[key] = { name, social, status, groups: name || social ? [{ name, social, ticket: '' }] : [] };
  }
  return { spaces, updated_at: new Date().toISOString() };
}