'use strict';
/* HarbourWatch dashboard. Plain JS, Leaflet for the map.
   Ships come from /api/ships every POLL_MS with position, speed, course and heading. Between polls each ship keeps
   sailing: its position is dead-reckoned from speed and course, and a new report blends in smoothly. Everything that came from the API is put on the page with textContent (via h()), never innerHTML:
   ship names are typed by the crews. */

const POLL_MS = 3000;
const PORT_MS = 5000;
const GRID_MS = 60000; // the weather over the ocean changes by the hour
const HEALTH_MS = 15000;
const JUMP_M = 20000; // a ship whose new report is further than this from where it was drawn is placed, not blended
const BLEND_MS = 2500; // a new report pulls the drawn ship onto its corrected track over this long
const FRAME_MS = 50; // marker updates per second: 20 is smooth and cheap
const DR_CAP_MS = 5 * 60 * 1000; // a ship silent for longer than this (as of the last poll) is not extrapolated
const DR_MAX_MS = 15 * 60 * 1000; // and no ship is ever drawn more than this far past its last report
const RATE_WINDOW_MS = 30000; // how far back the feed speed is measured
const RATE_MIN_MS = 12000; // and how much history it needs before it replaces the default of 1
const MOVING_KN = 0.3; // slower than this is treated as not moving
const M_PER_KN_S = 0.514444;
/* Ships are drawn as ships: a top view of the real thing (container stacks on a container ship, hatches and a pipe
   run on a tanker, a white superstructure on a passenger ship), pointing along the heading. The size is the real size
   (AIS length, else a typical one for the type) at the map's zoom, so at a berth you see a ship as long as the berth
   and at sea a symbol large enough to find. Static markup built here, no API data in it. */
const HULL = 'M10 0 C15 8 19 20 19 36 L19 90 Q19 99 10 100 Q1 99 1 90 L1 36 C1 20 5 8 10 0 Z';
const BOX_COLOURS = ['#b5483a', '#3f7cac', '#c9a227', '#7f8c8d', '#4f8f5b', '#8e5a9b'];
const SHIP_ART = {
  cargo: () => `<path class="hull-body" d="${HULL}"/>${[0, 1, 2, 3, 4].flatMap((r) => [0, 1, 2].map((c) =>
    `<rect x="${3.2 + c * 4.6}" y="${12 + r * 11}" width="4.2" height="9.6" rx=".5" fill="${BOX_COLOURS[(r * 3 + c * 2) % 6]}"/>`)).join('')}`
    + '<rect class="deck" x="4" y="70" width="12" height="13" rx="1"/><rect x="7" y="86" width="6" height="5" rx="1" fill="#c0392b"/>',
  tanker: () => `<path class="hull-body" d="${HULL}"/><rect x="9.3" y="10" width="1.4" height="60" fill="#e8e8e8"/>${[18, 29, 40, 51, 62].map((y) =>
    `<circle cx="5.6" cy="${y}" r="2.6" class="deck"/><circle cx="14.4" cy="${y}" r="2.6" class="deck"/>`).join('')}`
    + '<rect class="deck" x="4.5" y="74" width="11" height="14" rx="1"/>',
  passenger: () => `<path class="hull-body" d="${HULL}"/><rect class="white" x="3" y="16" width="14" height="66" rx="3"/>`
    + '<circle cx="10" cy="30" r="3" fill="#5aa5ea"/><rect x="5" y="40" width="10" height="3" rx="1" fill="#cfd6dc"/><rect x="5" y="48" width="10" height="3" rx="1" fill="#cfd6dc"/>'
    + '<rect x="6.5" y="58" width="7" height="9" rx="1.5" fill="#c0392b"/><rect x="5" y="72" width="10" height="5" rx="1" fill="#cfd6dc"/>',
  tug: () => `<path class="hull-body" d="${HULL}"/><rect class="deck" x="4.5" y="34" width="11" height="26" rx="2"/><rect x="2" y="12" width="16" height="3" rx="1.5" fill="#222"/>`,
  fishing: () => `<path class="hull-body" d="${HULL}"/><rect class="deck" x="5" y="52" width="10" height="22" rx="1.5"/>`
    + '<path d="M10 14 V52 M3 30 H17" stroke="#f4f4f4" stroke-width="1.2" fill="none"/>',
  other: () => `<path class="hull-body" d="${HULL}"/><rect class="deck" x="4" y="22" width="12" height="38" rx="1.5"/><rect class="deck" x="5" y="68" width="10" height="16" rx="1"/>`,
};
SHIP_ART.unknown = SHIP_ART.other;
const SHIP_LENGTH_M = { cargo: 230, tanker: 230, passenger: 200, tug: 30, fishing: 30, other: 60, unknown: 110 };
const SHIP_BEAM = { cargo: 0.16, tanker: 0.17, passenger: 0.15, tug: 0.3, fishing: 0.28, other: 0.2, unknown: 0.18 };
const SHIP_MIN_PX = [9, 24]; // beam, length: never smaller, so a ship is always something you can click
function shipSvg(type, beamPx, lengthPx, moving = false) {
  const art = (SHIP_ART[type] || SHIP_ART.unknown)();
  const wake = moving ? '<path class="wake" d="M2 100 L10 160 L18 100 Z"/>' : '';
  return `<svg viewBox="0 0 20 100" preserveAspectRatio="none" aria-hidden="true" style="width:${beamPx}px;height:${lengthPx}px">${wake}${art}</svg>`;
}
/* [beam px, length px] of a ship at the map's zoom. */
function shipPx(s) {
  const metresPerPx = (40075016.686 * Math.cos((s.lat * Math.PI) / 180)) / (256 * 2 ** map.getZoom());
  const type = SHIP_ART[s.ship_type] ? s.ship_type : 'unknown';
  const metres = s.length_m > 8 ? s.length_m : SHIP_LENGTH_M[type];
  const length = clamp(metres / metresPerPx, SHIP_MIN_PX[1], 1400);
  return [Math.round(Math.max(SHIP_MIN_PX[0], length * SHIP_BEAM[type])), Math.round(length)];
}
const STATES = ['underway', 'anchored', 'berthed', 'departed', 'dark'];
const LABEL = { dark: 'dark (no signal)' };
const PRIOS = ['high', 'medium', 'low'];
const RANK = { high: 0, medium: 1, low: 2 };
const ALERT_TYPES = { dark_ship: 'Dark ship', queue_growth: 'Queue growth' };
const LIMIT_DEFAULT = 1000; // most ships on the map; the server drops the first arrivals beyond it, never pinned ones
// What the weather is called when the pointer is over it (the picture itself is drawn by sky.js).
const WX = { cyclone: { label: 'Cyclone' }, storm: { label: 'Storm' }, rough: { label: 'Rain and rough sea' }, calm: { label: 'Fair weather' } };
// Why a ship waits: glyph and label per reason code from the API (each has a colour in app.css, --r-<code>).
const REASONS = {
  weather: { glyph: '⛈️', label: 'Weather' },
  congestion: { glyph: '🚢', label: 'Berth queue' },
  orders: { glyph: '📋', label: 'Waiting for orders' },
  early: { glyph: '⏰', label: 'Early arrival' },
  long_wait: { glyph: '⏳', label: 'Long stay' },
  unknown: { glyph: '❔', label: 'No clear cause' },
};
const BADGE_ZOOM = 11; // reason badges on waiting ships from this zoom in: further out they would hide the map
const known = (codes) => codes.filter((c) => c in REASONS && c !== 'unknown');
const reasonCodes = (s) => known((s.reasons || []).map((r) => r.code));

const $ = (id) => document.getElementById(id);
const h = (tag, attrs = {}, ...kids) => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (v == null || v === false) continue;
    if (k === 'class') e.className = v;
    else if (k.startsWith('on')) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v === true ? '' : v);
  }
  e.append(...kids.flat().filter((k) => k != null && k !== false));
  return e;
};
const store = {
  get(k, d) { try { const v = localStorage.getItem('hw.' + k); return v == null ? d : JSON.parse(v); } catch { return d; } },
  set(k, v) { try { localStorage.setItem('hw.' + k, JSON.stringify(v)); } catch { /* private mode: the page still works */ } },
};

const app = {
  ships: new Map(),
  ports: [],
  grid: [], // weather points over the open ocean
  mode: store.get('mode', 'live'), // 'live': the live API is polled; 'offline': history replay, no live calls
  stream: null, // the alert EventSource while it is open
  replay: { at: 0, chosen: false, playing: false, speed: store.get('rpSpeed', 60), dates: [], seq: 0, ticks: 0, busy: false, focus: null, journey: null },
  sky: [], // everything the weather picture is made of: the sea points and the ports' weather
  pendingFocus: null, // a ship to replay as soon as the offline mode is up
  alerts: new Map(),
  pins: new Set(store.get('pins', [])),
  acked: new Set(store.get('acked', [])),
  prio: { dark_ship: 'high', queue_growth: 'medium', ...store.get('prio', {}) },
  notify: store.get('notify', 'off'),
  prioFilter: new Set(PRIOS),
  filters: { q: '', states: new Set(STATES), group: 'all', type: '', port: '', pinned: false },
  limit: store.get('limit', LIMIT_DEFAULT),
  hidden: new Map(Object.entries(store.get('hidden', {}))), // mmsi -> feed time it was hidden at
  shown: new Set(), // opened from the history search: drawn although the map rules have retired them
  selected: null,
  follow: false,
  feedTime: 0,
  rate: 1, // feed seconds per wall second: 1 live, up to 60 in a replay
  feedWall: 0,
  lastOk: 0,
  first: true,
  dark: new Set(),
};

const API_BASE = (window.__API_BASE__ || localStorage.getItem('api_base') || (new URLSearchParams(window.location.search).get('api') || '')).replace(/\/$/, '');
if (new URLSearchParams(window.location.search).has('api')) {
  localStorage.setItem('api_base', API_BASE);
}
function apiUrl(path) {
  return API_BASE ? `${API_BASE}${path}` : path;
}

async function api(path, { method = 'GET', body: sent = null } = {}) {
  const headers = { Accept: 'application/json', ...(sent ? { 'Content-Type': 'application/json' } : {}) };
  const r = await fetch(apiUrl(path), { method, headers, body: sent ? JSON.stringify(sent) : undefined, signal: AbortSignal.timeout(20000) });
  let body = null;
  try { body = await r.json(); } catch { /* not JSON */ }
  if (!r.ok) throw Object.assign(new Error(body?.error?.message || `HTTP ${r.status}`), { code: body?.error?.code });
  return body;
}

function debounce(fn, ms = 400) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), ms);
  };
}

/* ---------- formatting ---------- */
const pad = (n) => String(n).padStart(2, '0');
function utc(ms) {
  if (!Number.isFinite(ms)) return 'time unknown';
  const d = new Date(ms);
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}Z`;
}
// Times the people of the monitored country read: its time zone (the server's MONITOR_TIMEZONE or geo/timezones.csv),
// with the offset. The replay controls stay in UTC (the history is partitioned by UTC day): `utc()` there.
function local(ms) {
  if (!Number.isFinite(ms)) return 'time unknown';
  try {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: app.monitor.timezone || 'UTC', year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZoneName: 'short',
    }).formatToParts(new Date(ms));
    const g = (t) => parts.find((p) => p.type === t)?.value;
    return `${g('year')}-${g('month')}-${g('day')} ${g('hour')}:${g('minute')} ${g('timeZoneName')}`;
  } catch {
    return utc(ms); // a time zone name the browser does not know
  }
}
function dur(ms) {
  if (ms == null || Number.isNaN(ms)) return '—';
  const s = Math.max(0, Math.round(ms / 1000));
  if (s < 90) return `${s}s`;
  const m = Math.round(s / 60);
  if (m < 90) return `${m}m`;
  return `${Math.floor(m / 60)}h ${pad(m % 60)}m`;
}
const sinceFeed = (iso) => (iso ? dur(app.feedTime - Date.parse(iso)) : '—');
const portName = (id) => app.ports.find((p) => p.port_id === id)?.name || id || 'at sea';
const shipName = (mmsi) => app.ships.get(mmsi)?.name || `MMSI ${mmsi}`;
const effState = (s) => (app.dark.has(s.mmsi) ? 'dark' : s.state);
const pill = (cls, text) => h('span', { class: `pill ${cls}` }, text ?? LABEL[cls] ?? cls);

/* ---------- map ---------- */
// The sea route crosses the 180th meridian. Longitudes west of -30 are drawn east of 180, so the Pacific is one
// piece with Los Angeles at its right-hand end; the tiles repeat there, so nothing is missing.
const SM = L.Projection.SphericalMercator;
const wrapLon = (lon) => (lon < -30 ? lon + 360 : lon);
const PacificCRS = L.extend({}, L.CRS.EPSG3857, {
  projection: { ...SM, project: (ll) => SM.project(L.latLng(ll.lat, wrapLon(ll.lng))) },
  wrapLng: null,
  infinite: true, // tiles are asked for past 180; the layer below wraps their x back into the world
});
const PacificTiles = L.TileLayer.extend({
  getTileUrl(c) {
    const n = 2 ** c.z;
    if (c.y < 0 || c.y >= n) return L.Util.emptyImageUrl;
    return L.TileLayer.prototype.getTileUrl.call(this, { x: ((c.x % n) + n) % n, y: c.y, z: c.z });
  },
});
const fitTo = (points, opts) => map.fitBounds(L.latLngBounds(points.map(([lat, lon]) => [lat, wrapLon(lon)])), opts);

const map = L.map('map', { preferCanvas: true, crs: PacificCRS }).setView([33.72, -118.22], 11);
const baseMaps = {
  'Street map': new PacificTiles('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
  }),
  Satellite: new PacificTiles('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    maxZoom: 17,
    attribution: 'Imagery &copy; Esri, Maxar, Earthstar Geographics',
  }),
};
// The coverage view: a muted street map under the coverage layers, chosen like the satellite or street map.
const COVERAGE_VIEW = 'Coverage view';
baseMaps[COVERAGE_VIEW] = new PacificTiles('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 18,
  className: 'tiles-muted',
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
});
const baseName = baseMaps[store.get('base', 'Satellite')] ? store.get('base', 'Satellite') : 'Satellite';
baseMaps[baseName].addTo(map);
L.control.layers(baseMaps, null, { position: 'topright', collapsed: true }).addTo(map);
map.on('baselayerchange', (e) => { // the coverage layers are the coverage view's: on with it, off with any other
  store.set('base', e.name);
  setCoverage(e.name === COVERAGE_VIEW);
  document.body.classList.toggle('sat-base', e.name === 'Satellite');
});
document.body.classList.toggle('sat-base', baseName === 'Satellite');
map.createPane('cloud').style.zIndex = 350; // above the tiles, under zones, routes and ships
const clouds = new Sky.CloudLayer().addTo(map);
const zoneLayer = L.layerGroup().addTo(map);
const portLayer = L.layerGroup().addTo(map);
const shipLayer = L.layerGroup().addTo(map);
const routeLayer = L.layerGroup().addTo(map);
const stormLayer = L.layerGroup().addTo(map);

let palette = {};
function readPalette() {
  const css = getComputedStyle(document.documentElement);
  for (const s of STATES) palette[s] = css.getPropertyValue('--' + s).trim();
}
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', readPalette);

const legend = L.control({ position: 'bottomleft' });
legend.onAdd = () => {
  const mini = (s, more = '') => { const i = h('i', { class: `mini ship-icon ${s} ${more}` }); i.innerHTML = shipSvg('cargo', 8, 16); return i; };
  const why = (c) => h('span', {}, h('b', { class: `why why-${c} legend-why`, 'aria-hidden': 'true' }, REASONS[c].glyph), REASONS[c].label);
  return h('div', { class: 'legend' }, STATES.map((s) => h('span', {}, mini(s), LABEL[s] || s)),
    h('span', {}, mini('underway', 'monitored'), 'monitored voyage (glow)'),
    h('small', { class: 'legend-head' }, 'Why waiting'), ['weather', 'congestion', 'orders', 'early', 'long_wait'].map(why));
};
legend.addTo(map);

const ZONE_STYLE = {
  area: { color: '#5b7a99', weight: 1, dashArray: '2 5', fillOpacity: 0.03 },
  anchorage: { color: '#c98a0f', weight: 1.5, dashArray: '6 4', fillOpacity: 0.08 },
  berth: { color: '#2a9460', weight: 1.5, fillOpacity: 0.15 },
};
async function loadZones() {
  try {
    const geo = await api('/api/zones');
    const style = (f) => ZONE_STYLE[f.properties.kind] || ZONE_STYLE.area;
    // The big port-area outline is only drawn: as a hover target it would cover every ship in it.
    L.geoJSON(geo, { style, interactive: false, filter: (f) => f.properties.kind === 'area' }).addTo(zoneLayer);
    L.geoJSON(geo, {
      style,
      filter: (f) => f.properties.kind !== 'area',
      onEachFeature: (f, layer) => layer.bindTooltip(() => h('span', {}, `${f.properties.name} (${f.properties.kind})`), { sticky: true }),
    }).addTo(zoneLayer);
  } catch { /* zones are decoration: the map still works without them */ }
}

/* ---------- ship markers ---------- */
app.markers = new Map(); // mmsi -> { ship, marker, rot, key, err, errT }

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const feedNow = (now) => app.feedTime + (now - app.feedWall) * app.rate;

/* Where the ship should be drawn right now: its last report moved along its course at its speed for the
   feed time that has passed since that report. */
function reckon(s, now) {
  const course = s.cog ?? s.heading;
  if (course == null || s.sog == null || s.sog < MOVING_KN) return [s.lat, s.lon];
  if (app.feedTime - s.t > DR_CAP_MS) return [s.lat, s.lon]; // went quiet: show where it was last heard
  const dt = clamp(feedNow(now) - s.t, 0, DR_MAX_MS) / 1000;
  const metres = s.sog * M_PER_KN_S * dt;
  const th = (course * Math.PI) / 180;
  return [s.lat + (metres * Math.cos(th)) / 111320, s.lon + (metres * Math.sin(th)) / (111320 * Math.cos((s.lat * Math.PI) / 180))];
}

function iconFor(m) {
  const s = m.ship;
  const cls = ['ship-icon', effState(s), app.pins.has(s.mmsi) ? 'pinned' : '', app.selected === s.mmsi ? 'sel' : '', s.estimated ? 'estimated' : '', isMonitored(s) ? 'monitored' : ''].join(' ');
  const [beam, length] = shipPx(s);
  const box = Math.max(beam, length) + 14; // the marker box: the ship and room for the selection ring
  const moving = effState(s) === 'underway' && s.sog >= MOVING_KN && length >= 40;
  return L.divIcon({
    className: cls,
    html: `<div class="hull" style="--rot:${m.rot}deg">${shipSvg(s.ship_type, beam, length, moving)}</div>${badge(s, Math.round(box / 2) + 4)}`,
    iconSize: [box, box],
    iconAnchor: [box / 2, box / 2],
  });
}
// Every reason as a round badge beside the hull, in a row that does not turn with it.
// Only codes listed in REASONS reach the markup, so nothing from the API is injected.
function badge(s, left = 30) {
  const codes = reasonCodes(s);
  if (!codes.length) return '';
  return `<span class="whys" style="left:${left}px" aria-hidden="true">${codes.map((c) => `<b class="why why-${c}">${REASONS[c].glyph}</b>`).join('')}</span>`;
}
function restyle(mmsi) {
  const m = app.markers.get(mmsi);
  if (!m) return;
  const key = [effState(m.ship), app.pins.has(mmsi), app.selected === mmsi, m.ship.ship_type, m.ship.length_m, shipPx(m.ship).join('x'), m.ship.estimated, reasonCodes(m.ship).join('+')].join();
  if (key === m.key) return;
  m.key = key;
  m.marker.setIcon(iconFor(m));
  m.marker.setZIndexOffset(app.selected === mmsi ? 1000 : app.pins.has(mmsi) ? 500 : 0);
}
function setRotation(m, deg) {
  m.rot += ((deg - m.rot) % 360 + 540) % 360 - 180; // turn the short way round, never through 359 -> 0
  m.marker.getElement()?.firstChild?.style.setProperty('--rot', `${m.rot}deg`);
}
function zoomScale() {
  map.getContainer().classList.toggle('badges', map.getZoom() >= BADGE_ZOOM);
  for (const m of app.markers.values()) { m.key = null; restyle(m.ship.mmsi); } // true size: every ship is redrawn
}
map.on('zoomend', zoomScale);
zoomScale();

// A hidden ship comes back by itself when a problem starts after it was hidden: it anchors to wait, or goes dark.
function darkSince(mmsi) {
  let t = 0;
  for (const a of app.alerts.values()) if (a.type === 'dark_ship' && a.mmsi === mmsi) t = Math.max(t, Date.parse(a.event_time) || 0);
  return t;
}
function isHidden(ship) {
  const at = app.hidden.get(ship.mmsi);
  if (at == null) return false;
  if (ship.state === 'anchored' && (Date.parse(ship.since || ship.last_seen) || 0) > at) return false;
  if (app.dark.has(ship.mmsi) && darkSince(ship.mmsi) > at) return false;
  return true;
}
function setHidden(mmsi, on) {
  if (on) app.hidden.set(mmsi, app.feedTime); else app.hidden.delete(mmsi);
  store.set('hidden', Object.fromEntries(app.hidden));
  renderHidden();
  applyFilters(); renderSummary();
}
function renderHidden() {
  const n = app.hidden.size;
  $('unhide').hidden = !n;
  $('unhide').textContent = `Show ${n} hidden`;
}
/* Ships by whose voyage it is, not by flag (the server's `kind`, common.monitor):
   monitored  a voyage of ours: it started at a monitored port or is bound for one
   crossing   in the waters on a voyage of its own: its destination (and source, if known) is outside the
              countries we list, so it does not come to our port, it only uses our water
   other      in the waters with no voyage of ours: destination not sent, not a port we know, or a port of a
              listed country that is not one we monitor (harbour craft, tugs, local traffic)
   Monitored + crossing + other = "All ships". */
const isMonitored = (ship) => ship.kind === 'monitored';
const isCrossing = (ship) => ship.kind === 'crossing';
const isOther = (ship) => !isMonitored(ship) && !isCrossing(ship); // in the waters, voyage not known or not clearly either
function visible(ship) {
  const f = app.filters;
  if (ship.mmsi === app.selected) return true;
  if (isHidden(ship)) return false;
  if (!f.states.has(effState(ship))) return false;
  if (f.group === 'monitored' && !isMonitored(ship)) return false;
  if (f.group === 'crossing' && !isCrossing(ship)) return false;
  if (f.group === 'other' && !isOther(ship)) return false;
  if (f.type && ship.ship_type !== f.type) return false;
  if (f.port && ship.port !== f.port) return false;
  if (f.pinned && !app.pins.has(ship.mmsi)) return false;
  const q = f.q.trim().toLowerCase();
  return !q || ship.mmsi.includes(q) || (ship.name || '').toLowerCase().includes(q);
}
function applyFilters() {
  let shown = 0;
  const kinds = { all: app.markers.size, monitored: 0, crossing: 0, other: 0 };
  for (const m of app.markers.values()) {
    if (isMonitored(m.ship)) kinds.monitored++;
    if (isCrossing(m.ship)) kinds.crossing++;
    if (isOther(m.ship)) kinds.other++;
    const on = visible(m.ship);
    if (on) shown++;
    if (on && !shipLayer.hasLayer(m.marker)) shipLayer.addLayer(m.marker);
    if (!on && shipLayer.hasLayer(m.marker)) shipLayer.removeLayer(m.marker);
  }
  $('shown').textContent = `Showing ${shown} of ${app.markers.size}`;
  for (const o of $('f-group').options) o.textContent = `${GROUP_LABEL(o.value)} (${kinds[o.value] ?? 0})`;
}
function computeDark() {
  app.dark = new Set();
  for (const a of app.alerts.values()) {
    const ship = a.type === 'dark_ship' && a.mmsi ? app.ships.get(a.mmsi) : null;
    if (ship && Date.parse(ship.last_seen) <= Date.parse(a.event_time)) app.dark.add(a.mmsi);
  }
}

/* The feed clock: newest report time, and how fast it advances against the wall clock. Spark delivers in
   bursts, so the speed is measured over the last RATE_WINDOW_MS of polls, not between two polls. A stalled
   feed measures 0 and the ships stop sailing. */
const clockLog = []; // [wall ms, newest feed ms] per poll
function tickClock(list, now) {
  // The 95th percentile, not the maximum: a ship with a wrong clock must not drag every ship's time with it.
  const times = list.map((s) => s.t).sort((x, y) => x - y);
  const newest = times.length ? times[Math.floor((times.length - 1) * 0.95)] : 0;
  clockLog.push([now, newest]);
  while (clockLog.length > 2 && now - clockLog[0][0] > RATE_WINDOW_MS) clockLog.shift();
  const [wall0, feed0] = clockLog[0];
  if (now - wall0 >= RATE_MIN_MS) app.rate = clamp((newest - feed0) / (now - wall0), 0, 120);
  app.feedTime = newest;
  app.feedWall = now;
}

function updateShips(list) {
  const now = performance.now();
  for (const s of list) s.t = Date.parse(s.last_seen) || 0;
  app.ships = new Map(list.map((s) => [s.mmsi, s]));
  if (app.mode === 'offline') { // the replay owns the clock: ships sail as fast as it plays, and stand still while paused
    app.feedTime = app.replay.at;
    app.feedWall = now;
    app.rate = app.replay.playing ? app.replay.speed : 0;
  } else tickClock(list, now);
  computeDark();
  for (const s of list) {
    let m = app.markers.get(s.mmsi);
    if (!m) {
      const marker = L.marker(reckon(s, now), { icon: L.divIcon({ className: 'ship-icon' }), keyboard: false });
      marker.bindTooltip(() => {
        const c = app.ships.get(s.mmsi);
        const why = reasonCodes(c).map((code) => `${REASONS[code].glyph} ${REASONS[code].label.toLowerCase()}`).join(', ');
        const roles = (c.roles || []).map((r) => ROLE_LABEL[r]).join(', ');
        return h('span', {}, `${c.name || c.mmsi} · ${effState(c)}${roles ? ` · ${roles}` : ''}${c.estimated ? ' · estimated position, out of AIS range' : ''}${why ? ` · waiting: ${why}` : ''}`);
      });
      marker.on('click', () => select(s.mmsi, { fly: false }));
      m = { ship: s, marker, rot: s.heading ?? s.cog ?? 0, key: null, err: null, errT: 0 };
      marker.on('add', () => marker.getElement()?.firstChild?.style.setProperty('--rot', `${m.rot}deg`));
      app.markers.set(s.mmsi, m);
    } else {
      // A new report or a corrected feed clock moves where the ship should be. Keep drawing it where it is
      // now and blend it onto the corrected track, so nothing jumps.
      const drawn = m.marker.getLatLng();
      m.ship = s;
      const [lat, lon] = reckon(s, now);
      const off = drawn.distanceTo([lat, lon]);
      if (off > JUMP_M) { m.err = null; m.marker.setLatLng([lat, lon]); }
      else if (off > 0.5) { m.err = [drawn.lat - lat, drawn.lng - lon]; m.errT = now; }
    }
    const turn = s.heading ?? s.cog;
    if (turn != null) setRotation(m, turn);
    restyle(s.mmsi);
  }
  for (const [mmsi, m] of app.markers) {
    if (!app.ships.has(mmsi)) { shipLayer.removeLayer(m.marker); app.markers.delete(mmsi); }
  }
  fillTypeFilter();
  applyFilters();
  renderSummary();
  renderPins();
  if (app.ports.length) renderPorts(); // the wait-reason counts on the port cards come from the ships
  if (app.first && list.length) {
    app.first = false;
    renderAlerts(); // names are known now
    const here = list.filter((s) => !isHidden(s));
    fitTo((here.length ? here : list).map((s) => [s.lat, s.lon]), { padding: [40, 40], maxZoom: 12 });
  }
}

let lastFrame = 0;
function frame(now) {
  requestAnimationFrame(frame);
  if (now - lastFrame < FRAME_MS) return;
  lastFrame = now;
  for (const m of app.markers.values()) {
    const sailing = m.ship.sog >= MOVING_KN && (m.ship.cog ?? m.ship.heading) != null;
    if (!sailing && !m.err) continue;
    let [lat, lon] = reckon(m.ship, now);
    if (m.err) {
      const left = 1 - (now - m.errT) / BLEND_MS;
      if (left <= 0) m.err = null;
      else { lat += m.err[0] * left; lon += m.err[1] * left; }
    }
    m.marker.setLatLng([lat, lon]);
  }
  const sel = app.selected && app.markers.get(app.selected);
  if (sel) {
    const here = sel.marker.getLatLng();
    if (app.follow) map.panTo(here, { animate: false });
    if (app.routeLines) for (const line of app.routeLines.lines) line.setLatLngs([here, ...app.routeLines.rest]);
  }
}

/* ---------- loading ships ---------- */
function mapStatus(text) {
  $('map-status').hidden = !text;
  $('map-status').replaceChildren(text ? h('span', {}, text) : '');
}
function banner(text) {
  $('banner').hidden = !text;
  $('banner-text').textContent = text || '';
}

function formatUserMessage(err, fallback = 'Service is temporarily unavailable') {
  if (!err) return fallback;
  const msg = typeof err === 'string' ? err : (err.message || '');
  if (!msg) return fallback;
  if (/run:|python\s+-m|docker|curl|npm|bash|sh\s+-c|pip|apt-get/i.test(msg)) {
    return fallback;
  }
  if (/^HTTP\s*\d{3}/i.test(msg) || /status\s*(code)?\s*\d{3}/i.test(msg)) {
    return fallback;
  }
  if (/hbase|kafka|duckdb|sql|database|traceback|connection refused|timeout|failed to fetch|networkerror/i.test(msg)) {
    return fallback;
  }
  if (/^internal\s+error$/i.test(msg) || /store is unavailable/i.test(msg) || /_UNAVAILABLE|_ERROR/i.test(msg)) {
    return fallback;
  }
  return msg;
}

function showToast({ title, message, type = 'info', duration = 8000, onClick = null } = {}) {
  // Notification popups disabled
  return null;
}

function showModal({ title, content, actions = [] }) {
  const dlg = $('modal');
  if (!dlg) return;
  $('modal-title').textContent = title || 'Notice';
  const body = $('modal-body');
  if (typeof content === 'string') {
    body.textContent = content;
  } else if (content instanceof HTMLElement) {
    body.replaceChildren(content);
  }
  const actWrap = $('modal-actions');
  actWrap.replaceChildren(...(actions.length ? actions.map(({ label, cls = 'btn', onClick }) => {
    const btn = h('button', { type: 'button', class: cls }, label);
    btn.onclick = () => { if (onClick) onClick(); dlg.close(); };
    return btn;
  }) : [h('button', { type: 'button', class: 'btn', onclick: () => dlg.close() }, 'Dismiss')]));
  $('modal-close').onclick = () => dlg.close();
  if (typeof dlg.showModal === 'function') {
    try { dlg.showModal(); } catch { dlg.setAttribute('open', ''); }
  } else {
    dlg.setAttribute('open', '');
  }
}

async function loadShips() {
  try {
    const pins = [...app.pins].slice(0, 200).join(',');
    const shown = [...app.shown].slice(0, 200).join(',');
    updateShips(await api(`/api/ships?limit=${app.limit}${pins ? `&pinned=${pins}` : ''}${shown ? `&show=${shown}` : ''}${monitorQuery()}`));
    app.lastOk = Date.now();
    banner('');
    mapStatus(app.ships.size ? '' : 'No ships reported yet');
    if (app.selected) refreshSelected();
  } catch (err) {
    banner('Live data unavailable. Retrying…');
    if (!app.ships.size) mapStatus('Live data unavailable');
  }
}

/* ---------- header ---------- */
function renderSummary() {
  const counts = Object.fromEntries(STATES.map((s) => [s, 0]));
  for (const s of app.ships.values()) if (!isHidden(s)) counts[effState(s)]++;
  const high = [...app.alerts.values()].filter((a) => !app.acked.has(a.alert_id) && prioOf(a) === 'high').length;
  $('summary').replaceChildren(
    ...STATES.map((s) => h('span', { title: LABEL[s] || s }, h('i', { style: `background:var(--${s})` }), `${counts[s]} ${s}`)),
    h('span', { title: 'Unacknowledged high-priority alerts' }, h('i', { style: 'background:var(--high)' }), `${high} high alerts`),
  );
}
function renderClock() {
  const age = app.lastOk ? dur(Date.now() - app.lastOk) : 'never';
  if (app.mode === 'offline') {
    const speed = app.replay.playing ? `offline replay ×${app.replay.speed}` : 'offline replay paused';
    $('data-time').textContent = app.feedTime ? `${utc(app.feedTime)} · ${speed}` : '';
    return;
  }
  const speed = app.rate > 1.5 ? `replay ×${Math.round(app.rate)}` : app.rate < 0.2 ? 'feed paused' : 'live';
  $('data-time').textContent = app.feedTime ? `feed ${local(app.feedTime)} · ${speed} · updated ${age} ago` : '';
}
async function loadHealth() {
  const el = $('health');
  try {
    const r = await fetch(apiUrl('/health'), { signal: AbortSignal.timeout(8000) });
    const body = await r.json();
    el.dataset.status = body.status === 'ok' ? 'ok' : 'degraded';
    el.textContent = Object.entries(body.checks).map(([k, v]) => `${k} ${v}`).join(' · ');
  } catch {
    el.dataset.status = 'down';
    el.textContent = 'dashboard unreachable';
  }
}

/* ---------- filters ---------- */
function chipRow(container, items, pressed, onToggle, dotted) {
  container.replaceChildren(...items.map((it) => {
    const b = h('button', { type: 'button', class: 'chip', 'aria-pressed': String(pressed.has(it)) },
      dotted ? h('i', { style: `background:var(--${it})` }) : null, LABEL[it] || it);
    b.addEventListener('click', () => { onToggle(it); b.setAttribute('aria-pressed', String(pressed.has(it))); });
    return b;
  }));
}
function fillTypeFilter() {
  const sel = $('f-type');
  const types = [...new Set([...app.ships.values()].map((s) => s.ship_type).filter(Boolean))].sort();
  if (types.join() === [...sel.options].slice(1).map((o) => o.value).join()) return;
  sel.replaceChildren(h('option', { value: '' }, 'All types'), ...types.map((t) => h('option', { value: t }, t)));
  sel.value = app.filters.type;
}
function resetFilters() {
  Object.assign(app.filters, { q: '', states: new Set(STATES), group: 'all', type: '', port: '', pinned: false });
  $('f-group').value = 'all';
  $('q').value = ''; $('f-type').value = ''; $('f-pinned').checked = false;
  closeSearch();
  initStateChips(); applyFilters(); renderPorts();
}
/* The "Ships" drop-down: everything, only the monitored ships, or only other countries' ships crossing the waters. */
const GROUP_LABEL = (value) => {
  const name = app.monitor.country ? countryName(app.monitor.country) : 'the monitored country';
  return { all: 'All ships', monitored: `Monitored: to or from ${name}`, crossing: `Crossing ${name}'s waters, own voyage`, other: `Other ships in ${name}'s waters (voyage not ours or not known)` }[value];
};
function initGroupFilter() {
  const sel = $('f-group');
  sel.replaceChildren(...['all', 'monitored', 'crossing', 'other'].map((v) => h('option', { value: v }, GROUP_LABEL(v))));
  sel.value = app.filters.group;
}
function initStateChips() {
  chipRow($('state-chips'), STATES, app.filters.states, (s) => {
    app.filters.states.has(s) ? app.filters.states.delete(s) : app.filters.states.add(s);
    applyFilters();
  }, true);
}

/* ---------- ship detail ---------- */
const detail = $('ship-detail');
async function select(mmsi, { fly = true } = {}) {
  const prev = app.selected;
  app.selected = mmsi;
  if (prev) restyle(prev);
  restyle(mmsi);
  applyFilters();
  routeLayer.clearLayers();
  app.routeLines = null;
  detail.hidden = false;
  detail.replaceChildren(h('p', { class: 'muted' }, 'Loading ship…'));
  const m = app.markers.get(mmsi);
  if (fly && m) map.flyTo(m.marker.getLatLng(), Math.max(map.getZoom(), 12), { duration: 0.8 });
  await refreshSelected();
}
function deselect() {
  const prev = app.selected;
  app.selected = null;
  if (prev) app.shown.delete(prev); // a ship opened from history leaves the map with its detail panel
  app.follow = false;
  app.routeLines = null;
  routeLayer.clearLayers();
  detail.hidden = true;
  if (prev) restyle(prev);
  applyFilters();
}
/* Where a selected ship has been (solid) and where it may go (dashed): the track it sailed, the route to its
   destination along the sea route and the lanes, or, with no destination, its course for the next 12 hours. */
const COURSE_AHEAD_H = 12;
function courseAhead(s) {
  const course = s.cog ?? s.heading;
  if (course == null || s.sog == null || s.sog < MOVING_KN) return null;
  const metres = s.sog * M_PER_KN_S * COURSE_AHEAD_H * 3600;
  const th = (course * Math.PI) / 180;
  return [[s.lat, s.lon], [s.lat + (metres * Math.cos(th)) / 111320, s.lon + (metres * Math.sin(th)) / (111320 * Math.cos((s.lat * Math.PI) / 180))]];
}
async function refreshSelected() {
  const mmsi = app.selected;
  if (!mmsi) return;
  const [track, states, route] = app.mode === 'offline' ? await offlineSelected(mmsi) : await Promise.allSettled([
    api(`/api/ships/${mmsi}/track?limit=500`),
    api(`/api/ships/${mmsi}/states`),
    api(`/api/ships/${mmsi}/route`),
  ]);
  if (app.selected !== mmsi) return; // the user clicked another ship meanwhile
  routeLayer.clearLayers();
  app.routeLines = null;
  const s = app.ships.get(mmsi);
  const from = route.status === 'fulfilled' ? route.value.origin : null;
  if (track.status === 'fulfilled') {
    // The voyage is the stretch since the ship left its source port: solid, and drawn from the port itself.
    // What was stored before that (an earlier voyage) is faint. A ship first heard at sea has no source in the
    // data, so its line starts where it was first heard.
    const tsOf = (p) => p.ts ?? Date.parse(p.event_time);
    const all = track.value.filter((p) => p.lat != null && p.lon != null).reverse(); // oldest first
    const cut = from?.voyage_from == null ? 0 : Math.max(0, all.findIndex((p) => tsOf(p) >= from.voyage_from));
    const earlier = all.slice(0, cut + 1).map((p) => [p.lat, p.lon]);
    const pts = all.slice(cut).map((p) => [p.lat, p.lon]);
    if (from?.known && from.lat != null) pts.unshift([from.lat, from.lon]);
    if (earlier.length > 1 && cut > 0) L.polyline(earlier, { color: '#586b79', weight: 2, opacity: 0.5, dashArray: '2 6', interactive: false }).addTo(routeLayer).bindTooltip('Earlier voyages');
    if (pts.length > 1) L.polyline(pts, { color: palette.underway, weight: 3, opacity: 0.9 }).addTo(routeLayer).bindTooltip(from?.known ? `Path taken from ${from.name}` : 'Path taken since first heard');
    if (track.future?.length > 1) { // replaying one ship: the rest of what the data holds, not sailed yet
      L.polyline(track.future, { color: '#586b79', weight: 2, opacity: 0.6, dashArray: '3 7', interactive: false }).addTo(routeLayer);
    }
    for (const e of track.events || []) {
      L.circleMarker([e.lat, e.lon], { radius: 5, color: '#fff', weight: 2, fillColor: EVENT_COLOR[e.type] || '#586b79', fillOpacity: e.future ? 0.35 : 1 })
        .bindTooltip(() => h('span', {}, `${local(e.ts)} · ${e.text}`)).on('click', () => seekTo(e.ts)).addTo(routeLayer);
    }
  }
  if (from) { // where the voyage began: its source port, or (grey) only where it was first heard
    L.circleMarker([from.lat, from.lon], { radius: 6, color: '#fff', weight: 2, fillColor: from.known ? '#2a9460' : '#8a97a3', fillOpacity: 1 })
      .bindTooltip(() => h('span', {}, from.known ? `Source: ${from.name}${from.at ? ` · left ${local(Date.parse(from.at))}` : ''}` : `Source not known: first heard ${from.place || 'here'}${from.at ? ` · ${local(Date.parse(from.at))}` : ''}`)).addTo(routeLayer);
  }
  const dest = route.status === 'fulfilled' ? route.value.destination : null;
  if (dest && route.value.path.length) {
    const d = dest;
    const halo = L.polyline(route.value.path, { color: '#ffffff', weight: 5, opacity: 0.5, interactive: false }).addTo(routeLayer);
    // solid when the crew declared this port, dashed when we inferred it
    const dash = L.polyline(route.value.path, { color: palette.dark, weight: 3, dashArray: d.basis === 'declared' ? '10 6' : '4 8' }).addTo(routeLayer)
      .bindTooltip(`Route ahead to ${d.name}`);
    app.routeLines = { lines: [halo, dash], rest: route.value.path.slice(1) }; // the start follows the moving ship every frame
    L.marker([d.lat, d.lon], { icon: L.divIcon({ className: '', html: '<span class="dest-flag">⚑</span>', iconSize: [20, 20], iconAnchor: [4, 18] }) })
      .bindTooltip(() => h('span', {}, `Destination: ${d.name}`)).addTo(routeLayer);
  } else if (s) {
    const ahead = courseAhead(s);
    if (ahead) L.polyline(ahead, { color: palette.dark, weight: 2, opacity: 0.8, dashArray: '2 8', interactive: false }).addTo(routeLayer)
      .bindTooltip(`On its course for ${COURSE_AHEAD_H} h at ${s.sog.toFixed(1)} kn`);
  }
  renderDetail(track, states, route);
}
function renderDetail(track, states, route) {
  const s = app.ships.get(app.selected);
  if (!s) { detail.replaceChildren(h('p', { class: 'err' }, 'This ship is no longer in the feed.')); return; }
  const eff = effState(s);
  const r = route.status === 'fulfilled' ? route.value : null;
  const dest = r?.destination;
  const dec = r?.declared; // what the crew typed: may name a port we do not cover, or no destination yet
  const typedNote = dec?.text && !dec.orders && dec.port_id !== dest?.port_id ? `crew typed “${dec.text}”` : dec?.orders ? 'waiting for orders' : '';
  let destText;
  if (route.status === 'rejected') destText = h('span', { class: 'err' }, 'Route calculation unavailable');
  else if (dest) destText = [`${dest.name} (${dest.port_id})`, r.distance_nm == null ? '' : `${Math.round(r.distance_nm)} nm`, dest.basis === 'declared' || dest.basis === 'arrival' ? '' : 'inferred', typedNote].filter(Boolean).join(' · ');
  else destText = typedNote || 'not known';
  // Source: the port, else the place it was first heard (with coordinates), else the coordinates, else not known.
  const org = r?.origin;
  const orgAt = org?.at ? local(Date.parse(org.at)) : '';
  let srcText = 'not known';
  if (org?.name) srcText = `${org.name} (${org.port_id})${orgAt ? ` · ${org.basis === 'left this port' ? 'left' : 'first heard'} ${orgAt}` : ''}`;
  else if (org?.lat != null && org?.lon != null) {
    const xy = `${org.lat.toFixed(2)}, ${org.lon.toFixed(2)}`;
    srcText = `${org.place ? `${org.place} · ` : ''}${xy}${orgAt ? ` · first heard ${orgAt}` : ''}`;
  }
  // The crew's ETA when sent, else the one worked out from the speed.
  let eta = null;
  if (dec?.eta) eta = `${local(Date.parse(dec.eta))} (${Date.parse(dec.eta) >= app.feedTime ? `in ${dur(Date.parse(dec.eta) - app.feedTime)}` : `${dur(app.feedTime - Date.parse(dec.eta))} ago`})`;
  else if (r?.eta_hours != null) eta = `${dur(r.eta_hours * 3600000)} at ${s.sog.toFixed(1)} kn`;
  // Tracked, with names: the port it left, the port it is bound for, or the country's waters.
  const country = app.monitor.country ? countryName(app.monitor.country) : 'the monitored country';
  const tracked = (s.roles || []).map((role) => ({
    from: `left ${org?.name || 'a monitored port'}`,
    to: `bound for ${dest?.name || 'a monitored port'}`,
    in: `in ${country}'s waters`,
  })[role]).filter(Boolean).join(', ');
  const pinned = app.pins.has(s.mmsi);
  const statesBox = states.status === 'rejected'
    ? h('p', { class: 'err' }, 'State transition history unavailable')
    : states.value.length
      ? h('ul', { class: 'hist' }, states.value.map((st) => h('li', { class: st.future ? 'future' : null, title: st.seek ? 'Go to this time' : null, onclick: st.seek ? () => seekTo(st.seek) : null },
        h('span', {}, st.state), h('span', {}, st.port || '—'),
        h('span', {}, st.future ? local(Date.parse(st.since || st.ts)) : `${local(Date.parse(st.since || st.ts))} (${sinceFeed(st.since || st.ts)} ago)`))))
      : h('p', { class: 'empty' }, 'No state changes yet');
  detail.replaceChildren(...[
    h('div', { class: 'detail-head' },
      h('h3', {}, s.name || 'Unnamed ship'),
      h('button', { type: 'button', class: 'btn', 'aria-pressed': String(pinned), onclick: () => togglePin(s.mmsi) }, pinned ? '★ Pinned' : '☆ Pin'),
      h('button', { type: 'button', class: 'btn', title: 'Hide this ship until it starts waiting or goes dark', onclick: () => { setHidden(s.mmsi, true); deselect(); } }, 'Hide'),
      h('button', { type: 'button', class: 'btn', 'aria-label': 'Close ship detail', onclick: deselect }, '✕')),
    h('div', {}, pill(eff), ' ', s.state !== eff ? pill(s.state) : null, ' ',
      s.map === 'shown' ? h('small', { class: 'muted' }, 'from history: not on the live map') : null),
    h('dl', { class: 'kv' },
      ...[
        ['MMSI', s.mmsi],
        ['Flag', s.flag],
        ['Type', s.ship_type || 'unknown'],
        ['Speed', s.sog == null ? null : `${s.sog.toFixed(1)} kn${s.cog == null ? '' : ` · course ${Math.round(s.cog)}°`}`],
        ['Last seen', `${local(Date.parse(s.last_seen))} (${sinceFeed(s.last_seen)} ago)`],
        ['Position', [`${s.lat.toFixed(4)}, ${s.lon.toFixed(4)}`, s.estimated ? h('small', { class: 'muted' }, ` estimated along the sea route: out of AIS range, last report ${s.estimated_from.lat.toFixed(2)}, ${s.estimated_from.lon.toFixed(2)} ${sinceFeed(s.estimated_from.last_seen)} ago`) : null]],
        ['Tracked', tracked],
        ['Source', srcText],
        ['Destination', destText],
        ['ETA', eta],
      ].filter(([, v]) => v).flatMap(([k, v]) => [h('dt', {}, k), h('dd', {}, v)])),
    h('div', { class: 'detail-actions' },
      h('button', { type: 'button', class: 'btn', 'aria-pressed': String(app.follow), onclick: () => { app.follow = !app.follow; renderDetail(track, states, route); } }, 'Follow ship'),
      dest ? h('button', { type: 'button', class: 'btn', onclick: () => fitTo(r.path, { padding: [60, 60] }) }, 'Show route') : null,
      app.replay.focus?.mmsi === s.mmsi ? null : h('button', { type: 'button', class: 'btn', title: 'Replay this ship alone from its first report: the path it took, the waits, stops and signal gaps', onclick: () => startFocus(s.mmsi) },
        app.mode === 'offline' ? 'Replay this ship' : 'Replay in offline mode')),
    track.status === 'rejected' ? h('p', { class: 'empty' }, 'No track yet') : null,
    s.reasons?.length ? h('div', { class: 'sub' }, 'Why is it waiting? (likely)') : null,
    s.reasons?.length ? h('ul', { class: 'reasons' }, s.reasons.map((r) => h('li', { class: `reason ${r.code} ${r.level || ''}` },
      h('b', { class: `why why-${r.code in REASONS ? r.code : 'unknown'}`, 'aria-hidden': 'true' }, REASONS[r.code]?.glyph || '•'),
      h('span', {}, h('small', {}, REASONS[r.code]?.label || r.code), r.text)))) : null,
    h('div', { class: 'sub' }, 'Last states, newest first'),
    statesBox,
  ].filter(Boolean));
}

/* ---------- search: ships on the map and in history ---------- */
let searchTimer = 0;
let searchSeq = 0;
function closeSearch() { $('search-results').hidden = true; $('search-results').replaceChildren(); }
function searchSoon(text) {
  clearTimeout(searchTimer);
  if (text.trim().length < 2) { closeSearch(); return; }
  searchTimer = setTimeout(() => runSearch(text.trim()), 300);
}
async function runSearch(text) {
  if (app.mode === 'offline') { closeSearch(); return; } // the map filter above does the work on a replay
  const seq = ++searchSeq;
  const box = $('search-results');
  try {
    const hits = await api(`/api/ships/search?q=${encodeURIComponent(text)}`);
    if (seq !== searchSeq) return; // a newer search is on its way
    box.hidden = false;
    if (!hits.length) { box.replaceChildren(h('li', { class: 'empty' }, 'No ship with that name or MMSI')); return; }
    box.replaceChildren(...hits.map((hit) => h('li', {}, h('button', { type: 'button', class: 'ship-row', onclick: () => openShip(hit) },
      h('b', {}, hit.name || `MMSI ${hit.mmsi}`), pill(hit.state),
      h('small', {}, `${hit.mmsi} · ${portName(hit.port)} · seen ${local(Date.parse(hit.last_seen))}`),
      h('span', { class: `where ${hit.on_map ? 'on' : 'past'}` }, hit.on_map ? 'on map' : 'history')))));
  } catch (err) {
    box.hidden = false;
    box.replaceChildren(h('li', { class: 'err' }, 'Search service is temporarily unavailable. Please try again.'));
  }
}
async function openShip(hit) {
  closeSearch();
  if (app.hidden.has(hit.mmsi)) setHidden(hit.mmsi, false); // searching for it is asking to see it
  if (!app.ships.has(hit.mmsi)) { app.shown.add(hit.mmsi); await loadShips(); }
  if (app.ships.has(hit.mmsi)) select(hit.mmsi);
}

/* ---------- pins ---------- */
function togglePin(mmsi) {
  app.pins.has(mmsi) ? app.pins.delete(mmsi) : app.pins.add(mmsi);
  store.set('pins', [...app.pins]);
  restyle(mmsi);
  applyFilters();
  renderPins();
  renderAlerts();
  renderSummary();
  if (app.selected === mmsi) refreshSelected();
}
function renderPins() {
  const list = $('pins');
  if (!app.pins.size) {
    list.replaceChildren(h('li', { class: 'empty' }, 'Pin a ship from its detail panel to keep it here.'));
    return;
  }
  list.replaceChildren(...[...app.pins].map((mmsi) => {
    const s = app.ships.get(mmsi);
    const alerts = [...app.alerts.values()].filter((a) => a.mmsi === mmsi && !app.acked.has(a.alert_id)).length;
    return h('li', { class: 'pin-item' },
      h('button', { type: 'button', class: 'ship-row', onclick: () => s && select(mmsi) },
        h('b', {}, s ? s.name || `MMSI ${mmsi}` : `MMSI ${mmsi}`),
        s ? pill(effState(s)) : h('small', {}, 'not in feed'),
        h('small', {}, s ? `${portName(s.port)} · seen ${sinceFeed(s.last_seen)} ago` : mmsi),
        alerts ? h('span', { class: 'badge' }, `${alerts} alert${alerts > 1 ? 's' : ''}`) : h('span')),
      h('button', { type: 'button', class: 'btn', 'aria-label': `Unpin ${s?.name || mmsi}`, onclick: () => togglePin(mmsi) }, '✕'));
  }));
}

/* ---------- ports ---------- */
// How many ships wait at a port for each reason (a ship with two reasons counts in both).
function whyRow(portId) {
  const counts = {};
  for (const s of app.ships.values()) {
    if (s.state !== 'anchored' || s.port !== portId || isHidden(s)) continue;
    for (const c of reasonCodes(s)) counts[c] = (counts[c] || 0) + 1;
  }
  const codes = Object.keys(REASONS).filter((c) => counts[c]);
  if (!codes.length) return null;
  return h('span', { class: 'why-row', 'aria-label': 'Why ships are waiting' }, codes.map((c) =>
    h('span', { class: `why-chip why-${c}`, title: REASONS[c].label }, h('span', { 'aria-hidden': 'true' }, REASONS[c].glyph), ` ${counts[c]} ${REASONS[c].label.toLowerCase()}`)));
}
const countryName = (code) => { try { return new Intl.DisplayNames(['en'], { type: 'region' }).of(code) || code; } catch { return code; } };
function portCard(p) {
  return h('button', {
    type: 'button', class: 'port', 'aria-pressed': String(app.filters.port === p.port_id),
    title: 'Show only this port on the map',
    onclick: () => {
      const on = app.filters.port !== p.port_id;
      app.filters.port = on ? p.port_id : '';
      if (on) map.flyTo([p.lat, p.lon], 12, { duration: 0.8 });
      applyFilters(); renderPorts();
    },
  },
  h('div', {}, h('b', {}, p.name), h('small', {}, p.port_id),
    p.weather ? h('small', { class: `wx-line ${p.weather.condition}` }, wxText(p.weather)) : null,
    whyRow(p.port_id)),
  h('div', { class: `num${p.congested ? ' congested' : ''}`, title: p.congested ? 'Congested: long berth queue' : null }, String(p.waiting_now), h('small', {}, 'waiting now')),
  h('div', { class: 'num' }, p.window_count == null ? '—' : String(p.window_count), h('small', {}, '30-min window')));
}
/* The one place the monitored ports are shown and chosen: every port of the deployment's country, ticked when
   monitored (its queue card beside it). The country itself is not a choice. */
function renderPorts() {
  const box = $('ports'), m = app.monitor;
  const all = m.countries.find((c) => c.code === m.country)?.ports || [];
  $('h-ports').textContent = m.country ? `Port queues · ${countryName(m.country)} (${m.country})` : 'Port queues';
  $('ports-hint').textContent = m.country ? 'Tick the ports to monitor. The country is set when the stack is deployed.' : '';
  const rows = all.length ? all : app.ports.map((p) => ({ port_id: p.port_id, name: p.name }));
  if (!rows.length) { box.replaceChildren(h('p', { class: 'empty' }, 'No ports configured')); return; }
  const chosen = new Set(m.ports.length ? m.ports : rows.map((p) => p.port_id));
  const byId = new Map(app.ports.map((p) => [p.port_id, p]));
  box.replaceChildren(...rows.map((info) => {
    const on = chosen.has(info.port_id);
    const pick = h('label', { class: 'port-pick', title: `Monitor ${info.name}` },
      h('input', { type: 'checkbox', checked: on, 'aria-label': `Monitor ${info.name}`, onchange: (e) => {
        const next = new Set(chosen);
        if (e.target.checked) next.add(info.port_id); else next.delete(info.port_id);
        if (!next.size) next.add(info.port_id); // never none
        m.ports = [...next];
        debouncedSaveSelection();
      } }));
    const card = on && byId.has(info.port_id) ? portCard(byId.get(info.port_id))
      : h('div', { class: 'port off' }, h('b', {}, info.name), h('small', {}, on ? 'loading…' : 'not monitored'));
    return h('div', { class: 'port-row' }, pick, card);
  }));
}
async function loadPorts() {
  try {
    const firstLoad = !app.ports.length;
    const ids = monitoredPorts().join(',');
    app.ports = await api(app.mode === 'offline' ? `/api/ports?at=${isoOf(app.replay.at)}${ids ? `&ports=${ids}` : ''}` : `/api/ports${ids ? `?ports=${ids}` : ''}`);
    if (firstLoad) { // port names are known now
      renderAlerts();
      if (!$('reports').hidden) loadReports();
    }
    renderPorts();
    for (const p of app.ports) portLayer.addLayer(portMarker(p));
    drawStorms();
  } catch (err) {
    $('ports').replaceChildren(h('p', { class: 'err' }, 'Port queue data is temporarily unavailable.'));
  }
}
const portMarkers = new Map();
function portIcon(p) {
  // A congested port carries a red bubble with its queue. Only a number goes into the markup.
  const bubble = p.congested ? `<span class="queue-bubble">${Number(p.waiting_now) | 0}</span>` : '';
  return L.divIcon({ className: '', html: `<span class="dest-flag">⚓</span>${bubble}`, iconSize: [20, 20], iconAnchor: [10, 10] });
}
function portMarker(p) {
  if (!portMarkers.has(p.port_id)) {
    portMarkers.set(p.port_id, L.marker([p.lat, p.lon], { icon: portIcon(p), keyboard: false, zIndexOffset: -100000 }) // under the ships berthed around it
      .bindTooltip(() => { const c = app.ports.find((x) => x.port_id === p.port_id) || p; return h('span', {}, `${c.name}: ${c.waiting_now} waiting${c.congested ? ' (congested)' : ''}`); }));
  }
  const m = portMarkers.get(p.port_id);
  const key = `${p.congested}:${p.waiting_now}`;
  if (m.key !== key) { m.key = key; m.setIcon(portIcon(p)); }
  return m;
}

/* ---------- what is monitored ----------
   One country and a selection of its ports (common.monitor). Tracked ships are those that left a selected port,
   are bound for one from a partner country, or are in the country's waters; the others are not shown unless
   "Tracked ships only" is switched off. The choice is kept in this browser and sent with every request. */
const ROLE_LABEL = { from: 'left a monitored port', to: 'bound for a monitored port', in: "in the country's territorial sea or EEZ" };
app.monitor = { country: '', ports: [], partners: [], countries: [], coverage: [], waters: [], feed: null, basis: '', suggested: [], defaults: { ports: [], partners: [] }, maxPartners: 10 };
const watersLayer = L.layerGroup().addTo(map); // the country's own sea: any ship inside it is tracked, whatever its flag
function monitoredPorts() { // the ports to list in the port panel: the chosen ones, else all of the country's
  const m = app.monitor;
  if (!m.country) return [];
  return m.ports.length ? m.ports : (m.countries.find((c) => c.code === m.country)?.ports || []).map((p) => p.port_id);
}
function monitorQuery() {
  const m = app.monitor;
  if (!m.country) return '';
  // the country is the deployment's (the server holds every view to it); the viewer picks ports and source countries
  return `&ports=${m.ports.join(',')}&partners=${m.partners.join(',')}`;
}
function renderMonitor() {
  const m = app.monitor;
  // The source countries (all of a country's ports count): a ship is shown only when it started in one of these
  // AND is bound for a monitored port (or started at a monitored port and is bound for one of these). The
  // live feed listens to the coasts of the ones ticked, so ticking more means hearing more ships. Each row says
  // how many ships were seen on such voyages and how far coastal AIS follows them (green: the whole way);
  // "suggested" are the countries with the most ships seen, topped up by coverage. At most the server's MONITOR_MAX_PARTNERS.
  const picked = new Set(m.partners);
  const rows = [...m.coverage].sort((a, b) => (b.suggested - a.suggested) || (b.ships - a.ships) || (b.covered - a.covered));
  $('m-partners-sum').textContent = `Source countries (${picked.size})`;
  $('m-partners-list').replaceChildren(
    h('small', { class: 'muted' }, `Ships from these countries to ${countryName(m.country) || 'the monitored country'}, and back. Ticking a country makes the live feed listen to its coast.${m.basis ? ` Coverage: ${m.basis}.` : ''}`),
    h('div', { class: 'row-actions' },
      h('button', { type: 'button', class: 'btn', onclick: () => { m.partners = [...m.suggested]; debouncedSaveSelection(); } }, 'Use suggested'),
      h('button', { type: 'button', class: 'btn', onclick: () => { m.partners = [...m.defaults.partners]; debouncedSaveSelection(); } }, 'Back to default')),
    ...rows.map((c) => h('label', { class: `check cov ${c.covered >= 0.9 ? 'good' : c.covered >= 0.5 ? 'part' : 'poor'}` },
      h('input', { type: 'checkbox', checked: picked.has(c.country), disabled: !picked.has(c.country) && picked.size >= m.maxPartners, onchange: (e) => {
        const next = new Set(picked);
        if (e.target.checked) next.add(c.country); else next.delete(c.country);
        m.partners = [...next];
        debouncedSaveSelection();
      } }),
      ` ${countryName(c.country)} (${c.country}) · ${c.ships ? `${c.ships} ship${c.ships === 1 ? '' : 's'} seen · ` : ''}${Math.round(c.covered * 100)}% followed${c.gap_hours ? `, blind up to ${c.gap_hours} h` : ''}`,
      c.suggested ? h('span', { class: 'sug' }, ' ★ suggested') : null)));
  // the country's own sea on the map
  watersLayer.clearLayers();
  for (const w of m.waters) {
    waterLayer(w, { color: '#c98a0f', weight: 2, dashArray: w.kind === 'eez' ? '3 6' : '8 6', fillOpacity: 0.05, interactive: false }).addTo(watersLayer);
  }
  const f = m.feed;
  $('m-feed').textContent = !f ? ''
    : !f.matches ? `The live feed was started for ${f.country}, not ${m.country}: redeploy it (MONITOR_COUNTRY) so ${m.country}'s ships are received.`
    : !f.follows_selection ? 'The live feed switches to this choice within a minute.'
    : m.follow ? `Following ${m.follow.ships} voyage ships worldwide (up to ${m.follow.max}), until they arrive.`
    : '';
}
function applyMonitorChips() { initGroupFilter(); applyFilters(); }
function applyMonitor(cfg) {
  const m = app.monitor;
  Object.assign(m, {
    countries: cfg.countries, coverage: cfg.coverage, waters: cfg.waters, feed: cfg.feed, basis: cfg.coverage_basis,
    country: cfg.country, timezone: cfg.timezone, ports: cfg.ports, partners: cfg.partners, defaults: cfg.defaults, maxPartners: cfg.max_partners,
    follow: cfg.follow,
  });
  renderMonitor();
  applyMonitorChips();
  debouncedLoadSuggestions(); // from the ships in the store: not waited for
}
async function loadSuggestions() {
  const m = app.monitor;
  try {
    const found = await api(`/api/monitor/suggestions?ports=${m.ports.join(',')}`);
    m.suggested = found.suggested;
    for (const c of m.coverage) { c.ships = found.ships[c.country] || 0; c.suggested = found.suggested.includes(c.country); }
    renderMonitor();
  } catch { /* the list stays without the counts */ }
}
const debouncedSaveSelection = debounce(saveSelection, 400);
const debouncedLoadSuggestions = debounce(loadSuggestions, 300);

/* The choice is the server's (it also steers the live feed): save it, then show what the server holds. */
async function saveSelection() {
  const m = app.monitor;
  try {
    applyMonitor(await api('/api/monitor/selection', { method: 'PUT', body: { ports: m.ports, partners: m.partners } }));
  } catch (err) {
    banner('Unable to save port selection. Please check your connection and try again.');
    return;
  }
  // the other ports' markers and cards go, the chosen ones come
  for (const marker of portMarkers.values()) portLayer.removeLayer(marker);
  portMarkers.clear();
  renderPorts();
  reload();
  loadPorts();
  loadSeaRoute();
  if (app.coverageOn) loadCoverage();
}
async function loadMonitor() {
  try {
    applyMonitor(await api('/api/monitor')); // the server's: the environment's country, the saved ports and countries
    renderPorts();
    loadSeaRoute(); // decoration: not waited for
    if (baseName === COVERAGE_VIEW) setCoverage(true);
    await Promise.all([reload(), loadPorts()]); // the alert stream opens after these, so toasts know the ships
  } catch {
    await Promise.all([reload(), loadPorts()]); // the server has no monitoring: every ship and every port
  }
}

/* ---------- weather ---------- */
function wxText(w, where) {
  const parts = [w.wind_speed_kn == null ? null : `wind ${Math.round(w.wind_speed_kn)} kn`, w.wave_height_m == null ? null : `waves ${w.wave_height_m.toFixed(1)} m`].filter(Boolean);
  return `${WX[w.condition]?.label || w.condition}${where ? ` at ${where}` : ''}: ${parts.join(', ')} (${local(w.hour_ms)})`;
}
/* The weather the map shows: the sea points (/api/weather/grid) and the weather at each port. sky.js draws them as
   cloud, black where it storms; a cyclone is a spiral. Nothing is drawn again until a number has changed. */
let skyKey = '';
function drawStorms() {
  const ports = app.ports.filter((p) => p.weather).map((p) => ({ point_id: p.port_id, lat: p.lat, lon: p.lon, name: p.name, cyclone: false, ...p.weather }));
  app.sky = [...app.grid, ...ports];
  const key = app.sky.map((p) => `${p.point_id}:${p.wind_speed_kn}:${p.wave_height_m}:${p.cyclone}`).join('|');
  if (key === skyKey) return;
  skyKey = key;
  stormLayer.clearLayers();
  clouds.setPoints(app.sky, wrapLon);
  for (const p of app.grid) {
    if (!p.cyclone) continue;
    const m = L.marker([p.lat, p.lon], { icon: Sky.cycloneIcon(map, p.lat), keyboard: false, zIndexOffset: -80000 })
      .bindTooltip(() => h('span', {}, wxText({ ...p, condition: 'cyclone' }))).addTo(stormLayer);
    m.cyclone = p;
  }
}
map.on('zoomend', () => { // a cyclone is as wide as the storm: its picture follows the zoom
  stormLayer.eachLayer((m) => { if (m.cyclone) m.setIcon(Sky.cycloneIcon(map, m.cyclone.lat)); });
});
/* Pointing at the weather says what it is: cyclone, storm, rain and rough sea, or fair weather. */
const wxTip = L.tooltip({ direction: 'top', offset: [0, -4], className: 'wx-tip' });
let wxTipAt = 0;
function weatherAt(latlng) {
  const lon = wrapLon(latlng.lng);
  const cos = Math.cos((latlng.lat * Math.PI) / 180);
  let best = null, bestD = 5; // degrees: a weather point speaks for about 500 km round it
  for (const p of app.sky) {
    const d = Math.hypot((lon - wrapLon(p.lon)) * cos, latlng.lat - p.lat);
    if (d < bestD) { best = p; bestD = d; }
  }
  return best;
}
map.on('mousemove', (e) => {
  const now = performance.now();
  if (now - wxTipAt < 80) return;
  wxTipAt = now;
  const over = e.originalEvent.target;
  const w = over?.closest?.('.leaflet-marker-icon, .leaflet-control, .leaflet-tooltip') ? null : weatherAt(e.latlng);
  if (!w) { map.closeTooltip(wxTip); return; }
  wxTip.setContent(wxText({ ...w, condition: w.cyclone ? 'cyclone' : w.condition }, w.name || null));
  wxTip.setLatLng(e.latlng);
  if (!map.hasLayer(wxTip)) map.openTooltip(wxTip);
});
map.on('mouseout', () => map.closeTooltip(wxTip));
async function loadStorms() {
  try {
    app.grid = await api(app.mode === 'offline' ? `/api/weather/grid?at=${isoOf(app.replay.at)}` : '/api/weather/grid');
    drawStorms();
  } catch { /* weather at sea is decoration: the map still works without it */ }
}
/* The sea lanes ships are routed over, and the ports they join (the watched ports have their own anchor). */
const laneLayer = L.layerGroup().addTo(zoneLayer);
async function loadSeaRoute() {
  try {
    const m = app.monitor;
    const q = m.country ? `?ports=${m.ports.join(',')}&partners=${m.partners.join(',')}` : '';
    const { lanes, ports } = await api(`/api/sea-lanes${q}`);
    laneLayer.clearLayers();
    for (const lane of lanes) {
      L.polyline(lane, { color: '#5b7a99', weight: 2, opacity: 0.7, dashArray: '2 8', interactive: false }).addTo(laneLayer);
    }
    for (const p of ports.filter((x) => !x.watched)) {
      L.circleMarker([p.lat, p.lon], { radius: 3, color: '#fff', weight: 1, fillColor: '#5b7a99', fillOpacity: 1 })
        .bindTooltip(() => h('span', {}, `${p.name} (${p.port_id})`)).addTo(laneLayer);
    }
  } catch { /* decoration as well */ }
}

/* The country's sea: its territorial sea and EEZ as polygons, or the boxes/circles of the fallbacks. */
function waterLayer(w, style) {
  if (w.ring) return L.polygon([w.ring, ...(w.holes || [])], style); // holes: the islands are land
  if (w.radius_km != null) return L.circle([w.lat, w.lon], { ...style, radius: w.radius_km * 1000 });
  return L.rectangle([[w.min_lat, w.min_lon], [w.max_lat, w.max_lon]], style);
}

/* ---------- coverage: what the monitoring can see ---------- */
const coverageLayer = L.layerGroup();
const coverageLegend = L.control({ position: 'bottomright' });
coverageLegend.onAdd = () => {
  const sw = (cls, text) => h('span', {}, h('i', { class: `cov-sw ${cls}` }), text);
  return h('div', { class: 'cov-legend' },
    h('b', {}, 'Coverage'),
    h('small', { id: 'cov-count', class: 'muted' }, ''),
    sw('waters', 'Territorial sea + EEZ: every ship inside is shown'),
    sw('listen', 'Listening area: what the feed asks aisstream for'),
    sw('heard', 'AIS reach: ships were heard here (green outline = its boundary)'),
    sw('blind', 'Voyage leg nobody hears: inside the listening area, outside AIS reach'),
    sw('port', 'Monitored port'),
    sw('source', 'Source country (its main port)'));
};
async function loadCoverage() {
  const m = app.monitor;
  if (!m.country) return;
  try {
    const c = await api(`/api/coverage?ports=${m.ports.join(',')}&partners=${m.partners.join(',')}`);
    coverageLayer.clearLayers();
    const box = (b, style) => L.rectangle([[b.min_lat, b.min_lon], [b.max_lat, b.max_lon]], { interactive: false, ...style });
    for (const b of c.listening) box(b, { color: '#5fb3ff', weight: 2, fillColor: '#5fb3ff', fillOpacity: 0.12 }).addTo(coverageLayer);
    for (const cell of c.heard) {
      const half = cell.size / 2;
      const strength = Math.min(1, Math.log10(cell.ships + 1) / 2.5);
      L.rectangle([[cell.lat - half, cell.lon - half], [cell.lat + half, cell.lon + half]],
        { weight: 0, fillColor: '#2a9460', fillOpacity: 0.2 + 0.5 * strength })
        .bindTooltip(() => h('span', {}, `${cell.ships} ship${cell.ships === 1 ? '' : 's'} heard here in the survey`), { sticky: true })
        .addTo(coverageLayer);
    }
    for (const b of c.blind) {
      L.polyline(b.path, { color: '#c0392b', weight: 4, opacity: 0.9, dashArray: '2 7' })
        .bindTooltip(() => h('span', {}, `Out of AIS range on the way to ${b.country} (${b.to}): ${b.km} km, the position is estimated`), { sticky: true })
        .addTo(coverageLayer);
    }
    for (const e of c.reach) L.polyline(e, { color: '#1f7a4a', weight: 3, opacity: 0.95, interactive: false }).addTo(coverageLayer);
    for (const w of c.waters) {
      const eez = w.kind === 'eez';
      const style = { color: '#c98a0f', weight: eez ? 2 : 3, dashArray: eez ? '3 6' : '8 6', fillColor: '#c98a0f', fillOpacity: eez ? 0.06 : 0.22 };
      waterLayer(w, style)
        .bindTooltip(() => h('span', {}, `${c.country} ${eez ? 'exclusive economic zone' : 'territorial sea (12 nm)'}: every ship in here is shown`), { sticky: true })
        .addTo(coverageLayer);
    }
    const t = c.tracked || {};
    $('cov-count').textContent = `Shown now: ${t.in ?? 0} in the waters · ${t.from ?? 0} left ${c.country} · ${t.to ?? 0} bound for ${c.country}`;
    for (const s of c.sources) {
      L.circleMarker([s.lat, s.lon], { radius: 7, color: '#fff', weight: 2, fillColor: '#8e5a9b', fillOpacity: 1 })
        .bindTooltip(() => h('span', {}, `${s.country} · ${s.name}${s.covered == null ? '' : ` · ${Math.round(s.covered * 100)}% of the voyage heard${s.gap_hours ? `, blind up to ${s.gap_hours} h` : ''}`}`)).addTo(coverageLayer);
    }
    for (const p of c.monitored) {
      L.circleMarker([p.lat, p.lon], { radius: 8, color: '#fff', weight: 2, fillColor: '#c98a0f', fillOpacity: 1 })
        .bindTooltip(() => h('span', {}, `${p.name} (${p.port_id}): monitored port`)).addTo(coverageLayer);
    }
  } catch { /* decoration: the map works without it */ }
}
function setCoverage(on) {
  app.coverageOn = on;
  if (on) {
    coverageLayer.addTo(map);
    coverageLegend.addTo(map);
    loadCoverage();
  } else {
    coverageLayer.remove();
    coverageLegend.remove();
  }
}

/* ---------- alerts ---------- */
const prioOf = (a) => (a.mmsi && app.pins.has(a.mmsi) ? 'high' : app.prio[a.type] || 'medium');
function alertText(a) {
  const d = a.details || {};
  if (a.type === 'dark_ship') return `${shipName(a.mmsi)} stopped reporting${d.silent_hours != null ? ` ${d.silent_hours} h ago` : ''} near ${portName(a.port_id)}`;
  if (a.type === 'queue_growth') {
    return d.ships == null ? `${portName(a.port_id)}: queue is growing`
      : `${portName(a.port_id)}: waiting ships ${d.previous} → ${d.ships} (+${d.growth_pct}%)`;
  }
  return a.type;
}
function locate(a) {
  if (a.mmsi && app.ships.has(a.mmsi)) select(a.mmsi);
  else if (a.lat != null) map.flyTo([a.lat, a.lon], 13);
  else { const p = app.ports.find((x) => x.port_id === a.port_id); if (p) map.flyTo([p.lat, p.lon], 12); }
}
function ack(ids) {
  for (const id of ids) app.acked.add(id);
  store.set('acked', [...app.acked].slice(-1000));
  renderAlerts(); renderPins(); renderSummary();
}
function renderAlerts(freshId) {
  const all = [...app.alerts.values()];
  const unread = all.filter((a) => !app.acked.has(a.alert_id));
  $('unread').hidden = !unread.length;
  $('unread').textContent = String(unread.length);
  const nHigh = unread.filter((a) => prioOf(a) === 'high').length;
  document.title = nHigh ? `(${nHigh}) HarbourWatch` : 'HarbourWatch';
  const shown = all
    .filter((a) => app.prioFilter.has(prioOf(a)))
    .sort((a, b) => (app.acked.has(a.alert_id) - app.acked.has(b.alert_id))
      || RANK[prioOf(a)] - RANK[prioOf(b)]
      || (Date.parse(b.event_time) || 0) - (Date.parse(a.event_time) || 0))
    .slice(0, 100);
  if (!shown.length) { $('alerts').replaceChildren(h('li', { class: 'empty' }, all.length ? 'No alerts match this priority filter' : 'No alerts')); return; }
  $('alerts').replaceChildren(...shown.map((a) => {
    const p = prioOf(a);
    const li = h('li', {
      class: `alert ${p}${app.acked.has(a.alert_id) ? ' acked' : ''}${a.alert_id === freshId ? ' fresh' : ''}`,
    },
    h('div', { class: 'text' }, pill(p), ' ', h('b', {}, ALERT_TYPES[a.type] || a.type), h('div', {}, alertText(a))),
    app.acked.has(a.alert_id) ? h('span') : h('button', { type: 'button', class: 'btn', onclick: (e) => { e.stopPropagation(); ack([a.alert_id]); } }, 'Acknowledge'),
    h('div', { class: 'meta' }, `${local(Date.parse(a.event_time))} · click to locate`));
    li.addEventListener('click', () => locate(a));
    return li;
  }));
}
function ingest(a, live) {
  if (!a?.alert_id || app.alerts.has(a.alert_id)) return;
  app.alerts.set(a.alert_id, a);
  if (!live) return;
  computeDark();
  if (a.mmsi) restyle(a.mmsi);
  announce(a);
}
function announce(a) {
  // Notification popups disabled
}
async function loadAlerts() {
  try {
    for (const a of await api('/api/alerts?limit=500')) ingest(a, false);
    computeDark();
    for (const m of app.markers.values()) restyle(m.ship.mmsi);
    renderAlerts(); renderSummary();
  } catch (err) {
    $('alerts').replaceChildren(h('li', { class: 'err' }, 'Alert feed is temporarily unavailable.'));
  }
}
let streamWasDown = false;
function connectStream() {
  const status = $('stream');
  const es = new EventSource(apiUrl('/api/alerts/stream'));
  app.stream = es;
  es.onopen = () => {
    status.dataset.status = 'open'; status.textContent = 'Live';
    if (streamWasDown) { streamWasDown = false; loadAlerts(); } // fill what arrived while disconnected
  };
  es.onmessage = (e) => {
    try { const a = JSON.parse(e.data); ingest(a, true); renderAlerts(a.alert_id); renderPins(); renderSummary(); } catch { /* ignore a malformed event */ }
  };
  es.onerror = () => { streamWasDown = true; status.dataset.status = 'closed'; status.textContent = 'Alert stream disconnected — retrying'; };
}

function initAlertControls() {
  chipRow($('prio-chips'), PRIOS, app.prioFilter, (p) => {
    app.prioFilter.has(p) ? app.prioFilter.delete(p) : app.prioFilter.add(p);
    renderAlerts();
  });
  for (const type of Object.keys(ALERT_TYPES)) {
    const sel = $('prio-' + type);
    sel.replaceChildren(...PRIOS.map((p) => h('option', { value: p }, p)));
    sel.value = app.prio[type];
    sel.addEventListener('change', () => { app.prio[type] = sel.value; store.set('prio', app.prio); renderAlerts(); renderSummary(); });
  }
  const notify = $('notify-level');
  if (notify) {
    notify.value = app.notify;
    notify.addEventListener('change', () => {
      app.notify = notify.value;
      store.set('notify', app.notify);
    });
  }
  $('ack-all').addEventListener('click', () => ack([...app.alerts.keys()]));
}

/* ---------- reports (warehouse) ---------- */
const NS = 'http://www.w3.org/2000/svg';
const num = (v, d = 1) => (v == null ? '—' : Number(v).toFixed(d));
function table(head, rows) {
  if (!rows.length) return h('p', { class: 'empty' }, 'No data yet — the nightly job has not loaded any days');
  return h('table', {}, h('thead', {}, h('tr', {}, head.map((t) => h('th', { scope: 'col' }, t)))),
    h('tbody', {}, rows.map((r) => h('tr', {}, r.map((c) => h('td', {}, c))))));
}
// Per-day chart: one measure, one axis, a series per port. Bars for queue sizes, a line for waits.
const SERIES = ['--series-1', '--series-2', '--series-3']; // port colours in fixed order (app.css)
const svgEl = (tag, attrs = {}, ...kids) => {
  const e = document.createElementNS(NS, tag);
  for (const [k, v] of Object.entries(attrs)) if (v != null) e.setAttribute(k, v);
  e.append(...kids);
  return e;
};
const dayLabel = (d) => new Date(`${d}T00:00Z`).toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
function niceMax(v) {
  if (!(v > 0)) return 1;
  const step = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * step).find((m) => m >= v);
}
function chart({ dates, ports, value, unit, kind, digits }) {
  const W = Math.max(320, $('trends').clientWidth || 700), H = 240, L = 44, R = 16, T = 16, B = 34;
  const max = niceMax(Math.max(0, ...ports.flatMap((p) => p.rows.map(value).filter((v) => v != null))));
  const y = (v) => T + (H - T - B) * (1 - v / max);
  const band = (W - L - R) / dates.length;
  const xMid = (i) => L + band * (i + 0.5);
  const svg = svgEl('svg', { viewBox: `0 0 ${W} ${H}`, width: W, height: H, class: 'chart', role: 'img' });
  for (let k = 0; k <= 4; k++) { // recessive grid with the axis values
    const v = (max * k) / 4;
    svg.append(svgEl('line', { x1: L, x2: W - R, y1: y(v), y2: y(v), class: k ? 'grid' : 'base' }),
      svgEl('text', { x: L - 6, y: y(v) + 4, class: 'tick', 'text-anchor': 'end' }, num(v, v % 1 ? 1 : 0)));
  }
  const every = Math.ceil(dates.length / Math.max(1, Math.floor((W - L - R) / 80))); // keep date labels apart
  dates.forEach((d, i) => { if (i % every === 0) svg.append(svgEl('text', { x: xMid(i), y: H - 12, class: 'tick', 'text-anchor': 'middle' }, dayLabel(d))); });
  const tip = (port, d, v) => `${port.name}, ${dayLabel(d)}: ${v == null ? 'no data' : `${num(v, digits)} ${unit}`}`;
  ports.forEach((port, pi) => {
    const byDate = new Map(port.rows.map((r) => [r.date, value(r)]));
    const colour = `var(${SERIES[pi % SERIES.length]})`;
    let peak = Math.max(...port.rows.map(value).filter((v) => v != null)); // labelled once, on its first day
    if (kind === 'bars') {
      const w = Math.min(28, (band - 8) / ports.length - 2);
      dates.forEach((d, i) => {
        const v = byDate.get(d);
        if (v == null) return;
        const x = xMid(i) - (ports.length * (w + 2)) / 2 + pi * (w + 2) + 1;
        const top = y(v), r = Math.min(4, w / 2, y(0) - top);
        // rounded at the data end, square on the baseline
        svg.append(svgEl('path', { d: `M${x},${y(0)}V${top + r}q0,-${r} ${r},-${r}h${w - 2 * r}q${r},0 ${r},${r}V${y(0)}Z`, fill: colour, class: 'mark', 'data-tip': tip(port, d, v) }));
        if (v === peak && dates.length <= 31 && (peak = null) === null) svg.append(svgEl('text', { x: x + w / 2, y: top - 5, class: 'label', 'text-anchor': 'middle' }, num(v, digits)));
      });
    } else {
      // the line breaks on days without a value instead of joining across them
      let seg = '';
      dates.forEach((d, i) => { const v = byDate.get(d); seg += v == null ? ' ' : `${seg.endsWith(' ') || !seg ? 'M' : 'L'}${xMid(i).toFixed(1)},${y(v).toFixed(1)}`; });
      svg.append(svgEl('path', { d: seg.replace(/ /g, ''), fill: 'none', stroke: colour, class: 'line' }));
      dates.forEach((d, i) => {
        const v = byDate.get(d);
        if (v == null) return;
        svg.append(svgEl('circle', { cx: xMid(i), cy: y(v), r: 5, fill: colour, class: 'dot' }),
          svgEl('circle', { cx: xMid(i), cy: y(v), r: 14, class: 'hit', 'data-tip': tip(port, d, v) }));
        if (v === peak && (peak = null) === null) svg.append(svgEl('text', { x: xMid(i), y: y(v) - 10, class: 'label', 'text-anchor': 'middle' }, `${num(v, digits)} ${unit}`));
      });
    }
  });
  svg.setAttribute('aria-label', ports.map((p) => `${p.name}: ${p.rows.map((r) => `${dayLabel(r.date)} ${num(value(r), digits)}`).join(', ')}`).join('; '));
  return svg;
}
function chartCard(title, note, ports, svg) {
  return h('figure', { class: 'chart-card' },
    h('figcaption', {}, h('b', {}, title), h('small', {}, note)),
    h('div', { class: 'chart-legend' }, ports.map((p, i) => h('span', {}, h('i', { style: `background:var(${SERIES[i % SERIES.length]})` }), p.name))),
    svg);
}
// One plain sentence per port so the numbers read without the chart.
function portSummary(port) {
  const calls = port.rows.reduce((n, r) => n + r.calls, 0);
  const waits = port.rows.filter((r) => r.median_wait_hours != null);
  const peak = port.rows.reduce((a, r) => (r.max_hourly_queue > (a?.max_hourly_queue ?? -1) ? r : a), null);
  const lastWait = waits.at(-1);
  return h('li', {},
    h('b', {}, port.name), ` · ${calls} port call${calls === 1 ? '' : 's'} in ${port.rows.length} day${port.rows.length === 1 ? '' : 's'}`,
    peak ? ` · longest queue ${peak.max_hourly_queue} ships on ${dayLabel(peak.date)}` : '',
    lastWait ? ` · median wait ${num(lastWait.median_wait_hours)} h on ${dayLabel(lastWait.date)}` : ' · no wait measured yet',
    ' · ', h('span', { class: 'pill' }, `Congestion ${trendOf(port.rows.map((r) => r.max_hourly_queue))}`));
}
const chartTip = h('div', { class: 'chart-tip', role: 'tooltip', hidden: true });
document.body.append(chartTip);
document.addEventListener('pointerover', (e) => {
  const t = e.target.closest?.('[data-tip]');
  chartTip.hidden = !t;
  if (t) chartTip.textContent = t.getAttribute('data-tip');
});
document.addEventListener('pointermove', (e) => {
  if (!chartTip.hidden) Object.assign(chartTip.style, { left: `${Math.min(e.clientX + 12, innerWidth - chartTip.offsetWidth - 8)}px`, top: `${e.clientY - 36}px` });
});
// Rising / falling / steady: the second half of the period against the first.
function trendOf(values) {
  const v = values.filter((n) => n != null);
  if (v.length < 2) return 'not enough days';
  const mid = Math.floor(v.length / 2);
  const avg = (a) => a.reduce((s, n) => s + n, 0) / a.length;
  const a = avg(v.slice(0, mid)), b = avg(v.slice(mid));
  if (b > a * 1.1 + 0.01) return 'rising';
  if (b < a * 0.9 - 0.01) return 'falling';
  return 'steady';
}
async function loadReports() {
  const days = $('r-days').value;
  try {
    const [cmp, daily] = await Promise.all([api('/api/reports/compare'), api(`/api/reports/ports?days=${days}`)]);
    $('compare-note').textContent = 'Cargo ships and tankers with an observed arrival, by the month of the newest loaded day.';
    $('compare').replaceChildren(table(['Port', 'This month (h)', 'Last month (h)', 'Change'],
      cmp.map((r) => [portName(r.port_id), num(r.this_month_median), num(r.last_month_median),
        r.change_pct == null ? '—' : `${r.change_pct > 0 ? '+' : ''}${r.change_pct}%`])));
    const dates = [...new Set(daily.map((r) => r.date))].sort();
    const ports = [...Map.groupBy(daily, (r) => r.port_id)].map(([id, rows]) => ({ id, name: portName(id), rows }));
    $('trends').replaceChildren(...(ports.length ? [
      h('ul', { class: 'summary-list' }, ports.map(portSummary)),
      chartCard('Ships waiting at anchor', 'The most ships waiting in any one hour of the day: the size of the queue for a berth.', ports,
        chart({ dates, ports, value: (r) => r.max_hourly_queue, unit: 'ships', kind: 'bars', digits: 0 })),
      chartCard('Median wait before berthing (hours)', 'Half the cargo ships and tankers that berthed that day waited longer than this, from arriving at the anchorage. A gap means no such ship berthed.', ports,
        chart({ dates, ports, value: (r) => r.median_wait_hours, unit: 'h', kind: 'line', digits: 1 })),
    ] : []));
    $('daily').replaceChildren(table(['Date', 'Port', 'Calls', 'Median wait (h)', 'Avg turnaround (h)', 'Max queue'],
      daily.slice().reverse().map((r) => [r.date, portName(r.port_id), String(r.calls), num(r.median_wait_hours),
        num(r.avg_turnaround_hours), String(r.max_hourly_queue)])));
  } catch (err) {
    $('compare').replaceChildren(h('p', { class: 'err' }, 'Performance reports are temporarily unavailable.'));
    $('trends').replaceChildren(); $('daily').replaceChildren();
  }
}
function showView(name) {
  const reports = name === 'reports';
  document.querySelector('.layout').hidden = reports;
  $('reports').hidden = !reports;
  $('tab-live').setAttribute('aria-pressed', String(!reports));
  $('tab-reports').setAttribute('aria-pressed', String(reports));
  if (reports) loadReports(); else map.invalidateSize();
}

/* ---------- start ---------- */
// Run fn again `ms` after the previous run finished, so a slow API never gets overlapping requests.
function every(fn, ms) {
  const next = () => setTimeout(() => Promise.resolve(app.mode === 'live' ? fn() : null).finally(next), ms);
  next();
}

/* ---------- the two data modes ----------
   Live feed: the live API is polled (ships, ports, weather) and the alert stream is open.
   Offline replay: no live API calls at all. The map shows the history (NOAA and past aisstream data) of a day
   and time the user picks, can play it forward at a chosen speed, and a ship's whole journey can be replayed. */
const reload = () => (app.mode === 'offline' ? loadSnapshot() : loadShips());
const isoOf = (ms) => new Date(ms).toISOString();
const dayOf = (ms) => isoOf(ms).slice(0, 10);
const minuteOf = (ms) => { const d = new Date(ms); return d.getUTCHours() * 60 + d.getUTCMinutes(); };

function setMode(mode, { first = false } = {}) {
  if (!first && app.mode === mode) return;
  app.mode = mode;
  store.set('mode', mode);
  app.replay.focus = null;
  document.body.classList.remove('focus');
  $('rp-scrub').max = '1439';
  if (app.selected) deselect();
  $('mode-live').setAttribute('aria-pressed', String(mode === 'live'));
  $('mode-offline').setAttribute('aria-pressed', String(mode === 'offline'));
  document.body.classList.toggle('offline', mode === 'offline');
  $('replay-bar').hidden = mode !== 'offline';
  if (!first) { // what the other mode drew must not stay on the map
    for (const m of app.markers.values()) shipLayer.removeLayer(m.marker);
    app.markers.clear();
    app.ships = new Map();
    app.first = true;
    banner('');
  }
  const status = $('stream');
  if (mode === 'offline') {
    app.stream?.close();
    app.stream = null;
    streamWasDown = false;
    app.replay.playing = false;
    status.dataset.status = 'closed';
    status.textContent = 'Offline replay: live alerts are off';
    app.alerts.clear(); // live alerts say nothing about a past day
    app.dark = new Set();
    renderAlerts();
    renderSummary();
    mapStatus('Loading history…');
    loadReplayDates().then(() => {
      const ship = app.pendingFocus;
      app.pendingFocus = null;
      return ship ? startFocus(ship) : loadSnapshot();
    });
    loadPorts();
    loadStorms();
  } else {
    app.replay.playing = false;
    clockLog.length = 0; // the time spent offline must not be measured as a slow feed
    mapStatus('Loading ships…');
    if (!first) {
      loadShips();
      loadPorts();
      loadStorms();
      loadAlerts().then(() => { if (!app.stream) connectStream(); });
    }
  }
  renderClock();
}

/* ---------- offline replay ---------- */
const REPLAY_DAY_END = 24 * 60 - 1;
// NOAA's files hold US waters only: ships on international voyages come from the live feed's archive.
const SOURCE_LABEL = { noaa: 'NOAA (US waters only)', aisstream: 'live feed archive' };
const dayOption = (d) => h('option', { value: d.date }, `${d.date} · ${d.ships} ships · ${d.sources.map((s) => SOURCE_LABEL[s] || s).join(' + ')}`);
async function loadReplayDates() {
  const sel = $('rp-day');
  try {
    app.replay.dates = await api('/api/history/dates');
  } catch (err) {
    app.replay.dates = [];
    banner('Historical replay data is currently unavailable.');
  }
  if (!app.replay.dates.length) {
    sel.replaceChildren(h('option', { value: '' }, 'no history'));
    mapStatus('No historical data available for replay yet.');
    return;
  }
  sel.replaceChildren(...app.replay.dates.map(dayOption));
  const saved = store.get('rpAt', 0);
  const busiest = app.replay.dates.reduce((a, b) => (b.ships > a.ships ? b : a));
  const start = saved && app.replay.dates.some((d) => d.date === dayOf(saved)) ? saved : busiest.first_ts;
  // the user may already have chosen a time while the days were loading: keep it
  setReplayAt(app.replay.chosen ? app.replay.at : Math.floor(start / 60000) * 60000, { fetch: false });
}
function setReplayAt(ms, { fetch = true } = {}) {
  if (fetch) app.replay.chosen = true;
  app.replay.at = ms;
  store.set('rpAt', ms);
  const day = dayOf(ms);
  if ($('rp-day').value !== day) $('rp-day').value = day;
  const mins = minuteOf(ms);
  $('rp-time').value = `${pad(Math.floor(mins / 60))}:${pad(mins % 60)}`;
  const f = app.replay.focus;
  $('rp-scrub').value = String(f ? Math.round(((ms - f.start) / (f.end - f.start || 1)) * 1000) : mins);
  if (fetch) loadSnapshot();
}
async function loadSnapshot() {
  if (app.mode !== 'offline' || !app.replay.at) return;
  const seq = ++app.replay.seq;
  try {
    const f = app.replay.focus;
    const list = await api(`/api/history/ships?at=${isoOf(app.replay.at)}&limit=${app.limit}${f ? `&mmsi=${f.mmsi}` : monitorQuery()}`);
    if (seq !== app.replay.seq || app.mode !== 'offline') return; // a newer time was asked for meanwhile
    updateShips(list);
    banner('');
    mapStatus(list.length ? '' : f ? 'This ship has not reported yet at this time' : 'No ship reported in the hour before this time');
    $('rp-clock').textContent = f ? `${utc(app.replay.at)} · journey ${utc(f.start)} to ${utc(f.end)}` : `${utc(app.replay.at)} · ${list.length} ships`;
    if (f && !app.selected && app.ships.has(f.mmsi)) select(f.mmsi, { fly: false });
    else if (app.selected) refreshSelected();
    if (app.replay.ticks++ % 5 === 0 || !app.replay.playing) { loadPorts(); loadStorms(); }
  } catch (err) {
    if (err.code === 'NOT_MONITORED') { // the feed did not cover this country then: an empty sea would mislead
      updateShips([]);
      banner('');
      mapStatus('No tracking data available for the monitored region at this time.');
      return;
    }
    banner('Historical vessel positions are currently unavailable.');
  }
}
function setPlaying(on) {
  app.replay.playing = on;
  const btn = $('rp-play');
  btn.textContent = on ? '❚❚ Pause' : '▶ Play';
  btn.setAttribute('aria-pressed', String(on));
  const now = performance.now();
  app.rate = on ? app.replay.speed : 0; // ships stop at once when paused
  app.feedWall = now;
  app.feedTime = app.replay.at;
  renderClock();
}
async function replayTick() {
  if (app.mode !== 'offline' || !app.replay.playing || app.replay.busy) return;
  app.replay.busy = true;
  try {
    let at = app.replay.at + app.replay.speed * 1000;
    const f = app.replay.focus;
    if (f) { // one ship: stop when its data ends
      if (at >= f.end) { at = f.end; setPlaying(false); }
    } else if (!app.replay.dates.some((d) => d.date === dayOf(at))) { // past the last day with data: stop at the end of the day
      at = Date.parse(`${dayOf(app.replay.at)}T23:59:00Z`);
      setPlaying(false);
    }
    setReplayAt(at, { fetch: false });
    await loadSnapshot();
  } finally { app.replay.busy = false; }
}
function initReplayControls() {
  $('mode-live').addEventListener('click', () => setMode('live'));
  $('mode-offline').addEventListener('click', () => setMode('offline'));
  $('rp-day').addEventListener('change', () => {
    const d = app.replay.dates.find((x) => x.date === $('rp-day').value);
    if (d) setReplayAt(Math.floor(d.first_ts / 60000) * 60000);
  });
  const fromControls = (minutes) => setReplayAt(Date.parse(`${$('rp-day').value}T00:00:00Z`) + minutes * 60000);
  $('rp-time').addEventListener('change', (e) => {
    const [hh, mm] = e.target.value.split(':').map(Number);
    if (Number.isFinite(hh) && Number.isFinite(mm)) fromControls(hh * 60 + mm);
  });
  $('rp-scrub').addEventListener('input', (e) => {
    const f = app.replay.focus;
    if (f) setReplayAt(Math.round(f.start + (Number(e.target.value) / 1000) * (f.end - f.start)));
    else fromControls(Math.min(REPLAY_DAY_END, Number(e.target.value)));
  });
  $('rp-focus-exit').addEventListener('click', endFocus);
  $('rp-play').addEventListener('click', () => {
    const f = app.replay.focus;
    if (!app.replay.playing && f && app.replay.at >= f.end) setReplayAt(f.start, { fetch: false }); // play it again
    setPlaying(!app.replay.playing);
  });
  $('rp-speed').value = String(app.replay.speed);
  $('rp-speed').addEventListener('change', (e) => {
    app.replay.speed = Number(e.target.value);
    store.set('rpSpeed', app.replay.speed);
    if (app.replay.playing) setPlaying(true);
  });
  setInterval(replayTick, 1000);
}

/* A selected ship in a replay: its journey cut at the replay time, shaped like the live track, states and route.
   The journey is fetched once per ship; the plan (the way on to the destination) again after half an hour of
   replay time, because the ship moves past its waypoints. */
const nm = (a, b) => (L.latLng(a).distanceTo(L.latLng(b)) / 1852);
const EVENT_COLOR = { first_seen: '#586b79', anchored: '#c98a0f', waited: '#c98a0f', berthed: '#2a9460', arrived: '#2a9460', departed: '#586b79', underway: '#2f6fd6', signal_lost: '#d6393a', stopped: '#c98a0f' };
const seekTo = (ms) => { if (app.mode === 'offline') { app.replay.playing && setPlaying(false); setReplayAt(ms); } };
async function offlineSelected(mmsi) {
  try {
    if (app.replay.journey?.mmsi !== mmsi) {
      app.replay.journey = { mmsi, data: await api(`/api/history/ships/${mmsi}/journey?full=true`), plan: null };
    }
    const j = app.replay.journey, data = j.data, at = app.replay.at;
    const s = app.ships.get(mmsi);
    const here = s ? [s.lat, s.lon] : null;
    const taken = data.track.filter((p) => p[0] <= at).map(([ts, lat, lon]) => ({ ts, lat, lon })).reverse(); // newest first
    if (here) taken.unshift({ ts: at, lat: here[0], lon: here[1] });
    const track = { status: 'fulfilled', value: taken, future: [], events: [] };
    if (app.replay.focus) {
      track.future = data.track.filter((p) => p[0] > at).map((p) => [p[1], p[2]]);
      if (here && track.future.length) track.future.unshift(here);
      track.events = data.events.map((e) => ({ ...e, future: e.ts > at }));
    }
    const shown = data.events.filter((e) => e.type !== 'first_seen' && (app.replay.focus || e.ts <= at));
    const states = { status: 'fulfilled', value: shown.map((e) => ({
      state: e.text, port: '', since: isoOf(e.ts), ts: isoOf(e.ts), future: e.ts > at, seek: e.ts,
    })).reverse() };
    const d = data.destination;
    let path = [], distance = null;
    if (d?.port_id && here) {
      const plan = j.plan;
      if (!plan || plan.to !== d.port_id || Math.abs(plan.at - at) > 30 * 60000) {
        try {
          const r = await api(`/api/route?lat=${here[0]}&lon=${here[1]}&to=${d.port_id}`);
          j.plan = { to: d.port_id, at, path: r.path, nm: r.distance_nm };
        } catch { j.plan = { to: d.port_id, at, path: [], nm: null }; }
      }
      path = j.plan.path.length ? [here, ...j.plan.path.slice(1)] : [];
      distance = j.plan.nm;
    }
    const route = { status: 'fulfilled', value: {
      origin: data.origin,
      destination: d ? { port_id: d.port_id, name: d.name || 'unknown', lat: d.lat, lon: d.lon, basis: d.basis } : null,
      path, distance_nm: distance, eta_hours: distance && s?.sog >= MOVING_KN ? Math.round((distance / s.sog) * 100) / 100 : null,
      declared: { text: data.declared, eta: null, port_id: d?.basis === 'declared' ? d.port_id : null, orders: false },
    } };
    return [track, states, route];
  } catch (err) {
    const no = { status: 'rejected', reason: err };
    return [no, no, no];
  }
}

/* ---------- replaying one ship ----------
   The same replay clock, bar and map as the whole fleet, narrowed to one ship: from its first report to the last
   data (or its arrival). Its path taken is drawn as the clock moves, the data still to come faintly, and the way
   on to the destination dashed. "All ships" goes back to the day. */
async function startFocus(mmsi) {
  if (app.mode !== 'offline') { app.pendingFocus = mmsi; setMode('offline'); return; }
  let data;
  try {
    data = await api(`/api/history/ships/${mmsi}/journey?full=true`);
  } catch (err) {
    banner('Voyage history for this vessel is currently unavailable.');
    return;
  }
  app.replay.journey = { mmsi, data, plan: null };
  const f = { mmsi, start: data.start_ts, end: data.end_ts };
  app.replay.focus = f;
  document.body.classList.add('focus');
  $('rp-focus-name').textContent = data.name || `MMSI ${mmsi}`;
  $('rp-scrub').max = '1000';
  // fast enough to play the whole journey in about a minute
  const speeds = [...$('rp-speed').options].map((o) => Number(o.value));
  app.replay.speed = speeds.find((v) => v >= (f.end - f.start) / 60000) || speeds.at(-1);
  $('rp-speed').value = String(app.replay.speed);
  setPlaying(false);
  app.first = true; // the map fits the ship's journey when it is drawn
  setReplayAt(f.start, { fetch: false });
  await loadSnapshot();
  fitTo(data.track.map((p) => [p[1], p[2]]), { padding: [60, 60], maxZoom: 12 });
  if (app.ships.has(mmsi)) select(mmsi, { fly: false });
  setPlaying(true);
}
function endFocus() {
  if (!app.replay.focus) return;
  app.replay.focus = null;
  document.body.classList.remove('focus');
  $('rp-scrub').max = '1439';
  if (app.selected) deselect();
  setReplayAt(app.replay.at);
}

function init() {
  readPalette();
  initStateChips();
  initAlertControls();
  renderPins();
  $('q').addEventListener('input', (e) => { app.filters.q = e.target.value; applyFilters(); });
  $('f-type').addEventListener('change', (e) => { app.filters.type = e.target.value; applyFilters(); });
  $('f-group').addEventListener('change', (e) => { app.filters.group = e.target.value; applyFilters(); });
  $('f-pinned').addEventListener('change', (e) => { app.filters.pinned = e.target.checked; applyFilters(); });
  $('f-limit').value = String(app.limit);
  $('f-limit').addEventListener('change', (e) => { app.limit = Number(e.target.value); store.set('limit', app.limit); reload(); });
  $('unhide').addEventListener('click', () => { for (const m of [...app.hidden.keys()]) setHidden(m, false); });
  renderHidden();
  $('q').addEventListener('input', (e) => searchSoon(e.target.value));
  $('q').addEventListener('keydown', (e) => { if (e.key === 'Escape') closeSearch(); });
  $('f-reset').addEventListener('click', resetFilters);
  $('filters').addEventListener('submit', (e) => e.preventDefault());
  $('retry').addEventListener('click', reload);
  $('tab-live').addEventListener('click', () => showView('live'));
  $('tab-reports').addEventListener('click', () => showView('reports'));
  $('r-days').addEventListener('change', loadReports);
  addEventListener('resize', () => { if (!$('reports').hidden) loadReports(); });
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape' && app.selected) deselect(); });
  map.on('dragstart', () => { app.follow = false; });
  $('ports').replaceChildren(h('p', { class: 'empty' }, 'Loading ports…'));
  $('alerts').replaceChildren(h('li', { class: 'empty' }, 'Connecting…'));
  mapStatus('Loading ships…');

  initReplayControls();
  const monitorReady = loadMonitor();
  setMode(app.mode, { first: true });
  loadZones();
  loadSeaRoute();
  loadHealth();
  if (app.mode === 'live') {
    loadStorms();
    // the ships and ports wait for what is monitored (loadMonitor loads them): one tracked list, not all and then tracked
    Promise.all([loadAlerts(), monitorReady]).then(() => { if (app.mode === 'live' && !app.stream) connectStream(); });
  }
  every(loadShips, POLL_MS);
  every(loadPorts, PORT_MS);
  every(loadStorms, GRID_MS);
  setInterval(loadHealth, HEALTH_MS);
  setInterval(renderClock, 1000);
  requestAnimationFrame(frame);
}
init();
