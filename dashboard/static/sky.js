'use strict';
/* Weather drawn like a satellite picture: soft white cloud fields lit from the upper left with a shadow on the sea,
   and cyclones with an eye, a bright eye wall and spiral rain bands.

   The weather itself comes from the weather points of the API (wind and waves at about 40 places along the sea
   route). Between the points the cover is interpolated; the shape of the clouds is fractal noise, so it is a
   picture of the weather, not a photograph: more wind means more and thicker cloud, a cyclone is a spiral.

   Sky.CloudLayer   a Leaflet tile layer (canvas tiles, 128 px drawn, 256 px shown)
   Sky.cycloneIcon  a Leaflet divIcon with the rendered cyclone, rotating slowly (see app.css) */
const Sky = (() => {
  const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
  const smooth = (a, b, x) => { const t = clamp((x - a) / (b - a), 0, 1); return t * t * (3 - 2 * t); };

  /* Value noise, 5 octaves: about 0.5 on average, 0..1. */
  function hash(ix, iy) {
    let h = (Math.imul(ix, 374761393) + Math.imul(iy, 668265263)) | 0;
    h = Math.imul(h ^ (h >>> 13), 1274126177);
    return ((h ^ (h >>> 16)) >>> 0) / 4294967295;
  }
  function vnoise(x, y) {
    const ix = Math.floor(x), iy = Math.floor(y);
    const fx = x - ix, fy = y - iy;
    const u = fx * fx * (3 - 2 * fx), v = fy * fy * (3 - 2 * fy);
    const a = hash(ix, iy), b = hash(ix + 1, iy), c = hash(ix, iy + 1), d = hash(ix + 1, iy + 1);
    return a + (b - a) * u + (c - a) * v + (a - b - c + d) * u * v;
  }
  function fbm(x, y) {
    let amp = 0.5, freq = 1, sum = 0;
    for (let i = 0; i < 5; i++) { sum += amp * vnoise(x * freq, y * freq); freq *= 2.03; amp *= 0.5; }
    return sum / 0.97;
  }

  /* ---- cloud fields over the weather points ---- */
  const SIGMA_DEG = 6.5; // how far a weather point's wind is trusted
  const NOISE_PER_DEG = 0.3; // about one cloud mass every 3 degrees, finer detail inside
  const WIND_CALM_KN = 8, WIND_RANGE_KN = 45;

  let lastCover = 0; // the wind-driven cover (0 calm .. 1 storm) of the place density() was asked about last

  /* Cloud density 0..1 at a place: the points set how much cloud, the noise where it is. */
  function density(pts, cyclones, lat, lon) {
    const cos = Math.cos((lat * Math.PI) / 180);
    let wsum = 0, vsum = 0;
    for (const p of pts) {
      const dy = lat - p.lat, dx = (lon - p.lon) * cos;
      const d2 = dx * dx + dy * dy;
      if (d2 > 520) continue; // further than 3.5 sigma: the weight is under 0.002
      const w = Math.exp(-d2 / (2 * SIGMA_DEG * SIGMA_DEG));
      wsum += w;
      vsum += w * p.f;
    }
    if (wsum < 0.03) return 0; // no weather data here: no cloud is drawn
    const cover = vsum / wsum; // 0 calm .. 1 storm
    lastCover = cover;
    let known = smooth(0.03, 0.4, wsum);
    for (const c of cyclones) { // the cyclone's own picture is drawn there: clear the field round it
      const dy = lat - c.lat, dx = (lon - c.lon) * cos;
      known *= smooth(4, 10, Math.hypot(dx, dy));
    }
    const x = lon * NOISE_PER_DEG, y = lat * NOISE_PER_DEG;
    // domain warping bends the masses into swirls and streaks, as in the real thing
    const n = fbm(x + 1.5 * (vnoise(x * 0.6 + 11.3, y * 0.6 + 3.1) - 0.5), y + 1.5 * (vnoise(x * 0.6 + 5.7, y * 0.6 + 17.9) - 0.5));
    const t = 0.7 - 0.62 * cover; // the threshold falls as the wind rises: more of the noise becomes cloud
    return smooth(t, t + 0.3, n) * known * clamp(0.45 + 0.8 * cover, 0, 1);
  }

  const CloudLayer = L.GridLayer.extend({
    options: { tileSize: 256, pane: 'cloud', updateWhenZooming: false, keepBuffer: 1 },
    initialize(options) {
      L.GridLayer.prototype.initialize.call(this, options);
      this._pts = [];
    },
    /* points: [{lat, lon, wind_speed_kn}], place: lon -> the map's longitude (the Pacific-centred map shifts it) */
    setPoints(points, place) {
      this._pts = points.map((p) => ({
        lat: p.lat,
        lon: place(p.lon),
        f: clamp(((p.wind_speed_kn ?? 0) - WIND_CALM_KN) / WIND_RANGE_KN, 0, 1),
        cyclone: !!p.cyclone,
      }));
      this.redraw();
    },
    createTile(coords) {
      const RES = 128;
      const canvas = L.DomUtil.create('canvas', 'leaflet-tile');
      canvas.width = canvas.height = RES;
      const pts = this._pts;
      if (!pts.length || !this._map) return canvas;
      const step = this.options.tileSize / RES;
      const ctx = canvas.getContext('2d');
      const img = ctx.createImageData(RES, RES);
      // Mercator: the longitude depends on the column only and the latitude on the row only.
      const lons = [], lats = [];
      for (let i = 0; i < RES; i++) lons.push(this._map.unproject(L.point(coords.x * 256 + i * step, coords.y * 256), coords.z).lng);
      for (let j = 0; j < RES; j++) lats.push(this._map.unproject(L.point(coords.x * 256, coords.y * 256 + j * step), coords.z).lat);
      const degPerPx = 360 / (256 * 2 ** coords.z) * step;
      const lit = degPerPx * 1.6; // light comes from the upper left
      const cast = degPerPx * 5; // and the cloud throws its shadow to the lower right
      const near = pts.filter((p) => p.lat > lats[RES - 1] - 25 && p.lat < lats[0] + 25); // cheap cull
      if (!near.length) return canvas;
      const storms = pts.filter((p) => p.cyclone);
      for (let j = 0; j < RES; j++) {
        const lat = lats[j];
        for (let i = 0; i < RES; i++) {
          const lon = lons[i];
          const d = density(near, storms, lat, lon);
          const cover = lastCover;
          const k = (j * RES + i) * 4;
          if (d < 0.02) {
            const shade = density(near, storms, lat + cast, lon - cast); // cloud up-left of here shades this spot
            if (shade > 0.08) { img.data[k] = 8; img.data[k + 1] = 18; img.data[k + 2] = 34; img.data[k + 3] = 70 * shade; }
            continue;
          }
          const toward = density(near, storms, lat + lit, lon - lit);
          const light = clamp(0.98 + 3.6 * (d - toward), 0.5, 1.08); // lit on the side that faces the light
          // fair weather is white cloud, rain grey, a storm gathers black cloud
          const dark = smooth(0.2, 0.62, cover);
          const base = 255 * light * (1 - 0.82 * dark);
          img.data[k] = clamp(base - 4, 0, 255);
          img.data[k + 1] = clamp(base - 1, 0, 255);
          img.data[k + 2] = clamp(base + 4, 0, 255);
          img.data[k + 3] = clamp(d * 255 * 0.92, 0, 255);
        }
      }
      ctx.putImageData(img, 0, 0);
      return canvas;
    },
  });

  /* ---- one cyclone, drawn once as an image that CSS turns ---- */
  const cache = new Map();
  /* A square image of a cyclone: eye in the middle, eye wall, spiral bands out to the edge. dir: 1 turns
     counter-clockwise (northern hemisphere), -1 clockwise. */
  function cycloneImage(dir) {
    if (cache.has(dir)) return cache.get(dir);
    const S = 420, C = S / 2;
    const field = (x, y) => {
      const dx = (x - C) / C, dy = (y - C) / C;
      const r = Math.hypot(dx, dy);
      if (r > 1) return 0;
      const theta = Math.atan2(dy, dx) * dir;
      const arms = 0.5 + 0.5 * Math.cos(3 * theta - 5.2 * Math.log(r + 0.07)); // three log-spiral bands
      const bands = arms ** 2.2; // dark gaps between the bands
      const ragged = fbm(x * 0.035, y * 0.035);
      const body = Math.exp(-((r / 0.82) ** 2.2)); // fades out towards the edge
      let d = body * (0.08 + 0.92 * bands) * (0.5 + 1.0 * ragged);
      d += 0.9 * Math.exp(-(((r - 0.13) / 0.07) ** 2)); // the bright eye wall
      d *= smooth(0.035, 0.08, r); // the eye is clear
      return clamp(d, 0, 1);
    };
    const canvas = document.createElement('canvas');
    canvas.width = canvas.height = S;
    const ctx = canvas.getContext('2d');
    const img = ctx.createImageData(S, S);
    for (let y = 0; y < S; y++) {
      for (let x = 0; x < S; x++) {
        const d = field(x, y);
        const k = (y * S + x) * 4;
        if (d < 0.01) continue;
        const light = clamp(0.97 + 3.6 * (d - field(x + 2, y + 2)), 0.5, 1.08);
        const grey = d * d * 0.16;
        const v = 255 * light * (1 - grey);
        img.data[k] = clamp(v - 4, 0, 255);
        img.data[k + 1] = clamp(v - 1, 0, 255);
        img.data[k + 2] = clamp(v + 5, 0, 255);
        img.data[k + 3] = clamp(d * 255 * 1.05, 0, 255);
      }
    }
    ctx.putImageData(img, 0, 0);
    const url = canvas.toDataURL('image/png');
    cache.set(dir, url);
    return url;
  }

  /* Icon for a cyclone at `lat`: about `radiusKm` wide at the map's current zoom. Call again after a zoom. */
  function cycloneIcon(map, lat, radiusKm = 1000) {
    const metresPerPx = (40075016.686 * Math.cos((lat * Math.PI) / 180)) / (256 * 2 ** map.getZoom());
    const size = clamp(Math.round((2 * radiusKm * 1000) / metresPerPx), 40, 1600);
    return L.divIcon({
      className: '',
      html: `<span class="wx cyclone"><img class="cyclone-img" alt="" src="${cycloneImage(lat >= 0 ? 1 : -1)}" width="${size}" height="${size}"></span>`,
      iconSize: [size, size],
      iconAnchor: [size / 2, size / 2],
    });
  }

  return { CloudLayer, cycloneIcon };
})();
