import json
import re
import uuid
from datetime import UTC, datetime

import pytest
from playwright.sync_api import BrowserContext, Page, expect

from common import topics
from common.kafka import JsonProducer
from tests.e2e.conftest import Dashboard

pytestmark = pytest.mark.e2e

WAIT = 20_000
SHIPS = ".leaflet-marker-pane .ship-icon"  # the legend has mini icons of its own


def point_at(page: Page, lat: float, lon: float) -> None:
    """Move the mouse over a place of the map."""
    page.wait_for_timeout(1200)  # the map may still be settling on its first fit
    x, y = page.evaluate(
        "([la, lo]) => { const p = map.latLngToContainerPoint([la, lo]); return [p.x, p.y]; }", [lat, lon]
    )
    box = page.locator("#map").bounding_box()
    page.mouse.move(box["x"] + x, box["y"] + y)


def choose_base(page: Page, name: str) -> None:
    """Pick a map view in the layers control, which opens when the pointer arrives."""
    for _ in range(8):  # on a busy machine the control can take a moment to open
        if page.locator(".leaflet-control-layers-expanded").count():
            break  # still open from the last choice (the pointer is over it)
        box = page.locator(".leaflet-control-layers-toggle").bounding_box()
        if box:
            page.mouse.move(1, 1)
            page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        page.wait_for_timeout(400)
    page.locator(".leaflet-control-layers-base label").filter(has_text=name).click()


def search(page: Page, text: str) -> None:
    page.fill("#q", text)


def only_ship(page: Page, name: str) -> None:
    """Filter the map down to one ship and wait until exactly its icon is drawn."""
    search(page, name)
    expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)


def send_alert(dash: Dashboard, **fields: object) -> str:
    alert = {
        "alert_id": f"e2e-{uuid.uuid4()}",
        "type": "dark_ship",
        "mmsi": dash.seed.anchored,
        "port_id": "USLGB",
        "event_time": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "lat": 33.74,
        "lon": -118.165,
        "details": {"silent_hours": 7},
    } | fields
    producer = JsonProducer()
    producer.send(topics.ALERTS, str(alert["type"]), alert)
    assert producer.flush() == 0
    return str(alert["alert_id"])


def test_live_map_shows_seeded_ships_and_health(page: Page, dashboard: Dashboard):
    expect(page.locator("#health")).to_contain_text("hbase ok", timeout=WAIT)
    expect(page.locator("#health")).to_contain_text("warehouse ok")
    assert page.locator("#map .leaflet-tile-pane").count() == 1  # a real Leaflet map, not circles on a canvas
    for name in (dashboard.seed.anchored_name, dashboard.seed.berthed_name):
        search(page, name)
        expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)
    search(page, dashboard.seed.anchored_name)
    expect(page.locator(f"{SHIPS}.anchored")).to_have_count(
        1
    )  # coloured by state (class), text in the detail
    search(page, "E2E")
    expect(page.locator("#shown")).to_contain_text("Showing 2 of")


def test_filters_by_state_port_and_reset(page: Page, dashboard: Dashboard):
    search(page, "E2E")
    expect(page.locator(SHIPS)).to_have_count(2, timeout=WAIT)
    page.get_by_role("button", name="anchored", exact=True).click()  # chip off
    expect(page.locator(SHIPS)).to_have_count(1)
    expect(page.locator(f"{SHIPS}.berthed")).to_have_count(1)
    page.locator("button.port", has_text="USLAX").click()  # the port card: only this port's ships
    expect(page.locator(SHIPS)).to_have_count(0)
    page.click("#f-reset")
    expect(page.locator("#q")).to_have_value("")
    expect(page.locator("button.port[aria-pressed=true]")).to_have_count(0)


def test_one_ports_control_and_source_countries_with_suggestions(page: Page, dashboard: Dashboard):
    expect(page.locator("#h-ports")).to_contain_text("United States (US)", timeout=WAIT)  # the fixed country
    expect(page.locator("#m-ports")).to_have_count(0)  # no second ports control
    expect(page.get_by_label("Monitor Long Beach")).to_be_checked()
    page.locator("#m-partners summary").click()
    expect(page.locator("#m-partners-list")).to_contain_text("Use suggested")
    expect(page.locator("#m-partners-list")).to_contain_text("% followed")


def test_coverage_layer_shows_waters_listening_area_and_blind_stretches(page: Page, dashboard: Dashboard):
    expect(page.locator("#f-coverage")).to_have_count(0)  # not a filter: it is a map view
    choose_base(page, "Coverage view")
    legend = page.locator(".cov-legend")
    expect(legend).to_contain_text("Listening area", timeout=WAIT)
    expect(legend).to_contain_text("nobody hears")
    choose_base(page, "Satellite")
    expect(legend).to_have_count(0)


def test_ships_drop_down_separates_monitored_ships_from_those_crossing_the_waters(
    page: Page, dashboard: Dashboard
):
    only_ship(page, dashboard.seed.berthed_name)  # in the waters, voyage not known
    group = page.locator("#f-group")
    expect(group.locator("option")).to_have_count(4)
    group.select_option("monitored")  # source or destination ours
    expect(page.locator(SHIPS)).to_have_count(0)
    group.select_option("crossing")  # its destination is not typed: not clearly a voyage of its own
    expect(page.locator(SHIPS)).to_have_count(0)
    group.select_option("other")  # in the waters, no voyage of ours: the rest, so the groups add up
    expect(page.locator(SHIPS)).to_have_count(1)
    group.select_option("all")
    expect(page.locator(SHIPS)).to_have_count(1)
    page.click("#f-reset")
    expect(group).to_have_value("all")


def test_ship_detail_track_states_and_route(page: Page, dashboard: Dashboard):
    only_ship(page, dashboard.seed.berthed_name)
    page.locator(SHIPS).click()
    detail = page.locator("#ship-detail")
    expect(detail).to_contain_text(dashboard.seed.berthed_name, timeout=WAIT)
    expect(detail).to_contain_text(dashboard.seed.berthed)
    expect(detail).to_contain_text("Long Beach (USLGB)")  # inferred destination: the port area it is in
    expect(detail.locator(".hist li")).to_have_count(2)  # last states, newest first: berthed, then underway
    expect(detail.locator(".hist li").first).to_contain_text("berthed")
    expect(detail.locator(".hist li").last).to_contain_text("underway")
    expect(detail).not_to_contain_text("No track yet")
    page.keyboard.press("Escape")
    expect(detail).to_be_hidden()


def test_pins_survive_a_reload_and_filter(page: Page, dashboard: Dashboard):
    only_ship(page, dashboard.seed.anchored_name)
    page.locator(SHIPS).click()
    page.get_by_role("button", name="☆ Pin").click()
    expect(page.locator("#pins")).to_contain_text(dashboard.seed.anchored_name)
    page.reload()
    expect(page.locator("#pins")).to_contain_text(dashboard.seed.anchored_name, timeout=WAIT)
    search(page, "E2E")
    page.check("#f-pinned")
    expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)
    assert "pinned" in (page.locator(SHIPS).get_attribute("class") or "")


def test_port_panel_shows_waiting_and_latest_window(page: Page, dashboard: Dashboard):
    card = page.locator("#ports .port", has_text="Long Beach")
    expect(card).to_contain_text(str(dashboard.seed.queue), timeout=WAIT)  # latest 30-minute window
    card.click()
    expect(card).to_have_attribute("aria-pressed", "true")


def clear_backlog(page: Page) -> None:
    """The dev alerts topic holds real alerts too; acknowledge them so the test alert is the one unread."""
    expect(page.locator("#stream")).to_have_text("Live", timeout=WAIT)
    page.click("#ack-all")


def test_pushed_alert_raises_toast_and_can_be_acknowledged(page: Page, dashboard: Dashboard):
    clear_backlog(page)
    send_alert(dashboard)
    toast = page.locator("#toasts .toast", has_text=dashboard.seed.anchored_name)
    expect(toast).to_be_visible(timeout=WAIT)
    expect(toast).to_contain_text("High priority")
    item = page.locator("#alerts .alert", has_text=dashboard.seed.anchored_name).first
    item.get_by_role("button", name="Acknowledge").click()
    expect(item.get_by_role("button", name="Acknowledge")).to_have_count(0)
    page.reload()  # acknowledgement is remembered by the browser
    expect(page.locator("#alerts .alert.acked", has_text=dashboard.seed.anchored_name).first).to_be_visible(
        timeout=WAIT
    )


def test_alert_priority_follows_settings_and_pins(page: Page, dashboard: Dashboard):
    clear_backlog(page)
    send_alert(
        dashboard, type="queue_growth", mmsi=None, details={"ships": 12, "previous": 2, "growth_pct": 500}
    )
    item = page.locator("#alerts .alert", has_text="waiting ships 2 → 12").first
    expect(item).to_have_class(re.compile(r"\bmedium\b"), timeout=WAIT)  # default priority of queue_growth
    page.get_by_text("Priority and notifications").click()
    page.select_option("#prio-queue_growth", "high")
    expect(page.locator("#alerts .alert", has_text="waiting ships 2 → 12").first).to_have_class(
        re.compile(r"\bhigh\b")
    )


def test_reports_tab_shows_warehouse_figures(page: Page):
    page.click("#tab-reports")
    expect(page.locator("#compare")).to_contain_text("Long Beach", timeout=WAIT)
    expect(page.locator("#compare")).to_contain_text("6.0")  # this month: waits 5 h and 7 h
    expect(page.locator("#compare")).to_contain_text("10.0")  # last month
    expect(page.locator("#compare")).to_contain_text("-40%")
    expect(page.locator("#trends")).to_contain_text("Congestion rising")
    expect(page.locator("#trends figure")).to_have_count(2)  # queue bars, wait line
    expect(page.locator("#trends .chart-legend").first).to_contain_text("Long Beach")
    page.locator("#trends [data-tip]").first.hover()
    expect(page.locator(".chart-tip")).to_contain_text("Long Beach")
    expect(page.locator("#daily tbody tr")).to_have_count(3)
    page.select_option("#r-days", "7")
    expect(page.locator("#daily tbody tr")).to_have_count(2)  # newest day minus 7 days: Dec 20 drops out
    page.click("#tab-live")
    expect(page.locator("#map")).to_be_visible()


def test_error_state_when_live_api_fails(context: BrowserContext, dashboard: Dashboard):
    page = context.new_page()
    page.route(
        re.compile(r"/api/ships(\?|$)"),
        lambda r: r.fulfill(
            status=503,
            body=json.dumps({"error": {"code": "HBASE_UNAVAILABLE", "message": "down"}}),
            content_type="application/json",
        ),
    )
    page.goto(dashboard.url)
    expect(page.locator("#banner")).to_contain_text("Live data unavailable", timeout=WAIT)
    expect(page.locator("#map-status")).to_contain_text("Live data unavailable")


def test_phone_width_has_no_sideways_scroll(context: BrowserContext, dashboard: Dashboard):
    page = context.new_page()
    page.set_viewport_size({"width": 400, "height": 800})
    page.goto(dashboard.url)
    expect(page.locator("#ports .port").first).to_be_visible(timeout=WAIT)
    assert not page.evaluate("document.documentElement.scrollWidth > window.innerWidth")
    page.click("#tab-reports")
    expect(page.locator("#compare table")).to_be_visible(timeout=WAIT)
    assert not page.evaluate("document.documentElement.scrollWidth > window.innerWidth")


def stub(context: BrowserContext, path: str, body: object) -> None:
    context.route(
        re.compile(re.escape(path) + r"(\?|$)"),
        lambda r: r.fulfill(status=200, body=json.dumps(body), content_type="application/json"),
    )


HOUR = 3_600_000
NOW = 1_893_456_000_000  # 2030-01-01T00:00Z


def stub_ship(mmsi: str, name: str, **extra: object) -> dict:
    seen = datetime.fromtimestamp(NOW / 1000, UTC).isoformat().replace("+00:00", "Z")
    return {"mmsi": mmsi, "name": name, "ship_type": "cargo", "state": "underway", "port": None,
            "since": None,
            "last_seen": seen, "first_seen": seen, "lat": 33.70, "lon": -118.20, "sog": 0.0, "cog": None,
            "heading": None, "reasons": [], "map": "live"} | extra  # fmt: skip


def test_weather_congestion_and_wait_reasons(context: BrowserContext, dashboard: Dashboard):
    """Stubbed live API, so the weather and the queue are known whatever the dev stack holds."""
    waiting = stub_ship(
        "367500001", "STUB WAITING", state="anchored", port="USLGB", lat=33.74, lon=-118.165, map="problem",
        reasons=[{"code": "weather", "level": "storm", "text": "Storm weather at Long Beach while waiting"},
                 {"code": "congestion", "text": "Berth queue: 12 ships waiting at Long Beach"}],
    )  # fmt: skip
    storm = {"hour_ms": NOW, "wind_speed_kn": 41.0, "wave_height_m": 4.5, "condition": "storm"}
    ports = [
        {"port_id": "USLGB", "name": "Long Beach", "lat": 33.754, "lon": -118.216, "waiting_now": 12,
         "window_count": 12, "window_end": None, "congested": True, "weather": storm},
        {"port_id": "USLAX", "name": "Los Angeles", "lat": 33.729, "lon": -118.262, "waiting_now": 0,
         "window_count": None, "window_end": None, "congested": False, "weather": None},
    ]  # fmt: skip
    stub(context, "/api/ships", [waiting, stub_ship("367500002", "STUB SAILING")])
    stub(context, "/api/ports", ports)
    stub(context, "/api/ships/367500001/route", {
        "destination": {"port_id": "USLGB", "name": "Long Beach", "lat": 33.754, "lon": -118.216,
                        "basis": "declared"},
        "path": [[33.60, -118.10], [33.754, -118.216]], "distance_nm": 8.0, "eta_hours": None,
        "declared": {"text": "US LGB", "eta": "2030-01-01T06:00:00.000Z", "port_id": "USLGB",
                     "orders": False},
    })  # fmt: skip
    stub(context, "/api/ships/367500001/track", [])
    stub(context, "/api/ships/367500001/states", [])
    page = context.new_page()
    page.goto(dashboard.url)
    expect(page.locator(".leaflet-cloud-pane canvas").first).to_be_attached(
        timeout=WAIT
    )  # the weather picture
    expect(page.locator(".leaflet-marker-pane .wx")).to_have_count(0)  # and no weather icons
    for _ in range(10):  # the map may still settle on a busy machine: point again until the picture answers
        point_at(page, 33.68, -118.22)  # pointing at the storm round Long Beach says what it is
        if page.locator(".wx-tip").count():
            break
        page.wait_for_timeout(700)
    expect(page.locator(".wx-tip")).to_contain_text("Storm at Long Beach: wind 41 kn", timeout=WAIT)
    expect(page.locator(".queue-bubble")).to_have_text("12")  # congested port
    card = page.locator("#ports .port", has_text="Long Beach")
    expect(card).to_contain_text("Storm")
    expect(card.locator(".why-chip")).to_have_text(
        ["⛈️ 1 weather", "🚢 1 berth queue"]
    )  # why ships wait there
    badges = page.locator(".leaflet-marker-pane .ship-icon .whys .why")  # every reason beside the ship
    expect(badges).to_have_count(2)
    expect(page.locator(".leaflet-marker-pane .ship-icon .why.why-weather")).to_be_visible()
    expect(page.locator(".leaflet-marker-pane .ship-icon .why.why-congestion")).to_be_visible()
    search(page, "STUB WAITING")
    expect(page.locator(SHIPS)).to_have_count(1)
    page.locator(SHIPS).click()
    detail = page.locator("#ship-detail")
    expect(detail.locator(".reason")).to_have_count(2, timeout=WAIT)
    expect(detail).to_contain_text("Storm weather at Long Beach while waiting")
    expect(detail).to_contain_text("Long Beach (USLGB) · 8 nm")  # the crew named the port: no "inferred"
    expect(detail).not_to_contain_text("inferred")
    expect(detail).to_contain_text("Source" + "not known")  # no report stored for it: not known, not a guess
    # the monitored country's time (Los Angeles), not UTC
    expect(detail).to_contain_text("2029-12-31 22:00 GMT-8 (in 6h 00m)")


def test_hide_search_history_and_size_cap(context: BrowserContext, dashboard: Dashboard):
    sailing = stub_ship("367600001", "STUB SAILING")
    old = stub_ship("367600009", "STUB HISTORIC", state="berthed", port="USLGB", map="shown")
    asked: list[str] = []

    def ships(route):  # the history ship comes back only when the page asks for it with show=
        asked.append(route.request.url)
        body = [sailing] + ([old] if "show=367600009" in route.request.url else [])
        route.fulfill(status=200, body=json.dumps(body), content_type="application/json")

    context.route(re.compile(r"/api/ships(\?|$)"), ships)
    hits = [
        {"mmsi": "367600009", "name": "STUB HISTORIC", "ship_type": "cargo", "state": "berthed",
         "port": "USLGB",
         "last_seen": "2029-12-20T00:00:00.000Z", "on_map": False},
    ]  # fmt: skip
    stub(context, "/api/ships/search", hits)
    for path, body in [("track", []), ("states", []), ("route", {"destination": None, "path": []})]:
        stub(context, f"/api/ships/367600001/{path}", body)
        stub(context, f"/api/ships/367600009/{path}", body)
    page = context.new_page()
    page.goto(dashboard.url)
    search(page, "STUB")
    expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)

    page.locator(SHIPS).click()  # hide it: gone from the map, and remembered
    page.get_by_role("button", name="Hide", exact=True).click()
    expect(page.locator(SHIPS)).to_have_count(0)
    expect(page.locator("#unhide")).to_have_text("Show 1 hidden")
    page.reload()
    search(page, "STUB")
    expect(page.locator("#unhide")).to_have_text("Show 1 hidden", timeout=WAIT)
    expect(page.locator(SHIPS)).to_have_count(0)
    page.click("#unhide")
    expect(page.locator(SHIPS)).to_have_count(1)

    results = page.locator("#search-results")  # history is searchable and can be opened on the map
    expect(results).to_contain_text("history", timeout=WAIT)
    results.get_by_role("button").first.click()
    expect(page.locator("#ship-detail")).to_contain_text("STUB HISTORIC", timeout=WAIT)
    expect(page.locator("#ship-detail")).to_contain_text("from history")
    assert any("show=367600009" in u for u in asked)

    with page.expect_request(re.compile(r"/api/ships\?limit=250")):  # the server drops the first arrivals
        page.select_option("#f-limit", "250")


def test_sea_weather_clouds_cyclone_and_the_dateline(context: BrowserContext, dashboard: Dashboard):
    """Open-ocean weather drawn over the sea route, and ships either side of the 180th meridian in order."""
    ocean = {
        "hour_ms": NOW,
        "wind_speed_kn": 5.0,
        "wave_height_m": 0.5,
        "condition": "calm",
        "cyclone": False,
    }

    def point(point_id: str, lat: float, lon: float, **extra: object) -> dict:
        return ocean | {"point_id": point_id, "lat": lat, "lon": lon} | extra

    points = [
        point("SEA-01", 30.0, 140.0, condition="rough", wind_speed_kn=25.0),
        point("SEA-02", 40.0, 160.0, condition="storm", wind_speed_kn=40.0),
        point("SEA-03", 44.0, 178.0, condition="storm", wind_speed_kn=80.0, wave_height_m=9.0, cyclone=True),
        point("SEA-04", 37.0, -128.0),
    ]
    west = stub_ship("367700001", "STUB WEST OF 180", lat=40.0, lon=165.0)
    east = stub_ship("367700002", "STUB EAST OF 180", lat=44.0, lon=-165.0)
    stub(context, "/api/ships", [west, east])
    stub(context, "/api/weather/grid", points)
    stub(context, "/api/sea-lanes", {"lanes": [[[40.0, 160.0], [45.0, -165.0]]], "ports": []})
    page = context.new_page()
    page.goto(dashboard.url)
    expect(page.locator(SHIPS)).to_have_count(2, timeout=WAIT)
    expect(page.locator(".leaflet-marker-pane .cyclone-img")).to_have_count(1, timeout=WAIT)
    expect(page.locator(".leaflet-marker-pane .wx.sea")).to_have_count(0)  # no storm icons: cloud only
    expect(
        page.locator(".leaflet-cloud-pane canvas").first
    ).to_be_attached()  # the satellite-style cloud tiles
    assert page.evaluate(
        """() => [...document.querySelectorAll('.leaflet-cloud-pane canvas')].some((c) => {
            const px = c.getContext('2d').getImageData(0, 0, c.width, c.height).data;
            return px.some((v, i) => i % 4 === 3 && v > 40);
        })"""
    )  # and some of them have cloud in them
    expect(page.locator(".leaflet-marker-pane .cyclone-img")).to_have_count(
        1
    )  # one spiral for the one cyclone
    expect(page.locator(".legend")).not_to_contain_text("Cyclone")  # the picture is the legend
    page.locator(".wx.cyclone").hover(force=True)  # it spins, so it is never "stable"
    expect(page.locator(".leaflet-tooltip")).to_contain_text("Cyclone: wind 80 kn, waves 9.0 m")
    page.evaluate("map.setView([36, 160], 3)")  # the whole route in view, so every point can be pointed at
    point_at(page, 40.0, 160.0)
    expect(page.locator(".wx-tip")).to_contain_text("Storm: wind 40 kn")
    point_at(page, 30.0, 140.0)
    expect(page.locator(".wx-tip")).to_contain_text("Rain and rough sea: wind 25 kn")
    point_at(page, 37.0, -128.0)
    expect(page.locator(".wx-tip")).to_contain_text("Fair weather")
    # 165 E is left of 165 W on the Pacific-centred map.
    xs = page.evaluate(
        """() => ['367700001', '367700002'].map(
            (m) => map.latLngToContainerPoint(app.markers.get(m).marker.getLatLng()).x)"""
    )
    assert xs[0] < xs[1], xs


def test_offline_mode_stops_the_live_calls_and_live_resumes_them(
    context: BrowserContext, dashboard: Dashboard
):
    mover = stub_ship("367800001", "STUB MOVER", lat=20.0, lon=150.0, sog=30.0, cog=0.0, heading=0)
    stub(context, "/api/ships", [mover])
    page = context.new_page()
    live_calls: list[str] = []
    page.on(
        "request",
        lambda r: (
            live_calls.append(r.url) if re.search(r"/api/(ships\?|ports$|alerts/stream)", r.url) else None
        ),
    )
    page.goto(dashboard.url)
    expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)
    expect(page.locator("#mode-live")).to_have_attribute("aria-pressed", "true")
    expect(page.locator("#replay-bar")).to_be_hidden()

    page.locator("#mode-offline").click()
    expect(page.locator("#replay-bar")).to_be_visible()
    expect(page.locator("#stream")).to_have_text("Offline replay: live alerts are off")
    expect(page.locator("#summary")).to_contain_text("0 high alerts")  # live alerts are not shown on a replay
    expect(page.locator("#rp-day option")).to_have_text(
        re.compile(r"2023-01-01 · \d+ ships · NOAA \(US waters only\)")
    )
    expect(page.locator("#rp-clock")).to_contain_text("2023-01-01 00:00Z", timeout=WAIT)
    assert not page.evaluate("() => app.ships.has('367800001')")  # the live ship is gone from the map
    calls = len(live_calls)
    page.wait_for_timeout(4500)  # longer than a live poll
    assert len(live_calls) == calls  # no call to the live API while offline

    page.reload()  # the mode survives a reload
    expect(page.locator("#mode-offline")).to_have_attribute("aria-pressed", "true", timeout=WAIT)
    page.locator("#mode-live").click()
    expect(page.locator(SHIPS)).to_have_count(1, timeout=WAIT)
    assert len(live_calls) > calls  # polling is back


def test_offline_replay_shows_a_day_plays_it_and_replays_a_journey(
    context: BrowserContext, dashboard: Dashboard
):
    page = context.new_page()
    page.goto(dashboard.url)
    page.locator("#mode-offline").click()
    page.select_option("#rp-day", "2023-01-01")
    page.fill("#rp-time", "09:30")
    seed = dashboard.seed
    # at 09:30 the ship waits at anchor and the other one is at its berth
    expect(page.locator(SHIPS)).to_have_count(2, timeout=WAIT)
    expect(page.locator("#data-time")).to_contain_text("2023-01-01 09:30Z · offline replay paused")
    only_ship(page, seed.history_name)
    page.locator(SHIPS).click()
    detail = page.locator("#ship-detail")
    expect(detail).to_contain_text(seed.history_name, timeout=WAIT)
    expect(detail).to_contain_text("Anchored at Long Beach: waiting")
    expect(detail).to_contain_text("anchored")  # state pill

    # the path it took (solid) and the way on to the destination are drawn for the selected ship
    page.wait_for_function("() => routeLayer.getLayers().length > 0", timeout=WAIT)

    # the same replay, narrowed to this ship: from its first report to the arrival, one clock, one bar
    detail.get_by_role("button", name="Replay this ship").click()
    expect(page.locator("#rp-focus-name")).to_have_text(seed.history_name, timeout=WAIT)
    expect(page.locator(".rp-when").first).to_be_hidden()  # no day picker: this is the ship's own span
    expect(page.locator("#rp-clock")).to_contain_text("journey 2023-01-01 06:00Z to 2023-01-01 11:00Z")
    expect(page.locator(SHIPS)).to_have_count(1)  # only this ship is drawn
    expect(detail).to_contain_text("Arrived: berthed at Long Beach", timeout=WAIT)
    expect(detail.locator(".hist li.future").first).to_be_visible()  # what has not happened yet is dimmed
    detail.locator(
        ".hist li", has_text="Arrived: berthed at Long Beach"
    ).click()  # click an event: seek to it
    expect(page.locator("#data-time")).to_contain_text("2023-01-01 10:30Z")
    expect(page.locator("#data-time")).to_contain_text("offline replay paused")

    # play it: the clock runs through the journey and stops at its end
    page.select_option("#rp-speed", "3600")
    page.locator("#rp-play").click()
    expect(page.locator("#data-time")).to_contain_text("2023-01-01 11:00Z", timeout=15_000)
    expect(page.locator("#data-time")).to_contain_text("offline replay paused", timeout=5000)

    # back to the day with all ships; play it at speed
    page.get_by_role("button", name="All ships").click()
    expect(page.locator(".rp-when").first).to_be_visible()
    expect(page.locator(SHIPS)).not_to_have_count(0, timeout=WAIT)
    page.locator("#rp-play").click()
    expect(page.locator("#data-time")).to_contain_text("offline replay ×3600")
    page.locator("#rp-play").click()  # pause
    expect(page.locator("#data-time")).to_contain_text("offline replay paused")
