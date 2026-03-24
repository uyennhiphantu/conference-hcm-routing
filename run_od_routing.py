"""Route strategic OD pairs through both Valhalla instances (2018 & 2025)."""
import csv
import json
import requests

from config import VALHALLA_2018, VALHALLA_2025, COSTING, ROUTE_TIMEOUT

OD_CSV = "datasets/od_pairs_significant.csv"
OUT_CSV = "datasets/routing_results.csv"


def route_pair(base_url, olat, olon, dlat, dlon):
    """Route a single OD pair. Returns (status, time_s, distance_km, shape) or error."""
    payload = {
        "locations": [
            {"lat": olat, "lon": olon},
            {"lat": dlat, "lon": dlon},
        ],
        "costing": COSTING,
        "directions_options": {"units": "kilometers"},
    }
    try:
        r = requests.post(f"{base_url}/route", json=payload, timeout=ROUTE_TIMEOUT)
        if r.status_code == 400:
            body = r.json()
            code = body.get("error_code", body.get("status_code", ""))
            msg = body.get("error", body.get("status", ""))
            return "no_route", None, None, None, f"{code}: {msg}"
        if r.status_code == 404:
            return "no_route", None, None, None, "endpoint_404"
        r.raise_for_status()
        data = r.json()
        trip = data.get("trip", {})
        summary = trip.get("summary", {})
        legs = trip.get("legs", [])
        shape = legs[0].get("shape", "") if legs else ""
        return (
            "ok",
            round(summary.get("time", 0), 1),
            round(summary.get("length", 0), 3),
            shape,
            "",
        )
    except requests.exceptions.Timeout:
        return "timeout", None, None, None, "request_timeout"
    except Exception as e:
        return "error", None, None, None, str(e)[:200]


def main():
    with open(OD_CSV) as f:
        od_pairs = list(csv.DictReader(f))

    print(f"Loaded {len(od_pairs)} OD pairs from {OD_CSV}")

    results = []
    for od in od_pairs:
        oid = od["id"]
        olat = float(od["origin_lat"])
        olon = float(od["origin_lon"])
        dlat = float(od["dest_lat"])
        dlon = float(od["dest_lon"])

        # Route on 2018
        s18, t18, d18, sh18, err18 = route_pair(VALHALLA_2018, olat, olon, dlat, dlon)
        # Route on 2025
        s25, t25, d25, sh25, err25 = route_pair(VALHALLA_2025, olat, olon, dlat, dlon)

        # Compute deltas
        delta_time = None
        delta_dist = None
        pct_time = None
        pct_dist = None
        if t18 is not None and t25 is not None and t18 > 0:
            delta_time = round(t25 - t18, 1)
            delta_dist = round(d25 - d18, 3)
            pct_time = round((delta_time / t18) * 100, 2)
            pct_dist = round((delta_dist / d18) * 100, 2) if d18 > 0 else None

        row = {
            "od_id": oid,
            "origin_name": od["origin_name"],
            "dest_name": od["dest_name"],
            "expected_quadrant": od["expected_quadrant"],
            "status_2018": s18,
            "time_s_2018": t18,
            "distance_km_2018": d18,
            "status_2025": s25,
            "time_s_2025": t25,
            "distance_km_2025": d25,
            "delta_time_s": delta_time,
            "delta_distance_km": delta_dist,
            "pct_time": pct_time,
            "pct_distance": pct_dist,
            "error_2018": err18,
            "error_2025": err25,
            "shape_2018": sh18 or "",
            "shape_2025": sh25 or "",
        }
        results.append(row)

        status_icon = "ok" if s18 == "ok" and s25 == "ok" else "!!"
        print(f"  [{status_icon}] {oid}: 2018={s18}({t18}s) 2025={s25}({t25}s) delta={delta_time}s")

    # Write CSV
    fieldnames = list(results[0].keys())
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} results to {OUT_CSV}")

    # Summary
    ok_both = [r for r in results if r["status_2018"] == "ok" and r["status_2025"] == "ok"]
    no_2018 = [r for r in results if r["status_2018"] == "no_route" and r["status_2025"] == "ok"]
    no_2025 = [r for r in results if r["status_2018"] == "ok" and r["status_2025"] == "no_route"]
    both_fail = [r for r in results if r["status_2018"] != "ok" and r["status_2025"] != "ok"]

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Both OK:          {len(ok_both)}")
    print(f"No route 2018 only: {len(no_2018)} (new infrastructure!)")
    print(f"No route 2025 only: {len(no_2025)} (degraded?)")
    print(f"Both fail:        {len(both_fail)}")

    if ok_both:
        improved = [r for r in ok_both if r["delta_time_s"] and r["delta_time_s"] < 0]
        degraded = [r for r in ok_both if r["delta_time_s"] and r["delta_time_s"] > 0]
        print(f"\nOf {len(ok_both)} routable pairs:")
        print(f"  Improved (faster 2025): {len(improved)}")
        print(f"  Degraded (slower 2025): {len(degraded)}")

        if improved:
            best = min(improved, key=lambda r: r["delta_time_s"])
            print(f"  Best improvement: {best['od_id']} ({best['origin_name']} → {best['dest_name']}): {best['delta_time_s']}s ({best['pct_time']}%)")


if __name__ == "__main__":
    main()
