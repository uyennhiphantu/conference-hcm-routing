#!/usr/bin/env python3
"""
Build completeness_metrics.csv: 1km grid over Districts 1/2/3 bounding box.
Computes road_length, road_count, building_count, connectivity for 2018 and 2025.
Uses osmium to extract data from PBF files.
"""

import subprocess
import json
import csv
import math
import os
from collections import defaultdict

# Bounding box from config.py (Districts 1/2/3)
MIN_LON = 106.665
MIN_LAT = 10.770
MAX_LON = 106.760
MAX_LAT = 10.820

# Grid cell size in degrees (~1km at this latitude)
# 1km ≈ 0.009 degrees latitude, ≈ 0.0093 degrees longitude at lat ~10.8
CELL_SIZE_LAT = 0.009
CELL_SIZE_LON = 0.0093

# POC data for has_new_infra spatial join
POC_FILE = os.path.join(os.path.dirname(__file__), "datasets", "points_of_change.csv")

# PBF files
PBF_2018 = os.path.expanduser("~/gis/custom_2018/hcm_2018.osm.pbf")
PBF_2025 = os.path.expanduser("~/gis/custom_2025/hcm_2025.osm.pbf")


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def build_grid():
    """Generate grid cells covering the bounding box."""
    cells = []
    row = 0
    lat = MIN_LAT
    while lat < MAX_LAT:
        col = 0
        lon = MIN_LON
        while lon < MAX_LON:
            grid_id = f"GRID-{row:02d}{col:02d}"
            center_lat = lat + CELL_SIZE_LAT / 2
            center_lon = lon + CELL_SIZE_LON / 2
            cells.append({
                "grid_id": grid_id,
                "grid_lat": round(center_lat, 6),
                "grid_lon": round(center_lon, 6),
                "min_lat": lat,
                "max_lat": lat + CELL_SIZE_LAT,
                "min_lon": lon,
                "max_lon": lon + CELL_SIZE_LON,
            })
            lon += CELL_SIZE_LON
            col += 1
        lat += CELL_SIZE_LAT
        row += 1
    return cells


def extract_roads_from_pbf(pbf_path, bbox):
    """Extract road ways from PBF using osmium export to GeoJSON."""
    min_lon, min_lat, max_lon, max_lat = bbox

    # First extract the bbox area
    tmp_pbf = "/tmp/extract_bbox.osm.pbf"
    tmp_geojson = "/tmp/roads.geojsonl"

    # Extract bbox
    cmd_extract = [
        "osmium", "extract",
        "-b", f"{min_lon},{min_lat},{max_lon},{max_lat}",
        pbf_path, "-o", tmp_pbf, "--overwrite"
    ]
    subprocess.run(cmd_extract, check=True, capture_output=True)

    # Export to GeoJSON lines (only ways with highway tag)
    cmd_export = [
        "osmium", "export", tmp_pbf,
        "-o", tmp_geojson, "--overwrite",
        "-f", "geojsonseq"
    ]
    subprocess.run(cmd_export, check=True, capture_output=True)

    # Parse the GeoJSON lines
    roads = []
    buildings = []

    with open(tmp_geojson, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                feature = json.loads(line)
            except json.JSONDecodeError:
                continue

            props = feature.get("properties", {})
            geom = feature.get("geometry", {})

            if "highway" in props and geom.get("type") == "LineString":
                highway = props["highway"]
                # Filter to routable road types
                if highway in ("motorway", "trunk", "primary", "secondary", "tertiary",
                              "residential", "unclassified", "service",
                              "motorway_link", "trunk_link", "primary_link",
                              "secondary_link", "tertiary_link"):
                    coords = geom.get("coordinates", [])
                    length_m = 0
                    for i in range(1, len(coords)):
                        length_m += haversine_km(
                            coords[i-1][1], coords[i-1][0],
                            coords[i][1], coords[i][0]
                        ) * 1000

                    # Use centroid for grid assignment
                    if coords:
                        mid_idx = len(coords) // 2
                        roads.append({
                            "lat": coords[mid_idx][1],
                            "lon": coords[mid_idx][0],
                            "length_m": length_m,
                            "nodes": len(coords),
                        })

            elif "building" in props and geom.get("type") in ("Polygon", "MultiPolygon"):
                # Get centroid approximation
                if geom["type"] == "Polygon":
                    ring = geom["coordinates"][0]
                elif geom["type"] == "MultiPolygon":
                    ring = geom["coordinates"][0][0]
                else:
                    continue

                avg_lat = sum(c[1] for c in ring) / len(ring)
                avg_lon = sum(c[0] for c in ring) / len(ring)
                buildings.append({"lat": avg_lat, "lon": avg_lon})

    # Cleanup
    for f in [tmp_pbf, tmp_geojson]:
        if os.path.exists(f):
            os.remove(f)

    return roads, buildings


def assign_to_grid(items, cells):
    """Assign items (with lat/lon) to grid cells."""
    cell_items = defaultdict(list)
    for item in items:
        for cell in cells:
            if (cell["min_lat"] <= item["lat"] < cell["max_lat"] and
                cell["min_lon"] <= item["lon"] < cell["max_lon"]):
                cell_items[cell["grid_id"]].append(item)
                break
    return cell_items


def compute_connectivity(roads_in_cell):
    """Simple connectivity: nodes / edges ratio."""
    if not roads_in_cell:
        return 0.0
    total_nodes = sum(r["nodes"] for r in roads_in_cell)
    total_edges = len(roads_in_cell)
    if total_edges == 0:
        return 0.0
    return round(total_nodes / total_edges, 4)


def load_pocs():
    """Load POCs and their impact radii."""
    pocs = []
    if not os.path.exists(POC_FILE):
        print(f"Warning: {POC_FILE} not found, has_new_infra will be FALSE for all cells")
        return pocs

    # Only include POCs that affect car routing (exclude metro/metro_station)
    road_categories = {"bridge", "road_new", "road_expansion", "interchange", "tunnel"}
    with open(POC_FILE, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("category", "") in road_categories and
                row.get("osm_in_2018") == "FALSE"):
                pocs.append({
                    "id": row["id"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "impact_radius_km": float(row.get("impact_radius_km", 1.0)),
                })
    return pocs


def check_has_new_infra(cell, pocs):
    """Check if any POC's impact radius overlaps with this grid cell."""
    cell_lat = cell["grid_lat"]
    cell_lon = cell["grid_lon"]
    related = []
    for poc in pocs:
        dist = haversine_km(cell_lat, cell_lon, poc["lat"], poc["lon"])
        if dist <= poc["impact_radius_km"]:
            related.append(poc["id"])
    return len(related) > 0, related


def main():
    print("Building grid...")
    cells = build_grid()
    print(f"  {len(cells)} grid cells created")

    pocs = load_pocs()
    print(f"  {len(pocs)} POCs loaded for spatial join")

    # Extract roads and buildings from both snapshots
    bbox = (MIN_LON, MIN_LAT, MAX_LON, MAX_LAT)

    print("Extracting 2018 data...")
    roads_2018, buildings_2018 = extract_roads_from_pbf(PBF_2018, bbox)
    print(f"  {len(roads_2018)} roads, {len(buildings_2018)} buildings")

    print("Extracting 2025 data...")
    roads_2025, buildings_2025 = extract_roads_from_pbf(PBF_2025, bbox)
    print(f"  {len(roads_2025)} roads, {len(buildings_2025)} buildings")

    # Assign to grid
    print("Assigning to grid cells...")
    roads_2018_grid = assign_to_grid(roads_2018, cells)
    roads_2025_grid = assign_to_grid(roads_2025, cells)
    buildings_2018_grid = assign_to_grid(buildings_2018, cells)
    buildings_2025_grid = assign_to_grid(buildings_2025, cells)

    # Compute metrics per cell
    print("Computing metrics...")
    output_rows = []
    for cell in cells:
        gid = cell["grid_id"]

        r18 = roads_2018_grid.get(gid, [])
        r25 = roads_2025_grid.get(gid, [])
        b18 = buildings_2018_grid.get(gid, [])
        b25 = buildings_2025_grid.get(gid, [])

        road_len_2018 = round(sum(r["length_m"] for r in r18), 2)
        road_len_2025 = round(sum(r["length_m"] for r in r25), 2)

        conn_2018 = compute_connectivity(r18)
        conn_2025 = compute_connectivity(r25)

        if road_len_2025 > 0:
            completeness_delta = round((road_len_2025 - road_len_2018) / road_len_2025, 4)
        else:
            completeness_delta = 0.0

        has_infra, related_pocs = check_has_new_infra(cell, pocs)

        output_rows.append({
            "grid_id": gid,
            "grid_lat": cell["grid_lat"],
            "grid_lon": cell["grid_lon"],
            "road_length_2018_m": road_len_2018,
            "road_length_2025_m": road_len_2025,
            "road_count_2018": len(r18),
            "road_count_2025": len(r25),
            "building_count_2018": len(b18),
            "building_count_2025": len(b25),
            "connectivity_2018": conn_2018,
            "connectivity_2025": conn_2025,
            "completeness_delta": completeness_delta,
            "has_new_infra": has_infra,
            "related_poc_ids": str(related_pocs) if related_pocs else "",
        })

    # Write CSV
    out_path = os.path.join(os.path.dirname(__file__), "datasets", "completeness_metrics.csv")
    with open(out_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_rows[0].keys())
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"\nWritten {len(output_rows)} rows to {out_path}")

    # Summary stats
    infra_cells = sum(1 for r in output_rows if r["has_new_infra"])
    non_empty = sum(1 for r in output_rows if r["road_count_2025"] > 0)
    print(f"  Cells with infrastructure: {infra_cells}/{len(output_rows)}")
    print(f"  Non-empty cells: {non_empty}/{len(output_rows)}")


if __name__ == "__main__":
    main()
