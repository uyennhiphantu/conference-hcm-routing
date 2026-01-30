# HCM Road Network Analysis

Routing comparison between 2018 and 2025 OSM data using Valhalla engine, focused on central Ho Chi Minh City.

## Overview

This project analyzes road network evolution in Districts 1, 2, and 3 of Ho Chi Minh City by comparing routing between 2018 and 2025 OpenStreetMap snapshots. The analysis highlights one-way street asymmetries and infrastructure changes.

## Key Results

| Metric | Value |
|--------|-------|
| Sample Points | 50 |
| OD Pairs | 2,500 |
| Null Routes (2018) | 0% |
| Null Routes (2025) | 0% |
| Avg Δ Time | -16.3s (2025 faster) |
| Avg Δ Distance | -0.07 km |

### Top Asymmetries Detected

Routes where forward (A→B) and return (B→A) differ significantly due to one-way streets:

| OD Pair | Forward Time | Asymmetry |
|---------|--------------|-----------|
| 16↔28 | 563s | +140s, +2.43km |
| 35↔28 | 653s | +104s, +1.67km |
| 5↔28 | 629s | +90s, +1.59km |

## Bounding Box

```
Districts 1, 2, 3 - Ho Chi Minh City
MIN_LON, MIN_LAT = 106.665, 10.762
MAX_LON, MAX_LAT = 106.760, 10.820
```

## Quick Start

### Prerequisites

- Docker
- Python 3 with: `pip install pandas requests folium`
- osmium-tool: `brew install osmium-tool`

### 1. Extract OSM Data

```bash
osmium extract -b 106.665,10.762,106.760,10.820 vietnam-180101.osm.pbf -o custom_2018/hcm_2018.osm.pbf
osmium extract -b 106.665,10.762,106.760,10.820 vietnam-251201.osm.pbf -o custom_2025/hcm_2025.osm.pbf
```

### 2. Start Valhalla Docker Instances

```bash
./run_2018.sh  # Terminal 1 - Port 8004
./run_2025.sh  # Terminal 2 - Port 8005
```

### 3. Generate Points & Run Analysis

```bash
python generate_points.py --n 50
cp points_data.csv points.csv
python run_matrix_and_delta.py
python analysis.py
```

### 4. Visualize

```bash
python draw_compare_routes.py --top-k 10 --metric delta_time_s
open route_compare_layers.html
```

## Visualization

The map uses color-coded bidirectional routes:

| Color | Line Style | Meaning |
|-------|------------|---------|
| Blue | Solid | Forward (A→B) 2018 |
| Blue | Dashed | Forward (A→B) 2025 |
| Red | Solid | Return (B→A) 2018 |
| Red | Dashed | Return (B→A) 2025 |

When blue and red routes diverge, it indicates one-way street effects.

## Project Structure

```
├── config.py                 # Bounding box, endpoints, settings
├── generate_points.py        # Generate sample points
├── run_matrix_and_delta.py   # Calculate OD matrices
├── draw_compare_routes.py    # Bidirectional visualization
├── analysis.py               # Statistics output
├── run_2018.sh / run_2025.sh # Docker scripts
├── points.csv                # 50 sample points
├── matrix_2018.csv           # OD matrix 2018
├── matrix_2025.csv           # OD matrix 2025
└── matrix_delta.csv          # Delta comparison
```

## Data Schema

**points.csv:** `id, lat, lon, snap_m`

**matrix_delta.csv:** `src, dst, time_s_2018, distance_km_2018, time_s_2025, distance_km_2025, delta_time_s, delta_distance_km, pct_time, pct_distance`

## Findings

1. **2025 is generally faster** - Average 16.3 seconds improvement per route
2. **One-way asymmetries** - Several routes show 1-2+ minute differences between forward and return
3. **Point 28** - Hub location with multiple high-delta connections
4. **Full coverage** - 0% null routes in both years for this focused area

## Technical Notes

- Routing uses `costing: auto` (car-only, excludes pedestrian/bike paths)
- Points are snapped to road network within 40m tolerance
- Asymmetry flagged when |Δtime| > 60s or |Δdist| > 0.3km between forward/return
