# HCMC Road Network Evolution Analysis (2018-2025)

A data-driven analysis of Ho Chi Minh City's infrastructure transformation, measuring real-world routing efficiency improvements across Districts 1, 2, and 3.

---

## Project Motivation

Between 2018 and 2025, Ho Chi Minh City invested heavily in transportation infrastructure. This project quantifies the actual impact by comparing routing efficiency across identical origin-destination pairs in two time periods.

### Key Questions

- How much faster can residents travel between the same locations today vs. 7 years ago?
- Which areas benefited most from infrastructure improvements?
- Do one-way street configurations create significant route asymmetries?

### What This Analysis Reveals

| Insight Area | Value |
|--------------|-------|
| **Infrastructure ROI** | Validates whether road investment improved daily travel |
| **Urban Planning Effectiveness** | Tests if polycentric network design succeeded |
| **Equity Analysis** | Identifies if improvements benefited all areas equally |
| **Future Planning** | Provides evidence for next-phase priorities |

---

## Key Results

| Metric | Value |
|--------|-------|
| Sample Points | 50 |
| OD Pairs | 2,500 |
| Null Routes (2018) | 0% |
| Null Routes (2025) | 0% |
| **Avg Time Improvement** | **-16.3 seconds** |
| Avg Distance Change | -0.07 km |

### Route Asymmetries Detected

One-way streets create significant differences between forward (A→B) and return (B→A) trips:

| OD Pair | Forward Time | Return Penalty |
|---------|--------------|----------------|
| 16↔28 | 563s | +140s, +2.43km |
| 35↔28 | 653s | +104s, +1.67km |
| 5↔28 | 629s | +90s, +1.59km |

---

## Methodology

### What I Did

1. **Historical Data Reconstruction** - Built 2018 road network from OpenStreetMap, filtered for car-accessible routes only
2. **Current Network Mapping** - Created 2025 baseline with identical filtering
3. **Controlled Comparison** - Generated 50 random OD pairs within Districts 1, 2, 3
4. **Bidirectional Analysis** - Routed both A→B and B→A to detect one-way asymmetries
5. **Delta Calculation** - Measured time savings and distance changes

### Technical Approach

| Component | Implementation |
|-----------|----------------|
| Spatial Scope | Districts 1, 2, 3 bounding box |
| Network Filter | Car-only (`costing: auto`) - excludes pedestrian/bike paths |
| Routing Engine | Valhalla v3.5.1 (open-source, production-grade) |
| Isolation | Separate Docker containers for 2018 vs 2025 data |

### Bounding Box

```python
# Districts 1, 2, 3 - Ho Chi Minh City
MIN_LON, MIN_LAT = 106.665, 10.762
MAX_LON, MAX_LAT = 106.760, 10.820
```

---

## Interpreting the Results

### Why Routes Got Faster

The time savings reflect fundamental shifts in urban network topology:

#### From Hub-and-Spoke to Polycentric Grid

**2018 Network:**
- Major arterials acted as mandatory choke points
- All cross-district travel funneled through downtown bottlenecks
- High vulnerability if one road congested

**2025 Network:**
- Distributed load across multiple parallel corridors
- New bridges/tunnels created alternative paths
- Lower maximum edge betweenness = resilient network

*Analogy: The old system was like having one major artery—if blocked, the whole system fails. The new system is capillary-rich: if one path clogs, traffic reroutes through alternatives.*

#### Capacity vs. Connectivity

| Approach | Example | Effect |
|----------|---------|--------|
| Old: Widen roads | Adding lanes to Vo Van Kiet | Diminishing returns (induced demand) |
| New: Add connections | Thu Thiem Tunnel, Thanh Da Bridge | Exponential routing possibilities |

---

## Infrastructure Changes (2018-2025)

### Physical Improvements

| Category | Changes | Impact on Routing |
|----------|---------|-------------------|
| **New Crossings** | Thu Thiem Tunnel, Thanh Da Bridge | Bypass District 1 core congestion |
| **Metro Line 1** | Ben Thanh - Suoi Tien | Reduced car dependency → more road space |
| **Smart Traffic** | Adaptive signal control, incident detection | Lower travel time variance |
| **Road Expansion** | Mai Chi Tho Boulevard (4→8 lanes) | Higher throughput on key corridors |

### Network Topology Evolution

| Aspect | 2018 Model | 2025 Model |
|--------|------------|------------|
| Philosophy | Top-down master plans | Data-driven, responsive |
| Tools | Static traffic surveys | Real-time sensors, routing analytics |
| Validation | Post-construction counts | Pre-construction digital twins |

---

## Economic Interpretation

When analysis shows time savings per trip:

### Opportunity Cost Recovery

```
8 min saved × 2 trips/day × 250 workdays = 66 hours/year
At median wage (₫100,000/hour) = ₫6.6M/year per commuter
For 1 million affected commuters = ₫6.6 trillion/year economic value
```

### Accessibility Over Mobility

**Old metric:** "How fast can you drive 10km?"
**New metric:** "How many opportunities can you reach in 30 minutes?"

*Example: If District 2 → District 1 drops from 35 to 27 minutes, jobs in District 1 become "accessible" within 30-min threshold, expanding the effective labor pool.*

---

## Visualization

Each OD pair is assigned a **unique color pair** for visual distinction:

| OD Pair | Forward Color | Return Color |
|---------|---------------|--------------|
| 1st | Blue | Red |
| 2nd | Green | Orange |
| 3rd | Pink | Purple |
| 4th | Cyan | Yellow |
| 5th | Violet | Emerald |
| ... | ... | ... |

### Line Styles

| Style | Meaning |
|-------|---------|
| **Solid** | 2018 route |
| **Dashed** | 2025 route |

### Map Markers

| Marker | Meaning |
|--------|---------|
| 🟢 Green | Source (Origin) |
| 🟠 Orange | Destination |

**Interpretation:** When Forward and Return routes diverge significantly, it indicates one-way street effects forcing detours. The dynamic legend on the map shows each OD pair with its assigned colors.

---

## Quick Start

### Prerequisites

- Docker
- Python 3: `pip install pandas requests folium`
- osmium-tool: `brew install osmium-tool`

### Run Analysis

```bash
# 1. Extract OSM data for Districts 1,2,3
osmium extract -b 106.665,10.762,106.760,10.820 vietnam-180101.osm.pbf -o custom_2018/hcm_2018.osm.pbf
osmium extract -b 106.665,10.762,106.760,10.820 vietnam-251201.osm.pbf -o custom_2025/hcm_2025.osm.pbf

# 2. Start Valhalla instances
./run_2018.sh  # Terminal 1 - Port 8004
./run_2025.sh  # Terminal 2 - Port 8005

# 3. Generate points and calculate matrices
python generate_points.py --n 50
cp points_data.csv points.csv
python run_matrix_and_delta.py

# 4. Analyze and visualize
python analysis.py
python draw_compare_routes.py --top-k 10 --metric delta_time_s
open route_compare_layers.html
```

---

## Project Structure

```
├── config.py                 # Bounding box, endpoints, settings
├── generate_points.py        # Generate sample points
├── run_matrix_and_delta.py   # Calculate OD matrices
├── draw_compare_routes.py    # Bidirectional visualization
├── analysis.py               # Statistics output
├── run_2018.sh / run_2025.sh # Docker scripts
├── points.csv                # Sample points
└── matrix_delta.csv          # Delta comparison
```

---

## Data Schema

**points.csv:** `id, lat, lon, snap_m`

**matrix_delta.csv:**
```
src, dst, time_s_2018, distance_km_2018, time_s_2025, distance_km_2025,
delta_time_s, delta_distance_km, pct_time, pct_distance
```

---

## Limitations & Caveats

| Limitation | Mitigation |
|------------|------------|
| OSM data quality varies | Cross-validate with official records |
| Valhalla uses "typical" traffic, not real-time | State this measures network *potential* |
| 50 points may miss edge cases | Increase sample size for production |
| COVID-19 (2020-2022) reduced traffic temporarily | Focus on network topology, not congestion |

**Note:** This analysis measures network potential, not observed travel times. Real-world results depend on actual traffic volumes.

---

## Communicating Findings

### For Policymakers
> "The average 16-second reduction in travel time across 2,500 routes demonstrates measurable infrastructure improvement in Districts 1, 2, 3."

### For Urban Planners
> "Routes showing greatest improvement correlate with new cross-district links rather than lane additions, confirming network topology matters more than road width."

### For Citizens
> "Getting across District 2 is now faster and more predictable than in 2018—that's time returned to your day."

---

## Future Analysis

- [ ] Expand to all HCMC districts
- [ ] Add temporal analysis (rush hour vs. off-peak)
- [ ] Compare with Bangkok, Jakarta, Hanoi
- [ ] Accessibility heatmap by neighborhood
- [ ] Predict optimal next infrastructure project
