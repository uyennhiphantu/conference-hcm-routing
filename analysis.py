import pandas as pd

from config import MATRIX_DELTA_CSV

# Color pairs used in visualization (forward, return)
COLOR_PAIRS = [
    ("Blue", "Red"),
    ("Green", "Orange"),
    ("Pink", "Purple"),
    ("Cyan", "Yellow"),
    ("Violet", "Emerald"),
    ("Rose", "Sky"),
    ("Amber", "Indigo"),
    ("Teal", "Red-Pink"),
    ("Indigo", "Lime"),
    ("Red", "Teal"),
    ("Purple", "Green"),
    ("Blue", "Orange"),
]

df = pd.read_csv(MATRIX_DELTA_CSV)

summary = {
    "rows": len(df),
    "null_2018_time": int(df["time_s_2018"].isna().sum()),
    "null_2025_time": int(df["time_s_2025"].isna().sum()),
    "avg_delta_time_s": df["delta_time_s"].dropna().mean(),
    "avg_delta_distance_km": df["delta_distance_km"].dropna().mean(),
    "median_delta_time_s": df["delta_time_s"].dropna().median(),
    "median_delta_distance_km": df["delta_distance_km"].dropna().median(),
}

top_faster = (
    df.dropna(subset=["delta_time_s"])
      .sort_values("delta_time_s")
      .head(20)[["src","dst","time_s_2018","time_s_2025","delta_time_s"]]
)

top_slower = (
    df.dropna(subset=["delta_time_s"])
      .sort_values("delta_time_s", ascending=False)
      .head(20)[["src","dst","time_s_2018","time_s_2025","delta_time_s"]]
)

# Top 10 for visualization with colors
top_10_viz = (
    df.dropna(subset=["delta_time_s"])
      .sort_values("delta_time_s")
      .head(10)[["src","dst","delta_time_s"]]
      .reset_index(drop=True)
)

print("="*60)
print("SUMMARY STATISTICS")
print("="*60)
for key, value in summary.items():
    print(f"{key}: {value}")
print()

print("="*60)
print("TOP 20 FASTER ROUTES IN 2025 (negative delta = faster)")
print("="*60)
print(top_faster.to_string(index=False))
print()

print("="*60)
print("TOP 20 SLOWER ROUTES IN 2025 (positive delta = slower)")
print("="*60)
print(top_slower.to_string(index=False))
print()

print("="*60)
print("VISUALIZATION COLOR MAPPING (Top 10 by delta_time_s)")
print("="*60)
print("Each OD pair has unique colors: Forward / Return")
print("-"*60)
for i, row in top_10_viz.iterrows():
    fwd_color, ret_color = COLOR_PAIRS[i % len(COLOR_PAIRS)]
    print(f"OD {int(row['src']):2d}↔{int(row['dst']):2d}  |  Δtime: {row['delta_time_s']:+8.0f}s  |  {fwd_color} / {ret_color}")
print("-"*60)
print("Line style: Solid = 2018, Dashed = 2025")
