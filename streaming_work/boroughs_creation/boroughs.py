import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import clickhouse_connect

# -----------------------------
# Step 1: Connect to ClickHouse
# -----------------------------
client = clickhouse_connect.get_client(
    host='127.0.0.1',       # replace with your host
    port=8123,
    username='default',      # replace if needed
    password='123',          # replace if needed
    database='gtfs_batch'
)

# -----------------------------
# Step 2: Read stops table
# -----------------------------
stops_df = client.query_df("SELECT stop_id, stop_lat, stop_lon FROM stops")

# Create geometry points
stops_df['geometry'] = stops_df.apply(lambda row: Point(row['stop_lon'], row['stop_lat']), axis=1)
stops_gdf = gpd.GeoDataFrame(stops_df, geometry='geometry', crs="EPSG:4326")

# -----------------------------
# Step 3: Load borough shapefile
# -----------------------------
boroughs_gdf = gpd.read_file(
    "/home/rabie/data-engineering-platform/python_codes/boroughs_creation/nybb_22a/nybb.shp"
).to_crs("EPSG:4326")

# -----------------------------
# Step 4: Spatial join stops -> boroughs
# -----------------------------
stops_with_borough = gpd.sjoin(
    stops_gdf,
    boroughs_gdf[['BoroName', 'geometry']],
    how='left',
    predicate='within'      # use predicate instead of deprecated op
)

# Select relevant columns and rename
stops_lookup = stops_with_borough[['stop_id', 'BoroName']].rename(columns={'BoroName': 'borough'})

# Fill NaN with 'Unknown' for stops outside polygons
stops_lookup['borough'] = stops_lookup['borough'].fillna('Unknown')

# Ensure string types for ClickHouse
stops_lookup['stop_id'] = stops_lookup['stop_id'].astype(str)
stops_lookup['borough'] = stops_lookup['borough'].astype(str)

# -----------------------------
# Step 5: Insert into ClickHouse
# -----------------------------
client.insert_df('gtfs_batch.stop_boroughs', stops_lookup)

print("✅ stop_boroughs table populated successfully!")
