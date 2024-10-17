# %%
import pandas as pd
import numpy as np
import json
import geopandas as gpd
import osmnx as ox
from shapely.geometry import Point, LineString
from shapely.ops import unary_union
import matplotlib.pyplot as plt
import contextily as ctx

# %%



geo_points = gpd.read_file("proj4_points.geojson").to_crs(epsg=2180)


geo_points['buffer'] = geo_points.geometry.buffer(100)

spatial_index = geo_points.sindex


with open("proj4_params.json") as file:
    id_column = json.load(file)["id_column"]


counts = []


for idx, point in geo_points.iterrows():
    potential_matches_idx = list(spatial_index.intersection(point['buffer'].bounds))
    potential_matches = geo_points.iloc[potential_matches_idx]
    actual_matches = potential_matches[potential_matches.intersects(point['buffer'])]
    counts.append((point[id_column], len(actual_matches)))


counts_df = gpd.GeoDataFrame(counts, columns=[id_column, 'count'])
print(counts_df)
counts_df.to_csv("proj4_ex01_counts.csv", index=False)



# %%
geo_data = gpd.read_file("proj4_points.geojson")

geo_data_wgs84 = geo_data.to_crs(epsg=4326)

geo_data_wgs84['lon'] = geo_data_wgs84.geometry.x
geo_data_wgs84['lat'] = geo_data_wgs84.geometry.y
selected_data = geo_data_wgs84[[id_column, 'lat','lon']]
selected_data.to_csv("proj4_ex01_coords.csv", index=False)
print(selected_data)

# %%
with open("proj4_params.json", 'r') as file:
    config = json.load(file)
    city = config["city"]
    city = "Kraków" if city == "Cracow" else ("Warszawa" if city == "Warsaw" else city)


G = ox.graph_from_place(f"{city}, Poland", network_type="drive", custom_filter='["highway"="tertiary"]')
roads_geo_df = ox.graph_to_gdfs(G, nodes=False, edges=True)
roads_geo_df = roads_geo_df.rename(columns={"osmid": "osm_id"})
roads_geo_df["osm_id"] = roads_geo_df["osm_id"].apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else str(x))
roads_geo_df["name"] = roads_geo_df["name"].fillna('').astype(str)
selected_columns = roads_geo_df[["osm_id", "name", "geometry"]]
selected_columns.to_file("proj4_ex02_roads.geojson", driver="GeoJSON", index=False)



# %%




points = gpd.read_file("proj4_points.geojson").to_crs(epsg=2180)


roads = selected_columns.copy()


roads['point_count'] = 0


points_index = points.sindex


for road in roads.itertuples(index=True):
    buffer_box = road.geometry.buffer(50).bounds

    potential_points = list(points_index.intersection(buffer_box))

    if potential_points:
        nearby_points = points.iloc[potential_points]
        roads.at[road.Index, 'point_count'] = nearby_points.distance(road.geometry).le(50).sum()


aggregated_counts = roads.groupby('name').agg(point_count=('point_count', 'sum')).reset_index()

aggregated_counts.to_csv("proj4_ex03_streets_points.csv", index=False)

print(aggregated_counts)


# %%



gdf = gpd.read_file('proj4_countries.geojson')


gdf = gdf.to_crs(epsg=3857)  


gdf.to_pickle('proj4_ex04_gdf.pkl')


for idx, country in gdf.iterrows():

    single_country_gdf = gpd.GeoDataFrame([country], columns=gdf.columns)


    fig, ax = plt.subplots(figsize=(8, 8))
    
    single_country_gdf.boundary.plot(ax=ax, color='black', linewidth=1)
    
   
    ax.set_aspect('equal')
    ax.set_xticks([])
    ax.set_yticks([])
    
    
    if single_country_gdf.crs:
        ctx.add_basemap(ax, crs=single_country_gdf.crs.to_string(), source=ctx.providers.Stamen.TerrainBackground)
    else:
        print("CRS is not set properly.")
    

    filename = f'proj4_ex04_{country["name"].lower().replace(" ", "_")}.png'
    plt.savefig(filename, bbox_inches='tight')
    plt.close(fig)  



# %%



