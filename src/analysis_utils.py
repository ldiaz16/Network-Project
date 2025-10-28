import networkx as nx
import pandas as pd
from .data_utils import compute_distance

def process_routes(routes_df, airports_df, aircraft_df):
    # merge in airport coords
    routes = routes_df.merge(
        airports_df[["IATA","Latitude","Longitude"]],
        left_on="Source airport", right_on="IATA", how="left"
    ).rename(columns={"Latitude":"Src Lat","Longitude":"Src Lon"})
    routes = routes.merge(
        airports_df[["IATA","Latitude","Longitude"]],
        left_on="Destination airport", right_on="IATA", how="left"
    ).rename(columns={"Latitude":"Dst Lat","Longitude":"Dst Lon"})

    # compute distance
    routes["Distance (miles)"] = routes.apply(
        lambda r: compute_distance(r["Src Lat"],r["Src Lon"],r["Dst Lat"],r["Dst Lon"]), axis=1
    )

    # merge aircraft config
    routes = routes.merge(
        aircraft_df[["Airline","Aircraft","Total"]],
        left_on=["Airline","Equipment"], right_on=["Airline","Aircraft"], how="left"
    )

    # seats per mile
    routes["Total Seats"] = routes["Total"].fillna(0)
    routes["Seats per Mile"] = routes.apply(
        lambda r: r["Total Seats"]/r["Distance (miles)"] if r["Distance (miles)"]>0 else 0, axis=1
    )
    return routes

def build_network(routes_df):
    G = nx.DiGraph()
    for _, r in routes_df.iterrows():
        G.add_edge(r["Source airport"], r["Destination airport"], distance=r["Distance (miles)"])
    return G

def analyze_network(G):
    return {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "top_hubs": sorted(G.degree, key=lambda x: x[1], reverse=True)[:5]
    }

def cost_analysis(routes_df):
    df = routes_df.copy()
    df["ASM"] = df["Total Seats"] * df["Distance (miles)"]
    df["CASM"] = 0.11  # simple default
    df["LF"] = 0.82    # assumed load factor
    df["Revenue"] = df["ASM"] * 0.14 * df["LF"]  # yield * rpm
    df["Cost"] = df["ASM"] * df["CASM"]
    df["Profit"] = df["Revenue"] - df["Cost"]
    return df
