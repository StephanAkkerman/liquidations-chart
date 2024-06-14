import glob
import os
from datetime import datetime

import pandas as pd


def convert_timestamp_to_date(timestamp):
    return datetime.utcfromtimestamp(timestamp / 1000).strftime("%Y-%m-%d")


def summarize_liquidations(coin="BTCUSDT", market="um"):
    file_pattern = f"data/{coin}/{market}/*.csv"
    # Read all CSV files matching the pattern
    all_files = glob.glob(file_pattern)

    df_list = []
    for file in all_files:
        df = pd.read_csv(file)
        df_list.append(df)

    # Concatenate all DataFrames into a single DataFrame
    all_data = pd.concat(df_list, ignore_index=True)

    # Remove duplicate rows
    all_data.drop_duplicates(inplace=True)

    # Convert the 'time' column to date
    all_data["date"] = all_data["time"].apply(convert_timestamp_to_date)

    # Calculate total volume in USD
    all_data["volume"] = all_data["original_quantity"] * all_data["average_price"]

    # Summarize the data
    summary = (
        all_data.groupby(["date", "side"])
        .agg(
            total_volume=("volume", "sum"),
            total_liquidations=("original_quantity", "sum"),  # used for avg price
        )
        .reset_index()
    )

    summary["average_price"] = summary["total_volume"] / summary["total_liquidations"]

    # Pivot the summary to have separate columns for buy and sell sides
    pivot_summary = summary.pivot(
        index="date", columns="side", values=["total_volume", "average_price"]
    ).fillna(0)
    pivot_summary.columns = [
        "_".join(col).strip() for col in pivot_summary.columns.values
    ]
    pivot_summary = pivot_summary.rename(
        columns={
            "total_volume_BUY": "Buy Volume (USD)",
            "total_volume_SELL": "Sell Volume (USD)",
            "average_price_BUY": "Average Buy Price",
            "average_price_SELL": "Average Sell Price",
        }
    )

    # Calculate overall average price
    pivot_summary["Average Price"] = (
        pivot_summary["Average Buy Price"] * pivot_summary["Buy Volume (USD)"]
        + pivot_summary["Average Sell Price"] * pivot_summary["Sell Volume (USD)"]
    ) / (pivot_summary["Buy Volume (USD)"] + pivot_summary["Sell Volume (USD)"])

    # Drop individual average price columns if only overall average price is needed
    pivot_summary.drop(
        columns=["Average Buy Price", "Average Sell Price"], inplace=True
    )

    # Rename columns as required
    pivot_summary.rename(
        columns={
            "Buy Volume (USD)": "Shorts",
            "Sell Volume (USD)": "Longs",
            "Average Price": "price",
        },
        inplace=True,
    )

    # Convert the index to datetime and set it as index
    pivot_summary["date"] = pd.to_datetime(pivot_summary.index)
    pivot_summary = pivot_summary.set_index("date")

    # Save it locally
    os.makedirs("data/summary", exist_ok=True)
    os.makedirs(f"data/summary/{coin}", exist_ok=True)
    os.makedirs(f"data/summary/{coin}/{market}", exist_ok=True)
    pivot_summary.to_csv(f"data/summary/{coin}/{market}/liquidation_summary.csv")
