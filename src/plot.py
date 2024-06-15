from datetime import timedelta
from math import floor, log

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib import ticker

from data import get_new_data
from summary import summarize_liquidations

BACKGROUND_COLOR = "#0d1117"
FIGURE_SIZE = (15, 7)
COLORS_LABELS = {"#d9024b": "Shorts", "#45bf87": "Longs", "#f0b90b": "Price"}


def human_format(number: float, absolute: bool = False, decimals: int = 0) -> str:
    """
    Takes a number and returns a human readable string.
    Taken from: https://stackoverflow.com/questions/579310/formatting-long-numbers-as-strings-in-python/45846841.

    Parameters
    ----------
    number : float
        The number to be formatted.
    absolute : bool
        If True, the number will be converted to its absolute value.
    decimals : int
        The number of decimals to be used.

    Returns
    -------
    str
        The formatted number as a string.
    """

    # Try to convert to float
    if isinstance(number, str):
        try:
            number = float(number)
        except ValueError:
            number = 0

    if number == 0:
        return "0"

    # https://idlechampions.fandom.com/wiki/Large_number_abbreviations
    units = ["", "K", "M", "B", "t", "q"]
    k = 1000.0
    magnitude = int(floor(log(abs(number), k)))

    if decimals > 0:
        rounded_number = round(number / k**magnitude, decimals)
    else:
        rounded_number = int(number / k**magnitude)

    if absolute:
        rounded_number = abs(rounded_number)

    return f"{rounded_number}{units[magnitude]}"


def add_legend(ax):
    # Create custom legend handles with square markers, including BTC price
    legend_handles = [
        plt.Line2D(
            [0],
            [0],
            marker="s",
            color=BACKGROUND_COLOR,
            markerfacecolor=color,
            markersize=10,
            label=label,
        )
        for color, label in zip(
            list(COLORS_LABELS.keys()), list(COLORS_LABELS.values())
        )
    ]

    # Add legend
    legend = ax.legend(
        handles=legend_handles,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.0),
        ncol=len(legend_handles),
        frameon=False,
        fontsize="small",
        labelcolor="white",
    )

    # Make legend text bold
    for text in legend.get_texts():
        text.set_fontweight("bold")

    # Adjust layout to reduce empty space around the plot
    plt.subplots_adjust(left=0.05, right=0.95, top=0.875, bottom=0.1)


def liquidations_plot(df):
    """
    Copy chart like https://www.coinglass.com/LiquidationData

    Codes based on:
    https://github.com/OpenBB-finance/OpenBBTerminal/blob/main/openbb_terminal/cryptocurrency/due_diligence/coinglass_view.py
    """

    if df is None or df.empty:
        return

    df_price = df[["price"]].copy()
    df_without_price = df.drop("price", axis=1)
    df_without_price["Shorts"] = df_without_price["Shorts"] * -1

    # This plot has 2 axes
    fig, ax1 = plt.subplots()
    fig.patch.set_facecolor(BACKGROUND_COLOR)
    ax1.set_facecolor(BACKGROUND_COLOR)

    ax2 = ax1.twinx()

    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=14))

    ax1.bar(
        df_without_price.index,
        df_without_price["Shorts"],
        label="Shorts",
        color="#d9024b",
    )

    ax1.bar(
        df_without_price.index,
        df_without_price["Longs"],
        label="Longs",
        color="#45bf87",
    )

    ax1.get_yaxis().set_major_formatter(
        ticker.FuncFormatter(lambda x, _: f"${human_format(x, absolute=True)}")
    )

    # Set price axis
    ax2.plot(df_price.index, df_price, color="#edba35", label="BTC Price")
    ax2.set_xlim([df_price.index[0], df_price.index[-1]])
    ax2.set_ylim(bottom=df_price.min().values * 0.95, top=df_price.max().values * 1.05)
    ax2.get_yaxis().set_major_formatter(lambda x, _: f"${human_format(x)}")

    # Add combined legend using the custom add_legend function
    add_legend(ax2)

    # Add gridlines
    plt.grid(axis="y", color="grey", linestyle="-.", linewidth=0.5, alpha=0.5)

    # Remove spines
    ax1.spines["top"].set_visible(False)
    ax1.spines["bottom"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.spines["left"].set_visible(False)
    ax1.tick_params(left=False, bottom=False, right=False, colors="white")

    ax2.spines["top"].set_visible(False)
    ax2.spines["bottom"].set_visible(False)
    ax2.spines["right"].set_visible(False)
    ax2.spines["left"].set_visible(False)
    ax2.tick_params(left=False, bottom=False, right=False, colors="white")

    # Fixes first and last bar not showing
    ax1.set_xlim(
        left=df_without_price.index[0] - timedelta(days=1),
        right=df_without_price.index[-1] + timedelta(days=1),
    )
    ax2.set_xlim(
        left=df_without_price.index[0] - timedelta(days=1),
        right=df_without_price.index[-1] + timedelta(days=1),
    )

    # Set correct size
    fig.set_size_inches(FIGURE_SIZE)

    # Add the title in the top left corner
    plt.text(
        -0.025,
        1.125,
        "Total Liquidations Chart",
        transform=ax1.transAxes,
        fontsize=14,
        verticalalignment="top",
        horizontalalignment="left",
        color="white",
        weight="bold",
    )

    plt.show()


def show_plot(coin="BTCUSDT", market="um"):
    new_data = get_new_data(coin, market=market)
    if new_data:
        print(f"Downloaded {len(new_data)} new files.")
        # Recreate the summary
        summarize_liquidations(coin=coin, market=market)
    # Load the summary
    df = pd.read_csv(
        f"data/summary/{coin}/{market}/liquidation_summary.csv",
        index_col=0,
        parse_dates=True,
    )

    # Use the last 6 months
    liquidations_plot(df[-180:])
