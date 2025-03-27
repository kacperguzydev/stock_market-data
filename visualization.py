import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_price_comparison_data() -> pd.DataFrame:
    """
    Pulls coin_name, lowest_price, lowest_date, highest_price, highest_date
    from lowest_price and highest_price tables.
    """
    query = """
    SELECT l.coin_name, l.lowest_price, l.lowest_date, 
           h.highest_price, h.highest_date
      FROM lowest_price l
      JOIN highest_price h ON l.coin_name = h.coin_name;
    """
    try:
        engine = create_engine(
            f"postgresql://{config.DB_SETTINGS['user']}:{config.DB_SETTINGS['password']}@"
            f"{config.DB_SETTINGS['host']}:{config.DB_SETTINGS['port']}/{config.DB_SETTINGS['dbname']}"
        )
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        return df
    except Exception as exc:
        logger.error("Error fetching comparison data: %s", exc)
        return pd.DataFrame()


def plot_price_comparison(df: pd.DataFrame) -> None:
    """
    Displays a bar chart of lowest vs. highest prices per coin,
    annotated with values.
    """
    if df.empty:
        logger.error("No data to visualize.")
        return

    df["lowest_price"] = df["lowest_price"].astype(float)
    df["highest_price"] = df["highest_price"].astype(float)

    df_melted = df.melt(
        id_vars="coin_name",
        value_vars=["lowest_price", "highest_price"],
        var_name="Price Type",
        value_name="Price",
    )

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(x="coin_name", y="Price", hue="Price Type", data=df_melted)

    for patch in ax.patches:
        if patch.get_height() > 0:
            ax.annotate(
                f"{patch.get_height():.2f}",
                (patch.get_x() + patch.get_width() / 2.0, patch.get_height()),
                ha="center",
                va="bottom",
                xytext=(0, 5),
                textcoords="offset points",
                fontsize=8,
                color="black",
            )

    plt.title("Lowest vs. Highest Prices per Coin")
    plt.xlabel("Coin")
    plt.ylabel("Price (USD)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def main() -> None:
    df = get_price_comparison_data()
    if not df.empty:
        logger.info("Comparison data:\n%s", df)
        plot_price_comparison(df)
    else:
        logger.error("No comparison data to plot.")


if __name__ == "__main__":
    main()
