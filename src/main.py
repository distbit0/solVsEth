import requests
import pandas as pd
import plotly.graph_objects as go
from urllib.parse import urlparse
import re
from tqdm import tqdm
import logging
import os
import json
import time
import sqlite3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("script.log"), logging.StreamHandler()],
)

# Constants
CMC_CHART_URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart"
HEADERS = {
    "User-Agent": "curl/7.64.1",
    "Accept": "*/*",
    "Content-Type": "application/json",
}
CACHE_DB = "cache.db"  # SQLite database file
INPUT_FILE = os.path.join(".", "memeCoins.txt")


def extract_slug(url):
    """
    Extracts the slug from a CoinMarketCap currency URL.
    Example: 'https://coinmarketcap.com/currencies/ethereum/' -> 'ethereum'
    """
    path = urlparse(url).path
    match = re.match(r"^/currencies/([^/]+)/?$", path)
    if match:
        return match.group(1)
    else:
        logging.warning(f"Invalid CoinMarketCap URL format: {url}")
        return None


def initialize_database(db_path):
    """
    Initializes the SQLite database with necessary tables.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create table for URL cache
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS url_cache (
            url TEXT PRIMARY KEY,
            cmc_id INTEGER,
            chain TEXT
        )
    """
    )
    # Create table for Market Cap cache
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_cap_cache (
            cmc_id INTEGER,
            timestamp INTEGER,
            market_cap REAL,
            PRIMARY KEY (cmc_id, timestamp)
        )
    """
    )
    conn.commit()
    return conn


def get_token_info(url, conn):
    """
    Fetches token information by parsing the HTML of the given CoinMarketCap URL.
    Utilizes SQLite caching to avoid redundant network calls.
    Returns a tuple of (cmc_id, chain) or (None, None) if not applicable.
    """
    cursor = conn.cursor()
    # Check cache
    cursor.execute("SELECT cmc_id, chain FROM url_cache WHERE url = ?", (url,))
    result = cursor.fetchone()
    if result:
        logging.info(f"Using cached data for URL: {url}")
        return result if result[0] is not None else (None, None)

    # Fetch HTML
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 404:
            logging.error(f"HTTP error 404 for URL: {url}")
            cursor.execute(
                "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
                (url, None, None),
            )
            conn.commit()
            return (None, None)
        response.raise_for_status()
        html = response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception for URL {url}: {e}")
        cursor.execute(
            "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
            (url, None, None),
        )
        conn.commit()
        return (None, None)

    # Extract JSON from HTML
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "lxml")
    script_tag = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not script_tag or not script_tag.string:
        logging.warning(f"Embedded JSON data not found in HTML for URL: {url}")
        cursor.execute(
            "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
            (url, None, None),
        )
        conn.commit()
        return (None, None)

    try:
        data = json.loads(script_tag.string)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error for URL {url}: {e}")
        cursor.execute(
            "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
            (url, None, None),
        )
        conn.commit()
        return (None, None)

    try:
        detail = (
            data.get("props", {})
            .get("pageProps", {})
            .get("detailRes", {})
            .get("detail", {})
        )
        cmc_id = detail.get("id")
        platforms = detail.get("platforms", [])

        if not platforms:
            logging.warning(f"No platforms information found for URL: {url}")
            cursor.execute(
                "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
                (url, None, None),
            )
            conn.commit()
            return (None, None)

        first_platform = platforms[0]
        platform_name = first_platform.get("contractPlatform", "").strip()

        if not platform_name:
            logging.warning(f"No contract platform name found for URL: {url}")
            cursor.execute(
                "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
                (url, None, None),
            )
            conn.commit()
            return (None, None)

        # Standardize platform name (e.g., Capitalize each word)
        standardized_chain = " ".join(
            word.capitalize() for word in platform_name.split()
        )

        if not cmc_id:
            logging.warning(f"No CMC ID found for URL: {url}")
            cursor.execute(
                "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
                (url, None, None),
            )
            conn.commit()
            return (None, None)

        # Insert into cache
        cursor.execute(
            "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
            (url, cmc_id, standardized_chain),
        )
        conn.commit()
        return (cmc_id, standardized_chain)
    except Exception as e:
        logging.error(f"Error extracting token info from JSON for URL {url}: {e}")
        cursor.execute(
            "INSERT INTO url_cache (url, cmc_id, chain) VALUES (?, ?, ?)",
            (url, None, None),
        )
        conn.commit()
        return (None, None)


def get_historical_market_cap(cmc_id, conn):
    """
    Retrieves historical market capitalization data for a given CMC ID.
    Utilizes SQLite caching to avoid redundant network calls.
    Returns a pandas DataFrame with 'date' and 'market_cap' columns or None if not applicable.
    """
    cursor = conn.cursor()
    # Check if data exists in cache
    cursor.execute(
        "SELECT timestamp, market_cap FROM market_cap_cache WHERE cmc_id = ?", (cmc_id,)
    )
    rows = cursor.fetchall()
    if rows:
        logging.info(f"Using cached market cap data for CMC ID: {cmc_id}")
        data = rows
    else:
        # Fetch data from API
        params = {"id": cmc_id, "range": "ALL", "convertId": 2781}  # USD
        try:
            response = requests.get(
                CMC_CHART_URL, headers=HEADERS, params=params, timeout=10
            )
            if response.status_code == 404:
                logging.error(f"HTTP error 404 for CMC ID: {cmc_id}")
                return None
            response.raise_for_status()
            data_json = response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Request exception for CMC ID {cmc_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            logging.error(f"JSON decode error for CMC ID {cmc_id}: {e}")
            return None

        # Parse JSON to extract market cap
        if "data" in data_json and "points" in data_json["data"]:
            points = data_json["data"]["points"]
            market_cap_data = []
            for timestamp, values in points.items():
                # Correct market cap extraction from 'c[2]'
                market_cap = (
                    values["c"][2] if "c" in values and len(values["c"]) > 2 else 0
                )
                market_cap_data.append((int(timestamp), float(market_cap)))
            if not market_cap_data:
                logging.warning(f"No market cap data found for CMC ID: {cmc_id}")
                return None
            # Insert into cache
            cursor.executemany(
                "INSERT INTO market_cap_cache (cmc_id, timestamp, market_cap) VALUES (?, ?, ?)",
                [(cmc_id, ts, mc) for ts, mc in market_cap_data],
            )
            conn.commit()
            data = market_cap_data
            logging.info(f"Fetched and cached market cap data for CMC ID: {cmc_id}")
        else:
            logging.warning(f"No market cap data found for CMC ID: {cmc_id}")
            return None

    # Convert data to DataFrame
    if not data:
        return None
    df = pd.DataFrame(data, columns=["timestamp", "market_cap"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="s").dt.date
    df = df[["date", "market_cap"]]
    return df


def aggregate_market_cap(cmc_ids, chain_name, conn):
    """
    Aggregates market capitalization for a list of CMC IDs.
    Returns a pandas DataFrame with 'date' and 'total_market_cap'.
    Applies a rolling average to smooth out volatility.
    """
    aggregated_market_cap = {}

    for cmc_id in tqdm(cmc_ids, desc=f"Aggregating {chain_name} Market Caps"):
        df = get_historical_market_cap(cmc_id, conn)
        if df is not None:
            # Apply a rolling average to smooth the data (window size = 30 days)
            df_sorted = df.sort_values("date")
            df_sorted["smoothed_market_cap"] = (
                df_sorted["market_cap"].rolling(window=30, min_periods=1).mean()
            )

            for _, row in df_sorted.iterrows():
                date = row["date"]
                market_cap = row["smoothed_market_cap"]
                if date in aggregated_market_cap:
                    aggregated_market_cap[date] += market_cap
                else:
                    aggregated_market_cap[date] = market_cap
        # Introduce a small delay to reduce CPU usage
        time.sleep(0.05)

    if not aggregated_market_cap:
        return pd.DataFrame(columns=["date", "total_market_cap"])

    # Convert the aggregated market cap dictionary to DataFrame
    agg_df = pd.DataFrame(
        list(aggregated_market_cap.items()), columns=["date", "total_market_cap"]
    )
    agg_df.sort_values("date", inplace=True)
    return agg_df


def main():
    # Initialize SQLite database
    conn = initialize_database(CACHE_DB)

    # Check if input file exists
    if not os.path.isfile(INPUT_FILE):
        logging.error(f"Input file not found: {INPUT_FILE}")
        return

    # Read URLs from the input file
    with open(INPUT_FILE, "r") as file:
        currency_urls = [line.strip() for line in file if line.strip()]

    if not currency_urls:
        logging.error("No URLs found in the input file.")
        return

    # Extract slugs and fetch token info
    all_chains = {}  # Dictionary to hold chain_name: list of cmc_ids

    for url in tqdm(currency_urls, desc="Processing URLs"):
        cmc_id, chain = get_token_info(url, conn)
        if cmc_id and chain:
            if chain not in all_chains:
                all_chains[chain] = []
            all_chains[chain].append(cmc_id)

    # Count tokens per chain
    token_counts = {chain: len(ids) for chain, ids in all_chains.items()}
    for chain, count in token_counts.items():
        logging.info(f"Chain: {chain}, Tokens: {count}")

    if not all_chains:
        logging.error("No tokens found on any chains.")
        return

    # Aggregate market cap for each chain
    logging.info("Aggregating market cap data for each chain...")
    aggregated_data = {}
    for chain_name, ids in all_chains.items():
        if ids:
            logging.info(f"Aggregating market caps for {chain_name}...")
            agg_df = aggregate_market_cap(ids, chain_name, conn)
            if not agg_df.empty:
                aggregated_data[chain_name] = agg_df

    if not aggregated_data:
        logging.error("No market cap data available to plot.")
        return

    # Merge all aggregated data on date
    combined_df = None
    for chain, df in aggregated_data.items():
        df = df.rename(columns={"total_market_cap": f"total_market_cap_{chain}"})
        if combined_df is None:
            combined_df = df
        else:
            combined_df = pd.merge(combined_df, df, on="date", how="outer")

    combined_df.fillna(0, inplace=True)
    combined_df.sort_values("date", inplace=True)

    # Downsample data to reduce the number of points plotted
    # For example, take one data point per month
    combined_df["date"] = pd.to_datetime(combined_df["date"])
    combined_df.set_index("date", inplace=True)
    combined_df = combined_df.resample("M").mean().reset_index()

    # Plotting using Plotly
    fig = go.Figure()

    for chain in aggregated_data.keys():
        fig.add_trace(
            go.Scatter(
                x=combined_df["date"],
                y=combined_df[f"total_market_cap_{chain}"],
                mode="lines",
                name=f"{chain} Ecosystem Market Cap",
            )
        )

    fig.update_layout(
        title="Ecosystem Market Cap Over Time by Blockchain",
        xaxis_title="Date",
        yaxis_title="Total Market Cap (USD)",
        hovermode="x unified",
        template="plotly_dark",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    fig.update_yaxes()  # Optional: Use logarithmic scale for better visualization

    fig.show()

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
