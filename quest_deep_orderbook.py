import pandas as pd
import polars as pl
from typing import List, Tuple
from unicorn_binance_local_depth_cache import BinanceLocalDepthCacheManager, DepthCacheOutOfSync
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from questdb.ingress import Sender, IngressError, TimestampNanos
import warnings, logging, sys, os, time, asyncio
from get_docker_secret import get_docker_secret

# Define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("binance_local_depth_cache")
logFormatter = logging.Formatter(\
    "%(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

# Ignore FutureWarning messages for reducing the verbosity.
warnings.simplefilter(action='ignore', category=FutureWarning)

# Secrets are lower-case, envvars upper-case. 
# Automatic conversion of name can be switched off via autocast_name=False.
QUEST_HOST = get_docker_secret('QUEST_HOST', default='127.0.0.1')
QUEST_PORT = get_docker_secret('QUEST_PORT', default=9009)

class OrderBookStreamer():
    """
    Stream orderbook data using python-binance (https://github.com/sammchardy/python-binance)
    at max depth, extract statistics using polars, and push into QuestDB for further analysis.
    """

    def __init__(self, exchange: str ="binance.com-futures", markets : List =['BTCUSDT', 'ETHUSDT']):
        self.exchange = exchange
        self.markets = markets
        self.ubwa = BinanceWebSocketApiManager(exchange=self.exchange, enable_stream_signal_buffer=True)
        self.ubldc = BinanceLocalDepthCacheManager(exchange=self.exchange, ubwa_manager=self.ubwa)

    def get_book(self, market: str) -> Tuple[List, List]:
        """
        Get orderbook asks and bids for the given market pair.

        Args:
            market (str): The currently processed market.

        Returns:
            asks, bids (tuple): a two list tuple containing asks and bids data
        """
        while True:
            try:
                asks = self.ubldc.get_asks(market=market)
                bids = self.ubldc.get_bids(market=market)
                break
            except DepthCacheOutOfSync:
                logger.info(f"{market} orderbook out of sync")
                time.sleep(1)
        return asks, bids

    def create_dfs(self, asks: List, bids: List) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """
        Create dataframes from obtained asks and bids lists.
        Args:
            asks (list): asks list
            bids (list): bids list

        Returns:
            df_asks, df_bids (Tuple[pl.DataFrame, pl.DataFrame]): a two polars DataFrames tuple containing asks and bids data
        """
        df_asks = pl.DataFrame(asks, schema=[("price", pl.Float32), ("size", pl.Float32)])
        df_bids = pl.DataFrame(bids, schema=[("price", pl.Float32), ("size", pl.Float32)])
        return df_asks, df_bids

    def prepare_columns(self, df: pl.DataFrame, side_prefix: str) -> pl.DataFrame:
        """
        Create unique column names to prepare before pushing to QuestDB.

        Args:
            df (pl.DataFrame): polars DataFrame
            side_prefix (str): string to prefix column names with

        Returns:
            price_size_df (pl.DataFrame): a singular polars DataFrame containing asks and bids data
        """
        cols = df.get_column('statistic').to_list()
        price_stats = df.select([
            pl.col("price").cast(pl.Float32),
        ]).transpose(include_header=False, column_names=cols).select(
            pl.all().reverse().name.prefix("price_")
        )
        size_stats = df.select([
            pl.col("size").cast(pl.Float32),
        ]).transpose(include_header=False, column_names=cols).select(
            pl.all().reverse().name.prefix("size_")
        )
        price_size_df = price_stats.hstack(size_stats).select(
            pl.all().reverse().name.prefix(side_prefix)
        )
        return price_size_df

    def analyse_book(self, df_asks: pl.DataFrame, df_bids: pl.DataFrame) -> pl.DataFrame:
        """
        Extract statistics from the orderbook data stored in polars DataFrame.

        Args:
            df_asks (pl.DataFrame): polars DataFrame with asks data
            df_bids (pl.DataFrame): polars DataFrame with bids data

        Returns:
            df (pl.DataFrame): a singular polars DataFrame containing asks and bids data

        """
        asks_stats = df_asks.describe()
        bids_stats = df_bids.describe()
        formatted_asks_df = self.prepare_columns(asks_stats, 'asks_')
        formatted_bids_df = self.prepare_columns(bids_stats, 'bids_')
        df = formatted_asks_df.hstack(formatted_bids_df)
        return df

    def push_to_db(self, df: pd.DataFrame, key: str = 'book') -> None:
        """
        Insert new row into QuestDB table.
        It will automatically create a new table if it doesn't exists yet.

        Args:
            df (pd.DataFrame): a pandas DataFrame ready for ingestion
            key (str): a table name to push data into
        """
        logger.info(f"Pushing data to QuestDB table={key}")
        try:
            with Sender(QUEST_HOST, QUEST_PORT) as sender:
                sender.dataframe(
                    df,
                    table_name=key,  # Table name to insert into.
                    symbols=["pair", "exchange"],  # Columns to be inserted as SYMBOL types.
                    at=TimestampNanos.now())  # Timestamp.
        except IngressError as e:
            logger.error(f"Got error: {e}")

    def populate_dataframe(self, depth_cache: DepthCache, market: str) -> pd.DataFrame:
        """
        Populate the statistics dataframe.

        Args:
            depth_cache (DepthCache): Initialized DepthCache for the given market.
            market (str): The currently processed market.
        """
        asks, bids = self.get_book(depth_cache)
        df_asks, df_bids = self.create_dfs(asks, bids)
        df = self.analyse_book(df_asks, df_bids)
        df = df.with_columns(pl.lit(market).alias('pair'))
        df = df.with_columns(pl.lit(self.exchange).alias('exchange'))
        df = df.select(pl.all().name.map(lambda col_name: col_name.replace('%', '')))
        return df.to_pandas()

    def callback(self, depth_cache: DepthCache):
        """
        Callback pushing the obtained dataframe to the db.

        Args:
            depth_cache (DepthCache): a pandas DataFrame ready for ingestion
        """
        df = self.populate_dataframe(depth_cache, depth_cache.symbol)
        self.push_to_db(df)

    def __call__(self) -> None:
        """
        Call the OrderBookStreamer.
        """
        dcm = ThreadedFDCManager(api_key = API_KEY, api_secret = SECRET_KEY)
        dcm.start()

        for market in self.markets:
            dcm_name = dcm.start_futures_depth_socket(self.callback, limit = ORDERBOOK_DEPTH, symbol = market)

        dcm.join()

if __name__ == "__main__":
    current_whitelist = [
        "ETHUSDT",
        "BTCUSDT"
    ]
    orderbook_streamer = OrderBookStreamer(exchange = "binance.com-futures", markets = current_whitelist)
    orderbook_streamer()
