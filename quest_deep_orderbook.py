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

    def calculate_mdr(self, df_asks: pl.DataFrame, df_bids: pl.DataFrame, price_range: float = 0.01) -> pl.DataFrame:
        """
        Calculate MDR (Market Depth Ratio) filtered by cutoffs within price_range from the midpoint.

        Args:
            df_asks (pl.DataFrame): a polars DataFrame with asks [price, size] columns.
            df_bids (pl.DataFrame): a polars DataFrame with bids [price, size] columns.
            price_range (float): a price range from the midpoint (defaults to 0.01).
        """
        best_ask = df_asks[0, 0]
        best_bid = df_bids[0, 0]
        mid_price = (best_bid + best_ask) / 2
        bid_cutoff = mid_price * (1 - price_range)
        ask_cutoff = mid_price * (1 + price_range)
        bid_volume = df_bids.select([
            pl.col("size").filter((pl.col("price") >= bid_cutoff)).sum()
        ])
        ask_volume = df_asks.select([
            pl.col("size").filter((pl.col("price") <= ask_cutoff)).sum()
        ])
        mdr = (bid_volume - ask_volume) / (bid_volume + ask_volume)
        mdr = mdr.rename({"size": "mdr"})
        return mdr

    def populate_dataframe(self, market: str) -> pd.DataFrame:
        """
        Populate the metrics dataframe.
        Prepare the dataframe for ingestion (include market and exchange information).
        Convert to a QuestDB's ingress supported pd.DataFrame.
        
        Args:
            market (str): The currently processed market.
        """
        asks, bids = self.get_book(market)
        df_asks, df_bids = self.create_dfs(asks, bids)
        df_mdr = self.calculate_mdr(df_asks, df_bids)
        df_mdr = df_mdr.with_columns(pl.lit(market).alias('pair'))
        df_mdr = df_mdr.with_columns(pl.lit(self.exchange).alias('exchange'))
        return df_mdr.to_pandas()

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
