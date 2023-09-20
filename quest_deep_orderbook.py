# Copyright (c) 2022-2023, LUCIT Systems and Development (https://www.lucit.tech) and Oliver Zehentleitner
# All rights reserved.
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import pandas as pd
import polars as pl
from unicorn_binance_local_depth_cache import BinanceLocalDepthCacheManager, DepthCacheOutOfSync
from unicorn_binance_websocket_api import BinanceWebSocketApiManager
from questdb.ingress import Sender, IngressError, TimestampNanos
import logging
import sys
import time

# Define logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.getLogger("unicorn_binance_local_depth_cache")
logFormatter = logging.Formatter(\
    "%(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

QUEST_HOST = '127.0.0.1'
QUEST_PORT = 9009

class OrderBookStreamer():
    """
    Leveraging the brilliance of unicorn_binance_local_depth_cache project
    (https://github.com/LUCIT-Systems-and-Development/unicorn-binance-local-depth-cache)
    to get orderbook at max available depth and extract statistics using polars, 
    then push into QuestDB for further analysis.
    """
    def __init__(self, exchange="binance.com-futures", markets=['BTCUSDT', 'ETHUSDT']):
        self.exchange = exchange
        self.markets = markets
        self.ubwa = BinanceWebSocketApiManager(exchange=self.exchange, enable_stream_signal_buffer=True)
        self.ubldc = BinanceLocalDepthCacheManager(exchange=self.exchange, ubwa_manager=self.ubwa)

    def get_book(self, market: str):
        """
        Get asks and bids from the given market name
        Args:
            market (str): market name
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

    def create_dfs(self, asks: list, bids: list):
        """
        Create dataframes from asks and bids list from the Binance Local Depth Cache
        Args:
            asks (list): asks list
            bids (list): bids list
        """
        print(asks)
        df_asks = pl.DataFrame(asks, schema=[("price", pl.Float32), ("size", pl.Float32)])
        df_bids = pl.DataFrame(bids, schema=[("price", pl.Float32), ("size", pl.Float32)])
        return df_asks, df_bids

    def prepare_columns(self, df: pl.DataFrame, side_prefix: str):
        """
        Create unique column names to prepare before pushing to QuestDB
        Args:
            df (pl.DataFrame): polars DataFrame
            side_prefix (str): string to prefix column names with
        """
        cols = df.get_column('describe').to_list()
        price_stats = df.select([
            pl.col("price").cast(pl.Float32),
        ]).transpose(include_header=False, column_names=cols).select(
            pl.all().reverse().prefix("price_")
        )
        size_stats = df.select([
            pl.col("size").cast(pl.Float32),
        ]).transpose(include_header=False, column_names=cols).select(
            pl.all().reverse().prefix("size_")
        )
        price_size_df = price_stats.hstack(size_stats).select(
            pl.all().reverse().prefix(side_prefix)
        )
        return price_size_df

    def analyse_book(self, df_asks: pl.DataFrame, df_bids: pl.DataFrame):
        """
        Extract statistics from the orderbook data stored in polars DataFrame
        Args:
            df_asks (pl.DataFrame): asks polars DataFrame
            df_bids (pl.DataFrame): bids polars DataFrame
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
            df (pd.DataFrame): orderbook statistics pandas DataFrame 
            key (str): what table to write into
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
            sys.stderr.write(f'Got error: {e}\n')

    def __call__(self):
        """
        Call the orderbook local depth cache streamer
        """
        for market in self.markets:
            self.ubldc.create_depth_cache(markets=market)
        while True:
            for market in self.markets:
                asks, bids = self.get_book(market)
                df_asks, df_bids = self.create_dfs(asks, bids)
                df = self.analyse_book(df_asks, df_bids)
                df = df.with_columns(pl.lit(market).alias('pair'))
                df = df.with_columns(pl.lit(self.exchange).alias('exchange'))
                df = df.select(pl.all().map_alias(lambda col_name: col_name.replace('%', '')))
                self.push_to_db(df.to_pandas())
            time.sleep(1)


if __name__ == "__main__":
    orderbook_streamer = OrderBookStreamer()
    orderbook_streamer()
    