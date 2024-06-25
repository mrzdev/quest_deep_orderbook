import pandas as pd
from sqlalchemy import create_engine

class OBPipeline():
    """
        Retrieve orderbook metrics stored in QuestDB.
    """
    def __init__(self):
        # QuestDB PostgreSQL url
        self.db_url = 'postgresql://admin:quest@localhost:8812/qdb'

    @staticmethod
    def convert_name(pair: str) -> str:
        """
        Convert pair name from Freqtrade naming convention to the one stored in the database.
        Example: "BTC/USDT:USDT" -> "BTCUSDT".

        Args:
            pair (str): The pair name used in Freqtrade.
        """
        pair_in_db = pair.split(":")[0].replace("/", "")
        return pair_in_db

    def create_query_string(self) -> str:
        """
        Create a query string to get sampled data from QuestDB. 
        Pair name example: "BTCUSDT".
        """
        query = "SELECT timestamp, pair, mdr FROM book WHERE pair=%s LIMIT -1";
        return query
    
    def market_depth_ratio(self, orderbook: pd.DataFrame) -> pd.Series:
        """
        Return MDR (Market Depth Ratio) Series.

        Args:
            orderbook (pd.DataFrame): orderbook metrics DataFrame obtained with query.
        """
        mdr = orderbook.mdr
        return mdr

    def query_db(self, pair: str) -> pd.DataFrame:
        """
        Create SQL engine and query QuestDB.
        Pair name example: "BTCUSDT".

        Args:
            pair (str): The pair name used in QuestDB.
        """
        orderbook = pd.DataFrame()
        engine = create_engine(self.db_url)
        query_string = self.create_query_string()
        try:
            with engine.connect() as conn:
                orderbook = pd.read_sql(query_string, con=conn, params=(pair,))
        finally:
            if engine:
                engine.dispose()
        return orderbook

    def __call__(self, pair: str) -> pd.Series:
        """
        Obtain orderbook metrics for the currently processed pair.

        Args:
            pair (str): The pair name used in Freqtrade.
        """
        pair_in_db = self.convert_name(pair)
        df = self.query_db(pair_in_db)
        mdr = self.market_depth_ratio(df)
        return mdr
