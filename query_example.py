import pandas as pd
import sqlalchemy

MARKETS = ["BTCUSDT", "ETHUSDT"]

def create_query_string(market):
    """
    Example QuestDB query string that samples data using 5m timeframe, autofills NaNs 
    """
    assert market in MARKETS, "Not available market in this example"
    query = f"SELECT timestamp, pair, avg(asks_price_mean) asks_price_mean, avg(asks_price_std) asks_price_std, avg(bids_price_mean) bids_price_mean, avg(bids_price_std) bids_price_std FROM book WHERE pair='{market}' SAMPLE BY 5m FILL(PREV) ALIGN TO CALENDAR WITH OFFSET '00:05'";
    return query

def get_db_engine(url: str):
    """
    Connect to QuestDB and return the engine instance
    """
    engine = sqlalchemy.create_engine(url)
    return engine

def query_db(url: str, market: str):
    """
    Query QuestDB per singular market 
    """
    engine = get_db_engine(url)
    query_string = create_query_string(market)
    
    try:
        with engine.connect() as conn:
            orderbook_data = pd.read_sql(query_string, con=conn)
    except sqlalchemy.exc.DatabaseError as err:
        print(err)
        orderbook_data = pd.DataFrame()
    finally:
        if engine:
            engine.dispose()
    return orderbook_data



if __name__ == "__main__":
    questdb_url = 'postgresql://admin:quest@localhost:8812/qdb'
    for market in MARKETS:
        ob_df = query_db(questdb_url, market)
        print(ob_df.tail())