# quest_deep_orderbook
Store Binance Futures orderbook data in QuestDB leveraging the brilliance of [unicorn_binance_local_depth_cache](https://github.com/LUCIT-Systems-and-Development/unicorn-binance-local-depth-cache).    
The basic statistics are extracted from high-granularity (max depth) data.    
    
The data is intended to be used for further analysis, such as anomaly detection techniques i.e. Dissimilarity Index.  
This is a [quest_cryptostore](https://github.com/mrzdev/quest_cryptostore) successor, better suited for this goal.

Usage:
```console
  bash setup.sh
  python query_example.py
```
