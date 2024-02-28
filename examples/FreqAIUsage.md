Steps to include orderbook Market Depth Ratio in FreqAI strategy (example):

- move both orderbook_pipeline.py, XGBoostRegressorOBExample.py to your `freqtrade/freqai/prediction_models` directory
- move config_ob_freqai.example.json to your `user_data/configs/` directory

Then run:
```bash
freqtrade trade --strategy FreqaiExampleStrategy \
                --config user_data/configs/config_ob_freqai.example.json \
                --freqaimodel XGBoostRegressorOBExample \
                --db-url sqlite:///tradesv3_obtest.dry_run.sqlite \
                --dry-run
```

Now `orderbook_mdr` becomes available to the strat and can be plotted:
![image](https://github.com/mrzdev/quest_deep_orderbook/assets/106373816/19504090-f6d3-4012-a0fa-d5928b89c489)

*Note*: Following model is merely an example.
