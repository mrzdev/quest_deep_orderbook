Steps to include orderbook Market Depth Ratio in FreqAI strategy (example):

- move both orderbook_pipeline.py, XGBoostRegressorOBExample.py to your `freqtrade/freqai/prediction_models` directory
- move config_ob_freqai.example.json to your `user_data/configs/` directory

Then run:
```bash
freqtrade trade --strategy FreqaiExampleStrategy --config user_data/configs/config_ob_freqai.example.json --freqaimodel XGBoostRegressorOBExample --dry-run --db-url sqlite:///tradesv3_obtest.dry_run.sqlite
```

*Note*: Following model is merely an example.