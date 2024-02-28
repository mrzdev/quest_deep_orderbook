import logging
from typing import Any, Dict

import scipy as spy
from xgboost import XGBRegressor

from freqtrade.freqai.base_models.BaseRegressionModel import BaseRegressionModel
from freqtrade.freqai.data_kitchen import FreqaiDataKitchen
from freqtrade.freqai.tensorboard import TBCallback

from orderbook_pipeline import OBPipeline

logger = logging.getLogger(__name__)


class XGBoostRegressor(BaseRegressionModel):
    """
    User created prediction model. The class inherits IFreqaiModel, which
    means it has full access to all Frequency AI functionality. Typically,
    users would use this to override the common `fit()`, `train()`, or
    `predict()` methods to add their custom data handling tools or change
    various aspects of the training that cannot be configured via the
    top level config.json file.
    """
    
    ob_pipeline = OBPipeline()
    
    def fit(self, data_dictionary: Dict, dk: FreqaiDataKitchen, **kwargs) -> Any:
        """
        User sets up the training and test data to fit their desired model here
        :param data_dictionary: the dictionary holding all data for train, test,
            labels, weights
        :param dk: The datakitchen object for the current coin/model
        """

        X = data_dictionary["train_features"]
        y = data_dictionary["train_labels"]

        if self.freqai_info.get("data_split_parameters", {}).get("test_size", 0.1) == 0:
            eval_set = None
            eval_weights = None
        else:
            eval_set = [(data_dictionary["test_features"], data_dictionary["test_labels"])]
            eval_weights = [data_dictionary['test_weights']]

        sample_weight = data_dictionary["train_weights"]

        xgb_model = self.get_init_model(dk.pair)

        model = XGBRegressor(**self.model_training_parameters)

        model.set_params(callbacks=[TBCallback(dk.data_path)])
        model.fit(X=X, y=y, sample_weight=sample_weight, eval_set=eval_set,
                  sample_weight_eval_set=eval_weights, xgb_model=xgb_model)
        # set the callbacks to empty so that we can serialize to disk later
        model.set_params(callbacks=[])

        return model

    def fit_live_predictions(self, dk: FreqaiDataKitchen, pair: str) -> None:
        """
        Fit the labels with a gaussian distribution
        """

        # add classes from classifier label types if used
        full_labels = dk.label_list + dk.unique_class_list

        num_candles = self.freqai_info.get("fit_live_predictions_candles", 100)
        dk.data["labels_mean"], dk.data["labels_std"] = {}, {}
        for label in full_labels:
            if self.dd.historic_predictions[dk.pair][label].dtype == object:
                continue
            f = spy.stats.norm.fit(
                self.dd.historic_predictions[dk.pair][label].tail(num_candles))
            dk.data["labels_mean"][label], dk.data["labels_std"][label] = f[0], f[1]
        
        # Get market depth ratio
        mdr = self.ob_pipeline(pair)
        dk.data['extra_returns_per_train']["orderbook_mdr"] = mdr.iloc[-1]
        
        return