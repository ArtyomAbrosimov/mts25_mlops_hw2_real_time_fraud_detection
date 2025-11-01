import pandas as pd
import logging
from catboost import CatBoostClassifier

logger = logging.getLogger(__name__)

model = None
model_th = 0.98


def load_model(model_path):
    global model
    model = CatBoostClassifier()
    model.load_model(model_path)
    logger.info('Model loaded successfully')


def make_prediction(processed_data):
    categorical_cols = processed_data.select_dtypes(include=['object']).columns
    for col in categorical_cols:
        processed_data[col] = processed_data[col].astype(str)
    probabilities = model.predict_proba(processed_data)[:, 1]
    binary_predictions = (probabilities >= model_th).astype(int)
    result = pd.DataFrame({
        'score': probabilities,
        'fraud_flag': binary_predictions
    })
    logger.debug(f'Prediction completed: {len(result)} rows')
    return result
