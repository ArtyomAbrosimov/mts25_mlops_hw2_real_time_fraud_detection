import os
import sys
import pandas as pd
import json
import logging
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath('./src'))
from preprocessing import load_train_data, run_preprocessing
from scorer import load_model, make_prediction

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Set kafka configuration file
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")


class KafkaFraudDetector:
    def __init__(self):
        logger.info('Initializing Kafka Fraud Detector...')

        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'fraud-detector',
            'auto.offset.reset': 'earliest'
        }

        self.producer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
        }

        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])
        self.producer = Producer(self.producer_config)

        model_path = '/app/models/catboost_model.cbm'
        load_model(model_path)
        self.train_data = load_train_data()
        logger.info('Service initialized successfully')

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()}')

    def process_message(self, msg):
        try:
            logger.info(f"Received message: {msg.value()}")
            data = json.loads(msg.value().decode('utf-8'))
            transaction_id = data.get('transaction_id')

            if not transaction_id:
                logger.error('Missing transaction_id in message')
                return

            logger.info(f'Processing transaction: {transaction_id}')

            input_df = pd.DataFrame([data['data']])

            processed_df = run_preprocessing(input_df)
            logger.info(f'Preprocessing completed. Shape: {processed_df.shape}')

            prediction_result = make_prediction(processed_df)
            logger.info(f'Prediction completed: {prediction_result.iloc[0].to_dict()}')

            result = {
                'transaction_id': transaction_id,
                'score': float(prediction_result.iloc[0]['score']),
                'fraud_flag': int(prediction_result.iloc[0]['fraud_flag']),
                'timestamp': pd.Timestamp.now().isoformat()
            }

            logger.info(f'Sending to scores topic: {result}')

            self.producer.produce(
                SCORES_TOPIC,
                value=json.dumps(result),
                callback=self.delivery_report
            )
            self.producer.flush()

            logger.info(f'Successfully processed transaction {transaction_id}')

        except Exception as e:
            logger.error(f'Error processing message: {e}', exc_info=True)

    def run(self):
        logger.info('Starting Kafka fraud detection service...')
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                self.process_message(msg)

        except KeyboardInterrupt:
            logger.info('Service stopped by user')
        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == "__main__":
    service = KafkaFraudDetector()
    service.run()
