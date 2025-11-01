import os
import json
import logging
import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_detection")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")


class ResultSaver:
    def __init__(self):
        self.setup_database()

        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'result-saver',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.consumer_config)
        self.consumer.subscribe([SCORES_TOPIC])

        logger.info('Result saver initialized')

    def setup_database(self):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_scores (
                    id SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(255) UNIQUE,
                    score FLOAT,
                    fraud_flag INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            cursor.close()
            conn.close()
            logger.info('Database setup completed')

        except Exception as e:
            logger.error(f'Database setup failed: {e}')

    def save_to_postgres(self, data):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
                VALUES (%s, %s, %s)
                ON CONFLICT (transaction_id) 
                DO UPDATE SET score = EXCLUDED.score, fraud_flag = EXCLUDED.fraud_flag
            """, (data['transaction_id'], data['score'], data['fraud_flag']))

            conn.commit()
            cursor.close()
            conn.close()

            logger.debug(f'Saved transaction {data["transaction_id"]} to database')

        except Exception as e:
            logger.error(f'Error saving to database: {e}')

    def run(self):
        logger.info('Starting result saver service...')
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    data = json.loads(msg.value().decode('utf-8'))
                    self.save_to_postgres(data)

                except Exception as e:
                    logger.error(f'Error processing message: {e}')

        except KeyboardInterrupt:
            logger.info('Service stopped by user')
        finally:
            self.consumer.close()


if __name__ == "__main__":
    saver = ResultSaver()
    saver.run()
