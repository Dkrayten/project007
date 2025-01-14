import pika
import json
import logging
import time
import random
from datetime import datetime
from typing import Dict, Optional

class NewsPublisher:
    def __init__(self, host: str = 'localhost', port: int = 5672, max_retries: int = 3):
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.exchange_name = 'news_exchange'
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel = None
        
        # Enhanced logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='news_publisher.log',
            filemode='a'
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """
        Robust connection to RabbitMQ with retry mechanism
        """
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Connecting to RabbitMQ (Attempt {attempt + 1})")
                
                # Enhanced connection parameters
                connection_params = pika.ConnectionParameters(
                    host=self.host, 
                    port=self.port,
                    connection_attempts=self.max_retries,
                    retry_delay=3
                )
                
                self.connection = pika.BlockingConnection(connection_params)
                self.channel = self.connection.channel()
                
                # Exchange declaration with more robust settings
                self.channel.exchange_declare(
                    exchange=self.exchange_name,
                    exchange_type='topic',
                    durable=True,
                    auto_delete=False
                )
                
                self.logger.info(f"Successfully connected and declared exchange '{self.exchange_name}'")
                return True
            
            except Exception as e:
                self.logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                time.sleep(3)
        
        self.logger.error("Failed to connect to RabbitMQ after multiple attempts")
        return False

    def generate_news(self) -> Dict[str, str]:
        """
        Enhanced news generation with more comprehensive data
        """
        categories = ['Technology', 'Business', 'World', 'Science']
        category = random.choice(categories)
        
        keywords_map = {
            'Technology': ['innovation', 'tech', 'startup'],
            'Business': ['market', 'economy', 'investment'],
            'World': ['global', 'politics', 'international'],
            'Science': ['research', 'discovery', 'breakthrough']
        }
        
        return {
            "id": random.randint(1000, 9999),
            "title": f"Breaking News: {category} Breakthrough",
            "content": f"Latest developments and insights in the {category} sector",
            "category": category,
            "timestamp": datetime.now().isoformat(),
            "keywords": keywords_map.get(category, []) + [category.lower()]
        }

    def publish_news(self):
        """
        Advanced news publishing with enhanced error handling
        """
        try:
            # Ensure connection exists
            if not self.connection or self.connection.is_closed:
                if not self.connect():
                    return
            
            news_item = self.generate_news()
            routing_key = f"news.{news_item['category'].lower()}"
            
            try:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=json.dumps(news_item),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent
                        content_type='application/json'
                    )
                )
                self.logger.info(f"Published: {news_item['title']}")
            
            except Exception as publish_error:
                self.logger.error(f"Publish failed: {publish_error}")
        
        except Exception as e:
            self.logger.error(f"Unexpected error in publishing: {e}")

    def start_publishing(self, interval: int = 5):
        """
        Robust continuous publishing mechanism
        """
        self.logger.info("Starting news publishing...")
        try:
            while True:
                self.publish_news()
                time.sleep(interval)
        
        except KeyboardInterrupt:
            self.logger.info("Publishing stopped by user.")
        finally:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.info("Connection safely closed.")

def main():
    publisher = NewsPublisher()
    publisher.start_publishing(interval=5)

if __name__ == "__main__":
    main()
