import pika
import json
import random
import logging
import socket
import time
from datetime import datetime
from typing import Dict, Any, Optional

class RobustNewsPublisher:
    def __init__(self, 
                 host: str = 'localhost', 
                 port: int = 5672, 
                 max_retries: int = 10):
        """
        Comprehensive RabbitMQ News Publisher with maximum reliability
        
        Args:
            host (str): RabbitMQ server host
            port (int): RabbitMQ server port
            max_retries (int): Maximum connection retry attempts
        """
        # Logging configuration
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='news_publisher.log'
        )
        self.logger = logging.getLogger(__name__)
        
        # Connection parameters
        self.host = host
        self.port = port
        self.max_retries = max_retries
        
        # Validation checks
        self._validate_network_prerequisites()
    
    def _validate_network_prerequisites(self):
        """
        Perform comprehensive pre-connection network validations
        """
        try:
            # Check network socket
            test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            test_socket.settimeout(5)
            result = test_socket.connect_ex((self.host, self.port))
            
            if result != 0:
                raise ConnectionError(f"Port {self.port} is not accessible")
            
            test_socket.close()
            self.logger.info(f"Network prerequisites validated for {self.host}:{self.port}")
        
        except Exception as e:
            self.logger.critical(f"Network validation failed: {e}")
            raise
    
    def _create_connection_parameters(self) -> pika.ConnectionParameters:
        """
        Create highly configurable and secure connection parameters
        
        Returns:
            pika.ConnectionParameters: Robust connection configuration
        """
        credentials = pika.PlainCredentials(
            username='guest', 
            password='guest',
            erase_on_connect=True  # Security enhancement
        )
        
        return pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host='/',
            credentials=credentials,
            connection_attempts=self.max_retries,
            socket_timeout=10,
            heartbeat=600,  # 10-minute heartbeat
            retry_delay=3,
            blocked_connection_timeout=300  # 5-minute block timeout
        )
    
    def establish_connection(self) -> Optional[pika.BlockingConnection]:
        """
        Establish a connection with multi-layered error handling
        
        Returns:
            Optional[pika.BlockingConnection]: Verified RabbitMQ connection
        """
        connection = None
        for attempt in range(self.max_retries):
            try:
                # Create connection parameters
                connection_params = self._create_connection_parameters()
                
                # Attempt connection
                connection = pika.BlockingConnection(connection_params)
                
                # Verify connection
                channel = connection.channel()
                channel.basic_qos(prefetch_count=1)
                
                # Declare exchange with comprehensive configuration
                channel.exchange_declare(
                    exchange='news_exchange',
                    exchange_type='topic',
                    durable=True,
                    auto_delete=False
                )
                
                self.logger.info(f"Successfully established connection (Attempt {attempt + 1})")
                return connection
            
            except (
                pika.exceptions.AMQPConnectionError, 
                pika.exceptions.AMQPChannelError,
                socket.error
            ) as e:
                self.logger.warning(
                    f"Connection attempt {attempt + 1} failed: {e}. "
                    f"Retrying in 3 seconds..."
                )
                time.sleep(3)
            
            except Exception as unexpected_error:
                self.logger.critical(f"Unexpected connection error: {unexpected_error}")
                break
        
        self.logger.error("Failed to establish RabbitMQ connection after multiple attempts")
        return None
    
    def generate_news_item(self) -> Dict[str, Any]:
        """
        Generate a comprehensive and realistic news item
        
        Returns:
            Dict[str, Any]: Detailed news item
        """
        categories = ['Technology', 'Business', 'Science', 'World']
        category = random.choice(categories)
        
        return {
            'id': random.randint(1000, 9999),
            'title': f"Breaking News: {category} Breakthrough",
            'content': f"Detailed report on latest developments in {category}",
            'category': category,
            'timestamp': datetime.now().isoformat(),
            'keywords': [
                random.choice(['innovation', 'research', 'discovery']),
                category.lower()
            ],
            'source': 'NewsGenerator'
        }
    
    def publish_message(self, message: Dict[str, Any]):
        """
        Publish message with guaranteed delivery
        
        Args:
            message (Dict[str, Any]): News item to publish
        """
        connection = None
        try:
            # Establish connection
            connection = self.establish_connection()
            
            if not connection:
                raise ConnectionError("Could not establish RabbitMQ connection")
            
            # Create channel
            channel = connection.channel()
            
            # Serialize message
            message_body = json.dumps(message).encode('utf-8')
            
            # Publish with confirmation
            channel.confirm_delivery()
            
            result = channel.basic_publish(
                exchange='news_exchange',
                routing_key=f"news.{message.get('category', 'general')}",
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Persistent message
                    content_type='application/json',
                    timestamp=int(datetime.now().timestamp())
                )
            )
            
            if result:
                self.logger.info(f"Successfully published: {message.get('title')}")
            else:
                self.logger.warning("Message publication not confirmed")
        
        except Exception as e:
            self.logger.error(f"Publication failed: {e}")
        
        finally:
            # Ensure connection closure
            if connection and not connection.is_closed:
                connection.close()
    
    def start_news_generation(self, interval: float = 5):
        """
        Continuously generate and publish news with error resilience
        
        Args:
            interval (float): Time between news generations
        """
        self.logger.info("Starting robust news generation...")
        
        try:
            while True:
                try:
                    news_item = self.generate_news_item()
                    self.publish_message(news_item)
                except Exception as item_error:
                    self.logger.error(f"News generation error: {item_error}")
                
                time.sleep(interval)
        
        except KeyboardInterrupt:
            self.logger.info("News generation stopped by user")

def main():
    publisher = RobustNewsPublisher()
    publisher.start_news_generation()

if __name__ == "__main__":
    main()
