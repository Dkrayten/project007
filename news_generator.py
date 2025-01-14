
import pika
import json
import random
import time
from datetime import datetime

class NewsGenerator:
    def __init__(self):
        # RabbitMQ Connection Parameters
        self.connection_params = pika.ConnectionParameters('localhost')
        self.connection = None
        self.channel = None

        # News Categories
        self.categories = [
            "Technology",
            "Business",
            "World",
            "Science"
        ]

    def generate_news_item(self):
        """Generate a simulated news item"""
        return {
            "title": self._generate_title(),
            "content": self._generate_content(),
            "category": random.choice(self.categories),
            "timestamp": datetime.now().isoformat(),
            "keywords": self._generate_keywords()
        }

    def _generate_title(self):
        """Generate a random news title"""
        title_templates = [
            "Breaking: {} Revolutionizes {}",
            "New Study Reveals Surprising {} in {}",
            "Major Breakthrough in {} Sector"
        ]
        return random.choice(title_templates).format(
            random.choice(self.categories),
            random.choice(["Industry", "Research", "Technology"])
        )

    def _generate_content(self):
        """Generate lorem ipsum style content"""
        # Placeholder for more sophisticated content generation
        return f"Detailed report about recent developments in {random.choice(self.categories)} sector."

    def _generate_keywords(self):
        """Generate relevant keywords"""
        return [
            random.choice(["innovation", "breakthrough", "research", "development", "technology"]),
            random.choice(self.categories.lower())
        ]

    def connect_to_rabbitmq(self):
        """Establish connection to RabbitMQ"""
        self.connection = pika.BlockingConnection(self.connection_params)
        self.channel = self.connection.channel()

        # Declare a topic exchange
        self.channel.exchange_declare(exchange='news_exchange', exchange_type='topic')

    def publish_news(self):
        """Publish news items to RabbitMQ"""
        while True:
            news_item = self.generate_news_item()

            # Convert to JSON
            message = json.dumps(news_item)

            # Publish to RabbitMQ
            self.channel.basic_publish(
                exchange='news_exchange',
                routing_key='news.generated',
                body=message
            )

            print(f"Published news: {news_item['title']}")

            # Random interval between 5-10 seconds
            time.sleep(random.uniform(5, 10))

    def start(self):
        """Start the news generation process"""
        try:
            self.connect_to_rabbitmq()
            self.publish_news()
        except Exception as e:
            print(f"Error in news generation: {e}")
        finally:
            if self.connection:
                self.connection.close()

if __name__ == "__main__":
    generator = NewsGenerator()
    generator.start()