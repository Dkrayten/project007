import pika
import json
import random
import time
import logging
from datetime import datetime
from typing import List, Dict

class NewsGenerator:
    def __init__(self):
        # äâãøú îéìåú îôúç ñôöéôéåú ìëì ÷èâåøéä
        self.category_keywords = {
            "Technology": [
                "innovation", "artificial intelligence", 
                "machine learning", "tech breakthrough", 
                "digital transformation"
            ],
            "Business": [
                "market trends", "startup", "investment", 
                "economic growth", "corporate strategy"
            ],
            "World": [
                "global politics", "international relations", 
                "diplomacy", "geopolitical", "world affairs"
            ],
            "Science": [
                "research", "discovery", "scientific breakthrough", 
                "innovation", "academic research"
            ]
        }
        
        # äâãøú ÷èâåøéåú
        self.categories = list(self.category_keywords.keys())
        
        # äâãøú ìåâø
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def _generate_keywords(self, category: str) -> List[str]:
        """
        Generate dynamic keywords based on the news category
        
        Args:
            category (str): News category
        
        Returns:
            List[str]: List of relevant keywords
        """
        # áçéøú îéìåú îôúç øìååðèéåú ì÷èâåøéä
        category_specific_keywords = self.category_keywords.get(category, [])
        
        # áçéøú 2-3 îéìåú îôúç øðãåîìéåú
        return random.sample(category_specific_keywords, min(3, len(category_specific_keywords)))
    
    def generate_news_item(self) -> Dict[str, any]:
        """
        Generate a comprehensive news item with dynamic content
        
        Returns:
            Dict[str, any]: News item with all required fields
        """
        # áçéøú ÷èâåøéä øðãåîìéú
        category = random.choice(self.categories)
        
        news_item = {
            "title": self._generate_title(category),
            "content": self._generate_content(category),
            "category": category,
            "timestamp": datetime.now().isoformat(),
            "keywords": self._generate_keywords(category)
        }
        return news_item
    
    def _generate_title(self, category: str) -> str:
        """
        Generate a title specific to the category
        
        Args:
            category (str): News category
        
        Returns:
            str: Generated news title
        """
        title_templates = {
            "Technology": [
                "Breaking: {} Revolutionizes Tech Industry",
                "New AI Breakthrough in {}",
                "Tech Giant Unveils Innovative Solution"
            ],
            "Business": [
                "Market Shift: {} Disrupts Economic Landscape",
                "Startup Secures Major Investment in {}",
                "Global Business Trends Emerging"
            ],
            "World": [
                "Diplomatic Breakthrough in {}",
                "Global Leaders Discuss Critical Issues",
                "International Relations Evolve"
            ],
            "Science": [
                "Scientific Breakthrough in {}",
                "Researchers Uncover Groundbreaking Discovery",
                "New Research Challenges Existing Theories"
            ]
        }
        
        return random.choice(title_templates.get(category, [])).format(category)
    
    def _generate_content(self, category: str) -> str:
        """
        Generate content based on the category
        
        Args:
            category (str): News category
        
        Returns:
            str: Generated news content
        """
        content_templates = {
            "Technology": "Latest advancements in {} are pushing the boundaries of innovation...",
            "Business": "The {} sector is experiencing significant transformations...",
            "World": "Recent developments in {} highlight the complex nature of global politics...",
            "Science": "Groundbreaking research in {} promises to revolutionize our understanding..."
        }
        
        return content_templates.get(category, "").format(category)
    
    def publish_news(self):
        """
        Publish news with dynamic routing key based on category
        """
        try:
            # RabbitMQ connection setup
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost')
            )
            channel = connection.channel()
            
            # Declare exchange
            channel.exchange_declare(
                exchange='news_exchange', 
                exchange_type='topic'
            )
            print("Exchange 'news_exchange' declared successfully!")
            # Generate and publish news
            news_item = self.generate_news_item()
            
            # Dynamic routing key based on category
            routing_key = f"news.{news_item['category'].lower()}"
            
            channel.basic_publish(
                exchange='news_exchange',
                routing_key=routing_key,
                body=json.dumps(news_item),
                properties=pika.BasicProperties(
                    delivery_mode=2  # Persistent message
                )
            )
            
            self.logger.info(f"Published news: {news_item['title']} with routing key: {routing_key}")
        
        except Exception as e:
            self.logger.error(f"Failed to publish news: {e}")
        finally:
            # Ensure connection closure
            if 'connection' in locals() and not connection.is_closed:
                connection.close()

def main():
    """
    Main execution point for the News Generator
    Publishes news every 5-10 seconds
    """
    generator = NewsGenerator()
    
    try:
        while True:
            generator.publish_news()
            time.sleep(random.uniform(5, 10))
    
    except KeyboardInterrupt:
        print("News generation stopped.")

if __name__ == "__main__":
    main()
