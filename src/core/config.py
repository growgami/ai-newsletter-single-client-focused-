"""
Configuration settings for the Twitter List Scraper

This file centralizes all configuration by loading from environment variables.
The .env file should contain all required settings.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

# Twitter API credentials
API_KEY = os.getenv('TWITTER_API_KEY', '')
API_SECRET = os.getenv('TWITTER_API_SECRET', '')
BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', '')

# Application settings
DAYS_TO_SCRAPE = int(os.getenv('DAYS_TO_SCRAPE', '1'))
MAX_TWEETS_PER_USER = int(os.getenv('MAX_TWEETS_PER_USER', '100'))
RATE_LIMIT_WAIT = int(os.getenv('RATE_LIMIT_WAIT', '60'))
MAX_RETRY_ATTEMPTS = int(os.getenv('MAX_RETRY_ATTEMPTS', '5'))

# Output settings
OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'data/raw')
CATEGORIES_CONFIG = os.getenv('CATEGORIES_CONFIG', 'categories.json')

# Worker settings
MAX_SCRAPER_WORKERS = int(os.getenv('MAX_SCRAPER_WORKERS', '5'))
CATEGORY_WORKERS = int(os.getenv('CATEGORY_WORKERS', '1'))

# Category Configuration
# Note: This can be defined in categories.json instead (recommended), but you can also define it here
# Format: {"Category_Name": ["list_url1", "list_url2"], ...}
# Example:
# CATEGORIES = {
#     "Polkadot": [
#         "https://twitter.com/i/lists/example_dot_creators",
#         "https://twitter.com/i/lists/example_dot_accounts"
#     ],
#     "Ethereum": [
#         "https://twitter.com/i/lists/example_eth_devs",
#         "https://twitter.com/i/lists/example_eth_news"
#     ]
# }
# CATEGORIES = {}  # Uncomment and populate if you want to define categories in code 