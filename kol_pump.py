"""Slack bot integration for monitoring channels and processing Twitter URLs"""

import logging
import json
from pathlib import Path
from datetime import datetime
import zoneinfo
import re
import os
from typing import List, Dict
import asyncio
import sys

from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from apify_client import ApifyClient
from dotenv import load_dotenv

from alpha_filter import AlphaFilter
from error_handler import with_retry, APIError, log_error, RetryConfig
from category_mapping import CATEGORY_MAP, CATEGORY_KEYWORDS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KOLPump:
    def __init__(self, config: Dict):
        """Initialize KOLPump with configuration"""
        if not all(key in config for key in ['slack_bot_token', 'slack_app_token', 'apify_api_token']):
            raise ValueError("Missing required configuration keys")
            
        self.config = config
        self.slack_app = AsyncApp(token=config['slack_bot_token'])
        self.apify_client = ApifyClient(config['apify_api_token'])
        self.alpha_filter = AlphaFilter(config)
        self.handler = None
        
        # Setup Slack event handlers
        self.setup_slack_handlers()
        
    def setup_slack_handlers(self):
        """Setup Slack event handlers"""
        @self.slack_app.event("app_mention")
        async def handle_mention(event, say):
            logger.info(f"🎯 Received raw event: {json.dumps(event, indent=2)}")
            try:
                await self.handle_mention(event, say)
            except Exception as e:
                logger.error(f"Error handling mention: {str(e)}")
                await say(f"Error processing request: {str(e)}")

        @self.slack_app.event("message")
        async def handle_message(message, say):
            logger.info(f"📨 Received message event: {json.dumps(message, indent=2)}")

        # Add startup message
        logger.info("🚀 Slack event handlers registered")

    def _extract_twitter_urls_and_categories(self, text: str) -> List[Dict[str, str]]:
        """Extract Twitter URLs and their associated categories from text"""
        # Split message into lines
        lines = text.strip().split('\n')
        urls_with_categories = []
        
        for line in lines:
            # Skip the @mention line
            if line.startswith('<@'):
                continue
                
            # Match "Category - URL" format
            parts = line.split('-', 1)
            if len(parts) != 2:
                continue
                
            category = parts[0].strip()
            url_part = parts[1].strip()
            
            # Extract URL using existing pattern
            twitter_pattern = r'https?://(?:www\.)?(?:twitter\.com|x\.com)/\S+'
            urls = re.findall(twitter_pattern, url_part)
            
            if urls:
                urls_with_categories.append({
                    'category': category,
                    'url': urls[0].strip('<>')  # Clean URL
                })
                logger.info(f"Found Twitter URL for category '{category}': {urls[0]}")
            
        if not urls_with_categories:
            logger.info("No category-URL pairs found in message")
        else:
            logger.info(f"Extracted {len(urls_with_categories)} category-URL pairs")
            
        return urls_with_categories

    def _determine_column_id(self, category: str) -> str:
        """Determine column ID based on category name"""
        # First try exact match in category map
        for col_id, cat_name in CATEGORY_MAP.items():
            if category.lower() == cat_name.lower():
                return col_id
                
        # Then try matching keywords
        category_lower = category.lower()
        for cat_name, keywords in CATEGORY_KEYWORDS.items():
            if any(kw.lower() in category_lower for kw in keywords):
                # Find column ID for this category
                for col_id, map_cat in CATEGORY_MAP.items():
                    if cat_name.lower() == map_cat.lower():
                        return col_id
                        
        # Default to column 1 (KOL) if no match found
        logger.warning(f"No category mapping found for '{category}', using default column 1")
        return "1"

    async def handle_mention(self, event: Dict, say):
        """Handle mentions in Slack channels"""
        message_text = event.get('text', '')
        user_id = event.get('user', 'unknown')
        channel_id = event.get('channel', 'unknown')
        
        logger.info(f"Received mention from user <@{user_id}> in channel <#{channel_id}>")
        logger.info(f"Message: {message_text}")
        
        # Extract Twitter URLs with categories
        urls_with_categories = self._extract_twitter_urls_and_categories(message_text)
        if not urls_with_categories:
            await say("I couldn't find any properly formatted category-URL pairs. Please use the format:\n`Category - URL`\nFor example:\n`Polkadot - https://x.com/...`")
            return
            
        await say(f"I found {len(urls_with_categories)} tweet{'' if len(urls_with_categories) == 1 else 's'} to process! Let me handle {'it' if len(urls_with_categories) == 1 else 'them'} for you... 🚀")
        
        try:
            all_processed_tweets = []
            category_summary = []
            
            for item in urls_with_categories:
                category = item['category']
                url = item['url']
                column_id = self._determine_column_id(category)
                
                # Scrape tweet
                await say(f"Getting the tweet for {category}... 📥")
                tweets = await self._scrape_tweets([url])
                if not tweets:
                    await say(f"⚠️ Couldn't fetch the tweet for {category}. Skipping...")
                    continue
                    
                # Transform and add category
                transformed_tweets = self._transform_to_alpha_format(tweets)
                for tweet in transformed_tweets:
                    tweet['category'] = category
                
                # Add to alpha filter with specific column
                await self._add_to_alpha_filter(transformed_tweets, column_id)
                
                all_processed_tweets.extend(transformed_tweets)
                category_summary.append(f"• {category} → Column #{column_id} 📊")
            
            if all_processed_tweets:
                # Send detailed success message
                current_time = datetime.now(zoneinfo.ZoneInfo('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                success_message = (
                    f"All done! Here's what I processed:\n\n"
                    f"{chr(10).join(category_summary)}\n\n"
                    f"Total tweets processed: {len(all_processed_tweets)} ✅\n"
                    f"Processed at: {current_time} UTC 🕒\n\n"
                    f"The content will be included in the next analysis run. Is there anything else you need help with? 😊"
                )
                await say(success_message)
                
                # Log success
                logger.info(f"Successfully processed {len(all_processed_tweets)} tweets from user <@{user_id}>")
            else:
                await say("I wasn't able to process any of the tweets. Please check if the tweets are accessible and try again. 🤔")
            
        except Exception as e:
            error_msg = (
                f"I ran into an issue while processing your request:\n"
                f"```{str(e)}```\n"
                f"Could you try again? If the problem persists, there might be an issue with accessing the tweets or our services. 🔧"
            )
            logger.error(f"Error processing tweets: {str(e)}")
            await say(error_msg)

    @with_retry(RetryConfig(max_retries=3, base_delay=2.0))
    async def _scrape_tweets(self, urls: List[str]) -> List[Dict]:
        """Scrape tweets using Apify's Tweet Scraper"""
        try:
            # Clean URLs and extract tweet IDs
            clean_urls = [url.strip('<>') for url in urls]
            tweet_ids = []
            for url in clean_urls:
                match = re.search(r'/status/(\d+)', url)
                if match:
                    tweet_ids.append(match.group(1))
            
            if not tweet_ids:
                raise APIError("No valid tweet IDs found in URLs")
                
            logger.info(f"Scraping tweets from URLs: {clean_urls}")
            logger.info(f"Using tweet IDs: {tweet_ids}")
            
            # Start Apify Twitter Scraper with correct parameters
            run = await asyncio.to_thread(
                lambda: self.apify_client.actor('kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest').call(
                    run_input={
                        "tweetIDs": tweet_ids,  # Correct parameter name is tweetIDs
                        "maxItems": len(tweet_ids),  # One item per tweet ID
                        "queryType": "Latest",  # Get most recent version
                        "filter:safe": True  # Exclude NSFW content
                    }
                )
            )
            
            if not run or 'defaultDatasetId' not in run:
                raise APIError("Failed to start Apify run or get dataset ID")
                
            logger.info(f"Started Apify run with ID: {run.get('id')}")
            
            # Wait for the run to finish and get results
            logger.info("Waiting for dataset to be ready...")
            
            # Get dataset items
            dataset_items = await asyncio.to_thread(
                lambda: self.apify_client.dataset(run['defaultDatasetId']).list_items().items
            )
            
            # Log raw data for debugging
            logger.debug(f"Raw dataset items: {json.dumps(dataset_items, indent=2)}")
            
            # Filter and transform the tweets
            valid_items = []
            for item in dataset_items:
                if not isinstance(item, dict):
                    continue
                    
                # Extract tweet data
                tweet_data = {
                    'text': item.get('text', ''),
                    'id': item.get('id', ''),
                    'url': item.get('url', ''),
                    'author': item.get('author', {}).get('userName', '') if item.get('author') else '',
                    'quoted_content': '',
                    'reposted_content': ''
                }
                
                # Get quoted tweet content if any
                if item.get('quoted_tweet'):
                    tweet_data['quoted_content'] = item['quoted_tweet'].get('text', '')
                    
                # Get reposted tweet content if any
                if item.get('retweeted_tweet'):
                    tweet_data['reposted_content'] = item['retweeted_tweet'].get('text', '')
                
                # Validate tweet data
                if tweet_data['text'] and tweet_data['author'] and tweet_data['id']:
                    valid_items.append(tweet_data)
            
            if not valid_items:
                logger.error(f"No valid tweets found. Raw data: {json.dumps(dataset_items, indent=2)}")
                raise APIError("No valid tweets found in the dataset")
            
            logger.info(f"Successfully scraped {len(valid_items)} valid tweets from {len(urls)} URLs")
            return valid_items
            
        except Exception as e:
            logger.error(f"Error scraping tweets: {str(e)}")
            raise APIError(f"Failed to scrape tweets: {str(e)}")
            
    def _transform_to_alpha_format(self, apify_tweets: List[Dict]) -> List[Dict]:
        """Transform Apify tweet format to alpha filter format"""
        transformed = []
        date_str = datetime.now(zoneinfo.ZoneInfo("UTC")).strftime('%Y%m%d')
        
        for tweet in apify_tweets:
            try:
                transformed.append({
                    "tweet": tweet['text'],
                    "author": tweet['author'],
                    "url": tweet['url'],
                    "quoted_content": tweet['quoted_content'],
                    "reposted_content": tweet['reposted_content'],
                    "content_id": str(tweet['id']),
                    "category": "KOL",  # Default category for KOL-sourced tweets
                    "processed_date": date_str
                })
                
                logger.debug(f"Successfully transformed tweet {tweet['id']}")
                
            except KeyError as e:
                logger.error(f"Error transforming tweet: {str(e)}")
                continue
                
        return transformed
        
    async def _add_to_alpha_filter(self, tweets: List[Dict], column_id: str = "1"):
        """Add transformed tweets to alpha filter with specific column"""
        if not tweets:
            logger.warning("No tweets to add to alpha filter")
            return
            
        date_str = datetime.now(zoneinfo.ZoneInfo("UTC")).strftime('%Y%m%d')
        column_file = self.alpha_filter.filtered_dir / f'column_{column_id}.json'
        
        try:
            # Ensure directory exists
            column_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Read existing data or create new structure
            if column_file.exists():
                logger.info(f"Reading existing data from {column_file}")
                with open(column_file, 'r') as f:
                    existing_data = json.load(f)
                    existing_tweets = existing_data.get('tweets', [])
            else:
                logger.info(f"Creating new data file at {column_file}")
                existing_tweets = []
                
            # Add new tweets
            existing_tweets.extend(tweets)
            
            # Update file
            file_data = {
                'tweets': existing_tweets,
                'metadata': {
                    'total_tweets': len(existing_tweets),
                    'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat(),
                    'last_processed_date': date_str,
                    'processing_dates': [date_str]
                }
            }
            
            # Log before writing
            logger.info(f"Writing {len(tweets)} new tweets to {column_file}")
            logger.info(f"Total tweets after update: {len(existing_tweets)}")
            
            # Use atomic write
            with open(column_file, 'w') as f:
                json.dump(file_data, f, indent=2)
            
            logger.info(f"Successfully updated {column_file}")
            
        except Exception as e:
            logger.error(f"Error adding tweets to alpha filter: {str(e)}")
            raise
            
    async def start(self):
        """Start the Slack bot"""
        try:
            logger.info("Starting bot...")
            # Initialize socket mode handler
            self.handler = AsyncSocketModeHandler(
                app=self.slack_app,
                app_token=self.config['slack_app_token']
            )
            
            # Log the bot's configuration
            logger.info(f"Bot configured with:")
            logger.info(f"- Socket Mode: Enabled")
            logger.info(f"- Bot User ID: {await self._get_bot_user_id()}")
            
            await self.handler.start_async()
            logger.info("✅ Bot successfully started and listening for events!")
            
        except Exception as e:
            logger.error(f"Error starting bot: {str(e)}")
            raise

    async def _get_bot_user_id(self):
        """Get the bot's user ID"""
        try:
            auth_response = await self.slack_app.client.auth_test()
            return auth_response["user_id"]
        except Exception as e:
            logger.error(f"Error getting bot user ID: {str(e)}")
            return "unknown"

    async def stop(self):
        """Stop the Slack bot"""
        try:
            logger.info("Stopping bot...")
            if self.handler:
                await self.handler.disconnect()  # Use disconnect() instead of stop()
            await self.slack_app.stop()
            logger.info("Bot stopped successfully")
        except Exception as e:
            logger.error(f"Error stopping bot: {str(e)}")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    
    # Setup configuration
    config = {
        'slack_bot_token': os.getenv('SLACK_BOT_TOKEN'),
        'slack_app_token': os.getenv('SLACK_APP_TOKEN'),
        'apify_api_token': os.getenv('APIFY_API_TOKEN'),
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY')
    }
    
    # Create and run KOLPump
    try:
        kol_pump = KOLPump(config)
        asyncio.run(kol_pump.start())
    except KeyboardInterrupt:
        logger.info("Shutting down bot...")
        asyncio.run(kol_pump.stop())
    except Exception as e:
        logger.error(f"Bot error: {str(e)}")
        sys.exit(1)
