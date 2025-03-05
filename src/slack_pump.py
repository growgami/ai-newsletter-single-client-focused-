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

from utils.error_handler import with_retry, APIError, RetryConfig
from category_mapping import CATEGORY, CATEGORY_KEYWORDS
from processors.content_filter import ContentFilter

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SLACKPump:
    def __init__(self, config):
        """Initialize SLACKPump with configuration"""
        if not all(key in config for key in ['slack_bot_token', 'slack_app_token', 'apify_api_token']):
            raise ValueError("Missing required configuration keys")
            
        self.config = config
        self.slack_app = AsyncApp(token=config['slack_bot_token'])
        self.apify_client = ApifyClient(config['apify_api_token'])
        self.content_filter = ContentFilter(config)
        self.handler = None
        
        # Setup Slack event handlers
        self.setup_slack_handlers()
        
    def setup_slack_handlers(self):
        """Setup Slack event handlers"""
        @self.slack_app.event("message")
        async def handle_message(message, say):
            logger.info(f"📨 Received message event: {json.dumps(message, indent=2)}")
            try:
                await self.handle_message(message, say)
            except Exception as e:
                logger.error(f"Error handling message: {str(e)}")
                await say(f"❌ Error processing message: {str(e)}")

        # Add startup message
        logger.info("🚀 Slack event handlers registered")

    def _extract_twitter_urls(self, text: str) -> List[str]:
        """Extract Twitter URLs from text"""
        if not text:
            return []
            
        # Extract URL using pattern
        twitter_pattern = r'https?://(?:www\.)?(?:twitter\.com|x\.com)/\S+'
        urls = re.findall(twitter_pattern, text)
        
        # Clean URLs
        cleaned_urls = [url.strip('<>') for url in urls]
        
        if cleaned_urls:
            logger.info(f"Found {len(cleaned_urls)} Twitter URLs")
        
        return cleaned_urls

    async def handle_message(self, message: Dict, say):
        """Handle messages in Slack channels"""
        message_text = message.get('text', '').lower()
        
        # Check if message contains any category keywords
        if not any(keyword.lower() in message_text for keyword in CATEGORY_KEYWORDS):
            return
            
        # Extract Twitter URLs
        urls = self._extract_twitter_urls(message_text)
        if not urls:
            return
            
        logger.info(f"Processing {len(urls)} URLs from message containing {CATEGORY} keywords")
        await say(f"🔍 Found {len(urls)} tweet{'' if len(urls) == 1 else 's'} related to {CATEGORY}. Processing...")
        
        try:
            # Scrape tweets
            tweets = await self._scrape_tweets(urls)
            if not tweets:
                await say("⚠️ No tweets could be retrieved from the provided URLs.")
                return
                
            # Transform to content filter format
            transformed_tweets = await self._transform_to_alpha_format(tweets)
            
            # Add to content filter output
            await self._add_to_alpha_output(transformed_tweets)
            
            # Send success message with details
            current_time = datetime.now(zoneinfo.ZoneInfo('UTC')).strftime('%H:%M:%S UTC')
            
            # Create detailed summary
            tweet_summaries = []
            for tweet in transformed_tweets:
                summary = (
                    f"• *{tweet['attribution']}* {tweet['content']}"
                )
                tweet_summaries.append(summary)
            
            success_message = (
                f"✅ Successfully processed {len(transformed_tweets)} tweet{'' if len(transformed_tweets) == 1 else 's'}!\n\n"
                f"*Summary:*\n"
                f"• Category: {CATEGORY}\n"
                f"• Time: {current_time}\n\n"
                f"{chr(10).join(tweet_summaries)}"
            )
            await say(success_message)
            
        except Exception as e:
            error_message = (
                f"❌ Error processing tweets: {str(e)}\n"
                f"Please try again or contact support if the issue persists."
            )
            logger.error(f"Error processing message: {str(e)}")
            await say(error_message)

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
                    'created_at': item.get('createdAt', ''),
                    'quoted_tweet': item.get('quoted_tweet', {}),
                    'retweeted_tweet': item.get('retweeted_tweet', {})
                }
                
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

    async def _transform_to_alpha_format(self, apify_tweets: List[Dict]) -> List[Dict]:
        """Transform Apify tweet format to content filter format"""
        transformed = []
        
        for tweet in apify_tweets:
            # Extract tweet content
            tweet_text = tweet['text']
            author = tweet['author']
            url = tweet['url']
            quoted_text = tweet.get('quoted_tweet', {}).get('text', '')
            reposted_text = tweet.get('retweeted_tweet', {}).get('text', '')
            
            # Use content filter's extract_summary method
            result = await self.content_filter._extract_summary(
                tweet_text=tweet_text,
                quoted_text=quoted_text,
                reposted_text=reposted_text,
                author=author
            )
            
            if result:
                transformed.append({
                    'attribution': result['attribution'],
                    'content': result['content'],
                    'url': url,
                    'original_date': tweet.get('created_at', ''),
                    'from_slack': True  # Add this field to identify tweets from Slack
                })
            else:
                logger.warning(f"Failed to generate summary for tweet: {url}")
        
        return transformed

    async def _add_to_alpha_output(self, new_tweets: List[Dict]):
        """Add transformed tweets to content filter output"""
        if not new_tweets:
            return
            
        output_file = Path('data/filtered/content_filtered/combined_filtered.json')
        
        try:
            # Read existing data or create new structure
            if output_file.exists():
                with open(output_file, 'r') as f:
                    existing_data = json.load(f)
            else:
                existing_data = {
                    CATEGORY: {
                        'tweets': []
                    }
                }
            
            # Add new tweets
            existing_data[CATEGORY]['tweets'].extend(new_tweets)
            
            # Save atomically
            temp_file = output_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(existing_data, f, indent=2)
            temp_file.replace(output_file)
            
            logger.info(f"Added {len(new_tweets)} new tweets to content filter output")
            
        except Exception as e:
            logger.error(f"Error adding tweets to content filter output: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()

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
            logger.info(f"- Monitoring for category: {CATEGORY}")
            logger.info(f"- Keywords: {', '.join(CATEGORY_KEYWORDS)}")
            
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
                await self.handler.disconnect()
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
    
    # Create and run SLACKPump
    try:
        slack_pump = SLACKPump(config)
        asyncio.run(slack_pump.start())
    except KeyboardInterrupt:
        logger.info("Shutting down bot...")
        asyncio.run(slack_pump.stop())
    except Exception as e:
        logger.error(f"Bot error: {str(e)}")
        sys.exit(1)
