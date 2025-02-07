import logging
import json
from pathlib import Path
import asyncio
from datetime import datetime
from error_handler import with_retry, RetryConfig, log_error

logger = logging.getLogger(__name__)

class TweetScraper:
    def __init__(self, page, config):
        self.page = page
        self.config = config
        self.columns = {}
        self.latest_tweets = {}
        
        # File paths
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        
        # Get today's date for file organization
        self.today = datetime.now().strftime('%Y%m%d')
        self.today_dir = self.raw_dir / self.today
        
        # Create directories if they don't exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.today_dir.mkdir(parents=True, exist_ok=True)
        
        # Latest tweets file is in data root (not in raw)
        self.latest_tweets_file = self.data_dir / 'latest_tweets.json'
        
        # Rate limiting and error handling
        self.last_scrape_time = {}  # Track last scrape time per column
        self.error_count = {}       # Track consecutive errors per column
        self.min_scrape_interval = 0.1  # Minimum time between scrapes (100ms)
        self.max_backoff = 5.0      # Maximum backoff time in seconds
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        self.retry_config = RetryConfig(
            max_retries=3,
            base_delay=self.min_scrape_interval,
            max_delay=self.max_backoff
        )
        
    async def identify_columns(self):
        """Identify all columns in the TweetDeck"""
        try:
            logger.info("Searching for TweetDeck columns...")
            
            # Try multiple times to find columns with a short delay
            max_attempts = 3
            for attempt in range(max_attempts):
                columns = await self.page.query_selector_all('div[data-testid="multi-column-layout-column-content"]')
                column_count = len(columns)
                
                if column_count > 0:
                    break
                    
                if attempt < max_attempts - 1:
                    logger.info(f"No columns found on attempt {attempt + 1}, waiting briefly...")
                    await asyncio.sleep(2)
            
            if column_count == 0:
                logger.warning("No columns found after all attempts! Checking page content...")
                body_content = await self.page.content()
                logger.info(f"Page content length: {len(body_content)} characters")
                return False
                
            logger.info(f"Found {column_count} columns in TweetDeck")
            
            # Process each column
            for index, column in enumerate(columns):
                column_id = str(index)
                title_element = await column.query_selector('div[data-testid="columnHeader"]')
                column_title = await title_element.inner_text() if title_element else str(index)
                
                # Store column info with file in today's directory
                self.columns[column_id] = {
                    'title': column_title,
                    'file': self.today_dir / f"column_{column_id}.json"
                }
                
                logger.info(f"Column {index + 1}/{column_count}: {column_title} ({column_id})")
            
            logger.info(f"Successfully identified {len(self.columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Error identifying columns: {str(e)}")
            return False
            
    def load_latest_tweets(self):
        """Load the latest tweet IDs from file"""
        try:
            if self.latest_tweets_file.exists():
                with open(self.latest_tweets_file, 'r') as f:
                    self.latest_tweets = json.load(f)
                logger.info(f"Loaded {len(self.latest_tweets)} latest tweet IDs")
        except Exception as e:
            logger.error(f"Error loading latest tweets: {str(e)}")
            
    def save_latest_tweets(self):
        """Save the latest tweet IDs to file"""
        try:
            with open(self.latest_tweets_file, 'w') as f:
                json.dump(self.latest_tweets, f, indent=2)
            logger.info("Saved latest tweet IDs")
        except Exception as e:
            logger.error(f"Error saving latest tweets: {str(e)}")
            
    @with_retry(RetryConfig(max_retries=3, base_delay=0.1, max_delay=5.0))
    async def get_column_tweets(self, column_id, is_monitoring=False):
        """Get all tweets from a specific column with rate limiting"""
        try:
            # Check rate limiting
            current_time = asyncio.get_event_loop().time()
            if is_monitoring and column_id in self.last_scrape_time:
                time_since_last = current_time - self.last_scrape_time[column_id]
                if time_since_last < self.min_scrape_interval:
                    # Too soon, skip this check
                    return []
            
            # Update last scrape time
            self.last_scrape_time[column_id] = current_time
            
            # Get tweets with existing logic
            tweets = await self._get_column_tweets_internal(column_id, is_monitoring)
            
            # Reset error count on success
            self.error_count[column_id] = 0
            
            return tweets
            
        except Exception as e:
            logger.error(f"Error getting tweets from column {column_id}: {str(e)}", exc_info=True)
            raise
            
    async def _get_column_tweets_internal(self, column_id, is_monitoring=False):
        """Internal method with the original get_column_tweets logic"""
        try:
            column = self.columns.get(column_id)
            if not column:
                return []
                
            # Get all tweet elements in the column
            index = int(column_id)
            columns = await self.page.query_selector_all('div[data-testid="multi-column-layout-column-content"]')
            if index >= len(columns):
                logger.error(f"Column index {index} out of range")
                return []
                
            column_element = columns[index]
            
            # Wait for tweets to load with retries
            max_attempts = 3
            tweets = []
            for attempt in range(max_attempts):
                # First wait for the timeline to be present
                timeline = await column_element.query_selector('div[data-testid="cellInnerDiv"]')
                if timeline:
                    # Wait a bit for tweets to fully load
                    await asyncio.sleep(1)
                    # Get all tweets directly
                    tweets = await column_element.query_selector_all('article[data-testid="tweet"]')
                    if len(tweets) > 0:
                        break
                        
                if attempt < max_attempts - 1 and not is_monitoring:
                    logger.info(f"No tweets found in column {column['title']} on attempt {attempt + 1}, waiting...")
                    await asyncio.sleep(2)  # Wait 2 seconds between attempts
            
            if not is_monitoring:
                logger.info(f"Found {len(tweets)} tweets in column {column['title']}")
            
            # For monitoring, only process first tweet if we have latest ID
            if is_monitoring and self.latest_tweets.get(column_id) and len(tweets) > 0:
                latest_id = self.latest_tweets[column_id]
                first_tweet = tweets[0]
                
                # Get tweet ID
                tweet_link = await first_tweet.query_selector('a[href*="/status/"]')
                if not tweet_link:
                    return []
                    
                href = await tweet_link.get_attribute('href')
                tweet_id = href.split('/status/')[-1]
                
                # If ID matches latest, no new tweets
                if tweet_id == latest_id:
                    return []
                    
                # Only process first tweet during monitoring
                tweets = [first_tweet]
                
            tweet_data = []
            for tweet in tweets:
                try:
                    # Check for repost indicator
                    social_context = await tweet.evaluate("""
                        tweet => {
                            const context = tweet.parentElement?.querySelector('[data-testid="socialContext"]');
                            return context ? context.textContent : null;
                        }
                    """)
                    is_repost = social_context and "reposted" in social_context.lower() if social_context else False
                    original_author = social_context.split(' reposted')[0].strip() if is_repost else ''
                    
                    # Check for quote tweet structure
                    text_elements = await tweet.query_selector_all('[data-testid="tweetText"]')
                    user_elements = await tweet.query_selector_all('[data-testid="User-Name"]')
                    is_quote_retweet = not is_repost and len(text_elements) == 2 and len(user_elements) == 2
                    
                    quoted_content = None
                    reposted_content = None
                    
                    if is_quote_retweet:
                        # Get quoted content
                        quoted_text = await text_elements[1].inner_text()
                        quoted_handle = await user_elements[1].evaluate("""
                            el => Array.from(el.querySelectorAll('span'))
                                .find(span => span.textContent.includes('@'))?.textContent.trim().replace(/^@/, '') || ''
                        """)
                        quoted_content = {
                            'text': quoted_text,
                            'authorHandle': quoted_handle
                        }
                    elif is_repost:
                        # Get reposted content
                        reposted_text = await text_elements[0].inner_text() if text_elements else ''
                        reposted_handle = await user_elements[0].evaluate("""
                            el => Array.from(el.querySelectorAll('span'))
                                .find(span => span.textContent.includes('@'))?.textContent.trim().replace(/^@/, '') || ''
                        """)
                        reposted_content = {
                            'text': reposted_text,
                            'authorHandle': reposted_handle
                        }
                    
                    # Get tweet ID and basic info
                    tweet_link = await tweet.query_selector('a[href*="/status/"]')
                    if tweet_link:
                        href = await tweet_link.get_attribute('href')
                        tweet_id = href.split('/status/')[-1]
                        
                        # Get tweet text
                        text_element = await tweet.query_selector('[data-testid="tweetText"]')
                        text = await text_element.inner_text() if text_element else ""
                        
                        # Get author info
                        author_element = await tweet.query_selector('[data-testid="User-Name"]')
                        author_handle = await author_element.evaluate("""
                            el => Array.from(el.querySelectorAll('span'))
                                .find(span => span.textContent.includes('@'))?.textContent.trim().replace(/^@/, '') || ''
                        """) if author_element else ""
                        
                        tweet_data.append({
                            'id': tweet_id,
                            'text': text,
                            'authorHandle': author_handle,
                            'url': f"https://twitter.com/i/status/{tweet_id}",
                            'isRepost': is_repost,
                            'isQuoteRetweet': is_quote_retweet,
                            'quotedContent': quoted_content,
                            'repostedContent': reposted_content,
                            'originalAuthor': original_author,
                            'column': column['title']
                        })
                except Exception as e:
                    logger.error(f"Error processing tweet in column {column_id}: {str(e)}")
                    continue
            
            if tweet_data and (not is_monitoring or len(tweet_data) > 0):
                logger.info(f"Successfully processed {len(tweet_data)} new tweets from column {column['title']}")
            return tweet_data
            
        except Exception as e:
            logger.error(f"Error getting tweets from column {column_id}: {str(e)}")
            return [] 

    async def scrape_all_columns(self, is_monitoring=False):
        """Scrape all columns concurrently"""
        try:
            # Create tasks for each column
            tasks = []
            for column_id in self.columns:
                task = asyncio.create_task(self.get_column_tweets(column_id, is_monitoring))
                tasks.append((column_id, task))
            
            # Wait for all tasks to complete
            results = []
            errors = []  # Track errors
            for column_id, task in tasks:
                try:
                    tweets = await task
                    if tweets:
                        # Update latest tweet ID
                        self.latest_tweets[column_id] = tweets[0]['id']
                        
                        # Save tweets to column file
                        column = self.columns[column_id]
                        if is_monitoring:
                            # Load existing tweets for monitoring
                            existing_tweets = []
                            if column['file'].exists():
                                with open(column['file'], 'r') as f:
                                    existing_tweets = json.load(f)
                            # Add new tweets at the beginning
                            existing_tweets = tweets + existing_tweets
                            tweets_to_save = existing_tweets
                        else:
                            # For initial scrape, just save the tweets
                            tweets_to_save = tweets
                            
                        # Save to file
                        with open(column['file'], 'w') as f:
                            json.dump(tweets_to_save, f, indent=2)
                            
                        results.append((column_id, len(tweets)))
                except Exception as e:
                    errors.append(str(e))  # Collect errors
                    logger.error(f"Error in column {column_id}: {str(e)}")
            
            # If all columns failed, propagate error
            if len(errors) == len(self.columns):
                raise Exception(f"All columns failed: {'; '.join(errors)}")
                
            # Save latest tweet IDs if any new tweets were found
            if results:
                self.save_latest_tweets()
                
            return results
            
        except Exception as e:
            logger.error(f"Error in concurrent scraping: {str(e)}")
            raise  # Propagate error up 