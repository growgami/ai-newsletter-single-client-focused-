"""TweetDeck scraping functionality using Playwright best practices"""

import logging
import json
from pathlib import Path
import asyncio
from datetime import datetime
from typing import Dict, List, Optional

# Get logger with explicit module name
logger = logging.getLogger('deck_scraper')

class ScrapingError(Exception):
    """Custom error class for scraping errors"""
    pass

class DeckScraper:
    def __init__(self, page, config: dict):
        """Initialize DeckScraper with Playwright page and config"""
        self.page = page
        self.config = config
        self.columns: Dict[str, dict] = {}
        self.latest_tweets: Dict[str, str] = {}
        
        # Setup data directories
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.today = datetime.now().strftime('%Y%m%d')
        self.today_dir = self.raw_dir / self.today
        
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.today_dir.mkdir(parents=True, exist_ok=True)
        
        # Latest tweets file for tracking
        self.latest_tweets_file = self.data_dir / 'latest_tweets.json'
        
        # Load any existing latest tweets
        self.load_latest_tweets()

        # Rate limiting - adjusted for 1 second cycles
        self.last_scrape_time = {}  # Track last scrape time per column
        self.min_scrape_interval = 0.1  # Minimum time between scrapes (100ms)
        self.max_backoff = 1.0      # Maximum backoff time in seconds

    def load_latest_tweets(self) -> None:
        """Load the latest tweet IDs from file"""
        try:
            if self.latest_tweets_file.exists():
                with open(self.latest_tweets_file, 'r') as f:
                    self.latest_tweets = json.load(f)
                logger.info(f"Loaded {len(self.latest_tweets)} latest tweet IDs")
        except Exception as e:
            logger.error(f"Error loading latest tweets: {str(e)}")

    def save_latest_tweets(self) -> None:
        """Save the latest tweet IDs to file"""
        try:
            with open(self.latest_tweets_file, 'w') as f:
                json.dump(self.latest_tweets, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving latest tweets: {str(e)}")

    async def identify_columns(self) -> bool:
        """Identify all columns in TweetDeck"""
        try:
            logger.info("Searching for TweetDeck columns...")
            
            # Wait for TweetDeck to fully load with longer timeout
            await asyncio.sleep(10)  # Increased initial wait
            
            # First wait for at least one column to be visible with longer timeout
            try:
                await self.page.wait_for_selector('[data-testid="multi-column-layout-column-content"]', 
                                                state="visible", 
                                                timeout=30000)  # Increased timeout to 30 seconds
            except Exception as e:
                logger.error(f"Failed to find any columns: {str(e)}")
                return False
            
            # Get all columns with retries
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    # Get all columns
                    column_locator = self.page.locator('[data-testid="multi-column-layout-column-content"]')
                    columns = await column_locator.all()
                    column_count = len(columns)
                    
                    if column_count == 0:
                        if attempt < max_attempts - 1:
                            logger.warning(f"No columns found on attempt {attempt + 1}, retrying...")
                            await asyncio.sleep(5)
                            continue
                        else:
                            logger.error("No columns found after all attempts!")
                            return False
                    
                    logger.info(f"Found {column_count} columns")
                    break
                    
                except Exception as e:
                    if attempt < max_attempts - 1:
                        logger.warning(f"Error getting columns on attempt {attempt + 1}: {str(e)}")
                        await asyncio.sleep(5)
                        continue
                    else:
                        logger.error(f"Failed to get columns after all attempts: {str(e)}")
                        return False
            
            # Process each column with more resilient header detection
            successful_columns = 0
            for index, column in enumerate(columns):  # Keep 0-based indexing
                column_id = str(index)
                
                try:
                    # Try multiple ways to get the column title
                    title_element = None
                    
                    # First try: Wait for header with standard selector
                    try:
                        header_locator = column.locator('[data-testid="columnHeader"]')
                        title_element = await header_locator.first()
                    except Exception:
                        pass
                    
                    # Second try: Look for any text in the column header area
                    if not title_element:
                        try:
                            header_area = column.locator('div[role="heading"]')
                            title_element = await header_area.first()
                        except Exception:
                            pass
                    
                    # Get column title or use fallback
                    if title_element:
                        column_title = await title_element.inner_text()
                    else:
                        column_title = f"Column {index}"  # Keep 0-based display
                    
                    # Store column info
                    self.columns[column_id] = {
                        'title': column_title,
                        'file': self.today_dir / f"column_{column_id}.json"
                    }
                    
                    successful_columns += 1
                    logger.info(f"Column {index}/{column_count}: {column_title} ({column_id})")
                    
                except Exception as column_error:
                    logger.warning(f"Error processing column {index}: {str(column_error)}")
                    # Continue with next column instead of failing completely
                    continue
            
            # Return success if we found at least one valid column
            if successful_columns > 0:
                logger.info(f"Successfully identified {successful_columns} columns")
                return True
            else:
                logger.error("No columns were successfully identified")
                return False
            
        except Exception as e:
            logger.error(f"Error identifying columns: {str(e)}")
            return False

    async def get_tweet_data(self, tweet_element) -> Optional[dict]:
        """Extract data from a single tweet element"""
        try:
            # Check for repost indicator using evaluate
            social_context = await tweet_element.evaluate("""
                tweet => {
                    const context = tweet.parentElement?.querySelector('[data-testid="socialContext"]');
                    return context ? context.textContent : null;
                }
            """)
            is_repost = social_context and "reposted" in social_context.lower() if social_context else False
            original_author = social_context.split(' reposted')[0].strip() if is_repost else ''
            
            # Check for quote tweet structure
            text_elements = await tweet_element.query_selector_all('[data-testid="tweetText"]')
            user_elements = await tweet_element.query_selector_all('[data-testid="User-Name"]')
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
            
            # Get tweet link and ID
            tweet_link = await tweet_element.query_selector('a[href*="/status/"]')
            if not tweet_link:
                return None
                
            href = await tweet_link.get_attribute('href')
            tweet_id = href.split('/status/')[-1]
            
            # Get tweet text
            text = ""
            text_element = await tweet_element.query_selector('[data-testid="tweetText"]')
            if text_element:
                text = await text_element.inner_text()
            
            # Get author info
            author_handle = ""
            author_element = await tweet_element.query_selector('[data-testid="User-Name"]')
            if author_element:
                author_handle = await author_element.evaluate("""
                    el => Array.from(el.querySelectorAll('span'))
                        .find(span => span.textContent.includes('@'))?.textContent.trim().replace(/^@/, '') || ''
                """)
            
            tweet_data = {
                'id': tweet_id,
                'text': text,
                'authorHandle': author_handle,
                'url': f"https://twitter.com/i/status/{tweet_id}",
                'isRepost': is_repost,
                'isQuoteRetweet': is_quote_retweet,
                'originalAuthor': original_author,
                'quotedContent': quoted_content,
                'repostedContent': reposted_content
            }
            
            logger.debug(f"Successfully extracted tweet data: ID={tweet_id}, Author={author_handle}")
            return tweet_data
            
        except Exception as e:
            logger.error(f"Error processing tweet: {str(e)}")
            return None

    async def get_column_tweets(self, column_id: str, is_monitoring: bool = False) -> List[dict]:
        """Get tweets from a specific column"""
        try:
            column = self.columns.get(column_id)
            if not column:
                return []
                
            # Get column element
            columns = await self.page.query_selector_all('div[data-testid="multi-column-layout-column-content"]')
            index = int(column_id)  # Keep 0-based indexing
            if index >= len(columns):
                logger.error(f"🚫 Column {index}: Out of range")
                raise ScrapingError(f"Column {index} out of range")
                
            column_element = columns[index]
            
            # Wait for tweets to load with retries
            max_attempts = 3
            tweets = []
            for attempt in range(max_attempts):
                timeline = await column_element.query_selector('div[data-testid="cellInnerDiv"]')
                if timeline:
                    await asyncio.sleep(1)
                    tweets = await column_element.query_selector_all('article[data-testid="tweet"]')
                    if len(tweets) > 0:
                        break
                        
                if attempt < max_attempts - 1 and not is_monitoring:
                    await asyncio.sleep(2)
            
            if not tweets:
                return []  # No tweets is a valid state
                
            # For monitoring, only process first tweet if we have latest ID
            if is_monitoring and self.latest_tweets.get(column_id) and len(tweets) > 0:
                latest_id = self.latest_tweets[column_id]
                first_tweet = tweets[0]
                
                tweet_link = await first_tweet.query_selector('a[href*="/status/"]')
                if not tweet_link:
                    raise ScrapingError(f"Failed to get tweet link in column {column_id}")
                    
                href = await tweet_link.get_attribute('href')
                tweet_id = href.split('/status/')[-1]
                
                if tweet_id == latest_id:
                    return []  # No new tweets is a valid state
                    
                tweets = [first_tweet]
            
            # Process tweets
            tweet_data_list = []
            for tweet_element in tweets:
                tweet_data = await self.get_tweet_data(tweet_element)
                if tweet_data:
                    tweet_data['column'] = column['title']
                    tweet_data_list.append(tweet_data)
            
            return tweet_data_list
            
        except ScrapingError:
            raise  # Re-raise ScrapingError
        except Exception as e:
            logger.error(f"❌ Error in column {column_id}: {str(e)}")
            raise ScrapingError(f"Failed to scrape column {column_id}: {str(e)}")

    async def scrape_all_columns(self, is_monitoring=False):
        """Scrape all columns concurrently"""
        try:
            tasks = []
            for column_id in self.columns:
                task = asyncio.create_task(self.get_column_tweets(column_id, is_monitoring))
                tasks.append((column_id, task))
            
            results = []
            
            for column_id, task in tasks:
                try:
                    tweets = await task
                    if tweets:  # Only process if we got tweets
                        self.latest_tweets[column_id] = tweets[0]['id']
                        column = self.columns[column_id]
                        
                        if is_monitoring:
                            existing_tweets = []
                            if column['file'].exists():
                                with open(column['file'], 'r') as f:
                                    existing_tweets = json.load(f)
                            tweets_to_save = tweets + existing_tweets
                        else:
                            tweets_to_save = tweets
                            
                        with open(column['file'], 'w') as f:
                            json.dump(tweets_to_save, f, indent=2)
                            
                        results.append((column_id, len(tweets)))
                except Exception as e:
                    logger.error(f"Column {column_id}: {str(e)}")
                    raise ScrapingError(f"Failed to scrape column {column_id}")
            
            if results:
                self.save_latest_tweets()
                total_tweets = sum(count for _, count in results)
                logger.info(f"Found {total_tweets} new tweets across {len(results)} columns")
            
            return results
            
        except Exception as e:
            logger.error(f"Critical scrape failure: {str(e)}")
            raise 