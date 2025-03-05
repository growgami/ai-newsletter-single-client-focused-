import logging
import json
from pathlib import Path
import re
import asyncio
from datetime import datetime, timedelta
import zoneinfo
from utils.error_handler import with_retry, DataProcessingError, log_error, RetryConfig

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.data_dir = Path('data')
        self.raw_dir = self.data_dir / 'raw'
        self.processed_dir = self.data_dir / 'processed'
        self.retry_config = RetryConfig(max_retries=3, base_delay=1.0, max_delay=15.0)
        self.min_words = 2
        self.min_tweets_per_category = 3
        
        # Ensure directories exist
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        
    def load_column_tweets(self, column_file):
        """Load tweets from a column file"""
        try:
            with open(column_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading tweets from {column_file}: {str(e)}")
            return []
            
    def normalize_text(self, text):
        """Normalize special characters and symbols from text"""
        if not text:
            return text
            
        # Replace multiple spaces with single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove non-printable characters
        text = ''.join(char for char in text if char.isprintable())
        
        # Normalize unicode characters
        text = text.replace('"', '"').replace('"', '"')  # Smart quotes
        text = text.replace(''', "'").replace(''', "'")  # Smart apostrophes
        text = text.replace('…', '...')  # Ellipsis
        text = text.replace('–', '-')    # En dash
        text = text.replace('—', '-')    # Em dash
        
        # Remove URLs (optional, but they often contain special chars)
        text = re.sub(r'http[s]?://\S+', '', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
        
    def is_valid_tweet(self, tweet):
        """Check if a tweet is valid according to our criteria"""
        # Must have text
        if not tweet.get('text'):
            return False
            
        # Text must be at least 2 words (after normalization)
        normalized_text = self.normalize_text(tweet['text'])
        words = [w for w in normalized_text.split() if w.strip()]  # Remove empty strings
        if len(words) < 2:
            return False
            
        return True
        
    @with_retry(RetryConfig(max_retries=3, base_delay=1.0))
    async def process_tweets(self, date_str=None):
        """Process tweets with retry logic"""
        try:
            if not date_str:
                # Use the same date as provided by the news generator
                # which now reads from the persistent date file
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                date_str = current_time.strftime('%Y%m%d')
                
            logger.info(f"Processing tweets for date: {date_str}")
            
            # Set the date-specific directory
            self.today_dir = self.raw_dir / date_str
            if not self.today_dir.exists():
                logger.error(f"Raw directory not found: {self.today_dir}")
                return 0
            
            # Load and combine raw tweets
            raw_columns = await self._load_raw_tweets(date_str)
            if not raw_columns:
                logger.warning("No raw tweets found to process")
                return 0
                
            # Process tweets by column
            processed_data = self.process_columns(raw_columns)
            logger.info(f"Final count: {processed_data['metadata']['total_tweets']} tweets remaining")
            
            # Save processed tweets
            await self._save_processed_tweets(processed_data, date_str)
            
            return processed_data['metadata']['total_tweets']
            
        except Exception as e:
            log_error(logger, e, f"Failed to process tweets for date {date_str}")
            raise DataProcessingError(f"Tweet processing failed: {str(e)}")
            
    async def _load_raw_tweets(self, date_str):
        """Load raw tweets with error handling"""
        try:
            columns = {}
            total_tweets = 0
            
            # Check if directory exists
            if not self.today_dir.exists():
                logger.error(f"Directory not found: {self.today_dir}")
                return {}
                
            # List all JSON files in directory - now using *_Tweets.json pattern
            files = list(self.today_dir.glob('*.json'))
            if not files:
                logger.error(f"No JSON files found in {self.today_dir}")
                return {}
                
            logger.info(f"Found {len(files)} tweet files: {[f.name for f in files]}")
            
            for file in files:
                try:
                    # Extract category name from filename (e.g., "Arbitrum" from "Arbitrum_Tweets.json")
                    category = file.stem.split('_')[0] if '_' in file.stem else file.stem
                    logger.info(f"Loading tweets from {file.name} for category: {category}")
                    
                    with open(file, 'r', encoding='utf-8') as f:
                        file_data = json.load(f)
                        
                        # Handle both formats:
                        # 1. Direct array of tweets
                        # 2. Nested structure with metadata and tweets array
                        if isinstance(file_data, dict) and 'tweets' in file_data and isinstance(file_data['tweets'], list):
                            tweets = file_data['tweets']
                            logger.info(f"Found {len(tweets)} tweets in nested structure")
                        elif isinstance(file_data, list):
                            tweets = file_data
                            logger.info(f"Found {len(tweets)} tweets in direct array")
                        else:
                            logger.error(f"Unexpected format in {file.name}")
                            continue
                            
                        columns[category] = tweets
                        total_tweets += len(tweets)
                        logger.info(f"Loaded {len(tweets)} tweets from {file.name}")
                        
                except Exception as e:
                    log_error(logger, e, f"Failed to load tweets from {file}")
                    continue
                    
            logger.info(f"Loaded {total_tweets} raw tweets from {len(columns)} categories in {self.today_dir}")
            return columns
            
        except Exception as e:
            log_error(logger, e, "Failed to load raw tweets")
            raise DataProcessingError(f"Raw tweet loading failed: {str(e)}")
            
    def deduplicate(self, columns):
        """Step 3.1: Remove duplicate tweets by ID across all columns while maintaining original column structure"""
        initial_total = sum(len(t) for t in columns.values())
        seen_ids = set()
        deduped = {}
        
        # Process columns in order, keeping first occurrence of each tweet
        for col_id in sorted(columns.keys()):
            deduped[col_id] = []
            for tweet in columns[col_id]:
                tweet_id = tweet.get('id')
                if tweet_id and tweet_id not in seen_ids:
                    seen_ids.add(tweet_id)
                    deduped[col_id].append(tweet)
            
            logger.info(f"Column {col_id}: {len(columns[col_id])}→{len(deduped[col_id])} tweets after deduplication")
        
        deduped_total = sum(len(t) for t in deduped.values())
        logger.info(f"Cross-column deduplication: {initial_total} → {deduped_total} tweets (-{initial_total - deduped_total})")
        return deduped
            
    def _remove_duplicates(self, tweets):
        """Remove duplicate tweets based on tweet ID"""
        try:
            seen_ids = set()
            unique_tweets = []
            
            for tweet in tweets:
                tweet_id = tweet.get('id')
                if not tweet_id or tweet_id in seen_ids:
                    continue
                    
                seen_ids.add(tweet_id)
                unique_tweets.append(tweet)
                
            logger.info(f"Removed {len(tweets) - len(unique_tweets)} duplicate tweets")
            return unique_tweets
            
        except Exception as e:
            log_error(logger, e, "Failed to remove duplicates")
            raise DataProcessingError(f"Duplicate removal failed: {str(e)}")
            
    def _normalize_tweet(self, tweet):
        """Normalize tweet structure for processing"""
        try:
            normalized = {
                'id': tweet.get('id', ''),
                'text': tweet.get('text', ''),
                'authorHandle': tweet.get('author_handle', tweet.get('authorHandle', '')),
                'url': tweet.get('url', ''),
                'processed_at': datetime.now().isoformat(),
                'is_repost': tweet.get('is_repost', False),
                'is_quote_tweet': tweet.get('is_quote_tweet', False)
            }
            
            # Handle quoted content
            if tweet.get('quoted_content'):
                quoted = tweet['quoted_content']
                normalized['quotedContent'] = {
                    'id': quoted.get('id', ''),
                    'text': quoted.get('text', ''),
                    'authorHandle': quoted.get('author_handle', quoted.get('authorHandle', '')),
                    'url': quoted.get('url', '')
                }
                
            # Normalize text
            if normalized['text']:
                normalized['text'] = self.normalize_text(normalized['text'])
                
            return normalized
            
        except Exception as e:
            logger.warning(f"Failed to normalize tweet {tweet.get('id', 'unknown')}: {str(e)}")
            return None
            
    def _process_raw_tweets(self, raw_columns):
        """Add metadata to processed data"""
        try:
            processed_data = {
                'metadata': {
                    'processed_at': datetime.now().isoformat(),
                    'categories_processed': len(raw_columns)
                },
                'categories': {},
                'total_tweets': 0
            }
            
            for category, tweets in raw_columns.items():
                logger.info(f"Processing category {category} with {len(tweets)} tweets")
                
                # Remove duplicates based on tweet ID
                seen_ids = set()
                unique_tweets = []
                for tweet in tweets:
                    if not isinstance(tweet, dict):
                        logger.warning(f"Skipping non-dict tweet in category {category}")
                        continue
                    
                    tweet_id = tweet.get('id')
                    if not tweet_id:
                        logger.warning(f"Skipping tweet without ID in category {category}")
                        continue
                        
                    if tweet_id not in seen_ids:
                        seen_ids.add(tweet_id)
                        # Filter and normalize tweets
                        if self._is_valid_tweet(tweet):
                            normalized_tweet = self._normalize_tweet(tweet)
                            if normalized_tweet:
                                unique_tweets.append(normalized_tweet)
                
                if unique_tweets:
                    processed_data['categories'][category] = unique_tweets
                    processed_data['total_tweets'] += len(unique_tweets)
                    logger.info(f"Processed {len(unique_tweets)} valid tweets for category {category}")
                else:
                    logger.warning(f"No valid tweets found for category {category}")
            
            # Make sure metadata reflects the actual counts
            processed_data['metadata']['total_tweets'] = processed_data['total_tweets']
            processed_data['metadata']['categories_processed'] = len(processed_data['categories'])
            logger.info(f"Processed data structure created with {processed_data['total_tweets']} tweets across {len(processed_data['categories'])} categories")
            return processed_data
            
        except Exception as e:
            log_error(logger, e, "Failed to process raw tweets")
            raise DataProcessingError(f"Raw tweet processing failed: {str(e)}")
            
    def _is_valid_tweet(self, tweet):
        """Check if a tweet is valid for processing"""
        try:
            # Make sure tweet has required fields
            if not isinstance(tweet, dict):
                return False
                
            # Require at least an id and text
            if 'id' not in tweet or 'text' not in tweet:
                return False
                
            # Check if the tweet has enough text content
            text = tweet.get('text', '')
            if not text or len(text.split()) < self.min_words:
                logger.debug(f"Tweet {tweet.get('id', 'unknown')} rejected: not enough text")
                return False
                
            # Basic content filtering - you can expand this
            if text.startswith('RT @') and not tweet.get('is_repost', False):
                logger.debug(f"Tweet {tweet.get('id', 'unknown')} rejected: likely a retweet")
                return False
                
            return True
            
        except Exception as e:
            logger.warning(f"Error validating tweet {tweet.get('id', 'unknown')}: {str(e)}")
            return False
            
    async def _save_processed_tweets(self, processed_data, date_str):
        """Save processed tweets to file"""
        try:
            # Ensure directory exists
            output_file = self.processed_dir / f"{date_str}.json"
            
            logger.info(f"Saving {processed_data['metadata']['total_tweets']} processed tweets to {output_file}")
            
            # Create parent directories if they don't exist
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Successfully saved processed tweets to {output_file}")
            
            # Also save individual category files for easier access
            categories_dir = self.processed_dir / f"{date_str}_categories"
            categories_dir.mkdir(parents=True, exist_ok=True)
            
            for category, tweets in processed_data['categories'].items():
                category_file = categories_dir / f"{category}.json"
                with open(category_file, 'w', encoding='utf-8') as f:
                    json.dump(tweets, f, indent=2, ensure_ascii=False)
                    
                logger.info(f"Saved {len(tweets)} tweets for category {category} to {category_file}")
                
            return True
            
        except Exception as e:
            log_error(logger, e, f"Failed to save processed tweets for date {date_str}")
            raise DataProcessingError(f"Failed to save processed tweets: {str(e)}")

    def process_columns(self, raw_columns):
        """Steps 3.1-3.4: Process all columns"""
        # Process tweets
        processed_data = self._process_raw_tweets(raw_columns)
        
        # Ensure metadata always has required fields
        if 'metadata' not in processed_data:
            processed_data['metadata'] = {}
        
        if 'total_tweets' not in processed_data['metadata']:
            # Use the total_tweets field from the root object
            processed_data['metadata']['total_tweets'] = processed_data.get('total_tweets', 0)
            
        if 'categories_processed' not in processed_data['metadata']:
            processed_data['metadata']['categories_processed'] = len(processed_data.get('categories', {}))
        
        # Log results
        logger.info(f"After processing: {processed_data['metadata']['total_tweets']} tweets across {processed_data['metadata']['categories_processed']} categories")
        if processed_data['metadata']['total_tweets'] == 0:
            logger.warning("No valid tweets found after processing")
            
        return processed_data

    def clean_tweets(self, columns):
        """Steps 3.2-3.4: Text validation and normalization"""
        valid_tweets = {}
        initial_total = sum(len(t) for t in columns.values())
        
        for col_id, tweets in columns.items():
            cleaned = []
            for t in tweets:
                if self._is_valid(t) and self._normalize(t):
                    cleaned.append(t)
            valid_tweets[col_id] = cleaned
            logger.info(f"Column {col_id}: Cleaned {len(tweets)}→{len(cleaned)}")
            
        cleaned_total = sum(len(t) for t in valid_tweets.values())
        logger.info(f"Cleaning: {initial_total} → {cleaned_total} tweets (-{initial_total - cleaned_total})")
        return valid_tweets

    def _is_valid(self, tweet):
        """Check minimum text requirements"""
        text = tweet.get('text', '')
        return len(text.split()) >= self.min_words

    def _normalize(self, tweet):
        """Special character normalization"""
        text = tweet['text']
        # Add normalization logic from existing implementation
        tweet['text'] = self.normalize_text(text)  
        return True

    def structure_output(self, columns):
        """Steps 3.5-3.6: Create unified JSON structure with total count"""
        total = sum(len(tweets) for tweets in columns.values())
        return {
            'metadata': {
                'processed_at': datetime.now().isoformat(),
                'categories_processed': len(columns),
                'total_tweets': total
            },
            'categories': columns
        }

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = DataProcessor()
    
    # Get date from command line argument
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Run async processing
    asyncio.run(processor.process_tweets(date_to_process)) 