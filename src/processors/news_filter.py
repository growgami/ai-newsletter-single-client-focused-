"""News filtering and categorization service"""

import logging
import json
from pathlib import Path
from datetime import datetime, timezone
import asyncio
from openai import AsyncOpenAI
from category_mapping import CATEGORY
import sys

logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    def __init__(self, max_failures=5, reset_timeout=60):
        self.max_failures = max_failures
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None

    async def check(self):
        if self.failure_count >= self.max_failures:
            if (datetime.now() - self.last_failure_time).seconds < self.reset_timeout:
                raise Exception("Circuit breaker open")
            else:
                self.reset()

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()

    def reset(self):
        self.failure_count = 0
        self.last_failure_time = None

class NewsFilter:
    def __init__(self, config):
        self.config = config
        self.data_dir = Path('data')
        self.input_dir = self.data_dir / 'filtered' / 'content_filtered'  # Input from content_filter
        self.output_dir = self.data_dir / 'filtered' / 'news_filtered'  # Output directory
        
        # Store API keys as instance variables first
        self.deepseek_api_key = config['deepseek_api_key']
        self.openai_api_key = config['openai_api_key']
        
        # Initialize both API clients
        self.deepseek_client = AsyncOpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.openai_client = AsyncOpenAI(api_key=self.openai_api_key)
        
        self.circuit_breaker = CircuitBreaker()
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_input_file(self):
        """Get input file path from content_filter"""
        return self.input_dir / 'combined_filtered.json'

    def _get_output_file(self):
        """Get output file path"""
        current_time = datetime.now(timezone.utc)
        date_str = current_time.strftime('%Y%m%d')
        return self.output_dir / f'{CATEGORY.lower()}_summary_{date_str}.json'

    async def _try_deepseek_request(self, prompt):
        """Attempt to get a response from Deepseek"""
        try:
            # Increase timeout from 3s to 30s
            response = await asyncio.wait_for(
                self.deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    max_tokens=4096
                ),
                timeout=30  # Increased timeout
            )
            
            if not response.choices:
                logger.warning("Deepseek response contains no choices")
                return None
            
            return response.choices[0].message.content
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.warning(f"Deepseek request failed: {str(e)}")
            return None

    async def _try_openai_request(self, prompt):
        """Attempt to get a response from OpenAI"""
        try:
            # Increase timeout from 10s to 60s
            response = await asyncio.wait_for(
                self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    response_format={"type": "json_object"},
                    max_tokens=4096
                ),
                timeout=60  # Increased timeout
            )
            
            return response.choices[0].message.content
            
        except asyncio.TimeoutError:
            logger.warning("OpenAI request timed out after 60 seconds")
            return None
        except Exception as e:
            logger.warning(f"OpenAI request failed: {str(e)}")
            return None

    async def _api_request(self, prompt):
        """Unified API request handler"""
        try:
            await self.circuit_breaker.check()
            
            # First try Deepseek
            response = await self._try_deepseek_request(prompt)
            
            # If Deepseek fails, try OpenAI as fallback
            if response is None:
                response = await self._try_openai_request(prompt)
                
            # If both failed, raise exception
            if response is None:
                raise Exception("Both Deepseek and OpenAI requests failed")
            
            return response
            
        except Exception as e:
            self.circuit_breaker.record_failure()
            logger.error(f"API request failed: {str(e)}")
            return None

    def _validate_tweet_fields(self, tweet):
        """Validate tweet has all required fields"""
        required_fields = ['attribution', 'content', 'url']
        missing = [f for f in required_fields if f not in tweet]
        if missing:
            logger.error(f"Missing required fields {missing} in tweet")
            return False
        return True

    def _build_subcategory_prompt(self, tweets, category):
        """Build prompt for generating subcategories from tweets"""
        return f"""
        You are organizing {category} tweets into logical subcategories based on their content and themes.

        TWEETS TO ORGANIZE:
        {json.dumps(tweets, indent=2)}

        REQUIRED OUTPUT FORMAT:
        {{
            "{category}": {{
                "Subcategory Name": [
                    {{
                        "attribution": "original attribution",
                        "content": "original content",
                        "url": "original url"
                    }}
                ]
            }}
        }}

        CATEGORIZATION RULES:
        1. Create 2-4 clear, descriptive subcategories that best group the content
        2. Each tweet MUST be placed in exactly one subcategory
        3. Use clear, descriptive names that reflect the content theme (e.g., "Network Metrics", "Development Updates")
        4. Group similar topics and themes together
        5. Consider these aspects when categorizing:
           - Technical updates and metrics
           - Financial and market data
           - Community and governance
           - Development and infrastructure
           - Partnerships and adoption
           - Research and innovation

        CRITICAL REQUIREMENTS:
        1. Preserve ALL original fields and values exactly as they appear in input
        2. Required fields that MUST be preserved exactly: attribution, content, url
        3. DO NOT modify or rewrite any content - use exact values from input
        4. DO NOT create empty subcategories
        5. DO NOT add or remove tweets

        Return ONLY the JSON output with subcategorized tweets, no explanations."""

    def _build_content_dedup_prompt(self, tweets):
        """Build prompt for content-based deduplication"""
        return f"""
        Analyze these tweets and remove semantic duplicates where the attribution and content convey the same information.
        Choose the most informative version when duplicates are found.

        TWEETS TO DEDUPLICATE:
        {json.dumps(tweets, indent=2)}

        DEDUPLICATION RULES:
        1. Compare attribution + content semantically
        2. If multiple tweets convey the same core information, keep only the most informative one
        3. Preserve exact field values - do not modify content
        4. Keep all required fields: attribution, content, url

        REQUIRED OUTPUT FORMAT:
        {{
            "tweets": [
                {{
                    "attribution": "original attribution",
                    "content": "original content",
                    "url": "original url"
                }}
            ]
        }}

        Return ONLY the JSON output with deduplicated tweets, no explanations."""

    async def _content_based_dedup(self, tweets):
        """Perform content-based deduplication using AI"""
        try:
            if not tweets:
                return []

            logger.info(f"Starting content-based deduplication on {len(tweets)} tweets")
            
            # Build and send prompt
            dedup_prompt = self._build_content_dedup_prompt(tweets)
            dedup_response = await self._api_request(dedup_prompt)
            
            if not dedup_response:
                logger.error("No response from content deduplication request")
                return tweets  # Fall back to original tweets
                
            # Parse response
            result = json.loads(dedup_response)
            
            if not isinstance(result, dict) or 'tweets' not in result:
                logger.error("Invalid content deduplication response format")
                return tweets
                
            deduped_tweets = result['tweets']
            
            # Validate output
            valid_deduped = []
            for tweet in deduped_tweets:
                if self._validate_tweet_fields(tweet):
                    valid_deduped.append(tweet)
                    
            removed_count = len(tweets) - len(valid_deduped)
            logger.info(f"Content-based deduplication removed {removed_count} similar tweets")
            
            return valid_deduped
            
        except Exception as e:
            logger.error(f"Error in content-based deduplication: {str(e)}")
            return tweets  # Fall back to original tweets on error

    async def process_content(self):
        """Process content from combined input file"""
        try:
            # Load input file
            input_file = self._get_input_file()
            if not input_file.exists():
                logger.error(f"No input file found at: {input_file}")
                return False

            try:
                with open(input_file, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Error loading input file: {str(e)}")
                return False

            # Get tweets from input
            tweets = data.get(CATEGORY, {}).get('tweets', [])
            if not tweets:
                logger.warning("No tweets found in input file")
                return False

            # Validate input tweets have required fields
            valid_tweets = []
            for tweet in tweets:
                if self._validate_tweet_fields(tweet):
                    valid_tweets.append(tweet)
                else:
                    logger.warning(f"Skipping tweet with missing fields: {tweet.get('url', 'unknown')}")

            if not valid_tweets:
                logger.error("No valid tweets found after field validation")
                return False

            # URL-based deduplication
            seen_urls = set()
            url_deduped_tweets = []
            for tweet in valid_tweets:
                url = tweet.get('url')
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    url_deduped_tweets.append(tweet)

            logger.info(f"URL deduplication: {len(valid_tweets)} → {len(url_deduped_tweets)} tweets")

            # Content-based deduplication
            content_deduped_tweets = await self._content_based_dedup(url_deduped_tweets)
            logger.info(f"Content deduplication: {len(url_deduped_tweets)} → {len(content_deduped_tweets)} tweets")

            # Generate subcategories with fully deduplicated tweets
            subcategory_prompt = self._build_subcategory_prompt(content_deduped_tweets, CATEGORY)
            subcategory_response = await self._api_request(subcategory_prompt)

            if not subcategory_response:
                logger.error("No response received from subcategory request")
                return False

            result = json.loads(subcategory_response)

            if not isinstance(result, dict) or CATEGORY not in result:
                logger.error(f"Invalid subcategory response format")
                return False

            # Validate subcategories and remove empty ones
            subcategories = result[CATEGORY]
            if not isinstance(subcategories, dict) or not subcategories:
                logger.error(f"Invalid subcategories format")
                return False

            logger.info("Checking subcategories before cleaning:")
            for subcat, tweets in subcategories.items():
                logger.info(f"   • {subcat}: {len(tweets) if isinstance(tweets, list) else 'invalid'} tweets")

            # Remove any empty subcategories
            non_empty_subcategories = {}
            for subcat, tweets in subcategories.items():
                if isinstance(tweets, list) and len(tweets) > 0:
                    non_empty_subcategories[subcat] = tweets
                    logger.info(f"   ✓ Keeping subcategory '{subcat}' with {len(tweets)} tweets")
                else:
                    logger.warning(f"   ✗ Removing empty or invalid subcategory '{subcat}'")

            if not non_empty_subcategories:
                logger.error("No valid non-empty subcategories found")
                return False

            # Update result with cleaned subcategories
            result[CATEGORY] = non_empty_subcategories

            # Count total categorized tweets
            categorized_tweets = sum(len(tweets) for tweets in non_empty_subcategories.values())

            # Log final results
            logger.info("Final subcategory distribution:")
            logger.info(f"📊 Processing Results for {CATEGORY}:")
            logger.info(f"   • Initial tweets: {len(valid_tweets)}")
            logger.info(f"   • After URL deduplication: {len(url_deduped_tweets)}")
            logger.info(f"   • After content deduplication: {len(content_deduped_tweets)}")
            logger.info(f"   • Final categorized tweets: {categorized_tweets}")
            logger.info(f"   • Subcategories created: {len(non_empty_subcategories)}")
            for subcat, tweets in non_empty_subcategories.items():
                logger.info(f"     - {subcat}: {len(tweets)} tweets")

            # Save output atomically
            output_file = self._get_output_file()
            temp_file = output_file.with_suffix('.tmp')
            try:
                with open(temp_file, 'w') as f:
                    json.dump(result, f, indent=2)
                temp_file.replace(output_file)
                logger.info(f"✅ Successfully saved {output_file.name}")
                return True
            except Exception as e:
                logger.error(f"Error saving output: {str(e)}")
                if temp_file.exists():
                    temp_file.unlink()
                return False

        except Exception as e:
            logger.error(f"Error in process_content: {str(e)}")
            return False

    async def process_all(self):
        """Process all content"""
        try:
            logger.info("Starting news filtering")
            return await self.process_content()
        except Exception as e:
            logger.error(f"Error in process_all: {str(e)}")
            return False

    def _validate_summary_file(self, file_path: Path) -> bool:
        """Validate summary file exists and has correct format"""
        try:
            if not file_path.exists():
                logger.error(f"Summary file not found: {file_path}")
                return False
                
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            # Validate basic structure
            if not isinstance(data, dict):
                logger.error("Summary file is not a valid JSON object")
                return False
                
            # Validate category exists
            if CATEGORY not in data:
                logger.error(f"Missing category {CATEGORY} in summary")
                return False
                
            category_data = data[CATEGORY]
            if not isinstance(category_data, dict):
                logger.error("Invalid category data structure")
                return False
                
            # Validate subcategories
            for subcategory, tweets in category_data.items():
                if not isinstance(tweets, list):
                    logger.error(f"Invalid tweets format in {subcategory}")
                    return False
                    
                # Validate tweet structure
                for tweet in tweets:
                    required_fields = ['attribution', 'content', 'url']
                    if not all(field in tweet for field in required_fields):
                        logger.error(f"Missing required fields in tweet under {subcategory}")
                        return False
            
            logger.info(f"✅ Summary file validation successful: {file_path.name}")
            return True
            
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in summary file: {file_path}")
            return False
        except Exception as e:
            logger.error(f"Error validating summary file: {str(e)}")
            return False

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    # Reduce external library logging
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('openai').setLevel(logging.WARNING)
    
    # Configuration setup
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY'),
        'base_url': os.getenv('API_BASE_URL', 'https://api.openai.com/v1')
    }
    
    # Run processor
    filter = NewsFilter(config)
    try:
        asyncio.run(filter.process_all())
    except KeyboardInterrupt:
        logger.info("\nShutdown requested... exiting gracefully")
    except Exception as e:
        logger.error(f"\nUnexpected error: {str(e)}")
        sys.exit(1)
