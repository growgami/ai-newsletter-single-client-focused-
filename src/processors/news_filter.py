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

        CRITICAL REQUIREMENTS - NO EXCEPTIONS:
        1. EVERY SINGLE TWEET ({len(tweets)} total) MUST be categorized
        2. Double check that no tweets are missing or skipped
        3. Count tweets in each subcategory to ensure total matches input count: {len(tweets)}
        4. Each tweet must appear in exactly one subcategory

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
        1. Analyze the actual content of tweets to determine natural groupings
        2. Create 2-4 subcategories based on the dominant themes present
        3. Each tweet MUST be placed in exactly one subcategory
        4. Use clear, descriptive subcategory names that reflect the actual content
        5. Ensure logical grouping of related information

        CRITICAL DATA REQUIREMENTS:
        1. Preserve ALL original fields and values exactly as they appear in input
        2. Required fields that MUST be preserved exactly: attribution, content, url
        3. ONLY 2-4 subcategories are allowed
        3. DO NOT modify or rewrite any content - use exact values from input
        4. DO NOT create empty subcategories
        5. DO NOT add or remove tweets
        6. VERIFY FINAL TWEET COUNT MATCHES INPUT: {len(tweets)}

        Return ONLY the JSON output with subcategorized tweets, no explanations."""

    def _build_content_dedup_prompt(self, tweets):
        """Build prompt for content-based deduplication"""
        return f"""
        Analyze and deduplicate these tweets by identifying and removing redundant information while preserving meaningful variations.

        TWEETS TO DEDUPLICATE:
        {json.dumps(tweets, indent=2)}

        DEDUPLICATION RULES:
        1. Remove tweets only when they are truly redundant:
           - Exact same information with no additional context
           - Same event with no new details
           - Same metrics without additional analysis
           - Same announcement without unique perspective
        
        2. Similarity Analysis:
           - Focus on semantic similarity of the content
           - Identify information overlap
           - Keep tweets that add new context or details
           - Preserve unique perspectives on same topic
           - Maintain different angles of coverage
        
        3. Data Requirements:
           - Keep exact field values - no modifications
           - Maintain required fields: attribution, content, url
           - Preserve data structure integrity

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
        """Remove tweets with duplicate or nearly identical content"""
        try:
            if not tweets:
                return []

            logger.info(f"Starting content-based deduplication on {len(tweets)} tweets")

            # Separate tweets from Slack to preserve them
            slack_tweets = []
            regular_tweets = []
            
            for tweet in tweets:
                if tweet.get('from_slack', False):
                    logger.info(f"Preserving tweet from Slack in content deduplication: {tweet.get('url', '')}")
                    slack_tweets.append(tweet)
                else:
                    regular_tweets.append(tweet)
                    
            logger.info(f"Found {len(slack_tweets)} tweets from Slack that will bypass content deduplication")
            
            # If we only have Slack tweets or no regular tweets, return all tweets
            if not regular_tweets:
                logger.info("No regular tweets to deduplicate, returning all tweets from Slack")
                return tweets

            # If we have a small number of tweets, process them all at once
            if len(regular_tweets) <= 15:
                dedup_prompt = self._build_content_dedup_prompt(regular_tweets)
                dedup_response = await self._api_request(dedup_prompt)
                
                if not dedup_response:
                    logger.error("No response from content deduplication request")
                    # Return all regular tweets plus Slack tweets if request fails
                    return regular_tweets + slack_tweets
                    
                # Parse response
                result = json.loads(dedup_response)
                
                if not isinstance(result, dict) or 'tweets' not in result:
                    logger.error("Invalid content deduplication response format")
                    # Return all regular tweets plus Slack tweets if response is invalid
                    return regular_tweets + slack_tweets
                    
                deduped_tweets = result['tweets']
                
            else:
                # Process in chunks of 15
                chunks = [regular_tweets[i:i + 15] for i in range(0, len(regular_tweets), 15)]
                deduped_chunks = []
                
                for chunk in chunks:
                    dedup_prompt = self._build_content_dedup_prompt(chunk)
                    dedup_response = await self._api_request(dedup_prompt)
                    
                    if not dedup_response:
                        logger.warning("No response for chunk, keeping original")
                        deduped_chunks.extend(chunk)
                        continue
                        
                    try:
                        result = json.loads(dedup_response)
                        if isinstance(result, dict) and 'tweets' in result:
                            deduped_chunks.extend(result['tweets'])
                        else:
                            logger.warning("Invalid chunk response format, keeping original")
                            deduped_chunks.extend(chunk)
                    except Exception as e:
                        logger.warning(f"Error processing chunk: {str(e)}")
                        deduped_chunks.extend(chunk)
                
                deduped_tweets = deduped_chunks
            
            # Add back the Slack tweets
            final_tweets = deduped_tweets + slack_tweets
            logger.info(f"Content deduplication complete: {len(regular_tweets)} → {len(deduped_tweets)} regular tweets, {len(slack_tweets)} Slack tweets preserved")
            return final_tweets
            
        except Exception as e:
            logger.error(f"Content deduplication failed: {str(e)}")
            # Return original tweets if deduplication fails
            return tweets

    def _build_news_worthiness_prompt(self, tweets):
        """Build prompt for news worthiness filtering"""
        prompt = f"""You are an expert news editor for {CATEGORY}. 
        Review the following {len(tweets)} tweets and filter based on news worthiness.
        
        Your task is to identify and select tweets with meaningful news value. 

        TWEETS TO ANALYZE:
        {json.dumps(tweets, indent=2)}

        SELECTION RULES:
        1. Prioritize tweets that:
           - Contain significant announcements or updates
           - Report important developments or changes
           - Discuss meaningful events or milestones
           - Provide valuable insights or analysis
           - Share notable metrics or achievements
        
        2. Deprioritize tweets that:
           - Are purely promotional without substance
           - Contain generic statements or observations
           - Repeat commonly known information
           - Lack concrete information or specifics
           - Are overly speculative or unsubstantiated

        3. Output Requirements:
           - If input has > 15 tweets, select 15-20 most newsworthy ones
           - If input has ≤ 15 tweets, keep all
           - Maintain exact field values - no modifications
           - Keep required fields: attribution, content, url

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

        Return ONLY the JSON output with selected newsworthy tweets, no explanations."""
        
        return prompt

    async def _filter_news_worthiness(self, tweets):
        """Filter tweets based on news worthiness"""
        try:
            if not tweets:
                return []

            logger.info(f"Starting news worthiness filtering on {len(tweets)} tweets")
            
            # Separate tweets from Slack to preserve them
            slack_tweets = []
            regular_tweets = []
            
            for tweet in tweets:
                if tweet.get('from_slack', False):
                    logger.info(f"Preserving tweet from Slack (from_slack=True): {tweet.get('url', '')}")
                    slack_tweets.append(tweet)
                elif 'url' in tweet and 'x.com' in tweet.get('url', '') and 'slack' in tweet.get('url', '').lower():
                    # For backward compatibility - preserve URLs with 'x.com' and 'slack' in them
                    logger.info(f"Preserving tweet with Slack URL pattern: {tweet.get('url', '')}")
                    slack_tweets.append(tweet)
                else:
                    regular_tweets.append(tweet)
                    
            logger.info(f"Found {len(slack_tweets)} tweets from Slack that will bypass news worthiness filtering")
            
            # If we only have Slack tweets or no regular tweets, return all tweets
            if not regular_tweets:
                logger.info("No regular tweets to filter, returning all tweets from Slack")
                return tweets
                
            # If we have 15 or fewer regular tweets, process them all at once
            if len(regular_tweets) <= 15:
                filter_prompt = self._build_news_worthiness_prompt(regular_tweets)
                filter_response = await self._api_request(filter_prompt)
                
                if not filter_response:
                    logger.error("No response from news worthiness filter request")
                    # Return all regular tweets and slack tweets
                    return regular_tweets + slack_tweets
                    
                # Parse response
                result = json.loads(filter_response)
                
                if not isinstance(result, dict) or 'tweets' not in result:
                    logger.error("Invalid news worthiness filter response format")
                    # Return all regular tweets and slack tweets
                    return regular_tweets + slack_tweets
                    
                filtered_tweets = result['tweets']
                
            else:
                # Process in chunks of 15
                chunks = [regular_tweets[i:i + 15] for i in range(0, len(regular_tweets), 15)]
                filtered_chunks = []
                
                for chunk in chunks:
                    filter_prompt = self._build_news_worthiness_prompt(chunk)
                    filter_response = await self._api_request(filter_prompt)
                    
                    if not filter_response:
                        logger.warning("No response for chunk, keeping original")
                        filtered_chunks.extend(chunk)
                        continue
                        
                    try:
                        result = json.loads(filter_response)
                        if isinstance(result, dict) and 'tweets' in result:
                            filtered_chunks.extend(result['tweets'])
                        else:
                            logger.warning("Invalid chunk response format, keeping original")
                            filtered_chunks.extend(chunk)
                    except Exception as e:
                        logger.warning(f"Error processing chunk: {str(e)}")
                        filtered_chunks.extend(chunk)
                
                filtered_tweets = filtered_chunks
            
            # Add back the Slack tweets
            final_tweets = filtered_tweets + slack_tweets
            logger.info(f"News worthiness filtering complete: {len(regular_tweets)} regular tweets → {len(filtered_tweets)} filtered + {len(slack_tweets)} Slack tweets = {len(final_tweets)} total")
            
            return final_tweets
            
        except Exception as e:
            logger.error(f"Error in news worthiness filtering: {str(e)}")
            return tweets

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

            # Separate tweets from Slack to preserve them
            slack_tweets = []
            regular_tweets = []
            
            for tweet in tweets:
                if tweet.get('from_slack', False):
                    logger.info(f"Preserving tweet from Slack: {tweet.get('url', '')}")
                    slack_tweets.append(tweet)
                else:
                    regular_tweets.append(tweet)
                    
            logger.info(f"Found {len(slack_tweets)} tweets from Slack that will be preserved")

            # Validate input tweets have required fields
            valid_tweets = []
            for tweet in regular_tweets:
                if self._validate_tweet_fields(tweet):
                    valid_tweets.append(tweet)
                else:
                    logger.warning(f"Skipping tweet with missing fields: {tweet.get('url', 'unknown')}")

            if not valid_tweets and not slack_tweets:
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

            # News worthiness filtering
            newsworthy_tweets = await self._filter_news_worthiness(content_deduped_tweets)
            logger.info(f"News worthiness filtering: {len(content_deduped_tweets)} → {len(newsworthy_tweets)} tweets")

            # Add back tweets from Slack (they bypass filtering)
            newsworthy_tweets.extend(slack_tweets)
            logger.info(f"Added back {len(slack_tweets)} tweets from Slack")
            
            # Generate subcategories with filtered tweets
            subcategory_prompt = self._build_subcategory_prompt(newsworthy_tweets, CATEGORY)
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
            
            # Check if we need to add a special 'From Slack' subcategory
            if slack_tweets:
                # Create a dedicated subcategory for Slack tweets if they weren't already categorized
                slack_subcategory = "From Slack"
                
                # Check if any slack tweets were already categorized by the model
                slack_tweet_urls = set(tweet.get('url', '') for tweet in slack_tweets)
                categorized_slack_urls = set()
                
                # Check all subcategories for slack tweets
                for subcategory, subcat_tweets in result[CATEGORY].items():
                    for i, tweet in enumerate(subcat_tweets[:]):
                        if tweet.get('url', '') in slack_tweet_urls:
                            categorized_slack_urls.add(tweet.get('url', ''))
                
                # Find uncategorized slack tweets
                uncategorized_slack_tweets = [
                    tweet for tweet in slack_tweets 
                    if tweet.get('url', '') not in categorized_slack_urls
                ]
                
                # If there are any uncategorized slack tweets, add them to the Slack subcategory
                if uncategorized_slack_tweets:
                    logger.info(f"Adding {len(uncategorized_slack_tweets)} uncategorized Slack tweets to '{slack_subcategory}' subcategory")
                    if slack_subcategory not in result[CATEGORY]:
                        result[CATEGORY][slack_subcategory] = []
                    result[CATEGORY][slack_subcategory].extend(uncategorized_slack_tweets)
            
            # Count total categorized tweets
            categorized_tweets = sum(len(tweets) for tweets in non_empty_subcategories.values())

            # Log final results
            logger.info("Final subcategory distribution:")
            logger.info(f"📊 Processing Results for {CATEGORY}:")
            logger.info(f"   • Initial tweets: {len(valid_tweets)}")
            logger.info(f"   • After URL deduplication: {len(url_deduped_tweets)}")
            logger.info(f"   • After content deduplication: {len(content_deduped_tweets)}")
            logger.info(f"   • After news worthiness filtering: {len(newsworthy_tweets)}")
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
