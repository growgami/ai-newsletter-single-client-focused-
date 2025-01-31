"""News filtering and categorization service"""

import logging
import json
from pathlib import Path
from datetime import datetime
import asyncio
from openai import AsyncOpenAI
from collections import defaultdict
from category_mapping import CATEGORY_MAP, CATEGORY_FOCUS
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
        self.input_dir = self.data_dir / 'filtered' / 'content_filtered'
        self.output_dir = self.data_dir / 'filtered' / 'news_filtered'
        
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

    def _build_prompt(self, tweets, category):
        """Build prompt for categorizing tweets into dynamic subcategories"""
        # Get category description from mapping
        category_desc = CATEGORY_FOCUS.get(category, [])
        category_context = '\n'.join([f'- {desc}' for desc in category_desc])
        
        return f"""
        You are analyzing tweets for the {category} category. First, validate each tweet's relevance using this category context:

        CATEGORY CONTEXT:
        {category_context}

        Then, group relevant tweets into 2-4 logical subcategories. Exclude any tweets that don't match the category context.

        TWEETS TO ANALYZE:
        {json.dumps(tweets, indent=2)}

        REQUIRED OUTPUT FORMAT:
        {{
            "{category}": {{
                "Subcategory Name": [
                    {{
                        "author": "handle",
                        "text": "exact tweet text",
                        "url": "tweet_url"
                    }}
                ]
            }}
        }}

        Rules:
        1. ONLY include tweets that clearly relate to {category} based on the category context
        2. Create 2-4 clear, descriptive subcategories for the relevant tweets
        3. Each relevant tweet must be in exactly one subcategory
        4. Preserve exact tweet text and metadata
        5. Irrelevant tweets should be excluded completely

        Example Output Structure:
        {{
            "SUI": {{
                "Project Launches": [
                    {{
                        "author": "lianyanshe",
                        "text": "Walrus, the top project on Sui, will launch its mainnet...",
                        "url": "https://twitter.com/..."
                    }}
                ],
                "Investment and Market Dynamics": [
                    {{
                        "author": "EmanAbio",
                        "text": "Sui claims it doesn't need Solana apps...",
                        "url": "https://twitter.com/..."
                    }}
                ]
            }}
        }}

        Return ONLY the JSON output, no explanations.
        """

    async def process_column(self, column_file):
        """Process a single column file"""
        try:
            logger.info(f"Starting column {column_file.name}")
            
            with open(column_file) as f:
                data = json.load(f)
            
            category = list(data.keys())[0]
            original_tweets = data[category]['tweets']
            
            if not original_tweets:
                logger.warning(f"No tweets found in column {column_file.name}")
                return False
            
            logger.info(f"Processing {len(original_tweets)} tweets from {category}")
            
            prompt = self._build_prompt(original_tweets, category)
            response = await self._api_request(prompt)
            
            if response:
                try:
                    # Log raw response for debugging
                    logger.debug(f"Raw response: {response}")
                    
                    # Validate JSON structure
                    result = json.loads(response)
                    if not isinstance(result, dict):
                        logger.error(f"Invalid response format - not a dictionary: {response}")
                        return False
                        
                    if category not in result:
                        logger.error(f"Missing category '{category}' in response: {response}")
                        return False
                        
                    # Validate subcategories and tweets
                    subcategories = result[category]
                    if not isinstance(subcategories, dict) or not subcategories:
                        logger.error(f"Invalid subcategories format: {subcategories}")
                        return False
                    
                    # Count total filtered tweets
                    filtered_tweets = sum(len(tweets) for tweets in subcategories.values())
                    filtered_out = len(original_tweets) - filtered_tweets
                    
                    # Log filtering results
                    logger.info(f"📊 Filtering Results for {category}:")
                    logger.info(f"   • Original tweets: {len(original_tweets)}")
                    logger.info(f"   • Kept tweets: {filtered_tweets}")
                    logger.info(f"   • Filtered out: {filtered_out}")
                    logger.info(f"   • Subcategories created: {len(subcategories)}")
                    for subcat, tweets in subcategories.items():
                        logger.info(f"     - {subcat}: {len(tweets)} tweets")
                    
                    # Check each subcategory has required tweet fields
                    for subcat, tweets in subcategories.items():
                        if not isinstance(tweets, list):
                            logger.error(f"Invalid tweets format in {subcat}: {tweets}")
                            return False
                        for tweet in tweets:
                            required_fields = ['author', 'text', 'url']
                            missing = [f for f in required_fields if f not in tweet]
                            if missing:
                                logger.error(f"Missing required fields {missing} in tweet: {tweet}")
                                return False
                    
                    # If validation passes, save the file
                    output_path = self.output_dir / f"{category.lower()}_summary.json"
                    temp_file = output_path.with_suffix('.tmp')
                    try:
                        with open(temp_file, 'w') as f:
                            json.dump(result, f, indent=2)
                        temp_file.replace(output_path)
                        logger.info(f"✅ Successfully saved {output_path.name}")
                        return True
                    except Exception as e:
                        logger.error(f"Error saving file: {str(e)}")
                        if temp_file.exists():
                            temp_file.unlink()
                        return False
                        
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON response: {str(e)}\nResponse: {response}")
                    return False
                except Exception as e:
                    logger.error(f"Error processing response: {str(e)}\nResponse: {response}")
                    return False
            
            logger.warning(f"No valid response for {column_file.name}")
            return False
            
        except Exception as e:
            logger.error(f"Error processing column {column_file.name}: {str(e)}")
            return False

    async def process_all(self):
        """Process all content-filtered columns one at a time"""
        try:
            # Find all column files
            columns = list(self.input_dir.glob('column_*.json'))
            if not columns:
                logger.info("No columns to process")
                return
            
            # Process one column at a time
            for column_file in columns:
                try:
                    success = await self.process_column(column_file)
                    if success:
                        logger.info(f"✅ {column_file.name} processed successfully")
                    else:
                        logger.warning(f"❌ {column_file.name} processing failed")
                    
                    # Brief pause between columns
                    if column_file != columns[-1]:
                        await asyncio.sleep(5)
                        
                except Exception as e:
                    logger.error(f"Error processing {column_file.name}: {str(e)}")
                    continue
                
        except Exception as e:
            logger.error(f"Error in process_all: {str(e)}")

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
