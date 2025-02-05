import logging
import json
from pathlib import Path
from datetime import datetime, timedelta
import zoneinfo
import asyncio
import aiohttp
import re
from openai import AsyncOpenAI
import signal

logger = logging.getLogger(__name__)

class CircuitBreaker:
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

class ContentFilter:
    def _calculate_chunk_size(self, items):
        """Calculate optimal chunk size based on content length"""
        if not items or len(items) <= 2:
            return 2  # Minimum chunk size
            
        # Calculate average content length including all text fields
        total_length = 0
        for item in items:
            text = item.get('tweet', '')
            quoted = item.get('quoted_content', '')
            reposted = item.get('reposted_content', '')
            total_length += len(text) + len(quoted) + len(reposted)
            
        avg_length = total_length / len(items)
        
        # Estimate tokens (1 token ~4 chars)
        # Target max 2048 tokens for prompt (half of 4096 max)
        # Leave room for system prompt and response
        max_tokens = 2048
        tokens_per_item = max(1, avg_length / 4)  # Ensure at least 1 token per item
        
        # Calculate how many items we can fit
        optimal_size = int(max_tokens / tokens_per_item)
        
        # Clamp between 2 and 5 items
        return max(2, min(5, optimal_size))
        
    def __init__(self, config):
        self.config = config
        self.data_dir = Path('data')
        self.input_dir = self.data_dir / 'filtered' / 'alpha_filtered'  # Input from alpha filter
        self.output_dir = self.data_dir / 'filtered' / 'content_filtered'  # Output directory
        self.state_file = self.output_dir / 'state.json'  # Track processing state
        
        # Initialize API clients
        self.deepseek_api_key = config['deepseek_api_key']
        self.openai_api_key = config['openai_api_key']
        
        self.deepseek_client = AsyncOpenAI(
            api_key=self.deepseek_api_key,
            base_url="https://api.deepseek.com"
        )
        self.openai_client = AsyncOpenAI(api_key=self.openai_api_key)
        
        self.circuit_breaker = CircuitBreaker()
        self.is_shutting_down = False
        self.force_exit = False
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_state(self):
        """Load processing state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state file: {str(e)}")
        return {
            'last_run_date': None,
            'last_processed_date': None
        }

    def _save_state(self, state):
        """Save processing state to file"""
        try:
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            temp_file.replace(self.state_file)  # Atomic write
        except Exception as e:
            logger.error(f"Error saving state file: {str(e)}")
            if temp_file.exists():
                temp_file.unlink()

    def _should_run_content_filter(self):
        """Check if content filter should run based on last run date"""
        try:
            state = self._load_state()
            last_run = state.get('last_run_date')
            
            if not last_run:
                return True
                
            last_run_date = datetime.strptime(last_run, '%Y%m%d')
            current_date = datetime.now(zoneinfo.ZoneInfo("UTC"))
            days_since_last_run = (current_date - last_run_date).days
            
            return days_since_last_run >= 3
            
        except Exception as e:
            logger.error(f"Error checking run state: {str(e)}")
            return False

    async def cleanup(self):
        """Cleanup before shutdown"""
        if not self.is_shutting_down:
            self.is_shutting_down = True
            logger.info("Cleaning up before shutdown...")
            # Add any cleanup tasks here
            await asyncio.sleep(0.5)  # Brief pause for cleanup

    async def _try_deepseek_request(self, prompt):
        """Attempt to get a response from Deepseek"""
        try:
            # Add 3 second timeout for Deepseek
            response = await asyncio.wait_for(
                self.deepseek_client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=500
                ),
                timeout=3  # 3 second timeout
            )
            
            if not response.choices:
                logger.warning("Deepseek response contains no choices")
                return None
                
            return response.choices[0].message.content.strip()
            
        except asyncio.TimeoutError:
            logger.warning("Deepseek request timed out after 3 seconds")
            return None
        except Exception as e:
            logger.warning(f"Deepseek request failed: {str(e)}")
            return None

    async def _try_openai_request(self, prompt):
        """Attempt to get a response from OpenAI"""
        try:
            # Add 10 second timeout for OpenAI
            response = await asyncio.wait_for(
                self.openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.3,
                    max_tokens=500
                ),
                timeout=10  # 10 second timeout
            )
            
            return response.choices[0].message.content.strip()
            
        except asyncio.TimeoutError:
            logger.warning("OpenAI request timed out after 10 seconds")
            return None
        except Exception as e:
            logger.warning(f"OpenAI request failed: {str(e)}")
            return None

    async def _extract_summary(self, tweet_text, reposted_text='', quoted_text='', category=''):
        """Extract an accurate and informative summary from all content sources"""
        try:
            await self.circuit_breaker.check()
            
            prompt = f"""
            Create a clear, factual summary combining key information from ALL provided content. Follow these rules:

            1. CONTENT TO ANALYZE:
            Main tweet: {tweet_text}
            Quoted content: {quoted_text}
            Reposted content: {reposted_text}

            2. SUMMARY REQUIREMENTS:
            - Use 6-12 words only
            - Include most critical information related to the tweets {category}
            - Keep exact numbers and symbols
            - Preserve token names ($BTC, $ETH)
            - Focus on key metrics/events
            - Remove unnecessary words

            3. WRITING STYLE:
            - Use confident, direct statements
            - Remove uncertain language (might, could, maybe)
            - Present market events as facts
            - Keep technical terms unchanged
            - Maintain professional tone

            4. EXAMPLES:
            Input: "Looks like $TOSHI might get listed on Coinbase soon! Hearing rumors of ~$750m daily volume"
            Output: "$TOSHI reaches $750M Coinbase daily volume"

            Input: "probably gonna see Hyperliquid hit new ATH this month, volume almost at $157b (was $156b in Dec)"
            Output: "Hyperliquid hits $157B monthly volume record"

            Input: "Jupiter protocol news! $JUP has been ranging between $1.30 - $0.70 for 9 months. 50% of protocol fees now going to JUP buybacks, 30% total supply burned"
            Output: "Jupiter allocates 50% fees for buybacks"

            Return ONLY the summary text, no explanations.
            """
            
            # Retry configuration
            max_retries = 3
            base_delay = 2  # Start with 2 second delay
            
            for attempt in range(max_retries):
                try:
                    # First try Deepseek
                    logger.debug(f"Attempt {attempt + 1}: Trying Deepseek")
                    response = await self._try_deepseek_request(prompt)
                    
                    # If Deepseek fails, try OpenAI
                    if response is None:
                        logger.debug(f"Attempt {attempt + 1}: Deepseek failed, trying OpenAI")
                        response = await self._try_openai_request(prompt)
                        
                    # If both failed, try next attempt or use fallback
                    if response is None:
                        if attempt < max_retries - 1:
                            delay = base_delay * (2 ** attempt)  # 2s, 4s, 8s
                            logger.info(f"Both APIs failed. Retrying in {delay} seconds...")
                            await asyncio.sleep(delay)
                            continue
                        else:
                            logger.warning(f"All attempts failed after {max_retries} retries")
                            return self._create_fallback_summary(tweet_text)
                    
                    # Clean up response
                    extracted = response.strip().strip('"').strip()
                    
                    # Verify word count (5-8 words)
                    word_count = len(extracted.split())
                    if not (6 <= word_count <= 12):
                        logger.warning(f"❌ Summary removed: Word count {word_count} outside 5-8 range")
                        return self._create_fallback_summary(tweet_text)
                    
                    # Verify all numbers and symbols are preserved
                    source_text = ' '.join([t for t in [tweet_text, reposted_text, quoted_text] if t])
                    numbers_symbols = re.findall(r'\$[\d.]+ *[KMBkmb]?|\$[A-Za-z]+|\d+(?:\.\d+)?%?', source_text)
                    
                    if numbers_symbols and not any(num in extracted for num in numbers_symbols):
                        logger.warning(f"❌ Summary removed: Missing important numbers/symbols: {', '.join(numbers_symbols)}")
                        return self._create_fallback_summary(tweet_text)
                    
                    logger.info(f"✅ Summary created: {extracted}")
                    return extracted
                    
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1}")
                    continue
                except Exception as e:
                    if "Circuit breaker open" in str(e):
                        logger.warning("Circuit breaker open - skipping")
                        return self._create_fallback_summary(tweet_text)
                    else:
                        self.circuit_breaker.record_failure()
                        logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                        continue
            
            return self._create_fallback_summary(tweet_text)
            
        except Exception as e:
            logger.error(f"Error in summary extraction: {str(e)}")
            return self._create_fallback_summary(tweet_text)

    def _create_fallback_summary(self, text):
        """Create a fallback summary when extraction fails"""
        try:
            # Split into sentences
            sentences = re.split(r'[.!?]+', text)
            
            for sentence in sentences:
                sentence = sentence.strip()
                # Find sentence with numbers, symbols, or meaningful content
                if (re.search(r'\$[\d.]+ *[KMBkmb]?|\$[A-Za-z]+|\d+(?:\.\d+)?%?', sentence) or
                    len(sentence.split()) >= 5) and len(sentence) <= 200:
                    return sentence
                    
            # If no good sentence found, return first substantial sentence
            for sentence in sentences:
                sentence = sentence.strip()
                if len(sentence) >= 20 and not sentence.startswith(('RT', '@', '#')):
                    return sentence
                    
            # Last resort: clean first sentence
            first_sentence = sentences[0].strip()
            return re.sub(r'^\W+|\W+$', '', first_sentence)
        except Exception as e:
            logger.error(f"Error creating fallback summary: {str(e)}")
            return text[:200]  # Fallback to truncated text

    def _extract_metrics(self, text):
        """Extract numerical metrics from text"""
        try:
            # Pattern for currency amounts with optional decimals
            currency_pattern = r'\$([0-9,]+(?:\.[0-9]+)?)'
            
            # Find all currency amounts
            amounts = re.findall(currency_pattern, text)
            
            # Convert to float, removing commas
            return [float(amount.replace(',', '')) for amount in amounts]
        except Exception as e:
            logger.error(f"Error extracting metrics: {str(e)}")
            return []

    def _is_metric_update(self, items):
        """Check if tweets are metric updates of the same type"""
        try:
            # Need at least 2 items to compare
            if len(items) < 2:
                return False, []
                
            # Check if all tweets are from the same attribution
            attributions = {item['attribution'] for item in items}
            if len(attributions) > 1:
                return False, []
                
            # Get metrics from each tweet
            tweet_metrics = []
            for item in items:
                metrics = self._extract_metrics(item['content'])
                if metrics:
                    tweet_metrics.append({
                        'content': item['content'],
                        'metrics': metrics,
                        'date': item.get('created_at', ''),
                        'index': items.index(item)
                    })
            
            # Check if we have consistent number of metrics across tweets
            if not tweet_metrics or not all(len(m['metrics']) == len(tweet_metrics[0]['metrics']) for m in tweet_metrics):
                return False, []
                
            # Sort by date, most recent first
            tweet_metrics.sort(key=lambda x: x['date'], reverse=True)
            
            # Check if metrics are consistently increasing/decreasing
            first_metrics = tweet_metrics[0]['metrics']
            is_update = True
            
            for i in range(1, len(tweet_metrics)):
                current_metrics = tweet_metrics[i]['metrics']
                # Check if all metrics show consistent change
                if not all(first_metrics[j] >= current_metrics[j] for j in range(len(first_metrics))):
                    is_update = False
                    break
            
            if is_update:
                # Return True and the index of the most recent tweet
                return True, [tweet_metrics[0]['index']]
                
            return False, []
            
        except Exception as e:
            logger.error(f"Error checking metric updates: {str(e)}")
            return False, []

    async def _check_duplicate_content(self, items):
        """Check for duplicate content among tweets and select the best version"""
        try:
            if len(items) <= 1:
                return items

            logger.info(f"\n📊 Analyzing {len(items)} tweets for duplicates...")
            
            # First check if these are metric updates
            is_metric_update, keep_indices = self._is_metric_update(items)
            if is_metric_update:
                kept_items = [items[idx] for idx in keep_indices]
                removed_count = len(items) - len(kept_items)
                
                # Log detailed metric update comparison
                logger.info("\n🔄 Metric Update Analysis:")
                logger.info("Found sequence of related metric updates:")
                for idx, item in enumerate(items):
                    metrics = self._extract_metrics(item['content'])
                    status = "✅ KEPT (most recent)" if idx in keep_indices else "❌ REMOVED (older)"
                    logger.info(f"{status} - {item['content'][:100]}...")
                    if metrics:
                        logger.info(f"   └─ Metrics: {', '.join(['$' + str(m) for m in metrics])}")
                    if 'original_date' in item:
                        logger.info(f"   └─ Date: {item['original_date']}")
                
                logger.info(f"\n📝 Summary: Kept most recent update, removed {removed_count} older versions")
                return kept_items

            # For non-metric duplicates, show detailed analysis
            logger.info("\n🔍 Content Similarity Analysis:")
            
            prompt = """Analyze these tweets and determine if they contain the same news/information.
For tweets reporting metrics or numbers (like prices, supply, TVL etc):
1. If they show the same type of metric increasing/decreasing over time, keep only the most recent update
2. Example of metric updates (keep only the first one):
   - "Printed $142,760,509 USDC; Supply: $52,073,308,257"
   - "Printed $136,330,961 USDC; Supply: $51,930,547,748"
   - "Printed $124,732,078 USDC; Supply: $51,794,216,787"

For other duplicate content, select the most informative version based on:
1. Specificity (more specific details are better)
2. Completeness (more context is better)
3. Clarity (clearer explanation is better)
4. Numbers and metrics (prefer tweets with specific numbers)

Tweets to analyze:
{}

Return ONLY a JSON object in this exact format:
{{
    "are_duplicates": boolean,
    "keep_item_ids": [integer array of indices to keep],
    "reason": "string explaining the decision (mention if metric update)",
    "confidence": float between 0 and 1,
    "comparison": [
        {{
            "id": integer,
            "status": "kept" or "removed",
            "reason": "string explaining why this specific tweet was kept or removed"
        }}
    ]
}}""".format(json.dumps([{
                'id': idx,
                'content': item['content'],
                'date': item.get('original_date', ''),
                'url': item['url']
            } for idx, item in enumerate(items)], indent=2))

            response = await self._try_deepseek_request(prompt)
            if not response:
                response = await self._try_openai_request(prompt)

            if response:
                try:
                    result = json.loads(response.strip())
                    if isinstance(result, dict) and result.get('are_duplicates') is not None and isinstance(result.get('keep_item_ids'), list):
                        # Keep only the selected items
                        kept_items = [items[idx] for idx in result['keep_item_ids'] if idx < len(items)]
                        if kept_items:  # Only log if we actually kept some items
                            removed_count = len(items) - len(kept_items)
                            if removed_count > 0:
                                logger.info("\n📋 Duplicate Analysis Results:")
                                logger.info(f"Overall decision: {result.get('reason', 'No reason provided')}")
                                logger.info(f"Confidence: {result.get('confidence', 0):.2f}")
                                
                                # Show detailed comparison for each tweet
                                logger.info("\nDetailed Tweet Analysis:")
                                for comparison in result.get('comparison', []):
                                    tweet_id = comparison.get('id', 0)
                                    if tweet_id < len(items):
                                        status_emoji = "✅" if comparison.get('status') == "kept" else "❌"
                                        logger.info(f"\n{status_emoji} Tweet {tweet_id + 1}:")
                                        logger.info(f"   Content: {items[tweet_id]['content'][:100]}...")
                                        logger.info(f"   Status: {comparison.get('status', 'unknown').upper()}")
                                        logger.info(f"   Reason: {comparison.get('reason', 'No specific reason provided')}")
                                        if 'original_date' in items[tweet_id]:
                                            logger.info(f"   Date: {items[tweet_id]['original_date']}")
                                
                                logger.info(f"\n Summary: Removed {removed_count} duplicate tweets, kept {len(kept_items)} unique tweets")
                            return kept_items
                except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
                    logger.error(f"Failed to parse duplicate detection response: {str(e)}")

            return items

        except Exception as e:
            logger.error(f"Error in duplicate detection: {str(e)}")
            return items

    async def process_column(self, items, column_id):
        """Process a single column of items"""
        try:
            filtered_items = []
            
            logger.info(f"Processing column {column_id}")
            
            # Process in chunks for rate limiting
            chunk_size = 10
            for i in range(0, len(items), chunk_size):
                chunk = items[i:i+chunk_size]
                chunk_filtered = []
                
                # Process each item in chunk
                for item in chunk:
                    try:
                        tweet_text = item.get('tweet', '')
                        reposted_text = item.get('reposted_content', '')
                        quoted_text = item.get('quoted_content', '')
                        
                        extracted_text = await self._extract_summary(tweet_text, reposted_text, quoted_text)
                        
                        if extracted_text:
                            # Format the processed_date into YYYY-MM-DD format
                            processed_date = item.get('processed_date', '')
                            if processed_date:
                                try:
                                    # Convert YYYYMMDD to YYYY-MM-DD format
                                    date_obj = datetime.strptime(processed_date, '%Y%m%d')
                                    formatted_date = date_obj.strftime('%Y-%m-%d')
                                except ValueError:
                                    formatted_date = ''
                            else:
                                formatted_date = ''
                            
                            filtered_item = {
                                'attribution': item.get('author', ''),
                                'content': extracted_text,
                                'url': item.get('url', ''),
                                'original_date': formatted_date
                            }
                            chunk_filtered.append(filtered_item)
                            
                    except Exception as e:
                        logger.error(f"Error processing item: {str(e)}")
                        continue
                
                # Check for duplicates within the chunk
                if chunk_filtered:
                    chunk_filtered = await self._check_duplicate_content(chunk_filtered)
                    filtered_items.extend(chunk_filtered)
                
                # Rate limiting between chunks
                if i + chunk_size < len(items):
                    await asyncio.sleep(2)
            
            # Final duplicate check across all items
            if len(filtered_items) > 1:
                filtered_items = await self._check_duplicate_content(filtered_items)
            
            if not filtered_items:
                logger.warning(f"No tweets found in column {column_id}")
                return []
            
            logger.info(f"Found {len(filtered_items)} unique tweets")
            
            return {
                'tweets': filtered_items
            }
            
        except Exception as e:
            logger.error(f"Error in process_column: {str(e)}")
            return []

    def _get_category_name(self, column_id):
        """Get category name from column id"""
        category_map = {
            '0': '$TRUMP',
            '1': 'Stablecoins',
            '2': 'SEI',
            '3': 'SUI',
            '4': 'Marketing',
            '5': 'Yappers'
        }
        return category_map.get(column_id, f'Category_{column_id}')

    async def filter_content(self, date_str=None):
        """Process and filter content"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                date_str = current_time.strftime('%Y%m%d')
                
            logger.info(f"Starting content filtering for {date_str}")
            
            # Find all column files
            column_files = list(self.input_dir.glob('column_*.json'))
            if not column_files:
                logger.error(f"No column files found in: {self.input_dir}")
                return
                
            logger.info(f"Found {len(column_files)} columns to process")
            
            # Process each column file
            for column_file in column_files:
                column_id = column_file.stem.split('_')[1]
                category_name = self._get_category_name(column_id)
                logger.info(f"Processing column {column_id} ({category_name})")
                
                try:
                    # Load column data
                    with open(column_file, 'r') as f:
                        column_data = json.load(f)
                        items = column_data.get('tweets', [])
                    
                    if not items:
                        continue
                        
                    logger.info(f"Column {column_id}: Found {len(items)} items")
                    
                    # Process column
                    result = await self.process_column(items, column_id)
                    
                    if result and result.get('tweets'):
                        # Create category-based structure
                        categorized_output = {
                            category_name: {
                                'tweets': result['tweets']
                            }
                        }
                        
                        # Save individual column file
                        output_file = self.output_dir / f'column_{column_id}.json'
                        with open(output_file, 'w') as f:
                            json.dump(categorized_output, f, indent=2)
                            
                        logger.info(f"Column {column_id}: Saved {len(result['tweets'])} items for {category_name}")
                    
                except Exception as e:
                    logger.error(f"Error processing column {column_id}: {str(e)}")
                    continue
                
                # Brief pause between columns
                if column_file != column_files[-1]:
                    await asyncio.sleep(5)
            
            logger.info("Content filtering complete")
            
        except Exception as e:
            logger.error(f"Error during content filtering: {str(e)}")
            raise
            
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
    
    # Get date to process
    import sys
    date_to_process = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d')
    
    # Load config
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    config = {
        'deepseek_api_key': os.getenv('DEEPSEEK_API_KEY'),
        'openai_api_key': os.getenv('OPENAI_API_KEY')
    }
    
    content_filter = ContentFilter(config)
    
    # Signal handling
    def signal_handler(signum, frame):
        if content_filter.is_shutting_down:
            logger.warning("Forcing immediate exit...")
            content_filter.force_exit = True
            sys.exit(1)
        else:
            logger.info("Initiating graceful shutdown (Ctrl+C again to force exit)...")
            asyncio.create_task(content_filter.cleanup())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        asyncio.run(content_filter.filter_content(date_to_process))
    except KeyboardInterrupt:
        if not content_filter.force_exit:
            logger.info("Waiting for cleanup to complete...")
            asyncio.run(content_filter.cleanup())
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1) 