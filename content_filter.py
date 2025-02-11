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
from category_mapping import CATEGORY

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
            'last_processed_date': None,
            'last_chunk': 0,
            'total_chunks': 0,
            'completed': False
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

    def _get_input_file(self, date_str):
        """Get input file path from alpha_filter"""
        # Always use combined_filtered.json from alpha_filter output
        return self.input_dir / 'combined_filtered.json'
    
    def _get_output_file(self, date_str):
        """Get output file path"""
        return self.output_dir / 'combined_filtered.json'

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
            
            # Save current state if processing was interrupted
            state = self._load_state()
            if not state.get('completed', False):
                logger.info("Saving processing state before shutdown...")
                self._save_state(state)
            
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
                    response_format={"type": "json_object"},
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
                    response_format={"type": "json_object"},
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

    async def _extract_summary(self, tweet_text, reposted_text='', quoted_text='', author=''):
        """Extract an accurate and informative summary from all content sources"""
        try:
            await self.circuit_breaker.check()
            
            prompt = f"""
            Create a clear, factual summary combining key information from ALL provided content. Follow these rules:

            1. CONTENT TO ANALYZE:
            Main tweet: {tweet_text}
            Quoted content: {quoted_text}
            Reposted content: {reposted_text}

            2. ATTRIBUTION AND CONTENT FLOW:
            - Attribution and Content must form ONE natural sentence
            - Choose the most appropriate subject as Attribution:
              * Use project/platform name when:
                - Official announcements from their verified account (@SeiNetwork, @Binance)
                - Project metrics/stats from reliable sources
                - Project launches or updates from team members
                - Example: "Sei Surpasses $23B in Volume" (from @SeiNetwork)
              * Use organization name when:
                - Official announcements from their verified account
                - Organization metrics/stats from reliable sources
                - Example: "Binance Lists New Trading Pairs" (from @binance)
              * Use research firm name when:
                - Publishing official research/analysis from their account
                - Example: "Delphi Digital Analyzes Layer-2 Growth" (from @Delphi_Digital)
              * For personal content, use the ACTUAL TWEET AUTHOR:
                - For price predictions/analysis
                - For trading strategies
                - For personal opinions
                - The author of this tweet is: {author}
                - Example input: "Just analyzed $SEI price action"
                  Output: "{author}: $SEI Shows Strong Support at $0.33"
              * IMPORTANT: Token Symbol Rules
                - NEVER use token symbols ($BTC, $SEI, etc.) as attribution
                - When tweet is about a token, use the actual tweet author with colon
                  BAD: "$SEI Reaches Support at $0.33"
                  GOOD: "{author}: $SEI Support Level at $0.33"
              * For metrics and stats:
                - Use project name if metric directly relates to project AND from official account
                  GOOD: "Sei Reports 4.1M Weekly Active Users" (from @SeiNetwork)
                - Use actual tweet author if personal analysis/interpretation
                  GOOD: "{author}: $SEI Shows Increasing Volume"
            - Attribution should be the natural subject of the sentence
            - Content should be the predicate that completes the sentence

            3. ATTRIBUTION FORMATTING:
            - For project/platform names:
              * Use clean name without @ symbol
              * Example: "Sei", "Jupiter", "Berachain"
            - For organizations:
              * Use official name without @ symbol
              * Example: "Binance", "Coinbase"
            - For research firms:
              * Use official name
              * Example: "Delphi Digital", "Messari"
            - For authors (from tweet):
              * Use the exact author name provided above: {author}
              * Add colon when discussing token price/analysis
              * Example: "{author}: Analysis Shows Support Level"
            - Keep attribution concise and relevant
            - No verbs or connecting words in attribution (except colon where specified)

            IMPORTANT RULES:
            1. The author of this tweet is: {author}
            2. For personal analysis/predictions, use this exact author name
            3. Only use project names for official announcements from verified accounts
            4. When in doubt, use {author} with a colon

            4. CONTENT FORMATTING:
            - Content must start with a verb that connects naturally to attribution
            - CRITICAL: Preserve ALL numerical data and symbols:
              * Token symbols: Always keep $BTC, $ETH, $ANIME, etc.
              * Exact numbers: Keep all prices, percentages, volumes
              * Time references: Keep dates, durations, deadlines
              * Rankings: Keep position numbers (#1, #30, etc.)
              * Metrics: TVL, APR, APY, volume, market cap
            - If source has multiple numbers, prioritize the most significant ones
            - Use active voice and present tense
            - Focus on key metrics, updates, or findings
            - 8-15 words total (attribution + content combined)
            - Start content with connecting verbs like:
              * Action verbs: "Surpasses", "Reports", "Launches", "Reveals"
              * Analysis verbs: "Analyzes", "Predicts", "Shows", "Demonstrates"
              * Update verbs: "Achieves", "Reaches", "Hits", "Gains"

            5. EXAMPLES (showing natural flow):
            Input: "@peblo100xfinder: Sei network just hit $23B in perp volume! $SEI momentum building up"
            Output: "Sei Surpasses $23 Billion in Perpetual Volume as $SEI Momentum Builds"

            Input: "@research_firm: New report analyzing Binance token performance and drawdown"
            Output: "Presto Research Analyzes Maximum Drawdown Patterns in Binance Listings"

            Input: "@random_user: Coinbase users lost over $300M to scams this year according to @zachxbt investigation"
            Output: "ZachXBT Reveals Coinbase Users Lost Over $300M to Social Engineering"

            Input: "@wlf_intern: World Liberty Fi team announced new token acquisition strategy"
            Output: "World Liberty Fi Announces New Initiative to Acquire Project Tokens"

            Input: "@berachain_dev: The Honeypaper documentation is now available!"
            Output: "Berachain Launches Comprehensive Protocol Documentation 'The Honeypaper'"

            IMPORTANT: Your summary MUST:
            1. Use the most authoritative name as Attribution (not Twitter handles)
            2. Form a natural flowing sentence when combined
            3. Preserve ALL numerical values and token symbols
            4. Start Content with a verb that connects naturally to the Attribution
            5. Keep the total length between 8-15 words

            Return the summary in this exact format (JSON):
            {{
                "attribution": "the chosen attribution",
                "content": "the content starting with a verb"
            }}
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
                            return None
                    
                    # Parse the JSON response
                    try:
                        result = json.loads(response.strip())
                        attribution = result.get('attribution', '').strip()
                        content = result.get('content', '').strip()
                        
                        if not attribution or not content:
                            logger.warning("Missing attribution or content in response")
                            return None
                            
                        # Verify word count (8-15 words)
                        full_text = f"{attribution} {content}"
                        word_count = len(full_text.split())
                        if not (8 <= word_count <= 15):
                            logger.warning(f"❌ Summary removed: Word count {word_count} outside 8-15 range")
                            return None
                        
                        # Verify all numbers and symbols are preserved
                        source_text = ' '.join([t for t in [tweet_text, reposted_text, quoted_text] if t])
                        numbers_symbols = re.findall(r'\$[\d.]+ *[KMBkmb]?|\$[A-Za-z]+|\d+(?:\.\d+)?%?', source_text)
                        
                        if numbers_symbols and not any(num in full_text for num in numbers_symbols):
                            logger.warning(f"❌ Summary removed: Missing important numbers/symbols: {', '.join(numbers_symbols)}")
                            return None
                        
                        logger.info(f"✅ Summary created: {full_text}")
                        return {'attribution': attribution, 'content': content}
                        
                    except json.JSONDecodeError:
                        logger.warning(f"Failed to parse JSON response: {response}")
                        continue
                    
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout on attempt {attempt + 1}")
                    continue
                except Exception as e:
                    if "Circuit breaker open" in str(e):
                        logger.warning("Circuit breaker open - skipping")
                        return None
                    else:
                        self.circuit_breaker.record_failure()
                        logger.error(f"Error on attempt {attempt + 1}: {str(e)}")
                        continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error in summary extraction: {str(e)}")
            return None

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
                
            # Check if all tweets are about the same subject by comparing first word of content
            subjects = {item['content'].split()[0] for item in items}
            if len(subjects) > 1:
                return False, []
                
            # Get metrics from each tweet
            tweet_metrics = []
            for item in items:
                metrics = self._extract_metrics(item['content'])
                if metrics:
                    tweet_metrics.append({
                        'content': item['content'],
                        'metrics': metrics,
                        'date': item.get('original_date', ''),
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
                        tweet_text = item.get('tweet', '')  # Updated from alpha_filter output
                        reposted_text = item.get('reposted_content', '')  # Updated from alpha_filter output
                        quoted_text = item.get('quoted_content', '')  # Updated from alpha_filter output
                        author = item.get('author', '')  # Updated from alpha_filter output
                        
                        result = await self._extract_summary(tweet_text, reposted_text, quoted_text, author)
                        
                        if result:
                            # Create filtered item with original fields structure
                            filtered_item = {
                                'attribution': result['attribution'],
                                'content': result['content'],
                                'url': item.get('url', ''),
                                'original_date': item.get('original_date', '')
                            }
                            chunk_filtered.append(filtered_item)
                            
                    except Exception as e:
                        logger.error(f"Error processing item: {str(e)}")
                        continue
                
                # Check for duplicates within the chunk
                if chunk_filtered:
                    chunk_filtered = await self._check_duplicate_content(chunk_filtered)
                filtered_items.extend(chunk_filtered)
                
                # Brief pause between chunks
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
        """Get category name"""
        return CATEGORY

    async def filter_content(self, date_str=None):
        """Process and filter content"""
        try:
            if not date_str:
                current_time = datetime.now(zoneinfo.ZoneInfo("UTC"))
                date_str = current_time.strftime('%Y%m%d')
                
            logger.info(f"Starting content filtering for {date_str}")
            
            # Load input file from alpha_filter
            input_file = self._get_input_file(date_str)
            if not input_file.exists():
                logger.error(f"No input file found at: {input_file}")
                return
                
            try:
                with open(input_file, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                logger.error(f"Error loading input file: {str(e)}")
                return
            
            # Get tweets from input
            tweets = data.get('tweets', [])
            if not tweets:
                logger.warning("No tweets found in input file")
                return
            
            # Load existing output or create new structure
            output_file = self._get_output_file(date_str)
            if output_file.exists():
                try:
                    with open(output_file, 'r') as f:
                        output = json.load(f)
                except Exception as e:
                    logger.error(f"Error loading existing output: {str(e)}")
                    output = {
                        CATEGORY: {
                            'tweets': []
                        }
                    }
            else:
                output = {
                    CATEGORY: {
                        'tweets': []
                    }
                }
            
            # Process tweets in chunks for rate limiting
            chunk_size = 5  # Process 5 tweets at a time
            total_chunks = (len(tweets) + chunk_size - 1) // chunk_size
            
            # Get current state
            state = self._load_state()
            start_chunk = state.get('last_chunk', 0)
            
            logger.info(f"Processing {len(tweets)} tweets in {total_chunks} chunks, starting from chunk {start_chunk + 1}")
            
            for i in range(start_chunk * chunk_size, len(tweets), chunk_size):
                if self.is_shutting_down:
                    logger.info("Graceful shutdown requested...")
                    state['last_chunk'] = i // chunk_size
                    state['total_chunks'] = total_chunks
                    state['last_processed_date'] = date_str
                    self._save_state(state)
                    break
                
                chunk = tweets[i:i+chunk_size]
                chunk_number = i // chunk_size + 1
                
                logger.info(f"Processing chunk {chunk_number}/{total_chunks}")
                
                # Process chunk
                result = await self.process_column(chunk, "combined")
                
                if result and result.get('tweets'):
                    # Add new tweets to output under category
                    output[CATEGORY]['tweets'].extend(result['tweets'])
                    
                    # Save output atomically
                    try:
                        temp_file = output_file.with_suffix('.tmp')
                        with open(temp_file, 'w') as f:
                            json.dump(output, f, indent=2)
                        temp_file.replace(output_file)  # Atomic write
                        logger.info(f"Saved {len(result['tweets'])} new tweets (total: {len(output[CATEGORY]['tweets'])})")
                    except Exception as e:
                        logger.error(f"Error saving output: {str(e)}")
                        if temp_file.exists():
                            temp_file.unlink()
                
                # Update state
                state['last_chunk'] = chunk_number
                state['total_chunks'] = total_chunks
                state['last_processed_date'] = date_str
                self._save_state(state)
                
                # Rate limiting between chunks
                if i + chunk_size < len(tweets):
                    await asyncio.sleep(2)
            
            # Mark as completed if not shutdown
            if not self.is_shutting_down:
                state['completed'] = True
                state['last_run_date'] = date_str
                self._save_state(state)
                logger.info(f"Completed processing {date_str} with {len(output[CATEGORY]['tweets'])} filtered tweets")
            
            return output
            
        except Exception as e:
            logger.error(f"Error in filter_content: {str(e)}")
            return None
            
    def _validate_state(self, state):
        """Validate state structure and values"""
        try:
            required_fields = ['last_run_date', 'last_processed_date', 'last_chunk', 'total_chunks', 'completed']
            if not all(field in state for field in required_fields):
                logger.error("Invalid state structure")
                return False
            
            # Validate numeric fields
            if not isinstance(state['last_chunk'], int) or state['last_chunk'] < 0:
                logger.error("Invalid last_chunk value")
                return False
            
            if not isinstance(state['total_chunks'], int) or state['total_chunks'] < 0:
                logger.error("Invalid total_chunks value")
                return False
            
            # Validate date format if exists
            if state['last_processed_date']:
                try:
                    datetime.strptime(state['last_processed_date'], '%Y%m%d')
                except ValueError:
                    logger.error("Invalid date format in state")
                    return False
                    
            if state['last_run_date']:
                try:
                    datetime.strptime(state['last_run_date'], '%Y%m%d')
                except ValueError:
                    logger.error("Invalid date format in state")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"Error validating state: {str(e)}")
            return False

    def _validate_output_file(self, output_file):
        """Validate output file structure and content"""
        try:
            if not output_file.exists():
                return False
            
            with open(output_file, 'r') as f:
                data = json.load(f)
            
            # Check basic structure
            if not isinstance(data, dict):
                logger.error("Output file is not a valid JSON object")
                return False
            
            if CATEGORY not in data:
                logger.error(f"Missing category {CATEGORY} in output")
                return False
            
            if not isinstance(data[CATEGORY], dict):
                logger.error("Invalid category structure")
                return False
            
            if 'tweets' not in data[CATEGORY]:
                logger.error("Missing tweets array in category")
                return False
            
            if not isinstance(data[CATEGORY]['tweets'], list):
                logger.error("Tweets field is not an array")
                return False
            
            # Validate tweet structure if any exist
            for tweet in data[CATEGORY]['tweets']:
                required_tweet_fields = ['attribution', 'content', 'url', 'original_date']
                if not all(field in tweet for field in required_tweet_fields):
                    logger.error("Invalid tweet structure")
                    return False
            
            return True
        except json.JSONDecodeError:
            logger.error("Invalid JSON in output file")
            return False
        except Exception as e:
            logger.error(f"Error validating output file: {str(e)}")
            return False

    def reset_state(self):
        """Reset processing state to start fresh"""
        try:
            if self.state_file.exists():
                self.state_file.unlink()
            # Also clear output files
            output_file = self._get_output_file(None)
            if output_file.exists():
                output_file.unlink()
            logger.info("Reset processing state and cleared outputs")
        except Exception as e:
            logger.error(f"Error resetting state: {str(e)}")

    async def recover_state(self):
        """Emergency recovery of processing state"""
        try:
            logger.info("Starting emergency state recovery")
            
            # Check output file
            output_file = self._get_output_file(None)
            if self._validate_output_file(output_file):
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    total_tweets = len(data[CATEGORY]['tweets'])
                
                # Load input file to get total chunks
                input_file = self._get_input_file(None)
                if input_file.exists():
                    with open(input_file, 'r') as f:
                        input_data = json.load(f)
                        total_tweets_input = len(input_data.get('tweets', []))
                        chunk_size = 5  # Match our chunk size
                        total_chunks = (total_tweets_input + chunk_size - 1) // chunk_size
                else:
                    logger.error("Cannot find input file for recovery")
                    return False
                
                # Reconstruct state
                state = {
                    'last_processed_date': datetime.now(zoneinfo.ZoneInfo("UTC")).strftime('%Y%m%d'),
                    'last_run_date': None,  # Reset last run date
                    'last_chunk': 0,  # Reset to beginning to be safe
                    'total_chunks': total_chunks,
                    'completed': False
                }
                
                self._save_state(state)
                logger.info(f"Recovered state with {total_tweets} processed tweets")
                logger.info(f"Processing will resume from the beginning to ensure completeness")
                return True
                
        except Exception as e:
            logger.error(f"Error during state recovery: {str(e)}")
        return False

    def get_processing_progress(self):
        """Get current processing progress"""
        try:
            state = self._load_state()
            if not self._validate_state(state):
                return None
            
            output_file = self._get_output_file(None)
            output_valid = self._validate_output_file(output_file)
            
            # Get tweet counts if output is valid
            total_tweets = 0
            if output_valid:
                with open(output_file, 'r') as f:
                    data = json.load(f)
                    total_tweets = len(data[CATEGORY]['tweets'])
            
            progress = {
                'date': state['last_processed_date'],
                'progress': f"{state['last_chunk']}/{state['total_chunks']} chunks",
                'percentage': round((state['last_chunk'] / state['total_chunks'] * 100) if state['total_chunks'] > 0 else 0, 2),
                'completed': state['completed'],
                'output_valid': output_valid,
                'total_tweets_processed': total_tweets,
                'last_update': datetime.now(zoneinfo.ZoneInfo("UTC")).isoformat()
            }
            
            logger.info(f"Progress: {progress['percentage']}% ({progress['progress']}) - {total_tweets} tweets processed")
            return progress
            
        except Exception as e:
            logger.error(f"Error getting progress: {str(e)}")
            return None

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