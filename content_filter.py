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
                    temperature=0.6,
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

    async def _extract_summary(self, tweet_text, reposted_text='', quoted_text='', author='', category=''):
        """Extract an accurate and informative summary from all content sources"""
        try:
            await self.circuit_breaker.check()
            
            prompt = f"""
Generate a concise, news-style summary from the provided content. Follow these rules exactly:

INPUT CONTENT:
Tweet: {tweet_text}
Quoted: {quoted_text}
Reposted: {reposted_text}
Author: {author}

SUMMARY STRUCTURE:
A summary must consist of two strictly separated parts that flow naturally together:
1. Attribution – Who is responsible for the action/achievement:
   - Use "{category}" when the content is about:
     * Ecosystem metrics (transaction volume, users, TVL)
     * Treasury/spending figures
     * Network performance (throughput, latency)
     * Official announcements
   - Use the tweet's author when they are:
     * Providing analysis/commentary
     * Making predictions
     * Sharing personal research/insights
     * Discussing third-party developments
   - Never credit a reporter/analyst for the project's own metrics or achievements
   - The attribution must connect naturally to the content that follows
   - NEVER include @ symbols in attributions - remove them if present in author names
   - Format author names as plain text

2. Content – A concise, news-like headline (5-15 words) that starts with a strong connector:
   - Choose connectors that create a natural flow from the attribution
   - The content should read as a natural continuation of the attribution
   - Avoid repeating the attribution subject in the content unless needed for clarity
   - Should be framed positively in relation to the subject
   - Example pairs:
     * "{category} reports record transaction growth"
     * "AnalystName explores rising network metrics"
     * "{category} reveals treasury figures"

VALID FORMATS (choose based on content type):
1. Analysis/Commentary:
   - Format: "Attribution on Content"
   - Example: "AnalystName on rising network metrics"
   - Best for: market analysis, trend observations, and general updates.
2. Official Updates/Actions:
   - Format: "Attribution [action_word] Content"
   - Example: "{category} reveals treasury balance"
   - Best for: official metrics, launches, announcements, and achievements.
3. Direct Statements/Predictions:
   - Format: "Attribution: 'Content'"
   - Example: "AnalystName: 'support level holds at $5.2'"
   - Best for: price predictions, direct quotes, or strong opinions.

FORMAT SELECTION RULES:
- For content with metrics/numbers, choose the best connector—be it a preposition or an action word—that naturally fits the content.
- For analysis or trends, use a lower-case preposition; determine the best one based on the content, DO NOT always use "on".
- For predictions or direct statements, use the colon format.
- Let your choice of connector be determined by the content; do not default to a specific word.
- Vary your language to keep each summary unique and natural.

CONTENT FORMATTING:
- The entire summary must be 5-15 words long.
- It should be clear, concise, and styled as a news headline.
- Include key metrics, time references, token symbols, and specific values over general terms.
- Start with a strong verb or lower-case preposition and use active voice.
- The attribution is not always the subject of the Content.
- Content MUST connect to the subject.
- Project name usage rules:
  * If the project is in the Attribution, do not repeat it in the Content
  * If another entity is the Attribution, you may include the project name in Content for clarity
  * When included, project name should add necessary context
- NEVER use unnecessary words or redundant information.

Return a JSON object in this exact format:
{{
    "attribution": "who is reporting/discussing (no prepositions/colons)",
    "content": "include the lower-case preposition/action_word/colon and the actual content"
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
                            
                        # Verify word count (5-15 words)
                        full_text = f"{attribution} {content}"
                        word_count = len(full_text.split())
                        if not (5 <= word_count <= 15):
                            logger.warning(f"❌ Summary removed: Word count {word_count} outside 5-15 range")
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
                    # Parse the date if it exists, otherwise use processed_at
                    date_str = item.get('original_date', '')
                    if date_str:
                        try:
                            date = datetime.strptime(date_str, '%a %b %d %H:%M:%S %z %Y')
                        except ValueError:
                            # If parsing fails, try processed_at
                            date = datetime.fromisoformat(item.get('processed_at', ''))
                    else:
                        # Default to processed_at if no original_date
                        date = datetime.fromisoformat(item.get('processed_at', ''))
                        
                    tweet_metrics.append({
                        'content': item['content'],
                        'metrics': metrics,
                        'date': date,
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
            
            prompt = """Analyze these tweets for negative sentiment and duplicates. Follow these steps in order:

STEP 1 - REMOVE NEGATIVE SENTIMENT:
- Remove ALL tweets with:
  * Negative words/phrases: "despite", "fails", "drop", "decline", "unnoticed", "yet", "invisible"
  * Critical or unfavorable framing
  * Negative comparisons or trends
  * NO EXCEPTIONS - Even if tweet contains useful metrics

STEP 2 - HANDLE DUPLICATES:
For remaining tweets, identify and handle similar content:
1. Group tweets that share:
   * Same metrics (e.g. specific amounts, percentages)
   * Same topic (e.g. treasury, transactions, growth)
   * Same time period references
2. From each group, keep ONLY ONE tweet that is:
   * Most recent AND
   * Most complete (has all relevant numbers)
   * Most clearly written
3. Remove ALL other tweets from the same group
4. NEVER keep multiple tweets about:
   * Same financial figures
   * Same growth percentages
   * Same time period metrics

Example of duplicates to remove:
- "reveals spending amount" vs "reveals spending amount and balance" (keep second - more complete)
- "reports growth percentage" vs "reports growth percentage with timeframe" (keep second - more specific)
- "reports metric increased" vs "reports metric increased by specific amount" (keep second - has numbers)

Tweets to analyze:
{}

Return ONLY a JSON object in this exact format:
{{
    "are_duplicates": boolean,
    "keep_item_ids": [integer array of tweets to keep],
    "reason": "string explaining which tweets were kept and why",
    "comparison": [
        {{
            "id": integer,
            "status": "kept" or "removed",
            "reason": "string explaining if removed due to sentiment or similarity"
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

    def _is_similar_content(self, content1, content2):
        """Check if two content strings are semantically similar"""
        # Remove common words and punctuation
        def clean_content(text):
            common_words = {'on', 'in', 'at', 'by', 'with', 'and', 'the', 'for', 'to', 'of', 'a', 'an'}
            words = text.lower().split()
            return ' '.join(w for w in words if w not in common_words)
        
        c1 = clean_content(content1)
        c2 = clean_content(content2)
        
        # Check for high word overlap
        words1 = set(c1.split())
        words2 = set(c2.split())
        overlap = len(words1.intersection(words2))
        max_words = max(len(words1), len(words2))
        
        if max_words == 0:
            return False
        
        similarity = overlap / max_words
        return similarity > 0.6  # 60% word overlap threshold

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
                                'original_date': item.get('processed_at', '')  # Use processed_at as original_date
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