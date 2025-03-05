#!/usr/bin/env python
"""
Twitter List Scraper - Main Entry Point

This script scrapes tweets from Twitter lists organized by categories
and saves them to JSON files.
"""
import argparse
import os
import sys
import time
import concurrent.futures
from typing import Dict, List, Any
from pathlib import Path
from datetime import datetime

# Load environment variables from .env if present
try:
    from dotenv import load_dotenv
    env_file = Path('.env')
    if env_file.exists():
        print("Loading environment variables from .env file")
        load_dotenv()
except ImportError:
    print("dotenv package not installed. Skipping .env loading.")

from core.list_scraper import ListScraper
from core.file_handler import FileHandler

def _safe_set_env(env_var: str, value):
    """Safely set an environment variable, ensuring it's a string"""
    os.environ[env_var] = str(value)

def process_category(category: str, list_urls: List[str], file_handler: FileHandler) -> Dict[str, Any]:
    """Process a single category and return summary stats"""
    start_time = time.time()
    
    print(f"\nProcessing category: {category}")
    print(f"Lists to scrape: {len(list_urls)}")
    
    # Create list scraper for this category
    list_scraper = ListScraper()
    
    try:
        # Scrape tweets from all lists in this category
        category_tweets = list_scraper.scrape_category_lists(category, list_urls)
        
        # Save category tweets to a JSON file
        output_file = file_handler.save_category_tweets(category, category_tweets)
        
        # Return stats
        return {
            "category": category,
            "status": "success",
            "tweet_count": len(category_tweets),
            "list_count": len(list_urls),
            "processing_time": time.time() - start_time,
            "output_file": output_file
        }
    except Exception as e:
        print(f"Error processing category {category}: {str(e)}")
        return {
            "category": category,
            "status": "error",
            "error": str(e),
            "processing_time": time.time() - start_time
        }

def main():
    """Main entry point for the Twitter List Scraper"""
    # Track overall execution time
    start_time = time.time()
    
    parser = argparse.ArgumentParser(description='Scrape tweets from Twitter lists by category')
    parser.add_argument('--config', '-c', default='categories.json',
                        help='Path to categories configuration file (default: categories.json)')
    parser.add_argument('--output-dir', '-o', 
                        help='Directory to save output files (default: from .env or "output")')
    parser.add_argument('--workers', '-w', type=int,
                        help='Number of worker threads per list (default: 5)')
    parser.add_argument('--category-workers', '-cw', type=int, default=1,
                        help='Number of categories to process in parallel (default: 1)')
    parser.add_argument('--days', '-d', type=int,
                        help='Number of days to look back for tweets (default: 1)')
    parser.add_argument('--date', '-dt', 
                        help='Specific date to use for output folder in YYYYMMDD format (default: yesterday)')
    parser.add_argument('--rate-limit-wait', '-r', type=int,
                        help='Base seconds to wait when rate limited (default: 60)')
    parser.add_argument('--max-retries', '-m', type=int,
                        help='Maximum retries on rate limiting (default: 5)')
    parser.add_argument('--category', '-cat', 
                        help='Process only this category from the config file')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')
    args = parser.parse_args()
    
    # Set verbosity
    if args.verbose:
        _safe_set_env('VERBOSE', '1')
        print("Verbose logging enabled")
    
    # Override settings if specified in command line
    if args.output_dir:
        _safe_set_env('OUTPUT_DIR', args.output_dir)
        output_dir = Path(args.output_dir)
        if not output_dir.exists():
            print(f"Creating output directory: {args.output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
    
    if args.workers:
        _safe_set_env('MAX_SCRAPER_WORKERS', args.workers)
        print(f"Using {args.workers} worker threads per list")
        
    if args.days:
        _safe_set_env('DAYS_TO_SCRAPE', args.days)
        print(f"Scraping tweets from the past {args.days} days")
        
    if args.date:
        _safe_set_env('TARGET_DATE', args.date)
        print(f"Using specific date for output folder: {args.date}")
        
    if args.rate_limit_wait:
        _safe_set_env('RATE_LIMIT_WAIT', args.rate_limit_wait)
        print(f"Setting base rate limit wait time to {args.rate_limit_wait} seconds")
        
    if args.max_retries:
        _safe_set_env('MAX_RETRY_ATTEMPTS', args.max_retries)
        print(f"Setting maximum rate limit retries to {args.max_retries}")
    
    # Initialize file handler
    file_handler = FileHandler()
    
    # Load category configuration
    categories = file_handler.load_category_configuration(args.config)
    
    if not categories:
        print("No categories found. Please check your configuration file.")
        sys.exit(1)
    
    # Filter to a single category if specified
    if args.category and args.category in categories:
        print(f"Processing only category: {args.category}")
        categories = {args.category: categories[args.category]}
        
    print(f"Found {len(categories)} categories in configuration")
    
    # Process categories
    category_results = []
    
    if args.category_workers > 1 and len(categories) > 1:
        print(f"Processing {len(categories)} categories in parallel using {args.category_workers} workers")
        
        # Process categories in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.category_workers) as executor:
            # Submit all category processing jobs
            future_to_category = {
                executor.submit(process_category, category, list_urls, file_handler): category 
                for category, list_urls in categories.items()
            }
            
            # Process results as they come in
            for future in concurrent.futures.as_completed(future_to_category):
                category = future_to_category[future]
                try:
                    result = future.result()
                    category_results.append(result)
                except Exception as e:
                    print(f"Exception processing category {category}: {str(e)}")
                    category_results.append({
                        "category": category,
                        "status": "error",
                        "error": str(e)
                    })
    else:
        # Process categories sequentially
        for category, list_urls in categories.items():
            result = process_category(category, list_urls, file_handler)
            category_results.append(result)
    
    # Print summary
    print("\n=== Summary ===")
    print(f"Total categories processed: {len(category_results)}")
    
    success_count = sum(1 for r in category_results if r.get("status") == "success")
    print(f"Successful categories: {success_count}")
    
    error_count = sum(1 for r in category_results if r.get("status") == "error")
    print(f"Failed categories: {error_count}")
    
    total_tweets = sum(r.get("tweet_count", 0) for r in category_results)
    print(f"Total tweets collected: {total_tweets}")
    
    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.2f} seconds")
    
    if success_count > 0:
        avg_tweets_per_second = total_tweets / total_time if total_time > 0 else 0
        print(f"Average processing speed: {avg_tweets_per_second:.1f} tweets/second")
    
    # Show detailed category results
    print("\n=== Category Details ===")
    for result in category_results:
        if result.get("status") == "success":
            print(f"✓ {result['category']}: {result['tweet_count']} tweets in {result['processing_time']:.2f} seconds")
        else:
            print(f"✗ {result['category']}: FAILED - {result.get('error', 'Unknown error')}")
    
    # Exit with error code if any categories failed
    if error_count > 0:
        print("\nScraping completed with errors!")
        sys.exit(1)
    else:
        print("\nScraping completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()
