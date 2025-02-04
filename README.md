# AI Newsletter Bot

An AI-powered news aggregator and summarizer for crypto and web3 content. The service scrapes tweets from Twitter/X, processes them using AI to generate categorized news summaries, and distributes them through Telegram channels.

## 🚀 Features

- **Automated Twitter/X Scraping**
  - Browser automation using Playwright
  - Continuous monitoring of TweetDeck columns
  - Smart session management and error recovery

- **AI-Powered Processing**
  - Advanced tweet scoring using DeepSeek API
  - Intelligent content categorization
  - Automated news filtering and summarization
  - Memory-optimized for 2GB RAM environments

- **Smart Distribution**
  - Automated Telegram channel distribution
  - Category-specific channels
  - Daily summaries at 6 AM UTC
  - Customizable delivery schedules

## 🔧 Prerequisites

- Python 3.10 or higher
- 2GB RAM minimum
- Twitter/X account with TweetDeck access
- Telegram Bot Token and Channel IDs
- DeepSeek API Key

## 📦 Installation

1. **Clone the repository**
```bash
git clone [repository-url]
cd ai-newsletter
```

2. **Set up Python environment**
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# For Linux/Mac:
source venv/bin/activate
# For Windows:
venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Install Playwright browsers**
```bash
playwright install
```

5. **Configure environment variables**
   Create a `.env` file using `.env.example` as template:
```env
# Twitter/X Credentials
TWITTER_USERNAME=your_username
TWITTER_PASSWORD=your_password
TWITTER_VERIFICATION_CODE=your_2fa_code
TWEETDECK_URL=https://tweetdeck.twitter.com/

# AI Integration
DEEPSEEK_API_KEY=your_api_key

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_POLKADOT_CHANNEL_ID=channel_id
TELEGRAM_IOTA_CHANNEL_ID=channel_id
TELEGRAM_ARBITRUM_CHANNEL_ID=channel_id
TELEGRAM_NEAR_CHANNEL_ID=channel_id
TELEGRAM_AI_AGENT_CHANNEL_ID=channel_id
TELEGRAM_DEFI_CHANNEL_ID=channel_id
TELEGRAM_TEST_CHANNEL_ID=channel_id

# Performance Settings
MONITOR_INTERVAL=0.1       # Tweet check interval in seconds
MAX_RETRIES=3             # Maximum retries for operations
RETRY_DELAY=2.0           # Base delay between retries
MAX_DAYS_TO_KEEP=7        # Days to keep historical data
MAX_FILE_SIZE_MB=50       # Maximum file size for garbage collection
GC_CHECK_INTERVAL=3600    # Garbage collection interval in seconds
```

## 🚀 Usage

### Starting the Bot
```bash
python main.py
```

### Automated Operations
The service will automatically:
1. Initialize browser and authenticate with Twitter/X
2. Monitor configured TweetDeck columns continuously
3. Process and score new tweets using DeepSeek API
4. Generate categorized summaries
5. Distribute content to appropriate Telegram channels
6. Perform daily cleanup and maintenance

## 📁 Project Structure

```
ai-newsletter/
├── main.py                 # Application entry point and orchestration
├── browser_automation.py   # Playwright-based browser automation
├── tweet_scraper.py       # Tweet collection and monitoring
├── data_processor.py      # Raw data processing pipeline
├── alpha_filter.py        # Tweet scoring and relevance filtering
├── content_filter.py      # Content refinement and categorization
├── news_filter.py         # News categorization and filtering
├── telegram_sender.py     # Telegram distribution system
├── garbage_collector.py   # Memory and storage management
├── category_mapping.py    # Category definitions and mapping
├── error_handler.py       # Error handling and retry logic
├── data/                  # Data storage
│   ├── raw/              # Raw scraped data
│   ├── processed/        # Processed data
│   ├── filtered/         # Filtered content
│   │   ├── alpha_filtered/   # Relevance-filtered content
│   │   ├── content_filtered/ # Category-filtered content
│   │   └── news_filtered/    # Final news summaries
│   └── session/          # Browser session data
└── logs/                 # Application logs
```

## 📊 Category Structure

The bot processes content for the following ecosystems:

### Main Categories
- **NEAR Ecosystem**: Development, partnerships, governance
- **Polkadot Ecosystem**: Parachains, crowdloans, governance
- **Arbitrum Ecosystem**: L2 developments, DeFi, governance
- **IOTA Ecosystem**: Smart contracts, IoT, partnerships
- **AI Agents**: AI developments, agent ecosystems
- **DeFi**: Protocols, trends, market analysis

Each category has specific filters and scoring criteria defined in `category_mapping.py`.

## 🚀 Deployment

### Production Deployment (Linux)

1. **Set up service file**
```bash
sudo cp newsbot.service /etc/systemd/system/
```

2. **Create service user and set permissions**
```bash
# Create service user
sudo useradd -r -s /bin/false newsbot

# Set ownership
sudo chown -R newsbot:newsbot /opt/ai_newsletter
```

3. **Enable and start service**
```bash
sudo systemctl enable newsbot
sudo systemctl start newsbot
```

### Monitoring

- Check service status:
```bash
sudo systemctl status newsbot
```

- View logs:
```bash
journalctl -u newsbot -f
```

## 🔍 Troubleshooting

Common issues and solutions:

1. **Browser Automation Issues**
   - Ensure Playwright browsers are installed
   - Check Twitter/X credentials
   - Verify 2FA settings

2. **Memory Issues**
   - Monitor RAM usage
   - Adjust garbage collection settings
   - Check log files for memory warnings

3. **API Rate Limits**
   - Monitor DeepSeek API usage
   - Adjust scraping intervals
   - Check error logs for rate limit messages

## 📄 License

Copyright © 2024 Growgami. All rights reserved.

This software is proprietary and confidential. Unauthorized copying, transfer, or reproduction of the contents of this software, via any medium, is strictly prohibited. 