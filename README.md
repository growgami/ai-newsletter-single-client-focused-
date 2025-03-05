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
  - Daily summaries at 4 AM UTC
  - Customizable delivery schedules

- **KOL Pump Integration**
  - Slack bot for real-time tweet monitoring
  - Automatic tweet scraping from shared URLs
  - Direct integration with content filtering pipeline
  - Instant processing of KOL (Key Opinion Leader) content
  - Preservation of Slack tweets through all filtering stages
  - Special categorization of content from Slack sources

## 🔧 Prerequisites

- Python 3.10 or higher
- Node.js and npm
- 2GB RAM minimum
- Twitter/X account with TweetDeck access
- Telegram Bot Token
- DeepSeek API Key
- Slack Bot Token and App Token
- Apify API Token

## 📦 Installation

1. **Clone the repository**
```bash
git clone [repository-url]
cd ai-newsletter
```

2. **Install Node.js and PM2**
```bash
# Install Node.js (Ubuntu/Debian)
curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
sudo apt-get install -y nodejs
sudo apt install npm -y
# Install PM2 globally
sudo npm install pm2 -g
```

3. **Install Python dependencies**
```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install python3 python3-pip

# Install Python packages
pip3 install -r requirements.txt

# Install Playwright browsers
playwright install chromium
playwright install-deps
```

4. **Configure environment variables**
Create a `.env` file:
```env
# Twitter/X Credentials
TWITTER_USERNAME=your_username
TWITTER_PASSWORD=your_password
TWITTER_VERIFICATION_CODE=your_2fa_code
TWEETDECK_URL=your_tweetdeck_url

# AI Integration
DEEPSEEK_API_KEY=your_api_key
OPENAI_API_KEY=your_api_key

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_GROWGAMI_CHANNEL_ID=your_growgami_channel_id
TELEGRAM_CATEGORY_CHANNEL_ID=your_category_channel_id

# Discord Configuration
DISCORD_GROWGAMI_WEBHOOK=your_growgami_webhook
DISCORD_CATEGORY_WEBHOOK=your_category_webhook

# Slack Integration
SLACK_BOT_TOKEN=your_slack_bot_token
SLACK_APP_TOKEN=your_slack_app_token
APIFY_API_TOKEN=your_apify_token

# Memory Management
BROWSER_MEMORY_THRESHOLD=512    # MB before browser restart
SWAP_THRESHOLD=60              # % before swap cleanup
MEMORY_THRESHOLD=70            # % for normal cleanup
CRITICAL_MEMORY_THRESHOLD=85   # % for aggressive cleanup
CHECK_INTERVAL=900             # 15 minutes between checks
MAX_DAYS_TO_KEEP=3            # Keep files for 3 days
MAX_FILE_SIZE_MB=25           # Maximum file size

# Processing Settings
ALPHA_THRESHOLD=0.8           # Threshold for alpha content filtering
RISK_THRESHOLD=0.4           # Threshold for risk assessment
```

5. **Customize Categories**
Edit `src/category_mapping.py` to configure your category. You only need to modify these two sections:

```python
# Primary category constant - MODIFY THIS
CATEGORY: str = 'YourCategory'  # e.g., 'Polkadot', 'Bitcoin', etc.

# Additional alpha signal considerations - MODIFY THIS
ALPHA_CONSIDERATIONS: List[str] = [
    'Place importance on discussions of your category',
    'Look out for specific technical terms'
]

# The following configurations are pre-set and don't need modification:
# - TELEGRAM_CHANNELS (uses values from .env)
# - DISCORD_WEBHOOKS (uses values from .env)
# - CATEGORY_KEYWORDS (auto-includes your category)
# - EMOJI_MAP (predefined for all categories)
```

Key configuration points:
- `CATEGORY`: Set your main category name (this affects all filtering)
- `ALPHA_CONSIDERATIONS`: Define what's important for your category
- All other configurations are pre-set and will work automatically

6. **Configure Slack App**

1. Create a New Slack App
   - Go to https://api.slack.com/apps
   - Click "Create New App"
   - Choose "From scratch"
   - Name your app (e.g., "Arbitrum News Bot")
   - Select your workspace

2. Setup OAuth & Permissions
   - Navigate to "OAuth & Permissions" in sidebar
   - Under "Scopes", add these Bot Token Scopes:
     ```
     app_mentions:read
     channels:history
     chat:write
     groups:history
     im:history
     ```
   - Save changes

3. Enable Socket Mode
   - Go to "Socket Mode" in sidebar
   - Toggle "Enable Socket Mode" to On
   - Create an App-Level Token when prompted
   - Give it the name "Socket Mode Token"
   - Add the `connections:write` scope
   - Copy the token starting with `xapp-` for your .env file

4. Setup Event Subscriptions
   - Go to "Event Subscriptions" in sidebar
   - Toggle "Enable Events" to On
   - Under "Subscribe to bot events", add:
     ```
     message.channels  # Receive messages in channels
     message.groups
     ```
   - Save changes

5. Install App to Workspace
   - Go to "Install App" in sidebar
   - Click "Install to Workspace"
   - Review and allow permissions
   - Copy the "Bot User OAuth Token" starting with `xoxb-` for your .env file

6. Add Environment Variables
   Add to your `.env` file:
   ```env
   SLACK_BOT_TOKEN=xoxb-your-bot-token    # Bot User OAuth Token
   SLACK_APP_TOKEN=xapp-your-app-token    # App-Level Token
   ```

7. Add Bot to Channels
   - In each Slack channel where you want the bot:
     ```
     /invite @YourBotName
     ```
   - Bot must be invited to monitor each channel

8. Test the Integration
   - In any channel with the bot:
     ```
     arbitrum https://twitter.com/arbitrum/status/123456789
     ```
   - Bot should respond with processing status
   - Successful response includes attribution and content summary

Important Notes:
- Both tokens are required for the bot to function
- Bot token (xoxb-) is for API actions
- App token (xapp-) is for Socket Mode connection
- Keep your tokens secure and never commit them
- Bot needs channel invite to monitor messages
- Messages must include category keyword ('arbitrum') and Twitter URL

## 🚀 Deployment

The service uses PM2 for process management and consists of three main processes:

1. **tweet_collector**: Handles browser automation and tweet collection
2. **newsletter_generator**: Processes tweets and generates newsletters
3. **slack_pump**: Manages Slack integration and KOL content processing

### Starting the Service

```bash
# Start all processes
pm2 start ecosystem.config.js

# Start individual processes
pm2 start ecosystem.config.js --only tweet_collector
pm2 start ecosystem.config.js --only newsletter_generator
pm2 start ecosystem.config.js --only slack_pump

# Save process list
pm2 save

# Setup PM2 startup script
pm2 startup
```

### Process Management

```bash
# Monitor processes
pm2 monit

# View logs
pm2 logs tweet_collector
pm2 logs newsletter_generator
pm2 logs slack_pump

# Restart processes
pm2 restart tweet_collector
pm2 restart newsletter_generator
pm2 restart slack_pump

# List all processes
pm2 list
```

## 📁 Project Structure

```
ai-newsletter/
├── ecosystem.config.js     # PM2 process configuration
├── src/
│   ├── __init__.py        # Python package marker
│   ├── tweet_collector.py  # Tweet collection orchestration
│   ├── newsletter_generator.py  # Newsletter generation orchestration
│   ├── slack_pump.py      # Slack bot integration
│   ├── core/
│   │   ├── __init__.py
│   │   ├── browser_automation.py  # Playwright browser automation
│   │   └── deck_scraper.py       # Tweet collection logic
│   ├── processors/
│   │   ├── __init__.py
│   │   ├── data_processor.py    # Raw data processing
│   │   ├── alpha_filter.py      # Initial content filtering
│   │   ├── content_filter.py    # Content relevance filtering
│   │   └── news_filter.py       # News categorization
│   ├── senders/
│   │   ├── __init__.py
│   │   ├── telegram_sender.py   # Telegram message sending
│   │   └── discord_sender.py    # Discord message sending
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── error_handler.py     # Error handling utilities
│   │   └── garbage_collector.py # Memory management
│   ├── category_mapping.py  # Category configuration
│   ├── data/
│   │   ├── raw/               # Raw tweet data
│   │   ├── processed/         # Processed tweets
│   │   ├── filtered/          # Filtered content
│   │   │   ├── alpha_filtered/    # Alpha filter output
│   │   │   ├── content_filtered/  # Content filter output
│   │   │   └── news_filtered/     # News filter output
│   │   └── session/           # Browser session data
│   └── logs/                  # Application logs
│       ├── tweet_collector.log
│       ├── newsletter_generator.log
│       └── slack_pump.log
└── requirements.txt      # Python dependencies
```

## 📊 Data Flow for Slack Tweets

The system preserves tweets from Slack throughout the entire filtering pipeline with this data flow:

1. **Slack Integration (`slack_pump.py`)**
   - Monitors Slack channels for Twitter/X URLs
   - Processes URLs using Apify API
   - Adds `from_slack: true` flag to mark tweets
   - Saves tweets directly to `combined_filtered.json`

2. **Content Filtering (`content_filter.py`)**
   - Identifies tweets with `from_slack: true` flag
   - Bypasses normal filtering for these tweets
   - Preserves all original fields and attributes
   - Logs preservation of Slack tweets

3. **News Filtering (`news_filter.py`)**
   - Preserves tweets with `from_slack: true` flag
   - Bypasses content deduplication process
   - Bypasses news worthiness filtering
   - Creates special "From Slack" subcategory when needed
   - Ensures Slack tweets appear in final output

4. **Alpha Filtering (`alpha_filter.py`)**
   - Identifies and preserves tweets with `from_slack: true` flag
   - Assigns high alpha scores (10.0) to Slack tweets
   - Adds "Slack source" as the alpha signal
   - Bypasses normal alpha filtering criteria

This implementation ensures that important content shared in Slack channels is automatically preserved throughout all filtering stages and appears in the final newsletter output.

## 🔍 Troubleshooting

Common issues and solutions:

1. **Browser Automation Issues**
   - Check browser session in src/data/session/
   - Verify Twitter credentials
   - Review tweet_collector.log
   - Check if browser process is properly terminated

2. **Processing Issues**
   - Check newsletter_generator.log
   - Verify API keys (DeepSeek and OpenAI)
   - Monitor memory usage with `pm2 monit`
   - Check filtered data in src/data/filtered/

3. **Slack Pump Issues**
   - Verify Slack bot and app tokens
   - Check slack_pump.log for errors
   - Ensure Apify API token is valid
   - Monitor Slack bot connection status
   - Verify tweets have `from_slack: true` flag set
   - Check if Slack tweets appear in `combined_filtered.json`

4. **Process Management**
   - Use `pm2 logs` to check for errors
   - Monitor process restarts with `pm2 list`
   - Check system resources with `top` or `htop`
   - Review PM2 error logs in src/logs/

5. **Slack Tweet Preservation Issues**
   - Check logs for "Preserving tweet from Slack" messages
   - Verify tweets are marked with `from_slack: true` flag
   - Ensure Slack tweets appear in final output
   - Look for special "From Slack" subcategory in results
   - Verify alpha scores for Slack tweets (should be 10.0)

6. **PM2 Process Configuration**
   - All processes run with `python3` interpreter
   - Processes auto-restart on exit codes [0, 1]
   - 30-second minimum uptime requirement
   - 10-second kill timeout for clean shutdown
   - Exponential backoff for restart delays
   - Maximum 10 restarts before requiring manual intervention

## 📄 License

Copyright © 2024 Growgami. All rights reserved. 