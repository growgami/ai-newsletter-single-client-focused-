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
TWITTER_2FA=your_2fa_code
TWEETDECK_URL=https://tweetdeck.twitter.com/

# AI Integration
DEEPSEEK_API_KEY=your_api_key
OPENAI_API_KEY=your_api_key

# Telegram Configuration
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHANNEL_ID=your_channel_id

# Processing Settings
ALPHA_THRESHOLD=0.8
RISK_THRESHOLD=0.4

# KOL Pump Configuration
SLACK_BOT_TOKEN=your_slack_bot_token
SLACK_APP_TOKEN=your_slack_app_token
APIFY_API_TOKEN=your_apify_token
```

5. **Customize Categories**
Edit `category_mapping.py` to configure your desired categories:

```python
# Primary category constant - Change this to your main category
CATEGORY: str = 'YourCategory'  # e.g., 'Bitcoin', 'Ethereum', etc.

# Channel ID mapping - Add your Telegram channels
TELEGRAM_CHANNELS: Dict[str, str] = {
    'CHANNEL_NAME': os.getenv('TELEGRAM_CHANNEL_ID_ENV_VAR', ''),  # Add channel env var to .env
    # Add more channels as needed
}

# Keywords for category identification
CATEGORY_KEYWORDS: List[str] = [
    'keyword1',
    'keyword2',
    # Add relevant keywords in lowercase
]
```

Key configuration points:
- `CATEGORY`: Main category for content filtering (e.g., 'Polkadot', 'Bitcoin')
- `TELEGRAM_CHANNELS`: Map channel names to their Telegram IDs (add corresponding env vars)
- `CATEGORY_FOCUS`: Define 8-10 focus areas for content relevance
- `CATEGORY_KEYWORDS`: Keywords for identifying relevant content (lowercase)
- `EMOJI_MAP`: Predefined emojis for different content types (no need to modify)

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

1. **tweet_collection**: Handles browser automation and tweet collection
2. **tweet_summary**: Processes tweets and sends updates
3. **kol_pump**: Manages Slack integration and KOL content processing

### Starting the Service

```bash
# Start all processes
pm2 start ecosystem.config.js

# Start individual processes
pm2 start ecosystem.config.js --only tweet_collection
pm2 start ecosystem.config.js --only tweet_summary
pm2 start ecosystem.config.js --only kol_pump

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
pm2 logs tweet_collection
pm2 logs tweet_summary
pm2 logs kol_pump

# Restart processes
pm2 restart tweet_collection
pm2 restart tweet_summary
pm2 restart kol_pump

# List all processes
pm2 list
```

## 📁 Project Structure

```
ai-newsletter/
├── ecosystem.config.js     # PM2 process configuration
├── tweet_collection.py     # Tweet collection orchestration
├── tweet_summary.py        # Processing orchestration
├── kol_pump.py            # Slack bot integration
├── browser_automation.py   # Playwright browser automation
├── tweet_scraper.py        # Tweet collection logic
├── data_processor.py       # Raw data processing
├── alpha_filter.py         # Initial content filtering
├── content_filter.py       # Content relevance filtering
├── news_filter.py          # News categorization
├── telegram_sender.py      # Message formatting and sending
├── category_mapping.py     # Category configuration
├── error_handler.py        # Error handling utilities
├── data/
│   ├── raw/               # Raw tweet data
│   ├── processed/         # Processed tweets
│   ├── filtered/          # Filtered content
│   │   ├── alpha_filtered/    # Alpha filter output
│   │   ├── content_filtered/  # Content filter output
│   │   └── news_filtered/     # News filter output
│   └── session/           # Browser session data
└── logs/                  # Application logs
    ├── tweet_collection.log
    ├── tweet_summary.log
    └── kol_pump.log
```

## 🔍 Troubleshooting

Common issues and solutions:

1. **Browser Automation Issues**
   - Check browser session in data/session/
   - Verify Twitter credentials
   - Review tweet_collection.log

2. **Processing Issues**
   - Check tweet_summary.log
   - Verify API keys (DeepSeek and OpenAI)
   - Monitor memory usage with `pm2 monit`
   - Check state.json files in filtered directories

3. **KOL Pump Issues**
   - Verify Slack bot and app tokens
   - Check kol_pump.log for errors
   - Ensure Apify API token is valid
   - Monitor Slack bot connection status

4. **Process Management**
   - Use `pm2 logs` to check for errors
   - Monitor process restarts with `pm2 list`
   - Check system resources with `top` or `htop`

## 📄 License

Copyright © 2024 Growgami. All rights reserved. 