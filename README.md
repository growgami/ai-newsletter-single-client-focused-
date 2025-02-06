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

## 🔧 Prerequisites

- Python 3.10 or higher
- Node.js and npm
- 2GB RAM minimum
- Twitter/X account with TweetDeck access
- Telegram Bot Token
- DeepSeek API Key

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
```

## 🚀 Deployment

The service uses PM2 for process management and consists of two main processes:

1. **tweet-scraper**: Handles browser automation and tweet collection
2. **newsletter**: Processes tweets and sends updates

### Starting the Service

```bash
# Start all processes
pm2 start ecosystem.config.js

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
pm2 logs tweet-scraper
pm2 logs newsletter

# Restart processes
pm2 restart tweet-scraper
pm2 restart newsletter

# List all processes
pm2 list
```

## 📁 Project Structure

```
ai-newsletter/
├── ecosystem.config.js    # PM2 process configuration
├── scraper_process.py    # Tweet scraping orchestration
├── newsletter_process.py # Newsletter processing orchestration
├── browser_automation.py # Playwright browser automation
├── tweet_scraper.py     # Tweet collection logic
├── error_handler.py     # Error handling utilities
├── garbage_collector.py # Memory management
├── data/
│   ├── raw/            # Raw tweet data
│   ├── processed/      # Processed tweets
│   ├── filtered/       # Filtered content
│   └── session/        # Browser session data
└── logs/               # Application logs
```

## 🔍 Troubleshooting

Common issues and solutions:

1. **Browser Automation Issues**
   - Check browser session in data/session/
   - Verify Twitter credentials
   - Review tweet_scraper_error.log

2. **Processing Issues**
   - Check newsletter_error.log
   - Verify API keys
   - Monitor memory usage with `pm2 monit`

3. **Process Management**
   - Use `pm2 logs` to check for errors
   - Monitor process restarts with `pm2 list`
   - Check system resources with `top` or `htop`

## 📄 License

Copyright © 2024 Growgami. All rights reserved. 