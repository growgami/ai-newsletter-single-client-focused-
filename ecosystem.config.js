module.exports = {
  apps: [
    {
      name: 'tweet-scraper',
      script: './scraper_process.py',
      interpreter: 'python3',
      autorestart: true,
      watch: false,
      max_memory_restart: '1.5G',
      env: {
        PYTHONUNBUFFERED: '1'
      },
      error_file: './logs/tweet_scraper_error.log',
      out_file: './logs/tweet_scraper_out.log',
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      kill_timeout: 5000,
      min_uptime: '10s',
      restart_delay: 5000
    },
    {
      name: 'newsletter',
      script: './newsletter_process.py',
      interpreter: 'python3',
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      env: {
        PYTHONUNBUFFERED: '1'
      },
      error_file: './logs/newsletter_error.log',
      out_file: './logs/newsletter_out.log',
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z'
    }
  ]
}; 