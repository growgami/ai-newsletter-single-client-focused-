module.exports = {
  apps: [
    {
      name: 'tweet-scraper',
      script: './scraper_process.py',
      interpreter: 'python3',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1'
      }
    },
    {
      name: 'ai_newsletter',
      script: './scheduler_process.py',
      interpreter: 'python3',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      env: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1'
      },
      error_file: './logs/pm2_error.log',
      out_file: './logs/pm2_out.log',
      time: true,
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      max_restarts: 10,
      min_uptime: '10s',
      restart_delay: 5000,
      kill_timeout: 5000,
      wait_ready: true,
      listen_timeout: 10000
    }
  ]
}; 