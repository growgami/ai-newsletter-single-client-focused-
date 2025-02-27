module.exports = {
  apps: [
    {
      name: 'tweet_collector',
      script: 'tweet_collector.py',
      interpreter: 'python3',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/tweet_collector_error.log',
      out_file: './logs/tweet_collector_out.log',
      log_file: './logs/tweet_collector.log',
      min_uptime: '30s',
      exitCodes: [1, 0],
      env: {
        NODE_ENV: 'development',
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      },
      env_production: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      },
    },
    {
      name: 'news_generator',
      script: 'news_generator.py',
      interpreter: 'python3',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/news_generator_error.log',
      out_file: './logs/news_generator_out.log',
      log_file: './logs/news_generator.log',
      min_uptime: '30s',
      max_restarts: 10,
      restart_delay: 5000,
      kill_timeout: 10000,
      exp_backoff_restart_delay: 100,
      exitCodes: [1, 0],
      env: {
        NODE_ENV: 'development',
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      },
      env_production: {
        NODE_ENV: 'production',
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      },
    },
    {
      name: 'slack_pump',
      script: 'slack_pump.py',
      interpreter: 'python3',
      version: '1.0',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/slack_pump_error.log',
      out_file: './logs/slack_pump_out.log',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      }
    }
  ]
};
