module.exports = {
  apps: [
    {
      name: 'tweet_collection',
      script: 'tweet_collection.py',
      interpreter: 'python3',
      version: 'v2.0',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/tweet_collection_error.log',
      out_file: './logs/tweet_collection_out.log',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      }
    },
    {
      name: 'tweet_summary',
      script: 'tweet_summary.py',
      interpreter: 'python3',
      version: 'v2.0',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/tweet_summary_error.log',
      out_file: './logs/tweet_summary_out.log',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      }
    },
    {
      name: 'kol_pump',
      script: 'kol_pump.py',
      interpreter: 'python3',
      version: 'v1.0',
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      error_file: './logs/kol_pump_error.log',
      out_file: './logs/kol_pump_out.log',
      env: {
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8'
      }
    }
  ]
};
