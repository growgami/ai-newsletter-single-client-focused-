"""Category mappings and configurations for the news bot"""

from typing import Dict, List
import os

# Mapping of column IDs to category names
CATEGORY_MAP: Dict[str, str] = {
    '0': '$TRUMP',
    '1': 'Stablecoins',
    '2': 'SEI',
    '3': 'SUI',
    '4': 'Marketing',
    '5': 'Yappers'
}

# Mapping of category names to Telegram channel keys
TELEGRAM_CHANNEL_MAP: Dict[str, str] = {
    '$TRUMP': 'TRUMP',
    'Stablecoins': 'STABLECOINS',
    'SEI': 'SEI',
    'SUI': 'SUI',
    'Marketing': 'MARKETING',
    'Yappers': 'YAPPERS'
}

# Category-specific focus areas for news filtering
CATEGORY_FOCUS: Dict[str, List[str]] = {
    '$TRUMP': [
        'Political Crypto Movements',
        'Meme Coin Dynamics',
        'Election-related Developments',
        'Celebrity Crypto Endorsements',
        'Regulatory Impacts'
    ],
    'Stablecoins': [
        'Stability Mechanisms',
        'Regulatory Developments',
        'Cross-chain Compatibility',
        'Adoption Metrics',
        'Reserve Audits'
    ],
    'SEI': [
        'Network Upgrades',
        'DeFi Ecosystem Growth',
        'Developer Activity',
        'Token Utility Expansions',
        'Strategic Partnerships'
    ],
    'SUI': [
        'Move Language Innovations',
        'Scalability Solutions',
        'Wallet Ecosystem',
        'NFT Developments',
        'Governance Proposals'
    ],
    'Marketing': [
        'Viral Campaigns',
        'Community Engagement',
        'Partnership Announcements',
        'Exchange Listings',
        'Social Media Trends'
    ],
    'Yappers': [
        'Influencer Activity Patterns',          # Tracking post frequency/volume trends
        'Alpha Leaks & Market Signals',          # Early project hints and market-moving claims
        'Viral Thread Analysis',                 # Break down most-shared content structures
        'Cross-Platform Influence',              # Track Twitter/Farcaster/YouTube dominance
        'Community Sentiment Drivers',           # Identify key opinion leaders shaping narratives
        'Emerging Thought Leaders',              # Monitor rising stars in crypto commentary
        'Controversy & Debate Impact',           # Measure FUD/FOMO generation effectiveness
        'Project Endorsement Patterns',          # Track shilling cycles and partnership reveals
        'Technical Analysis Trends',             # Popular charting methods and indicators
        'Regulatory Commentary'                  # Influence on policy perception
    ]
}

# Emoji mappings for subcategories
EMOJI_MAP: Dict[str, str] = {
    # Technical & Development
    'Protocol Development': '⚡',
    'Technical Infrastructure': '🔧',
    'Infrastructure Development': '🔧',
    'Network Security': '🔒',
    'Developer Tools': '🛠️',
    
    # Integration & Partnerships
    'Cross-chain Integration': '🌉',
    'Industry Partnerships': '🤝',
    'Ecosystem Partnerships': '🤝',
    'IoT Integration': '📱',
    
    # Governance & Community
    'Governance': '⚖️',
    'Treasury': '💰',
    'DAO Activities': '🏛️',
    
    # Growth & Adoption
    'Ecosystem Growth': '📈',
    'Adoption': '🚀',
    'TVL': '💹',
    
    # AI & Innovation
    'AI Integration': '🤖',
    'AI Development': '🧠',
    'AI Safety': '🛡️',
    'Multi-agent Systems': '🎯',
    
    # Default
    'Other Updates': '📌'
}

CATEGORY_KEYWORDS: Dict[str, List[str]] = {
    'TRUMP Coin': [
        'trump', 'maga', 'donald trump', 'trump coin',
        'trump token', 'maga coin', 'political crypto'
    ],
    'Stablecoins': [
        'stablecoin', 'usdt', 'usdc', 'dai', 'frax',
        'peg', 'collateral', 'price stability'
    ],
    'SEI Economy': [
        'sei network', 'sei blockchain', 'sei ecosystem',
        'sei token', 'sei chain', 'cosmos sei'
    ],
    'SUI Economy': [
        'sui network', 'sui blockchain', 'sui ecosystem',
        'sui token', 'sui chain', 'mysten labs'
    ],
    'Marketing': [
        'partnership', 'ama', 'community update',
        'branding', 'campaign', 'outreach'
    ],
    'Yappers': [
        'community discussion', 'governance talk',
        'ecosystem debate', 'town hall', 'community chat'
    ]
}

def validate_telegram_config():
    missing = []
    for category, channel_id in TELEGRAM_CHANNEL_MAP.items():
        if not channel_id:
            missing.append(category)
    if missing:
        raise ValueError(f"Missing Telegram channel IDs for: {', '.join(missing)}") 