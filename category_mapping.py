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
        'Meme cryptocurrency launched in 2023',
        'Built on Ethereum blockchain as an ERC-20 token',
        'Named after former US President Donald Trump',
        'Trading primarily on decentralized exchanges',
        'High social media and political event correlation',
        'Part of broader political token ecosystem',
        'Community driven by Trump supporters and crypto traders',
        'Known for high volatility during political events'
    ],
    'Stablecoins': [
        'Digital tokens designed to maintain fixed value (usually $1)',
        'Types include fiat-backed, crypto-backed, and algorithmic',
        'Core infrastructure for crypto trading and DeFi',
        'Major players: USDT (Tether), USDC (Circle), DAI (MakerDAO)',
        'Used for trading, savings, payments, and cross-border transfers',
        'Subject to increasing regulatory scrutiny worldwide',
        'Critical for market liquidity and price stability',
        'Backbone of crypto-fiat on/off ramps'
    ],
    'SEI': [
        'Layer-1 blockchain optimized for trading applications',
        'Built using Cosmos SDK with custom consensus mechanism',
        'Features parallel transaction processing (PPOR)',
        'Native built-in orderbook functionality',
        'Specialized for high-throughput DeFi applications',
        'Uses proof-of-stake consensus mechanism',
        'Integrated with Cosmos IBC for cross-chain operations',
        'Launched mainnet in 2023 with focus on DeFi scalability'
    ],
    'SUI': [
        'Layer-1 blockchain developed by Mysten Labs',
        'Uses Move programming language for smart contracts',
        'Horizontal scaling through parallel transaction processing',
        'Object-centric data model for efficient storage',
        'Implements Narwhal and Bullshark consensus',
        'Focus on NFTs and gaming applications',
        'Features dynamic gas fees and fast finality',
        'Launched mainnet in May 2023'
    ],
    'Marketing': [
        'Crypto marketing encompasses social media, content, and community',
        'Key channels include Twitter, Telegram, Discord, and YouTube',
        'Focus on community building and token awareness',
        'Involves influencer partnerships and AMAs',
        'Requires compliance with varying regulatory restrictions',
        'Emphasizes organic growth and community engagement',
        'Utilizes both traditional and crypto-native strategies',
        'Critical for project visibility and adoption'
    ],
    'Yappers': [
        'Crypto social media influencers and thought leaders',
        'Known for sharing market analysis and project reviews',
        'Range from anonymous to public personalities',
        'Significant impact on retail trader sentiment',
        'Platform presence across Twitter, YouTube, and Telegram',
        'Often early adopters of new projects and trends',
        'Mix of technical analysis and fundamental research',
        'Important for project discovery and adoption'
    ]
}

# Emoji mappings for subcategories
EMOJI_MAP: Dict[str, str] = {
    # Market & Trading
    'Market': '📊',
    'Trading': '💱',
    'Price': '📈',
    'Investment': '💸',
    'Volume': '📊',
    'Analysis': '📊',
    'Performance': '📈',
    
    # Technical
    'Technical': '⚙️',
    'Protocol': '⚡',
    'Infrastructure': '🔧',
    'Security': '🔒',
    'Development': '⚡',
    'Tools': '🛠️',
    'Updates': '🔄',
    
    # Integration
    'Integration': '🌉',
    'Partnerships': '🤝',
    'Partnership': '🤝',
    'Ecosystem': '🌱',
    
    # Community
    'Community': '👥',
    'Governance': '⚖️',
    'Treasury': '💰',
    'DAO': '🏛️',
    
    # Growth
    'Growth': '📈',
    'Adoption': '🚀',
    'TVL': '💹',
    'Launch': '🚀',
    'Launches': '🚀',
    
    # AI & Innovation
    'AI': '🤖',
    'Innovation': '💡',
    'Safety': '🛡️',
    
    # Marketing & Awareness
    'Marketing': '📢',
    'Awareness': '🌟',
    'Engagement': '🤝',
    'Token': '🪙',
    'Project': '📌',
    
    # Default
    'Other': '📌'
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