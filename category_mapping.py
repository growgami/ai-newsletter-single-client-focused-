"""Category mappings and configurations for the news bot"""

from typing import Dict, List
import os

# Primary category constant
# Used in:
# - news_filter.py: For categorizing tweets
# - content_filter.py: For filtering relevant content
# - alpha_filter.py: For initial filtering
# - telegram_sender.py: For message routing
CATEGORY: str = 'Polkadot'

# Mapping of category to Telegram channel IDs
# Used in:
# - telegram_sender.py: For determining which channel to send messages to
# - newsletter_process.py: For validating channel configuration
TELEGRAM_CHANNEL_MAP: Dict[str, str] = {
    'Growgami': os.getenv('TELEGRAM_GROWGAMI_CHANNEL_ID', ''),  # General updates channel
    'Polkadot': os.getenv('TELEGRAM_POLKADOT_CHANNEL_ID', '')   # Category-specific channel
}

# Category-specific focus areas for news filtering
# Used in:
# - news_filter.py: For determining tweet relevance
# - content_filter.py: For content summarization
# - alpha_filter.py: For initial filtering criteria
CATEGORY_FOCUS: Dict[str, List[str]] = {
    CATEGORY: [
        'Layer-0 blockchain platform for cross-chain interoperability',
        'Built on Substrate framework with custom consensus mechanism',
        'Features parachain and parathread architecture',
        'Uses nominated proof-of-stake (NPoS) consensus',
        'Specialized for cross-chain communication and scalability',
        'Native DOT token for governance and parachain auctions',
        'Ecosystem of parachains and cross-chain applications',
        'Focus on interoperability and shared security model'
    ]
}

# Keywords for category identification
# Used in:
# - alpha_filter.py: For initial tweet filtering
# - content_filter.py: For relevance scoring
# - news_filter.py: For subcategory assignment
CATEGORY_KEYWORDS: List[str] = [
    'polkadot', 'dot', 'kusama', 'ksm', 'parachain',
    'substrate', 'web3 foundation', 'gavin wood',
    'parathread', 'xcm', 'cross-chain', 'relay chain',
    'astar', 'acala', 'moonbeam', 'interlay'
]

# Emoji mappings for subcategories
# Used in:
# - telegram_sender.py: For message formatting and visual categorization
# Each category has a unique emoji to improve readability
EMOJI_MAP: Dict[str, str] = {
    # Market & Trading - Used for price, volume, and trading related updates
    'Market': '📊',
    'Markets': '📊',
    'Trading': '📈',
    'Price': '💲',
    'Prices': '💲',
    'Investment': '💰',
    'Investments': '💰',
    'Volume': '📉',
    'Volumes': '📉',
    'Analysis': '🔍',
    'Analytics': '🔍',
    'Performance': '📈',
    'Metrics': '📏',
    'Stats': '📊',
    'Statistics': '📊',
    'Liquidity': '💧',
    'Volatility': '🌊',
    'Momentum': '🔄',
    'Trend': '📈',
    'Trends': '📈',
    'Bearish': '🐻',
    'Bullish': '🐂',
    
    # Technical & Development - Used for technical updates and development news
    'Technical': '⚙️',
    'Protocol': '⚡',
    'Protocols': '⚡',
    'Infrastructure': '🏗️',
    'Architecture': '🏛️',
    'Security': '🔒',
    'Development': '👨‍💻',
    'Developer': '👨‍💻',
    'Developers': '👨‍💻',
    'Tools': '🛠️',
    'Updates': '🔄',
    'Update': '🔄',
    'Upgrade': '⬆️',
    'Upgrades': '⬆️',
    'Bug': '🐛',
    'Bugs': '🐛',
    'Fix': '🔧',
    'Fixes': '🔧',
    'Code': '💻',
    'Coding': '💻',
    'Testing': '🧪',
    'Tests': '🧪',
    'Release': '🚀',
    'Releases': '🚀',
    
    # Integration & Partnerships - Used for ecosystem collaboration news
    'Integration': '🔌',
    'Integrations': '🔌',
    'Partnerships': '🤝',
    'Partnership': '🤝',
    'Partners': '🤝',
    'Ecosystem': '🌱',
    'Collaboration': '👥',
    'Collaborations': '👥',
    'Bridge': '🌉',
    'Bridges': '🌉',
    'Cross-chain': '⛓️',
    'Interoperability': '🔗',
    'Connection': '🔗',
    'Connections': '🔗',
    
    # Community & Governance - Used for community and governance updates
    'Community': '🏘️',
    'Communities': '🏘️',
    'Governance': '⚖️',
    'Treasury': '🏦',
    'DAO': '🏛️',
    'DAOs': '🏛️',
    'Proposal': '📜',
    'Proposals': '📜',
    'Vote': '🗳️',
    'Voting': '🗳️',
    'Votes': '🗳️',
    'Members': '👥',
    'Discussion': '💭',
    'Discussions': '💭',
    'Forum': '🗣️',
    'Forums': '🗣️',
    
    # Growth & Adoption - Used for adoption metrics and growth updates
    'Growth': '📈',
    'Adoption': '📱',
    'Adoptions': '📱',
    'TVL': '💹',
    'Launch': '🚀',
    'Launches': '🚀',
    'Scale': '📐',
    'Scaling': '📐',
    'Expansion': '🌍',
    'Global': '🌍',
    'Users': '👥',
    'User': '👤',
    'Onboarding': '🚪',
    'Milestone': '🏆',
    'Milestones': '🏆',
    'Success': '🎯',
    
    # AI & Innovation - Used for technology and innovation news
    'AI': '🤖',
    'Artificial': '🤖',
    'Intelligence': '🤖',
    'Machine': '⚙️',
    'Learning': '🧠',
    'Innovation': '💡',
    'Innovative': '💡',
    'Research': '🔬',
    'Science': '🔬',
    'Technology': '💻',
    'Tech': '💻',
    'Future': '🔮',
    'Smart': '🧠',
    'Neural': '🧠',
    'Data': '📊',
    
    # Marketing & Awareness - Used for marketing and promotional updates
    'Marketing': '📢',
    'Advertisement': '📣',
    'Advertising': '📣',
    'Awareness': '🌟',
    'Engagement': '🎯',
    'Token': '🪙',
    'Tokens': '🪙',
    'Coin': '💰',
    'Coins': '💰',
    'Brand': '™️',
    'Branding': '™️',
    'Campaign': '📣',
    'Campaigns': '📣',
    'Promotion': '📢',
    'Promotions': '📢',
    
    # DeFi & Financial - Used for DeFi and financial updates
    'DeFi': '🏦',
    'Finance': '💱',
    'Financial': '💱',
    'Lending': '💵',
    'Borrowing': '💸',
    'Yield': '🌾',
    'Yields': '🌾',
    'Farming': '👨‍🌾',
    'Staking': '🥩',
    'Stake': '🥩',
    'Swap': '🔄',
    'Swaps': '🔄',
    'Pool': '🏊',
    'Pools': '🏊',
    
    # Events & News - Used for event announcements and news updates
    'Event': '📅',
    'Events': '📅',
    'Conference': '🎤',
    'Conferences': '🎤',
    'Meetup': '🤝',
    'Meetups': '🤝',
    'News': '📰',
    'Update': '📝',
    'Updates': '📝',
    'Announcement': '📢',
    'Announcements': '📢',
    
    # Risk & Security - Used for security and risk-related updates
    'Risk': '⚠️',
    'Risks': '⚠️',
    'Warning': '⚠️',
    'Alert': '🚨',
    'Alerts': '🚨',
    'Security': '🔒',
    'Secure': '🔒',
    'Protection': '🛡️',
    'Safe': '🛡️',
    'Safety': '🛡️',
    'Audit': '🔍',
    'Audits': '🔍',
    
    # Project Status - Used for project progress and status updates
    'Status': '📊',
    'Progress': '⏳',
    'Roadmap': '🗺️',
    'Timeline': '⏰',
    'Phase': '📑',
    'Phases': '📑',
    'Stage': '📑',
    'Stages': '📑',
    'Complete': '✅',
    'Completed': '✅',
    'Pending': '⏳',
    'Active': '🟢',
    'Inactive': '🔴',
    
    # Default - Used for general categorization when specific category not found
    'Other': '📌',
    'Project': '📌',
    'Projects': '📌',
    'Misc': '📌',
    'General': '📌'
}

# Used in:
# - newsletter_process.py: For startup validation
# - telegram_sender.py: For channel validation
def validate_telegram_config():
    """Validate Telegram channel configuration
    Raises ValueError if any channel ID is missing"""
    missing = []
    for category, channel_id in TELEGRAM_CHANNEL_MAP.items():
        if not channel_id:
            missing.append(category)
    if missing:
        raise ValueError(f"Missing Telegram channel IDs for: {', '.join(missing)}") 