"""Category mappings and configurations for the news bot"""

from typing import Dict, List
import os
from dotenv import load_dotenv

# Primary category constant
# Used in:
# - news_filter.py: For categorizing tweets
# - content_filter.py: For filtering relevant content
# - alpha_filter.py: For initial filtering
# - telegram_sender.py: For message formatting
CATEGORY: str = 'Polkadot'

# Load environment variables first
load_dotenv()

# Channel ID mapping for Telegram
# Used in:
# - telegram_sender.py: For sending messages to configured channels
TELEGRAM_CHANNELS: Dict[str, str] = {
    'GROWGAMI': os.getenv('TELEGRAM_GROWGAMI_CHANNEL_ID', ''),
    'POLKADOT': os.getenv('TELEGRAM_POLKADOT_CHANNEL_ID', '')
}

# Category-specific focus areas for news filtering
# Used in:
# - news_filter.py: For determining tweet relevance
CATEGORY_FOCUS: Dict[str, List[str]] = {
    CATEGORY: [
        'Parachain slot auctions and crowdloans - DOT token utility and staking',
        'Cross-chain messaging (XCM) updates and integrations between parachains',
        'Network upgrades affecting performance, security, or scalability',
        'Ecosystem growth metrics (TVL, active addresses, transaction volume)',
        'Major parachain launches, upgrades, or significant milestones',
        'Governance proposals and treasury funding decisions',
        'Strategic partnerships and enterprise adoption',
        'DeFi protocols and cross-chain liquidity developments',
        'Technical innovations in consensus, runtime, or core infrastructure',
        'Market dynamics and token economics (DOT, KSM, parachain tokens)'
    ]
}

# Keywords for category identification
# Used in:
# - kol_pump.py: For tweet categorization
CATEGORY_KEYWORDS: List[str] = [
    'polkadot'
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