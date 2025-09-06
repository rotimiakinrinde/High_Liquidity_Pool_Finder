# ==========================
# UNISWAP V3 HIGH-LIQUIDITY POOL FINDER
# Enhanced Streamlit Web Application with Beautiful Sidebar
# ==========================

import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import os
from datetime import datetime
import hashlib

# ==========================
# PAGE CONFIG
# ==========================
st.set_page_config(
    page_title="🦄 Uniswap V3 Pool Finder",
    page_icon="🦄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==========================
# CACHE DIRECTORIES
# ==========================
# Get the directory where this script is located
script_dir = os.path.dirname(os.path.abspath(__file__))

CACHE_DIR = os.path.join(script_dir, "cache")
DATA_DIR = os.path.join(script_dir, "data")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

COINGECKO_CACHE = os.path.join(CACHE_DIR, "coingecko_streamlit_cache.csv")
DEFILLAMA_CACHE = os.path.join(CACHE_DIR, "defillama_streamlit_cache.csv")

# ==========================
# UTILITY FUNCTIONS
# ==========================
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_refined_data():
    """Load pre-processed data from CSV files - prioritize full refined data"""
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define possible paths - both relative to script and absolute
    possible_paths = [
        # Relative to script location (database folder)
        os.path.join(script_dir, "data", "uniswap_v3_full_refined.csv"),
        os.path.join(script_dir, "data", "uniswap_v3_top100_pools.csv"),
        
        # In case we're running from root directory
        os.path.join(script_dir, "..", "database", "data", "uniswap_v3_full_refined.csv"),
        os.path.join(script_dir, "..", "database", "data", "uniswap_v3_top100_pools.csv"),
        
        # Original relative paths as fallback
        "data/uniswap_v3_full_refined.csv",
        "../data/uniswap_v3_full_refined.csv", 
        "./data/uniswap_v3_full_refined.csv",
        "uniswap_v3_full_refined.csv",
        "data/uniswap_v3_top100_pools.csv",
        "../data/uniswap_v3_top100_pools.csv",
        "./data/uniswap_v3_top100_pools.csv",
        "uniswap_v3_top100_pools.csv"
    ]
    
    # Try each path
    for file_path in possible_paths:
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path)
                if "full_refined" in file_path:
                    return df, "full"
                else:
                    return df, "top100"
            except Exception as e:
                st.error(f"Error loading {file_path}: {str(e)}")
                continue
    
    # Debug information
    st.error("❌ No refined data files found.")
    st.info(f"Script is running from: {script_dir}")
    st.info("Looking for files in these locations:")
    for path in possible_paths[:4]:  # Show first few paths
        st.write(f"- {path} (exists: {os.path.exists(path)})")
    
    st.info("""
    **To fix this:**
    1. Make sure you've run the main analyzer script first
    2. Check that CSV files are generated in the `data/` folder
    3. Run Streamlit from the same directory as your `data/` folder
    """)
    return pd.DataFrame(), "none"

# ==========================
# MAIN APP
# ==========================
def main():
    # Custom Header with Modern Design
    st.markdown("""
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%, #f093fb 100%); 
                padding: 40px 20px; border-radius: 20px; margin-bottom: 30px; text-align: center;
                box-shadow: 0 10px 30px rgba(0,0,0,0.1);">
        <div style="background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); 
                    border-radius: 15px; padding: 30px; border: 1px solid rgba(255,255,255,0.2);">
            <h1 style="color: white; font-size: 48px; font-weight: 800; margin: 0; 
                       text-shadow: 2px 2px 4px rgba(0,0,0,0.3); letter-spacing: -1px;">
                🦄 Uniswap V3 Pool Finder
            </h1>
            <p style="color: rgba(255,255,255,0.9); font-size: 18px; margin: 15px 0 0 0; 
                      font-weight: 400; text-shadow: 1px 1px 2px rgba(0,0,0,0.2);">
                Discover and analyze high-liquidity pools with advanced filtering and real-time insights
            </p>
            <div style="margin-top: 20px;">
                <span style="background: rgba(255,255,255,0.2); color: white; padding: 8px 16px; 
                            border-radius: 20px; font-size: 14px; font-weight: 600;">
                    ⚡ Powered by CoinGecko Data
                </span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load refined data first
    with st.spinner("Loading refined pool data..."):
        df, data_source = load_refined_data()
    
    if df.empty:
        st.error("❌ No data available. Please run the main analyzer script first to generate refined data.")
        return
    
    # Ensure required columns exist and clean data
    required_cols = ['volume_usd', 'liquidity_score', 'trust_grade']
    
    for col in required_cols:
        if col not in df.columns:
            if col == 'volume_usd':
                volume_candidates = [c for c in df.columns if 'volume' in c.lower()]
                if volume_candidates:
                    df['volume_usd'] = pd.to_numeric(df[volume_candidates[0]], errors='coerce').fillna(0)
                else:
                    df['volume_usd'] = 0
            elif col == 'liquidity_score':
                max_vol = df['volume_usd'].max() if 'volume_usd' in df.columns else 1
                df['liquidity_score'] = ((df['volume_usd'] / max_vol) * 100).round(2) if max_vol > 0 else 0
            elif col == 'trust_grade':
                df['trust_grade'] = df.get('liquidity_score', 0).apply(lambda x: 
                    'A' if x >= 80 else 'B' if x >= 50 else 'C' if x >= 20 else 'D')
    
    if 'volume_formatted' not in df.columns and 'volume_usd' in df.columns:
        df['volume_formatted'] = df['volume_usd'].apply(lambda x: f"${x:,.0f}")
    
    # ==========================
    # SIMPLIFIED SIDEBAR
    # ==========================
    
    # Custom CSS for enhanced sidebar styling
    st.markdown("""
    <style>
    /* Enhanced Sidebar Styling */
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 50%, #f093fb 100%);
    }

    /* Custom sidebar header */
    .sidebar-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px 15px;
        border-radius: 15px;
        margin: -20px -15px 25px -15px;
        text-align: center;
        box-shadow: 0 8px 25px rgba(0,0,0,0.15);
        position: relative;
        overflow: hidden;
    }

    .sidebar-header::before {
        content: '';
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: linear-gradient(45deg, transparent, rgba(255,255,255,0.1), transparent);
        transform: rotate(45deg);
        animation: shimmer 3s infinite;
    }

    @keyframes shimmer {
        0% { transform: translateX(-100%) translateY(-100%) rotate(45deg); }
        100% { transform: translateX(100%) translateY(100%) rotate(45deg); }
    }

    .sidebar-header h2 {
        color: white;
        margin: 0;
        font-size: 24px;
        font-weight: 700;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        position: relative;
        z-index: 1;
    }

    .sidebar-header p {
        color: rgba(255,255,255,0.9);
        margin: 8px 0 0 0;
        font-size: 14px;
        font-weight: 500;
        position: relative;
        z-index: 1;
    }

    /* Control sections */
    .control-section {
        background: rgba(255,255,255,0.95);
        border-radius: 12px;
        padding: 15px;
        margin: 20px 0;
        box-shadow: 0 4px 15px rgba(0,0,0,0.1);
        border-left: 4px solid #667eea;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }

    .control-section:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(0,0,0,0.15);
    }

    .control-section::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(102,126,234,0.1), transparent);
        transition: left 0.5s;
    }

    .control-section:hover::before {
        left: 100%;
    }

    .control-section h4 {
        color: #2d3748;
        margin: 0 0 12px 0;
        font-size: 16px;
        font-weight: 600;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    .control-section .emoji {
        font-size: 18px;
    }

    /* Enhanced slider styling */
    .stSlider > div > div > div > div {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    }

    /* Enhanced selectbox styling */
    .stSelectbox > div > div {
        background: rgba(255,255,255,0.9);
        border-radius: 8px;
    }

    /* Animated background pattern */
    .sidebar-bg-pattern {
        position: fixed;
        top: 0;
        left: 0;
        width: 300px;
        height: 100vh;
        background: 
            radial-gradient(circle at 20% 80%, rgba(120,119,198,0.3) 0%, transparent 50%),
            radial-gradient(circle at 80% 20%, rgba(255,119,198,0.3) 0%, transparent 50%);
        pointer-events: none;
        z-index: -1;
    }

    /* Custom tabs styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background-color: #f8f9fa;
        border-radius: 12px;
        padding: 8px;
        margin: 20px 0;
    }
    .stTabs [data-baseweb="tab"] {
        height: 60px;
        padding: 0 24px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 8px;
        color: white !important;
        font-weight: 600;
        font-size: 16px;
        border: none;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }
    .stTabs [data-baseweb="tab"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%) !important;
        box-shadow: 0 4px 16px rgba(240,147,251,0.3);
    }

    /* Mobile responsive */
    @media (max-width: 768px) {
        .sidebar-header {
            margin: -10px -10px 20px -10px;
            padding: 15px 10px;
        }
        
        .control-section {
            padding: 12px;
            margin: 15px 0;
        }
    }
    </style>
    """, unsafe_allow_html=True)

    # Add background pattern
    st.sidebar.markdown('<div class="sidebar-bg-pattern"></div>', unsafe_allow_html=True)

    # Enhanced Sidebar Header
    st.sidebar.markdown("""
    <div class="sidebar-header">
        <h2>🎯 Pool Controls</h2>
        <p>Filter and sort your pools</p>
    </div>
    """, unsafe_allow_html=True)

    # Quick Action Buttons
    st.sidebar.markdown("""
    <div style="margin: 20px 0;">
        <h4 style="color: white; text-align: center; margin-bottom: 15px;">⚡ Quick Actions</h4>
    </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔥 High Volume", key="high_vol", help="Show pools with >$1M volume"):
            st.session_state.quick_filter = "high_volume"
            st.rerun()

    with col2:
        if st.button("⭐ Top Rated", key="top_rated", help="Show only Grade A pools"):
            st.session_state.quick_filter = "top_rated"
            st.rerun()

    col3, col4 = st.sidebar.columns(2)
    with col3:
        if st.button("💎 Trending", key="trending", help="Show trending pools"):
            st.session_state.quick_filter = "trending"
            st.rerun()

    with col4:
        if st.button("🎯 Clear All", key="clear_all", help="Reset all filters"):
            if 'quick_filter' in st.session_state:
                del st.session_state.quick_filter
            st.rerun()

    # Volume filter with enhanced styling
    min_volume = st.sidebar.number_input(
        "💵 Minimum Volume ($)",
        min_value=0,
        max_value=int(df['volume_usd'].max()) if 'volume_usd' in df.columns else 10**9,
        value=0,
        step=1000,
        help="Filter pools by minimum trading volume"
    )

    # Volume tier with custom styling
    volume_tier = st.sidebar.selectbox(
        "🏆 Volume Categories",
        options=["All Volumes", "🐋 Whale Pools (>$1M)", "🦈 Shark Pools (>$100K)", "🐠 Fish Pools (>$10K)"],
        index=0,
        help="Quick filter by volume tiers"
    )

    # Trust grade filter
    trust_grades = st.sidebar.multiselect(
        "🎖️ Trust Grade",
        options=['A', 'B', 'C', 'D'],
        default=['A', 'B', 'C', 'D'],
        help="A = Excellent (80-100) • B = Good (50-79) • C = Fair (20-49) • D = Poor (0-19)"
    )

    # Quality score range
    if 'liquidity_score' in df.columns:
        min_liquidity_score = st.sidebar.slider(
            "⚡ Min Liquidity Score",
            min_value=0,
            max_value=100,
            value=0,
            step=5,
            help="Higher indicates better liquidity"
        )
    else:
        min_liquidity_score = 0

    # Advanced toggles
    show_trending = st.sidebar.checkbox(
        "📈 Show Trending Only",
        value=False,
        help="Display top 20% pools by volume"
    )

    show_stable = st.sidebar.checkbox(
        "🏛️ Major Pairs Only",
        value=False,
        help="Filter for USDT, USDC, WETH, WBTC pairs"
    )

    # Market selection (if available)
    selected_markets = None
    if 'market' in df.columns:
        available_markets = df['market'].dropna().unique().tolist()
        if len(available_markets) > 1:
            st.sidebar.markdown("🏪 **Select Markets**")
            selected_markets = st.sidebar.multiselect(
                "Markets",
                options=available_markets,
                default=available_markets,
                help="Filter by specific exchanges"
            )

    # Refresh Section
    st.sidebar.markdown("""
    <div style="margin: 30px 0 20px 0; text-align: center;">
    """, unsafe_allow_html=True)

    if st.sidebar.button("🔄 Refresh Data", help="Clear cache and reload fresh data", type="primary"):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("</div>", unsafe_allow_html=True)

    # Footer info
    st.sidebar.markdown("""
    ---
    <div style="text-align: center; padding: 15px; background: rgba(255,255,255,0.1); border-radius: 10px; margin: 10px 0;">
        <div style="color: rgba(255,255,255,0.9); font-size: 12px; font-weight: 600; margin-bottom: 5px;">
            🦄 Uniswap V3 Analytics
        </div>
        <div style="color: rgba(255,255,255,0.7); font-size: 10px;">
            Real-time DeFi intelligence
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # ---------- END Simplified Sidebar ----------
    
    # Main-page controls
    st.subheader("🔎 View Controls")
    colA, colB = st.columns([2, 1])
    with colA:
        if 'market' in df.columns:
            available_markets = df['market'].dropna().unique().tolist()
            if selected_markets is None:
                selected_markets = available_markets
            selected_markets_main = st.multiselect(
                "Markets",
                options=available_markets,
                default=selected_markets,
                help="Filter by specific markets/exchanges"
            )
        else:
            selected_markets_main = None
    with colB:
        available_sort_columns = [c for c in ['volume_usd', 'liquidity_score', 'last_price'] if c in df.columns]
        if not available_sort_columns:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            available_sort_columns = numeric_cols[:4] if numeric_cols else [df.columns[0]]
        sort_by = st.selectbox("Sort By", options=available_sort_columns, index=0)
        ascending = st.checkbox("Ascending Order", value=False, help="Toggle sort order")
    
    # Metrics Cards
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown("""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center; margin-bottom: 10px;">
            <h4 style="margin: 0; font-size: 16px;">💼 Total Pools</h4>
            <h2 style="margin: 10px 0; font-size: 28px; font-weight: bold;">{:,}</h2>
        </div>
        """.format(len(df)), unsafe_allow_html=True)
    
    with col2:
        top_100_volume = df.nlargest(100, 'volume_usd') if 'volume_usd' in df.columns else df.head(100)
        st.markdown("""
        <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center; margin-bottom: 10px;">
            <h4 style="margin: 0; font-size: 16px;">🔥 High Volume Pools</h4>
            <h2 style="margin: 10px 0; font-size: 28px; font-weight: bold;">{:,}</h2>
        </div>
        """.format(len(top_100_volume)), unsafe_allow_html=True)
    
    with col3:
        total_volume = df['volume_usd'].sum() if 'volume_usd' in df.columns else 0
        st.markdown("""
        <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center; margin-bottom: 10px;">
            <h4 style="margin: 0; font-size: 16px;">💰 Total Volume</h4>
            <h2 style="margin: 10px 0; font-size: 28px; font-weight: bold;">${:,.0f}</h2>
        </div>
        """.format(total_volume), unsafe_allow_html=True)
    
    with col4:
        avg_liquidity = df['liquidity_score'].mean() if 'liquidity_score' in df.columns else 0
        st.markdown("""
        <div style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%); 
                    padding: 20px; border-radius: 10px; color: white; text-align: center; margin-bottom: 10px;">
            <h4 style="margin: 0; font-size: 16px;">⭐ Avg Liquidity Score</h4>
            <h2 style="margin: 10px 0; font-size: 28px; font-weight: bold;">{:.1f}</h2>
        </div>
        """.format(avg_liquidity), unsafe_allow_html=True)
    
    # Apply all filters with enhanced logic
    filtered_df = df.copy()
    
    # Apply volume filter
    if min_volume > 0 and 'volume_usd' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['volume_usd'] >= min_volume]
    
    # Apply volume tier filter
    if volume_tier != "All Volumes" and 'volume_usd' in filtered_df.columns:
        if "Whale" in volume_tier:
            filtered_df = filtered_df[filtered_df['volume_usd'] >= 1000000]
        elif "Shark" in volume_tier:
            filtered_df = filtered_df[filtered_df['volume_usd'] >= 100000]
        elif "Fish" in volume_tier:
            filtered_df = filtered_df[filtered_df['volume_usd'] >= 10000]
    
    # Apply trust grade filter
    if trust_grades and 'trust_grade' in filtered_df.columns:
        if len(trust_grades) < 4:  # Not all grades selected
            filtered_df = filtered_df[filtered_df['trust_grade'].isin(trust_grades)]
    
    # Apply liquidity score filter
    if min_liquidity_score > 0 and 'liquidity_score' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['liquidity_score'] >= min_liquidity_score]
    
    # Apply quick filters (if session state exists)
    if 'quick_filter' in st.session_state:
        if st.session_state.quick_filter == "high_volume" and 'volume_usd' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['volume_usd'] >= 1000000]  # >$1M
        elif st.session_state.quick_filter == "top_rated" and 'trust_grade' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['trust_grade'] == 'A']
        elif st.session_state.quick_filter == "trending" and 'volume_usd' in filtered_df.columns:
            volume_threshold = filtered_df['volume_usd'].quantile(0.8)
            filtered_df = filtered_df[filtered_df['volume_usd'] >= volume_threshold]
    
    # Advanced filters
    if show_trending and 'volume_usd' in filtered_df.columns:
        # Show top 20% by volume as "trending"
        volume_threshold = filtered_df['volume_usd'].quantile(0.8)
        filtered_df = filtered_df[filtered_df['volume_usd'] >= volume_threshold]
    
    if show_stable and 'trading_pair' in filtered_df.columns:
        # Filter for major stable pairs
        stable_tokens = ['USDT', 'USDC', 'DAI', 'WETH', 'WBTC']
        mask = pd.Series([False] * len(filtered_df))
        for token in stable_tokens:
            mask |= filtered_df['trading_pair'].str.contains(token, case=False, na=False)
        filtered_df = filtered_df[mask]
    
    # Market filter (use main page selection if available)
    markets_to_use = selected_markets_main if selected_markets_main is not None else selected_markets
    if markets_to_use and 'market' in filtered_df.columns:
        available_markets = df['market'].dropna().unique().tolist()
        if len(markets_to_use) < len(available_markets):
            filtered_df = filtered_df[filtered_df['market'].isin(markets_to_use)]
    
    # Sorting
    if sort_by in filtered_df.columns:
        try:
            filtered_df = filtered_df.sort_values(by=sort_by, ascending=ascending)
        except Exception:
            pass
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Data Table", "🔝 Top Performers", "📈 Analytics Charts"])
    
    with tab1:
        st.subheader("🔍 Pool Data")
        
        default_columns = ['trading_pair', 'volume_formatted', 'base', 'target', 
                         'last_price', 'bid_ask_spread', 'liquidity_score', 
                         'trust_grade', 'market']
        display_columns = [col for col in default_columns if col in filtered_df.columns]
        
        if display_columns and len(filtered_df) > 0:
            display_df = filtered_df[display_columns]
            # Rename volume_formatted column to Volume($) for display
            if 'volume_formatted' in display_df.columns:
                display_df = display_df.rename(columns={'volume_formatted': 'Volume($)'})
            st.dataframe(display_df, width='stretch', height=500)
            
            # Download button
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"uniswap_pools_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No pools match the current filters. Try adjusting your criteria.")
    
    with tab2:
        st.subheader("🔝 Top Performing Pools")
        if len(filtered_df) > 0:
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.write("**💰 Highest Volume Pools**")
                if 'volume_usd' in filtered_df.columns:
                    top_volume_cols = [col for col in ['trading_pair', 'volume_formatted', 'liquidity_score', 'trust_grade'] 
                                     if col in filtered_df.columns]
                    top_volume = filtered_df.nlargest(10, 'volume_usd')[top_volume_cols]
                    if 'volume_formatted' in top_volume.columns:
                        top_volume = top_volume.rename(columns={'volume_formatted': 'Volume($)'})
                    st.dataframe(top_volume, width='stretch')
            
            with col_b:
                st.write("**⭐ Highest Liquidity Score Pools**")
                if 'liquidity_score' in filtered_df.columns:
                    top_liquidity_cols = [col for col in ['trading_pair', 'liquidity_score', 'volume_formatted', 'trust_grade'] 
                                        if col in filtered_df.columns]
                    top_liquidity = filtered_df.nlargest(10, 'liquidity_score')[top_liquidity_cols]
                    if 'volume_formatted' in top_liquidity.columns:
                        top_liquidity = top_liquidity.rename(columns={'volume_formatted': 'Volume($)'})
                    st.dataframe(top_liquidity, width='stretch')
            
            if 'bid_ask_spread' in filtered_df.columns and filtered_df['bid_ask_spread'].max() > 0:
                st.write("**🎯 Tightest Spreads Pools**")
                tight_spreads_cols = [col for col in ['trading_pair', 'bid_ask_spread', 'volume_formatted', 'liquidity_score'] 
                                    if col in filtered_df.columns]
                tight_spreads = filtered_df[filtered_df['bid_ask_spread'] > 0].nsmallest(10, 'bid_ask_spread')[tight_spreads_cols]
                if 'volume_formatted' in tight_spreads.columns:
                    tight_spreads = tight_spreads.rename(columns={'volume_formatted': 'Volume($)'})
                st.dataframe(tight_spreads, width='stretch')
        else:
            st.warning("No data available for top performers with current filters.")
    
    with tab3:
        st.subheader("📈 Analytics Charts")
        if len(filtered_df) > 0:
            try:
                import plotly.express as px
                
                st.write("**💎 Pool Efficiency Analysis: Volume vs Liquidity Score**")
                if 'volume_usd' in filtered_df.columns and 'liquidity_score' in filtered_df.columns:
                    chart_df = filtered_df.copy()
                    # Clean liquidity_score (fix NaN issue)
                    chart_df["liquidity_score"] = pd.to_numeric(chart_df["liquidity_score"], errors="coerce").fillna(0)
                    
                    size_column = None
                    if 'bid_ask_spread' in chart_df.columns:
                        chart_df['spread_size'] = chart_df['bid_ask_spread'].abs() + 0.1
                        size_column = 'spread_size'
                    
                    hover_data = ['trading_pair']
                    if 'market' in chart_df.columns:
                        hover_data.append('market')
                    
                    fig_scatter = px.scatter(
                        chart_df,
                        x='volume_usd',
                        y='liquidity_score',
                        color='trust_grade' if 'trust_grade' in chart_df.columns else None,
                        size=size_column,
                        hover_data=hover_data,
                        title="Pool Efficiency: Higher Volume + Higher Liquidity = Better Pools",
                        labels={'volume_usd': 'Volume (USD)', 'liquidity_score': 'Liquidity Score'}
                    )
                    fig_scatter.update_layout(height=500)
                    st.plotly_chart(fig_scatter, use_container_width=True)
                else:
                    st.warning("Volume or liquidity score data not available for scatter plot.")
                
                st.write("**🏆 Top 20 Pools by Volume**")
                if 'volume_usd' in filtered_df.columns and 'trading_pair' in filtered_df.columns:
                    top_20_volume = filtered_df.nlargest(20, 'volume_usd')
                    fig_bar = px.bar(
                        top_20_volume,
                        x='volume_usd',
                        y='trading_pair',
                        orientation='h',
                        color='trust_grade' if 'trust_grade' in top_20_volume.columns else None,
                        title="Highest Volume Trading Pairs",
                        labels={'volume_usd': 'Volume (USD)', 'trading_pair': 'Trading Pair'}
                    )
                    fig_bar.update_layout(height=600)
                    st.plotly_chart(fig_bar, use_container_width=True)
                else:
                    st.warning("Volume or trading pair data not available for bar chart.")
                
                st.write("**📊 Bid-Ask Spread Distribution by Market**")
                if 'bid_ask_spread' in filtered_df.columns and 'market' in filtered_df.columns:
                    spread_by_market = filtered_df.groupby('market')['bid_ask_spread'].agg(['mean', 'count']).reset_index()
                    spread_by_market.columns = ['market', 'avg_spread', 'pool_count']
                    spread_by_market = spread_by_market[spread_by_market['pool_count'] >= 3]
                    if not spread_by_market.empty:
                        fig_spread = px.bar(
                            spread_by_market,
                            x='market',
                            y='avg_spread',
                            title="Average Bid-Ask Spread by Market (Lower = More Efficient)",
                            labels={'avg_spread': 'Average Bid-Ask Spread (%)', 'market': 'Market/Exchange'},
                            text='pool_count'
                        )
                        fig_spread.update_traces(texttemplate='%{text} pools', textposition='outside')
                        fig_spread.update_layout(height=400)
                        st.plotly_chart(fig_spread, use_container_width=True)
                    else:
                        st.warning("Not enough market data for spread analysis.")
                else:
                    st.warning("Bid-ask spread or market data not available for market analysis.")
                    
            except ImportError:
                st.error("Plotly is required for charts. Install with: pip install plotly")
            except Exception as e:
                st.error(f"Error creating charts: {str(e)}")
        else:
            st.warning("No data available for charts with current filters.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    **📝 Notes:**
    - Using pre-processed and cleaned data from the analyzer
    - Data includes converted token symbols and calculated metrics  
    - Liquidity Score: 0-100 scale based on relative volume
    - Trust Grade: A (>80), B (50-80), C (20-50), D (<20) liquidity score
    
    **🔗 Data Sources:** Pre-processed CoinGecko & DefiLlama data from refined analysis
    """)

if __name__ == "__main__":
    main()