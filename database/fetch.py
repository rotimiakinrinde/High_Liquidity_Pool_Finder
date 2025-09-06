# ==========================
# IMPORTS
# ==========================
import requests
import pandas as pd
import os
from datetime import datetime
import time
import logging
import hashlib

# ==========================
# LOGGING SETUP
# ==========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ==========================
# DIRECTORY STRUCTURE
# ==========================
CACHE_DIR = "cache"
DATA_DIR = "data"

# Ensure directories exist
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

# Cache files (temporary/internal use)
COINGECKO_CACHE = os.path.join(CACHE_DIR, "coingecko_full_data_cache.csv")
DEFILLAMA_CACHE = os.path.join(CACHE_DIR, "defillama_metadata_cache.csv")

# Output files (final results)
FULL_OUTPUT = os.path.join(DATA_DIR, "uniswap_v3_full_refined.csv")
TOP100_OUTPUT = os.path.join(DATA_DIR, "uniswap_v3_top100_pools.csv")

# ==========================
# STEP 1: FETCH COINGECKO LP DATA
# ==========================
def fetch_uniswap_v3_pools(use_cache=True, force_refresh=False, investigate=True):
    # Check for cached data
    if use_cache and not force_refresh and os.path.exists(COINGECKO_CACHE):
        print(f"✅ Loading cached data from {COINGECKO_CACHE}")
        cached_df = pd.read_csv(COINGECKO_CACHE)
        print(f"📁 Cached data: {len(cached_df)} pools from {cached_df['page'].max()} pages")
        return cached_df
    
    all_pools = []
    page = 1
    last_page = 0
    
    print("🚀 Starting fresh CoinGecko data fetch...")
    
    while True:
        url = f"https://api.coingecko.com/api/v3/exchanges/uniswap_v3/tickers"
        params = {"page": page, "per_page": 100}
        
        try:
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            tickers = resp.json().get("tickers", [])
            
            # Investigation logging
            if investigate:
                print(f"📊 Page {page}: {len(tickers)} tickers, Status: {resp.status_code}")
            
            last_page = page
            
            # Process tickers
            for t in tickers:
                all_pools.append({
                    "page": page,
                    "base": t.get("base"),
                    "target": t.get("target"),
                    "last_price": t.get("last"),
                    "volume_usd": t.get("converted_volume", {}).get("usd", 0),
                    "bid_ask_spread": t.get("bid_ask_spread_percentage"),
                    "trust_score": t.get("trust_score"),
                    "market": t.get("market", {}).get("name", ""),
                    "coin_id": t.get("coin_id", ""),
                    "target_coin_id": t.get("target_coin_id", "")
                })
            
            # Check if this is the last page
            if not tickers:
                print(f"🏁 LAST PAGE: {page} - Empty response")
                break
            elif len(tickers) < 100:
                print(f"🏁 LAST PAGE: {page} - Partial page ({len(tickers)} tickers)")
                break
            
            page += 1
            time.sleep(1.2)  # Normal rate limit
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                print(f"⚠️ Rate limited at page {page} - waiting 60 seconds")
                time.sleep(60)
                continue
            else:
                print(f"❌ Error fetching page {page}: {e}")
                break
        except Exception as e:
            print(f"❌ Unexpected error at page {page}: {e}")
            break
    
    df = pd.DataFrame(all_pools)
    
    # Cache the complete dataset with smart update
    if not df.empty:
        if smart_save_csv(df, COINGECKO_CACHE, "CoinGecko cache"):
            print(f"💾 Cached complete dataset to {COINGECKO_CACHE}")
        else:
            print(f"💾 CoinGecko cache already up to date")
    
    # Investigation summary
    if investigate and not df.empty:
        print(f"\n📋 FETCH SUMMARY:")
        print(f"   Total pages fetched: {last_page}")
        print(f"   Total pools found: {len(df)}")
        print(f"   Volume range: ${df['volume_usd'].min():,.0f} - ${df['volume_usd'].max():,.0f}")
        
        # Page distribution
        page_counts = df['page'].value_counts().sort_index()
        print(f"   Pools per page:")
        for p in sorted(page_counts.index)[:5]:  # Show first 5 pages
            print(f"     Page {p}: {page_counts[p]} pools")
        if len(page_counts) > 5:
            print(f"     ... and {len(page_counts)-5} more pages")
    
    return df

# ==========================
# STEP 2: FETCH DEFILLAMA METADATA
# ==========================
def fetch_defillama_metadata(token_addresses, use_cache=True, force_refresh=False):
    # Check for cached data
    if use_cache and not force_refresh and os.path.exists(DEFILLAMA_CACHE):
        print(f"✅ Loading cached metadata from {DEFILLAMA_CACHE}")
        cached_df = pd.read_csv(DEFILLAMA_CACHE)
        print(f"📁 Cached metadata: {len(cached_df)} tokens")
        return cached_df
    
    print("🚀 Starting fresh DefiLlama metadata fetch...")
    
    metadata = []
    if not token_addresses:
        return pd.DataFrame(metadata)
    
    # Filter out non-address tokens (already symbols)
    address_tokens = [addr for addr in token_addresses if addr.startswith('0x') or addr.startswith('0X')]
    
    if not address_tokens:
        print("❌ No contract addresses found to fetch metadata for")
        return pd.DataFrame(metadata)
    
    print(f"📊 Fetching metadata for {len(address_tokens)} token addresses...")
    
    # Batch addresses to avoid URL length limits
    batch_size = 50
    for i in range(0, len(address_tokens), batch_size):
        batch = address_tokens[i:i+batch_size]
        keys = [f"ethereum:{addr.lower()}" for addr in batch]
        url = f"https://coins.llama.fi/prices/current/{','.join(keys)}"
        
        try:
            print(f"📊 Fetching batch {i//batch_size + 1}/{(len(address_tokens)-1)//batch_size + 1}")
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            coins = resp.json().get("coins", {})
            
            for k, v in coins.items():
                addr = k.split(":")[1].lower()
                metadata.append({
                    "address": addr,
                    "symbol": v.get("symbol"),
                    "decimals": v.get("decimals"),
                    "price": v.get("price")
                })
            
            time.sleep(1.2)  # Rate limiting between batches
            
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                print(f"⚠️ Rate limited on batch {i//batch_size + 1} - waiting 60 seconds")
                time.sleep(60)
                continue
            else:
                print(f"❌ Error fetching batch {i//batch_size + 1}: {e}")
                continue
        except Exception as e:
            print(f"❌ Unexpected error on batch {i//batch_size + 1}: {e}")
            continue
    
    df_meta = pd.DataFrame(metadata)
    
    # Cache the metadata with smart update
    if not df_meta.empty:
        if smart_save_csv(df_meta, DEFILLAMA_CACHE, "DefiLlama metadata cache"):
            print(f"💾 Cached metadata to {DEFILLAMA_CACHE}")
            print(f"📊 Successfully fetched metadata for {len(df_meta)} tokens")
        else:
            print(f"💾 DefiLlama cache already up to date")
    else:
        print("❌ No metadata fetched")
    
    return df_meta

# ==========================
# SMART FILE UPDATE FUNCTIONS
# ==========================
def get_dataframe_hash(df):
    """Generate a hash of DataFrame content for comparison"""
    if df.empty:
        return None
    # Create hash based on DataFrame content (excluding index)
    # Sort columns for consistent hashing regardless of column order
    df_sorted = df.reindex(sorted(df.columns), axis=1)
    df_string = df_sorted.to_csv(index=False)
    return hashlib.md5(df_string.encode()).hexdigest()

def smart_save_csv(df, filename, description="data"):
    """Save CSV only if content has changed"""
    if df.empty:
        print(f"⚠️ Empty DataFrame - skipping {filename}")
        return False
    
    current_hash = get_dataframe_hash(df)
    hash_file = f"{filename}.hash"
    
    # Check if file and hash exist
    if os.path.exists(filename) and os.path.exists(hash_file):
        try:
            with open(hash_file, 'r') as f:
                stored_hash = f.read().strip()
            
            if stored_hash == current_hash:
                print(f"✅ {description} unchanged - skipping {filename}")
                return False
            else:
                print(f"🔄 {description} changed - updating {filename}")
        except Exception as e:
            print(f"⚠️ Error reading hash file: {e} - proceeding with save")
    else:
        print(f"💾 Creating new {description} file: {filename}")
    
    # Save the DataFrame and hash
    try:
        df.to_csv(filename, index=False)
        with open(hash_file, 'w') as f:
            f.write(current_hash)
        print(f"✅ Successfully saved {filename}")
        return True
    except Exception as e:
        print(f"❌ Error saving {filename}: {e}")
        return False
def integrate_metadata(lp_df, llama_df):
    print(f"🔄 Converting addresses to symbols...")
    
    # Create metadata lookup dictionary
    metadata = {row['address'].lower(): row for idx, row in llama_df.iterrows()}
    
    print(f"📊 Metadata available for {len(metadata)} addresses")
    
    def get_symbol(addr):
        if not isinstance(addr, str):
            return str(addr)
        
        # Clean address
        addr_clean = addr.strip().lower()
        
        # Check if it's a contract address
        if addr_clean.startswith('0x') and len(addr_clean) == 42:
            symbol = metadata.get(addr_clean, {}).get("symbol")
            if symbol:
                return symbol
            else:
                return addr[:8]+"..."  # Fallback for unknown addresses
        else:
            return addr  # Already a symbol or different format
    
    # Create copy and convert addresses to symbols
    lp_df = lp_df.copy()
    
    print(f"🔄 Converting base addresses...")
    lp_df['base_symbol'] = lp_df['base'].apply(get_symbol)
    
    print(f"🔄 Converting target addresses...")
    lp_df['target_symbol'] = lp_df['target'].apply(get_symbol)
    
    # Update base and target columns with symbols
    lp_df['base'] = lp_df['base_symbol']
    lp_df['target'] = lp_df['target_symbol']
    
    # Create trading pair in SYMBOL/SYMBOL format
    lp_df['trading_pair'] = lp_df['base'] + "/" + lp_df['target']
    
    # Format volume for display
    lp_df['volume_formatted'] = lp_df['volume_usd'].apply(lambda x: f"${x:,.0f}")
    
    # Compute liquidity score
    lp_df['liquidity_score'] = (lp_df['volume_usd'] / lp_df['volume_usd'].max() * 100).round(2)
    
    # Compute trust grade
    def trust_grade(score):
        if score >= 80: return "A"
        elif score >= 50: return "B"
        elif score >= 20: return "C"
        return "D"
    lp_df['trust_grade'] = lp_df['liquidity_score'].apply(trust_grade)
    
    # Drop helper columns
    lp_df = lp_df.drop(['base_symbol', 'target_symbol'], axis=1, errors='ignore')
    
    # Show conversion sample
    print(f"✅ Address conversion complete!")
    print(f"📊 Sample conversions:")
    sample_pairs = lp_df['trading_pair'].head(10).tolist()
    for i, pair in enumerate(sample_pairs, 1):
        print(f"   {i:2d}. {pair}")
    
    return lp_df

# ==========================
# HIGH LP ANALYSIS FUNCTIONS
# ==========================
def analyze_high_lp_pages(df, volume_thresholds=[1000000, 100000, 50000, 10000]):
    """Analyze which pages contain high-value LPs"""
    print("🔍 ANALYZING HIGH LP DISTRIBUTION BY PAGE\n")
    
    for threshold in volume_thresholds:
        print(f"💰 LPs with volume > ${threshold:,}")
        
        # Filter high-volume LPs
        high_lps = df[df['volume_usd'] > threshold].copy()
        
        if len(high_lps) == 0:
            print(f"   ❌ No LPs found above ${threshold:,}")
            continue
            
        # Page distribution
        page_counts = high_lps['page'].value_counts().sort_index()
        total_high_lps = len(high_lps)
        
        print(f"   📊 Total count: {total_high_lps}")
        print(f"   📄 Pages with high LPs: {len(page_counts)} out of {df['page'].max()}")
        print(f"   🏁 Last page with high LP: Page {high_lps['page'].max()}")
        print(f"   🎯 First page with high LP: Page {high_lps['page'].min()}")
        
        # Show distribution for first 10 pages that have high LPs
        print(f"   📈 Distribution by page:")
        for page in sorted(page_counts.index)[:10]:
            count = page_counts[page]
            percentage = (count / total_high_lps) * 100
            print(f"      Page {page:2d}: {count:3d} LPs ({percentage:5.1f}%)")
        
        if len(page_counts) > 10:
            remaining_pages = len(page_counts) - 10
            remaining_lps = sum(page_counts.iloc[10:])
            remaining_pct = (remaining_lps / total_high_lps) * 100
            print(f"      ... {remaining_pages} more pages with {remaining_lps} LPs ({remaining_pct:.1f}%)")
        
        print("-" * 60)

def find_optimal_cutoff(df, target_percentage=90):
    """Find optimal page cutoff to capture X% of high-value LPs"""
    print(f"🎯 FINDING OPTIMAL CUTOFF FOR {target_percentage}% OF HIGH LPs\n")
    
    thresholds = [1000000, 100000, 50000, 10000]
    
    for threshold in thresholds:
        high_lps = df[df['volume_usd'] > threshold].copy()
        
        if len(high_lps) == 0:
            continue
            
        page_counts = high_lps['page'].value_counts().sort_index()
        total_high_lps = len(high_lps)
        target_count = int(total_high_lps * target_percentage / 100)
        
        cumulative = 0
        optimal_page = None
        
        for page in sorted(page_counts.index):
            cumulative += page_counts[page]
            if cumulative >= target_count:
                optimal_page = page
                break
        
        captured_pct = (cumulative / total_high_lps) * 100
        time_saved = ((df['page'].max() - optimal_page) / df['page'].max()) * 100
        
        print(f"💰 Volume > ${threshold:,}:")
        print(f"   🎯 To capture {target_percentage}% ({target_count}/{total_high_lps} LPs)")
        print(f"   📄 Optimal cutoff: Page {optimal_page}")
        print(f"   ✅ Actually captures: {cumulative} LPs ({captured_pct:.1f}%)")
        print(f"   ⚡ Time savings: {time_saved:.1f}%")
        print()

# ==========================
# MAIN EXECUTION
# ==========================
if __name__ == "__main__":
    print("=" * 60)
    print("🦄 UNISWAP V3 LIQUIDITY POOL ANALYZER")
    print("=" * 60)
    
    # 1️⃣ Fetch CoinGecko LP data
    pools_df = fetch_uniswap_v3_pools(use_cache=True, force_refresh=False, investigate=True)
    
    if not pools_df.empty:
        print(f"\n🎯 COINGECKO DATA LOADED:")
        print(f"DataFrame shape: {pools_df.shape}")
        print(f"Pools with >$1M volume: {(pools_df['volume_usd'] > 1000000).sum()}")
        print(f"Pools with >$100K volume: {(pools_df['volume_usd'] > 100000).sum()}")
        
        # Analyze high LP distribution by page
        analyze_high_lp_pages(pools_df)
        find_optimal_cutoff(pools_df, target_percentage=90)
        find_optimal_cutoff(pools_df, target_percentage=95)
        
        # 2️⃣ Collect unique token addresses for metadata
        unique_tokens = list(set(pools_df['base'].tolist() + pools_df['target'].tolist()))
        print(f"\n🔍 Found {len(unique_tokens)} unique token addresses")
        
        # 3️⃣ Fetch metadata from DefiLlama
        llama_df = fetch_defillama_metadata(unique_tokens, use_cache=True, force_refresh=False)
        
        # 4️⃣ Integrate data and apply filtering
        if not llama_df.empty:
            print(f"\n🔗 INTEGRATING DATA...")
            refined_df = integrate_metadata(pools_df, llama_df)
            
            print(f"✅ Integration complete!")
            
            # Apply top pools filtering by volume
            top_pools = refined_df.nlargest(100, 'volume_usd').reset_index(drop=True)
            
            # Define display columns for cleaner output
            display_cols = ['trading_pair', 'volume_formatted', 'last_price', 'liquidity_score', 'trust_grade']
            
            print(f"🎯 FILTERED TOP 100 POOLS:")
            print(f"📊 DataFrame shape: {top_pools.shape}")
            print(f"📊 Top 20 pools with converted symbols:")
            print(top_pools[display_cols].head(20).to_string(index=False))
            
            print(f"\n📊 FULL DATAFRAME - Top 100 pools:")
            print(top_pools.to_string(index=False))
            
            print(f"\n📈 Top 100 pools statistics:")
            print(f"   Volume range: ${top_pools['volume_usd'].min():,.0f} - ${top_pools['volume_usd'].max():,.0f}")
            print(f"   Pages represented: {top_pools['page'].min()} to {top_pools['page'].max()}")
            print(f"   Average liquidity score: {top_pools['liquidity_score'].mean():.1f}")
            
            # Count by trust grade
            trust_counts = top_pools['trust_grade'].value_counts()
            print(f"   Trust grade distribution:")
            for grade in ['A', 'B', 'C', 'D']:
                count = trust_counts.get(grade, 0)
                print(f"     Grade {grade}: {count} pools")
            
            # Save both full and filtered data with smart updates
            full_filename = FULL_OUTPUT
            top_filename = TOP100_OUTPUT
            
            # Smart save - only update if content changed
            full_updated = smart_save_csv(refined_df, full_filename, "full refined dataset")
            top_updated = smart_save_csv(top_pools, top_filename, "top 100 pools")
            
            # Summary of file operations
            print(f"\n📁 FILE UPDATE SUMMARY:")
            print(f"   Full dataset: {'Updated' if full_updated else 'No changes'} - {full_filename}")
            print(f"   Top 100 pools: {'Updated' if top_updated else 'No changes'} - {top_filename}")
        else:
            print("❌ No metadata available for integration")
            
    else:
        print("❌ No CoinGecko data available")