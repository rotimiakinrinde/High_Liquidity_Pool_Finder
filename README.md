# High Liquidity Pool Finder

> **Find the best Uniswap V3 liquidity pools in seconds — scored, ranked, and simplified.**

Liquidity hunting in DeFi often feels like gambling.  
Raw volume numbers look great until you get trapped by thin depth or hidden spreads.  

The **Uniswap V3 Pool Finder** changes that by assigning every pool a **quality score** and ranking them like a delivery app ranks restaurants.  

No more rolling dice on pools. Just clarity.  

---

## Features

- **Liquidity Quality Scoring**  
  Pools are rated from 0–100 and categorized into:  
  - 🏆 Premium (80–100) — exceptional liquidity  
  - ⭐ Quality (50–79) — solid pools  
  - 📈 Standard (20–49) — usable but average  
  - ⚠️ Risky (0–19) — avoid thin pools  

- **Smart Filters**  
  One-click pool discovery for every type of trader:  
  - 🐋 Whale Territory (>$1M volume)  
  - 🦈 Shark Waters (>$100K)  
  - 🐠 Community Pools (>$10K)  
  - 🔥 Hot Picks (momentum plays)  

- **Comprehensive Analytics**  
  - Volume consistency (not just peaks)  
  - Spread efficiency  
  - Market depth across price levels  
  - Historical reliability  
  - Cross-exchange comparison  

- **Interactive Dashboard**  
  - Volume vs. Efficiency charts  
  - Top performers ranked by composite score  
  - Multi-exchange comparisons  
  - Real-time spread tracking   

---
## How It Works

- Fetches live liquidity + volume data from APIs  
- Runs custom algorithms to compute a **composite liquidity score**  
- Categorizes pools into **Premium, Quality, Standard, Risky** tiers  
- Renders an interactive **Streamlit dashboard** with filters & charts  
