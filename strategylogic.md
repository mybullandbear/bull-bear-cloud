# Trading Strategy Logic Design

This document outlines the logic for the 3 advanced trading strategies to be implemented in the trading bot.

## 1. PCR Signal Strategy (Market Sentiment)
**Concept**: The Put-Call Ratio (PCR) indicates the overall sentiment of option writers. High PCR = Bullish (More Puts written/supported), Low PCR = Bearish (More Calls written/resisted).

**Logic Rules**:
*   **Bullish Signal (Buy CE)**:
    *   `PCR` >= `Bullish_Threshold` (Default: 1.2)
    *   *Confirmation*: optional check if PCR is *rising* compared to 5 mins ago (requires history, v1 will use absolute threshold).
*   **Bearish Signal (Buy PE)**:
    *   `PCR` <= `Bearish_Threshold` (Default: 0.8)
*   **Neutral (No Trade)**:
    *   `PCR` is between 0.8 and 1.2.

**Strike Selection**: ATM (At-The-Money).

---

## 2. Max Pain & PCR Confluence (Mean Reversion)
**Concept**: Max Pain (MP) is the strike price where option sellers lose the least. Prices tend to "gravitate" towards MP by expiry. We trade this gravitation ONLY if sentiment (PCR) aligns.

**Logic Rules**:
*   **Bullish Reversion (Buy CE)**:
    *   **Spot Price** is significanly BELOW Max Pain (Gap > `Divergence_Points`, e.g., 100 pts)
    *   **AND** `PCR` > 1.0 (Sentiment is NOT Bearish, allowing for a rise).
    *   *Why*: Price is oversold relative to MP, and sentiment supports a bounce.
*   **Bearish Reversion (Buy PE)**:
    *   **Spot Price** is significantly ABOVE Max Pain (Gap > `Divergence_Points`)
    *   **AND** `PCR` < 1.0 (Sentiment is NOT Bullish, allowing for a drop).
    *   *Why*: Price is overbought relative to MP, and sentiment supports a correction.

**Strike Selection**: 
*   Buy Strike closer to Max Pain (e.g., MP - 100 for CE, MP + 100 for PE) or strictly ATM.

---

## 3. OI Change Analysis (Smart Money Flow)
**Concept**: Change in Open Interest (ChgOI) represents distinct new positions. We follow the "Aggressive Writers".
*   If `PE_Chg_OI` >> `CE_Chg_OI`: Smart money is aggressively writing Puts (Creating Support) -> **Bullish**.
*   If `CE_Chg_OI` >> `PE_Chg_OI`: Smart money is aggressively writing Calls (Creating Resistance) -> **Bearish**.

**Logic Rules**:
1.  **Calculate Net Flows**:
    *   Sum `Chg_OI` for Calls (`Sum_CE_Chg`) for [ATM - 2] to [ATM + 2].
    *   Sum `Chg_OI` for Puts (`Sum_PE_Chg`) for [ATM - 2] to [ATM + 2].
2.  **Evaluate Flow**:
    *   **Bullish Signal (Buy CE)**:
        *   `Sum_PE_Chg` > (`Sum_CE_Chg` * `Aggression_Factor` e.g., 1.5)
        *   *Meaning*: Put writing is 1.5x stronger than Call writing.
    *   **Bearish Signal (Buy PE)**:
        *   `Sum_CE_Chg` > (`Sum_PE_Chg` * `Aggression_Factor` e.g., 1.5)
        *   *Meaning*: Call writing is 1.5x stronger than Put writing.


---

## 4. Smart Money Trend (Accumulation/Distribution)
**Concept**: "Smart Money" leaves a footprint through specific price/OI combinations. We analyze the **Aggregate Trend** of the top 5 active strikes to determine the market phase.

**Phases**:
1.  **Long Build Up (LB)**: Price ⬆️ + OI ⬆️ (Strong Bullish - New Buying)
2.  **Short Build Up (SB)**: Price ⬇️ + OI ⬆️ (Strong Bearish - New Selling/Writing)
3.  **Short Covering (SC)**: Price ⬆️ + OI ⬇️ (Explosive Bullish - Bears Exiting)
4.  **Long Unwinding (LU)**: Price ⬇️ + OI ⬇️ (Panic Bearish - Bulls Exiting)

**Logic Rules**:
*   **Analyze ATM +/- 2 Strikes (Total 5 strikes)**.
*   Count the occurrence of each phase for CE and PE.
*   **Bullish Signal (Buy CE)**:
    *   Majority of CE strikes are **LB** (Long Buildup) OR Majority of PE strikes are **SC** (Short Covering).
    *   *Confidence*: >3 out of 5 strikes align.
*   **Bearish Signal (Buy PE)**:
    *   Majority of PE strikes are **LB** (Long Buildup - implying buying Puts) OR Majority of CE strikes are **SB** (Short Buildup - Writing Calls).
    *   *Note*: For Options Selling, SB on CE is bearish. For Options Buying, we look for LB on PE.

**Strike Selection**: ATM.

