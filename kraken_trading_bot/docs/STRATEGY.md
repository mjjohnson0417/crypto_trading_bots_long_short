# STRATEGY.md – Hybrid Grid Trading Bot (Production)

## Market State Detection

The bot classifies the current market into **exactly one** of the following five states at any given time.

**Production Rules**:
- All calculations use real-time rolling windows based on trade and ticker data only (Klines are not used).
- Calculations run every second.
- State is recalculated frequently.
- Every state change must be logged with timestamp, all indicator values, previous state, and new state.

### Order of Operations for State Identification
The market state must be determined by checking the conditions **in the following strict order** (first matching condition wins):

1. **Sideways** (checked first)  
   - 4h Donchian Channel Width normalized by ATR < 1.0  
     → `(4h Donchian High - 4h Donchian Low) / ATR(4h) < 1.0`

2. **Strong Uptrend**  
   - 1h SMA (rolling) > 4h SMA (rolling) **and** 1h SMA slope > 0.3

3. **Strong Downtrend**  
   - 1h SMA (rolling) < 4h SMA (rolling) **and** 1h SMA slope < -0.3

4. **Uptrend**  
   - 1h SMA (rolling) > 4h SMA (rolling)

5. **Downtrend**  
   - 1h SMA (rolling) < 4h SMA (rolling)

If none of the above conditions are met, the state defaults to **Sideways**.

**Hysteresis (State Change Confirmation)**:
- A new market state is only accepted if it remains the top matching state for **90 consecutive seconds**.
- During the 90-second confirmation window, the bot continues operating under the **previous confirmed state**.

## Core Trading Strategy

The bot uses a **one-sided buy-only ladder** below the current market price.  
After any buy order fills, a paired **limit sell order** is immediately placed above the buy fill price at the configured take-profit percentage (inclusive of all fees).  
All order management is performed via WebSocket connections.

### 1. Strong Uptrend

**Behavior**:
- Pure momentum-chasing mode.
- **No grid ladder exists**. The ladder is completely inactive and is not used in any way during Strong Uptrend.
- All pending limit buy orders are **immediately canceled** upon confirmed entry.

**Important Protection (Hard Price Floor)**:
- When the bot first enters confirmed Strong Uptrend, it records `strong_uptrend_start_price` = current market price at the exact moment the state is confirmed.
- This price acts as a **hard floor** for all market buys while in this state. No chase buys are permitted below this level.

**Chase Sequencing (One Active Pair)**:
- The bot maintains **strict one active buy/sell pair** at a time during Strong Uptrend.
- A new chase market buy is **not permitted** while there is still an open take-profit sell order from a previous chase buy (including partial fills).
- Partial fills continue to block the next chase buy until resolved.
- The next chase buy only becomes eligible after the previous TP sell has fully resolved **or** the confirmed state has exited Strong Uptrend.

**State Exit Handling (from Strong Uptrend)**:
- Upon confirmed exit from Strong Uptrend:
  - Any fully unfilled pending chase buy is canceled.
  - Any partially filled chase buy has a sell order placed for the filled portion; the remaining unfilled portion of the buy is canceled.
- Sell orders are never canceled.

**After a Market Buy Fills**:
- Immediately place the paired limit sell order at `fill_price × (1 + take_profit_percentage)` (net of fees).
- This sell order becomes part of the set of open orders that future buys must respect spacing from.

**Multiple Pairs**:
- Normally only one active buy/sell pair is maintained.
- Multiple simultaneous pairs are allowed only in rare, exceptional circumstances while still under the hard exposure cap.

**Goal**: Clean stepped chasing upward while preventing continued buying as price falls during a Strong Uptrend, and avoiding clustered entries that become underwater when the move fades.

### 2. Non-Strong-Uptrend States (Uptrend, Downtrend, Sideways, Strong Downtrend)

All four states use the same **unified grid mode** behavior.

**Hard Rule**:
- A grid limit buy is **never placed above the current market price** under any circumstances.

**Core Objectives**:
- Maintain a consistent target number of active buy rungs (N, configured).
- Keep the top rung reasonably near the current price while defending the full rung count below it.
- Favor simple, consistent logic over complex optimization.

#### Ladder Adjustment Logic

- Adjustments occur **only** when price movement exceeds the configured proximity threshold.
- **Upward price moves**: Cancel the lowest rung(s) and add new rung(s) closer to the current price (placed top-down), while respecting proximity to all existing buy and sell orders.
- **Downward price moves**: Add new lower rung(s) to maintain the target count.
- New rungs are placed **top-down** when multiple rungs are added in one cycle.

#### Downtrend / Strong Downtrend Mode (Anchored Spacing)

- On entry to Downtrend or Strong Downtrend: The entire ladder is canceled and rebuilt under anchored rules.
- An **anchor price** is recorded at the exact moment the confirmed state enters Downtrend or Strong Downtrend.
- While in Downtrend or Strong Downtrend:
  - The ladder uses **anchored absolute spacing**. The absolute spacing distance is calculated from the anchor and locked for the duration of the downtrend regime.
  - New lower rungs use this fixed absolute distance from the current lowest rung.
  - The top rung should still move closer to the current price when possible (subject to proximity and the hard rule above).
- On exit from Downtrend or Strong Downtrend: The entire ladder is canceled and rebuilt under normal responsive spacing.

#### Refill and Replenishment

- When a TP sell fills, the system does **not** automatically replace the rung at the exact historical price.
- Replacement rungs are positioned using proximity-based spacing from the current price and neighboring rungs (or the locked anchor distance while in downtrend mode).
- When below the target rung count, the bot attempts to restore the full N rungs as quickly as possible (top-down), subject to proximity and capital.

#### State Transition Ladder Rules

The ladder is fully canceled and rebuilt under the appropriate regime on these transitions:
- Entry to Strong Uptrend (ladder is canceled and remains inactive for the duration of the state).
- Entry to Downtrend or Strong Downtrend.
- Exit from Downtrend or Strong Downtrend.

Brief periods with zero active rungs during these transitions are acceptable.

#### General Requirements

- The ladder should generally defend the target rung count.
- Temporary dips below the target count during fast moves are acceptable provided the ladder is never fully exhausted.
- All order placement, cancellation, and modification uses WebSocket.
- High trading volume is valued for fee efficiency in normal conditions; anchored spacing in downtrends is used to manage capital deployment risk during extended grinds.
- Every ladder adjustment and state-driven cancellation must be logged.