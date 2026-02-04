from flask import Flask, request, jsonify, render_template, redirect
from datetime import datetime, timedelta
import json
import os
import threading
import time
import sqlite3
from fyers_apiv3 import fyersModel
import firebase_admin
from firebase_admin import credentials, messaging

app = Flask(__name__)

# --- Firebase Init ---
try:
    if not firebase_admin._apps:
        cred = credentials.Certificate('serviceAccountKey.json')
        firebase_admin.initialize_app(cred)
        print("DEBUG: Firebase Admin Initialized", flush=True)
except Exception as e:
    print(f"ERROR: Firebase Init Failed: {e}", flush=True)

# --- Database Setup ---
DATA_DIR = 'data'
DB_FILES = {
    'NIFTY': os.path.join(DATA_DIR, 'nifty.db'),
    'BANKNIFTY': os.path.join(DATA_DIR, 'banknifty.db'),
    'FINNIFTY': os.path.join(DATA_DIR, 'finnifty.db')
}

def get_ist_now():
    """Returns current time in IST (UTC+5:30)."""
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

def init_dbs():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    for symbol, db_path in DB_FILES.items():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # SCHEMA MIGRATION CHECK: Option Chain
        try:
            cursor.execute("SELECT timestamp FROM option_chain LIMIT 1")
        except sqlite3.OperationalError:
            print(f"MIGRATION: Dropping old option_chain table for {symbol}")
            cursor.execute("DROP TABLE IF EXISTS option_chain")
            conn.commit()
            
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS option_chain (
                timestamp DATETIME,
                strike INTEGER,
                type TEXT,
                ltp REAL,
                oi INTEGER,
                oich INTEGER,
                volume INTEGER,
                iv REAL,
                delta REAL
            )
        ''')
        conn.commit()
        conn.close()
        print(f"DEBUG: Initialized DB for {symbol} at {db_path}", flush=True)

    # Create Signals DB (Global or per symbol? Per symbol is consistent)
    for symbol, db_path in DB_FILES.items():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # SCHEMA MIGRATION CHECK: Signals
        try:
            cursor.execute("SELECT timestamp FROM signals LIMIT 1")
        except sqlite3.OperationalError:
            print(f"MIGRATION: Dropping old signals table for {symbol}")
            cursor.execute("DROP TABLE IF EXISTS signals")
            conn.commit()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS signals (
                timestamp DATETIME,
                type TEXT,
                strategy TEXT,
                description TEXT
            )
        ''')
        conn.commit()
        conn.close()

    # Create Market History DB (New! For Spot Price Chart)
    for symbol, db_path in DB_FILES.items():
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # PERFORMANCE: Enable Write-Ahead Logging (WAL) for concurrency
        cursor.execute("PRAGMA journal_mode=WAL;")
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS market_history (
                timestamp DATETIME,
                price REAL
            )
        ''')
        conn.commit()
        conn.close()

def save_to_db(symbol, chain_data, timestamp=None):
    """
    Saves a snapshot of the option chain to the respective SQLite DB.
    """
    if not chain_data:
        return

    if not timestamp:
        timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
        
    db_path = DB_FILES.get(symbol)
    
    if not db_path:
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        records = []
        for row in chain_data:
            # CE Record
            records.append((
                timestamp, row['strike'], 'CE', 
                row.get('ce_ltp', 0), row.get('ce_oi', 0), row.get('ce_oich', 0), 
                row.get('ce_vol', 0), row.get('ce_iv', 0), row.get('ce_delta', 0)
            ))
            # PE Record
            records.append((
                timestamp, row['strike'], 'PE', 
                row.get('pe_ltp', 0), row.get('pe_oi', 0), row.get('pe_oich', 0), 
                row.get('pe_vol', 0), row.get('pe_iv', 0), row.get('pe_delta', 0)
            ))
            
        cursor.executemany('''
            INSERT INTO option_chain (timestamp, strike, type, ltp, oi, oich, volume, iv, delta)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', records)
        
        conn.commit()
        conn.close()
        print(f"DEBUG: Saved {len(records)} records to {symbol} DB", flush=True)
        
    except Exception as e:
        print(f"Error saving to DB for {symbol}: {e}", flush=True)

def save_signals_to_db(symbol, signals, timestamp=None):
    """Saves generated signals to the database."""
    if not signals: return

    db_path = DB_FILES.get(symbol)
    if not db_path: return

    if not timestamp:
        timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        records = []
        for sig in signals:
            records.append((timestamp, sig['type'], sig['strategy'], sig['desc']))
            
        cursor.executemany('INSERT INTO signals (timestamp, type, strategy, description) VALUES (?, ?, ?, ?)', records)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving signals for {symbol}: {e}")

def save_market_price(symbol, spot_price, timestamp=None):
    """Saves the spot price to the database for charting."""
    if not spot_price: return
    db_path = DB_FILES.get(symbol)
    if not db_path: return
    
    if not timestamp:
        timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('INSERT INTO market_history (timestamp, price) VALUES (?, ?)', (timestamp, spot_price))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving market price for {symbol}: {e}")

# Initialize DBs on start
init_dbs()

# --- Global Storage ---
stocks = {}
TOKEN_FILE = "token.json"

def load_token():
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading token: {e}")
    return {"client_id": "", "secret_key": "", "access_token": None}

def save_token_data(data):
    try:
        with open(TOKEN_FILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving token: {e}")

# Live Market Data Storage
market_data = {
    'NIFTY': {'spot': 0, 'chain': []},
    'BANKNIFTY': {'spot': 0, 'chain': []},
    'FINNIFTY': {'spot': 0, 'chain': []}
}

# --- Helpers ---

def get_next_thursday():
    """Finds the next weekly expiry (Thursday)."""
    today = get_ist_now()
    days_ahead = 3 - today.weekday()  # Thursday is 3
    if days_ahead < 0: # Target day already happened this week
        days_ahead += 7
    if days_ahead == 0 and today.hour > 15: # If today is Thursday but market closed, get next
        days_ahead += 7
    
    next_thursday = today + timedelta(days=days_ahead)
    return next_thursday

def generate_strikes(spot, step, count=20):
    """Generates strikes around ATM."""
    if spot == 0: return []
    atm = round(spot / step) * step
    strikes = []
    # Below ATM
    for i in range(count, 0, -1):
        strikes.append(atm - (i * step))
    # Above ATM
    for i in range(0, count + 1):
        strikes.append(atm + (i * step))
    return strikes

def calculate_max_pain(chain):
    """Calculates Max Pain Strike (where option writers lose least)."""
    if not chain: return 0
    
    strikes = [row['strike'] for row in chain]
    pains = {}
    
    for expiration_price in strikes:
        total_pain = 0
        for row in chain:
            strike = row['strike']
            ce_oi = row.get('ce_oi', 0)
            pe_oi = row.get('pe_oi', 0)
            
            # Call Pain (Writer loses if Exp > Strike)
            if expiration_price > strike:
                total_pain += (expiration_price - strike) * ce_oi
                
            # Put Pain (Writer loses if Exp < Strike)
            if expiration_price < strike:
                total_pain += (strike - expiration_price) * pe_oi
                
        pains[expiration_price] = total_pain
        
    if not pains: return 0
    return min(pains, key=pains.get)

def calculate_signals(symbol, chain, spot, pcr, max_pain, atm_strike):
    """
    Generates trading signals based on 4 strategies:
    1. PCR Sentiment
    2. Max Pain Reversion
    3. OI Flow (Aggressive Writing/Unwinding)
    4. Smart Money Trend (Weighted Scoring)
    """
    signals = []
    if not chain or not atm_strike: return signals, {}

    # --- Robustness Check ---
    if len(chain) < 10:
        return signals, {}

    # Thresholds
    PCR_BULL = 1.1
    PCR_BEAR = 0.9
    MP_DIV = 100 if symbol == 'BANKNIFTY' else 50
    OI_AGGR_RATIO = 1.5

    # 1. PCR Signal
    if pcr >= PCR_BULL:
        signals.append({'type': 'BULLISH', 'strategy': 'PCR Sentiment', 'desc': f"High PCR ({pcr}) indicates put writing support."})
    elif pcr <= PCR_BEAR:
        signals.append({'type': 'BEARISH', 'strategy': 'PCR Sentiment', 'desc': f"Low PCR ({pcr}) indicates call writing resistance."})

    # 2. Max Pain Reversion
    # Bullish: Oversold (Spot < MP) AND Sentiment is NOT Bearish
    if spot < (max_pain - MP_DIV) and pcr > 0.9:
        signals.append({'type': 'BULLISH', 'strategy': 'Max Pain Reversion', 'desc': f"Price oversold below Max Pain ({max_pain}). Rebound likely."})
    # Bearish: Overbought (Spot > MP) AND Sentiment is NOT Bullish
    elif spot > (max_pain + MP_DIV) and pcr < 1.1:
        signals.append({'type': 'BEARISH', 'strategy': 'Max Pain Reversion', 'desc': f"Price overbought above Max Pain ({max_pain}). Correction likely."})

    # 3. OI Flow Analysis (Near ATM)
    # Refined: Check for Unwinding vs Writing
    threshold = atm_strike * 0.012 # 1.2% range
    near_chain = [r for r in chain if abs(r['strike'] - atm_strike) <= threshold]
    
    ce_chg_sum = sum(r.get('ce_oich', 0) for r in near_chain)
    pe_chg_sum = sum(r.get('pe_oich', 0) for r in near_chain)

    flow_status = "Balanced"
    flow_desc = "Neutral activity"
    is_flow_bull = False
    is_flow_bear = False

    # Scenario A: Call Unwinding (Bullish)
    if ce_chg_sum < 0 and pe_chg_sum > 0:
        flow_status = "Call Unwinding"
        flow_desc = "Calls are exiting while Puts are being written."
        is_flow_bull = True
    # Scenario B: Put Unwinding (Bearish)
    elif pe_chg_sum < 0 and ce_chg_sum > 0:
        flow_status = "Put Unwinding"
        flow_desc = "Puts are exiting while Calls are being written."
        is_flow_bear = True
    # Scenario C: Both Writing (Check Ratio)
    elif ce_chg_sum > 0 and pe_chg_sum > 0:
        if pe_chg_sum > (ce_chg_sum * OI_AGGR_RATIO):
            flow_status = "Strong Put Writing"
            is_flow_bull = True
        elif ce_chg_sum > (pe_chg_sum * OI_AGGR_RATIO):
            flow_status = "Strong Call Writing"
            is_flow_bear = True
    
    if is_flow_bull:
        signals.append({'type': 'BULLISH', 'strategy': 'Smart Money Flow', 'desc': flow_desc})
    elif is_flow_bear:
        signals.append({'type': 'BEARISH', 'strategy': 'Smart Money Flow', 'desc': flow_desc})


    # 4. Trend Analysis (Weighted Scoring)
    # Score: LB-CE(+1), SB-PE(+1) | LB-PE(-1), SB-CE(-1) | SC-CE(+0.5), SC-PE(-0.5)
    trend_score = 0
    bull_count = 0
    bear_count = 0
    
    for row in near_chain:
        ce_t = row.get('ce_trend', 'Neutral')
        pe_t = row.get('pe_trend', 'Neutral')

        # CE Analysis
        if ce_t == 'Long Buildup': trend_score += 1; bull_count += 1
        elif ce_t == 'Short Buildup': trend_score -= 1; bear_count += 1
        elif ce_t == 'Short Covering': trend_score += 0.5 # Weak Bullish (Resist weakening)
        elif ce_t == 'Long Unwinding': trend_score -= 0.5 # Weak Bearish

        # PE Analysis
        if pe_t == 'Short Buildup': trend_score += 1; bull_count += 1 # Selling Puts = Bullish
        elif pe_t == 'Long Buildup': trend_score -= 1; bear_count += 1 # Buying Puts = Bearish
        elif pe_t == 'Short Covering': trend_score -= 0.5 # Weak Bearish (Support weakening)
        elif pe_t == 'Long Unwinding': trend_score += 0.5 # Weak Bullish

    trend_status = 'Neutral'
    if trend_score >= 2.5: 
        trend_status = 'Bullish'
    elif trend_score <= -2.5:
        trend_status = 'Bearish'

    if trend_status == 'Bullish':
        signals.append({'type': 'BULLISH', 'strategy': 'Trend Alignment', 'desc': f"Market Structure is Bullish (Score: {trend_score})."})
    elif trend_status == 'Bearish':
         signals.append({'type': 'BEARISH', 'strategy': 'Trend Alignment', 'desc': f"Market Structure is Bearish (Score: {trend_score})."})

    # --- 5. Composite Confluence Score (-10 to +10) ---
    # Weight: Flow(40%), Trend(30%), PCR(20%), MaxPain(10%)
    
    score = 0
    reasons = []

    # A. Flow Score (Max 4)
    if is_flow_bull: 
        if "Unwinding" in flow_status: score += 4; reasons.append("Call Unwinding (Explosive)")
        else: score += 3; reasons.append("Put Writing (Support)")
    elif is_flow_bear:
        if "Unwinding" in flow_status: score -= 4; reasons.append("Put Unwinding (Crash)")
        else: score -= 3; reasons.append("Call Writing (Resistance)")

    # B. Trend Score (Max 3)
    # trend_score is already roughly -3 to +3 range based on strike count logic earlier
    # Let's normalize it slightly
    if trend_status == 'Bullish': score += 2; reasons.append("Bullish Structure")
    elif trend_status == 'Bearish': score -= 2; reasons.append("Bearish Structure")

    # C. PCR Score (Max 2)
    if pcr >= PCR_BULL: score += 2; reasons.append("PCR Oversold/Bullish")
    elif pcr <= PCR_BEAR: score -= 2; reasons.append("PCR Overbought/Bearish")

    # D. Max Pain Score (Max 1)
    # If Spot > MaxPain -> Lean Bearish (Reversion)
    if spot > (max_pain + MP_DIV): score -= 1 
    elif spot < (max_pain - MP_DIV): score += 1

    # --- Trade Recommendation ---
    action = "WAIT / NEUTRAL"
    color = "slate"
    
    if score >= 5:
        action = "STRONG BUY (CE)"
        color = "emerald"
    elif score >= 2:
        action = "BUY ON DIPS"
        color = "teal"
    elif score <= -5:
        action = "STRONG SELL (PE)"
        color = "rose"
    elif score <= -2:
        action = "SELL ON RISE"
        color = "orange"

    # --- Matrix / Card Construction ---
    # We return a 'card' object for the UI to render the Gauge & Action
    signal_card = {
        'symbol': symbol,
        'score': score, # -10 to 10
        'action': action,
        'color': color,
        'reasons': reasons[:2], # Top 2 active drivers
        'pcr': pcr,
        'max_pain': max_pain,
        'spot': spot
    }

    return signals, signal_card
    
def get_step(symbol):
    if symbol == 'NIFTY' or symbol == 'FINNIFTY': return [50, 0] # range is dummy
    return [100, 0]

def send_fcm_alert(title, body):
    """Sends a push notification to all devices subscribed to 'alerts'."""
    try:
        # See documentation on defining a message payload.
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
            ),
            topic='alerts',
        )
        response = messaging.send(message)
        print(f"DEBUG: Sent FCM Message: {response}", flush=True)
    except Exception as e:
        print(f"ERROR: FCM Send Failed: {e}", flush=True)



# --- Expiry Helpers (2026 Rules) ---

def get_expiry_code(date_obj, force_monthly=False):
    """
    Returns Fyers Expiry Code.
    Rules:
    - Monthly Format: YYMMM (e.g. 26FEB) -> For BankNifty/FinNifty
    - Weekly Format: YYMdd (e.g. 26203) -> For Nifty (unless it's monthly expiry week)
    """
    year_short = date_obj.strftime('%y')
    month_short = date_obj.strftime('%b').upper()
    day_str = date_obj.strftime('%d')
    m_map = {1:'1', 2:'2', 3:'3', 4:'4', 5:'5', 6:'6', 7:'7', 8:'8', 9:'9', 10:'O', 11:'N', 12:'D'}
    month_code = m_map[date_obj.month]

    # Calculate Last Tuesday of the month
    if date_obj.month == 12:
        next_month_start = date_obj.replace(year=date_obj.year+1, month=1, day=1)
    else:
        next_month_start = date_obj.replace(month=date_obj.month+1, day=1)
    
    last_day_month = next_month_start - timedelta(days=1)
    # Tuesday is 1
    delta = (last_day_month.weekday() - 1) % 7
    last_tuesday = last_day_month - timedelta(days=delta)

    # Use Monthly format if it's the monthly expiry date OR if force_monthly is True
    if force_monthly or date_obj.date() == last_tuesday.date():
        return f"{year_short}{month_short}"
    else:
        return f"{year_short}{month_code}{day_str}"

def get_next_tuesday():
    """Next Weekly Expiry (Tuesday)"""
    today = get_ist_now()
    days_ahead = 1 - today.weekday() # Tuesday is 1
    if days_ahead < 0: 
        days_ahead += 7
    if days_ahead == 0 and today.hour > 15:
        days_ahead += 7
    return today + timedelta(days=days_ahead)

def get_monthly_tuesday():
    """Next Monthly Expiry (Last Tuesday of Month)"""
    today = get_ist_now()
    
    # helper to find last tuesday of a specific month
    def get_last_tue(d):
        if d.month == 12:
            nm = d.replace(year=d.year+1, month=1, day=1)
        else:
            nm = d.replace(month=d.month+1, day=1)
        return (nm - timedelta(days=1)) - timedelta(days=((nm - timedelta(days=1)).weekday() - 1) % 7)

    last_tue_current = get_last_tue(today)
    
    # If today passed current month's expiry, get next month's
    if today.date() > last_tue_current.date() or (today.date() == last_tue_current.date() and today.hour > 15):
        if today.month == 12:
            next_d = today.replace(year=today.year+1, month=1, day=1)
        else:
            next_d = today.replace(month=today.month+1, day=1)
        return get_last_tue(next_d)
    else:
        return last_tue_current

# --- Background Worker ---

def get_trend(price_chg, oi_chg):
    """
    Determines trend based on Price Change and OI Change.
    """
    if price_chg > 0 and oi_chg > 0: return "Long Buildup"
    if price_chg < 0 and oi_chg > 0: return "Short Buildup"
    if price_chg > 0 and oi_chg < 0: return "Short Covering"
    if price_chg < 0 and oi_chg < 0: return "Long Unwinding"
    return "Neutral"

def fetch_option_chain_data(fyers, symbol, strike_count=40, atm_strike=None, interval=None):
    """
    Fetches option chain using fyers.optionchain API.
    Returns simplified list of dicts.
    """
    chain_list = []
    try:
        data = {
            "symbol": symbol,
            "strikecount": strike_count,
            "timestamp": "" 
        }
        res = fyers.optionchain(data=data)
        
        if res and 'data' in res and 'optionsChain' in res['data']:
            strike_map = {}
            raw_chain = res['data']['optionsChain']
            
            for item in raw_chain:
                strike = item.get('strike_price')
                if strike not in strike_map:
                    strike_map[strike] = {'strike': strike, 'is_atm': False} 
                
                side = 'ce' if item.get('option_type') == 'CE' else 'pe'
                
                strike_map[strike][f'{side}_ltp'] = item.get('ltp', 0)
                strike_map[strike][f'{side}_oi'] = item.get('oi', 0)
                strike_map[strike][f'{side}_ch'] = item.get('ltpch', 0) 
                strike_map[strike][f'{side}_oich'] = item.get('oich', 0) 
                strike_map[strike][f'{side}_vol'] = item.get('volume', 0)
                strike_map[strike][f'{side}_iv'] = item.get('iv', 0)
                strike_map[strike][f'{side}_delta'] = item.get('delta', 0)
                strike_map[strike][f'{side}_theta'] = item.get('theta', 0)
                
                # Calculate Trend (Only if within 20 strikes of ATM)
                trend = "Neutral"
                if atm_strike and interval:
                    # Check range
                    diff = abs(strike - atm_strike)
                    if diff <= (20 * interval):
                         price_chg = item.get('ltpch', 0)
                         oi_chg = item.get('oich', 0)
                         trend = get_trend(price_chg, oi_chg)
                else: 
                     # Fallback if no ATM provided (or previous behavior)
                     # For now, user asked to limit it. If no ATM, maybe skip? 
                     # Let's keep it safe: if no ATM, apply to all (or none). 
                     # User said "rest data we can leave". So safer to default Neutral.
                     pass 

                strike_map[strike][f'{side}_trend'] = trend

            chain_list = list(strike_map.values())
            chain_list.sort(key=lambda x: x['strike'])
        else:
            # Optionally log failure
            pass
            
    except Exception as e:
        print(f"Error in fetch_option_chain_data: {e}")
    
    return chain_list

def cleanup_old_data():
    """Deletes records older than 7 days to save disk space."""
    try:
        cutoff_date = (get_ist_now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
        databases = ['data/nifty.db', 'data/banknifty.db', 'data/finnifty.db']
        
        for db_path in databases:
            if os.path.exists(db_path):
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM option_chain WHERE timestamp < ?", (cutoff_date,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    conn.close()
                    if deleted_count > 0:
                        print(f"CLEANUP: Removed {deleted_count} old records from {db_path}", flush=True)
                except Exception as e:
                    print(f"CLEANUP ERROR in {db_path}: {e}", flush=True)
    except Exception as e:
        print(f"CLEANUP GLOBAL ERROR: {e}", flush=True)

def data_worker():
    """Polls Fyers API for live data."""
    print("Background Worker Started")
    
    # Run cleanup on startup
    cleanup_old_data()
    last_cleanup = datetime.now()
    
    error_count = 0
    
    while True:
        # Periodic Cleanup (Every 24 hours)
        if (datetime.now() - last_cleanup).total_seconds() > 86400:
            cleanup_old_data()
            last_cleanup = datetime.now()

        # Reload creds from file every loop
        creds = load_token()
        
        if not creds.get('access_token'):
            print("Worker: Waiting for Access Token in token.json...")
            time.sleep(3)
            continue

        try:
            fyers = fyersModel.FyersModel(
                client_id=creds['client_id'], 
                token=creds['access_token'],
                is_async=False, 
                log_path=""
            )
            print("DEBUG: Fyers Object Methods:", dir(fyers))
            # Confirmed token presence
            # print("Worker: Connected. processing...") 

            # 1. Fetch Spot Prices
            spot_symbols = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX", "NSE:NIFTYFIN-INDEX", "NSE:FINNIFTY-INDEX"]
            data = {"symbols": ",".join(spot_symbols)}
            response = fyers.quotes(data=data)
            
            if 'd' not in response:
                print(f"Error fetching spot: {response}")
                error_count += 1
                time.sleep(5)
                continue
            error_count = 0

            # Update Spots
            indices = {
                "NSE:NIFTY50-INDEX": "NIFTY",
                "NSE:NIFTYBANK-INDEX": "BANKNIFTY",
                "NSE:NIFTYFIN-INDEX": "FINNIFTY",
                "NSE:FINNIFTY-INDEX": "FINNIFTY"
            }
            
            spot_prices = {}
            for i, item in enumerate(response['d']):
                if i == 0: 
                    print(f"DEBUG: Sample Spot Item: {item}") # Inspect structure
                    if 'v' in item:
                        print(f"DEBUG: Available Fields in 'v': {list(item['v'].keys())}")

                name = item['n']
                if name in indices:
                    key = indices[name]
                    # Safe access
                    if 'v' in item and 'lp' in item['v']:
                        lp = item['v']['lp']
                        ch = item['v'].get('ch', 0)
                        chp = item['v'].get('chp', 0)
                        
                        market_data[key]['spot'] = lp
                        market_data[key]['ch'] = ch
                        market_data[key]['chp'] = chp
                        spot_prices[key] = lp
                    else:
                        print(f"WARNING: 'lp' missing for {name}")

            # 2. Expiry Calculations
            nifty_date = get_next_tuesday()
            nifty_expiry = get_expiry_code(nifty_date)

            monthly_date = get_monthly_tuesday()
            monthly_expiry = get_expiry_code(monthly_date, force_monthly=True)
            
            # --- Fetch All Chains ---
            time.sleep(1) 
            # --- Generate Synchronized Timestamp ---
            current_timestamp = get_ist_now().strftime('%Y-%m-%d %H:%M:%S')
            
            # NIFTY
            if 'NIFTY' in spot_prices:
                spot = spot_prices['NIFTY']
                atm_strike = round(spot / 50) * 50
                ref_symbol = f"NSE:NIFTY{nifty_expiry}{atm_strike}CE"
                
                chain_data = fetch_option_chain_data(fyers, ref_symbol, strike_count=100, atm_strike=atm_strike, interval=50)
                
                if chain_data:
                    # Mark ATM
                    for row in chain_data:
                         if row['strike'] == atm_strike:
                             row['is_atm'] = True
                             
                    # Calc PCR & Sentiment
                    total_ce_oi = sum(row.get('ce_oi', 0) for row in chain_data)
                    total_pe_oi = sum(row.get('pe_oi', 0) for row in chain_data)
                    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0
                    
                    if pcr >= 1: senti = "Bullish"
                    elif pcr <= 0.8: senti = "Bearish"
                    else: senti = "Sideways"
                    
                    market_data['NIFTY']['chain'] = chain_data
                    market_data['NIFTY']['pcr'] = pcr
                    market_data['NIFTY']['sentiment'] = senti
                    market_data['NIFTY']['max_pain'] = calculate_max_pain(chain_data)
                    market_data['NIFTY']['alerts'], market_data['NIFTY']['matrix'] = calculate_signals('NIFTY', chain_data, spot, pcr, market_data['NIFTY']['max_pain'], atm_strike)
                    
                    # Push Notification (First urgent signal only)
                    if market_data['NIFTY']['alerts']:
                        top_alert = market_data['NIFTY']['alerts'][0]
                        print(f"SIGNAL: {top_alert['strategy']} - {top_alert['type']}", flush=True)
                        send_fcm_alert("NIFTY Alert", f"{top_alert['strategy']} - {top_alert['type']}: {top_alert['desc']}")
                        save_signals_to_db('NIFTY', market_data['NIFTY']['alerts'], current_timestamp)
                    
                    save_to_db('NIFTY', chain_data, current_timestamp)
                    save_market_price('NIFTY', spot, current_timestamp)
                else:
                    pass

            # BANKNIFTY
            if 'BANKNIFTY' in spot_prices:
                spot = spot_prices['BANKNIFTY']
                atm_strike = round(spot / 100) * 100
                ref_symbol = f"NSE:BANKNIFTY{monthly_expiry}{atm_strike}CE"
                
                chain_data = fetch_option_chain_data(fyers, ref_symbol, strike_count=100, atm_strike=atm_strike, interval=100)
                if chain_data:
                    for row in chain_data:
                         if row['strike'] == atm_strike: row['is_atm'] = True

                    # Calc PCR & Sentiment
                    total_ce_oi = sum(row.get('ce_oi', 0) for row in chain_data)
                    total_pe_oi = sum(row.get('pe_oi', 0) for row in chain_data)
                    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0
                    
                    if pcr >= 1: senti = "Bullish"
                    elif pcr <= 0.8: senti = "Bearish"
                    else: senti = "Sideways"

                    market_data['BANKNIFTY']['chain'] = chain_data
                    market_data['BANKNIFTY']['pcr'] = pcr
                    market_data['BANKNIFTY']['sentiment'] = senti
                    market_data['BANKNIFTY']['max_pain'] = calculate_max_pain(chain_data)
                    market_data['BANKNIFTY']['alerts'], market_data['BANKNIFTY']['matrix'] = calculate_signals('BANKNIFTY', chain_data, spot, pcr, market_data['BANKNIFTY']['max_pain'], atm_strike)

                    # Push Notification
                    if market_data['BANKNIFTY']['alerts']:
                        top_alert = market_data['BANKNIFTY']['alerts'][0]
                        send_fcm_alert("BANKNIFTY Alert", f"{top_alert['strategy']} - {top_alert['type']}: {top_alert['desc']}")
                    
                    save_signals_to_db('BANKNIFTY', market_data['BANKNIFTY']['alerts'], current_timestamp)
                    save_to_db('BANKNIFTY', chain_data, current_timestamp)
                    save_market_price('BANKNIFTY', spot, current_timestamp)

            # FINNIFTY
            if 'FINNIFTY' in spot_prices:
                spot = spot_prices['FINNIFTY']
                atm_strike = round(spot / 50) * 50
                ref_symbol = f"NSE:FINNIFTY{monthly_expiry}{atm_strike}CE"
                
                chain_data = fetch_option_chain_data(fyers, ref_symbol, strike_count=100, atm_strike=atm_strike, interval=50)
                if chain_data:
                    for row in chain_data:
                         if row['strike'] == atm_strike: row['is_atm'] = True

                    # Calc PCR & Sentiment
                    total_ce_oi = sum(row.get('ce_oi', 0) for row in chain_data)
                    total_pe_oi = sum(row.get('pe_oi', 0) for row in chain_data)
                    pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0
                    
                    if pcr >= 1: senti = "Bullish"
                    elif pcr <= 0.8: senti = "Bearish"
                    else: senti = "Sideways"

                    market_data['FINNIFTY']['chain'] = chain_data
                    market_data['FINNIFTY']['pcr'] = pcr
                    market_data['FINNIFTY']['sentiment'] = senti
                    market_data['FINNIFTY']['max_pain'] = calculate_max_pain(chain_data)
                    market_data['FINNIFTY']['alerts'], market_data['FINNIFTY']['matrix'] = calculate_signals('FINNIFTY', chain_data, spot, pcr, market_data['FINNIFTY']['max_pain'], atm_strike)
                    
                    if market_data['FINNIFTY']['alerts']:
                        top_alert = market_data['FINNIFTY']['alerts'][0]
                        send_fcm_alert("FINNIFTY Alert", f"{top_alert['strategy']} - {top_alert['type']}: {top_alert['desc']}")

                    save_signals_to_db('FINNIFTY', market_data['FINNIFTY']['alerts'], current_timestamp)
                    save_to_db('FINNIFTY', chain_data, current_timestamp)
                    save_market_price('FINNIFTY', spot, current_timestamp)

            print("DEBUG: Cycle Complete. Sleeping 60s...", flush=True)
            time.sleep(60)

            time.sleep(3) 
            
        except Exception as e:
            print(f"Worker Exception: {e}")
            time.sleep(5)

# Start Thread
t = threading.Thread(target=data_worker)
t.daemon = True
t.start()

# --- Routes ---

@app.route('/')
def dashboard():
    creds = load_token()
    stocks_list = list(stocks.items())
    return render_template('dashboard.html', stocks=stocks_list, fyers_connected=bool(creds.get('access_token')))

@app.route('/connect')
def connect_page():
    creds = load_token()
    return render_template('connect.html', creds=creds)

@app.route('/api/save_creds', methods=['POST'])
def save_creds():
    data = request.json
    creds = load_token()
    creds['client_id'] = data.get('client_id', '').strip()
    creds['secret_key'] = data.get('secret_key', '').strip()
    save_token_data(creds)
    return jsonify({"status": "success"})

@app.route('/api/fyers_login')
def fyers_login():
    creds = load_token()
    if not creds.get('client_id') or not creds.get('secret_key'):
        return "Please save credentials first.", 400
    session = fyersModel.SessionModel(
        client_id=creds['client_id'],
        secret_key=creds['secret_key'],
        redirect_uri=request.host_url.rstrip('/') + "/fyers/callback", 
        response_type="code", grant_type="authorization_code"
    )
    return redirect(session.generate_authcode())

@app.route('/fyers/callback')
def fyers_callback():
    auth_code = request.args.get('auth_code')
    creds = load_token()
    try:
        session = fyersModel.SessionModel(
            client_id=creds['client_id'],
            secret_key=creds['secret_key'],
            redirect_uri=request.host_url.rstrip('/') + "/fyers/callback",
            response_type="code", grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        if "access_token" in response:
            creds['access_token'] = response['access_token']
            save_token_data(creds)
            return "<html><body><h1>Login Successful!</h1><script>window.opener.location.href='/';window.close();</script></body></html>"
        else:
            return f"Login Failed: {response}", 400
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/logout')
def logout():
    data = load_token()
    data['access_token'] = None
    save_token_data(data)
    return redirect('/')
    creds = load_token()
    try:
        session = fyersModel.SessionModel(
            client_id=creds['client_id'],
            secret_key=creds['secret_key'],
            redirect_uri=request.host_url.rstrip('/') + "/fyers/callback",
            response_type="code", grant_type="authorization_code"
        )
        session.set_token(auth_code)
        response = session.generate_token()
        if "access_token" in response:
            creds['access_token'] = response['access_token']
            save_token_data(creds)
            return "<html><body><h1>Login Successful!</h1><script>window.opener.location.href='/';window.close();</script></body></html>"
        else:
            return f"Login Failed: {response}", 400
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/api/option_chain')
def get_option_chain():
    """Returns the latest market data."""
    return jsonify(market_data)

@app.route('/api/oi_history')
def get_oi_history():
    """
    Fetches aggregated OI Change history for charting.
    Logic: Sum of OI Chg for Near ATM Strikes only.
    - NIFTY/BANKNIFTY: +/- 20 strikes
    - FINNIFTY: +/- 10 strikes
    """
    symbol = request.args.get('symbol', 'NIFTY')
    history = []
    
    db_path = DB_FILES.get(symbol)
    if not db_path or not os.path.exists(db_path):
        return jsonify([])

    # Define Range per Symbol
    # NIFTY step 50 * 20 = 1000
    # BANKNIFTY step 100 * 20 = 2000
    # FINNIFTY step 50 * 10 = 500
    range_limit = 1000 
    if symbol == 'BANKNIFTY': range_limit = 2000
    elif symbol == 'FINNIFTY': range_limit = 500

    try:
        # Get data from today (midnight onwards) - IST
        start_of_day = get_ist_now().strftime('%Y-%m-%d 00:00:00')
        market_start = get_ist_now().strftime('%Y-%m-%d 09:15:00')
        market_end = get_ist_now().strftime('%Y-%m-%d 15:30:00')

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 1. Fetch Spot Price Data First (to determine ATM)
        cursor.execute("SELECT timestamp, price FROM market_history WHERE timestamp >= ? ORDER BY timestamp ASC", (start_of_day,))
        price_rows = cursor.fetchall()
        price_map = {r[0]: r[1] for r in price_rows}
        
        # 2. Fetch Raw Option Chain Data (needed for filtering)
        # Includes LTP now to help estimate spot if missing
        query_chain = '''
            SELECT timestamp, strike, type, oich, ltp, oi 
            FROM option_chain 
            WHERE timestamp >= ?
            ORDER BY timestamp ASC
        '''
        cursor.execute(query_chain, (start_of_day,))
        chain_rows = cursor.fetchall()
        conn.close()

        # 3. Process Data Grouped by Timestamp
        from collections import defaultdict
        grouped_data = defaultdict(list)
        for r in chain_rows:
            grouped_data[r[0]].append(r)

        # 4. Calculate stats per timestamp
        for ts, rows in grouped_data.items():
            # Filter Time: 09:15 to 15:30
            time_part = ts.split(' ')[1]
            if not ("09:15:00" <= time_part <= "15:30:00"):
                continue

            spot = price_map.get(ts)
            
            # --- Fallback: Estimate Spot from ATM (Min Call/Put LTP Diff) ---
            if spot is None and rows:
                # Find row with minimal abs(ce_ltp - pe_ltp)
                # Rows are mixed CE/PE. Need to group by strike first.
                strike_map = defaultdict(dict)
                for r in rows:
                    strike = r[1]
                    otype = r[2]
                    ltp = r[4]
                    strike_map[strike][otype] = ltp
                
                best_strike = None
                min_diff = float('inf')
                
                for strike, data in strike_map.items():
                    if 'CE' in data and 'PE' in data:
                        diff = abs(data['CE'] - data['PE'])
                        if diff < min_diff:
                            min_diff = diff
                            best_strike = strike
                
                if best_strike:
                    spot = best_strike # Proxy spot is the ATM strike

            if spot is None: 
                continue

            # Determine ATM
            # Simplistic ATM: Round to nearest step not strictly needed, just checking distance
            
            ce_sum = 0
            pe_sum = 0
            
            for r in rows:
                strike = r[1]
                otype = r[2] # CE/PE
                oich = r[3]
                
                # Filter Condition: Strike is within +/- Range of Spot
                if abs(strike - spot) <= range_limit:
                    if otype == 'CE': ce_sum += oich
                    elif otype == 'PE': pe_sum += oich
            
            history.append({
                'time': ts,
                'ce_change': ce_sum,
                'pe_change': pe_sum,
                'price': price_map.get(ts, None) # Send explicit None for price if missing, frontend handles it (gap in purple line)
            })
            
        # Sort by time just in case dict unordered
        history.sort(key=lambda x: x['time'])

    except Exception as e:
        print(f"OI History Error for {symbol}: {e}")
        return jsonify({"error": str(e)})

    return jsonify(history)

@app.route('/api/signal_history')
def get_signal_history():
    """Fetches valid signals from the last 24 hours."""
    history = {}
    try:
        limit_date = (get_ist_now() - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        for symbol, db_path in DB_FILES.items():
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT timestamp, type, strategy, description FROM signals WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 50", (limit_date,))
                rows = cursor.fetchall()
                conn.close()
                
                history[symbol] = [
                    {'time': r[0], 'type': r[1], 'strategy': r[2], 'desc': r[3]} for r in rows
                ]
    except Exception as e:
        print(f"History Error: {e}")
        return jsonify({"error": str(e)})

    return jsonify(history)

@app.route('/full_chain')
def full_chain_page():
    return render_template('full_chain.html')

@app.route('/webhook', methods=['POST'])
def webhook():
    global stocks
    if request.is_json: data = request.json
    else: 
        try: data = json.loads(request.data)
        except: return jsonify({"status": "error"}), 400
    ticker = data.get('ticker', 'UNKNOWN').upper()
    signal_entry = {
        'strategy': data.get('strategy', 'Alert'),
        'action': data.get('action', 'INFO').upper(),
        'price': data.get('price', 0),
        'timestamp': datetime.now().strftime('%H:%M:%S')
    }
    if ticker not in stocks: stocks[ticker] = {'last_updated': datetime.min, 'signals': []}
    stocks[ticker]['signals'].insert(0, signal_entry)
    stocks[ticker]['signals'] = stocks[ticker]['signals'][:5]
    stocks[ticker]['last_updated'] = datetime.now()
    return jsonify({"status": "success"}), 200

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False, host='0.0.0.0', port=5000)
