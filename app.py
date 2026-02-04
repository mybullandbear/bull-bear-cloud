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

def save_to_db(symbol, chain_data):
    """
    Saves a snapshot of the option chain to the respective SQLite DB.
    """
    if not chain_data:
        return

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

def save_signals_to_db(symbol, signals):
    """Saves generated signals to the database."""
    if not signals: return

    db_path = DB_FILES.get(symbol)
    if not db_path: return

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
    3. OI Flow (Aggressive Writing)
    4. Smart Money Trend (Buildup/Covering)
    """
    signals = []
    if not chain or not atm_strike: return signals, {}

    # --- Robustness Check ---
    # If API returns partial data (e.g. < 10 strikes), signals will fluctuate wildly.
    if len(chain) < 10:
        print(f"WARNING: Insufficient chain data for signals ({len(chain)} strikes). Skipping.")
        return signals, {}

    # Thresholds
    PCR_BULL = 1.2
    PCR_BEAR = 0.8
    MP_DIV = 100 if symbol == 'BANKNIFTY' else 50
    OI_AGGR = 1.5

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
    threshold = atm_strike * 0.01 # 1% range
    near_chain = [r for r in chain if abs(r['strike'] - atm_strike) <= threshold]
    
    ce_chg_sum = sum(r.get('ce_oich', 0) for r in near_chain)
    pe_chg_sum = sum(r.get('pe_oich', 0) for r in near_chain)

    if pe_chg_sum > (ce_chg_sum * OI_AGGR) and pe_chg_sum > 0:
         signals.append({'type': 'BULLISH', 'strategy': 'OI Smart Money', 'desc': "Aggressive Put Writing detected (Support building)."})
    elif ce_chg_sum > (pe_chg_sum * OI_AGGR) and ce_chg_sum > 0:
         signals.append({'type': 'BEARISH', 'strategy': 'OI Smart Money', 'desc': "Aggressive Call Writing detected (Resistance building)."})

    # 4. Trend Analysis (Top 5 Strikes)
    # Count trends for ATM +/- 2
    strikes_to_check = [atm_strike - (2*step) for step in get_step(symbol)] if symbol else [] # Simplify: just check `near_chain`
    
    lb_count_ce = 0
    sc_count_pe = 0
    lb_count_pe = 0
    sb_count_ce = 0
    
    # We use near_chain (approx 5-10 strikes)
    for row in near_chain:
        if row.get('ce_trend') == 'Long Buildup': lb_count_ce += 1
        if row.get('pe_trend') == 'Short Covering': sc_count_pe += 1
        
        if row.get('pe_trend') == 'Long Buildup': lb_count_pe += 1
        if row.get('ce_trend') == 'Short Buildup': sb_count_ce += 1

    # Bullish: CE Long Buildup OR PE Short Covering dominance
    if (lb_count_ce + sc_count_pe) >= 3:
        signals.append({'type': 'BULLISH', 'strategy': 'Trend Alignment', 'desc': "Majority strikes showing Bullish Trend (LB/SC)."})
        
    # Bearish: PE Long Buildup OR CE Short Buildup dominance
    # Bearish: PE Long Buildup OR CE Short Buildup dominance
    if (lb_count_pe + sb_count_ce) >= 3:
        signals.append({'type': 'BEARISH', 'strategy': 'Trend Alignment', 'desc': "Majority strikes showing Bearish Trend (LB/SB)."})

    # --- Matrix Status Construction ---
    matrix = {
        'pcr': {'val': pcr, 'status': 'Bullish' if pcr >= PCR_BULL else ('Bearish' if pcr <= PCR_BEAR else 'Neutral')},
        'max_pain': {'val': max_pain, 'status': 'Bullish' if (spot < max_pain - MP_DIV) else ('Bearish' if (spot > max_pain + MP_DIV) else 'Neural')},
        'oi_flow': {'val': 'PE Writing' if (pe_chg_sum > ce_chg_sum*OI_AGGR) else ('CE Writing' if (ce_chg_sum > pe_chg_sum*OI_AGGR) else 'Balanced'),
                    'status': 'Bullish' if (pe_chg_sum > ce_chg_sum*OI_AGGR) else ('Bearish' if (ce_chg_sum > pe_chg_sum*OI_AGGR) else 'Neutral')},
        'trend': {'val': f"{lb_count_ce+sc_count_pe} vs {lb_count_pe+sb_count_ce}", 
                  'status': 'Bullish' if (lb_count_ce + sc_count_pe) >= 3 else ('Bearish' if (lb_count_pe + sb_count_ce) >= 3 else 'Neutral')}
    }

    return signals, matrix
    
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

            # 2. Expiry Calculations
            nifty_date = get_next_tuesday()
            nifty_expiry = get_expiry_code(nifty_date)

            monthly_date = get_monthly_tuesday()
            monthly_expiry = get_expiry_code(monthly_date, force_monthly=True)
            
            # --- Fetch All Chains ---
            time.sleep(1) 
            
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
                    market_data['NIFTY']['max_pain'] = calculate_max_pain(chain_data)
                    market_data['NIFTY']['alerts'], market_data['NIFTY']['matrix'] = calculate_signals('NIFTY', chain_data, spot, pcr, market_data['NIFTY']['max_pain'], atm_strike)
                    
                    # Push Notification (First urgent signal only)
                    if market_data['NIFTY']['alerts']:
                        top_alert = market_data['NIFTY']['alerts'][0]
                        print(f"SIGNAL: {top_alert['strategy']} - {top_alert['type']}", flush=True)
                        save_signals_to_db('NIFTY', market_data['NIFTY']['alerts'])
                    
                    save_to_db('NIFTY', chain_data)
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
                    for alert in market_data['BANKNIFTY']['alerts']:
                        send_fcm_alert("BANKNIFTY Alert", alert)
                    
                    save_signals_to_db('BANKNIFTY', market_data['BANKNIFTY']['alerts'])
                    save_to_db('BANKNIFTY', chain_data)

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
                    market_data['FINNIFTY']['max_pain'] = calculate_max_pain(chain_data)
                    market_data['FINNIFTY']['alerts'], market_data['FINNIFTY']['matrix'] = calculate_signals('FINNIFTY', chain_data, spot, pcr, market_data['FINNIFTY']['max_pain'], atm_strike)
                    
                    save_signals_to_db('FINNIFTY', market_data['FINNIFTY']['alerts'])
                    save_to_db('FINNIFTY', chain_data)

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
    """Fetches aggregated OI Change history for charting."""
    symbol = request.args.get('symbol', 'NIFTY')
    history = []
    
    db_path = DB_FILES.get(symbol)
    if not db_path or not os.path.exists(db_path):
        return jsonify([])

    try:
        # Get data from today (midnight onwards) - IST
        start_of_day = get_ist_now().strftime('%Y-%m-%d 00:00:00')
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Aggregate by timestamp: Sum of CE & PE Change
        # We perform a GROUP BY timestamp to get total market view per minute
        query = '''
            SELECT timestamp, 
                   SUM(CASE WHEN type='CE' THEN oich ELSE 0 END) as ce_change,
                   SUM(CASE WHEN type='PE' THEN oich ELSE 0 END) as pe_change,
                   SUM(CASE WHEN type='CE' THEN oi ELSE 0 END) as ce_total,
                   SUM(CASE WHEN type='PE' THEN oi ELSE 0 END) as pe_total
            FROM option_chain 
            WHERE timestamp >= ?
            GROUP BY timestamp
            ORDER BY timestamp ASC
        '''
        cursor.execute(query, (start_of_day,))
        rows = cursor.fetchall()
        conn.close()

        for r in rows:
            history.append({
                'time': r[0],
                'ce_change': r[1],
                'pe_change': r[2],
                'ce_total': r[3],
                'pe_total': r[4]
            })
            
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
