#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FINAL_OMEGA_SCANNER_ULTRA.py
OMEGA SCANNER PRO v5.0 - ULTRA PRODUCTION EDITION
Real-Time WS Hybrid | True Confluence | Cloud-Ready 24/7 Engine
"""

import os
import sys
import time
import json
import sqlite3
import signal
import gc
import threading
import logging
import warnings
from datetime import datetime, timezone, timedelta
from queue import Queue

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np

try:
    import websocket
except ImportError:
    print("[CRITICAL] 'websocket-client' not installed. Please run: pip install websocket-client")
    sys.exit(1)

warnings.filterwarnings("ignore")

# ============================================================
# LOGGING CONFIGURATION
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OmegaUltra")

# ============================================================
# CONFIGURATION & SECURITY
# ============================================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
DB_PATH = os.getenv("DATABASE_PATH", "omega_ultra.db")
CONFIG_MODE = os.getenv("CONFIG_MODE", "PRODUCTION")

if not TELEGRAM_TOKEN or not CHAT_ID:
    logger.critical("TELEGRAM_TOKEN or CHAT_ID missing from environment variables.")
    sys.exit(1)

# SCANNER SETTINGS
SCAN_TF = "1h"
TOP_COINS_COUNT = 35
MIN_QUOTE_VOLUME = 5000000
EXCLUDED_SYMBOLS = {'USDT', 'BUSD', 'USDC', 'DAI', 'TUSD', 'PAX', 'UST', 'FDUSD'}

# INTERVALS
FAST_SCAN_INTERVAL = 300     # Run confluence scan every 5 minutes
HEARTBEAT_INTERVAL = 14400   # 4 hours
TRADE_CHECK_INTERVAL = 2     # Check live WS trades every 2 seconds
COIN_REFRESH_INTERVAL = 86400 # Refresh top coins daily

# THRESHOLDS & COOLDOWNS
ELITE_THRESHOLD = 95
STRONG_THRESHOLD = 85
GOOD_THRESHOLD = 70
SIGNAL_COOLDOWN_HOURS = 6
ELITE_COOLDOWN_HOURS = 12

# ============================================================
# TELEGRAM MANAGER (Rate-Limited Queue)
# ============================================================
class TelegramManager:
    def __init__(self, token, chat_id):
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.chat_id = chat_id
        self.msg_queue = Queue()
        self.running = True
        self.worker = threading.Thread(target=self._process_queue, daemon=True)
        self.worker.start()

    def _process_queue(self):
        while self.running:
            if not self.msg_queue.empty():
                msg = self.msg_queue.get()
                try:
                    payload = {
                        "chat_id": self.chat_id,
                        "text": msg,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    }
                    requests.post(f"{self.base_url}/sendMessage", data=payload, timeout=10)
                except Exception as e:
                    logger.error(f"Telegram Error: {e}")
                time.sleep(1) # Prevent 429 Too Many Requests
            else:
                time.sleep(0.5)

    def send(self, message):
        self.msg_queue.put(message)

    def send_signal(self, sig):
        coin, tf = sig["coin"], sig["timeframe"]
        score, whale, momentum = sig["global_score"], sig["whale_score"], sig["momentum_score"]
        regime, is_elite = sig["btc_regime"], sig["is_elite"]
        
        icon, header = ("💎💎", "ELITE CONFLUENCE SETUP") if is_elite else ("🔥", "STRONG SETUP") if score >= STRONG_THRESHOLD else ("⚡", "GOOD SETUP")
        inds = sig.get("indicators",[])
        ind_text = " + ".join(inds) if inds else "Single"
        
        entry, sl, tp1 = sig.get("entry", 0), sig.get("sl", 0), sig.get("tp1", 0)
        tp2, tp3 = sig.get("tp2", "N/A"), sig.get("tp3", "N/A")
        
        rr_text = "N/A"
        if entry and sl and tp1 and entry != sl:
            rr_text = f"1:{abs(tp1 - entry) / abs(entry - sl):.1f}"

        msg = f"""
{icon} <b>{header} | #{coin.replace('USDT','')}</b>
===========================
⏱ <b>TF:</b> {tf}
📊 <b>Confluence Score:</b> {score}/100 
🎯 <b>Aligned Indicators ({len(inds)}):</b>
<i>{ind_text}</i>

<b>MARKET INTELLIGENCE:</b>
🐋 Whale Power: {whale}/100
📈 Momentum: {momentum}/100
🌍 BTC Regime: {regime}

<b>TRADE PLAN:</b>
🟢 <b>Entry:</b> {entry:.4f}
🔴 <b>Stop Loss:</b> {sl:.4f}
🎯 <b>TP1:</b> {tp1:.4f}
🎯 <b>TP2:</b> {tp2:.4f} if isinstance(tp2, float) else tp2
🎯 <b>TP3:</b> {tp3:.4f} if isinstance(tp3, float) else tp3
⚖️ <b>Risk/Reward:</b> {rr_text}

🔗 <a href="https://www.tradingview.com/chart/?symbol=BINANCE:{coin}">View Chart</a>
"""
        self.send(msg)

    def send_trade_update(self, coin, event, price, profit_pct):
        icons = {"TP1": "🎯", "TP2": "🎯🎯", "TP3": "🎯🎯🎯", "SL": "🛑", "BE_SL": "🛡️"}
        icon = icons.get(event, "🔔")
        msg = f"""
{icon} <b>TRADE UPDATE | #{coin.replace('USDT','')}</b>
===========================
<b>Event:</b> {event} HIT
<b>Price:</b> {price:.4f}
<b>PNL:</b> {profit_pct:+.2f}%

🔗 <a href="https://www.tradingview.com/chart/?symbol=BINANCE:{coin}">View Chart</a>
"""
        self.send(msg)

# ============================================================
# DATABASE MANAGER (WAL Mode + Thread Safe)
# ============================================================
class DatabaseManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_db()

    def init_db(self):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                cursor = conn.cursor()
                cursor.execute("""CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, coin TEXT, timeframe TEXT,
                    indicators TEXT, entry_price REAL, stop_loss REAL, tp1 REAL, global_score INTEGER, is_elite INTEGER)""")
                cursor.execute("""CREATE TABLE IF NOT EXISTS cooldowns (
                    coin TEXT PRIMARY KEY, last_signal_time TEXT, cooldown_hours INTEGER)""")
                cursor.execute("""CREATE TABLE IF NOT EXISTS active_trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, coin TEXT, entry_price REAL, current_sl REAL,
                    tp1 REAL, tp2 REAL, tp3 REAL, tp1_hit INTEGER DEFAULT 0, tp2_hit INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'OPEN', max_profit_pct REAL DEFAULT 0, entry_time TEXT)""")
                cursor.execute("""CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, total_signals INTEGER DEFAULT 0, elite_signals INTEGER DEFAULT 0,
                    tp_hits INTEGER DEFAULT 0, sl_hits INTEGER DEFAULT 0)""")
                conn.commit()

    def execute_query(self, query, params=(), fetch=False):
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(query, params)
                if fetch: return cursor.fetchall()
                conn.commit()

    def save_signal(self, sig):
        self.execute_query("""INSERT INTO signals (timestamp, coin, timeframe, indicators, entry_price, stop_loss, tp1, global_score, is_elite)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (datetime.now(timezone.utc).isoformat(), sig["coin"], sig["timeframe"], json.dumps(list(sig["indicators"])),
            sig["entry"], sig["sl"], sig["tp1"], sig["global_score"], 1 if sig["is_elite"] else 0))

    def log_trade(self, sig):
        self.execute_query("""INSERT INTO active_trades (coin, entry_price, current_sl, tp1, tp2, tp3, entry_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)""", (sig["coin"], sig["entry"], sig["sl"], sig["tp1"], sig.get("tp2"), sig.get("tp3"), datetime.now(timezone.utc).isoformat()))

    def update_stats(self, field):
        self.execute_query(f"UPDATE stats SET {field} = {field} + 1 WHERE id = 1")
        if not self.execute_query("SELECT id FROM stats WHERE id = 1", fetch=True):
            self.execute_query(f"INSERT INTO stats ({field}) VALUES (1)")

    def is_on_cooldown(self, coin, is_elite):
        res = self.execute_query("SELECT last_signal_time, cooldown_hours FROM cooldowns WHERE coin = ?", (coin,), fetch=True)
        if not res: return False
        last_time = datetime.fromisoformat(res[0][0])
        return datetime.now(timezone.utc) - last_time < timedelta(hours=res[0][1] if not is_elite else ELITE_COOLDOWN_HOURS)

    def set_cooldown(self, coin, hours):
        self.execute_query("INSERT OR REPLACE INTO cooldowns (coin, last_signal_time, cooldown_hours) VALUES (?, ?, ?)",
                           (coin, datetime.now(timezone.utc).isoformat(), hours))

    def cleanup_old_data(self):
        self.execute_query("DELETE FROM signals WHERE timestamp < datetime('now', '-30 days')")
        self.execute_query("DELETE FROM active_trades WHERE status != 'OPEN' AND entry_time < datetime('now', '-30 days')")

# ============================================================
# BINANCE HYBRID DATA MANAGER (REST + WEBSOCKET)
# ============================================================
class BinanceHybridManager:
    def __init__(self):
        self.rest_url = "https://api.binance.com"
        self.ws_url = "wss://stream.binance.com:9443/ws"
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=0.5)))
        
        self.klines_cache = {} # Format: {symbol: [list of candle dicts]}
        self.live_prices = {}
        self.active_coins =[]
        self.ws = None
        self.ws_thread = None
        self.running = True
        self.cache_lock = threading.Lock()

    def get_top_coins(self):
        try:
            res = self.session.get(f"{self.rest_url}/api/v3/ticker/24hr", timeout=15).json()
            valid = [{"sym": t["symbol"], "vol": float(t["quoteVolume"]), "chg": abs(float(t["priceChangePercent"]))}
                     for t in res if t["symbol"].endswith("USDT") and t["symbol"][:-4] not in EXCLUDED_SYMBOLS and float(t["quoteVolume"]) >= MIN_QUOTE_VOLUME]
            valid.sort(key=lambda x: (x["chg"] * 0.7) + (x["vol"] * 0.3), reverse=True)
            self.active_coins =["BTCUSDT", "ETHUSDT"] + [c["sym"] for c in valid[:TOP_COINS_COUNT]]
            self.active_coins = list(set(self.active_coins)) # Deduplicate
            logger.info(f"Updated Top Coins. Tracking {len(self.active_coins)} pairs.")
        except Exception as e:
            logger.error(f"REST Top Coins Error: {e}")

    def load_historical_klines(self):
        logger.info("Downloading historical klines via REST...")
        for coin in self.active_coins:
            try:
                res = self.session.get(f"{self.rest_url}/api/v3/klines", params={"symbol": coin, "interval": SCAN_TF, "limit": 200}, timeout=10).json()
                with self.cache_lock:
                    self.klines_cache[coin] =[
                        {"t": k[0], "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])} for k in res
                    ]
            except Exception as e:
                logger.error(f"Failed to fetch history for {coin}: {e}")
            time.sleep(0.05)

    def get_dataframe(self, symbol):
        with self.cache_lock:
            if symbol not in self.klines_cache or len(self.klines_cache[symbol]) < 200: return None
            df = pd.DataFrame(self.klines_cache[symbol])
            df.rename(columns={"t":"timestamp", "o":"open", "h":"high", "l":"low", "c":"close", "v":"volume"}, inplace=True)
            return df

    def _ws_on_message(self, ws, message):
        try:
            data = json.loads(message)
            # MiniTicker Handle
            if isinstance(data, list):
                for item in data:
                    if item.get('e') == '24hrMiniTicker':
                        self.live_prices[item['s']] = float(item['c'])
            # Kline Handle
            elif data.get('e') == 'kline':
                sym = data['s']
                k = data['k']
                if sym in self.klines_cache:
                    candle = {"t": k['t'], "o": float(k['o']), "h": float(k['h']), "l": float(k['l']), "c": float(k['c']), "v": float(k['v'])}
                    with self.cache_lock:
                        if self.klines_cache[sym][-1]['t'] == candle['t']:
                            self.klines_cache[sym][-1] = candle # Update forming candle
                        else:
                            self.klines_cache[sym].append(candle) # Append new closed candle
                            if len(self.klines_cache[sym]) > 200: self.klines_cache[sym].pop(0) # Maintain memory limit
        except Exception: pass

    def _ws_on_open(self, ws):
        logger.info("WebSocket Connected. Subscribing to streams...")
        # Subscribe to MiniTicker (All) + specific Klines
        streams = ["!miniTicker@arr"] +[f"{c.lower()}@kline_{SCAN_TF}" for c in self.active_coins]
        
        # Binance limits 50 streams per payload, split if needed
        chunks = [streams[x:x+50] for x in range(0, len(streams), 50)]
        for i, chunk in enumerate(chunks):
            ws.send(json.dumps({"method": "SUBSCRIBE", "params": chunk, "id": i+1}))
            time.sleep(1)

    def _ws_on_close(self, ws, close_status_code, close_msg):
        logger.warning("WebSocket Disconnected. Reconnecting in 5s...")
        if self.running:
            time.sleep(5)
            self.start_websocket()

    def start_websocket(self):
        self.ws = websocket.WebSocketApp(self.ws_url, on_open=self._ws_on_open, on_message=self._ws_on_message, on_close=self._ws_on_close)
        self.ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        self.ws_thread.start()

# ============================================================
# TECHNICAL INDICATORS
# ============================================================
def ema(series, length): return series.ewm(span=length, adjust=False).mean()
def sma(series, length): return series.rolling(window=length).mean()
def atr(df, length=14):
    tr = pd.concat([df["high"]-df["low"], (df["high"]-df["close"].shift(1)).abs(), (df["low"]-df["close"].shift(1)).abs()], axis=1).max(axis=1)
    return tr.rolling(window=length).mean()
def rsi(series, length=14):
    delta = series.diff()
    rs = delta.clip(lower=0).rolling(length).mean() / (-delta.clip(upper=0)).rolling(length).mean()
    return 100 - (100 / (1 + rs))
def stoch_rsi(rsi_series, length=14, k=3, d=3):
    r_min, r_max = rsi_series.rolling(length).min(), rsi_series.rolling(length).max()
    stoch = ((rsi_series - r_min) / (r_max - r_min) * 100).fillna(50)
    stoch_k = stoch.rolling(k).mean()
    return stoch_k, stoch_k.rolling(d).mean()

# ============================================================
# STRATEGY INDICATORS (Independent Engines)
# ============================================================
class GodModeIndicator:
    def analyze(self, df):
        i = -2
        c, o, l = df["close"].iloc[i], df["open"].iloc[i], df["low"].iloc[i]
        ema50, ema200, atr14 = ema(df["close"], 50).iloc[i], ema(df["close"], 200).iloc[i], atr(df, 14).iloc[i]
        if c < ema50 or ema50 < ema200 or (c - ema50)/ema50 > 0.10: return[] # Strict Overextension filter
        
        sk, sd = stoch_rsi(rsi(df["close"], 14))
        if sk.iloc[i] > sd.iloc[i] and sk.iloc[i] < 45:
            c2, o2, c1, o1 = df["close"].iloc[i-2], df["open"].iloc[i-2], df["close"].iloc[i-1], df["open"].iloc[i-1]
            if (c2 < o2) and (c1 > o1) and (c1 > df["high"].iloc[i-2]):
                return[{"type": "GOD_MODE BUY", "entry": c, "sl": l - (atr14*1.2), "tp1": c + (atr14*2), "tp2": c + (atr14*4), "tp3": c + (atr14*6)}]
        return[]

class RibbonIndicator:
    def analyze(self, df):
        i = -2
        c, atr14, ema50 = df["close"].iloc[i], atr(df, 14).iloc[i], ema(df["close"], 50).iloc[i]
        if (c - ema50)/ema50 > 0.10: return[]
        green = sum(1 for j in range(i-2, i+1) if df["close"].iloc[j] > ema50)
        if green >= 3 and df["close"].iloc[i-3] <= ema(df["close"], 50).iloc[i-3]:
            return[{"type": "RIBBON BUY", "entry": c, "sl": c - (atr14*2), "tp1": c + (atr14*3), "tp2": c + (atr14*5)}]
        return[]

class QuantumIndicator:
    def analyze(self, df):
        i = -2
        c, atr14 = df["close"].iloc[i], atr(df, 14).iloc[i]
        bb_sma, bb_stdev = sma(df["close"], 20).iloc[i], df["close"].iloc[i-20:i].std()
        is_squeeze = (bb_sma + 2*bb_stdev) - (bb_sma - 2*bb_stdev) < (atr14 * 2)
        if is_squeeze and df["close"].iloc[i] > df["high"].iloc[i-1]:
            return[{"type": "QUANTUM BUY", "entry": c, "sl": c - atr14*1.5, "tp1": c + atr14*2.5, "tp2": c + atr14*4.5}]
        return[]

# ============================================================
# INTELLIGENCE & CONFLUENCE ENGINE
# ============================================================
class TrueConfluenceEngine:
    def __init__(self):
        self.god = GodModeIndicator()
        self.rib = RibbonIndicator()
        self.qnt = QuantumIndicator()

    def analyze_market(self, df, btc_df):
        # 1. Regime
        btc_c, btc_e50, btc_e200 = btc_df["close"].iloc[-2], ema(btc_df["close"], 50).iloc[-2], ema(btc_df["close"], 200).iloc[-2]
        btc_rsi = rsi(btc_df["close"], 14).iloc[-2]
        regime_score = 40 if btc_c > btc_e200 else 0
        regime_score += 20 if btc_c > btc_e50 else 0
        regime_score += 20 if 40 < btc_rsi < 70 else (-20 if btc_rsi > 75 else 0)
        regime = "BULLISH" if regime_score >= 60 else "NEUTRAL" if regime_score >= 30 else "BEARISH"

        # 2. Whale & Momentum
        i = -2
        vol, vol_sma = df["volume"].iloc[i], sma(df["volume"], 20).iloc[i]
        c, o, h = df["close"].iloc[i], df["open"].iloc[i], df["high"].iloc[i]
        vol_ratio = vol / vol_sma if vol_sma > 0 else 0
        
        whale_score = 30 if vol_ratio > 3 else (15 if vol_ratio > 2 else 0)
        whale_score += 15 if c > o else 0
        if vol_ratio > 3 and (h - max(c,o)) > abs(c-o)*2: whale_score = 0 # Exhaustion rejection

        gain_24h = ((c - df["close"].iloc[max(i-24,0)]) / df["close"].iloc[max(i-24,0)]) * 100
        mom_score = 30 if 2 < gain_24h < 12 else (10 if gain_24h >= 12 else 0) # Overextended rejection
        if 50 < rsi(df["close"], 14).iloc[i] < 65: mom_score += 20

        # 3. Indicators
        god_s = self.god.analyze(df)
        rib_s = self.rib.analyze(df)
        qnt_s = self.qnt.analyze(df)
        
        all_sigs = god_s + rib_s + qnt_s
        if not all_sigs: return None

        active_inds = set([s["type"].split()[0] for s in all_sigs])
        
        # 4. Scoring & Filtering
        base_score = (whale_score * 0.3) + (mom_score * 0.3) + (regime_score * 0.2)
        if len(active_inds) == 1: base_score += 10
        elif len(active_inds) == 2: base_score += 25
        elif len(active_inds) >= 3: base_score += 40

        # Risk Reward Filter (Require RR >= 1.2)
        best_sig = all_sigs[0]
        risk = abs(best_sig["entry"] - best_sig["sl"])
        reward = abs(best_sig["tp1"] - best_sig["entry"])
        if risk == 0 or (reward / risk) < 1.2: return None # Low probability RR rejection

        is_elite = len(active_inds) >= 2 and whale_score >= 45 and mom_score >= 50 and regime == "BULLISH"
        final_score = max(ELITE_THRESHOLD, min(100, int(base_score))) if is_elite else min(100, int(base_score))

        return {
            "entry": best_sig["entry"], "sl": best_sig["sl"], "tp1": best_sig["tp1"], 
            "tp2": best_sig.get("tp2"), "tp3": best_sig.get("tp3"),
            "global_score": final_score, "whale_score": whale_score, "momentum_score": mom_score,
            "btc_regime": regime, "indicators": list(active_inds), "is_elite": is_elite
        }

# ============================================================
# TRADE LIFECYCLE MANAGER (Live WS Driven)
# ============================================================
class TradeLifecycleManager:
    def __init__(self, db, tg, data):
        self.db = db
        self.tg = tg
        self.data = data

    def manage(self):
        trades = self.db.execute_query("SELECT id, coin, entry_price, current_sl, tp1, tp2, tp3, tp1_hit, tp2_hit FROM active_trades WHERE status='OPEN'", fetch=True)
        for tid, coin, entry, sl, tp1, tp2, tp3, tp1_hit, tp2_hit in trades:
            curr_price = self.data.live_prices.get(coin)
            if not curr_price: continue
            
            pnl = ((curr_price - entry) / entry) * 100
            
            if curr_price <= sl:
                self.db.execute_query("UPDATE active_trades SET status='CLOSED_SL' WHERE id=?", (tid,))
                self.db.update_stats("sl_hits")
                self.tg.send_trade_update(coin, "SL", curr_price, pnl)
            
            elif tp1 and curr_price >= tp1 and not tp1_hit:
                # Move SL to Entry + 0.2% (Cover Fees)
                be_sl = entry * 1.002
                self.db.execute_query("UPDATE active_trades SET tp1_hit=1, current_sl=? WHERE id=?", (be_sl, tid))
                self.db.update_stats("tp_hits")
                self.tg.send_trade_update(coin, "TP1", curr_price, pnl)
                
            elif tp2 and curr_price >= tp2 and not tp2_hit:
                self.db.execute_query("UPDATE active_trades SET tp2_hit=1, current_sl=? WHERE id=?", (tp1, tid))
                self.tg.send_trade_update(coin, "TP2", curr_price, pnl)
                
            elif tp3 and curr_price >= tp3:
                self.db.execute_query("UPDATE active_trades SET status='CLOSED_TP' WHERE id=?", (tid,))
                self.tg.send_trade_update(coin, "TP3", curr_price, pnl)

# ============================================================
# MASTER ORCHESTRATOR
# ============================================================
class OmegaScannerUltra:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        self.db = DatabaseManager()
        self.tg = TelegramManager(TELEGRAM_TOKEN, CHAT_ID)
        self.data = BinanceHybridManager()
        self.engine = TrueConfluenceEngine()
        self.trader = TradeLifecycleManager(self.db, self.tg, self.data)

        self.last_scan = 0
        self.last_heartbeat = time.time()
        self.last_cleanup = time.time()

    def shutdown(self, *args):
        logger.info("Initiating graceful shutdown...")
        self.running = False
        self.data.ws.close()
        self.tg.running = False
        sys.exit(0)

    def run(self):
        logger.info("="*60)
        logger.info("🚀 OMEGA SCANNER PRO v5.0 [ULTRA PRODUCTION]")
        logger.info("Architecture: Live WS Hybrid | True Confluence | SQLite WAL")
        logger.info("="*60)
        self.tg.send("🟢 <b>SYSTEM ONLINE</b>\nOmega Scanner Ultra v5.0 Started.\nEngine: Live WebSocket Hybrid")
        
        # 1. Boot Sequence
        self.data.get_top_coins()
        self.data.load_historical_klines()
        self.data.start_websocket()

        while self.running:
            try:
                now = time.time()

                # Daily Maintenance
                if now - self.last_cleanup > 86400:
                    self.db.cleanup_old_data()
                    self.data.get_top_coins()
                    self.data.load_historical_klines() # Refresh cache integrity
                    self.last_cleanup = now

                # Fast Scan Loop (Iterating over Memory Cache, NO REST API)
                if now - self.last_scan > FAST_SCAN_INTERVAL:
                    btc_df = self.data.get_dataframe("BTCUSDT")
                    if btc_df is not None:
                        for coin in self.data.active_coins:
                            if coin == "BTCUSDT" or coin == "ETHUSDT": continue
                            df = self.data.get_dataframe(coin)
                            if df is None: continue

                            res = self.engine.analyze_market(df, btc_df)
                            if res and res["global_score"] >= GOOD_THRESHOLD:
                                if not self.db.is_on_cooldown(coin, res["is_elite"]):
                                    res["coin"] = coin
                                    res["timeframe"] = SCAN_TF
                                    self.db.save_signal(res)
                                    self.db.log_trade(res)
                                    self.db.set_cooldown(coin, ELITE_COOLDOWN_HOURS if res["is_elite"] else SIGNAL_COOLDOWN_HOURS)
                                    self.db.update_stats("elite_signals" if res["is_elite"] else "total_signals")
                                    self.tg.send_signal(res)
                                    logger.info(f"Signal Generated: {coin} | Score: {res['global_score']} | Elite: {res['is_elite']}")

                    self.last_scan = time.time()
                    gc.collect()

                # Live Trade Management Loop
                self.trader.manage()

                # Heartbeat
                if now - self.last_heartbeat > HEARTBEAT_INTERVAL:
                    stats = self.db.execute_query("SELECT total_signals, elite_signals, tp_hits, sl_hits FROM stats WHERE id=1", fetch=True)
                    if stats:
                        self.tg.send(f"💓 <b>SYSTEM HEARTBEAT</b>\nActive WS Streams: {len(self.data.active_coins)}\nTotal Signals: {stats[0][0]}\nElite Signals: {stats[0][1]}\nTP Hits: {stats[0][2]} | SL Hits: {stats[0][3]}")
                    self.last_heartbeat = now

                time.sleep(TRADE_CHECK_INTERVAL)

            except Exception as e:
                logger.error(f"Main Loop Error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    app = OmegaScannerUltra()
    app.run()
