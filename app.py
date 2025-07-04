import asyncio
import aiohttp
import aiofiles
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set
import json
import hashlib
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import threading
from functools import wraps
import numpy as np
import pandas as pd
from collections import defaultdict, deque
import sqlite3
import aiosqlite
import redis.asyncio as redis
from contextlib import asynccontextmanager
import websockets
import alpaca_trade_api as tradeapi
from flask import Flask, jsonify, request
from flask_cors import CORS
import signal
import sys
import os
from decimal import Decimal, ROUND_HALF_UP

# High-performance async Flask setup
app = Flask(__name__)
CORS(app)

# OPTIMIZED TRADING CONFIGURATION
TRADING_CONFIG = {
    'max_daily_exposure': 200.0,
    'max_position_size': 100.0,
    'min_score_threshold': 70.0,
    'min_stock_price': 5.00,
    'stop_loss_pct': 0.08,
    'take_profit_1_pct': 0.12,
    'take_profit_2_pct': 0.25,
    'max_daily_trades': 3,
    'cooldown_minutes': 30,
    'validation_mode': True,
    'circuit_breaker_threshold': 5,  # Max consecutive failures
    'data_staleness_limit': 60,      # Seconds before data considered stale
    'batch_size': 50,                # Symbols per batch
    'max_concurrent_requests': 10,   # API rate limiting
}

# ALPACA CONFIGURATION
ALPACA_CONFIG = {
    'api_key': "UV7nWgv_D-6aTJ@",
    'secret_key': "MRCCPF42QDPZO4UD",
    'base_url': "https://paper-api.alpaca.markets",
    'ws_url': "wss://stream.data.alpaca.markets/v2/iex"
}

# OPTIMIZED CACHE CONFIGURATION
CACHE_CONFIG = {
    'redis_url': os.getenv('REDIS_URL', 'redis://localhost:6379'),
    'scan_results_ttl': 180,
    'account_data_ttl': 30,
    'price_data_ttl': 15,
    'volume_data_ttl': 300,
    'market_data_ttl': 60,
    'max_cache_memory': '100mb'
}

# Enhanced logging with structured output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(funcName)s:%(lineno)d | %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('optimized_trading.log', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class CircuitBreaker:
    """Circuit breaker pattern for API failure handling"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self.lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        async with self.lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                else:
                    raise Exception("Circuit breaker OPEN - service unavailable")
            
            try:
                result = await func(*args, **kwargs)
                if self.state == 'HALF_OPEN':
                    self.state = 'CLOSED'
                    self.failure_count = 0
                return result
            
            except Exception as e:
                self.failure_count += 1
                self.last_failure_time = time.time()
                
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                    logger.error(f"Circuit breaker OPEN after {self.failure_count} failures")
                
                raise e

class OptimizedCache:
    """High-performance Redis-based caching with fallback"""
    
    def __init__(self):
        self.redis_client = None
        self.local_cache = {}
        self.cache_stats = defaultdict(int)
        self.lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize Redis connection with fallback to local cache"""
        try:
            self.redis_client = redis.from_url(
                CACHE_CONFIG['redis_url'],
                encoding="utf-8",
                decode_responses=True,
                max_connections=20
            )
            await self.redis_client.ping()
            logger.info("Redis cache initialized successfully")
        except Exception as e:
            logger.warning(f"Redis unavailable, using local cache: {e}")
            self.redis_client = None
    
    def _generate_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate consistent cache key with collision resistance"""
        key_data = f"{prefix}:{json.dumps(args, sort_keys=True)}:{json.dumps(kwargs, sort_keys=True)}"
        return f"squeeze:{hashlib.sha256(key_data.encode()).hexdigest()[:16]}"
    
    async def get(self, key: str) -> Optional[dict]:
        """Get from cache with automatic fallback"""
        try:
            if self.redis_client:
                data = await self.redis_client.get(key)
                if data:
                    self.cache_stats['redis_hits'] += 1
                    return json.loads(data)
            
            # Fallback to local cache
            async with self.lock:
                if key in self.local_cache:
                    data, timestamp = self.local_cache[key]
                    if time.time() - timestamp < 300:  # 5 minute local TTL
                        self.cache_stats['local_hits'] += 1
                        return data
                    else:
                        del self.local_cache[key]
            
            self.cache_stats['misses'] += 1
            return None
            
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
    
    async def set(self, key: str, data: dict, ttl: int = 300):
        """Set in cache with dual storage"""
        try:
            serialized = json.dumps(data, default=str)
            
            if self.redis_client:
                await self.redis_client.setex(key, ttl, serialized)
            
            # Always store locally as backup
            async with self.lock:
                self.local_cache[key] = (data, time.time())
                # Cleanup old local entries
                if len(self.local_cache) > 1000:
                    oldest_keys = sorted(
                        self.local_cache.keys(),
                        key=lambda k: self.local_cache[k][1]
                    )[:100]
                    for old_key in oldest_keys:
                        del self.local_cache[old_key]
        
        except Exception as e:
            logger.error(f"Cache set error: {e}")

    async def delete(self, pattern: str):
        """Delete cache entries by pattern"""
        try:
            if self.redis_client:
                keys = await self.redis_client.keys(f"squeeze:*{pattern}*")
                if keys:
                    await self.redis_client.delete(*keys)
        except Exception as e:
            logger.error(f"Cache delete error: {e}")

@dataclass
class MarketData:
    """Optimized market data structure"""
    symbol: str
    price: float
    volume: int
    timestamp: float
    avg_volume_20d: float = 0.0
    volume_ratio: float = 0.0
    market_cap: float = 0.0
    short_ratio: float = 0.0
    sector: str = ""
    data_quality: float = 100.0
    
    def is_stale(self, max_age: int = 60) -> bool:
        return time.time() - self.timestamp > max_age
    
    def to_dict(self) -> dict:
        return {
            'symbol': self.symbol,
            'price': self.price,
            'volume': self.volume,
            'timestamp': self.timestamp,
            'avg_volume_20d': self.avg_volume_20d,
            'volume_ratio': self.volume_ratio,
            'market_cap': self.market_cap,
            'short_ratio': self.short_ratio,
            'sector': self.sector,
            'data_quality': self.data_quality
        }

@dataclass
class TradingState:
    """Thread-safe trading state management"""
    daily_trade_count: int = 0
    daily_exposure: float = 0.0
    daily_pnl: float = 0.0
    last_reset_date: str = ""
    emergency_stop: bool = False
    circuit_breaker_active: bool = False
    consecutive_losses: int = 0
    
    def __post_init__(self):
        self.lock = asyncio.Lock()
    
    async def reset_if_new_day(self) -> bool:
        async with self.lock:
            current_date = datetime.now().date().isoformat()
            if current_date != self.last_reset_date:
                self.daily_trade_count = 0
                self.daily_exposure = 0.0
                self.daily_pnl = 0.0
                self.last_reset_date = current_date
                self.emergency_stop = False
                self.consecutive_losses = 0
                logger.info(f"Trading state reset for new day: {current_date}")
                return True
            return False
    
    async def can_trade(self, position_value: float) -> Tuple[bool, str]:
        async with self.lock:
            if self.emergency_stop:
                return False, "Emergency stop active"
            
            if self.circuit_breaker_active:
                return False, "Circuit breaker active"
            
            if self.daily_trade_count >= TRADING_CONFIG['max_daily_trades']:
                return False, f"Daily trade limit reached ({self.daily_trade_count}/{TRADING_CONFIG['max_daily_trades']})"
            
            if self.daily_exposure + position_value > TRADING_CONFIG['max_daily_exposure']:
                remaining = TRADING_CONFIG['max_daily_exposure'] - self.daily_exposure
                return False, f"Daily exposure limit: ${remaining:.0f} remaining"
            
            return True, "OK"
    
    async def add_trade(self, position_value: float):
        async with self.lock:
            self.daily_trade_count += 1
            self.daily_exposure += position_value

class OptimizedDataProvider:
    """High-performance multi-source data provider with streaming"""
    
    def __init__(self, cache: OptimizedCache):
        self.cache = cache
        self.session = None
        self.circuit_breaker = CircuitBreaker()
        self.rate_limiter = asyncio.Semaphore(TRADING_CONFIG['max_concurrent_requests'])
        self.websocket_data = {}
        self.data_quality_scores = defaultdict(float)
        
    async def initialize(self):
        """Initialize HTTP session and websocket connections"""
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'OptimizedSqueezeTrader/1.0'}
        )
        
        logger.info("Data provider initialized with optimized HTTP session")
    
    async def close(self):
        """Clean shutdown"""
        if self.session:
            await self.session.close()
    
    async def _fetch_with_rate_limit(self, url: str, params: dict = None) -> dict:
        """Rate-limited HTTP fetch with circuit breaker"""
        async with self.rate_limiter:
            return await self.circuit_breaker.call(self._fetch_raw, url, params)
    
    async def _fetch_raw(self, url: str, params: dict = None) -> dict:
        """Raw HTTP fetch with timeout and retry"""
        async with self.session.get(url, params=params) as response:
            if response.status != 200:
                raise aiohttp.ClientError(f"HTTP {response.status}")
            return await response.json()
    
    async def get_batch_market_data(self, symbols: List[str]) -> Dict[str, MarketData]:
        """Optimized batch data fetching with caching"""
        results = {}
        cache_misses = []
        
        # Check cache first
        for symbol in symbols:
            cache_key = self.cache._generate_key("market_data", symbol)
            cached_data = await self.cache.get(cache_key)
            
            if cached_data and not MarketData(**cached_data).is_stale():
                results[symbol] = MarketData(**cached_data)
            else:
                cache_misses.append(symbol)
        
        # Fetch missing data in batches
        if cache_misses:
            batch_results = await self._fetch_batch_data(cache_misses)
            
            for symbol, data in batch_results.items():
                if data:
                    results[symbol] = data
                    # Cache the result
                    cache_key = self.cache._generate_key("market_data", symbol)
                    await self.cache.set(cache_key, data.to_dict(), CACHE_CONFIG['market_data_ttl'])
        
        return results
    
    async def _fetch_batch_data(self, symbols: List[str]) -> Dict[str, Optional[MarketData]]:
        """Fetch data for multiple symbols using optimized API calls"""
        results = {}
        
        # Split into batches to avoid API limits
        batch_size = TRADING_CONFIG['batch_size']
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            batch_tasks = [self._fetch_single_symbol_optimized(symbol) for symbol in batch]
            
            try:
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for symbol, result in zip(batch, batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to fetch {symbol}: {result}")
                        results[symbol] = None
                        self.data_quality_scores[symbol] *= 0.9  # Degrade quality score
                    else:
                        results[symbol] = result
                        self.data_quality_scores[symbol] = min(100.0, self.data_quality_scores[symbol] + 1.0)
                
                # Rate limiting between batches
                if i + batch_size < len(symbols):
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Batch processing failed: {e}")
                for symbol in batch:
                    results[symbol] = None
        
        return results
    
    async def _fetch_single_symbol_optimized(self, symbol: str) -> Optional[MarketData]:
        """Optimized single symbol data fetch using multiple APIs"""
        try:
            # Primary: Yahoo Finance API (faster than yfinance library)
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            params = {
                'range': '5d',
                'interval': '1d',
                'indicators': 'quote',
                'includePrePost': 'true'
            }
            
            data = await self._fetch_with_rate_limit(url, params)
            
            if not data.get('chart', {}).get('result'):
                return None
            
            chart_data = data['chart']['result'][0]
            
            # Extract current price and volume
            quotes = chart_data['indicators']['quote'][0]
            timestamps = chart_data['timestamp']
            
            if not quotes['close'] or len(quotes['close']) == 0:
                return None
            
            current_price = quotes['close'][-1]
            current_volume = quotes['volume'][-1] if quotes['volume'][-1] else 0
            
            # Calculate 20-day average volume
            volumes = [v for v in quotes['volume'][-20:] if v is not None]
            avg_volume_20d = sum(volumes) / len(volumes) if volumes else 0
            
            volume_ratio = current_volume / avg_volume_20d if avg_volume_20d > 0 else 0
            
            # Get additional metadata (cached separately due to lower update frequency)
            metadata = await self._get_symbol_metadata(symbol)
            
            return MarketData(
                symbol=symbol,
                price=float(current_price) if current_price else 0.0,
                volume=int(current_volume) if current_volume else 0,
                timestamp=time.time(),
                avg_volume_20d=avg_volume_20d,
                volume_ratio=volume_ratio,
                market_cap=metadata.get('market_cap', 0.0),
                short_ratio=metadata.get('short_ratio', 0.0),
                sector=metadata.get('sector', ''),
                data_quality=self.data_quality_scores.get(symbol, 100.0)
            )
            
        except Exception as e:
            logger.error(f"Symbol data fetch failed for {symbol}: {e}")
            return None
    
    async def _get_symbol_metadata(self, symbol: str) -> dict:
        """Get symbol metadata with aggressive caching"""
        cache_key = self.cache._generate_key("metadata", symbol)
        cached = await self.cache.get(cache_key)
        
        if cached:
            return cached
        
        try:
            # Use Yahoo Finance quote API for metadata
            url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
            params = {
                'modules': 'defaultKeyStatistics,summaryProfile',
                'formatted': 'false'
            }
            
            data = await self._fetch_with_rate_limit(url, params)
            
            result = data.get('quoteSummary', {}).get('result', [{}])[0]
            
            key_stats = result.get('defaultKeyStatistics', {})
            profile = result.get('summaryProfile', {})
            
            metadata = {
                'market_cap': key_stats.get('marketCap', {}).get('raw', 0.0),
                'short_ratio': key_stats.get('shortRatio', {}).get('raw', 0.0),
                'sector': profile.get('sector', ''),
                'industry': profile.get('industry', '')
            }
            
            # Cache for 1 hour (metadata changes infrequently)
            await self.cache.set(cache_key, metadata, 3600)
            return metadata
            
        except Exception as e:
            logger.error(f"Metadata fetch failed for {symbol}: {e}")
            return {'market_cap': 0.0, 'short_ratio': 0.0, 'sector': '', 'industry': ''}

class OptimizedSqueezeScanner:
    """High-performance squeeze scanner with intelligent filtering"""
    
    def __init__(self, cache: OptimizedCache, data_provider: OptimizedDataProvider):
        self.cache = cache
        self.data_provider = data_provider
        self.performance_metrics = defaultdict(list)
        
        # Optimized symbol universe with proven performers
        self.core_symbols = [
            'QUBT', 'SOUN', 'RGTI', 'IONQ', 'QBTS',    # Proven quantum/AI
            'NVDA', 'AMD', 'PLTR', 'TSLA', 'AAPL',     # Blue chip momentum
            'AI', 'SNOW', 'NET', 'CRWD', 'ZS',         # High-quality tech
            'MSTR', 'COIN', 'RBLX', 'U', 'DDOG'        # Volatile momentum plays
        ]
        
        # Dynamic expansion candidates (higher risk)
        self.expansion_candidates = [
            'HOOD', 'SOFI', 'UPST', 'AFRM', 'SQ',
            'ROKU', 'PINS', 'SNAP', 'UBER', 'LYFT'
        ]
    
    async def scan_optimized(self, min_score: float = None) -> List[dict]:
        """Optimized parallel scanning with performance tracking"""
        scan_start = time.time()
        
        if min_score is None:
            min_score = TRADING_CONFIG['min_score_threshold']
        
        logger.info(f"Starting optimized scan (threshold: {min_score})")
        
        # Get market data for all symbols
        all_symbols = self.core_symbols + self.expansion_candidates
        market_data = await self.data_provider.get_batch_market_data(all_symbols)
        
        # Filter and score in parallel
        scoring_tasks = []
        for symbol, data in market_data.items():
            if data and not data.is_stale():
                task = self._score_symbol_optimized(symbol, data)
                scoring_tasks.append(task)
        
        scored_results = await asyncio.gather(*scoring_tasks, return_exceptions=True)
        
        # Process results
        qualified = []
        for result in scored_results:
            if isinstance(result, Exception):
                logger.error(f"Scoring failed: {result}")
                continue
            
            if result and result['score'] >= min_score:
                qualified.append(result)
        
        # Sort by score and apply final filters
        qualified.sort(key=lambda x: x['score'], reverse=True)
        
        # Performance tracking
        scan_duration = time.time() - scan_start
        self.performance_metrics['scan_times'].append(scan_duration)
        
        # Keep only last 100 scan times
        if len(self.performance_metrics['scan_times']) > 100:
            self.performance_metrics['scan_times'] = self.performance_metrics['scan_times'][-100:]
        
        logger.info(f"Scan complete: {len(qualified)} qualified in {scan_duration:.2f}s")
        
        # Cache scan results
        cache_key = self.cache._generate_key("scan_results", min_score, scan_start)
        await self.cache.set(cache_key, {
            'results': qualified,
            'scan_time': scan_duration,
            'total_symbols': len(all_symbols),
            'qualified_count': len(qualified)
        }, CACHE_CONFIG['scan_results_ttl'])
        
        return qualified
    
    async def _score_symbol_optimized(self, symbol: str, data: MarketData) -> Optional[dict]:
        """Optimized scoring algorithm with your proven parameters"""
        try:
            # Quick filters first (fail fast)
            if data.price < TRADING_CONFIG['min_stock_price']:
                return None
            
            if data.price > 100.0:  # Too expensive for small positions
                return None
            
            if data.volume_ratio < 1.5:  # Minimum volume surge
                return None
            
            if data.data_quality < 70.0:  # Require good data quality
                return None
            
            # Your proven scoring algorithm
            score = 0
            
            # Volume surge (40% of score) - Your highest weight
            if data.volume_ratio >= 3.0:
                volume_score = 40
            elif data.volume_ratio >= 2.0:
                volume_score = 30
            elif data.volume_ratio >= 1.5:
                volume_score = 20
            else:
                volume_score = 0
            score += volume_score
            
            # Price range optimization (25% of score)
            if 10 <= data.price <= 25:      # Your optimal range
                price_score = 25
            elif 8 <= data.price <= 35:
                price_score = 15
            elif 5 <= data.price <= 50:
                price_score = 8
            else:
                price_score = 0
            score += price_score
            
            # Market cap (20% of score) - Mid-cap preference
            if 500_000_000 <= data.market_cap <= 5_000_000_000:
                cap_score = 20
            elif 200_000_000 <= data.market_cap <= 10_000_000_000:
                cap_score = 12
            else:
                cap_score = 5  # Still give some points
            score += cap_score
            
            # Short interest (15% of score)
            if data.short_ratio >= 4:
                short_score = 15
            elif data.short_ratio >= 2.5:
                short_score = 10
            elif data.short_ratio >= 1.5:
                short_score = 5
            else:
                short_score = 0
            score += short_score
            
            # Data quality bonus
            if data.data_quality >= 95:
                score += 5
            elif data.data_quality >= 85:
                score += 2
            
            return {
                'symbol': symbol,
                'score': min(int(score), 100),
                'current_price': data.price,
                'volume_ratio': data.volume_ratio,
                'short_ratio': data.short_ratio,
                'market_cap': data.market_cap,
                'sector': data.sector,
                'data_quality': data.data_quality,
                'timestamp': data.timestamp,
                'breakdown': {
                    'volume_score': volume_score,
                    'price_score': price_score,
                    'market_cap_score': cap_score,
                    'short_score': short_score
                }
            }
            
        except Exception as e:
            logger.error(f"Scoring failed for {symbol}: {e}")
            return None

class OptimizedTradingExecutor:
    """High-performance trading executor with proper risk management"""
    
    def __init__(self, cache: OptimizedCache, trading_state: TradingState):
        self.cache = cache
        self.trading_state = trading_state
        self.alpaca_api = tradeapi.REST(
            ALPACA_CONFIG['api_key'],
            ALPACA_CONFIG['secret_key'],
            ALPACA_CONFIG['base_url'],
            api_version='v2'
        )
        
        # Performance tracking
        self.execution_times = deque(maxlen=100)
        self.success_rate = 1.0
    
    async def execute_trade_optimized(self, opportunity: dict) -> dict:
        """Execute trade with optimized performance and safety checks"""
        execution_start = time.time()
        
        try:
            symbol = opportunity['symbol']
            score = opportunity['score']
            current_price = opportunity['current_price']
            
            # Fast validation
            valid, reason = await self._validate_trade_fast(symbol, score, current_price)
            if not valid:
                return {
                    'success': False,
                    'reason': reason,
                    'execution_time_ms': (time.time() - execution_start) * 1000
                }
            
            # Calculate optimal position size
            position_size = await self._calculate_optimal_position_size(current_price, score)
            position_value = position_size * current_price
            
            # Generate exit levels
            stop_price = current_price * (1 - TRADING_CONFIG['stop_loss_pct'])
            tp1_price = current_price * (1 + TRADING_CONFIG['take_profit_1_pct'])
            tp2_price = current_price * (1 + TRADING_CONFIG['take_profit_2_pct'])
            
            # Execute bracket order
            order = await self._execute_bracket_order(
                symbol=symbol,
                quantity=position_size,
                entry_price=current_price,
                stop_price=stop_price,
                tp1_price=tp1_price
            )
            
            # Update trading state
            await self.trading_state.add_trade(position_value)
            
            execution_time = (time.time() - execution_start) * 1000
            self.execution_times.append(execution_time)
            
            # Log successful execution
            await self._log_trade_optimized(symbol, score, position_size, order, current_price)
            
            logger.info(f"✅ TRADE EXECUTED: {symbol} | {position_size} @ ${current_price:.2f} | Order: {order.id}")
            
            return {
                'success': True,
                'order_id': order.id,
                'symbol': symbol,
                'shares': position_size,
                'entry_price': current_price,
                'position_value': position_value,
                'stop_loss': stop_price,
                'take_profit_1': tp1_price,
                'take_profit_2': tp2_price,
                'execution_time_ms': execution_time,
                'daily_stats': {
                    'trade_count': self.trading_state.daily_trade_count,
                    'exposure': self.trading_state.daily_exposure
                }
            }
            
        except Exception as e:
            logger.error(f"Trade execution failed for {opportunity.get('symbol', 'unknown')}: {e}")
            execution_time = (time.time() - execution_start) * 1000
            return {
                'success': False,
                'error': str(e),
                'execution_time_ms': execution_time
            }
    
    async def _validate_trade_fast(self, symbol: str, score: float, price: float) -> Tuple[bool, str]:
        """Fast trade validation with minimal API calls"""
        # Reset daily state if needed
        await self.trading_state.reset_if_new_day()
        
        # Quick checks first
        if score < TRADING_CONFIG['min_score_threshold']:
            return False, f"Score {score} below threshold"
        
        if price < TRADING_CONFIG['min_stock_price']:
            return False, f"Price ${price:.2f} below minimum"
        
        # Position size calculation
        position_size = await self._calculate_optimal_position_size(price, score)
        position_value = position_size * price
        
        # Trading state validation
        can_trade, reason = await self.trading_state.can_trade(position_value)
        if not can_trade:
            return False, reason
        
        # Check buying power (cached)
        account_data = await self._get_account_data_cached()
        if account_data['buying_power'] < position_value:
            return False, f"Insufficient buying power: ${account_data['buying_power']:.0f} < ${position_value:.