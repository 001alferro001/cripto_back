import asyncio
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'tradingbase'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }

    async def initialize(self):
        """Инициализация подключения к базе данных"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = True
            logger.info("Подключение к базе данных установлено")

            await self.create_tables()
            logger.info("Таблицы базы данных проверены/созданы")

        except Exception as e:
            logger.error(f"Ошибка подключения к базе данных: {e}")
            raise

    async def create_tables(self):
        """Создание необходимых таблиц"""
        try:
            cursor = self.connection.cursor()

            # Таблица торговых пар
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    price_drop_percentage FLOAT,
                    current_price FLOAT,
                    historical_price FLOAT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Таблица исторических свечных данных (закрытые свечи)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kline_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    open_time_ms BIGINT NOT NULL,
                    close_time_ms BIGINT NOT NULL,
                    open_price DECIMAL(20, 8) NOT NULL,
                    high_price DECIMAL(20, 8) NOT NULL,
                    low_price DECIMAL(20, 8) NOT NULL,
                    close_price DECIMAL(20, 8) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL,
                    volume_usdt DECIMAL(20, 8) NOT NULL,
                    is_long BOOLEAN NOT NULL,
                    is_closed BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, open_time_ms)
                )
            """)

            # Новая таблица для потоковых данных (формирующиеся свечи)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streaming_kline_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    open_time_ms BIGINT NOT NULL,
                    close_time_ms BIGINT NOT NULL,
                    open_price DECIMAL(20, 8) NOT NULL,
                    high_price DECIMAL(20, 8) NOT NULL,
                    low_price DECIMAL(20, 8) NOT NULL,
                    close_price DECIMAL(20, 8) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL,
                    volume_usdt DECIMAL(20, 8) NOT NULL,
                    is_long BOOLEAN NOT NULL,
                    is_closed BOOLEAN DEFAULT FALSE,
                    last_updated TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, open_time_ms)
                )
            """)

            # Индексы для оптимизации
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_time 
                ON kline_data(symbol, open_time_ms DESC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_closed 
                ON kline_data(symbol, is_closed, open_time_ms DESC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_streaming_kline_symbol_time 
                ON streaming_kline_data(symbol, open_time_ms DESC)
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_streaming_kline_last_updated 
                ON streaming_kline_data(last_updated DESC)
            """)

            # Таблица алертов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    alert_type VARCHAR(50) NOT NULL,
                    price DECIMAL(20, 8) NOT NULL,
                    alert_timestamp_ms BIGINT NOT NULL,
                    close_timestamp_ms BIGINT,
                    volume_ratio FLOAT,
                    consecutive_count INTEGER,
                    current_volume_usdt DECIMAL(20, 2),
                    average_volume_usdt DECIMAL(20, 2),
                    is_closed BOOLEAN DEFAULT FALSE,
                    is_true_signal BOOLEAN,
                    has_imbalance BOOLEAN DEFAULT FALSE,
                    imbalance_data JSONB,
                    candle_data JSONB,
                    order_book_snapshot JSONB,
                    message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Таблица избранного
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS favorites (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    notes TEXT,
                    color VARCHAR(7) DEFAULT '#FFD700',
                    sort_order INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Таблица настроек торговли
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS trading_settings (
                    id SERIAL PRIMARY KEY,
                    account_balance DECIMAL(20, 2) DEFAULT 10000,
                    max_risk_per_trade DECIMAL(5, 2) DEFAULT 2.0,
                    max_open_trades INTEGER DEFAULT 5,
                    default_stop_loss_percentage DECIMAL(5, 2) DEFAULT 2.0,
                    default_take_profit_percentage DECIMAL(5, 2) DEFAULT 6.0,
                    auto_calculate_quantity BOOLEAN DEFAULT TRUE,
                    api_key VARCHAR(255),
                    api_secret VARCHAR(255),
                    enable_real_trading BOOLEAN DEFAULT FALSE,
                    default_leverage INTEGER DEFAULT 1,
                    default_margin_type VARCHAR(10) DEFAULT 'isolated',
                    confirm_trades BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Таблица бумажных сделок
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    trade_type VARCHAR(10) NOT NULL,
                    entry_price DECIMAL(20, 8) NOT NULL,
                    quantity DECIMAL(20, 8) NOT NULL,
                    stop_loss DECIMAL(20, 8),
                    take_profit DECIMAL(20, 8),
                    risk_amount DECIMAL(20, 2) NOT NULL,
                    risk_percentage DECIMAL(5, 2) NOT NULL,
                    potential_profit DECIMAL(20, 2),
                    potential_loss DECIMAL(20, 2),
                    risk_reward_ratio DECIMAL(10, 2),
                    status VARCHAR(20) DEFAULT 'OPEN',
                    exit_price DECIMAL(20, 8),
                    exit_reason VARCHAR(50),
                    pnl DECIMAL(20, 2),
                    pnl_percentage DECIMAL(10, 2),
                    notes TEXT,
                    alert_id INTEGER,
                    opened_at_ms BIGINT NOT NULL,
                    closed_at_ms BIGINT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            cursor.close()
            logger.info("Все таблицы созданы/проверены")

        except Exception as e:
            logger.error(f"Ошибка создания таблиц: {e}")
            raise

    async def save_historical_kline_data(self, symbol: str, kline_data: Dict):
        """Сохранение исторических данных свечи (только закрытые свечи)"""
        try:
            cursor = self.connection.cursor()

            open_time_ms = int(kline_data['start'])
            close_time_ms = int(kline_data['end'])
            open_price = float(kline_data['open'])
            high_price = float(kline_data['high'])
            low_price = float(kline_data['low'])
            close_price = float(kline_data['close'])
            volume = float(kline_data['volume'])
            volume_usdt = volume * close_price
            is_long = close_price > open_price

            cursor.execute("""
                INSERT INTO kline_data (
                    symbol, open_time_ms, close_time_ms, open_price, high_price, 
                    low_price, close_price, volume, volume_usdt, is_long, is_closed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, open_time_ms) 
                DO UPDATE SET
                    close_time_ms = EXCLUDED.close_time_ms,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    volume_usdt = EXCLUDED.volume_usdt,
                    is_long = EXCLUDED.is_long,
                    is_closed = EXCLUDED.is_closed
            """, (symbol, open_time_ms, close_time_ms, open_price, high_price,
                  low_price, close_price, volume, volume_usdt, is_long, True))

            cursor.close()
            logger.debug(f"Сохранены исторические данные для {symbol}: {open_time_ms}")

        except Exception as e:
            logger.error(f"Ошибка сохранения исторических данных свечи для {symbol}: {e}")

    async def save_streaming_kline_data(self, symbol: str, kline_data: Dict, is_closed: bool = False):
        """Сохранение потоковых данных свечи"""
        try:
            cursor = self.connection.cursor()

            open_time_ms = int(kline_data['start'])
            close_time_ms = int(kline_data['end'])
            open_price = float(kline_data['open'])
            high_price = float(kline_data['high'])
            low_price = float(kline_data['low'])
            close_price = float(kline_data['close'])
            volume = float(kline_data['volume'])
            volume_usdt = volume * close_price
            is_long = close_price > open_price

            if is_closed:
                # Если свеча закрылась, перемещаем её в основную таблицу и удаляем из потоковой
                cursor.execute("""
                    INSERT INTO kline_data (
                        symbol, open_time_ms, close_time_ms, open_price, high_price, 
                        low_price, close_price, volume, volume_usdt, is_long, is_closed
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, open_time_ms) 
                    DO UPDATE SET
                        close_time_ms = EXCLUDED.close_time_ms,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        volume_usdt = EXCLUDED.volume_usdt,
                        is_long = EXCLUDED.is_long,
                        is_closed = EXCLUDED.is_closed
                """, (symbol, open_time_ms, close_time_ms, open_price, high_price,
                      low_price, close_price, volume, volume_usdt, is_long, True))

                # Удаляем из потоковой таблицы
                cursor.execute("""
                    DELETE FROM streaming_kline_data 
                    WHERE symbol = %s AND open_time_ms = %s
                """, (symbol, open_time_ms))

                logger.debug(f"Закрытая свеча перемещена в основную таблицу: {symbol} {open_time_ms}")
            else:
                # Обновляем потоковые данные
                cursor.execute("""
                    INSERT INTO streaming_kline_data (
                        symbol, open_time_ms, close_time_ms, open_price, high_price, 
                        low_price, close_price, volume, volume_usdt, is_long, is_closed, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (symbol, open_time_ms) 
                    DO UPDATE SET
                        close_time_ms = EXCLUDED.close_time_ms,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        volume_usdt = EXCLUDED.volume_usdt,
                        is_long = EXCLUDED.is_long,
                        last_updated = NOW()
                """, (symbol, open_time_ms, close_time_ms, open_price, high_price,
                      low_price, close_price, volume, volume_usdt, is_long, False))

                logger.debug(f"Обновлены потоковые данные для {symbol}: {open_time_ms}")

            cursor.close()

        except Exception as e:
            logger.error(f"Ошибка сохранения потоковых данных свечи для {symbol}: {e}")

    async def save_kline_data(self, symbol: str, kline_data: Dict, is_closed: bool = False):
        """Универсальный метод сохранения данных свечи"""
        if is_closed:
            await self.save_streaming_kline_data(symbol, kline_data, is_closed=True)
        else:
            await self.save_streaming_kline_data(symbol, kline_data, is_closed=False)

    async def get_recent_candles(self, symbol: str, count: int = 20) -> List[Dict]:
        """Получение последних свечей для символа (только закрытые)"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            cursor.execute("""
                SELECT 
                    open_time_ms as timestamp,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume,
                    volume_usdt,
                    is_long,
                    is_closed
                FROM kline_data 
                WHERE symbol = %s AND is_closed = TRUE
                ORDER BY open_time_ms DESC 
                LIMIT %s
            """, (symbol, count))

            rows = cursor.fetchall()
            cursor.close()

            # Преобразуем в список словарей и сортируем по времени (старые первыми)
            candles = []
            for row in reversed(rows):
                candles.append({
                    'timestamp': int(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'volume_usdt': float(row['volume_usdt']),
                    'is_long': row['is_long'],
                    'is_closed': row['is_closed']
                })

            return candles

        except Exception as e:
            logger.error(f"Ошибка получения последних свечей для {symbol}: {e}")
            return []

    async def get_streaming_candles(self, symbol: str = None) -> List[Dict]:
        """Получение текущих потоковых свечей"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            if symbol:
                cursor.execute("""
                    SELECT 
                        symbol,
                        open_time_ms as timestamp,
                        open_price as open,
                        high_price as high,
                        low_price as low,
                        close_price as close,
                        volume,
                        volume_usdt,
                        is_long,
                        is_closed,
                        last_updated
                    FROM streaming_kline_data 
                    WHERE symbol = %s
                    ORDER BY open_time_ms DESC
                """, (symbol,))
            else:
                cursor.execute("""
                    SELECT 
                        symbol,
                        open_time_ms as timestamp,
                        open_price as open,
                        high_price as high,
                        low_price as low,
                        close_price as close,
                        volume,
                        volume_usdt,
                        is_long,
                        is_closed,
                        last_updated
                    FROM streaming_kline_data 
                    ORDER BY last_updated DESC
                """)

            rows = cursor.fetchall()
            cursor.close()

            candles = []
            for row in rows:
                candles.append({
                    'symbol': row['symbol'],
                    'timestamp': int(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'volume_usdt': float(row['volume_usdt']),
                    'is_long': row['is_long'],
                    'is_closed': row['is_closed'],
                    'last_updated': row['last_updated']
                })

            return candles

        except Exception as e:
            logger.error(f"Ошибка получения потоковых свечей: {e}")
            return []

    async def check_candle_exists(self, symbol: str, timestamp_ms: int) -> bool:
        """Проверка существования свечи в основной таблице"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                SELECT 1 FROM kline_data 
                WHERE symbol = %s AND open_time_ms = %s
                LIMIT 1
            """, (symbol, timestamp_ms))

            result = cursor.fetchone()
            cursor.close()

            return result is not None

        except Exception as e:
            logger.error(f"Ошибка проверки существования свечи: {e}")
            return False

    async def get_historical_long_volumes(self, symbol: str, hours: int, offset_minutes: int = 0,
                                          volume_type: str = 'long') -> List[float]:
        """Получение исторических объемов LONG свечей"""
        try:
            cursor = self.connection.cursor()

            # Рассчитываем временные границы
            current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            end_time_ms = current_time_ms - (offset_minutes * 60 * 1000)
            start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)

            # Формируем условие в зависимости от типа объема
            volume_condition = ""
            if volume_type == 'long':
                volume_condition = "AND is_long = TRUE"
            elif volume_type == 'short':
                volume_condition = "AND is_long = FALSE"
            # Для 'all' не добавляем условие

            cursor.execute(f"""
                SELECT volume_usdt 
                FROM kline_data 
                WHERE symbol = %s 
                AND open_time_ms >= %s 
                AND open_time_ms < %s 
                AND is_closed = TRUE
                {volume_condition}
                ORDER BY open_time_ms
            """, (symbol, start_time_ms, end_time_ms))

            rows = cursor.fetchall()
            cursor.close()

            return [float(row[0]) for row in rows]

        except Exception as e:
            logger.error(f"Ошибка получения исторических объемов для {symbol}: {e}")
            return []

    async def cleanup_old_candles(self, symbol: str, retention_hours: int):
        """Очистка старых свечей для символа"""
        try:
            cursor = self.connection.cursor()

            # Рассчитываем время отсечения
            cutoff_time_ms = int((datetime.now(timezone.utc) - timedelta(hours=retention_hours)).timestamp() * 1000)

            # Очищаем основную таблицу
            cursor.execute("""
                DELETE FROM kline_data 
                WHERE symbol = %s AND open_time_ms < %s
            """, (symbol, cutoff_time_ms))

            deleted_count = cursor.rowcount

            # Очищаем старые потоковые данные (старше 1 часа)
            stream_cutoff_time_ms = int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp() * 1000)
            cursor.execute("""
                DELETE FROM streaming_kline_data 
                WHERE symbol = %s AND open_time_ms < %s
            """, (symbol, stream_cutoff_time_ms))

            deleted_stream_count = cursor.rowcount
            cursor.close()

            if deleted_count > 0 or deleted_stream_count > 0:
                logger.debug(f"Удалено {deleted_count} старых свечей и {deleted_stream_count} потоковых данных для {symbol}")

        except Exception as e:
            logger.error(f"Ошибка очистки старых свечей для {symbol}: {e}")

    async def check_data_integrity(self, symbol: str, hours: int) -> Dict:
        """Проверка целостности данных для символа"""
        try:
            cursor = self.connection.cursor()

            # Рассчитываем ожидаемое количество свечей
            expected_candles = hours * 60  # 1 свеча в минуту

            # Рассчитываем временные границы
            current_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            start_time_ms = current_time_ms - (hours * 60 * 60 * 1000)

            # Считаем существующие свечи
            cursor.execute("""
                SELECT COUNT(*) 
                FROM kline_data 
                WHERE symbol = %s 
                AND open_time_ms >= %s 
                AND is_closed = TRUE
            """, (symbol, start_time_ms))

            existing_count = cursor.fetchone()[0]
            cursor.close()

            # Рассчитываем процент целостности
            integrity_percentage = (existing_count / expected_candles * 100) if expected_candles > 0 else 0
            missing_count = max(0, expected_candles - existing_count)

            return {
                'total_expected': expected_candles,
                'total_existing': existing_count,
                'missing_count': missing_count,
                'integrity_percentage': integrity_percentage
            }

        except Exception as e:
            logger.error(f"Ошибка проверки целостности данных для {symbol}: {e}")
            return {
                'total_expected': 0,
                'total_existing': 0,
                'missing_count': 0,
                'integrity_percentage': 0
            }

    async def get_watchlist(self) -> List[str]:
        """Получение списка активных торговых пар"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT symbol FROM watchlist WHERE is_active = TRUE ORDER BY symbol")
            rows = cursor.fetchall()
            cursor.close()
            return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения watchlist: {e}")
            return []

    async def get_watchlist_details(self) -> List[Dict]:
        """Получение детальной информации о торговых парах"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT w.*, 
                       CASE WHEN f.symbol IS NOT NULL THEN TRUE ELSE FALSE END as is_favorite
                FROM watchlist w
                LEFT JOIN favorites f ON w.symbol = f.symbol
                ORDER BY w.symbol
            """)
            rows = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения детальной информации watchlist: {e}")
            return []

    async def add_to_watchlist(self, symbol: str, price_drop: float = None, current_price: float = None,
                               historical_price: float = None):
        """Добавление торговой пары в watchlist"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT INTO watchlist (symbol, price_drop_percentage, current_price, historical_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) 
                DO UPDATE SET
                    is_active = TRUE,
                    price_drop_percentage = EXCLUDED.price_drop_percentage,
                    current_price = EXCLUDED.current_price,
                    historical_price = EXCLUDED.historical_price,
                    updated_at = NOW()
            """, (symbol, price_drop, current_price, historical_price))
            cursor.close()
            logger.info(f"Добавлена пара {symbol} в watchlist")
        except Exception as e:
            logger.error(f"Ошибка добавления {symbol} в watchlist: {e}")

    async def remove_from_watchlist(self, symbol: str = None, item_id: int = None):
        """Удаление торговой пары из watchlist"""
        try:
            cursor = self.connection.cursor()
            if item_id:
                cursor.execute("DELETE FROM watchlist WHERE id = %s", (item_id,))
            elif symbol:
                cursor.execute("DELETE FROM watchlist WHERE symbol = %s", (symbol,))
            cursor.close()
            logger.info(f"Удалена пара из watchlist: {symbol or item_id}")
        except Exception as e:
            logger.error(f"Ошибка удаления из watchlist: {e}")

    async def update_watchlist_item(self, item_id: int, symbol: str, is_active: bool):
        """Обновление элемента watchlist"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                UPDATE watchlist 
                SET symbol = %s, is_active = %s, updated_at = NOW()
                WHERE id = %s
            """, (symbol, is_active, item_id))
            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка обновления watchlist: {e}")

    async def save_alert(self, alert_data: Dict) -> int:
        """Сохранение алерта в базу данных"""
        try:
            cursor = self.connection.cursor()

            cursor.execute("""
                INSERT INTO alerts (
                    symbol, alert_type, price, alert_timestamp_ms, close_timestamp_ms,
                    volume_ratio, consecutive_count, current_volume_usdt, average_volume_usdt,
                    is_closed, is_true_signal, has_imbalance, imbalance_data, 
                    candle_data, order_book_snapshot, message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                alert_data['symbol'],
                alert_data['alert_type'],
                alert_data['price'],
                alert_data['timestamp'],
                alert_data.get('close_timestamp'),
                alert_data.get('volume_ratio'),
                alert_data.get('consecutive_count'),
                alert_data.get('current_volume_usdt'),
                alert_data.get('average_volume_usdt'),
                alert_data.get('is_closed', False),
                alert_data.get('is_true_signal'),
                alert_data.get('has_imbalance', False),
                json.dumps(alert_data.get('imbalance_data')) if alert_data.get('imbalance_data') else None,
                json.dumps(alert_data.get('candle_data')) if alert_data.get('candle_data') else None,
                json.dumps(alert_data.get('order_book_snapshot')) if alert_data.get('order_book_snapshot') else None,
                alert_data.get('message')
            ))

            alert_id = cursor.fetchone()[0]
            cursor.close()
            return alert_id

        except Exception as e:
            logger.error(f"Ошибка сохранения алерта: {e}")
            return None

    def _parse_json_field(self, field_value):
        """Безопасное парсинг JSON поля"""
        if field_value is None:
            return None
        
        # Если это уже словарь/список Python, возвращаем как есть
        if isinstance(field_value, (dict, list)):
            return field_value
        
        # Если это строка, пытаемся распарсить JSON
        if isinstance(field_value, str):
            try:
                return json.loads(field_value)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Не удалось распарсить JSON: {field_value}")
                return None
        
        return None

    async def get_all_alerts(self, limit: int = 1000) -> Dict:
        """Получение всех алертов"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            # Получаем алерты по объему
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE alert_type = 'volume_spike'
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (limit,))
            volume_alerts = [dict(row) for row in cursor.fetchall()]

            # Получаем алерты по последовательности
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE alert_type = 'consecutive_long'
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (limit,))
            consecutive_alerts = [dict(row) for row in cursor.fetchall()]

            # Получаем приоритетные алерты
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE alert_type = 'priority'
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (limit,))
            priority_alerts = [dict(row) for row in cursor.fetchall()]

            cursor.close()

            # Безопасно преобразуем JSON поля
            for alerts_list in [volume_alerts, consecutive_alerts, priority_alerts]:
                for alert in alerts_list:
                    alert['imbalance_data'] = self._parse_json_field(alert.get('imbalance_data'))
                    alert['candle_data'] = self._parse_json_field(alert.get('candle_data'))
                    alert['order_book_snapshot'] = self._parse_json_field(alert.get('order_book_snapshot'))

            return {
                'volume_alerts': volume_alerts,
                'consecutive_alerts': consecutive_alerts,
                'priority_alerts': priority_alerts
            }

        except Exception as e:
            logger.error(f"Ошибка получения алертов: {e}")
            return {
                'volume_alerts': [],
                'consecutive_alerts': [],
                'priority_alerts': []
            }

    async def get_recent_volume_alerts(self, symbol: str, minutes_back: int) -> List[Dict]:
        """Получение недавних алертов по объему для символа"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            cutoff_time_ms = int((datetime.now(timezone.utc) - timedelta(minutes=minutes_back)).timestamp() * 1000)

            cursor.execute("""
                SELECT * FROM alerts 
                WHERE symbol = %s 
                AND alert_type = 'volume_spike'
                AND alert_timestamp_ms > %s
                ORDER BY alert_timestamp_ms DESC
            """, (symbol, cutoff_time_ms))

            rows = cursor.fetchall()
            cursor.close()

            alerts = []
            for row in rows:
                alert = dict(row)
                alert['imbalance_data'] = self._parse_json_field(alert.get('imbalance_data'))
                alert['candle_data'] = self._parse_json_field(alert.get('candle_data'))
                alert['order_book_snapshot'] = self._parse_json_field(alert.get('order_book_snapshot'))
                alerts.append(alert)

            return alerts

        except Exception as e:
            logger.error(f"Ошибка получения недавних алертов по объему для {symbol}: {e}")
            return []

    async def get_chart_data(self, symbol: str, hours: int = 1, alert_time: str = None) -> List[Dict]:
        """Получение данных для графика"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            # Определяем временные границы
            if alert_time:
                try:
                    center_time_ms = int(alert_time)
                except:
                    center_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            else:
                center_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            # Берем данные вокруг времени алерта
            half_period_ms = (hours * 60 * 60 * 1000) // 2
            start_time_ms = center_time_ms - half_period_ms
            end_time_ms = center_time_ms + half_period_ms

            cursor.execute("""
                SELECT 
                    open_time_ms as timestamp,
                    open_price as open,
                    high_price as high,
                    low_price as low,
                    close_price as close,
                    volume,
                    volume_usdt,
                    is_long
                FROM kline_data 
                WHERE symbol = %s 
                AND open_time_ms >= %s 
                AND open_time_ms <= %s
                AND is_closed = TRUE
                ORDER BY open_time_ms
            """, (symbol, start_time_ms, end_time_ms))

            rows = cursor.fetchall()
            cursor.close()

            chart_data = []
            for row in rows:
                chart_data.append({
                    'timestamp': int(row['timestamp']),
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'volume_usdt': float(row['volume_usdt']),
                    'is_long': row['is_long']
                })

            return chart_data

        except Exception as e:
            logger.error(f"Ошибка получения данных графика для {symbol}: {e}")
            return []

    async def cleanup_old_data(self, retention_hours: int):
        """Очистка старых данных"""
        try:
            cursor = self.connection.cursor()

            # Очистка старых свечных данных
            cutoff_time_ms = int((datetime.now(timezone.utc) - timedelta(hours=retention_hours)).timestamp() * 1000)

            cursor.execute("DELETE FROM kline_data WHERE open_time_ms < %s", (cutoff_time_ms,))
            deleted_candles = cursor.rowcount

            # Очистка старых потоковых данных (старше 2 часов)
            stream_cutoff_time_ms = int((datetime.now(timezone.utc) - timedelta(hours=2)).timestamp() * 1000)
            cursor.execute("DELETE FROM streaming_kline_data WHERE open_time_ms < %s", (stream_cutoff_time_ms,))
            deleted_stream = cursor.rowcount

            # Очистка старых алертов (старше 7 дней)
            alert_cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp() * 1000)
            cursor.execute("DELETE FROM alerts WHERE alert_timestamp_ms < %s", (alert_cutoff_ms,))
            deleted_alerts = cursor.rowcount

            cursor.close()

            logger.info(f"Очищено {deleted_candles} старых свечей, {deleted_stream} потоковых данных и {deleted_alerts} старых алертов")

        except Exception as e:
            logger.error(f"Ошибка очистки старых данных: {e}")

    # Методы для работы с избранным
    async def get_favorites(self) -> List[Dict]:
        """Получение списка избранных пар"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT f.*, w.is_active, w.price_drop_percentage, w.current_price, w.historical_price,
                       f.created_at as favorite_added_at
                FROM favorites f
                LEFT JOIN watchlist w ON f.symbol = w.symbol
                ORDER BY f.sort_order, f.created_at
            """)
            rows = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения избранного: {e}")
            return []

    async def add_to_favorites(self, symbol: str, notes: str = None, color: str = '#FFD700'):
        """Добавление в избранное"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("""
                INSERT INTO favorites (symbol, notes, color)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO NOTHING
            """, (symbol, notes, color))
            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка добавления в избранное: {e}")

    async def remove_from_favorites(self, symbol: str):
        """Удаление из избранного"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("DELETE FROM favorites WHERE symbol = %s", (symbol,))
            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка удаления из избранного: {e}")

    async def update_favorite(self, symbol: str, notes: str = None, color: str = None, sort_order: int = None):
        """Обновление избранной пары"""
        try:
            cursor = self.connection.cursor()

            updates = []
            params = []

            if notes is not None:
                updates.append("notes = %s")
                params.append(notes)
            if color is not None:
                updates.append("color = %s")
                params.append(color)
            if sort_order is not None:
                updates.append("sort_order = %s")
                params.append(sort_order)

            if updates:
                updates.append("updated_at = NOW()")
                params.append(symbol)

                query = f"UPDATE favorites SET {', '.join(updates)} WHERE symbol = %s"
                cursor.execute(query, params)

            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка обновления избранного: {e}")

    async def reorder_favorites(self, symbol_order: List[str]):
        """Изменение порядка избранных пар"""
        try:
            cursor = self.connection.cursor()

            for index, symbol in enumerate(symbol_order):
                cursor.execute("""
                    UPDATE favorites 
                    SET sort_order = %s, updated_at = NOW()
                    WHERE symbol = %s
                """, (index, symbol))

            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка изменения порядка избранного: {e}")

    # Методы для торговли
    async def get_trading_settings(self) -> Dict:
        """Получение настроек торговли"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("SELECT * FROM trading_settings ORDER BY id DESC LIMIT 1")
            row = cursor.fetchone()
            cursor.close()

            if row:
                return dict(row)
            else:
                # Создаем настройки по умолчанию
                await self.update_trading_settings({})
                return await self.get_trading_settings()

        except Exception as e:
            logger.error(f"Ошибка получения настроек торговли: {e}")
            return {}

    async def update_trading_settings(self, settings: Dict):
        """Обновление настроек торговли"""
        try:
            cursor = self.connection.cursor()

            # Проверяем, есть ли уже настройки
            cursor.execute("SELECT id FROM trading_settings LIMIT 1")
            existing = cursor.fetchone()

            if existing:
                # Обновляем существующие настройки
                updates = []
                params = []

                for key, value in settings.items():
                    if key in ['account_balance', 'max_risk_per_trade', 'max_open_trades',
                               'default_stop_loss_percentage', 'default_take_profit_percentage',
                               'auto_calculate_quantity', 'api_key', 'api_secret', 'enable_real_trading',
                               'default_leverage', 'default_margin_type', 'confirm_trades']:
                        updates.append(f"{key} = %s")
                        params.append(value)

                if updates:
                    updates.append("updated_at = NOW()")
                    query = f"UPDATE trading_settings SET {', '.join(updates)} WHERE id = %s"
                    params.append(existing[0])
                    cursor.execute(query, params)
            else:
                # Создаем новые настройки
                cursor.execute("""
                    INSERT INTO trading_settings (
                        account_balance, max_risk_per_trade, max_open_trades,
                        default_stop_loss_percentage, default_take_profit_percentage,
                        auto_calculate_quantity
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    settings.get('account_balance', 10000),
                    settings.get('max_risk_per_trade', 2.0),
                    settings.get('max_open_trades', 5),
                    settings.get('default_stop_loss_percentage', 2.0),
                    settings.get('default_take_profit_percentage', 6.0),
                    settings.get('auto_calculate_quantity', True)
                ))

            cursor.close()
        except Exception as e:
            logger.error(f"Ошибка обновления настроек торговли: {e}")

    async def create_paper_trade(self, trade_data: Dict) -> int:
        """Создание бумажной сделки"""
        try:
            cursor = self.connection.cursor()

            opened_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            cursor.execute("""
                INSERT INTO paper_trades (
                    symbol, trade_type, entry_price, quantity, stop_loss, take_profit,
                    risk_amount, risk_percentage, potential_profit, potential_loss,
                    risk_reward_ratio, notes, alert_id, opened_at_ms
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                trade_data['symbol'],
                trade_data['trade_type'],
                trade_data['entry_price'],
                trade_data['quantity'],
                trade_data.get('stop_loss'),
                trade_data.get('take_profit'),
                trade_data['risk_amount'],
                trade_data['risk_percentage'],
                trade_data.get('potential_profit'),
                trade_data.get('potential_loss'),
                trade_data.get('risk_reward_ratio'),
                trade_data.get('notes'),
                trade_data.get('alert_id'),
                opened_at_ms
            ))

            trade_id = cursor.fetchone()[0]
            cursor.close()
            return trade_id

        except Exception as e:
            logger.error(f"Ошибка создания бумажной сделки: {e}")
            return None

    async def get_paper_trades(self, status: str = None, limit: int = 100) -> List[Dict]:
        """Получение бумажных сделок"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            if status:
                cursor.execute("""
                    SELECT * FROM paper_trades 
                    WHERE status = %s
                    ORDER BY opened_at_ms DESC 
                    LIMIT %s
                """, (status, limit))
            else:
                cursor.execute("""
                    SELECT * FROM paper_trades 
                    ORDER BY opened_at_ms DESC 
                    LIMIT %s
                """, (limit,))

            rows = cursor.fetchall()
            cursor.close()
            return [dict(row) for row in rows]

        except Exception as e:
            logger.error(f"Ошибка получения бумажных сделок: {e}")
            return []

    async def close_paper_trade(self, trade_id: int, exit_price: float, exit_reason: str = 'MANUAL') -> bool:
        """Закрытие бумажной сделки"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            # Получаем данные сделки
            cursor.execute("SELECT * FROM paper_trades WHERE id = %s AND status = 'OPEN'", (trade_id,))
            trade = cursor.fetchone()

            if not trade:
                return False

            # Рассчитываем P&L
            entry_price = float(trade['entry_price'])
            quantity = float(trade['quantity'])
            trade_type = trade['trade_type']

            if trade_type == 'LONG':
                pnl = (exit_price - entry_price) * quantity
            else:  # SHORT
                pnl = (entry_price - exit_price) * quantity

            position_value = entry_price * quantity
            pnl_percentage = (pnl / position_value) * 100 if position_value > 0 else 0

            closed_at_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

            # Обновляем сделку
            cursor.execute("""
                UPDATE paper_trades 
                SET status = 'CLOSED', exit_price = %s, exit_reason = %s, 
                    pnl = %s, pnl_percentage = %s, closed_at_ms = %s, updated_at = NOW()
                WHERE id = %s
            """, (exit_price, exit_reason, pnl, pnl_percentage, closed_at_ms, trade_id))

            cursor.close()
            return True

        except Exception as e:
            logger.error(f"Ошибка закрытия бумажной сделки: {e}")
            return False

    async def get_trading_statistics(self) -> Dict:
        """Получение статистики торговли"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)

            # Общая статистика
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(CASE WHEN status = 'OPEN' THEN 1 END) as open_trades,
                    COUNT(CASE WHEN status = 'CLOSED' THEN 1 END) as closed_trades,
                    COUNT(CASE WHEN status = 'CLOSED' AND pnl > 0 THEN 1 END) as winning_trades,
                    COUNT(CASE WHEN status = 'CLOSED' AND pnl < 0 THEN 1 END) as losing_trades,
                    COALESCE(SUM(CASE WHEN status = 'CLOSED' THEN pnl END), 0) as total_pnl,
                    COALESCE(AVG(CASE WHEN status = 'CLOSED' THEN pnl_percentage END), 0) as avg_pnl_percentage,
                    COALESCE(MAX(CASE WHEN status = 'CLOSED' THEN pnl END), 0) as max_profit,
                    COALESCE(MIN(CASE WHEN status = 'CLOSED' THEN pnl END), 0) as max_loss
                FROM paper_trades
            """)

            stats = dict(cursor.fetchone())
            cursor.close()

            # Рассчитываем винрейт
            closed_trades = stats['closed_trades']
            winning_trades = stats['winning_trades']
            stats['win_rate'] = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0

            return stats

        except Exception as e:
            logger.error(f"Ошибка получения статистики торговли: {e}")
            return {}

    async def get_alerts_by_type(self, alert_type: str, limit: int = 50) -> List[Dict]:
        """Получение алертов по типу"""
        try:
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE alert_type = %s
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (alert_type, limit))
            rows = cursor.fetchall()
            cursor.close()

            alerts = []
            for row in rows:
                alert = dict(row)
                # Безопасно преобразуем JSON поля
                alert['imbalance_data'] = self._parse_json_field(alert.get('imbalance_data'))
                alert['candle_data'] = self._parse_json_field(alert.get('candle_data'))
                alert['order_book_snapshot'] = self._parse_json_field(alert.get('order_book_snapshot'))
                alerts.append(alert)

            return alerts

        except Exception as e:
            logger.error(f"Ошибка получения алертов по типу {alert_type}: {e}")
            return []

    async def clear_alerts(self, alert_type: str):
        """Очистка алертов по типу"""
        try:
            cursor = self.connection.cursor()
            cursor.execute("DELETE FROM alerts WHERE alert_type = %s", (alert_type,))
            deleted_count = cursor.rowcount
            cursor.close()
            logger.info(f"Удалено {deleted_count} алертов типа {alert_type}")
        except Exception as e:
            logger.error(f"Ошибка очистки алертов типа {alert_type}: {e}")

    def close(self):
        """Закрытие соединения с базой данных"""
        if self.connection:
            self.connection.close()
            logger.info("Соединение с базой данных закрыто")