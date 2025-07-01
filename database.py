import asyncio
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, timezone
import json

logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self):
        self.connection = None
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'database': os.getenv('DB_NAME', 'trading_analyzer'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'password')
        }

    async def initialize(self):
        """Инициализация подключения к базе данных"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = True
            await self.create_tables()
            await self.migrate_database()  # Добавляем миграции
            logger.info("✅ База данных инициализирована")
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise

    async def migrate_database(self):
        """Выполнение миграций базы данных"""
        cursor = self.connection.cursor()
        try:
            logger.info("🔄 Проверка и выполнение миграций базы данных...")
            
            # Проверяем и добавляем недостающие колонки в watchlist
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'watchlist' AND column_name = 'added_at'
            """)
            
            if not cursor.fetchone():
                logger.info("🔄 Добавление колонки added_at в таблицу watchlist")
                cursor.execute("""
                    ALTER TABLE watchlist 
                    ADD COLUMN added_at TIMESTAMPTZ DEFAULT NOW()
                """)

            # Проверяем и добавляем updated_at если её нет
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'watchlist' AND column_name = 'updated_at'
            """)
            
            if not cursor.fetchone():
                logger.info("🔄 Добавление колонки updated_at в таблицу watchlist")
                cursor.execute("""
                    ALTER TABLE watchlist 
                    ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW()
                """)

            # Обновляем существующие записи, у которых нет времени
            cursor.execute("""
                UPDATE watchlist 
                SET added_at = COALESCE(added_at, NOW()), 
                    updated_at = COALESCE(updated_at, NOW()) 
                WHERE added_at IS NULL OR updated_at IS NULL
            """)

            logger.info("✅ Миграции базы данных выполнены успешно")

        except Exception as e:
            logger.error(f"❌ Ошибка выполнения миграций: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def create_tables(self):
        """Создание необходимых таблиц"""
        cursor = self.connection.cursor()
        
        try:
            # Таблица торговых пар
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS watchlist (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) UNIQUE NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    price_drop_percentage FLOAT,
                    current_price FLOAT,
                    historical_price FLOAT,
                    added_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Таблица свечных данных
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kline_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    open_price DECIMAL(20, 8) NOT NULL,
                    high_price DECIMAL(20, 8) NOT NULL,
                    low_price DECIMAL(20, 8) NOT NULL,
                    close_price DECIMAL(20, 8) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL,
                    is_closed BOOLEAN DEFAULT FALSE,
                    is_long BOOLEAN,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, timestamp_ms)
                )
            """)

            # Индексы для оптимизации
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_timestamp 
                ON kline_data(symbol, timestamp_ms DESC)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kline_symbol_closed 
                ON kline_data(symbol, is_closed, timestamp_ms DESC)
            """)

            # Таблица потоковых данных
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS streaming_data (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timestamp_ms BIGINT NOT NULL,
                    open_price DECIMAL(20, 8) NOT NULL,
                    high_price DECIMAL(20, 8) NOT NULL,
                    low_price DECIMAL(20, 8) NOT NULL,
                    close_price DECIMAL(20, 8) NOT NULL,
                    volume DECIMAL(20, 8) NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, timestamp_ms)
                )
            """)

            # Таблица алертов
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    alert_type VARCHAR(50) NOT NULL,
                    price DECIMAL(20, 8) NOT NULL,
                    volume_ratio FLOAT,
                    current_volume_usdt BIGINT,
                    average_volume_usdt BIGINT,
                    consecutive_count INTEGER,
                    alert_timestamp_ms BIGINT NOT NULL,
                    close_timestamp_ms BIGINT,
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
                    default_take_profit_percentage DECIMAL(5, 2) DEFAULT 4.0,
                    auto_calculate_quantity BOOLEAN DEFAULT TRUE,
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
                    exit_price DECIMAL(20, 8),
                    quantity DECIMAL(20, 8) NOT NULL,
                    stop_loss DECIMAL(20, 8),
                    take_profit DECIMAL(20, 8),
                    risk_amount DECIMAL(20, 2),
                    risk_percentage DECIMAL(5, 2),
                    potential_profit DECIMAL(20, 2),
                    potential_loss DECIMAL(20, 2),
                    risk_reward_ratio DECIMAL(10, 2),
                    actual_profit_loss DECIMAL(20, 2),
                    status VARCHAR(20) DEFAULT 'OPEN',
                    exit_reason VARCHAR(50),
                    notes TEXT,
                    alert_id INTEGER,
                    entry_time TIMESTAMPTZ DEFAULT NOW(),
                    exit_time TIMESTAMPTZ
                )
            """)

            # Вставляем настройки по умолчанию, если их нет
            cursor.execute("""
                INSERT INTO trading_settings (id) 
                SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM trading_settings WHERE id = 1)
            """)

            logger.info("✅ Таблицы созданы успешно")

        except Exception as e:
            logger.error(f"❌ Ошибка создания таблиц: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    # Методы для работы с watchlist
    async def get_watchlist(self) -> List[str]:
        """Получить список активных торговых пар"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("SELECT symbol FROM watchlist WHERE is_active = TRUE ORDER BY symbol")
            return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения watchlist: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def get_watchlist_details(self) -> List[Dict]:
        """Получить детальную информацию о торговых парах"""
        cursor = self.connection.cursor()
        try:
            logger.debug("🔍 Получение деталей watchlist...")
            
            # Сначала проверяем существование таблицы (используем обычный cursor)
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'watchlist'
                )
            """)
            
            result = cursor.fetchone()
            table_exists = result[0] if result else False
            
            if not table_exists:
                logger.warning("⚠️ Таблица watchlist не существует")
                return []
            
            # Проверяем существование колонок перед запросом
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'watchlist'
                ORDER BY column_name
            """)
            columns = [row[0] for row in cursor.fetchall()]
            logger.debug(f"🔍 Найденные колонки в watchlist: {columns}")
            
            if not columns:
                logger.warning("⚠️ Не найдено колонок в таблице watchlist")
                return []
            
            # Формируем запрос только с существующими колонками
            required_columns = ["id", "symbol", "is_active"]
            optional_columns = ["price_drop_percentage", "current_price", "historical_price", "added_at", "updated_at"]
            
            # Проверяем наличие обязательных колонок
            missing_required = [col for col in required_columns if col not in columns]
            if missing_required:
                logger.error(f"❌ Отсутствуют обязательные колонки: {missing_required}")
                return []
            
            # Собираем все доступные колонки
            available_columns = required_columns.copy()
            for col in optional_columns:
                if col in columns:
                    available_columns.append(col)
            
            query_columns = ", ".join(available_columns)
            logger.debug(f"🔍 Запрос с колонками: {query_columns}")
            
            # Закрываем обычный cursor и открываем RealDictCursor для основного запроса
            cursor.close()
            cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute(f"""
                SELECT {query_columns}
                FROM watchlist 
                ORDER BY symbol
            """)
            
            results = cursor.fetchall()
            logger.debug(f"🔍 Получено {len(results)} записей из watchlist")
            
            return [dict(row) for row in results]
            
        except psycopg2.Error as e:
            logger.error(f"❌ Ошибка PostgreSQL при получении деталей watchlist: {e.pgcode} - {e.pgerror}")
            return []
        except Exception as e:
            logger.error(f"❌ Ошибка получения деталей watchlist: {type(e).__name__}: {str(e)}")
            import traceback
            logger.error(f"❌ Полная трассировка: {traceback.format_exc()}")
            return []
        finally:
            if cursor:
                cursor.close()

    async def add_to_watchlist(self, symbol: str, price_drop: float = None, 
                              current_price: float = None, historical_price: float = None):
        """Добавить торговую пару в watchlist"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO watchlist (symbol, price_drop_percentage, current_price, historical_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    is_active = TRUE,
                    price_drop_percentage = EXCLUDED.price_drop_percentage,
                    current_price = EXCLUDED.current_price,
                    historical_price = EXCLUDED.historical_price,
                    updated_at = NOW()
            """, (symbol, price_drop, current_price, historical_price))
            logger.info(f"✅ Добавлена пара {symbol} в watchlist")
        except Exception as e:
            logger.error(f"❌ Ошибка добавления {symbol} в watchlist: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def remove_from_watchlist(self, symbol: str = None, item_id: int = None):
        """Удалить торговую пару из watchlist"""
        cursor = self.connection.cursor()
        try:
            if item_id:
                cursor.execute("DELETE FROM watchlist WHERE id = %s", (item_id,))
            elif symbol:
                cursor.execute("DELETE FROM watchlist WHERE symbol = %s", (symbol,))
            logger.info(f"✅ Удалена пара из watchlist")
        except Exception as e:
            logger.error(f"❌ Ошибка удаления из watchlist: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def update_watchlist_item(self, item_id: int, symbol: str, is_active: bool):
        """Обновить элемент watchlist"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                UPDATE watchlist 
                SET symbol = %s, is_active = %s, updated_at = NOW()
                WHERE id = %s
            """, (symbol, is_active, item_id))
        except Exception as e:
            logger.error(f"❌ Ошибка обновления watchlist: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    # Методы для работы с kline данными
    async def save_kline_data(self, symbol: str, kline_data: Dict, is_closed: bool = False):
        """Сохранить данные свечи"""
        cursor = self.connection.cursor()
        try:
            timestamp_ms = int(kline_data['start'])
            open_price = float(kline_data['open'])
            high_price = float(kline_data['high'])
            low_price = float(kline_data['low'])
            close_price = float(kline_data['close'])
            volume = float(kline_data['volume'])
            is_long = close_price > open_price

            if is_closed:
                # Для закрытых свечей сохраняем в основную таблицу
                cursor.execute("""
                    INSERT INTO kline_data (symbol, timestamp_ms, open_price, high_price, 
                                          low_price, close_price, volume, is_closed, is_long)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp_ms) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        is_closed = EXCLUDED.is_closed,
                        is_long = EXCLUDED.is_long
                """, (symbol, timestamp_ms, open_price, high_price, low_price, 
                     close_price, volume, is_closed, is_long))
            else:
                # Для потоковых данных сохраняем в отдельную таблицу
                cursor.execute("""
                    INSERT INTO streaming_data (symbol, timestamp_ms, open_price, high_price,
                                              low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp_ms) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        updated_at = NOW()
                """, (symbol, timestamp_ms, open_price, high_price, low_price, close_price, volume))

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения kline данных для {symbol}: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def save_historical_kline_data(self, symbol: str, kline_data: Dict):
        """Сохранить исторические данные свечи"""
        await self.save_kline_data(symbol, kline_data, is_closed=True)

    async def check_candle_exists(self, symbol: str, timestamp_ms: int) -> bool:
        """Проверить существование свечи"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT 1 FROM kline_data 
                WHERE symbol = %s AND timestamp_ms = %s
            """, (symbol, timestamp_ms))
            return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ Ошибка проверки существования свечи: {type(e).__name__}: {str(e)}")
            return False
        finally:
            cursor.close()

    async def get_recent_candles(self, symbol: str, count: int = 20) -> List[Dict]:
        """Получить последние свечи для символа"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT timestamp_ms as timestamp, open_price as open, high_price as high,
                       low_price as low, close_price as close, volume, is_long, is_closed
                FROM kline_data 
                WHERE symbol = %s AND is_closed = TRUE
                ORDER BY timestamp_ms DESC 
                LIMIT %s
            """, (symbol, count))
            
            candles = []
            for row in cursor.fetchall():
                candles.append({
                    'timestamp': row['timestamp'],
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'is_long': row['is_long'],
                    'is_closed': row['is_closed']
                })
            
            # Возвращаем в хронологическом порядке
            return list(reversed(candles))
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения последних свечей для {symbol}: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def get_streaming_candles(self, symbol: str = None) -> List[Dict]:
        """Получить потоковые данные"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            if symbol:
                cursor.execute("""
                    SELECT symbol, timestamp_ms, open_price, high_price, low_price, 
                           close_price, volume, updated_at
                    FROM streaming_data 
                    WHERE symbol = %s
                    ORDER BY timestamp_ms DESC
                """, (symbol,))
            else:
                cursor.execute("""
                    SELECT symbol, timestamp_ms, open_price, high_price, low_price, 
                           close_price, volume, updated_at
                    FROM streaming_data 
                    ORDER BY symbol, timestamp_ms DESC
                """)
            
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения потоковых данных: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def check_data_integrity(self, symbol: str, hours: int) -> Dict:
        """Проверить целостность данных за указанный период"""
        cursor = self.connection.cursor()
        try:
            # Определяем временной диапазон
            end_time_ms = int(datetime.utcnow().timestamp() * 1000)
            start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)
            
            # Ожидаемое количество свечей
            expected_count = hours * 60
            
            # Фактическое количество свечей
            cursor.execute("""
                SELECT COUNT(*) FROM kline_data 
                WHERE symbol = %s AND timestamp_ms >= %s AND timestamp_ms < %s
                AND is_closed = TRUE
            """, (symbol, start_time_ms, end_time_ms))
            
            result = cursor.fetchone()
            actual_count = result[0] if result else 0
            
            # Рассчитываем процент целостности
            integrity_percentage = (actual_count / expected_count * 100) if expected_count > 0 else 0
            missing_count = max(0, expected_count - actual_count)
            
            return {
                'total_expected': expected_count,
                'total_existing': actual_count,
                'missing_count': missing_count,
                'integrity_percentage': integrity_percentage
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки целостности данных для {symbol}: {type(e).__name__}: {str(e)}")
            return {
                'total_expected': 0,
                'total_existing': 0,
                'missing_count': 0,
                'integrity_percentage': 0
            }
        finally:
            cursor.close()

    async def check_data_integrity_range(self, symbol: str, start_time_ms: int, end_time_ms: int) -> Dict:
        """Проверить целостность данных в указанном диапазоне"""
        cursor = self.connection.cursor()
        try:
            # Ожидаемое количество свечей (в минутах)
            expected_count = (end_time_ms - start_time_ms) // 60000
            
            # Фактическое количество свечей
            cursor.execute("""
                SELECT COUNT(*) FROM kline_data 
                WHERE symbol = %s AND timestamp_ms >= %s AND timestamp_ms < %s
                AND is_closed = TRUE
            """, (symbol, start_time_ms, end_time_ms))
            
            result = cursor.fetchone()
            actual_count = result[0] if result else 0
            
            # Рассчитываем процент целостности
            integrity_percentage = (actual_count / expected_count * 100) if expected_count > 0 else 0
            missing_count = max(0, expected_count - actual_count)
            
            return {
                'total_expected': expected_count,
                'total_existing': actual_count,
                'missing_count': missing_count,
                'integrity_percentage': integrity_percentage
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка проверки целостности данных в диапазоне для {symbol}: {type(e).__name__}: {str(e)}")
            return {
                'total_expected': 0,
                'total_existing': 0,
                'missing_count': 0,
                'integrity_percentage': 0
            }
        finally:
            cursor.close()

    async def cleanup_old_candles(self, symbol: str, hours: int):
        """Очистить старые свечи"""
        cursor = self.connection.cursor()
        try:
            cutoff_time_ms = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)
            
            cursor.execute("""
                DELETE FROM kline_data 
                WHERE symbol = %s AND timestamp_ms < %s
            """, (symbol, cutoff_time_ms))
            
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                logger.debug(f"🧹 Удалено {deleted_count} старых свечей для {symbol}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка очистки старых свечей для {symbol}: {type(e).__name__}: {str(e)}")
        finally:
            cursor.close()

    async def cleanup_old_candles_before_time(self, symbol: str, before_time_ms: int) -> int:
        """Удалить свечи ДО указанного времени"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                DELETE FROM kline_data 
                WHERE symbol = %s AND timestamp_ms < %s
            """, (symbol, before_time_ms))
            
            deleted_count = cursor.rowcount
            return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка удаления старых свечей для {symbol}: {type(e).__name__}: {str(e)}")
            return 0
        finally:
            cursor.close()

    async def cleanup_future_candles_after_time(self, symbol: str, after_time_ms: int) -> int:
        """Удалить свечи ПОСЛЕ указанного времени"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                DELETE FROM kline_data 
                WHERE symbol = %s AND timestamp_ms >= %s
            """, (symbol, after_time_ms))
            
            deleted_count = cursor.rowcount
            return deleted_count
                
        except Exception as e:
            logger.error(f"❌ Ошибка удаления будущих свечей для {symbol}: {type(e).__name__}: {str(e)}")
            return 0
        finally:
            cursor.close()

    async def cleanup_old_data(self, hours: int):
        """Общая очистка старых данных"""
        cursor = self.connection.cursor()
        try:
            cutoff_time_ms = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)
            
            # Очищаем старые kline данные
            cursor.execute("DELETE FROM kline_data WHERE timestamp_ms < %s", (cutoff_time_ms,))
            kline_deleted = cursor.rowcount
            
            # Очищаем старые потоковые данные
            cursor.execute("DELETE FROM streaming_data WHERE timestamp_ms < %s", (cutoff_time_ms,))
            streaming_deleted = cursor.rowcount
            
            # Очищаем старые алерты (старше 7 дней)
            week_ago_ms = int((datetime.utcnow() - timedelta(days=7)).timestamp() * 1000)
            cursor.execute("DELETE FROM alerts WHERE alert_timestamp_ms < %s", (week_ago_ms,))
            alerts_deleted = cursor.rowcount
            
            logger.info(f"🧹 Очистка: kline={kline_deleted}, streaming={streaming_deleted}, alerts={alerts_deleted}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка общей очистки данных: {type(e).__name__}: {str(e)}")
        finally:
            cursor.close()

    # Методы для работы с объемами
    async def get_historical_long_volumes(self, symbol: str, hours: int, 
                                        offset_minutes: int = 0, volume_type: str = 'long') -> List[float]:
        """Получить исторические объемы LONG свечей"""
        cursor = self.connection.cursor()
        try:
            end_time_ms = int(datetime.utcnow().timestamp() * 1000) - (offset_minutes * 60 * 1000)
            start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)
            
            if volume_type == 'long':
                cursor.execute("""
                    SELECT volume * close_price as volume_usdt
                    FROM kline_data 
                    WHERE symbol = %s 
                    AND timestamp_ms >= %s AND timestamp_ms < %s
                    AND is_closed = TRUE AND is_long = TRUE
                    ORDER BY timestamp_ms
                """, (symbol, start_time_ms, end_time_ms))
            else:
                cursor.execute("""
                    SELECT volume * close_price as volume_usdt
                    FROM kline_data 
                    WHERE symbol = %s 
                    AND timestamp_ms >= %s AND timestamp_ms < %s
                    AND is_closed = TRUE
                    ORDER BY timestamp_ms
                """, (symbol, start_time_ms, end_time_ms))
            
            return [float(row[0]) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения исторических объемов для {symbol}: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    # Методы для работы с алертами
    async def save_alert(self, alert_data: Dict) -> int:
        """Сохранить алерт"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO alerts (
                    symbol, alert_type, price, volume_ratio, current_volume_usdt,
                    average_volume_usdt, consecutive_count, alert_timestamp_ms,
                    close_timestamp_ms, is_closed, is_true_signal, has_imbalance,
                    imbalance_data, candle_data, order_book_snapshot, message
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                alert_data['symbol'],
                alert_data['alert_type'],
                alert_data['price'],
                alert_data.get('volume_ratio'),
                alert_data.get('current_volume_usdt'),
                alert_data.get('average_volume_usdt'),
                alert_data.get('consecutive_count'),
                alert_data['timestamp'],
                alert_data.get('close_timestamp'),
                alert_data.get('is_closed', False),
                alert_data.get('is_true_signal'),
                alert_data.get('has_imbalance', False),
                json.dumps(alert_data.get('imbalance_data')) if alert_data.get('imbalance_data') else None,
                json.dumps(alert_data.get('candle_data')) if alert_data.get('candle_data') else None,
                json.dumps(alert_data.get('order_book_snapshot')) if alert_data.get('order_book_snapshot') else None,
                alert_data.get('message')
            ))
            
            result = cursor.fetchone()
            alert_id = result[0] if result else None
            return alert_id
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения алерта: {type(e).__name__}: {str(e)}")
            return None
        finally:
            cursor.close()

    async def get_all_alerts(self, limit: int = 100) -> Dict:
        """Получить все алерты"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT * FROM alerts 
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (limit,))
            
            all_alerts = [dict(row) for row in cursor.fetchall()]
            
            # Группируем по типам
            volume_alerts = [a for a in all_alerts if a['alert_type'] == 'volume_spike']
            consecutive_alerts = [a for a in all_alerts if a['alert_type'] == 'consecutive_long']
            priority_alerts = [a for a in all_alerts if a['alert_type'] == 'priority']
            
            return {
                'alerts': all_alerts,
                'volume_alerts': volume_alerts,
                'consecutive_alerts': consecutive_alerts,
                'priority_alerts': priority_alerts
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения алертов: {type(e).__name__}: {str(e)}")
            return {'alerts': [], 'volume_alerts': [], 'consecutive_alerts': [], 'priority_alerts': []}
        finally:
            cursor.close()

    async def get_alerts_by_type(self, alert_type: str, limit: int = 50) -> List[Dict]:
        """Получить алерты по типу"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE alert_type = %s
                ORDER BY alert_timestamp_ms DESC 
                LIMIT %s
            """, (alert_type, limit))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения алертов по типу {alert_type}: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def clear_alerts(self, alert_type: str):
        """Очистить алерты по типу"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("DELETE FROM alerts WHERE alert_type = %s", (alert_type,))
            deleted_count = cursor.rowcount
            logger.info(f"🧹 Удалено {deleted_count} алертов типа {alert_type}")
        except Exception as e:
            logger.error(f"❌ Ошибка очистки алертов типа {alert_type}: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def get_recent_volume_alerts(self, symbol: str, minutes_back: int) -> List[Dict]:
        """Получить недавние объемные алерты"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cutoff_time_ms = int((datetime.utcnow() - timedelta(minutes=minutes_back)).timestamp() * 1000)
            
            cursor.execute("""
                SELECT * FROM alerts 
                WHERE symbol = %s AND alert_type = 'volume_spike'
                AND alert_timestamp_ms >= %s
                ORDER BY alert_timestamp_ms DESC
            """, (symbol, cutoff_time_ms))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения недавних объемных алертов для {symbol}: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def get_chart_data(self, symbol: str, hours: int = 1, alert_time: str = None) -> List[Dict]:
        """Получить данные для графика"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            if alert_time:
                # Если указано время алерта, центрируем график вокруг него
                alert_timestamp = datetime.fromisoformat(alert_time.replace('Z', '+00:00'))
                center_time_ms = int(alert_timestamp.timestamp() * 1000)
                start_time_ms = center_time_ms - (hours * 30 * 60 * 1000)  # 30 минут до
                end_time_ms = center_time_ms + (hours * 30 * 60 * 1000)    # 30 минут после
            else:
                # Обычный запрос за последние N часов
                end_time_ms = int(datetime.utcnow().timestamp() * 1000)
                start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)
            
            cursor.execute("""
                SELECT timestamp_ms as timestamp, open_price as open, high_price as high,
                       low_price as low, close_price as close, volume
                FROM kline_data 
                WHERE symbol = %s AND timestamp_ms >= %s AND timestamp_ms <= %s
                AND is_closed = TRUE
                ORDER BY timestamp_ms
            """, (symbol, start_time_ms, end_time_ms))
            
            chart_data = []
            for row in cursor.fetchall():
                chart_data.append({
                    'timestamp': row['timestamp'],
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume'])
                })
            
            return chart_data
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения данных графика для {symbol}: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    # Методы для работы с избранным
    async def get_favorites(self) -> List[Dict]:
        """Получить список избранных пар"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT symbol, notes, color, sort_order, created_at, updated_at
                FROM favorites 
                ORDER BY sort_order, symbol
            """)
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"❌ Ошибка получения избранного: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def add_to_favorites(self, symbol: str, notes: str = None, color: str = '#FFD700'):
        """Добавить пару в избранное"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO favorites (symbol, notes, color)
                VALUES (%s, %s, %s)
                ON CONFLICT (symbol) DO UPDATE SET
                    notes = EXCLUDED.notes,
                    color = EXCLUDED.color,
                    updated_at = NOW()
            """, (symbol, notes, color))
        except Exception as e:
            logger.error(f"❌ Ошибка добавления {symbol} в избранное: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def remove_from_favorites(self, symbol: str):
        """Удалить пару из избранного"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("DELETE FROM favorites WHERE symbol = %s", (symbol,))
        except Exception as e:
            logger.error(f"❌ Ошибка удаления {symbol} из избранного: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def update_favorite(self, symbol: str, notes: str = None, color: str = None, sort_order: int = None):
        """Обновить избранную пару"""
        cursor = self.connection.cursor()
        try:
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
                
        except Exception as e:
            logger.error(f"❌ Ошибка обновления избранной пары {symbol}: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    async def reorder_favorites(self, symbol_order: List[str]):
        """Изменить порядок избранных пар"""
        cursor = self.connection.cursor()
        try:
            for i, symbol in enumerate(symbol_order):
                cursor.execute("""
                    UPDATE favorites SET sort_order = %s, updated_at = NOW()
                    WHERE symbol = %s
                """, (i, symbol))
        except Exception as e:
            logger.error(f"❌ Ошибка изменения порядка избранных пар: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    # Методы для торговых настроек
    async def get_trading_settings(self) -> Dict:
        """Получить настройки торговли"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("SELECT * FROM trading_settings WHERE id = 1")
            row = cursor.fetchone()
            return dict(row) if row else {}
        except Exception as e:
            logger.error(f"❌ Ошибка получения настроек торговли: {type(e).__name__}: {str(e)}")
            return {}
        finally:
            cursor.close()

    async def update_trading_settings(self, settings: Dict):
        """Обновить настройки торговли"""
        cursor = self.connection.cursor()
        try:
            updates = []
            params = []
            
            for key, value in settings.items():
                updates.append(f"{key} = %s")
                params.append(value)
            
            if updates:
                updates.append("updated_at = NOW()")
                query = f"UPDATE trading_settings SET {', '.join(updates)} WHERE id = 1"
                cursor.execute(query, params)
                
        except Exception as e:
            logger.error(f"❌ Ошибка обновления настроек торговли: {type(e).__name__}: {str(e)}")
            raise
        finally:
            cursor.close()

    # Методы для бумажной торговли
    async def create_paper_trade(self, trade_data: Dict) -> int:
        """Создать бумажную сделку"""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                INSERT INTO paper_trades (
                    symbol, trade_type, entry_price, quantity, stop_loss, take_profit,
                    risk_amount, risk_percentage, potential_profit, potential_loss,
                    risk_reward_ratio, notes, alert_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                trade_data['symbol'],
                trade_data['trade_type'],
                trade_data['entry_price'],
                trade_data['quantity'],
                trade_data.get('stop_loss'),
                trade_data.get('take_profit'),
                trade_data.get('risk_amount'),
                trade_data.get('risk_percentage'),
                trade_data.get('potential_profit'),
                trade_data.get('potential_loss'),
                trade_data.get('risk_reward_ratio'),
                trade_data.get('notes'),
                trade_data.get('alert_id')
            ))
            
            result = cursor.fetchone()
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания бумажной сделки: {type(e).__name__}: {str(e)}")
            return None
        finally:
            cursor.close()

    async def get_paper_trades(self, status: str = None, limit: int = 100) -> List[Dict]:
        """Получить бумажные сделки"""
        cursor = self.connection.cursor(cursor_factory=RealDictCursor)
        try:
            if status:
                cursor.execute("""
                    SELECT * FROM paper_trades 
                    WHERE status = %s
                    ORDER BY entry_time DESC 
                    LIMIT %s
                """, (status, limit))
            else:
                cursor.execute("""
                    SELECT * FROM paper_trades 
                    ORDER BY entry_time DESC 
                    LIMIT %s
                """, (limit,))
            
            return [dict(row) for row in cursor.fetchall()]
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения бумажных сделок: {type(e).__name__}: {str(e)}")
            return []
        finally:
            cursor.close()

    async def close_paper_trade(self, trade_id: int, exit_price: float, exit_reason: str = 'MANUAL') -> bool:
        """Закрыть бумажную сделку"""
        cursor = self.connection.cursor()
        try:
            # Получаем данные сделки
            cursor.execute("""
                SELECT trade_type, entry_price, quantity 
                FROM paper_trades 
                WHERE id = %s AND status = 'OPEN'
            """, (trade_id,))
            
            trade = cursor.fetchone()
            if not trade:
                return False
            
            trade_type, entry_price, quantity = trade
            
            # Рассчитываем прибыль/убыток
            if trade_type == 'LONG':
                profit_loss = (exit_price - entry_price) * quantity
            else:  # SHORT
                profit_loss = (entry_price - exit_price) * quantity
            
            # Обновляем сделку
            cursor.execute("""
                UPDATE paper_trades 
                SET exit_price = %s, exit_reason = %s, actual_profit_loss = %s,
                    status = 'CLOSED', exit_time = NOW()
                WHERE id = %s
            """, (exit_price, exit_reason, profit_loss, trade_id))
            
            return cursor.rowcount > 0
            
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия бумажной сделки {trade_id}: {type(e).__name__}: {str(e)}")
            return False
        finally:
            cursor.close()

    async def get_trading_statistics(self) -> Dict:
        """Получить статистику торговли"""
        cursor = self.connection.cursor()
        try:
            # Общая статистика
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_trades,
                    COUNT(*) FILTER (WHERE status = 'OPEN') as open_trades,
                    COUNT(*) FILTER (WHERE status = 'CLOSED') as closed_trades,
                    COUNT(*) FILTER (WHERE status = 'CLOSED' AND actual_profit_loss > 0) as winning_trades,
                    COUNT(*) FILTER (WHERE status = 'CLOSED' AND actual_profit_loss < 0) as losing_trades,
                    COALESCE(SUM(actual_profit_loss) FILTER (WHERE status = 'CLOSED'), 0) as total_pnl,
                    COALESCE(AVG(actual_profit_loss) FILTER (WHERE status = 'CLOSED'), 0) as avg_pnl,
                    COALESCE(MAX(actual_profit_loss) FILTER (WHERE status = 'CLOSED'), 0) as max_profit,
                    COALESCE(MIN(actual_profit_loss) FILTER (WHERE status = 'CLOSED'), 0) as max_loss
                FROM paper_trades
            """)
            
            stats = cursor.fetchone()
            
            total_trades, open_trades, closed_trades, winning_trades, losing_trades, \
            total_pnl, avg_pnl, max_profit, max_loss = stats
            
            # Рассчитываем дополнительные метрики
            win_rate = (winning_trades / closed_trades * 100) if closed_trades > 0 else 0
            
            return {
                'total_trades': total_trades,
                'open_trades': open_trades,
                'closed_trades': closed_trades,
                'winning_trades': winning_trades,
                'losing_trades': losing_trades,
                'win_rate': round(win_rate, 2),
                'total_pnl': float(total_pnl),
                'avg_pnl': float(avg_pnl),
                'max_profit': float(max_profit),
                'max_loss': float(max_loss)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики торговли: {type(e).__name__}: {str(e)}")
            return {}
        finally:
            cursor.close()

    def close(self):
        """Закрыть соединение с базой данных"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Соединение с базой данных закрыто")