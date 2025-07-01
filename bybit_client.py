import asyncio
import json
import logging
import websockets
from typing import List, Dict, Optional, Set
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class BybitWebSocketClient:
    def __init__(self, trading_pairs: List[str], alert_manager, connection_manager):
        self.trading_pairs = set()  # Начинаем с пустого множества
        self.alert_manager = alert_manager
        self.connection_manager = connection_manager
        self.websocket = None
        self.is_running = False
        self.ping_task = None
        self.subscription_update_task = None
        self.last_message_time = None
        self.websocket_connected = False  # Добавляем флаг состояния соединения

        # Bybit WebSocket URLs
        self.ws_url = "wss://stream.bybit.com/v5/public/linear"
        self.rest_url = "https://api.bybit.com"

        # Настраиваемый интервал обновления
        self.update_interval = alert_manager.settings.get('update_interval_seconds', 1)

        # Статистика для отладки
        self.messages_received = 0
        self.last_stats_log = datetime.utcnow()

        # Кэш для отслеживания обработанных свечей
        self.processed_candles = {}  # symbol -> last_processed_timestamp

        # Отслеживание подписок
        self.subscribed_pairs = set()  # Пары, на которые мы подписаны
        self.subscription_pending = set()  # Пары, ожидающие подписки
        self.last_subscription_update = datetime.utcnow()

        # Флаги состояния
        self.data_loading_complete = False
        self.initial_subscription_complete = False
        self.streaming_active = False  # Новый флаг для отслеживания активности потока

        # Настройки переподключения
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # секунд
        self.connection_stable_time = 60  # секунд для считания соединения стабильным

        # Улучшенные настройки кэширования и проверки данных
        self.data_load_cooldown = 3600  # 1 час между загрузками для одного символа
        self.last_data_load_time = {}  # symbol -> timestamp последней загрузки
        
        # Отслеживание последней проверки целостности данных
        self.last_integrity_check = {}  # symbol -> timestamp
        self.integrity_check_interval = 1800  # 30 минут между проверками целостности

        # Управление диапазоном данных
        self.data_range_manager_task = None
        self.data_range_check_interval = 300  # 5 минут между проверками диапазона

        # Настройки для предотвращения повторной загрузки
        self.min_integrity_for_skip = 85  # Минимальная целостность для пропуска загрузки (повышено с 90)
        self.min_candles_for_skip = 30   # Минимальное количество свечей для пропуска загрузки (снижено с 50)
        self.max_data_age_hours = 6      # Максимальный возраст данных в часах

        # Мониторинг потоковых данных
        self.last_stream_data = {}  # symbol -> timestamp последних данных
        self.stream_timeout_seconds = 300  # Таймаут для потоковых данных (5 минут)
        self.stream_monitor_task = None

        # Статистика по парам для диагностики
        self.pair_statistics = {}  # symbol -> stats
        self.failed_subscriptions = set()  # Пары с неудачными подписками
        self.subscription_retry_manager_task = None

    async def start(self):
        """Запуск WebSocket соединения с правильной очередностью"""
        self.is_running = True
        logger.info("🚀 Запуск системы мониторинга торговых пар")

        try:
            # Шаг 1: Загружаем список торговых пар
            logger.info("📋 Шаг 1: Загрузка списка торговых пар...")
            await self._load_trading_pairs()

            # Шаг 2: Умная проверка и загрузка исторических данных
            logger.info("📊 Шаг 2: Умная проверка и загрузка исторических данных...")
            await self._smart_check_and_load_historical_data()

            # Шаг 3: Подключаемся к WebSocket и подписываемся на все пары
            logger.info("🔌 Шаг 3: Подключение к WebSocket и подписка на пары...")
            await self._connect_and_subscribe()

            # Шаг 4: Запускаем периодические задачи
            logger.info("⚙️ Шаг 4: Запуск периодических задач...")
            await self._start_periodic_tasks()

            # Шаг 5: Запускаем мониторинг потоковых данных
            logger.info("📡 Шаг 5: Запуск мониторинга потоковых данных...")
            await self._start_stream_monitoring()

            logger.info("✅ Система мониторинга успешно запущена!")

        except Exception as e:
            logger.error(f"❌ Ошибка запуска системы: {e}")
            raise

    async def _load_trading_pairs(self):
        """Загрузка списка торговых пар из базы данных"""
        try:
            current_pairs = await self.alert_manager.db_manager.get_watchlist()
            self.trading_pairs = set(current_pairs)
            logger.info(f"📋 Загружено {len(self.trading_pairs)} торговых пар из базы данных")

            if len(self.trading_pairs) == 0:
                logger.warning("⚠️ Список торговых пар пуст. Система будет ожидать добавления пар.")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки торговых пар: {e}")
            raise

    async def _smart_check_and_load_historical_data(self):
        """Умная проверка и загрузка исторических данных с предотвращением повторной загрузки"""
        if not self.trading_pairs:
            logger.info("📊 Нет торговых пар для загрузки данных")
            self.data_loading_complete = True
            return

        try:
            # Получаем настройки периода хранения
            retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)
            analysis_hours = self.alert_manager.settings.get('analysis_hours', 1)
            total_hours_needed = retention_hours + analysis_hours + 1  # +1 час буфера

            logger.info(f"📊 Умная проверка данных для {len(self.trading_pairs)} пар (требуется: {total_hours_needed}ч)")

            # Анализируем состояние данных для всех пар с улучшенной логикой
            pairs_analysis = await self._smart_analyze_pairs_data_status(total_hours_needed)
            
            pairs_to_load = pairs_analysis['needs_loading']
            pairs_with_data = pairs_analysis['has_data']
            pairs_partial = pairs_analysis['partial_data']
            pairs_outdated = pairs_analysis['outdated_data']

            logger.info(f"📊 Умный анализ данных:")
            logger.info(f"  ✅ Пары с актуальными данными: {len(pairs_with_data)}")
            logger.info(f"  🔄 Пары с частичными данными: {len(pairs_partial)}")
            logger.info(f"  ⏰ Пары с устаревшими данными: {len(pairs_outdated)}")
            logger.info(f"  📥 Пары требующие полной загрузки: {len(pairs_to_load)}")

            # Загружаем данные только для пар, которым это действительно нужно
            if pairs_to_load:
                logger.info(f"📊 Полная загрузка данных для {len(pairs_to_load)} пар...")
                await self._load_data_for_pairs(pairs_to_load, total_hours_needed, load_type="full")
            
            # Для пар с частичными данными - дозагружаем недостающее
            if pairs_partial:
                logger.info(f"📊 Дозагрузка данных для {len(pairs_partial)} пар с частичными данными...")
                await self._load_data_for_pairs(pairs_partial, total_hours_needed, load_type="partial")
            
            # Для устаревших данных - обновляем только недавние свечи
            if pairs_outdated:
                logger.info(f"📊 Обновление данных для {len(pairs_outdated)} пар с устаревшими данными...")
                await self._update_recent_data_for_pairs(pairs_outdated)

            # Если все пары имеют достаточно данных
            if not pairs_to_load and not pairs_partial and not pairs_outdated:
                logger.info("✅ Все пары имеют актуальные и достаточные данные, загрузка не требуется")

            self.data_loading_complete = True
            logger.info("✅ Умная проверка и загрузка исторических данных завершена")

            # Уведомляем о готовности к потоковым данным
            await self.connection_manager.broadcast_json({
                "type": "historical_data_loaded",
                "pairs_count": len(self.trading_pairs),
                "pairs_with_data": len(pairs_with_data),
                "pairs_loaded": len(pairs_to_load),
                "pairs_updated": len(pairs_partial) + len(pairs_outdated),
                "ready_for_streaming": True,
                "timestamp": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"❌ Ошибка умной проверки и загрузки исторических данных: {e}")
            raise

    async def _smart_analyze_pairs_data_status(self, hours_needed: int) -> Dict:
        """Умный анализ состояния данных для всех пар с учетом времени последней загрузки"""
        pairs_with_data = []
        pairs_partial_data = []
        pairs_need_loading = []
        pairs_outdated_data = []

        current_time = datetime.utcnow()
        current_time_ms = int(current_time.timestamp() * 1000)

        for symbol in self.trading_pairs:
            try:
                # Проверяем, когда последний раз загружали данные для этого символа
                last_load_time = self.last_data_load_time.get(symbol)
                if last_load_time:
                    time_since_load = (current_time_ms - last_load_time) / 1000  # в секундах
                    if time_since_load < self.data_load_cooldown:
                        logger.debug(f"📊 {symbol}: Пропуск проверки - данные загружались {time_since_load/60:.1f} мин назад")
                        pairs_with_data.append(symbol)
                        continue

                # Проверяем целостность данных
                integrity_info = await self.alert_manager.db_manager.check_data_integrity(
                    symbol, hours_needed
                )

                total_existing = integrity_info.get('total_existing', 0)
                integrity_percentage = integrity_info.get('integrity_percentage', 0)
                expected_count = integrity_info.get('total_expected', hours_needed * 60)

                # Проверяем возраст данных
                latest_candle_time = await self._get_latest_candle_time(symbol)
                data_age_hours = 0
                if latest_candle_time:
                    data_age_hours = (current_time_ms - latest_candle_time) / (1000 * 60 * 60)

                logger.debug(f"📊 {symbol}: {total_existing}/{expected_count} свечей ({integrity_percentage:.1f}%), возраст: {data_age_hours:.1f}ч")

                # Классифицируем пары по состоянию данных с улучшенной логикой
                if (integrity_percentage >= self.min_integrity_for_skip and 
                    total_existing >= self.min_candles_for_skip and
                    data_age_hours <= self.max_data_age_hours):
                    pairs_with_data.append(symbol)
                    logger.debug(f"✅ {symbol}: Данные актуальны и достаточны")
                    
                elif (total_existing >= 20 and 
                      integrity_percentage >= 60 and 
                      data_age_hours <= self.max_data_age_hours):
                    pairs_partial_data.append(symbol)
                    logger.debug(f"🔄 {symbol}: Частичные данные, требуется дозагрузка")
                    
                elif (total_existing >= self.min_candles_for_skip and 
                      data_age_hours > self.max_data_age_hours):
                    pairs_outdated_data.append(symbol)
                    logger.debug(f"⏰ {symbol}: Данные устарели, требуется обновление")
                    
                else:
                    pairs_need_loading.append(symbol)
                    logger.debug(f"📥 {symbol}: Требуется полная загрузка")

            except Exception as e:
                logger.error(f"❌ Ошибка анализа данных для {symbol}: {e}")
                pairs_need_loading.append(symbol)

        return {
            'has_data': pairs_with_data,
            'partial_data': pairs_partial_data,
            'needs_loading': pairs_need_loading,
            'outdated_data': pairs_outdated_data
        }

    async def _get_latest_candle_time(self, symbol: str) -> Optional[int]:
        """Получить время последней свечи для символа"""
        try:
            cursor = self.alert_manager.db_manager.connection.cursor()
            cursor.execute("""
                SELECT MAX(timestamp_ms) FROM kline_data 
                WHERE symbol = %s AND is_closed = TRUE
            """, (symbol,))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result and result[0] else None
            
        except Exception as e:
            logger.error(f"❌ Ошибка получения времени последней свечи для {symbol}: {e}")
            return None

    async def _update_recent_data_for_pairs(self, pairs: List[str]):
        """Обновление только недавних данных для пар с устаревшими данными"""
        if not pairs:
            return

        logger.info(f"📊 Обновление недавних данных для {len(pairs)} пар...")

        # Обновляем данные пакетами
        batch_size = 10
        
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i + batch_size]
            logger.info(f"📊 Обновление пакета {i // batch_size + 1}: {len(batch)} пар")

            # Обновляем пары в пакете параллельно
            tasks = [self._update_recent_symbol_data(symbol) for symbol in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Проверяем результаты
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"❌ Ошибка обновления данных для {batch[j]}: {result}")

            # Небольшая пауза между пакетами
            if i + batch_size < len(pairs):
                await asyncio.sleep(0.5)

        logger.info(f"✅ Обновление недавних данных завершено")

    async def _update_recent_symbol_data(self, symbol: str):
        """Обновление недавних данных для одного символа"""
        try:
            # Получаем время последней свечи
            latest_candle_time = await self._get_latest_candle_time(symbol)
            
            if not latest_candle_time:
                logger.warning(f"⚠️ {symbol}: Не найдено время последней свечи, пропускаем обновление")
                return

            # Загружаем данные только с последней свечи до текущего времени
            current_time_ms = int(datetime.utcnow().timestamp() * 1000)
            start_time_ms = latest_candle_time
            
            # Добавляем небольшой буфер (1 час назад от последней свечи)
            start_time_ms = max(start_time_ms - (60 * 60 * 1000), 
                               current_time_ms - (6 * 60 * 60 * 1000))  # Максимум 6 часов

            logger.debug(f"📊 {symbol}: Обновление данных с {datetime.utcfromtimestamp(start_time_ms/1000)} до текущего времени")

            # Загружаем недавние данные
            success = await self._load_full_period(symbol, start_time_ms, current_time_ms)
            
            if success:
                # Обновляем время последней загрузки
                self.last_data_load_time[symbol] = current_time_ms
                logger.debug(f"✅ Недавние данные для {symbol} обновлены успешно")
            else:
                logger.warning(f"⚠️ Не удалось обновить недавние данные для {symbol}")

        except Exception as e:
            logger.error(f"❌ Ошибка обновления недавних данных для {symbol}: {e}")

    async def _load_data_for_pairs(self, pairs: List[str], hours: int, load_type: str = "full"):
        """Загрузка данных для списка пар с указанием типа загрузки"""
        if not pairs:
            return

        action = f"{load_type.capitalize()} загрузка"
        logger.info(f"📊 {action} данных для {len(pairs)} пар...")

        # Загружаем данные пакетами для избежания перегрузки API
        batch_size = 8 if load_type == "partial" else 10
        
        for i in range(0, len(pairs), batch_size):
            batch = pairs[i:i + batch_size]
            logger.info(f"📊 {action} пакета {i // batch_size + 1}: {len(batch)} пар")

            # Загружаем пары в пакете параллельно
            tasks = [self._load_symbol_data(symbol, hours, force_load=(load_type == "full")) for symbol in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Проверяем результаты
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"❌ Ошибка загрузки данных для {batch[j]}: {result}")

            # Небольшая пауза между пакетами
            if i + batch_size < len(pairs):
                await asyncio.sleep(0.5 if load_type == "partial" else 1)

        logger.info(f"✅ {action} данных завершена")

    async def _load_symbol_data(self, symbol: str, hours: int, force_load: bool = False):
        """Загрузка данных для одного символа с улучшенной проверкой кэша"""
        try:
            current_time = datetime.utcnow()
            current_time_ms = int(current_time.timestamp() * 1000)

            # Если не принудительная загрузка, проверяем нужна ли загрузка
            if not force_load:
                # Проверяем время последней загрузки
                last_load_time = self.last_data_load_time.get(symbol)
                if last_load_time:
                    time_since_load = (current_time_ms - last_load_time) / 1000  # в секундах
                    if time_since_load < self.data_load_cooldown:
                        logger.debug(f"📊 {symbol}: Пропуск загрузки - данные загружались {time_since_load/60:.1f} мин назад")
                        return

                # Проверяем актуальность данных
                integrity_info = await self.alert_manager.db_manager.check_data_integrity(symbol, hours)
                
                if (integrity_info.get('integrity_percentage', 0) >= self.min_integrity_for_skip and
                    integrity_info.get('total_existing', 0) >= self.min_candles_for_skip):
                    
                    # Дополнительно проверяем возраст данных
                    latest_candle_time = await self._get_latest_candle_time(symbol)
                    if latest_candle_time:
                        data_age_hours = (current_time_ms - latest_candle_time) / (1000 * 60 * 60)
                        if data_age_hours <= self.max_data_age_hours:
                            logger.debug(f"📊 {symbol}: Пропуск загрузки - данные актуальны (возраст: {data_age_hours:.1f}ч)")
                            # Обновляем время последней "загрузки" чтобы не проверять снова
                            self.last_data_load_time[symbol] = current_time_ms
                            return

            # Определяем период для загрузки с запасом
            end_time_ms = current_time_ms
            start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)

            logger.info(f"📊 {symbol}: Загрузка данных за {hours} часов ({(end_time_ms - start_time_ms) // 60000} минут)")

            # Загружаем данные с биржи
            success = await self._load_full_period(symbol, start_time_ms, end_time_ms)
            
            if success:
                # Обновляем время последней загрузки
                self.last_data_load_time[symbol] = current_time_ms
                logger.debug(f"✅ Данные для {symbol} загружены успешно")
                
                # Сразу после загрузки поддерживаем правильный диапазон
                await self._maintain_exact_data_range(symbol)
            else:
                logger.warning(f"⚠️ Не удалось загрузить данные для {symbol}")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных для {symbol}: {e}")
            raise

    async def _load_full_period(self, symbol: str, start_time_ms: int, end_time_ms: int) -> bool:
        """Загрузка полного периода данных с пагинацией"""
        try:
            total_loaded = 0
            total_skipped = 0
            current_start = start_time_ms
            
            # Максимальный лимит API Bybit для kline - 1000 свечей
            max_limit = 1000
            
            while current_start < end_time_ms:
                # Рассчитываем лимит для текущего запроса
                remaining_minutes = (end_time_ms - current_start) // 60000
                limit = min(remaining_minutes + 10, max_limit)  # +10 для запаса
                
                if limit <= 0:
                    break

                url = f"{self.rest_url}/v5/market/kline"
                params = {
                    'category': 'linear',
                    'symbol': symbol,
                    'interval': '1',
                    'start': current_start,
                    'end': end_time_ms,
                    'limit': limit
                }

                logger.debug(f"📊 {symbol}: Запрос {limit} свечей с {datetime.utcfromtimestamp(current_start/1000)}")

                response = requests.get(url, params=params, timeout=15)
                
                if response.status_code != 200:
                    logger.error(f"❌ HTTP ошибка {response.status_code} для {symbol}")
                    return False
                    
                data = response.json()

                if data.get('retCode') == 0:
                    klines = data['result']['list']
                    if not klines:
                        logger.debug(f"📊 {symbol}: Нет данных в текущем диапазоне")
                        break
                        
                    # Bybit возвращает данные в обратном порядке (новые -> старые)
                    klines.reverse()

                    batch_loaded = 0
                    batch_skipped = 0
                    last_timestamp = current_start

                    for kline in klines:
                        try:
                            # Биржа передает время в миллисекундах
                            kline_timestamp_ms = int(kline[0])
                            
                            # Пропускаем свечи вне нашего диапазона
                            if kline_timestamp_ms < start_time_ms or kline_timestamp_ms >= end_time_ms:
                                continue

                            # Для исторических данных округляем до минут
                            rounded_timestamp = (kline_timestamp_ms // 60000) * 60000

                            kline_data = {
                                'start': rounded_timestamp,
                                'end': rounded_timestamp + 60000,
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                                'volume': float(kline[5]),
                                'confirm': True  # Исторические данные всегда закрыты
                            }

                            # Проверяем, есть ли уже эта свеча в базе
                            existing = await self.alert_manager.db_manager.check_candle_exists(symbol, rounded_timestamp)
                            if not existing:
                                # Сохраняем как исторические данные
                                await self.alert_manager.db_manager.save_historical_kline_data(symbol, kline_data)
                                batch_loaded += 1
                            else:
                                batch_skipped += 1
                            
                            last_timestamp = max(last_timestamp, kline_timestamp_ms)
                                
                        except Exception as e:
                            logger.error(f"❌ Ошибка обработки свечи для {symbol}: {e}")
                            continue

                    total_loaded += batch_loaded
                    total_skipped += batch_skipped
                    
                    logger.debug(f"📊 {symbol}: Пакет - загружено {batch_loaded}, пропущено {batch_skipped}")

                    # Обновляем начальную точку для следующего запроса
                    # Добавляем 1 минуту к последней обработанной свече
                    current_start = last_timestamp + 60000
                    
                    # Если получили меньше данных чем запрашивали, значит достигли конца
                    if len(klines) < limit:
                        break
                        
                else:
                    logger.error(f"❌ Ошибка API при загрузке данных для {symbol}: {data.get('retMsg')}")
                    return False

                # Небольшая задержка между запросами
                await asyncio.sleep(0.2)

            if total_loaded > 0 or total_skipped > 0:
                logger.info(f"📊 {symbol}: Загружено {total_loaded} новых свечей, пропущено {total_skipped} существующих")
            
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки полного периода для {symbol}: {e}")
            return False

    async def _connect_and_subscribe(self):
        """Подключение к WebSocket и подписка на все пары"""
        if not self.trading_pairs:
            logger.info("🔌 Нет торговых пар для подписки")
            return

        # Запускаем WebSocket соединение
        asyncio.create_task(self._websocket_connection_loop())

        # Ждем установления соединения
        max_wait = 30  # максимум 30 секунд
        wait_time = 0
        while not self.websocket_connected:
            await asyncio.sleep(1)
            wait_time += 1
            if wait_time >= max_wait:
                raise Exception("Не удалось установить WebSocket соединение")

        logger.info("✅ WebSocket соединение установлено")

        # Ждем завершения подписки на все пары
        max_subscription_wait = 60  # максимум 60 секунд
        subscription_wait = 0
        while len(self.subscribed_pairs) < len(self.trading_pairs) and subscription_wait < max_subscription_wait:
            await asyncio.sleep(1)
            subscription_wait += 1
            logger.debug(f"📡 Подписка: {len(self.subscribed_pairs)}/{len(self.trading_pairs)} пар")

        if len(self.subscribed_pairs) == len(self.trading_pairs):
            logger.info(f"✅ Подписка завершена на все {len(self.subscribed_pairs)} пар")
            self.initial_subscription_complete = True
        else:
            logger.warning(f"⚠️ Подписка частично завершена: {len(self.subscribed_pairs)}/{len(self.trading_pairs)} пар")

    async def _websocket_connection_loop(self):
        """Основной цикл WebSocket соединения с улучшенной обработкой переподключений"""
        while self.is_running:
            try:
                await self._connect_websocket()
                # Если дошли сюда, соединение было успешным
                self.reconnect_attempts = 0
                
            except Exception as e:
                logger.error(f"❌ WebSocket ошибка: {e}")
                self.websocket_connected = False
                self.streaming_active = False
                
                if self.is_running:
                    self.reconnect_attempts += 1
                    
                    if self.reconnect_attempts <= self.max_reconnect_attempts:
                        delay = min(self.reconnect_delay * self.reconnect_attempts, 60)  # Максимум 60 секунд
                        logger.info(f"🔄 Переподключение через {delay} секунд... (попытка {self.reconnect_attempts}/{self.max_reconnect_attempts})")
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"❌ Превышено максимальное количество попыток переподключения ({self.max_reconnect_attempts})")
                        self.is_running = False
                        break

    async def _connect_websocket(self):
        """Подключение к WebSocket с улучшенными настройками"""
        try:
            logger.info(f"🔌 Подключение к WebSocket: {self.ws_url}")

            # Улучшенные настройки WebSocket
            async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,  # Уменьшаем интервал ping
                    ping_timeout=10,   # Уменьшаем таймаут ping
                    close_timeout=10,
                    max_size=10**7,    # Увеличиваем максимальный размер сообщения
                    compression=None   # Отключаем сжатие для стабильности
            ) as websocket:
                self.websocket = websocket
                self.websocket_connected = True
                self.last_message_time = datetime.utcnow()

                # Сбрасываем отслеживание подписок
                self.subscribed_pairs.clear()
                self.subscription_pending.clear()
                self.failed_subscriptions.clear()

                # Подписываемся на kline данные для ВСЕХ торговых пар
                if self.trading_pairs:
                    await self._subscribe_to_pairs(self.trading_pairs)

                logger.info(f"✅ Подписка завершена на {len(self.trading_pairs)} торговых пар")

                # Отправляем статус подключения
                await self.connection_manager.broadcast_json({
                    "type": "connection_status",
                    "status": "connected",
                    "pairs_count": len(self.trading_pairs),
                    "subscribed_count": len(self.subscribed_pairs),
                    "pending_count": len(self.subscription_pending),
                    "update_interval": self.update_interval,
                    "streaming_active": True,
                    "timestamp": datetime.utcnow().isoformat()
                })

                # Запускаем задачу мониторинга соединения
                self.ping_task = asyncio.create_task(self._monitor_connection())

                # Устанавливаем флаг активности потока
                self.streaming_active = True

                # Обработка входящих сообщений
                async for message in websocket:
                    if not self.is_running:
                        break

                    try:
                        self.last_message_time = datetime.utcnow()
                        self.messages_received += 1

                        data = json.loads(message)
                        await self._handle_message(data)

                        # Логируем статистику каждые 5 минут
                        if (datetime.utcnow() - self.last_stats_log).total_seconds() > 300:
                            logger.info(
                                f"📊 WebSocket статистика: {self.messages_received} сообщений, подписано на {len(self.subscribed_pairs)} пар")
                            self.last_stats_log = datetime.utcnow()

                    except json.JSONDecodeError as e:
                        logger.warning(f"⚠️ Некорректный JSON от WebSocket: {e}")
                        continue
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки сообщения: {e}")
                        continue

        except websockets.exceptions.ConnectionClosedError as e:
            logger.warning(f"⚠️ WebSocket соединение закрыто: {e}")
            raise
        except websockets.exceptions.InvalidStatusCode as e:
            logger.error(f"❌ Неверный статус код WebSocket: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Ошибка WebSocket соединения: {e}")
            raise
        finally:
            self.websocket_connected = False
            self.streaming_active = False
            if self.ping_task:
                self.ping_task.cancel()
                try:
                    await self.ping_task
                except asyncio.CancelledError:
                    pass

    async def _subscribe_to_pairs(self, pairs: Set[str]):
        """Подписка на торговые пары с обработкой ошибок"""
        if not pairs:
            return

        # Разбиваем на группы по 50 пар для избежания ограничений WebSocket
        batch_size = 50
        pairs_list = list(pairs)

        for i in range(0, len(pairs_list), batch_size):
            batch = pairs_list[i:i + batch_size]
            subscribe_message = {
                "op": "subscribe",
                "args": [f"kline.1.{pair}" for pair in batch]
            }

            try:
                await self.websocket.send(json.dumps(subscribe_message))
                logger.info(f"📡 Подписка на пакет {i // batch_size + 1}: {len(batch)} пар")

                # Добавляем в ожидающие подписки
                self.subscription_pending.update(batch)

                # Инициализируем статистику для пар
                for pair in batch:
                    if pair not in self.pair_statistics:
                        self.pair_statistics[pair] = {
                            'messages_count': 0,
                            'last_message_time': None,
                            'subscription_attempts': 0,
                            'subscription_errors': 0,
                            'is_subscribed': False
                        }
                    self.pair_statistics[pair]['subscription_attempts'] += 1

                # Небольшая задержка между пакетами
                if i + batch_size < len(pairs_list):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка подписки на пакет {i // batch_size + 1}: {e}")
                # Добавляем пары в список неудачных подписок
                self.failed_subscriptions.update(batch)
                # Обновляем статистику ошибок
                for pair in batch:
                    if pair in self.pair_statistics:
                        self.pair_statistics[pair]['subscription_errors'] += 1
                continue

    async def _start_periodic_tasks(self):
        """Запуск периодических задач"""
        # Задача обновления подписок
        self.subscription_update_task = asyncio.create_task(self._subscription_updater())

        # Задача управления диапазоном данных
        self.data_range_manager_task = asyncio.create_task(self._data_range_manager())

        # Задача очистки данных
        asyncio.create_task(self._data_cleanup_task())

        # Задача повторных попыток подписки
        self.subscription_retry_manager_task = asyncio.create_task(self._subscription_retry_manager())

    async def _subscription_retry_manager(self):
        """Менеджер повторных попыток подписки на неудачные пары"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # Проверяем каждую минуту

                if not self.is_running or not self.websocket_connected:
                    continue

                # Проверяем неудачные подписки
                if self.failed_subscriptions:
                    logger.info(f"🔄 Повторная попытка подписки на {len(self.failed_subscriptions)} неудачных пар")
                    
                    # Пытаемся переподписаться на неудачные пары
                    failed_pairs = self.failed_subscriptions.copy()
                    self.failed_subscriptions.clear()
                    
                    await self._subscribe_to_pairs(failed_pairs)

            except Exception as e:
                logger.error(f"❌ Ошибка менеджера повторных подписок: {e}")
                await asyncio.sleep(60)

    async def _start_stream_monitoring(self):
        """Запуск мониторинга потоковых данных"""
        self.stream_monitor_task = asyncio.create_task(self._stream_monitor())
        logger.info("📡 Мониторинг потоковых данных запущен")

    async def _stream_monitor(self):
        """Мониторинг активности потоковых данных с улучшенной диагностикой"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд

                if not self.is_running:
                    break

                current_time = datetime.utcnow()
                inactive_pairs = []
                critical_pairs = []

                # Проверяем активность потоковых данных для каждой пары
                for symbol in self.trading_pairs:
                    last_data_time = self.last_stream_data.get(symbol)
                    
                    if last_data_time:
                        time_since_last = (current_time - last_data_time).total_seconds()
                        
                        if time_since_last > self.stream_timeout_seconds:
                            inactive_pairs.append(symbol)
                            
                            # Критичные пары (без данных более 5 минут)
                            if time_since_last > 300:
                                critical_pairs.append(symbol)
                                logger.error(f"🚨 КРИТИЧНО: Нет потоковых данных для {symbol} уже {time_since_last:.0f} секунд")
                            else:
                                logger.warning(f"⚠️ Нет потоковых данных для {symbol} уже {time_since_last:.0f} секунд")
                    else:
                        # Пара вообще не получала данных
                        inactive_pairs.append(symbol)
                        critical_pairs.append(symbol)
                        logger.error(f"🚨 КРИТИЧНО: {symbol} не получал потоковых данных с момента запуска")

                # Отправляем детальную статистику потоковых данных
                streaming_stats = {
                    "type": "streaming_status",
                    "active_pairs": len(self.trading_pairs) - len(inactive_pairs),
                    "inactive_pairs": len(inactive_pairs),
                    "critical_pairs": len(critical_pairs),
                    "total_pairs": len(self.trading_pairs),
                    "websocket_connected": self.websocket_connected,
                    "streaming_active": self.streaming_active,
                    "messages_received": self.messages_received,
                    "failed_subscriptions": len(self.failed_subscriptions),
                    "pair_details": {
                        symbol: {
                            'last_message': self.last_stream_data.get(symbol).isoformat() if self.last_stream_data.get(symbol) else None,
                            'messages_count': self.pair_statistics.get(symbol, {}).get('messages_count', 0),
                            'is_subscribed': symbol in self.subscribed_pairs,
                            'subscription_attempts': self.pair_statistics.get(symbol, {}).get('subscription_attempts', 0),
                            'subscription_errors': self.pair_statistics.get(symbol, {}).get('subscription_errors', 0)
                        } for symbol in self.trading_pairs
                    },
                    "timestamp": current_time.isoformat()
                }

                await self.connection_manager.broadcast_json(streaming_stats)

                # Если слишком много критичных пар или неактивных пар, пытаемся переподключиться
                critical_threshold = max(1, len(self.trading_pairs) * 0.3)  # 30% пар
                
                if len(critical_pairs) >= critical_threshold:
                    logger.error(f"❌ Слишком много критичных пар ({len(critical_pairs)}/{len(self.trading_pairs)}). Принудительное переподключение...")
                    if self.websocket:
                        await self.websocket.close()
                elif len(inactive_pairs) > len(self.trading_pairs) * 0.5:  # 50% пар неактивны
                    logger.error(f"❌ Слишком много неактивных пар ({len(inactive_pairs)}/{len(self.trading_pairs)}). Переподключение...")
                    if self.websocket:
                        await self.websocket.close()

            except Exception as e:
                logger.error(f"❌ Ошибка мониторинга потоковых данных: {e}")
                await asyncio.sleep(60)

    async def _subscription_updater(self):
        """Периодическое обновление подписок на новые пары"""
        # Получаем интервал проверки из настроек (по умолчанию 30 минут)
        check_interval_minutes = self.alert_manager.settings.get('pairs_check_interval_minutes', 30)

        logger.info(f"⚙️ Запуск периодической проверки пар каждые {check_interval_minutes} минут")

        while self.is_running:
            try:
                await asyncio.sleep(check_interval_minutes * 60)  # Конвертируем в секунды

                if not self.is_running:
                    break

                logger.info("🔄 Начинаем периодическую проверку торговых пар...")

                # Получаем актуальный список пар из базы данных
                current_pairs = set(await self.alert_manager.db_manager.get_watchlist())

                # Находим новые пары
                new_pairs = current_pairs - self.trading_pairs

                # Находим удаленные пары
                removed_pairs = self.trading_pairs - current_pairs

                if new_pairs or removed_pairs:
                    await self.handle_pairs_update(new_pairs, removed_pairs)

                self.last_subscription_update = datetime.utcnow()

            except Exception as e:
                logger.error(f"❌ Ошибка периодической проверки пар: {e}")
                await asyncio.sleep(60)  # При ошибке ждем 1 минуту

    async def handle_pairs_update(self, new_pairs: Set[str], removed_pairs: Set[str]):
        """Обработка обновления списка пар (может быть вызвана извне)"""
        try:
            if new_pairs or removed_pairs:
                logger.info(f"📋 Обновление списка пар: +{len(new_pairs)} новых, -{len(removed_pairs)} удаленных")

                # Обновляем локальный список
                self.trading_pairs.update(new_pairs)
                self.trading_pairs -= removed_pairs

                # Загружаем данные для новых пар
                if new_pairs:
                    await self._load_data_for_new_pairs(new_pairs)

                # Если WebSocket активен, обновляем подписки
                if self.websocket_connected:
                    await self._update_subscriptions(new_pairs, removed_pairs)

                logger.info("✅ Обновление списка пар завершено")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки обновления пар: {e}")

    async def _load_data_for_new_pairs(self, new_pairs: Set[str]):
        """Загрузка исторических данных для новых пар"""
        try:
            retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)
            analysis_hours = self.alert_manager.settings.get('analysis_hours', 1)
            total_hours_needed = retention_hours + analysis_hours + 1

            logger.info(f"📊 Загрузка данных для {len(new_pairs)} новых пар (период: {total_hours_needed}ч)...")

            # Загружаем данные пакетами
            await self._load_data_for_pairs(list(new_pairs), total_hours_needed, load_type="full")

            logger.info("✅ Загрузка данных для новых пар завершена")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных для новых пар: {e}")

    async def _update_subscriptions(self, new_pairs: Set[str], removed_pairs: Set[str]):
        """Обновление подписок WebSocket"""
        try:
            # Отписываемся от удаленных пар
            if removed_pairs:
                unsubscribe_message = {
                    "op": "unsubscribe",
                    "args": [f"kline.1.{pair}" for pair in removed_pairs]
                }
                await self.websocket.send(json.dumps(unsubscribe_message))
                logger.info(f"📡 Отписка от {len(removed_pairs)} пар")

                # Обновляем отслеживание подписок
                self.subscribed_pairs -= removed_pairs

                # Удаляем из мониторинга потоковых данных
                for pair in removed_pairs:
                    self.last_stream_data.pop(pair, None)
                    self.pair_statistics.pop(pair, None)

            # Подписываемся на новые пары
            if new_pairs:
                await self._subscribe_to_pairs(new_pairs)

            # Отправляем обновленную статистику
            await self.connection_manager.broadcast_json({
                "type": "subscription_updated",
                "total_pairs": len(self.trading_pairs),
                "subscribed_pairs": len(self.subscribed_pairs),
                "new_pairs": list(new_pairs),
                "removed_pairs": list(removed_pairs),
                "streaming_active": self.streaming_active,
                "timestamp": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"❌ Ошибка обновления подписок WebSocket: {e}")

    async def _data_range_manager(self):
        """Менеджер диапазона данных - поддерживает точное количество свечей"""
        logger.info(f"📊 Запуск менеджера диапазона данных (проверка каждые {self.data_range_check_interval} сек)")
        
        while self.is_running:
            try:
                await asyncio.sleep(self.data_range_check_interval)

                if not self.is_running:
                    break

                logger.debug("📊 Проверка диапазона данных для всех пар...")

                # Проверяем диапазон данных для каждого символа
                for symbol in list(self.trading_pairs):
                    try:
                        await self._maintain_exact_data_range(symbol)
                    except Exception as e:
                        logger.error(f"❌ Ошибка поддержания диапазона данных для {symbol}: {e}")

                logger.debug("✅ Проверка диапазона данных завершена")

            except Exception as e:
                logger.error(f"❌ Ошибка менеджера диапазона данных: {e}")
                await asyncio.sleep(60)  # При ошибке ждем 1 минуту

    async def _maintain_exact_data_range(self, symbol: str):
        """Поддержание точного диапазона данных согласно настройкам"""
        try:
            # Получаем настройки
            retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)
            analysis_hours = self.alert_manager.settings.get('analysis_hours', 1)
            
            # Общий диапазон = retention + analysis (без буфера для точного количества)
            total_hours = retention_hours + analysis_hours
            expected_candles = total_hours * 60  # Количество минутных свечей
            
            # Определяем временные границы
            current_time_ms = int(datetime.utcnow().timestamp() * 1000)
            # Округляем до начала минуты
            current_minute_ms = (current_time_ms // 60000) * 60000
            
            # Начало диапазона - ровно N часов назад от текущей минуты
            start_time_ms = current_minute_ms - (total_hours * 60 * 60 * 1000)
            end_time_ms = current_minute_ms  # До текущей минуты (не включая)

            logger.debug(f"📊 {symbol}: Поддержание диапазона {total_hours}ч ({expected_candles} свечей)")
            logger.debug(f"📊 {symbol}: Диапазон {datetime.utcfromtimestamp(start_time_ms/1000)} - {datetime.utcfromtimestamp(end_time_ms/1000)}")

            # 1. Удаляем старые данные (до start_time_ms)
            deleted_count = await self.alert_manager.db_manager.cleanup_old_candles_before_time(symbol, start_time_ms)
            if deleted_count > 0:
                logger.debug(f"📊 {symbol}: Удалено {deleted_count} старых свечей")

            # 2. Удаляем будущие данные (после end_time_ms)
            future_deleted = await self.alert_manager.db_manager.cleanup_future_candles_after_time(symbol, end_time_ms)
            if future_deleted > 0:
                logger.debug(f"📊 {symbol}: Удалено {future_deleted} будущих свечей")

            # 3. Проверяем целостность данных в диапазоне
            integrity_info = await self.alert_manager.db_manager.check_data_integrity_range(
                symbol, start_time_ms, end_time_ms
            )

            current_count = integrity_info.get('total_existing', 0)
            missing_count = integrity_info.get('missing_count', 0)
            
            logger.debug(f"📊 {symbol}: Текущее количество свечей: {current_count}/{expected_candles}, пропущено: {missing_count}")

            # 4. Загружаем недостающие данные, если их много
            if missing_count > 10:  # Загружаем только если пропущено больше 10 свечей
                logger.info(f"📊 {symbol}: Загрузка {missing_count} недостающих свечей...")
                
                # Загружаем недостающие данные
                success = await self._load_full_period(symbol, start_time_ms, end_time_ms)
                if success:
                    # Повторно проверяем количество после загрузки
                    new_integrity = await self.alert_manager.db_manager.check_data_integrity_range(
                        symbol, start_time_ms, end_time_ms
                    )
                    new_count = new_integrity.get('total_existing', 0)
                    logger.info(f"📊 {symbol}: После загрузки: {new_count}/{expected_candles} свечей")

        except Exception as e:
            logger.error(f"❌ Ошибка поддержания точного диапазона данных для {symbol}: {e}")

    async def _data_cleanup_task(self):
        """Задача очистки старых данных"""
        while self.is_running:
            try:
                # Очищаем данные каждый час
                await asyncio.sleep(3600)

                if not self.is_running:
                    break

                logger.info("🧹 Начинаем очистку старых данных...")

                retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)

                # Очищаем данные для каждого символа
                for symbol in self.trading_pairs:
                    try:
                        await self.alert_manager.db_manager.cleanup_old_candles(symbol, retention_hours)
                    except Exception as e:
                        logger.error(f"❌ Ошибка очистки данных для {symbol}: {e}")

                logger.info("✅ Очистка старых данных завершена")

            except Exception as e:
                logger.error(f"❌ Ошибка задачи очистки данных: {e}")

    async def _handle_message(self, data: Dict):
        """Обработка входящих WebSocket сообщений"""
        try:
            # Обрабатываем системные сообщения
            if 'success' in data:
                if data['success']:
                    logger.debug("✅ Успешная подписка на WebSocket пакет")
                    # Перемещаем пары из ожидающих в подписанные
                    # (точное определение каких пар требует дополнительной логики)
                else:
                    logger.error(f"❌ Ошибка подписки WebSocket: {data}")
                return

            if 'op' in data:
                logger.debug(f"📡 Системное сообщение WebSocket: {data}")
                return

            # Обрабатываем данные свечей
            if data.get('topic', '').startswith('kline.1.'):
                kline_data = data['data'][0]
                symbol = data['topic'].split('.')[-1]

                # Проверяем, что символ в нашем списке
                if symbol not in self.trading_pairs:
                    logger.debug(f"📊 Получены данные для символа {symbol}, которого нет в watchlist")
                    return

                # Добавляем символ в подписанные (если получили данные, значит подписка работает)
                if symbol in self.subscription_pending:
                    self.subscription_pending.remove(symbol)
                if symbol in self.failed_subscriptions:
                    self.failed_subscriptions.remove(symbol)
                    logger.info(f"✅ Восстановлена подписка на {symbol}")
                
                self.subscribed_pairs.add(symbol)

                # Обновляем статистику пары
                if symbol in self.pair_statistics:
                    self.pair_statistics[symbol]['messages_count'] += 1
                    self.pair_statistics[symbol]['last_message_time'] = datetime.utcnow()
                    self.pair_statistics[symbol]['is_subscribed'] = True

                # Обновляем время последних потоковых данных
                self.last_stream_data[symbol] = datetime.utcnow()

                # Биржа передает время в миллисекундах
                start_time_ms = int(kline_data['start'])
                end_time_ms = int(kline_data['end'])
                is_closed = kline_data.get('confirm', False)

                # Для потоковых данных оставляем миллисекунды, но для закрытых свечей - округляем
                if is_closed:
                    # Закрытые свечи с округлением до минут
                    start_time_ms = (start_time_ms // 60000) * 60000
                    end_time_ms = (end_time_ms // 60000) * 60000

                # Преобразуем данные в нужный формат
                formatted_data = {
                    'start': start_time_ms,
                    'end': end_time_ms,
                    'open': kline_data['open'],
                    'high': kline_data['high'],
                    'low': kline_data['low'],
                    'close': kline_data['close'],
                    'volume': kline_data['volume'],
                    'confirm': is_closed
                }

                # Обрабатываем закрытые свечи
                if is_closed:
                    await self._process_closed_candle(symbol, formatted_data)

                # Сохраняем данные в базу (потоковые или закрытые)
                await self.alert_manager.db_manager.save_kline_data(symbol, formatted_data, is_closed)

                # Отправляем обновление данных клиентам (потоковые данные)
                stream_item = {
                    "type": "kline_update",
                    "symbol": symbol,
                    "data": formatted_data,
                    "timestamp": datetime.utcnow().isoformat(),
                    "is_closed": is_closed,
                    "streaming_active": self.streaming_active,
                    "server_timestamp": self.alert_manager._get_current_timestamp_ms() if hasattr(self.alert_manager,
                                                                                                  '_get_current_timestamp_ms') else int(
                        datetime.utcnow().timestamp() * 1000)
                }

                await self.connection_manager.broadcast_json(stream_item)

        except Exception as e:
            logger.error(f"❌ Ошибка обработки kline данных: {e}")

    async def _process_closed_candle(self, symbol: str, formatted_data: Dict):
        """Обработка закрытой свечи"""
        try:
            start_time_ms = formatted_data['start']

            # Простая проверка на дублирование для закрытых свечей
            last_processed = self.processed_candles.get(symbol, 0)
            if start_time_ms > last_processed:
                # Обрабатываем через менеджер алертов
                alerts = await self.alert_manager.process_kline_data(symbol, formatted_data)

                # Помечаем свечу как обработанную
                self.processed_candles[symbol] = start_time_ms

                logger.debug(f"📊 Обработана закрытая свеча {symbol} в {start_time_ms}")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки закрытой свечи для {symbol}: {e}")

    async def _monitor_connection(self):
        """Мониторинг состояния WebSocket соединения"""
        connection_start_time = datetime.utcnow()
        
        while self.is_running and self.websocket_connected:
            try:
                await asyncio.sleep(30)  # Проверяем каждые 30 секунд

                if not self.websocket_connected:
                    break

                current_time = datetime.utcnow()
                
                # Проверяем время последнего сообщения
                if self.last_message_time:
                    time_since_last_message = (current_time - self.last_message_time).total_seconds()

                    if time_since_last_message > 90:  # 90 секунд без сообщений
                        logger.warning(f"⚠️ Нет сообщений от WebSocket уже {time_since_last_message:.0f} секунд")

                        await self.connection_manager.broadcast_json({
                            "type": "connection_status",
                            "status": "warning",
                            "reason": f"No messages for {time_since_last_message:.0f} seconds",
                            "streaming_active": False,
                            "timestamp": current_time.isoformat()
                        })

                        # Если нет сообщений более 5 минут, принудительно переподключаемся
                        if time_since_last_message > 300:  # Снижено с 120 до 300 секунд
                            logger.error("❌ Принудительное переподключение из-за отсутствия сообщений")
                            self.streaming_active = False
                            break

                # Проверяем стабильность соединения
                connection_duration = (current_time - connection_start_time).total_seconds()
                if connection_duration > self.connection_stable_time:
                    # Соединение стабильно, сбрасываем счетчик попыток
                    self.reconnect_attempts = 0

            except Exception as e:
                logger.error(f"❌ Ошибка мониторинга соединения: {e}")
                break

    async def stop(self):
        """Остановка WebSocket соединения"""
        self.is_running = False
        self.websocket_connected = False
        self.streaming_active = False
        
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
                
        if self.subscription_update_task:
            self.subscription_update_task.cancel()
            try:
                await self.subscription_update_task
            except asyncio.CancelledError:
                pass

        if self.data_range_manager_task:
            self.data_range_manager_task.cancel()
            try:
                await self.data_range_manager_task
            except asyncio.CancelledError:
                pass

        if self.stream_monitor_task:
            self.stream_monitor_task.cancel()
            try:
                await self.stream_monitor_task
            except asyncio.CancelledError:
                pass

        if self.subscription_retry_manager_task:
            self.subscription_retry_manager_task.cancel()
            try:
                await self.subscription_retry_manager_task
            except asyncio.CancelledError:
                pass
                
        if self.websocket:
            try:
                await self.websocket.close()
            except Exception as e:
                logger.debug(f"Ошибка при закрытии WebSocket: {e}")
                
        logger.info("🛑 WebSocket клиент остановлен")

    def get_subscription_stats(self) -> Dict:
        """Получить статистику подписок"""
        return {
            'total_pairs': len(self.trading_pairs),
            'subscribed_pairs': len(self.subscribed_pairs),
            'pending_pairs': len(self.subscription_pending),
            'failed_pairs': len(self.failed_subscriptions),
            'last_update': self.last_subscription_update.isoformat() if self.last_subscription_update else None,
            'subscription_rate': len(self.subscribed_pairs) / len(
                self.trading_pairs) * 100 if self.trading_pairs else 0,
            'data_loading_complete': self.data_loading_complete,
            'initial_subscription_complete': self.initial_subscription_complete,
            'websocket_connected': self.websocket_connected,
            'streaming_active': self.streaming_active,
            'reconnect_attempts': self.reconnect_attempts,
            'max_reconnect_attempts': self.max_reconnect_attempts,
            'messages_received': self.messages_received,
            'active_streams': len([s for s, t in self.last_stream_data.items() 
                                 if (datetime.utcnow() - t).total_seconds() < self.stream_timeout_seconds]),
            'pair_statistics': self.pair_statistics,
            'data_load_times': {symbol: datetime.utcfromtimestamp(timestamp/1000).isoformat() 
                               for symbol, timestamp in self.last_data_load_time.items()}
        }