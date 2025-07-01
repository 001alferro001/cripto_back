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

        # Настройки переподключения
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 10
        self.reconnect_delay = 5  # секунд
        self.connection_stable_time = 60  # секунд для считания соединения стабильным

        # Кэш для предотвращения повторной загрузки данных
        self.data_load_cache = {}  # symbol -> last_load_timestamp
        self.data_load_cooldown = 300  # 5 минут между загрузками для одного символа
        
        # Отслеживание последней проверки целостности данных
        self.last_integrity_check = {}  # symbol -> timestamp
        self.integrity_check_interval = 1800  # 30 минут между проверками целостности

    async def start(self):
        """Запуск WebSocket соединения с правильной очередностью"""
        self.is_running = True
        logger.info("🚀 Запуск системы мониторинга торговых пар")

        try:
            # Шаг 1: Загружаем список торговых пар
            logger.info("📋 Шаг 1: Загрузка списка торговых пар...")
            await self._load_trading_pairs()

            # Шаг 2: Загружаем исторические данные для всех пар (ТОЛЬКО ОДИН РАЗ)
            logger.info("📊 Шаг 2: Загрузка исторических данных...")
            await self._load_historical_data()

            # Шаг 3: Подключаемся к WebSocket и подписываемся на все пары
            logger.info("🔌 Шаг 3: Подключение к WebSocket и подписка на пары...")
            await self._connect_and_subscribe()

            # Шаг 4: Запускаем периодические задачи
            logger.info("⚙️ Шаг 4: Запуск периодических задач...")
            await self._start_periodic_tasks()

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

    async def _load_historical_data(self):
        """Загрузка исторических данных для всех пар"""
        if not self.trading_pairs:
            logger.info("📊 Нет торговых пар для загрузки данных")
            self.data_loading_complete = True
            return

        try:
            # Получаем настройки периода хранения
            retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)
            analysis_hours = self.alert_manager.settings.get('analysis_hours', 1)
            total_hours_needed = retention_hours + analysis_hours + 1  # +1 час буфера

            logger.info(f"📊 Начинаем загрузку данных для {len(self.trading_pairs)} пар (период: {total_hours_needed}ч)")

            # Проверяем какие пары нуждаются в загрузке данных
            pairs_to_load = []
            pairs_with_data = []

            for symbol in self.trading_pairs:
                try:
                    integrity_info = await self.alert_manager.db_manager.check_data_integrity(
                        symbol, total_hours_needed
                    )

                    # Если данных мало или целостность низкая - добавляем в список для загрузки
                    if integrity_info['integrity_percentage'] < 80 or integrity_info['total_existing'] < 60:
                        pairs_to_load.append(symbol)
                        logger.debug(
                            f"📊 {symbol}: Требуется загрузка ({integrity_info['total_existing']}/{integrity_info['total_expected']} свечей)")
                    else:
                        pairs_with_data.append(symbol)
                        logger.debug(f"✅ {symbol}: Данные актуальны ({integrity_info['integrity_percentage']:.1f}%)")

                except Exception as e:
                    logger.error(f"❌ Ошибка проверки данных для {symbol}: {e}")
                    pairs_to_load.append(symbol)

            logger.info(
                f"📊 Найдено {len(pairs_with_data)} пар с актуальными данными, {len(pairs_to_load)} требуют загрузки")

            # Загружаем данные для пар, которым это нужно
            if pairs_to_load:
                logger.info(f"📊 Загрузка данных для {len(pairs_to_load)} пар...")

                # Загружаем данные пакетами для избежания перегрузки API
                batch_size = 10
                for i in range(0, len(pairs_to_load), batch_size):
                    batch = pairs_to_load[i:i + batch_size]
                    logger.info(f"📊 Загрузка пакета {i // batch_size + 1}: {len(batch)} пар")

                    # Загружаем пары в пакете параллельно
                    tasks = [self._load_symbol_data(symbol, total_hours_needed) for symbol in batch]
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Проверяем результаты
                    for j, result in enumerate(results):
                        if isinstance(result, Exception):
                            logger.error(f"❌ Ошибка загрузки данных для {batch[j]}: {result}")

                    # Небольшая пауза между пакетами
                    if i + batch_size < len(pairs_to_load):
                        await asyncio.sleep(1)

                logger.info("✅ Загрузка исторических данных завершена")
            else:
                logger.info("✅ Все данные актуальны, загрузка не требуется")

            # Помечаем все пары как загруженные
            current_time = datetime.utcnow()
            for symbol in self.trading_pairs:
                self.data_load_cache[symbol] = current_time
                self.last_integrity_check[symbol] = current_time

            self.data_loading_complete = True

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки исторических данных: {e}")
            raise

    async def _load_symbol_data(self, symbol: str, hours: int):
        """Загрузка данных для одного символа с проверкой кэша"""
        try:
            # Проверяем кэш загрузки
            current_time = datetime.utcnow()
            if symbol in self.data_load_cache:
                last_load = self.data_load_cache[symbol]
                if (current_time - last_load).total_seconds() < self.data_load_cooldown:
                    logger.debug(f"📊 {symbol}: Пропуск загрузки (кэш актуален)")
                    return

            # Определяем период для загрузки
            end_time_ms = int(datetime.utcnow().timestamp() * 1000)
            start_time_ms = end_time_ms - (hours * 60 * 60 * 1000)

            # Загружаем данные с биржи
            success = await self._load_full_period(symbol, start_time_ms, end_time_ms)
            
            if success:
                # Обновляем кэш загрузки
                self.data_load_cache[symbol] = current_time
                logger.debug(f"✅ Данные для {symbol} загружены успешно")
            else:
                logger.warning(f"⚠️ Не удалось загрузить данные для {symbol}")

        except Exception as e:
            logger.error(f"❌ Ошибка загрузки данных для {symbol}: {e}")
            raise

    async def _load_full_period(self, symbol: str, start_time_ms: int, end_time_ms: int) -> bool:
        """Загрузка полного периода данных"""
        try:
            hours = (end_time_ms - start_time_ms) / (60 * 60 * 1000)
            limit = min(int(hours * 60) + 60, 1000)

            url = f"{self.rest_url}/v5/market/kline"
            params = {
                'category': 'linear',
                'symbol': symbol,
                'interval': '1',
                'start': start_time_ms,
                'end': end_time_ms,
                'limit': limit
            }

            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.error(f"❌ HTTP ошибка {response.status_code} для {symbol}")
                return False
                
            data = response.json()

            if data.get('retCode') == 0:
                klines = data['result']['list']
                if not klines:
                    logger.warning(f"⚠️ Нет данных для {symbol}")
                    return False
                    
                klines.reverse()  # Bybit возвращает данные в обратном порядке

                saved_count = 0
                skipped_count = 0

                for kline in klines:
                    try:
                        # Биржа передает время в миллисекундах
                        kline_timestamp_ms = int(kline[0])

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
                            saved_count += 1
                        else:
                            skipped_count += 1
                            
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки свечи для {symbol}: {e}")
                        continue

                logger.info(f"📊 {symbol}: Загружено {saved_count} новых свечей, пропущено {skipped_count} существующих")
                return True
            else:
                logger.error(f"❌ Ошибка API при загрузке данных для {symbol}: {data.get('retMsg')}")
                return False

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
                    "timestamp": datetime.utcnow().isoformat()
                })

                # Запускаем задачу мониторинга соединения
                self.ping_task = asyncio.create_task(self._monitor_connection())

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

                # Небольшая задержка между пакетами
                if i + batch_size < len(pairs_list):
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"❌ Ошибка подписки на пакет {i // batch_size + 1}: {e}")
                # Продолжаем с следующим пакетом
                continue

    async def _start_periodic_tasks(self):
        """Запуск периодических задач"""
        # Задача обновления подписок
        self.subscription_update_task = asyncio.create_task(self._subscription_updater())

        # Задача очистки данных
        asyncio.create_task(self._data_cleanup_task())

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

            logger.info(f"📊 Загрузка данных для {len(new_pairs)} новых пар...")

            # Загружаем данные пакетами
            batch_size = 5
            new_pairs_list = list(new_pairs)
            
            for i in range(0, len(new_pairs_list), batch_size):
                batch = new_pairs_list[i:i + batch_size]
                logger.info(f"📊 Загрузка пакета новых пар {i // batch_size + 1}: {len(batch)} пар")

                # Загружаем пары в пакете параллельно
                tasks = [self._load_symbol_data(symbol, total_hours_needed) for symbol in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Проверяем результаты
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"❌ Ошибка загрузки данных для новой пары {batch[j]}: {result}")

                # Небольшая пауза между пакетами
                if i + batch_size < len(new_pairs_list):
                    await asyncio.sleep(1)

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
                "timestamp": datetime.utcnow().isoformat()
            })

        except Exception as e:
            logger.error(f"❌ Ошибка обновления подписок WebSocket: {e}")

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
                self.subscribed_pairs.add(symbol)

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

                # УБИРАЕМ автоматическое поддержание диапазона данных
                # await self._maintain_data_range(symbol)

                logger.debug(f"📊 Обработана закрытая свеча {symbol} в {start_time_ms}")

        except Exception as e:
            logger.error(f"❌ Ошибка обработки закрытой свечи для {symbol}: {e}")

    async def _maintain_data_range(self, symbol: str):
        """Поддержание диапазона данных в заданных пределах (ТОЛЬКО при необходимости)"""
        try:
            # Проверяем, нужна ли проверка целостности для этого символа
            current_time = datetime.utcnow()
            if symbol in self.last_integrity_check:
                last_check = self.last_integrity_check[symbol]
                if (current_time - last_check).total_seconds() < self.integrity_check_interval:
                    return  # Проверка не нужна

            # Получаем настройки диапазона
            retention_hours = self.alert_manager.settings.get('data_retention_hours', 2)
            analysis_hours = self.alert_manager.settings.get('analysis_hours', 1)
            total_hours_needed = retention_hours + analysis_hours + 1

            # Очищаем старые данные
            await self.alert_manager.db_manager.cleanup_old_candles(symbol, total_hours_needed)

            # Проверяем, нужно ли загрузить новые данные (ТОЛЬКО если критически мало)
            integrity_info = await self.alert_manager.db_manager.check_data_integrity(symbol, total_hours_needed)

            # Загружаем данные ТОЛЬКО если целостность очень низкая
            if integrity_info['integrity_percentage'] < 50 and integrity_info['missing_count'] > 30:
                logger.info(f"📊 Критическая нехватка данных для {symbol}: {integrity_info['missing_count']} свечей, загружаем...")
                await self._load_symbol_data(symbol, total_hours_needed)

            # Обновляем время последней проверки
            self.last_integrity_check[symbol] = current_time

        except Exception as e:
            logger.error(f"❌ Ошибка поддержания диапазона данных для {symbol}: {e}")

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
                            "timestamp": current_time.isoformat()
                        })

                        # Если нет сообщений более 2 минут, принудительно переподключаемся
                        if time_since_last_message > 120:
                            logger.error("❌ Принудительное переподключение из-за отсутствия сообщений")
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
            'last_update': self.last_subscription_update.isoformat() if self.last_subscription_update else None,
            'subscription_rate': len(self.subscribed_pairs) / len(
                self.trading_pairs) * 100 if self.trading_pairs else 0,
            'data_loading_complete': self.data_loading_complete,
            'initial_subscription_complete': self.initial_subscription_complete,
            'websocket_connected': self.websocket_connected,
            'reconnect_attempts': self.reconnect_attempts,
            'max_reconnect_attempts': self.max_reconnect_attempts
        }