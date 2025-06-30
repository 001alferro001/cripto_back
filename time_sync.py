import asyncio
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List
import time

logger = logging.getLogger(__name__)


class TimeServerSync:
    """Синхронизация с серверами точного времени"""
    
    def __init__(self):
        # Список серверов точного времени
        self.time_servers = [
            "http://worldtimeapi.org/api/timezone/UTC",
            "https://timeapi.io/api/Time/current/zone?timeZone=UTC",
            "http://worldclockapi.com/api/json/utc/now"
        ]
        self.last_sync = None
        self.time_offset_ms = 0  # Смещение локального времени относительно точного UTC
        self.is_synced = False
        
    async def sync_with_time_servers(self) -> bool:
        """Синхронизация с серверами точного времени"""
        for server_url in self.time_servers:
            try:
                success = await self._sync_with_server(server_url)
                if success:
                    self.is_synced = True
                    self.last_sync = datetime.utcnow()
                    logger.info(f"✅ Синхронизация с сервером времени успешна: {server_url}")
                    logger.info(f"⏰ Смещение времени: {self.time_offset_ms}мс")
                    return True
            except Exception as e:
                logger.warning(f"⚠️ Ошибка синхронизации с {server_url}: {e}")
                continue
        
        logger.error("❌ Не удалось синхронизироваться ни с одним сервером времени")
        return False
    
    async def _sync_with_server(self, server_url: str) -> bool:
        """Синхронизация с конкретным сервером"""
        try:
            # Засекаем время до запроса
            local_time_before = time.time() * 1000
            
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(server_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Засекаем время после получения ответа
                        local_time_after = time.time() * 1000
                        
                        # Извлекаем UTC время из ответа
                        server_time_ms = self._extract_utc_time(data, server_url)
                        if server_time_ms is None:
                            return False
                        
                        # Учитываем задержку сети
                        network_delay = (local_time_after - local_time_before) / 2
                        adjusted_local_time = local_time_before + network_delay
                        
                        # Рассчитываем смещение
                        self.time_offset_ms = server_time_ms - adjusted_local_time
                        
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Ошибка синхронизации с {server_url}: {e}")
            return False
    
    def _extract_utc_time(self, data: Dict, server_url: str) -> Optional[int]:
        """Извлечение UTC времени из ответа сервера"""
        try:
            if "worldtimeapi.org" in server_url:
                # WorldTimeAPI
                utc_datetime = data.get('utc_datetime')
                if utc_datetime:
                    dt = datetime.fromisoformat(utc_datetime.replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
            
            elif "timeapi.io" in server_url:
                # TimeAPI.io
                date_time = data.get('dateTime')
                if date_time:
                    dt = datetime.fromisoformat(date_time.replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
            
            elif "worldclockapi.com" in server_url:
                # WorldClockAPI
                current_date_time = data.get('currentDateTime')
                if current_date_time:
                    dt = datetime.fromisoformat(current_date_time.replace('Z', '+00:00'))
                    return int(dt.timestamp() * 1000)
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка извлечения времени из ответа {server_url}: {e}")
            return None
    
    def get_accurate_utc_timestamp_ms(self) -> int:
        """Получить точный UTC timestamp в миллисекундах"""
        if self.is_synced:
            local_time_ms = time.time() * 1000
            return int(local_time_ms + self.time_offset_ms)
        else:
            # Fallback на локальное UTC время
            return int(datetime.utcnow().timestamp() * 1000)
    
    def get_sync_status(self) -> Dict:
        """Получить статус синхронизации"""
        return {
            'is_synced': self.is_synced,
            'last_sync': self.last_sync.isoformat() if self.last_sync else None,
            'time_offset_ms': self.time_offset_ms,
            'sync_age_seconds': (datetime.utcnow() - self.last_sync).total_seconds() if self.last_sync else None,
            'accurate_utc_time': self.get_accurate_utc_timestamp_ms(),
            'status': 'synced' if self.is_synced else 'not_synced'
        }


class ExchangeTimeSync:
    """Класс для синхронизации времени с биржей Bybit и серверами точного времени"""

    def __init__(self):
        self.exchange_time_offset = 0  # Разница между локальным и биржевым временем в мс
        self.last_exchange_sync = None
        self.exchange_sync_interval = 300  # Синхронизация с биржей каждые 5 минут
        self.is_running = False
        self.is_exchange_synced = False
        self.sync_task = None
        
        # Синхронизация с серверами точного времени
        self.time_server_sync = TimeServerSync()
        self.time_server_sync_interval = 3600  # Синхронизация с серверами времени каждый час
        
        # Настройки синхронизации
        self.sync_method = 'auto'  # 'auto', 'exchange_only', 'time_servers_only'

    async def start(self):
        """Запуск автоматической синхронизации времени"""
        self.is_running = True
        logger.info("🕐 Запуск системы синхронизации времени UTC")

        # Первоначальная синхронизация с серверами точного времени
        await self.time_server_sync.sync_with_time_servers()

        # Первоначальная синхронизация с биржей
        await self.sync_exchange_time()

        # Запускаем периодическую синхронизацию
        self.sync_task = asyncio.create_task(self._periodic_sync())

    async def stop(self):
        """Остановка синхронизации"""
        self.is_running = False
        if self.sync_task:
            self.sync_task.cancel()
            try:
                await self.sync_task
            except asyncio.CancelledError:
                pass
        logger.info("🕐 Синхронизация времени остановлена")

    async def _periodic_sync(self):
        """Периодическая синхронизация времени"""
        last_time_server_sync = datetime.utcnow()
        
        while self.is_running:
            try:
                # Синхронизация с биржей каждые 5 минут
                await asyncio.sleep(self.exchange_sync_interval)
                if self.is_running:
                    await self.sync_exchange_time()
                
                # Синхронизация с серверами времени каждый час
                if (datetime.utcnow() - last_time_server_sync).total_seconds() > self.time_server_sync_interval:
                    await self.time_server_sync.sync_with_time_servers()
                    last_time_server_sync = datetime.utcnow()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Ошибка периодической синхронизации времени: {e}")
                await asyncio.sleep(60)  # Повторить через минуту при ошибке

    async def sync_exchange_time(self) -> bool:
        """Синхронизация времени с биржей"""
        try:
            url = "https://api.bybit.com/v5/market/time"

            # Используем точное UTC время для измерения задержки
            accurate_time_before = self.time_server_sync.get_accurate_utc_timestamp_ms()

            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Засекаем время после получения ответа
                        accurate_time_after = self.time_server_sync.get_accurate_utc_timestamp_ms()

                        if data.get('retCode') == 0:
                            # Получаем время биржи
                            exchange_time_seconds = int(data['result']['timeSecond'])
                            exchange_time_nanos = int(data['result']['timeNano'])

                            # Преобразуем в миллисекунды
                            exchange_time = exchange_time_seconds * 1000 + (exchange_time_nanos // 1_000_000) % 100

                            # Учитываем задержку сети
                            network_delay = (accurate_time_after - accurate_time_before) / 2
                            adjusted_accurate_time = accurate_time_before + network_delay

                            # Рассчитываем смещение биржи относительно точного UTC
                            self.exchange_time_offset = exchange_time - adjusted_accurate_time
                            self.last_exchange_sync = datetime.utcnow()
                            self.is_exchange_synced = True

                            # Проверяем корректность времени
                            expected_range_min = 1700000000000  # 2023 год
                            expected_range_max = 2000000000000  # 2033 год

                            if expected_range_min <= exchange_time <= expected_range_max:
                                logger.info(
                                    f"✅ Время синхронизировано с биржей Bybit. Смещение биржи: {self.exchange_time_offset:.0f}мс")
                                return True
                            else:
                                logger.error(
                                    f"❌ Некорректное время биржи: {exchange_time}")
                                self.is_exchange_synced = False
                                return False
                        else:
                            logger.error(f"❌ Ошибка API биржи при синхронизации времени: {data.get('retMsg')}")
                    else:
                        logger.error(f"❌ HTTP ошибка при синхронизации времени: {response.status}")

        except asyncio.TimeoutError:
            logger.error("⏰ Таймаут при синхронизации времени с биржей")
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации времени с биржей: {e}")

        self.is_exchange_synced = False
        return False

    def get_utc_timestamp_ms(self) -> int:
        """Получить точный UTC timestamp в миллисекундах"""
        if self.sync_method == 'time_servers_only' or not self.is_exchange_synced:
            # Используем серверы точного времени
            return self.time_server_sync.get_accurate_utc_timestamp_ms()
        elif self.sync_method == 'exchange_only':
            # Используем только биржевое время
            accurate_time = self.time_server_sync.get_accurate_utc_timestamp_ms()
            return int(accurate_time + self.exchange_time_offset)
        else:
            # Автоматический режим - приоритет серверам точного времени
            if self.time_server_sync.is_synced:
                return self.time_server_sync.get_accurate_utc_timestamp_ms()
            elif self.is_exchange_synced:
                accurate_time = self.time_server_sync.get_accurate_utc_timestamp_ms()
                return int(accurate_time + self.exchange_time_offset)
            else:
                # Fallback на локальное UTC время
                return int(datetime.utcnow().timestamp() * 1000)

    def get_exchange_timestamp_ms(self) -> int:
        """Получить timestamp биржи в миллисекундах"""
        if self.is_exchange_synced:
            accurate_time = self.time_server_sync.get_accurate_utc_timestamp_ms()
            return int(accurate_time + self.exchange_time_offset)
        else:
            # Fallback на точное UTC время
            return self.get_utc_timestamp_ms()

    def get_sync_status(self) -> dict:
        """Получить статус синхронизации"""
        utc_time = self.get_utc_timestamp_ms()
        
        return {
            'is_synced': self.time_server_sync.is_synced or self.is_exchange_synced,
            'time_servers': self.time_server_sync.get_sync_status(),
            'exchange_sync': {
                'is_synced': self.is_exchange_synced,
                'last_sync': self.last_exchange_sync.isoformat() if self.last_exchange_sync else None,
                'time_offset_ms': self.exchange_time_offset,
                'sync_age_seconds': (datetime.utcnow() - self.last_exchange_sync).total_seconds() if self.last_exchange_sync else None
            },
            'sync_method': self.sync_method,
            'utc_time': utc_time,
            'utc_time_iso': datetime.utcfromtimestamp(utc_time / 1000).isoformat() + 'Z',
            'serverTime': utc_time,  # Для совместимости с клиентом
            'status': 'active' if (self.time_server_sync.is_synced or self.is_exchange_synced) else 'not_synced'
        }

    def set_sync_method(self, method: str):
        """Установить метод синхронизации"""
        if method in ['auto', 'exchange_only', 'time_servers_only']:
            self.sync_method = method
            logger.info(f"🔧 Метод синхронизации изменен на: {method}")
        else:
            logger.error(f"❌ Неизвестный метод синхронизации: {method}")

    def is_candle_closed(self, kline_data: dict) -> bool:
        """Проверка закрытия свечи относительно UTC времени"""
        utc_time = self.get_utc_timestamp_ms()
        candle_end_time = int(kline_data['end'])

        # Свеча считается закрытой, если UTC время >= времени окончания свечи
        return utc_time >= candle_end_time

    def get_candle_close_time_utc(self, kline_start_time: int) -> datetime:
        """Получить время закрытия свечи в UTC"""
        return datetime.utcfromtimestamp((kline_start_time + 60000) / 1000)