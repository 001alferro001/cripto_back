import requests
import json

url = "https://api.bybit.com/v5/market/time"

try:
    response = requests.get(url)
    response.raise_for_status()  # Проверяем наличие ошибок HTTP
    data = response.json()
    server_time = data['time_now']
    print(f"Текущее время сервера Bybit: {server_time}")
except requests.exceptions.RequestException as e:
    print(f"Ошибка при запросе времени: {e}")
except (KeyError, json.JSONDecodeError) as e:
    print(f"Ошибка при обработке ответа: {e}")