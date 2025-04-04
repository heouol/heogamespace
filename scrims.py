# --- START OF FILE scrims.py ---

import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta, timezone
import time
from collections import defaultdict
import sys # Added for error handling exit

# --- КОНСТАНТЫ и НАСТРОЙКИ ---
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63") # Используйте переменную окружения или ваш ключ
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC" # Имя вашей команды для отображения
# !!! ВАЖНО: Убедитесь, что эти ID верны для ВАШЕЙ команды в системе GRID !!!
# Найденные ID в вашем старом коде:
PLAYER_IDS = {
    "26433": "Aytekn",
    "25262": "Pallet",
    "25266": "Tsiperakos",
    "20958": "Kenal",
    "21922": "CENTU"
}
# Сопоставление RiotIdGameName (после нормализации) с ID игрока GRID
# Это может потребовать ручного сопоставления или уточнения, если riotIdGameName не всегда совпадает с PLAYER_IDS ключами
# Пока оставим PLAYER_IDS как есть, но нормализация будет применяться к именам из JSON
ROSTER_RIOT_NAME_TO_GRID_ID = {
    "Aytekn": "26433",
    "Pallet": "25262",
    "Tsiperakos": "25266",
    "Kenal": "20958",
    "CENTU": "21922"
}
# Определяем роль для каждого ID (если нужно будет для валидации, пока роли берутся по позиции)
PLAYER_ROLES_BY_ID = {
    "26433": "TOP",
    "25262": "JUNGLE",
    "25266": "MIDDLE",
    "20958": "BOTTOM",
    "21922": "UTILITY"
}

SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed" # Имя Google таблицы
SCRIMS_WORKSHEET_NAME = "Scrims" # Имя листа внутри таблицы
API_REQUEST_DELAY = 0.5 # Уменьшена задержка, но следите за 429 ошибками

# Стандартный порядок ролей для ЗАПИСИ в таблицу
ROLE_ORDER_FOR_SHEET = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]

# --- НОВЫЙ ЗАГОЛОВОК ТАБЛИЦЫ (без драфт-пиков, баны по ID) ---
SCRIMS_HEADER = [
    # Основная информация
    "Date", "Patch", "Blue Team Name", "Red Team Name",
    "Duration", "Result", # Нашей команды. Duration игры нужен для расчета /min
    # Баны (ID)
    "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    # Данные по игрокам Синей команды
    "Blue_TOP_Player", "Blue_TOP_Champ", "Blue_TOP_K", "Blue_TOP_D", "Blue_TOP_A", "Blue_TOP_Dmg", "Blue_TOP_CS",
    "Blue_JGL_Player", "Blue_JGL_Champ", "Blue_JGL_K", "Blue_JGL_D", "Blue_JGL_A", "Blue_JGL_Dmg", "Blue_JGL_CS",
    "Blue_MID_Player", "Blue_MID_Champ", "Blue_MID_K", "Blue_MID_D", "Blue_MID_A", "Blue_MID_Dmg", "Blue_MID_CS",
    "Blue_BOT_Player", "Blue_BOT_Champ", "Blue_BOT_K", "Blue_BOT_D", "Blue_BOT_A", "Blue_BOT_Dmg", "Blue_BOT_CS",
    "Blue_SUP_Player", "Blue_SUP_Champ", "Blue_SUP_K", "Blue_SUP_D", "Blue_SUP_A", "Blue_SUP_Dmg", "Blue_SUP_CS",
    # Данные по игрокам Красной команды
    "Red_TOP_Player", "Red_TOP_Champ", "Red_TOP_K", "Red_TOP_D", "Red_TOP_A", "Red_TOP_Dmg", "Red_TOP_CS",
    "Red_JGL_Player", "Red_JGL_Champ", "Red_JGL_K", "Red_JGL_D", "Red_JGL_A", "Red_JGL_Dmg", "Red_JGL_CS",
    "Red_MID_Player", "Red_MID_Champ", "Red_MID_K", "Red_MID_D", "Red_MID_A", "Red_MID_Dmg", "Red_MID_CS",
    "Red_BOT_Player", "Red_BOT_Champ", "Red_BOT_K", "Red_BOT_D", "Red_BOT_A", "Red_BOT_Dmg", "Red_BOT_CS",
    "Red_SUP_Player", "Red_SUP_Champ", "Red_SUP_K", "Red_SUP_D", "Red_SUP_A", "Red_SUP_Dmg", "Red_SUP_CS",
    # ID игры для уникальности
    "Game ID"
]

# Порядок колонок для отображения в ИСТОРИИ МАТЧЕЙ
HISTORY_DISPLAY_ORDER = [
    "Date", "Patch", "Blue Team Name", "B Bans", "B Picks",
    "R Picks", "R Bans", "Red Team Name", "Result", "Duration" # Result и Duration теперь в конце
]

# --- НОВАЯ ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ---
def extract_team_tag(riot_id_game_name):
    """Пытается извлечь потенциальный тег команды (короткое слово в верхнем регистре в начале)."""
    if isinstance(riot_id_game_name, str) and ' ' in riot_id_game_name:
        parts = riot_id_game_name.split(' ', 1)
        tag = parts[0]
        # Простая эвристика: от 2 до 5 символов, все в верхнем регистре (допускаем цифры)
        if 2 <= len(tag) <= 5 and tag.isupper() and tag.isalnum():
             # Исключаем общие обозначения ролей, чтобы случайно не взять их за тег
             common_roles = {"MID", "TOP", "BOT", "JGL", "JUG", "JG", "JUN", "ADC", "SUP", "SPT"}
             if tag.upper() not in common_roles:
                  return tag
    return None # Возвращаем None, если тег не найден
# --- КОНЕЦ НОВОЙ ФУНКЦИИ ---
# --- DDRagon Helper Functions (Без изменений) ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1" # Fallback к известной версии
    except Exception: return "14.14.1" # Fallback к известной версии

@st.cache_data
@st.cache_data
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    """Нормализует имя чемпиона для использования в URL Data Dragon,
       включая ручные исправления и сохранение нужного регистра."""
    if not champ or champ == "N/A":
        return None

    # --- СЛОВАРЬ РУЧНЫХ ИСПРАВЛЕНИЙ / СОХРАНЕНИЯ РЕГИСТРА ---
    # Сюда добавляем случаи, где API/JSON имя отличается от ddragon имени,
    # или где нужно сохранить специфический регистр (CamelCase).
    # Формат: "Имя_из_API_или_JSON": "Имя_для_ddragon_URL"
    champion_name_overrides = {
        # Основные исключения
        "Nunu & Willump": "Nunu",
        "Wukong": "MonkeyKing",
        "Renata Glasc": "Renata",
        "K'Sante": "KSante",
        "LeBlanc": "Leblanc",
        "Miss Fortune": "MissFortune",
        "Jarvan IV": "JarvanIV",
        "Twisted Fate": "TwistedFate",
        "Dr. Mundo": "DrMundo",
        "Xin Zhao": "XinZhao",
        # Варианты для сохранения регистра или исправления возможных входных данных
        "MonkeyKing": "MonkeyKing",
        "KSante": "KSante",
        "Leblanc": "Leblanc",
        "MissFortune": "MissFortune",
        "Jarvaniv": "JarvanIV",
        "JarvanIV": "JarvanIV",
        "Twistedfate": "TwistedFate",
        "TwistedFate": "TwistedFate",
        "DrMundo": "DrMundo",
        "Xinzhao": "XinZhao", # Если вдруг придет в нижнем регистре
        "XinZhao": "XinZhao",
        # Добавляйте другие по мере необходимости
        # "Fiddlesticks": "Fiddlesticks", # Обычно не требует изменений
    }
    # --- КОНЕЦ СЛОВАРЯ ---

    # 1. Сначала проверяем точное совпадение в словаре
    if champ in champion_name_overrides:
        return champion_name_overrides[champ]

    # 2. Затем проверяем совпадение без учета регистра
    champ_lower = champ.lower()
    for k, v in champion_name_overrides.items():
        # Сравниваем в нижнем регистре
        if k.lower() == champ_lower:
            return v # Возвращаем значение из словаря (с правильным регистром ddragon)

    # 3. Если нет в словаре, применяем общую логику очистки
    # Удаляем апострофы, точки, пробелы и т.д.
    name_clean = ''.join(c for c in champ if c.isalnum())

    # 4. Стандартные исключения ddragon для имен с апострофами/т.п. ПОСЛЕ очистки
    # (на случай, если они не попали в overrides)
    ddragon_cleaned_exceptions = {
        # очищенное_имя_в_нижнем_регистре : имя_для_ddragon
        "khazix": "Khazix",
        "chogath": "Chogath",
        "kaisa": "Kaisa",
        "velkoz": "Velkoz",
        "reksai": "Reksai",
    }
    if name_clean.lower() in ddragon_cleaned_exceptions:
        return ddragon_cleaned_exceptions[name_clean.lower()]

    # 5. Для всех остальных имен возвращаем очищенное имя "как есть".
    # Data Dragon обычно чувствителен к регистру (например, 'XinZhao', а не 'Xinzhao').
    # Простая капитализация первой буквы (как было раньше) может быть неверной.
    # Поэтому лучше вернуть очищенное имя - если оно совпадает с ожидаемым ddragon, иконка загрузится.
    return name_clean if name_clean else None
    
def get_champion_icon_html(champion, width=25, height=25):
    patch_version = get_latest_patch_version(); norm = normalize_champion_name_for_ddragon(champion)
    if norm: url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{norm}.png"; return f'<img src="{url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return ""
# --- НОВАЯ ФУНКЦИЯ для получения данных чемпионов с ddragon ---
@st.cache_data(ttl=86400) # Кэшируем на сутки
def get_champion_data():
    """Загружает данные чемпионов с Data Dragon и возвращает словарь {id: name}."""
    patch_version = get_latest_patch_version()
    url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/data/en_US/champion.json"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()['data']
        # Создаем словарь: ключ - ID чемпиона (строка), значение - имя чемпиона
        # Riot API использует ID как строки (например, в participant['championId']), а ddragon как числа/строки в key. Приводим к строке.
        champion_id_to_name = {champ_info['key']: champ_info['name'] for champ_name, champ_info in data.items()}
        return champion_id_to_name
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to fetch champion data from ddragon: {e}")
        return {}
    except Exception as e:
        st.error(f"Error processing champion data: {e}")
        return {}

# --- Добавьте вызов этой функции где-нибудь в начале scrims_page или глобально ---
# champion_id_map = get_champion_data() # Вызывать ОДИН РАЗ при загрузке страницы/скрипта
def color_win_rate_scrims(value):
    try:
        v = float(value)
        if 0 <= v < 48:
            return f'<span style="color:#FF7F7F; font-weight:bold;">{v:.1f}%</span>'
        elif 48 <= v <= 52:
            return f'<span style="color:#FFD700; font-weight:bold;">{v:.1f}%</span>'
        elif v > 52:
            return f'<span style="color:#90EE90; font-weight:bold;">{v:.1f}%</span>'
        else:
            return f'{value}' # Fallback для неожиданных значений
    except (ValueError, TypeError):
        return f'{value}' # Возврат исходного значения, если не число

# --- Google Sheets Setup (Без изменений) ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation (Адаптировано под новый SCRIMS_HEADER) ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    """
    Проверяет существование листа и его заголовок.
    Создает лист с заголовком SCRIMS_HEADER, если он не найден или не соответствует.
    """
    try:
        wks = spreadsheet.worksheet(name)
        # Проверка и обновление заголовка существующего листа
        try:
            current_header = wks.row_values(1)
            # Сравниваем с новым заголовком
            if current_header != SCRIMS_HEADER:
                st.warning(f"Worksheet '{name}' header mismatch or outdated. "
                           f"Expected {len(SCRIMS_HEADER)} columns based on current script. Found {len(current_header)}. "
                           f"Attempting to clear and recreate sheet structure. BACKUP YOUR DATA FIRST IF NEEDED.")
                # Удаляем старый лист и создаем новый (РИСК ПОТЕРИ ДАННЫХ!)
                # spreadsheet.del_worksheet(wks) # Закомментировано для безопасности
                # raise gspread.exceptions.WorksheetNotFound # Имитируем отсутствие, чтобы пересоздать ниже
                # Вместо удаления, можно просто предупредить и остановить обновление
                st.error("Header mismatch detected. Halting update to prevent data corruption. "
                         "Please manually align the sheet header with SCRIMS_HEADER in the script or delete the sheet.")
                return None # Останавливаем выполнение
        except Exception as header_exc:
             st.warning(f"Could not verify header for worksheet '{name}': {header_exc}")
             # Продолжаем с осторожностью

    except gspread.exceptions.WorksheetNotFound:
        try:
            cols_needed = len(SCRIMS_HEADER)
            # Увеличиваем количество строк, если нужно
            wks = spreadsheet.add_worksheet(title=name, rows=2000, cols=max(cols_needed, 26))
            wks.append_row(SCRIMS_HEADER, value_input_option='USER_ENTERED')
            # Форматируем заголовок жирным
            wks.format(f'A1:{gspread.utils.rowcol_to_a1(1, cols_needed)}', {'textFormat': {'bold': True}})
            st.success(f"Created worksheet '{name}' with the required structure.")
        except Exception as e:
            st.error(f"Error creating worksheet '{name}': {e}")
            return None
    except Exception as e:
        st.error(f"Error accessing worksheet '{name}': {e}")
        return None
    return wks

# --- Функции для работы с GRID API ---

# Вспомогательная функция для вывода логов
def log_message(message, logs_list):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    log_entry = f"{timestamp} :: {message}"
    # print(log_entry) # Опционально: вывод в консоль сервера
    if logs_list is not None:
        logs_list.append(log_entry)

# Функция для выполнения GraphQL запросов (адаптировано из lol_basic_parser.py)
# Убедитесь, что ваша функция начинается именно так:
def post_graphql_request(query_string, variables, endpoint, api_key, logs_list, retries=3, initial_delay=1):
    # ... остальной код функции ...
    """ Отправляет GraphQL POST запрос с обработкой ошибок и повторами """
    headers = {"x-api-key": api_key, "Content-Type": "application/json"}
    # --- ИСПРАВЛЕНО: Формируем payload с query и variables ---
    payload = json.dumps({"query": query_string, "variables": variables})
    url = f"{GRID_BASE_URL}{endpoint}"
    last_exception = None

    for attempt in range(retries):
        try:
            log_message(f"GraphQL POST to {endpoint} (Attempt {attempt + 1}/{retries})", logs_list)
            # --- ИСПРАВЛЕНО: Передаем исправленный payload ---
            response = requests.post(url, headers=headers, data=payload, timeout=20) # Увеличен таймаут
            # --- КОНЕЦ ИСПРАВЛЕНИЙ в этой функции ---
            response.raise_for_status() # Проверка на HTTP ошибки (4xx, 5xx)

            response_data = response.json()
            # Проверка на GraphQL ошибки в ответе
            if "errors" in response_data and response_data["errors"]:
                error_msg = response_data["errors"][0].get("message", "Unknown GraphQL error")
                # Расширенное логирование ошибки GraphQL
                log_message(f"GraphQL Error in response: {json.dumps(response_data['errors'])}", logs_list)
                # Проверка на критические ошибки аутентификации/авторизации
                if "UNAUTHENTICATED" in error_msg or "UNAUTHORIZED" in error_msg or "forbidden" in error_msg.lower():
                     st.error(f"GraphQL Auth/Permission Error: {error_msg}. Check API Key/Permissions.")
                     return None # Нет смысла повторять
                last_exception = Exception(f"GraphQL Error: {error_msg}")
                # Задержка перед повтором
                time.sleep(initial_delay * (2 ** attempt))
                continue # Повторяем попытку

            log_message(f"GraphQL POST successful.", logs_list)
            return response_data.get("data")

        except requests.exceptions.HTTPError as http_err:
            log_message(f"HTTP error on attempt {attempt + 1}: {http_err}", logs_list)
            last_exception = http_err
            if response is not None: # Проверка, что response был получен перед проверкой status_code
                if response.status_code == 429: # Rate limit
                    retry_after = int(response.headers.get("Retry-After", initial_delay * (2 ** attempt)))
                    log_message(f"Rate limited (429). Retrying after {retry_after} seconds.", logs_list)
                    st.toast(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                elif response.status_code in [401, 403]:
                    st.error(f"Authorization error ({response.status_code}). Check API Key/Permissions.")
                    return None # Нет смысла повторять
                elif response.status_code == 400:
                     # Логируем тело ответа при 400 Bad Request для диагностики
                     try:
                         error_details = response.json()
                         log_message(f"Bad Request (400) details: {json.dumps(error_details)}", logs_list)
                     except json.JSONDecodeError:
                         log_message(f"Bad Request (400), could not decode JSON response body: {response.text[:500]}", logs_list)
                     last_exception = http_err # Сохраняем ошибку
                     # Часто 400 не исправить повтором, выходим из цикла
                     break
            # Другие HTTP ошибки - делаем задержку и повторяем (особенно 5xx)
            if response is None or 500 <= response.status_code < 600:
                 time.sleep(initial_delay * (2 ** attempt))
            else:
                 # Для других клиентских ошибок повтор может не помочь
                 break


        except requests.exceptions.RequestException as req_err:
            log_message(f"Request exception on attempt {attempt + 1}: {req_err}", logs_list)
            last_exception = req_err
            time.sleep(initial_delay * (2 ** attempt))

        except json.JSONDecodeError as json_err:
             log_message(f"JSON decode error on attempt {attempt+1}: {json_err}. Response text: {response.text[:200] if response else 'No response'}...", logs_list)
             last_exception = json_err
             time.sleep(initial_delay * (2 ** attempt))

        except Exception as e:
            # Ловим другие возможные ошибки
            import traceback
            log_message(f"Unexpected error in post_graphql_request attempt {attempt + 1}: {e}\n{traceback.format_exc()}", logs_list)
            last_exception = e
            time.sleep(initial_delay * (2 ** attempt))


    log_message(f"GraphQL request failed after {retries} attempts. Last error: {last_exception}", logs_list)
    # Не выводим ошибку в st.error здесь повторно, если она уже была выведена (напр. 401/403)
    if not isinstance(last_exception, requests.exceptions.HTTPError) or (last_exception.response.status_code not in [401, 403]):
        st.error(f"GraphQL request failed: {last_exception}")
    return None


# Функция для выполнения REST GET запросов (адаптировано из lol_basic_parser.py)
def get_rest_request(endpoint, api_key, logs_list, retries=5, initial_delay=2, expected_type='json'):
    """ Отправляет REST GET запрос с обработкой ошибок и повторами """
    headers = {"x-api-key": api_key}
    if expected_type == 'json':
        headers['Accept'] = 'application/json'
    # Добавить другие Accept типы если нужно (zip, jsonl, etc.)

    url = f"{GRID_BASE_URL}{endpoint}"
    last_exception = None

    for attempt in range(retries):
        try:
            log_message(f"REST GET from {endpoint} (Attempt {attempt + 1}/{retries})", logs_list)
            response = requests.get(url, headers=headers, timeout=15) # Таймаут 15 сек

            if response.status_code == 200:
                log_message(f"REST GET successful.", logs_list)
                if expected_type == 'json':
                    try:
                        return response.json()
                    except json.JSONDecodeError as json_err:
                        log_message(f"JSON decode error for 200 OK: {json_err}. Response text: {response.text[:200]}...", logs_list)
                        last_exception = json_err
                        # Не повторяем попытку при ошибке декодирования успешного ответа
                        break
                else:
                    # Вернуть контент для других типов (zip, etc.)
                    return response.content

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", initial_delay * (2 ** attempt)))
                log_message(f"Rate limited (429). Retrying after {retry_after} seconds.", logs_list)
                st.toast(f"Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                last_exception = requests.exceptions.HTTPError(f"429 Too Many Requests")
                continue

            elif response.status_code == 404:
                log_message(f"Resource not found (404) at {endpoint}", logs_list)
                last_exception = requests.exceptions.HTTPError(f"404 Not Found")
                # Часто 404 означает, что данных просто нет, не повторяем
                return None

            elif response.status_code in [401, 403]:
                 error_msg = f"Authorization error ({response.status_code}) for {endpoint}. Check API Key/Permissions."
                 log_message(error_msg, logs_list)
                 st.error(error_msg)
                 last_exception = requests.exceptions.HTTPError(f"{response.status_code} Unauthorized/Forbidden")
                 # Нет смысла повторять
                 return None
            else:
                 # Другие HTTP ошибки
                 response.raise_for_status()

        except requests.exceptions.HTTPError as http_err:
            log_message(f"HTTP error on attempt {attempt + 1}: {http_err}", logs_list)
            last_exception = http_err
            # Задержка перед повтором для серверных ошибок 5xx
            if 500 <= response.status_code < 600:
                time.sleep(initial_delay * (2 ** attempt))
            else:
                 # Для других клиентских ошибок (кроме 429, 404, 401, 403) повтор может не помочь
                 break

        except requests.exceptions.RequestException as req_err:
            log_message(f"Request exception on attempt {attempt + 1}: {req_err}", logs_list)
            last_exception = req_err
            time.sleep(initial_delay * (2 ** attempt))

        except Exception as e:
            log_message(f"Unexpected error on attempt {attempt + 1}: {e}", logs_list)
            last_exception = e
            time.sleep(initial_delay * (2 ** attempt))

    log_message(f"REST GET request failed after {retries} attempts for {endpoint}. Last error: {last_exception}", logs_list)
    st.error(f"REST GET request failed for {endpoint}: {last_exception}")
    return None

# Получение списка серий (без изменений, использует post_graphql_request)
# @st.cache_data(ttl=300) # Кэшируем на 5 минут
def get_all_series(api_key, logs_list):
    """ Получает список ID и дат начала LoL скримов за последние 14 дней """
    # Строка GraphQL запроса
    query_string = """
        query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
          allSeries(
            filter: $filter, first: $first, after: $after,
            orderBy: $orderBy, orderDirection: $orderDirection
          ) {
            totalCount,
            pageInfo { hasNextPage, endCursor },
            edges {
              node {
                id                 # ID Серии (s_id)
                startTimeScheduled
              }
            }
          }
        }
    """
    # Берем скримы за последние 14 дней (можно увеличить)
    start_thresh = (datetime.now(timezone.utc) - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Словарь с переменными для запроса
    variables_template = {
        "filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_thresh}},
        "first": 50,
        "orderBy": "StartTimeScheduled",
        "orderDirection": "DESC"
    }

    all_nodes = []
    cursor = None
    page_num = 1
    max_pages = 20 # Ограничение на всякий случай

    while page_num <= max_pages:
        current_variables = variables_template.copy()
        # Добавляем курсор для пагинации, если он есть
        if cursor:
            current_variables["after"] = cursor
        else:
            # Удаляем 'after', если курсора нет (для первого запроса)
            current_variables.pop("after", None)


        # --- ИСПРАВЛЕНО: Передаем query_string и current_variables отдельно ---
        response_data = post_graphql_request(
            query_string=query_string,
            variables=current_variables,
            endpoint="central-data/graphql",
            api_key=api_key,
            logs_list=logs_list
        )
        # --- КОНЕЦ ИСПРАВЛЕНИЙ в этой функции ---

        if not response_data:
            log_message(f"Failed to fetch series page {page_num}. Stopping.", logs_list)
            break # Прерываем пагинацию при ошибке

        series_data = response_data.get("allSeries", {})
        edges = series_data.get("edges", [])
        nodes = [edge["node"] for edge in edges if "node" in edge]
        all_nodes.extend(nodes)

        page_info = series_data.get("pageInfo", {})
        has_next_page = page_info.get("hasNextPage", False)
        cursor = page_info.get("endCursor")

        log_message(f"Fetched page {page_num}, {len(nodes)} series. Has next: {has_next_page}", logs_list)

        if not has_next_page or not cursor:
            break # Выходим, если нет следующей страницы или курсора

        page_num += 1
        time.sleep(API_REQUEST_DELAY) # Задержка между страницами

    log_message(f"Finished fetching series. Total series found: {len(all_nodes)}", logs_list)
    return all_nodes

# НОВАЯ функция для получения списка игр в серии
def get_series_state(series_id, api_key, logs_list):
    """ Получает список игр (id, sequenceNumber) для заданной серии """
    query_template = """
        query GetSeriesGames($seriesId: ID!) {
            seriesState (
                id: $seriesId
            ) {
                id
                games {
                    id
                    sequenceNumber
                    # Можно добавить другие поля при необходимости, например 'started', 'finished'
                }
            }
        }
    """
    # Используем переменные GraphQL вместо форматирования строки
    variables = {"seriesId": series_id}
    endpoint = "live-data-feed/series-state/graphql" # Эндпоинт для Series State API

    # --- ИСПРАВЛЕНО: Передаем query_template, variables и logs_list ---
    response_data = post_graphql_request(
        query_string=query_template,
        variables=variables, # Передаем словарь переменных
        endpoint=endpoint,
        api_key=api_key,
        logs_list=logs_list # Передаем список логов
    )
    # --- КОНЕЦ ИСПРАВЛЕНИЙ в этой функции ---

    if response_data and response_data.get("seriesState") and "games" in response_data["seriesState"]:
        games = response_data["seriesState"]["games"]
        # Проверяем, что games это список (может быть None, если серия найдена, но игр нет)
        if games is None:
            log_message(f"Series {series_id} found, but contains no games (games is null).", logs_list)
            return []
        log_message(f"Found {len(games)} games in series {series_id}", logs_list)
        return games
    elif response_data and not response_data.get("seriesState"):
         log_message(f"No seriesState found in response for series {series_id}. The series might not exist or is inaccessible.", logs_list)
         return []
    else:
        # Ошибка была залогирована внутри post_graphql_request
        log_message(f"Could not find games for series {series_id} (seriesState request failed or returned unexpected data).", logs_list)
        return [] # Возвращаем пустой список при ошибке

# НОВАЯ функция для скачивания Riot Summary данных
def download_riot_summary_data(series_id, sequence_number, api_key, logs_list):
    """ Скачивает Riot Summary JSON для конкретной игры """
    endpoint = f"file-download/end-state/riot/series/{series_id}/games/{sequence_number}/summary"
    summary_data = get_rest_request(endpoint, api_key, logs_list, expected_type='json')
    if summary_data:
        log_message(f"Successfully downloaded summary for s:{series_id} g:{sequence_number}", logs_list)
    else:
        log_message(f"Failed to download summary for s:{series_id} g:{sequence_number}", logs_list)
    return summary_data

# НОВАЯ функция нормализации имен игроков
def normalize_player_name(riot_id_game_name):
    """ Удаляет известные командные префиксы из игрового имени Riot ID """
    if isinstance(riot_id_game_name, str):
        if riot_id_game_name.startswith("GSMC "):
            return riot_id_game_name[5:].strip()
        # Добавьте сюда другие префиксы, если они есть, например:
        # elif riot_id_game_name.startswith("TEAM2 "):
        #     return riot_id_game_name[6:].strip()
    return riot_id_game_name # Возвращаем как есть, если префикс не найден или тип не строка

# --- ОСНОВНАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ ДАННЫХ (Переписана) ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ОСНОВНАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ ДАННЫХ (Добавлен парсинг патча) ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ОСНОВНАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ ДАННЫХ (Добавлено определение имени оппонента) ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ОСНОВНАЯ ФУНКЦИЯ ОБНОВЛЕНИЯ ДАННЫХ (Сохраняет KDA/Dmg/CS/Player) ---
def update_scrims_data(worksheet, series_list, api_key, debug_logs, progress_bar):
    """
    Скачивает Riot Summary JSON, парсит его, включая KDA/Dmg/CS/PlayerName,
    и добавляет расширенную строку в таблицу.
    """
    if not worksheet: log_message("Update Error: Invalid Worksheet.", debug_logs); st.error("Invalid Worksheet."); return False
    if not series_list: log_message("No series to process.", debug_logs); st.info("No series to process."); return False

    try:
        existing_data = worksheet.get_all_values()
        # Индекс колонки Game ID теперь другой! Найдем его по имени.
        game_id_col_index = -1
        if len(existing_data) > 0 and SCRIMS_HEADER[-1] == "Game ID": # Проверяем, что Game ID последний
             game_id_col_index = len(SCRIMS_HEADER) - 1
        # Если не нашли по имени или порядку, придется искать или использовать старый индекс (рискованно)
        if game_id_col_index == -1:
            log_message("Warning: Could not reliably determine Game ID column index based on new header. Duplicate check might fail.", debug_logs)
            # Пытаемся найти по имени (менее надежно, если заголовок не совпадает)
            try: game_id_col_index = SCRIMS_HEADER.index("Game ID")
            except ValueError: game_id_col_index = 1 # Возвращаемся к старому предположению, ОПАСНО

        existing_game_ids = set(row[game_id_col_index] for row in existing_data[1:] if len(row) > game_id_col_index and row[game_id_col_index]) if len(existing_data) > 1 else set()
        log_message(f"Found {len(existing_game_ids)} existing game IDs in the sheet (Column Index: {game_id_col_index}).", debug_logs)

    except Exception as e: log_message(f"Error reading sheet: {e}", debug_logs); st.error(f"Error reading sheet: {e}"); return False

    new_rows = []; processed_game_count = 0; skipped_existing_count = 0
    skipped_state_fail_count = 0; skipped_summary_fail_count = 0; skipped_parsing_fail_count = 0
    total_series_to_process = len(series_list)
    roles_in_order = ["TOP", "JGL", "MID", "BOT", "SUP"]
    role_abbr_map = {"TOP": "TOP", "JGL": "JGL", "MID": "MID", "BOT": "BOT", "SUP": "SUP"} # Маппинг для ключей словаря

    for i, series_summary in enumerate(series_list): # Цикл по сериям
        series_id = series_summary.get("id"); 
        if not series_id: continue
        prog = (i + 1) / total_series_to_process
        try: progress_bar.progress(prog, text=f"Series {i+1}/{total_series_to_process} ({series_id})")
        except Exception: pass

        games_in_series = get_series_state(series_id, api_key, debug_logs)
        if not games_in_series: skipped_state_fail_count += 1; time.sleep(API_REQUEST_DELAY / 2); continue

        for game_info in games_in_series: # Цикл по играм
            game_id = game_info.get("id"); sequence_number = game_info.get("sequenceNumber")
            if not game_id or sequence_number is None: continue
            if game_id in existing_game_ids: skipped_existing_count += 1; continue

            log_message(f"Processing G:{game_id} (S:{series_id}, Seq:{sequence_number})", debug_logs)
            summary_data = download_riot_summary_data(series_id, sequence_number, api_key, debug_logs)
            if not summary_data: skipped_summary_fail_count += 1; time.sleep(API_REQUEST_DELAY); continue

            try: # Парсинг summary_data
                participants = summary_data.get("participants", []); teams_data = summary_data.get("teams", [])
                game_duration_sec = summary_data.get("gameDuration", 0); game_creation_timestamp = summary_data.get("gameCreation")
                game_version = summary_data.get("gameVersion", "N/A"); patch_str = "N/A"
                if game_version!="N/A": parts=game_version.split('.'); patch_str=f"{parts[0]}.{parts[1]}" if len(parts)>=2 else "N/A"

                if not participants or len(participants) != 10 or not teams_data or len(teams_data) != 2:
                    log_message(f"Skip G:{game_id}: Invalid participants/teams count.", debug_logs); skipped_parsing_fail_count += 1; continue

                our_side = None; our_team_id = None # Определение нашей команды
                for idx, p in enumerate(participants):
                    normalized_name = normalize_player_name(p.get("riotIdGameName"))
                    if normalized_name in ROSTER_RIOT_NAME_TO_GRID_ID:
                        current_side='blue' if idx<5 else 'red'; current_team_id=100 if idx<5 else 200
                        if our_side is None: our_side=current_side; our_team_id=current_team_id
                        elif our_side!=current_side: log_message(f"Warn: Players on both sides! G:{game_id}", debug_logs)
                if our_side is None: log_message(f"Skip G:{game_id}: Roster players not found.", debug_logs); skipped_parsing_fail_count += 1; continue

                opponent_team_name = "Opponent"; opponent_tags = defaultdict(int) # Определение имени оппонента
                opponent_indices = range(5, 10) if our_side=='blue' else range(0, 5)
                for idx in opponent_indices:
                    if idx<len(participants): tag=extract_team_tag(participants[idx].get("riotIdGameName")); tag and opponent_tags.update({tag: opponent_tags[tag] + 1}) # Используем update для добавления/увеличения
                if opponent_tags: sorted_tags=sorted(opponent_tags.items(), key=lambda item: item[1], reverse=True); opponent_team_name=sorted_tags[0][0] if sorted_tags[0][1]>=3 else "Opponent"
                blue_team_name = TEAM_NAME if our_side == 'blue' else opponent_team_name; red_team_name = TEAM_NAME if our_side == 'red' else opponent_team_name

                result = "N/A" # Определение результата
                for team in teams_data: result = "Win" 
                    if team.get("win") else "Loss" 
                if team.get("teamId")==our_team_id else result; 
                break 
                if result!="N/A" else None

                blue_bans = ["N/A"]*5; red_bans = ["N/A"]*5 # Баны
                for team in teams_data:
                    target_bans = blue_bans if team.get("teamId")==100 else red_bans; bans_list = sorted(team.get("bans",[]), key=lambda x: x.get('pickTurn',99))
                    for i, ban in enumerate(bans_list[:5]): target_bans[i] = str(c_id) if (c_id := ban.get("championId", -1)) != -1 else "N/A"

                date_str = "N/A" # Дата и длительность
                if game_creation_timestamp: try: date_str=datetime.fromtimestamp(game_creation_timestamp/1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S") except: pass
                duration_str = "N/A"; duration_float_min = 0.0
                if game_duration_sec > 0: minutes, seconds = divmod(int(game_duration_sec), 60); duration_str = f"{minutes}:{seconds:02d}"; duration_float_min = game_duration_sec / 60.0

                # --- СОЗДАЕМ СЛОВАРЬ ДЛЯ СТРОКИ ---
                row_dict = {hdr: "N/A" for hdr in SCRIMS_HEADER} # Заполняем N/A по умолчанию
                row_dict.update({
                    "Date": date_str, "Patch": patch_str, "Blue Team Name": blue_team_name, "Red Team Name": red_team_name,
                    "Duration": duration_str, "Result": result, "Game ID": game_id
                })
                for i in range(5): row_dict[f"Blue Ban {i+1} ID"] = blue_bans[i]; row_dict[f"Red Ban {i+1} ID"] = red_bans[i]

                # --- ИЗВЛЕКАЕМ ДАННЫЕ ИГРОКОВ ---
                for idx, p in enumerate(participants):
                    role_name = roles_in_order[idx % 5] # TOP, JGL, MID, BOT, SUP
                    side_prefix = "Blue" if idx < 5 else "Red"
                    player_col_prefix = f"{side_prefix}_{role_abbr_map[role_name]}" # Blue_TOP, Red_JGL etc.

                    player_name = normalize_player_name(p.get("riotIdGameName")) or "Unknown"
                    champ_name = p.get("championName", "N/A")
                    kills = p.get('kills', 0)
                    deaths = p.get('deaths', 0)
                    assists = p.get('assists', 0)
                    damage = p.get('totalDamageDealtToChampions', 0)
                    cs = p.get('totalMinionsKilled', 0) + p.get('neutralMinionsKilled', 0)

                    row_dict[f"{player_col_prefix}_Player"] = player_name
                    row_dict[f"{player_col_prefix}_Champ"] = champ_name
                    row_dict[f"{player_col_prefix}_K"] = kills
                    row_dict[f"{player_col_prefix}_D"] = deaths
                    row_dict[f"{player_col_prefix}_A"] = assists
                    row_dict[f"{player_col_prefix}_Dmg"] = damage
                    row_dict[f"{player_col_prefix}_CS"] = cs

                # Преобразуем словарь в список в порядке заголовка
                new_row_data = [row_dict.get(hdr, "N/A") for hdr in SCRIMS_HEADER]
                new_rows.append(new_row_data)
                existing_game_ids.add(game_id); processed_game_count += 1

            except Exception as e: log_message(f"Parse fail G:{game_id}: {e}", debug_logs); import traceback; log_message(traceback.format_exc(), debug_logs); skipped_parsing_fail_count += 1; continue
            time.sleep(API_REQUEST_DELAY / 2) # Задержка между играми
        time.sleep(API_REQUEST_DELAY) # Задержка между сериями
    # --- Конец циклов ---

    # --- Вывод статистики и обновление таблицы ---
    try: progress_bar.progress(1.0, text="Update complete. Finalizing...")
    except: pass
    summary = [ # Формирование summary
        f"\n--- Update Summary ---", f"Series: {total_series_to_process}",
        f"Games Found: {processed_game_count + skipped_existing_count + skipped_summary_fail_count + skipped_parsing_fail_count}",
        f"Skipped(Exists):{skipped_existing_count}", f"Skipped(State):{skipped_state_fail_count}",
        f"Skipped(Summ):{skipped_summary_fail_count}", f"Skipped(Parse):{skipped_parsing_fail_count}",
        f"Added: {len(new_rows)}"
    ]
    if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
    st.session_state.scrims_update_logs = st.session_state.scrims_update_logs[-100:] + debug_logs[-50:] + summary
    st.code("\n".join(summary), language=None)

    if new_rows: # Запись в таблицу
        try: worksheet.append_rows(new_rows, value_input_option='USER_ENTERED'); log_message(f"Appended {len(new_rows)} rows.", debug_logs); st.success(f"Added {len(new_rows)} rows."); return True
        except Exception as e: error_msg=f"Append rows error: {e}"; log_message(error_msg, debug_logs); st.error(error_msg); return False
    else: st.info("No new records to add."); return False
# --- Конец функции update_scrims_data ---
# --- Конец функции update_scrims_data ---
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ФУНКЦИЯ АГРЕГАЦИИ ДАННЫХ (Патч, Иконки банов, Порядок колонок) ---
# @st.cache_data(ttl=180)
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ФУНКЦИЯ АГРЕГАЦИИ ДАННЫХ (Суммирует KDA/Dmg/CS/Duration) ---
# @st.cache_data(ttl=180)
def aggregate_scrims_data(worksheet, time_filter, champion_id_map):
    """
    Агрегирует расширенные данные из Google Sheet.
    Суммирует K, D, A, Dmg, CS, Duration для расчета средних KDA, DPM, CSPM.
    """
    if not worksheet: st.error("Agg Err: Invalid worksheet."); return {}, {}, pd.DataFrame(), {}
    if not champion_id_map: st.warning("Agg Warn: Champion ID map unavailable.")

    blue_stats = {"wins": 0, "losses": 0, "total": 0}; red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    # НОВАЯ СТРУКТУРА: { player: { champ: {games, wins, k, d, a, dmg, cs, duration_sec} } }
    player_stats = defaultdict(lambda: defaultdict(lambda: {
        'games': 0, 'wins': 0, 'k': 0, 'd': 0, 'a': 0, 'dmg': 0, 'cs': 0, 'duration_sec': 0.0
    }))

    # Фильтр по времени (без изменений)
    now_utc = datetime.now(timezone.utc); time_threshold = None
    if time_filter != "All Time":
        weeks_map={"1 Week":1,"2 Weeks":2,"3 Weeks":3,"4 Weeks":4}; days_map={"3 Days":3,"10 Days":10,"2 Months":60}
        if time_filter in weeks_map: time_threshold = now_utc - timedelta(weeks=weeks_map[time_filter])
        elif time_filter in days_map: time_threshold = now_utc - timedelta(days=days_map[time_filter])

    try: data = worksheet.get_all_values()
    except Exception as e: st.error(f"Read error agg: {e}"); return {}, {}, pd.DataFrame(), {}
    if len(data) <= 1: st.info("No data in sheet."); return {}, {}, pd.DataFrame(), {}

    header = data[0]
    # ВАЖНО: Убедитесь, что SCRIMS_HEADER в коде СООТВЕТСТВУЕТ реальному заголовку в таблице
    if header != SCRIMS_HEADER: st.error(f"Header mismatch agg."); return {}, {}, pd.DataFrame(), {}
    try: idx_map = {name: i for i, name in enumerate(header)}
    except Exception as e: st.error(f"Map creation fail: {e}"); return {}, {}, pd.DataFrame(), {}

    rows_processed_after_filter = 0
    relevant_player_names = set(ROSTER_RIOT_NAME_TO_GRID_ID.keys())
    ROLE_ORDER_FOR_SHEET = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}

    for row_index, row in enumerate(data[1:], start=2): # Обработка строк
        if len(row) < len(header): continue # Пропускаем короткие строки
        try:
            date_str = row[idx_map["Date"]]; passes_time_filter = True # Фильтр времени
            if time_threshold and date_str != "N/A":
                try: date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError: passes_time_filter = False
                else:
                    if date_obj < time_threshold: passes_time_filter = False
            if not passes_time_filter: continue
            rows_processed_after_filter += 1

            blue_team_name = row[idx_map["Blue Team Name"]]; red_team_name = row[idx_map["Red Team Name"]]
            result_our_team = row[idx_map["Result"]]
            is_our_blue = (blue_team_name == TEAM_NAME); is_our_red = (red_team_name == TEAM_NAME)
            if not is_our_blue and not is_our_red: continue

            # Статистика сторон (без изменений)
            if is_our_blue: blue_stats["total"] += 1; blue_stats["wins"] += (result_our_team == "Win"); blue_stats["losses"] += (result_our_team == "Loss")
            else: red_stats["total"] += 1; red_stats["wins"] += (result_our_team == "Win"); red_stats["losses"] += (result_our_team == "Loss")

            # --- СУММИРУЕМ СТАТИСТИКУ ИГРОКОВ ---
            our_side_prefix = "Blue" if is_our_blue else "Red"
            is_win = (result_our_team == "Win")
            # Получаем длительность игры в секундах для расчета /min
            duration_str = row[idx_map["Duration"]]; duration_sec = 0.0
            if duration_str and duration_str != "N/A":
                 try: mins, secs = map(int, duration_str.split(':')); duration_sec = float(mins * 60 + secs)
                 except: pass # Оставляем 0, если формат не M:SS

            for role in ROLE_ORDER_FOR_SHEET:
                role_abbr = role_to_abbr.get(role);
                if not role_abbr: continue
                player_col_prefix = f"{our_side_prefix}_{role_abbr}"

                # Извлекаем данные из колонок
                player_name = row[idx_map.get(f"{player_col_prefix}_Player", -1)]
                champion = row[idx_map.get(f"{player_col_prefix}_Champ", -1)]
                try: k = int(row[idx_map.get(f"{player_col_prefix}_K", -1)] or 0)
                except (ValueError, TypeError): k = 0
                try: d = int(row[idx_map.get(f"{player_col_prefix}_D", -1)] or 0)
                except (ValueError, TypeError): d = 0
                try: a = int(row[idx_map.get(f"{player_col_prefix}_A", -1)] or 0)
                except (ValueError, TypeError): a = 0
                try: dmg = int(row[idx_map.get(f"{player_col_prefix}_Dmg", -1)] or 0)
                except (ValueError, TypeError): dmg = 0
                try: cs = int(row[idx_map.get(f"{player_col_prefix}_CS", -1)] or 0)
                except (ValueError, TypeError): cs = 0

                # Проверяем, наш ли это игрок и есть ли чемпион
                if player_name in relevant_player_names and champion and champion != "N/A":
                    stats = player_stats[player_name][champion] # Получаем ссылку на словарь статы
                    stats['games'] += 1
                    if is_win: stats['wins'] += 1
                    stats['k'] += k
                    stats['d'] += d
                    stats['a'] += a
                    stats['dmg'] += dmg
                    stats['cs'] += cs
                    stats['duration_sec'] += duration_sec # Суммируем длительность игр на этом чемпе

            # --- Подготовка строки для истории матчей (без изменений) ---
            try:
                bb_icons = []; rb_icons = [] # Иконки банов
                if champion_id_map: # Генерируем иконки, только если карта есть
                    for i in range(1,6): ban_id=str(row[idx_map[f"Blue Ban {i} ID"]]); champ_name=champion_id_map.get(ban_id, f"ID:{ban_id}"); bb_icons.append(get_champion_icon_html(champ_name)) if ban_id not in ["-1","N/A"] else None
                    for i in range(1,6): ban_id=str(row[idx_map[f"Red Ban {i} ID"]]); champ_name=champion_id_map.get(ban_id, f"ID:{ban_id}"); rb_icons.append(get_champion_icon_html(champ_name)) if ban_id not in ["-1","N/A"] else None
                bb_html=" ".join(filter(None, bb_icons)); rb_html=" ".join(filter(None, rb_icons))

                bp_icons=[]; rp_icons=[] # Иконки пиков
                for role in ROLE_ORDER_FOR_SHEET:
                    b_champ=row[idx_map[f"Actual_Blue_{role_to_abbr[role]}"]]; r_champ=row[idx_map[f"Actual_Red_{role_to_abbr[role]}"]]
                    if b_champ not in ["N/A",None]: bp_icons.append(get_champion_icon_html(b_champ))
                    if r_champ not in ["N/A",None]: rp_icons.append(get_champion_icon_html(r_champ))
                bp_html=" ".join(bp_icons); rp_html=" ".join(rp_icons)

                patch_val = row[idx_map["Patch"]]; duration_val = row[idx_map["Duration"]] # Патч и длительность

                history_rows.append({ # Формируем словарь для истории
                    "Date": date_str, "Patch": patch_val, "Blue Team Name": blue_team_name,
                    "B Bans": bb_html, "B Picks": bp_html, "R Picks": rp_html,
                    "R Bans": rb_html, "Red Team Name": red_team_name, "Result": result_our_team,
                    "Duration": duration_val
                })
            except Exception as hist_err: st.warning(f"Hist err r.{row_index}: {hist_err}")

        except Exception as e_inner: st.warning(f"Proc err r.{row_index}: {e_inner}"); continue
    # --- Конец цикла ---

    if rows_processed_after_filter==0 and time_filter!="All Time": st.info(f"No data for filter: {time_filter}")
    elif not history_rows and rows_processed_after_filter>0: st.warning("Games processed, history empty.")

    df_hist = pd.DataFrame(history_rows) # Постобработка истории
    if not df_hist.empty:
        display_cols=HISTORY_DISPLAY_ORDER if 'HISTORY_DISPLAY_ORDER' in globals() else df_hist.columns.tolist()
        display_cols=[col for col in display_cols if col in df_hist.columns]; df_hist = df_hist[display_cols]
        try: df_hist['DT_temp']=pd.to_datetime(df_hist['Date'], errors='coerce', utc=True); df_hist.dropna(subset=['DT_temp'], inplace=True); df_hist=df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception as sort_ex: st.warning(f"Hist sort fail: {sort_ex}")

    # Конвертируем defaultdict в обычный dict для возврата
    final_player_stats = {player: dict(champions) for player, champions in player_stats.items()}
    # Сортировка чемпионов внутри каждого игрока (делаем в scrims_page)
    if not final_player_stats and rows_processed_after_filter > 0: st.info("Games processed, no player stats.")

    return blue_stats, red_stats, df_hist, final_player_stats # Возвращаем суммы
# --- Конец функции aggregate_scrims_data ---

# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ СТРАНИЦЫ SCRIMS (Адаптирована) ---

# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ СТРАНИЦЫ SCRIMS (Добавлены KDA/DPM/CSPM) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    if st.button("⬅️ Back to HLL Stats"): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()

    champion_id_map = get_champion_data() # Получаем карту чемпионов

    # Настройка Google Sheets (без изменений)
    client = setup_google_sheets();
    if not client: st.error("GSheets connection failed."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Sheet open error: {e}"); return
    # ВАЖНО: Перед первым запуском с новым SCRIMS_HEADER убедитесь, что лист пуст или имеет НОВЫЙ заголовок!
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: return # Ошибка доступа или несоответствия заголовка

    # Секция обновления данных (без изменений)
    with st.expander("Update Scrim Data from GRID API", expanded=False):
        logs = [];
        if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
        if st.button("Download & Update Scrims", key="update_scrims_btn"):
            st.session_state.scrims_update_logs = []; logs = st.session_state.scrims_update_logs
            log_message("Starting update...", logs)
            with st.spinner("Fetching series..."): series_list = get_all_series(GRID_API_KEY, logs)
            if series_list:
                st.info(f"Checking {len(series_list)} series...")
                progress_bar_placeholder=st.empty(); progress_bar=progress_bar_placeholder.progress(0,"Starting...")
                try: data_added = update_scrims_data(wks, series_list, GRID_API_KEY, logs, progress_bar)
                except Exception as e: log_message(f"Update error: {e}", logs); st.error(f"Update failed: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No recent series."); log_message("No series.", logs)
        if st.session_state.scrims_update_logs: st.text_area("Logs", "\n".join(st.session_state.scrims_update_logs), height=200, key="scrim_logs")

    st.divider()
    st.subheader("Scrim Performance Analysis")

    # Фильтр по времени (без изменений)
    time_f = st.selectbox("Filter:", ["All Time", "3 Days", "1 Week", "2 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # Агрегация данных (передаем карту чемпионов)
    blue_s, red_s, df_hist, player_stats_agg = aggregate_scrims_data(wks, time_f, champion_id_map)

    # Отображение общей статистики (без изменений)
    try:
        total_g = blue_s.get("total",0)+red_s.get("total",0); total_w = blue_s.get("wins",0)+red_s.get("wins",0); total_l = blue_s.get("losses",0)+red_s.get("losses",0)
        st.markdown(f"**Overall ({time_f})**"); co, cb, cr = st.columns(3)
        with co: wr=(total_w/total_g*100) if total_g>0 else 0; st.metric("Games", total_g); st.metric("Win Rate", f"{wr:.1f}%", f"{total_w}W-{total_l}L")
        with cb: bwr=(blue_s.get("wins",0)/blue_s.get("total",1)*100); st.metric("Blue WR",f"{bwr:.1f}%", f"{blue_s.get('wins',0)}W-{blue_s.get('losses',0)}L ({blue_s.get('total',0)}G)")
        with cr: rwr=(red_s.get("wins",0)/red_s.get("total",1)*100); st.metric("Red WR",f"{rwr:.1f}%", f"{red_s.get('wins',0)}W-{red_s.get('losses',0)}L ({red_s.get('total',0)}G)")
    except Exception as e: st.error(f"Err summary stats: {e}")

    st.divider()

    # --- ВКЛАДКИ ---
    tab1, tab2 = st.tabs(["📜 Match History (Games)", "📊 Player Champion Stats"])

    with tab1: # История матчей (без изменений в отображении)
        st.subheader(f"Game History ({time_f})")
        if df_hist is not None and not df_hist.empty:
            st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Стили как раньше
            st.markdown(df_hist.to_html(escape=False, index=False, classes='history-table', justify='center'), unsafe_allow_html=True)
        else: st.info(f"No history for: {time_f}.")

    with tab2: # Статистика игроков
        st.subheader(f"Player Champion Stats ({time_f})")
        if not player_stats_agg: st.info(f"No player stats for {time_f}.")
        else:
             player_order = [PLAYER_IDS[pid] for pid in ["26433","25262","25266","20958","21922"] if pid in PLAYER_IDS]
             player_cols = st.columns(len(player_order))

             for i, player_name in enumerate(player_order): # Цикл по игрокам
                 with player_cols[i]:
                     player_role = "Unknown" # Определение роли игрока
                     for pid, role in PLAYER_ROLES_BY_ID.items():
                          if PLAYER_IDS.get(pid) == player_name: player_role = role; break
                     st.markdown(f"**{player_name}** ({player_role})")

                     player_data = player_stats_agg.get(player_name, {}) # Получаем агрегированные данные (суммы)
                     stats_list = []

                     # Сортируем чемпионов по количеству игр
                     sorted_champs = sorted(player_data.items(), key=lambda item: item[1].get('games', 0), reverse=True)

                     if sorted_champs:
                         for champ, stats_sum in sorted_champs: # Итерируем по чемпионам
                             games = stats_sum.get('games', 0)
                             if games > 0:
                                 # --- ВЫЧИСЛЯЕМ СТАТИСТИКУ ---
                                 wins = stats_sum.get('wins', 0)
                                 k_sum = stats_sum.get('k', 0)
                                 d_sum = stats_sum.get('d', 0)
                                 a_sum = stats_sum.get('a', 0)
                                 dmg_sum = stats_sum.get('dmg', 0)
                                 cs_sum = stats_sum.get('cs', 0)
                                 duration_sum_sec = stats_sum.get('duration_sec', 0.0)

                                 win_rate = (wins / games * 100) if games > 0 else 0
                                 # KDA: (K+A)/max(1,D)
                                 kda = (k_sum + a_sum) / max(1, d_sum)
                                 # DPM: Total Dmg / (Total Duration in Min)
                                 dpm = (dmg_sum * 60) / max(1.0, duration_sum_sec) if duration_sum_sec > 0 else 0
                                 # CSPM: Total CS / (Total Duration in Min)
                                 cspm = (cs_sum * 60) / max(1.0, duration_sum_sec) if duration_sum_sec > 0 else 0
                                 # --- КОНЕЦ ВЫЧИСЛЕНИЙ ---

                                 stats_list.append({
                                     'Icon': get_champion_icon_html(champ, width=30, height=30),
                                     'Games': games,
                                     'WR%': win_rate,
                                     'KDA': f"{kda:.1f}", # Округляем до 1 знака
                                     'DPM': f"{dpm:.0f}", # Округляем до целого
                                     'CSPM': f"{cspm:.1f}" # Округляем до 1 знака
                                 })

                     if stats_list:
                         df_player = pd.DataFrame(stats_list)
                         # Применяем цвет к WR%
                         df_player['WR%'] = df_player['WR%'].apply(color_win_rate_scrims)
                         # Стиль таблицы статы игроков (без изменений)
                         st.markdown("""<style>...</style>""", unsafe_allow_html=True)
                         # --- ОТОБРАЖАЕМ НОВЫЕ КОЛОНКИ ---
                         st.markdown(
                              df_player.to_html(
                                  escape=False, index=False,
                                  columns=['Icon', 'Games', 'WR%', 'KDA', 'DPM', 'CSPM'], # Добавлены KDA, DPM, CSPM
                                  classes='player-stats', justify='center'),
                              unsafe_allow_html=True
                         )
                     else: st.caption("No stats.")

# --- Блок if __name__ == "__main__": (без изменений) ---
if __name__ == "__main__": pass
