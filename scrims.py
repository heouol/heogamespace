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
    "Date", "Patch", "Blue Team Name", "Red Team Name", # Заменили Game ID на Patch
    "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID", # Оставляем ID для хранения
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",   # Оставляем ID для хранения
    "Actual_Blue_TOP", "Actual_Blue_JGL", "Actual_Blue_MID", "Actual_Blue_BOT", "Actual_Blue_SUP",
    "Actual_Red_TOP", "Actual_Red_JGL", "Actual_Red_MID", "Actual_Red_BOT", "Actual_Red_SUP",
    "Duration", "Result" # Result и Duration пока здесь для записи
]

# Порядок колонок для отображения в ИСТОРИИ МАТЧЕЙ
HISTORY_DISPLAY_ORDER = [
    "Date", "Patch", "Blue Team Name", "B Bans", "B Picks",
    "R Picks", "R Bans", "Red Team Name", "Result", "Duration" # Result и Duration теперь в конце
]

# --- DDRagon Helper Functions (Без изменений) ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1" # Fallback к известной версии
    except Exception: return "14.14.1" # Fallback к известной версии

@st.cache_data
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    """Нормализует имя чемпиона для использования в URL Data Dragon,
       включая ручные исправления."""
    if not champ or champ == "N/A":
        return None

    # --- СЛОВАРЬ РУЧНЫХ ИСПРАВЛЕНИЙ ---
    # Сюда можно добавлять пары "Имя из API/JSON": "Имя для ddragon"
    champion_name_overrides = {
        "Nunu & Willump": "Nunu", # Пример из старой версии
        #"Wukong": "MonkeyKing",    # Пример из старой версии
        "Renata Glasc": "Renata", # Пример из старой версии
        #"K'Sante": "KSante",       # Пример из старой версии
        "LeBlanc": "Leblanc",      # API часто дает 'LeBlanc', ddragon хочет 'Leblanc'
        "MissFortune": "MissFortune",# API может дать 'MissFortune', ddragon хочет 'MissFortune'
        "Miss Fortune": "MissFortune", # На случай пробела
        # Добавляйте другие проблемные случаи сюда
        "JarvanIV": "JarvanIV", # Пример, если нужно убедиться в регистре
        "Fiddlesticks": "Fiddlesticks", # Для него обычно проблем нет
        "DrMundo": "DrMundo", # Для него обычно проблем нет
    }
    # --- КОНЕЦ СЛОВАРЯ ---

    # Сначала проверяем ручные исправления (с учетом регистра и без)
    if champ in champion_name_overrides:
        return champion_name_overrides[champ]
    # Проверка без учета регистра на всякий случай
    if champ.lower() in {k.lower(): v for k, v in champion_name_overrides.items()}:
         # Находим оригинальный ключ по lower() и возвращаем значение
         for k, v in champion_name_overrides.items():
              if k.lower() == champ.lower():
                   return v

    # Если ручных исправлений нет, применяем общую логику
    # Убираем пробелы, апострофы, точки и т.д., оставляем буквы и цифры
    name_clean = ''.join(c for c in champ if c.isalnum())

    # Важные стандартные замены ddragon (которые не покрываются простой очисткой)
    ddragon_exceptions = {
        "khazix": "Khazix",
        "chogath": "Chogath",
        "kaisa": "Kaisa",
        "velkoz": "Velkoz",
        "reksai": "Reksai",
    }
    if name_clean.lower() in ddragon_exceptions:
        return ddragon_exceptions[name_clean.lower()]

    # Для остальных - первая буква заглавная, остальные строчные (если не число)
    if name_clean:
        # Проверяем, нужно ли капитализировать первую букву (для стандартных имен)
        # Для имен типа 'Kaisa', 'Leblanc' это не нужно, они уже обработаны
        # Но для 'Ashe', 'Ezreal' и т.д. - нужно.
        # Простая капитализация может быть недостаточной для имен типа 'MissFortune'.
        # Используем простую капитализацию как fallback
        return name_clean[0].upper() + name_clean[1:].lower() # Простой вариант

    return None # Если имя совсем некорректное

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
def update_scrims_data(worksheet, series_list, api_key, debug_logs, progress_bar):
    """
    Обрабатывает список серий, получает список игр для каждой,
    скачивает Riot Summary JSON для каждой игры, парсит его и добавляет в таблицу.
    """
    if not worksheet:
        log_message("Update Error: Invalid Worksheet object provided.", debug_logs)
        st.error("Invalid Worksheet object.")
        return False
    if not series_list:
        log_message("No series found in the list to process.", debug_logs)
        st.info("No series found to process.")
        return False

    try:
        existing_data = worksheet.get_all_values()
        existing_game_ids = set(row[1] for row in existing_data[1:] if len(row) > 1 and row[1]) if len(existing_data) > 1 else set()
        log_message(f"Found {len(existing_game_ids)} existing game IDs in the sheet.", debug_logs)
    except Exception as e:
        log_message(f"Error reading existing sheet data: {e}", debug_logs)
        st.error(f"Error reading existing sheet data: {e}")
        return False

    new_rows = []
    processed_game_count = 0
    skipped_existing_count = 0
    skipped_state_fail_count = 0
    skipped_summary_fail_count = 0
    skipped_parsing_fail_count = 0
    total_series_to_process = len(series_list)

    # --- Цикл по сериям ---
    for i, series_summary in enumerate(series_list):
        series_id = series_summary.get("id")
        if not series_id: continue

        prog = (i + 1) / total_series_to_process
        try: progress_bar.progress(prog, text=f"Processing Series {i+1}/{total_series_to_process} (ID: {series_id})")
        except Exception: pass

        games_in_series = get_series_state(series_id, api_key, debug_logs)
        if not games_in_series:
            skipped_state_fail_count += 1
            time.sleep(API_REQUEST_DELAY / 2)
            continue

        # --- Цикл по играм внутри серии ---
        for game_info in games_in_series:
            game_id = game_info.get("id")
            sequence_number = game_info.get("sequenceNumber")
            if not game_id or sequence_number is None: continue

            # Используем Game ID для проверки дубликатов
            if game_id in existing_game_ids:
                skipped_existing_count += 1
                continue

            log_message(f"Processing game {game_id} (Series {series_id}, Seq {sequence_number})", debug_logs)
            summary_data = download_riot_summary_data(series_id, sequence_number, api_key, debug_logs)
            if not summary_data:
                skipped_summary_fail_count += 1
                time.sleep(API_REQUEST_DELAY)
                continue

            # 3. Парсим summary_data
            try:
                participants = summary_data.get("participants", [])
                teams_data = summary_data.get("teams", [])
                game_duration_sec = summary_data.get("gameDuration", 0)
                game_creation_timestamp = summary_data.get("gameCreation")
                # --- ИЗВЛЕКАЕМ ПАТЧ ---
                game_version = summary_data.get("gameVersion", "N/A")
                patch_str = "N/A"
                if game_version != "N/A":
                    parts = game_version.split('.')
                    if len(parts) >= 2:
                        patch_str = f"{parts[0]}.{parts[1]}" # Формат XX.YY
                # --- КОНЕЦ ИЗВЛЕЧЕНИЯ ПАТЧА ---

                if not participants or len(participants) != 10 or not teams_data or len(teams_data) != 2:
                     log_message(f"Skipping game {game_id}: Invalid participants ({len(participants)}) or teams ({len(teams_data)}) count.", debug_logs)
                     skipped_parsing_fail_count += 1
                     continue

                # --- Определяем нашу команду и результат ---
                our_side = None; our_team_id = None; opponent_team_name = "Opponent"
                blue_team_roster_names = set(); red_team_roster_names = set()
                for idx, p in enumerate(participants):
                    normalized_name = normalize_player_name(p.get("riotIdGameName"))
                    if normalized_name in ROSTER_RIOT_NAME_TO_GRID_ID:
                        side_idx = 0 if idx < 5 else 1 # 0 for blue, 1 for red
                        current_side = 'blue' if side_idx == 0 else 'red'
                        current_team_id = 100 if side_idx == 0 else 200
                        if our_side is None:
                            our_side = current_side
                            our_team_id = current_team_id
                        elif our_side != current_side:
                             log_message(f"Warning: Roster players found on both sides in game {game_id}! Assuming '{our_side}'.", debug_logs)
                        if side_idx == 0: blue_team_roster_names.add(normalized_name)
                        else: red_team_roster_names.add(normalized_name)

                if our_side is None:
                    log_message(f"Skipping game {game_id}: Roster players not found.", debug_logs)
                    skipped_parsing_fail_count += 1; continue

                blue_team_name = TEAM_NAME if our_side == 'blue' else opponent_team_name
                red_team_name = TEAM_NAME if our_side == 'red' else opponent_team_name
                result = "N/A"
                for team_summary in teams_data:
                    if team_summary.get("teamId") == our_team_id:
                        result = "Win" if team_summary.get("win") else "Loss"; break

                # --- Извлекаем баны (ID) ---
                blue_bans = ["N/A"] * 5; red_bans = ["N/A"] * 5
                for team_summary in teams_data:
                    bans_list = team_summary.get("bans", [])
                    target_bans = blue_bans if team_summary.get("teamId") == 100 else red_bans
                    bans_list_sorted = sorted(bans_list, key=lambda x: x.get('pickTurn', 99))
                    for i, ban_info in enumerate(bans_list_sorted[:5]):
                        champ_id = ban_info.get("championId", -1)
                        if champ_id != -1: target_bans[i] = str(champ_id)

                # --- Извлекаем фактических чемпионов по ролям (по индексу) ---
                actual_champs = {"blue": {}, "red": {}}
                roles_in_order = ["TOP", "JGL", "MID", "BOT", "SUP"]
                for idx, p in enumerate(participants):
                    champ_name = p.get("championName", "N/A")
                    role = roles_in_order[idx % 5]
                    side = 'blue' if idx < 5 else 'red'
                    actual_champs[side][role] = champ_name

                # --- Форматируем дату и длительность ---
                date_str = "N/A"
                if game_creation_timestamp:
                    try: dt_obj = datetime.fromtimestamp(game_creation_timestamp / 1000, timezone.utc); date_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e: log_message(f"Error parsing gameCreation timestamp {game_creation_timestamp}: {e}", debug_logs)
                duration_str = "N/A"
                if game_duration_sec > 0: minutes, seconds = divmod(int(game_duration_sec), 60); duration_str = f"{minutes}:{seconds:02d}"

                # --- Формируем строку для записи (порядок как в SCRIMS_HEADER) ---
                new_row_data = [
                    date_str, patch_str, blue_team_name, red_team_name, # Добавили patch_str
                    *blue_bans, *red_bans, # ID банов
                    actual_champs["blue"].get("TOP", "N/A"), actual_champs["blue"].get("JGL", "N/A"),
                    actual_champs["blue"].get("MID", "N/A"), actual_champs["blue"].get("BOT", "N/A"),
                    actual_champs["blue"].get("SUP", "N/A"),
                    actual_champs["red"].get("TOP", "N/A"), actual_champs["red"].get("JGL", "N/A"),
                    actual_champs["red"].get("MID", "N/A"), actual_champs["red"].get("BOT", "N/A"),
                    actual_champs["red"].get("SUP", "N/A"),
                    duration_str, result
                ]

                if len(new_row_data) != len(SCRIMS_HEADER):
                    log_message(f"Error: Row length mismatch for game {game_id}. Expected {len(SCRIMS_HEADER)}, got {len(new_row_data)}.", debug_logs)
                    skipped_parsing_fail_count += 1; continue

                new_rows.append(new_row_data)
                existing_game_ids.add(game_id)
                processed_game_count += 1

            except Exception as e:
                log_message(f"Failed to parse summary data for game {game_id}: {e}", debug_logs)
                import traceback
                log_message(traceback.format_exc(), debug_logs) # Добавим traceback для деталей
                skipped_parsing_fail_count += 1; continue

            time.sleep(API_REQUEST_DELAY / 2)
        time.sleep(API_REQUEST_DELAY)
    # --- Конец цикла по сериям ---

    # --- Вывод статистики и обновление таблицы ---
    try: progress_bar.progress(1.0, text="Update complete. Finalizing...")
    except Exception: pass

    # Формирование summary (без изменений)
    summary = [
        f"\n--- Update Summary ---", f"Input Series Processed: {total_series_to_process}",
        f"Games Found via seriesState: {processed_game_count + skipped_existing_count + skipped_summary_fail_count + skipped_parsing_fail_count}",
        f"Skipped (Already in Sheet): {skipped_existing_count}", f"Skipped (seriesState Fail): {skipped_state_fail_count} (series)",
        f"Skipped (Summary Download Fail): {skipped_summary_fail_count}", f"Skipped (Parsing/Data Error): {skipped_parsing_fail_count}",
        f"Processed & Added Successfully: {len(new_rows)}"
    ]
    if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
    st.session_state.scrims_update_logs = st.session_state.scrims_update_logs[-100:] + debug_logs[-50:] + summary
    st.code("\n".join(summary), language=None)

    # Запись в таблицу (без изменений)
    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            log_message(f"Successfully appended {len(new_rows)} new game records to '{worksheet.title}'.", debug_logs)
            st.success(f"Added {len(new_rows)} new game records to '{worksheet.title}'.")
            return True
        except Exception as e:
            error_msg = f"Error appending rows: {e}"; log_message(error_msg, debug_logs); st.error(error_msg);
            return False
    else:
        st.info("No new valid game records found to add.")
        total_skipped = skipped_existing_count + skipped_summary_fail_count + skipped_parsing_fail_count
        if processed_game_count == 0 and total_skipped > 0: st.warning(f"Found games but skipped all ({total_skipped} skipped). Check logs.")
        elif processed_game_count == 0 and skipped_state_fail_count > 0: st.warning(f"Failed to retrieve game lists for {skipped_state_fail_count} series.")
        return False
# --- Конец функции update_scrims_data ---


# --- ФУНКЦИЯ АГРЕГАЦИИ ДАННЫХ (Адаптирована) ---
# @st.cache_data(ttl=180) # Можно добавить кэш, но очищать при обновлении
# --- ЗАМЕНИТЕ ЭТУ ФУНКЦИЮ ---
# --- ФУНКЦИЯ АГРЕГАЦИИ ДАННЫХ (Патч, Иконки банов, Порядок колонок) ---
# @st.cache_data(ttl=180)
def aggregate_scrims_data(worksheet, time_filter, champion_id_map):
    """
    Агрегирует данные из Google Sheet. Отображает Патч, иконки банов.
    Result и Duration перемещены в конец истории. Названия команд добавлены.
    Использует переданную карту ID чемпионов.
    """
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        return {}, {}, pd.DataFrame(), {}

    if not champion_id_map:
        st.warning("Champion ID map not available for aggregation.")

    # Инициализация (без изменений)
    blue_stats = {"wins": 0, "losses": 0, "total": 0}
    red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0}))

    # Фильтр по времени (без изменений)
    now_utc = datetime.now(timezone.utc)
    time_threshold = None
    if time_filter != "All Time":
        weeks_map = {"1 Week": 1, "2 Weeks": 2, "3 Weeks": 3, "4 Weeks": 4}
        days_map = {"3 Days": 3, "10 Days": 10, "2 Months": 60}
        if time_filter in weeks_map: time_threshold = now_utc - timedelta(weeks=weeks_map[time_filter])
        elif time_filter in days_map: time_threshold = now_utc - timedelta(days=days_map[time_filter])

    # Чтение данных (без изменений)
    try: data = worksheet.get_all_values()
    except Exception as e: st.error(f"Read error during aggregation: {e}"); return {}, {}, pd.DataFrame(), {}
    if len(data) <= 1: st.info(f"No data in sheet '{worksheet.title}'."); return {}, {}, pd.DataFrame(), {}

    header = data[0]
    if header != SCRIMS_HEADER:
         st.error(f"Header mismatch in '{worksheet.title}'.")
         return {}, {}, pd.DataFrame(), {}

    try: idx_map = {name: i for i, name in enumerate(header)}
    except Exception as e: st.error(f"Failed map creation: {e}"); return {}, {}, pd.DataFrame(), {}

    # --- ИЗМЕНЕНО: Добавлены названия команд в порядок отображения ---
    HISTORY_DISPLAY_ORDER = [
        "Date", "Patch", "Blue Team Name", "B Bans", "B Picks", # Добавлено Blue Team Name
        "R Picks", "R Bans", "Red Team Name", "Result", "Duration" # Добавлено Red Team Name
    ]
    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

    # Обработка строк (основная логика без изменений)
    rows_processed_after_filter = 0
    relevant_player_names = set(ROSTER_RIOT_NAME_TO_GRID_ID.keys())
    ROLE_ORDER_FOR_SHEET = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    role_to_abbr = {"TOP": "TOP", "JUNGLE": "JGL", "MIDDLE": "MID", "BOTTOM": "BOT", "UTILITY": "SUP"}

    for row_index, row in enumerate(data[1:], start=2):
        if len(row) < len(header): continue
        try:
            date_str = row[idx_map["Date"]]
            passes_time_filter = True
            if time_threshold and date_str != "N/A":
                try: date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                except ValueError: passes_time_filter = False
                else:
                    if date_obj < time_threshold: passes_time_filter = False
            if not passes_time_filter: continue
            rows_processed_after_filter += 1

            blue_team_name = row[idx_map["Blue Team Name"]]
            red_team_name = row[idx_map["Red Team Name"]]
            result_our_team = row[idx_map["Result"]]
            is_our_blue = (blue_team_name == TEAM_NAME)
            is_our_red = (red_team_name == TEAM_NAME)
            if not is_our_blue and not is_our_red: continue

            # Статистика сторон (без изменений)
            if is_our_blue:
                blue_stats["total"] += 1;
                if result_our_team == "Win": blue_stats["wins"] += 1;
                elif result_our_team == "Loss": blue_stats["losses"] += 1;
            else:
                red_stats["total"] += 1;
                if result_our_team == "Win": red_stats["wins"] += 1;
                elif result_our_team == "Loss": red_stats["losses"] += 1;

            # Статистика игроков (без изменений)
            our_side_prefix = "Blue" if is_our_blue else "Red"
            is_win = (result_our_team == "Win")
            for role in ROLE_ORDER_FOR_SHEET:
                role_abbr = role_to_abbr.get(role);
                if not role_abbr: continue
                champ_col = f"Actual_{our_side_prefix}_{role_abbr}"
                if champ_col not in idx_map: continue
                champion = row[idx_map[champ_col]]
                if champion and champion != "N/A":
                    player_name_on_role = "Unknown"
                    for grid_id, roster_role in PLAYER_ROLES_BY_ID.items():
                        if roster_role == role: player_name_on_role = PLAYER_IDS.get(grid_id, "Unknown"); break
                    if player_name_on_role != "Unknown" and player_name_on_role in relevant_player_names:
                        player_stats[player_name_on_role][champion]['games'] += 1
                        if is_win: player_stats[player_name_on_role][champion]['wins'] += 1

            # Подготовка строки для истории матчей (без изменений в логике, кроме добавления полей в словарь)
            try:
                bb_icons = []
                for i in range(1, 6):
                    col_name = f"Blue Ban {i} ID"
                    if col_name in idx_map and idx_map[col_name] < len(row):
                        ban_id = str(row[idx_map[col_name]])
                        if ban_id and ban_id != "N/A" and ban_id != "-1" and champion_id_map:
                             champ_name = champion_id_map.get(ban_id, f"ID:{ban_id}")
                             bb_icons.append(get_champion_icon_html(champ_name))
                        elif ban_id and ban_id != "N/A" and ban_id != "-1": bb_icons.append(f"ID:{ban_id}")
                bb_html = " ".join(bb_icons) if bb_icons else ""

                rb_icons = []
                for i in range(1, 6):
                     col_name = f"Red Ban {i} ID"
                     if col_name in idx_map and idx_map[col_name] < len(row):
                         ban_id = str(row[idx_map[col_name]])
                         if ban_id and ban_id != "N/A" and ban_id != "-1" and champion_id_map:
                              champ_name = champion_id_map.get(ban_id, f"ID:{ban_id}")
                              rb_icons.append(get_champion_icon_html(champ_name))
                         elif ban_id and ban_id != "N/A" and ban_id != "-1": rb_icons.append(f"ID:{ban_id}")
                rb_html = " ".join(rb_icons) if rb_icons else ""

                bp_icons = []; rp_icons = []
                for role in ROLE_ORDER_FOR_SHEET:
                    role_abbr = role_to_abbr[role]
                    b_col_name = f"Actual_Blue_{role_abbr}"; r_col_name = f"Actual_Red_{role_abbr}"
                    if b_col_name in idx_map and idx_map[b_col_name] < len(row):
                         b_champion_name = row[idx_map[b_col_name]]
                         if b_champion_name and b_champion_name != "N/A": bp_icons.append(get_champion_icon_html(b_champion_name))
                    if r_col_name in idx_map and idx_map[r_col_name] < len(row):
                         r_champion_name = row[idx_map[r_col_name]]
                         if r_champion_name and r_champion_name != "N/A": rp_icons.append(get_champion_icon_html(r_champion_name))
                bp_html = " ".join(bp_icons) if bp_icons else ""
                rp_html = " ".join(rp_icons) if rp_icons else ""

                patch_val = row[idx_map["Patch"]] if "Patch" in idx_map else "N/A"
                duration_val = row[idx_map["Duration"]] if "Duration" in idx_map else "N/A"

                # Сохраняем все необходимые поля для последующего отображения
                history_rows.append({
                    "Date": date_str, "Patch": patch_val, "Blue Team Name": blue_team_name,
                    "B Bans": bb_html, "B Picks": bp_html, "R Picks": rp_html,
                    "R Bans": rb_html, "Red Team Name": red_team_name, "Result": result_our_team,
                    "Duration": duration_val
                })
            except Exception as hist_err: st.warning(f"Err history row {row_index}: {hist_err}")

        except Exception as e_inner: st.warning(f"Err processing row {row_index}: {e_inner}"); continue
    # --- Конец цикла ---

    if rows_processed_after_filter == 0 and time_filter != "All Time": st.info(f"No data matching filter: {time_filter}")
    elif not history_rows and rows_processed_after_filter > 0: st.warning(f"Processed {rows_processed_after_filter} games, but history empty.")

    df_hist = pd.DataFrame(history_rows)
    if not df_hist.empty:
        display_cols = HISTORY_DISPLAY_ORDER # Используем определенный порядок
        display_cols = [col for col in display_cols if col in df_hist.columns] # Проверка наличия колонок
        df_hist = df_hist[display_cols] # Применяем порядок
        try:
            df_hist['DT_temp'] = pd.to_datetime(df_hist['Date'], errors='coerce', utc=True)
            df_hist.dropna(subset=['DT_temp'], inplace=True)
            df_hist = df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception as sort_ex: st.warning(f"History sort failed: {sort_ex}")

    # Статистика игроков (без изменений)
    final_player_stats = {}
    for player, champ_data in player_stats.items():
        sorted_champs = dict(sorted(champ_data.items(), key=lambda item: item[1].get('games', 0), reverse=True))
        if sorted_champs: final_player_stats[player] = sorted_champs
    if not final_player_stats and rows_processed_after_filter > 0: st.info(f"Processed games, but no player stats.")

    return blue_stats, red_stats, df_hist, final_player_stats
# --- Конец функции aggregate_scrims_data ---

# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ СТРАНИЦЫ SCRIMS (Адаптирована) ---

def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    if st.button("⬅️ Back to HLL Stats"): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()

    # --- ПЕРЕМЕЩЕН ВЫЗОВ get_champion_data ВНУТРЬ ФУНКЦИИ ---
    # Получаем карту чемпионов один раз при отображении страницы
    champion_id_map = get_champion_data()
    # --- КОНЕЦ ПЕРЕМЕЩЕНИЯ ---

    # Настройка Google Sheets (без изменений)
    client = setup_google_sheets();
    if not client: st.error("Failed to connect to Google Sheets."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Could not open spreadsheet '{SCRIMS_SHEET_NAME}': {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: return

    # Секция обновления данных (без изменений)
    with st.expander("Update Scrim Data from GRID API", expanded=False):
        logs = [];
        if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
        if st.button("Download & Update Scrims", key="update_scrims_btn"):
            st.session_state.scrims_update_logs = []; logs = st.session_state.scrims_update_logs
            log_message("Starting scrim update process...", logs)
            with st.spinner("Fetching series list..."):
                series_list = get_all_series(GRID_API_KEY, logs)
            if series_list:
                st.info(f"Found {len(series_list)} potential scrim series to check...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting update...")
                try:
                    data_added = update_scrims_data(wks, series_list, GRID_API_KEY, logs, progress_bar)
                    if data_added: st.success("Data update complete. Refreshing statistics...")
                    else: st.warning("Update process finished, but no new data was added.")
                except Exception as e: log_message(f"Update error: {e}", logs); st.error(f"Update failed: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No recent scrim series found."); log_message("No series from get_all_series.", logs)
        if st.session_state.scrims_update_logs:
            st.text_area("Update Logs", "\n".join(st.session_state.scrims_update_logs), height=200, key="scrim_logs_display")

    st.divider()
    st.subheader("Scrim Performance Analysis")

    # Фильтр по времени (без изменений)
    time_f = st.selectbox("Filter by Time:", ["All Time", "3 Days", "1 Week", "2 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # --- ПЕРЕДАЕМ champion_id_map В aggregate_scrims_data ---
    blue_s, red_s, df_hist, player_champ_stats = aggregate_scrims_data(wks, time_f, champion_id_map)

    # Отображение общей статистики (без изменений)
    try:
        total_games_agg = blue_s.get("total", 0) + red_s.get("total", 0)
        total_wins_agg = blue_s.get("wins", 0) + red_s.get("wins", 0)
        total_losses_agg = blue_s.get("losses", 0) + red_s.get("losses", 0)
        st.markdown(f"**Overall Performance ({time_f})**"); col_ov, col_blue, col_red = st.columns(3)
        with col_ov:
            overall_wr = (total_wins_agg / total_games_agg * 100) if total_games_agg > 0 else 0
            st.metric("Total Games Analyzed", total_games_agg); st.metric("Overall Win Rate", f"{overall_wr:.1f}%", f"{total_wins_agg}W - {total_losses_agg}L")
        with col_blue:
            blue_wr = (blue_s.get("wins", 0) / blue_s.get("total", 0) * 100) if blue_s.get("total", 0) > 0 else 0
            st.metric("Blue Side Win Rate", f"{blue_wr:.1f}%", f"{blue_s.get('wins', 0)}W - {blue_s.get('losses', 0)}L ({blue_s.get('total', 0)} G)")
        with col_red:
            red_wr = (red_s.get("wins", 0) / red_s.get("total", 0) * 100) if red_s.get("total", 0) > 0 else 0
            st.metric("Red Side Win Rate", f"{red_wr:.1f}%", f"{red_s.get('wins', 0)}W - {red_s.get('losses', 0)}L ({red_s.get('total', 0)} G)")
    except Exception as e: st.error(f"Error displaying summary stats: {e}")

    st.divider()

    # --- ВКЛАДКИ (без изменений в логике, но стиль обновлен) ---
    tab1, tab2 = st.tabs(["📜 Match History (Games)", "📊 Player Champion Stats"])

    with tab1:
        st.subheader(f"Game History ({time_f})")
        if df_hist is not None and not df_hist.empty:
            # Стиль для истории (без изменений)
            st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Стили остаются как в пред. ответе
            st.markdown(df_hist.to_html(escape=False, index=False, classes='history-table', justify='center'), unsafe_allow_html=True)
        else:
            st.info(f"No match history found for the selected period: {time_f}.")

    with tab2:
        st.subheader(f"Player Champion Stats ({time_f})")
        if not player_champ_stats: st.info(f"No player stats for {time_f}.")
        else:
             player_order = [PLAYER_IDS[pid] for pid in ["26433", "25262", "25266", "20958", "21922"] if pid in PLAYER_IDS]
             player_cols = st.columns(len(player_order))
             for i, player_name in enumerate(player_order):
                 with player_cols[i]:
                     player_role = "Unknown"
                     for pid, role in PLAYER_ROLES_BY_ID.items():
                          if PLAYER_IDS.get(pid) == player_name: player_role = role; break
                     st.markdown(f"**{player_name}** ({player_role})")
                     player_data = player_champ_stats.get(player_name, {})
                     stats_list = []
                     if player_data:
                         for champ, stats in player_data.items():
                             games = stats.get('games', 0)
                             if games > 0:
                                 wins = stats.get('wins', 0); win_rate = round((wins / games) * 100, 1) if games > 0 else 0
                                 stats_list.append({
                                     'Icon': get_champion_icon_html(champ, width=30, height=30), # Размер иконок как просили
                                     'Games': games, 'WR%': win_rate
                                 })
                     if stats_list:
                         df_player = pd.DataFrame(stats_list)
                         df_player['WR%'] = df_player['WR%'].apply(color_win_rate_scrims)
                         # Стиль для статы игроков (без изменений)
                         st.markdown("""<style>...</style>""", unsafe_allow_html=True) # Стили остаются как в пред. ответе
                         st.markdown(df_player.to_html(escape=False, index=False, columns=['Icon', 'Games', 'WR%'], classes='player-stats', justify='center'), unsafe_allow_html=True)
                     else: st.caption("No stats.")

# --- Блок if __name__ == "__main__": (без изменений) ---
if __name__ == "__main__":
    pass
