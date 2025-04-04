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
    "Date", "Game ID", "Blue Team Name", "Red Team Name", # Используем Game ID
    "Blue Ban 1 ID", "Blue Ban 2 ID", "Blue Ban 3 ID", "Blue Ban 4 ID", "Blue Ban 5 ID",
    "Red Ban 1 ID", "Red Ban 2 ID", "Red Ban 3 ID", "Red Ban 4 ID", "Red Ban 5 ID",
    "Actual_Blue_TOP", "Actual_Blue_JGL", "Actual_Blue_MID", "Actual_Blue_BOT", "Actual_Blue_SUP",
    "Actual_Red_TOP", "Actual_Red_JGL", "Actual_Red_MID", "Actual_Red_BOT", "Actual_Red_SUP",
    "Duration", "Result" # Результат для НАШЕЙ команды (Win/Loss)
]

# --- DDRagon Helper Functions (Без изменений) ---
@st.cache_data(ttl=3600)
 get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1" # Fallback к известной версии
    except Exception: return "14.14.1" # Fallback к известной версии

@st.cache_data
 normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    ex = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"};
    if champ in ex: return ex[champ]
    # Убираем пробелы и апострофы, капитализируем первую букву
    name_clean = ''.join(c for c in champ if c.isalnum())
    if name_clean:
        return name_clean[0].upper() + name_clean[1:]
    return None

 get_champion_icon_html(champion, width=25, height=25):
    patch_version = get_latest_patch_version(); norm = normalize_champion_name_for_ddragon(champion)
    if norm: url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{norm}.png"; return f'<img src="{url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return ""

 color_win_rate_scrims(value):
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
 setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation (Адаптировано под новый SCRIMS_HEADER) ---
 check_if_scrims_worksheet_exists(spreadsheet, name):
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
 log_message(message, logs_list):
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
        {
            seriesState (
                id: "%s"  # Подставляем series_id сюда
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
    query = query_template % series_id # Формируем запрос с ID серии
    # Эндпоинт для Series State API
    endpoint = "live-data-feed/series-state/graphql"

    response_data = post_graphql_request(query, endpoint, api_key, logs_list)

    if response_data and response_data.get("seriesState") and "games" in response_data["seriesState"]:
        games = response_data["seriesState"]["games"]
        log_message(f"Found {len(games)} games in series {series_id}", logs_list)
        return games
    else:
        log_message(f"Could not find games for series {series_id} in seriesState response.", logs_list)
        return [] # Возвращаем пустой список, если игры не найдены

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

    # Получаем существующие Game ID из таблицы (вторая колонка, индекс 1)
    try:
        existing_data = worksheet.get_all_values()
        # Пропускаем заголовок (первая строка)
        existing_game_ids = set(row[1] for row in existing_data[1:] if len(row) > 1 and row[1]) if len(existing_data) > 1 else set()
        log_message(f"Found {len(existing_game_ids)} existing game IDs in the sheet.", debug_logs)
    except gspread.exceptions.APIError as api_err:
        log_message(f"GSpread API Error reading sheet: {api_err}", debug_logs)
        st.error(f"GSpread API Error reading sheet: {api_err}")
        return False
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
        if not series_id:
            log_message(f"Skipping series entry due to missing ID: {series_summary}", debug_logs)
            continue

        # Обновляем прогресс бар
        prog = (i + 1) / total_series_to_process
        try:
            progress_bar.progress(prog, text=f"Processing Series {i+1}/{total_series_to_process} (ID: {series_id})")
        except Exception as e:
             # Иногда progress_bar может быть None или вызвать ошибку, игнорируем
             # log_message(f"Progress bar update error: {e}", debug_logs)
             pass

        # 1. Получаем список игр для серии
        games_in_series = get_series_state(series_id, api_key, debug_logs)
        if not games_in_series:
            skipped_state_fail_count += 1
            log_message(f"Skipping series {series_id}: Failed to get game list from seriesState.", debug_logs)
            time.sleep(API_REQUEST_DELAY / 2) # Небольшая задержка перед следующей серией
            continue

        # --- Цикл по играм внутри серии ---
        for game_info in games_in_series:
            game_id = game_info.get("id")
            sequence_number = game_info.get("sequenceNumber")

            if not game_id or sequence_number is None:
                log_message(f"Skipping game in series {series_id} due to missing game_id or sequence_number: {game_info}", debug_logs)
                continue

            # Проверяем, есть ли уже игра с таким ID в таблице
            if game_id in existing_game_ids:
                skipped_existing_count += 1
                # log_message(f"Skipping game {game_id} (Series {series_id}, Seq {sequence_number}): Already exists in sheet.", debug_logs)
                continue

            log_message(f"Processing game {game_id} (Series {series_id}, Seq {sequence_number})", debug_logs)

            # 2. Скачиваем Riot Summary JSON для игры
            summary_data = download_riot_summary_data(series_id, sequence_number, api_key, debug_logs)
            if not summary_data:
                skipped_summary_fail_count += 1
                log_message(f"Skipping game {game_id}: Failed to download summary data.", debug_logs)
                time.sleep(API_REQUEST_DELAY) # Задержка при ошибке скачивания
                continue

            # 3. Парсим summary_data
            try:
                participants = summary_data.get("participants", [])
                teams_data = summary_data.get("teams", [])
                game_duration_sec = summary_data.get("gameDuration", 0)
                game_creation_timestamp = summary_data.get("gameCreation") # мс с эпохи

                if not participants or len(participants) != 10 or not teams_data or len(teams_data) != 2:
                     log_message(f"Skipping game {game_id}: Invalid participants ({len(participants)}) or teams ({len(teams_data)}) count in summary.", debug_logs)
                     skipped_parsing_fail_count += 1
                     continue

                # --- Определяем нашу команду и результат ---
                our_side = None # 'blue' or 'red'
                our_team_id = None # 100 or 200
                opponent_team_name = "Opponent" # Имя будет уточнено позже
                blue_team_roster_names = set()
                red_team_roster_names = set()

                for idx, p in enumerate(participants):
                    riot_name = p.get("riotIdGameName")
                    normalized_name = normalize_player_name(riot_name)
                    if normalized_name in ROSTER_RIOT_NAME_TO_GRID_ID:
                        if idx < 5: # Первые 5 - Blue
                            if our_side is None:
                                our_side = 'blue'
                                our_team_id = 100
                            elif our_side == 'red':
                                log_message(f"Warning: Found roster players on both sides in game {game_id}! Assuming blue.", debug_logs)
                                our_side = 'blue' # Приоритет синей? Или ошибка?
                                our_team_id = 100
                            blue_team_roster_names.add(normalized_name)
                        else: # 5-9 - Red
                            if our_side is None:
                                our_side = 'red'
                                our_team_id = 200
                            elif our_side == 'blue':
                                log_message(f"Warning: Found roster players on both sides in game {game_id}! Assuming {our_side}.", debug_logs)
                                # Не меняем our_side, если уже нашли на синей
                            red_team_roster_names.add(normalized_name)

                if our_side is None:
                    log_message(f"Skipping game {game_id}: Could not find any roster players.", debug_logs)
                    skipped_parsing_fail_count += 1
                    continue # Пропускаем, если не нашли наших игроков

                # --- Определяем имена команд (приблизительно) ---
                # Пытаемся угадать имена по тегам игроков или используем TEAM_NAME
                blue_team_name = TEAM_NAME if our_side == 'blue' else opponent_team_name
                red_team_name = TEAM_NAME if our_side == 'red' else opponent_team_name
                # Можно добавить логику для извлечения тега из riotIdGameName, если он есть у оппонентов

                # --- Определяем результат для нашей команды ---
                result = "N/A"
                for team_summary in teams_data:
                    if team_summary.get("teamId") == our_team_id:
                        if team_summary.get("win") is True:
                            result = "Win"
                        elif team_summary.get("win") is False:
                            result = "Loss"
                        break

                # --- Извлекаем баны (ID) ---
                blue_bans = ["N/A"] * 5
                red_bans = ["N/A"] * 5
                for team_summary in teams_data:
                    bans_list = team_summary.get("bans", [])
                    target_bans = blue_bans if team_summary.get("teamId") == 100 else red_bans
                    # Сортируем по pickTurn на всякий случай, берем первые 5
                    bans_list_sorted = sorted(bans_list, key=lambda x: x.get('pickTurn', 99))
                    for i, ban_info in enumerate(bans_list_sorted[:5]):
                        champ_id = ban_info.get("championId", -1)
                        if champ_id != -1: # Riot API использует -1 для отсутствия бана
                            target_bans[i] = str(champ_id)

                # --- Извлекаем фактических чемпионов по ролям (по индексу) ---
                actual_champs = {"blue": {}, "red": {}}
                roles_in_order = ["TOP", "JGL", "MID", "BOT", "SUP"] # Совпадают с ROLE_ORDER_FOR_SHEET, но без MIDDLE->MID и т.д.
                for idx, p in enumerate(participants):
                    champ_name = p.get("championName", "N/A")
                    role = roles_in_order[idx % 5] # Определяем роль по индексу
                    side = 'blue' if idx < 5 else 'red'
                    actual_champs[side][role] = champ_name

                # --- Форматируем дату и длительность ---
                date_str = "N/A"
                if game_creation_timestamp:
                    try:
                        # Timestamp в миллисекундах
                        dt_obj = datetime.fromtimestamp(game_creation_timestamp / 1000, timezone.utc)
                        date_str = dt_obj.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception as e:
                        log_message(f"Error parsing gameCreation timestamp {game_creation_timestamp}: {e}", debug_logs)

                duration_str = "N/A"
                if game_duration_sec > 0:
                    minutes, seconds = divmod(int(game_duration_sec), 60)
                    duration_str = f"{minutes}:{seconds:02d}"

                # --- Формируем строку для записи ---
                # Убедимся, что ключи в actual_champs совпадают с ожидаемыми в SCRIMS_HEADER
                # (TOP, JGL, MID, BOT, SUP)
                new_row_data = [
                    date_str, game_id, blue_team_name, red_team_name,
                    *blue_bans, # ID банов синих
                    *red_bans,  # ID банов красных
                    actual_champs["blue"].get("TOP", "N/A"), actual_champs["blue"].get("JGL", "N/A"),
                    actual_champs["blue"].get("MID", "N/A"), actual_champs["blue"].get("BOT", "N/A"),
                    actual_champs["blue"].get("SUP", "N/A"),
                    actual_champs["red"].get("TOP", "N/A"), actual_champs["red"].get("JGL", "N/A"),
                    actual_champs["red"].get("MID", "N/A"), actual_champs["red"].get("BOT", "N/A"),
                    actual_champs["red"].get("SUP", "N/A"),
                    duration_str, result
                ]

                # Проверка на соответствие заголовку
                if len(new_row_data) != len(SCRIMS_HEADER):
                    log_message(f"Error: Row length mismatch for game {game_id}. Expected {len(SCRIMS_HEADER)}, got {len(new_row_data)}. Row: {new_row_data}", debug_logs)
                    skipped_parsing_fail_count += 1
                    continue # Пропускаем эту строку

                new_rows.append(new_row_data)
                existing_game_ids.add(game_id) # Добавляем ID в обработанные, чтобы не дублировать в этом же запуске
                processed_game_count += 1

            except Exception as e:
                log_message(f"Failed to parse summary data for game {game_id}: {e}", debug_logs)
                # Вывод части данных для отладки
                # log_message(f"Problematic summary data snippet: {str(summary_data)[:500]}", debug_logs)
                skipped_parsing_fail_count += 1
                continue # Пропускаем эту игру

            # Небольшая задержка между обработкой игр внутри серии
            time.sleep(API_REQUEST_DELAY / 2)

        # Небольшая задержка между обработкой серий
        time.sleep(API_REQUEST_DELAY)
    # --- Конец цикла по сериям ---

    # --- Вывод статистики и обновление таблицы ---
    try:
        progress_bar.progress(1.0, text="Update complete. Finalizing...")
    except Exception: pass

    summary = [
        f"\n--- Update Summary ---",
        f"Input Series Processed: {total_series_to_process}",
        f"Games Found via seriesState: {processed_game_count + skipped_existing_count + skipped_summary_fail_count + skipped_parsing_fail_count}",
        f"Skipped (Already in Sheet): {skipped_existing_count}",
        f"Skipped (seriesState Fail): {skipped_state_fail_count} (series)",
        f"Skipped (Summary Download Fail): {skipped_summary_fail_count}",
        f"Skipped (Parsing/Data Error): {skipped_parsing_fail_count}",
        f"Processed & Added Successfully: {len(new_rows)}"
    ]
    # Добавляем логи и summary в session_state для отображения
    if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
    # Ограничиваем логи до последних ~100 + summary
    st.session_state.scrims_update_logs = st.session_state.scrims_update_logs[-100:] + debug_logs[-50:] + summary
    st.code("\n".join(summary), language=None) # Показываем summary

    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            log_message(f"Successfully appended {len(new_rows)} new game records to '{worksheet.title}'.", debug_logs)
            st.success(f"Added {len(new_rows)} new game records to '{worksheet.title}'.")
            # Очищаем кэш агрегации после добавления данных
            # aggregate_scrims_data.clear() # Раскомментировать, если aggregate_scrims_data будет кэшироваться
            return True
        except gspread.exceptions.APIError as api_err:
            error_msg = f"GSpread API Error appending rows: {api_err}"; log_message(error_msg, debug_logs); st.error(error_msg); st.error(f"Failed to add {len(new_rows)} rows.")
            return False
        except Exception as e:
            error_msg = f"Error appending rows: {e}"; log_message(error_msg, debug_logs); st.error(error_msg); st.error(f"Failed to add {len(new_rows)} rows.")
            return False
    else:
        st.info("No new valid game records found to add.")
        # Можно добавить доп. информацию, если были пропуски
        total_skipped = skipped_existing_count + skipped_summary_fail_count + skipped_parsing_fail_count
        if processed_game_count == 0 and total_skipped > 0:
             st.warning(f"Found potential games but skipped all of them ({total_skipped} total skipped). Check logs and sheet content.")
        elif processed_game_count == 0 and skipped_state_fail_count > 0:
             st.warning(f"Failed to retrieve game lists for {skipped_state_fail_count} series using seriesState.")

        return False
# --- Конец функции update_scrims_data ---


# --- ФУНКЦИЯ АГРЕГАЦИИ ДАННЫХ (Адаптирована) ---
# @st.cache_data(ttl=180) # Можно добавить кэш, но очищать при обновлении
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    """
    Агрегирует данные из Google Sheet на основе НОВОГО ЗАГОЛОВКА.
    Возвращает статистику по сторонам, историю матчей (игр) и статистику игроков.
    """
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        return {}, {}, pd.DataFrame(), {} # Возвращаем пустые структуры

    # Инициализация
    blue_stats = {"wins": 0, "losses": 0, "total": 0}
    red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    # Статистика игроков: { player_name: { champion_name: {'games': N, 'wins': M} } }
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0}))

    # Фильтр по времени
    now_utc = datetime.now(timezone.utc)
    time_threshold = None
    if time_filter != "All Time":
        # Добавим больше опций фильтрации
        weeks_map = {"1 Week": 1, "2 Weeks": 2, "3 Weeks": 3, "4 Weeks": 4}
        days_map = {"3 Days": 3, "10 Days": 10, "2 Months": 60} # Примерно 2 месяца
        if time_filter in weeks_map:
            time_threshold = now_utc - timedelta(weeks=weeks_map[time_filter])
        elif time_filter in days_map:
            time_threshold = now_utc - timedelta(days=days_map[time_filter])
        # Добавить другие периоды, если нужно

    # Чтение данных
    try:
        data = worksheet.get_all_values()
    except Exception as e:
        st.error(f"Read error during aggregation: {e}")
        return {}, {}, pd.DataFrame(), {}

    if len(data) <= 1:
        st.info(f"No data found in the sheet '{worksheet.title}' for aggregation.")
        return {}, {}, pd.DataFrame(), {}

    header = data[0]
    # Проверка соответствия заголовка (важно!)
    if header != SCRIMS_HEADER:
        st.error(f"Header mismatch in '{worksheet.title}' during aggregation. Cannot proceed.")
        st.error(f"Expected: {SCRIMS_HEADER}")
        st.error(f"Found:    {header}")
        return {}, {}, pd.DataFrame(), {}

    # Создаем индекс колонок
    try:
        idx_map = {name: i for i, name in enumerate(SCRIMS_HEADER)}
    except Exception as e:
         st.error(f"Failed to create column index map: {e}")
         return {}, {}, pd.DataFrame(), {}

    # Обработка строк
    rows_processed_after_filter = 0
    relevant_player_names = set(ROSTER_RIOT_NAME_TO_GRID_ID.keys()) # Имена игроков из нашего ростера

    for row_index, row in enumerate(data[1:], start=2):
        if len(row) != len(SCRIMS_HEADER):
            # Пропускаем строки с неверным количеством колонок
            # st.warning(f"Skipping row {row_index} due to column count mismatch.")
            continue
        try:
            # --- Фильтр по времени ---
            date_str = row[idx_map["Date"]]
            if time_threshold and date_str != "N/A":
                try:
                    # Даты должны быть в UTC для сравнения
                    date_obj = datetime.strptime(date_str.split('.')[0], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    if date_obj < time_threshold:
                        continue # Пропускаем старую запись
                except ValueError:
                    # st.warning(f"Skipping row {row_index} due to invalid date format: {date_str}")
                    continue # Пропускаем строку с неверной датой

            rows_processed_after_filter += 1 # Считаем строки, прошедшие фильтр

            # --- Определяем нашу команду и результат ---
            blue_team_name = row[idx_map["Blue Team Name"]]
            red_team_name = row[idx_map["Red Team Name"]]
            result_our_team = row[idx_map["Result"]] # Результат для НАШЕЙ команды

            is_our_blue = (blue_team_name == TEAM_NAME)
            is_our_red = (red_team_name == TEAM_NAME)

            # Если это не игра нашей команды (по имени), пропускаем
            # Это резервная проверка, основная логика определения - при парсинге
            if not is_our_blue and not is_our_red:
                # Может случиться, если TEAM_NAME изменился или парсинг был неточен
                continue

            # --- Обновляем статистику по сторонам ---
            if is_our_blue:
                blue_stats["total"] += 1
                if result_our_team == "Win": blue_stats["wins"] += 1
                elif result_our_team == "Loss": blue_stats["losses"] += 1
            else: # Наша команда красная
                red_stats["total"] += 1
                if result_our_team == "Win": red_stats["wins"] += 1
                elif result_our_team == "Loss": red_stats["losses"] += 1

            # --- Обновляем статистику игроков ---
            our_side_prefix = "Blue" if is_our_blue else "Red"
            is_win = (result_our_team == "Win")

            # Проходим по ролям и извлекаем чемпиона и игрока нашей команды
            for role in ROLE_ORDER_FOR_SHEET: # TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY
                champ_col = f"Actual_{our_side_prefix}_{role}"
                champion = row[idx_map[champ_col]]

                if champion and champion != "N/A":
                    # Находим игрока на этой роли
                    # Логика определения игрока может быть сложной, если состав меняется.
                    # Простой вариант: Ищем имя игрока из ростера в названии команды или ожидаем его на этой роли.
                    # Пока что просто агрегируем по чемпиону на роли для нашей команды.
                    # Для статистики по ИГРОКУ, нужно знать, КТО играл на этой роли.
                    # --- ДОБАВЛЯЕМ ЛОГИКУ СОПОСТАВЛЕНИЯ С РОСТЕРОМ ---
                    # Определяем индекс участника по роли и стороне
                    participant_index = -1
                    if is_our_blue:
                         if role == "TOP": participant_index = 0
                         elif role == "JGL": participant_index = 1
                         elif role == "MID": participant_index = 2
                         elif role == "BOT": participant_index = 3
                         elif role == "SUP": participant_index = 4
                    else: # is_our_red
                         if role == "TOP": participant_index = 5
                         elif role == "JGL": participant_index = 6
                         elif role == "MID": participant_index = 7
                         elif role == "BOT": participant_index = 8
                         elif role == "SUP": participant_index = 9

                    # Пытаемся найти имя игрока (непосредственно из данных не сохраняли,
                    # но можем попытаться сопоставить по роли)
                    # Это НЕ НАДЕЖНО, если игроки меняются ролями!
                    # Лучше было бы СОХРАНЯТЬ имя игрока в таблицу.
                    # Пока сделаем ЗАГЛУШКУ: ищем, кто из ростера обычно играет на этой роли
                    player_name_on_role = "Unknown"
                    grid_id_on_role = None
                    for grid_id, roster_role in PLAYER_ROLES_BY_ID.items():
                        if roster_role == role:
                             grid_id_on_role = grid_id
                             player_name_on_role = PLAYER_IDS.get(grid_id_on_role, "Unknown")
                             break

                    # Если нашли игрока, записываем статистику для него
                    if player_name_on_role != "Unknown" and player_name_on_role in relevant_player_names:
                        player_stats[player_name_on_role][champion]['games'] += 1
                        if is_win:
                            player_stats[player_name_on_role][champion]['wins'] += 1
                    # else:
                        # Либо роль не совпала, либо игрок не из основного ростера
                        # Можно добавить логирование или обработку замен

            # --- Подготовка строки для истории матчей ---
            # Показываем фактических чемпионов, баны по ID
            bb_str = " ".join(row[idx_map[f"Blue Ban {i} ID"]] for i in range(1, 6) if row[idx_map[f"Blue Ban {i} ID"]] != "N/A")
            rb_str = " ".join(row[idx_map[f"Red Ban {i} ID"]] for i in range(1, 6) if row[idx_map[f"Red Ban {i} ID"]] != "N/A")
            bp_html = " ".join(get_champion_icon_html(row[idx_map[f"Actual_Blue_{role}"]]) for role in ROLE_ORDER_FOR_SHEET if row[idx_map[f"Actual_Blue_{role}"]] != "N/A")
            rp_html = " ".join(get_champion_icon_html(row[idx_map[f"Actual_Red_{role}"]]) for role in ROLE_ORDER_FOR_SHEET if row[idx_map[f"Actual_Red_{role}"]] != "N/A")

            history_rows.append({
                "Date": date_str,
                "Blue Team": blue_team_name,
                "B Bans": bb_str, # Показываем ID банов
                "B Picks": bp_html, # Фактические пики
                "Result": result_our_team, # Результат нашей команды
                "Duration": row[idx_map["Duration"]],
                "R Picks": rp_html, # Фактические пики
                "R Bans": rb_str, # Показываем ID банов
                "Red Team": red_team_name,
                "Game ID": row[idx_map["Game ID"]] # ID игры
            })

        except IndexError as e_idx:
            st.warning(f"Skipping row {row_index} due to IndexError: {e_idx}. Check data integrity.")
            continue
        except KeyError as e_key:
             st.warning(f"Skipping row {row_index} due to KeyError: {e_key}. Check header/column index map.")
             continue
        except Exception as e_inner:
            st.warning(f"Skipping row {row_index} due to processing error: {e_inner}")
            continue
    # --- Конец цикла for row in data[1:] ---

    if rows_processed_after_filter == 0 and time_filter != "All Time":
        st.info(f"No scrim data found matching the filter: {time_filter}")

    # --- Постобработка ---
    df_hist = pd.DataFrame(history_rows)
    if not df_hist.empty:
        try:
            df_hist['DT_temp'] = pd.to_datetime(df_hist['Date'], errors='coerce', utc=True)
            df_hist.dropna(subset=['DT_temp'], inplace=True)
            df_hist = df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception as sort_ex:
             st.warning(f"Could not sort match history by date: {sort_ex}")

    # Конвертируем и сортируем статистику игроков
    final_player_stats = {}
    for player, champ_data in player_stats.items():
        # Сортируем чемпионов по количеству игр (убывание)
        sorted_champs = dict(sorted(
            champ_data.items(),
            key=lambda item: item[1].get('games', 0),
            reverse=True
        ))
        if sorted_champs: # Добавляем игрока, только если у него есть статистика
             final_player_stats[player] = sorted_champs

    if not final_player_stats and rows_processed_after_filter > 0:
         st.info(f"Processed {rows_processed_after_filter} scrims for '{time_filter}', but no specific player champion stats were generated (check player name/role matching).")

    # Возвращаем синюю статистику, красную, историю, статистику игроков
    return blue_stats, red_stats, df_hist, final_player_stats
# --- Конец функции aggregate_scrims_data ---

# --- ФУНКЦИЯ ОТОБРАЖЕНИЯ СТРАНИЦЫ SCRIMS (Адаптирована) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    if st.button("⬅️ Back to HLL Stats"): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()

    # Настройка Google Sheets
    client = setup_google_sheets();
    if not client: st.error("Failed to connect to Google Sheets."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Could not open spreadsheet '{SCRIMS_SHEET_NAME}': {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    # Если wks вернулся как None из-за ошибки заголовка, прерываем
    if not wks: return

    # Секция обновления данных
    with st.expander("Update Scrim Data from GRID API", expanded=False):
        logs = [];
        if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []

        # Кнопка обновления
        if st.button("Download & Update Scrims", key="update_scrims_btn"):
            st.session_state.scrims_update_logs = []; logs = st.session_state.scrims_update_logs
            log_message("Starting scrim update process...", logs)
            with st.spinner("Fetching series list..."):
                # Передаем API ключ в get_all_series
                series_list = get_all_series(GRID_API_KEY, logs)
            if series_list:
                st.info(f"Found {len(series_list)} potential scrim series to check...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting update...")
                try:
                    # Передаем API ключ в update_scrims_data
                    data_added = update_scrims_data(wks, series_list, GRID_API_KEY, logs, progress_bar)
                    if data_added:
                        st.success("Data update complete. Refreshing statistics...")
                        # Очистка кэша, если он используется для aggregate_scrims_data
                        # aggregate_scrims_data.clear()
                    else:
                        st.warning("Update process finished, but no new data was added. Check logs for details.")
                except Exception as e:
                    log_message(f"Unhandled error during update process: {e}", logs)
                    st.error(f"Update failed with error: {e}")
                finally:
                    # Убираем прогресс бар
                    progress_bar_placeholder.empty()
            else:
                st.warning("No recent scrim series found based on current filters.")
                log_message("No series returned from get_all_series.", logs)

        # Отображение логов обновления
        if st.session_state.scrims_update_logs:
            st.text_area("Update Logs", "\n".join(st.session_state.scrims_update_logs), height=200, key="scrim_logs_display")

    st.divider()
    st.subheader("Scrim Performance Analysis")

    # Фильтр по времени
    time_f = st.selectbox("Filter by Time:",
                          ["All Time", "3 Days", "1 Week", "2 Weeks", "4 Weeks", "2 Months"],
                          key="scrims_time_filter")

    # Агрегация данных
    # Передаем wks и time_f
    blue_s, red_s, df_hist, player_champ_stats = aggregate_scrims_data(wks, time_f)

    # Отображение общей статистики
    try:
        total_games_agg = blue_s.get("total", 0) + red_s.get("total", 0)
        total_wins_agg = blue_s.get("wins", 0) + red_s.get("wins", 0)
        total_losses_agg = blue_s.get("losses", 0) + red_s.get("losses", 0)

        st.markdown(f"**Overall Performance ({time_f})**")
        col_ov, col_blue, col_red = st.columns(3)

        with col_ov:
            overall_wr = (total_wins_agg / total_games_agg * 100) if total_games_agg > 0 else 0
            st.metric("Total Games Analyzed", total_games_agg)
            st.metric("Overall Win Rate", f"{overall_wr:.1f}%", f"{total_wins_agg}W - {total_losses_agg}L")

        with col_blue:
            blue_wr = (blue_s.get("wins", 0) / blue_s.get("total", 0) * 100) if blue_s.get("total", 0) > 0 else 0
            st.metric("Blue Side Win Rate", f"{blue_wr:.1f}%", f"{blue_s.get('wins', 0)}W - {blue_s.get('losses', 0)}L ({blue_s.get('total', 0)} G)")

        with col_red:
            red_wr = (red_s.get("wins", 0) / red_s.get("total", 0) * 100) if red_s.get("total", 0) > 0 else 0
            st.metric("Red Side Win Rate", f"{red_wr:.1f}%", f"{red_s.get('wins', 0)}W - {red_s.get('losses', 0)}L ({red_s.get('total', 0)} G)")

    except Exception as e:
        st.error(f"Error displaying summary statistics: {e}")

    st.divider()

    # --- ВКЛАДКИ ДЛЯ ИСТОРИИ И СТАТИСТИКИ ИГРОКОВ ---
    tab1, tab2 = st.tabs(["📜 Match History (Games)", "📊 Player Champion Stats"])

    with tab1:
        st.subheader(f"Game History ({time_f})")
        if df_hist is not None and not df_hist.empty:
            # Стилизация таблицы истории матчей
            st.markdown("""
            <style>
            .history-table { font-size: 0.85rem; width: auto; margin: 5px auto; border-collapse: collapse; }
            .history-table th, .history-table td { padding: 4px 6px; text-align: center; border: 1px solid #555; white-space: nowrap; }
            .history-table td:nth-child(3), .history-table td:nth-child(4), .history-table td:nth-child(7), .history-table td:nth-child(8) { min-width: 100px; } /* Колонки с иконками/банами */
            .history-table td:nth-child(9) { min-width: 150px; } /* ID игры */
            </style>
            """, unsafe_allow_html=True)
            # Отображаем таблицу HTML
            st.markdown(df_hist.to_html(escape=False, index=False, classes='history-table', justify='center'), unsafe_allow_html=True)
        else:
            st.info(f"No match history found for the selected period: {time_f}.")

    with tab2:
        st.subheader(f"Player Champion Stats ({time_f})")
        # Проверяем, есть ли данные для отображения
        if not player_champ_stats:
             st.info(f"No player champion stats available for the selected period: {time_f}.")
        else:
             # Определяем порядок игроков на основе PLAYER_IDS (можно настроить)
             player_order = [PLAYER_IDS[pid] for pid in ["26433", "25262", "25266", "20958", "21922"] if pid in PLAYER_IDS]
             player_cols = st.columns(len(player_order))

             for i, player_name in enumerate(player_order):
                 with player_cols[i]:
                     # Получаем роль игрока из константы
                     player_role = "Unknown"
                     for pid, role in PLAYER_ROLES_BY_ID.items():
                          # Находим ID игрока по имени
                          if PLAYER_IDS.get(pid) == player_name:
                              player_role = role
                              break
                     st.markdown(f"**{player_name}** ({player_role})")

                     # Получаем статистику для этого игрока
                     player_data = player_champ_stats.get(player_name, {})
                     stats_list = []
                     if player_data:
                         # player_data уже отсортирован в aggregate_scrims_data
                         for champ, stats in player_data.items():
                             games = stats.get('games', 0)
                             if games > 0:
                                 wins = stats.get('wins', 0)
                                 win_rate = round((wins / games) * 100, 1) if games > 0 else 0
                                 stats_list.append({
                                     'Icon': get_champion_icon_html(champ, 20, 20),
                                     # 'Champion': champ, # Можно убрать имя, если иконки достаточно
                                     'Games': games,
                                     'WR%': win_rate
                                 })

                     if stats_list:
                         df_player = pd.DataFrame(stats_list) # Уже отсортировано
                         # Применяем форматирование WR
                         df_player['WR%'] = df_player['WR%'].apply(color_win_rate_scrims)
                         # Стилизация таблицы статистики игрока
                         st.markdown("""
                         <style>
                         .player-stats { font-size: 0.8rem; width: auto; margin: 3px auto; border-collapse: collapse; }
                         .player-stats th, .player-stats td { padding: 2px 4px; text-align: center; border: 1px solid #444; white-space: nowrap; }
                         </style>
                         """, unsafe_allow_html=True)
                         # Отображаем таблицу (без индекса, только нужные колонки)
                         st.markdown(
                              df_player.to_html(escape=False, index=False, columns=['Icon', 'Games', 'WR%'], classes='player-stats', justify='center'),
                              unsafe_allow_html=True
                         )
                     else:
                         st.caption("No stats for this period.")

# --- Оставляем блок if __name__ == "__main__": ---
# Это позволяет импортировать scrims_page в app.py без выполнения кода напрямую
if __name__ == "__main__":
    # Этот код не будет выполняться при импорте из app.py
    # Можно добавить сюда тестовый запуск scrims_page, если нужно
    # st.info("Running scrims.py directly (for testing)")
    # scrims_page()
    pass

# --- END OF FILE scrims.py ---
