# --- START OF FILE scrims.py ---

import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta
import time
from collections import defaultdict

# --- КОНСТАНТЫ и НАСТРОЙКИ ---
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63")
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
OUR_TEAM_ID = "19770"
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed"
SCRIMS_WORKSHEET_NAME = "Scrims"
API_REQUEST_DELAY = 1.0 # Задержка между запросами к API

# Используем ID игроков для точного сопоставления
PLAYER_IDS = {
    "26433": "Aytekn",
    "25262": "Pallet",
    "25266": "Tsiperakos",
    "20958": "Kenal",
    "21922": "CENTU"
}
# Определяем роль для каждого ID
PLAYER_ROLES_BY_ID = {
    "26433": "TOP",
    "25262": "JUNGLE",
    "25266": "MIDDLE",
    "20958": "BOTTOM",
    "21922": "UTILITY"
}
# Стандартный порядок ролей для ЗАПИСИ в таблицу
ROLE_ORDER_FOR_SHEET = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
# --- ИЗМЕНЕНО: Заголовок соответствует данным, которые мы будем извлекать ---
SCRIMS_HEADER = [
    "Date", "Match ID", "Game SeqNum", "Blue Team", "Red Team", # Добавлен SeqNum для отладки
    "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
    "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
    # Пики по порядку драфта (B1, R1, R2, B2, B3, R3, R4, B4, B5, R5)
    "Draft_Pick_B1", "Draft_Pick_R1", "Draft_Pick_R2",
    "Draft_Pick_B2", "Draft_Pick_B3", "Draft_Pick_R3",
    "Draft_Pick_R4", "Draft_Pick_B4", "Draft_Pick_B5", "Draft_Pick_R5",
    # Фактические чемпионы по ролям
    "Actual_Blue_TOP", "Actual_Blue_JGL", "Actual_Blue_MID", "Actual_Blue_BOT", "Actual_Blue_SUP",
    "Actual_Red_TOP", "Actual_Red_JGL", "Actual_Red_MID", "Actual_Red_BOT", "Actual_Red_SUP",
    # Стандартные колонки в конце
    "Duration", "Result"
]

# --- DDRagon Helper Functions (Без изменений) ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1"
    except Exception: return "14.14.1" # Fallback version
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    ex = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"};
    if champ in ex: return ex[champ]
    # General normalization for names like Kai'Sa, Kha'Zix etc.
    name = "".join(c for c in champ if c.isalnum() or c == ' ')
    name = name.replace(' ', '')
    # Capitalize first letter, rest lower, except for specific cases like KaiSa
    if name == 'Kaisa': return 'Kaisa'
    if name == 'Ksante': return 'KSante'
    # Add other specific normalizations if needed
    return name[0].upper() + name[1:] if len(name) > 1 else name.upper()

def get_champion_icon_html(champion, width=25, height=25):
    patch_version = get_latest_patch_version(); norm = normalize_champion_name_for_ddragon(champion)
    if norm: url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{norm}.png"; return f'<img src="{url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return ""
def color_win_rate_scrims(value):
    try:
        v = float(value)
        if 0 <= v < 48: return f'<span style="color:#FF7F7F; font-weight:bold;">{v:.1f}%</span>'
        elif 48 <= v <= 52: return f'<span style="color:#FFD700; font-weight:bold;">{v:.1f}%</span>'
        elif v > 52: return f'<span style="color:#90EE90; font-weight:bold;">{v:.1f}%</span>'
        else: return f'{value}'
    except (ValueError, TypeError): return f'{value}'

# --- Google Sheets Setup (Без изменений) ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation (Модифицирована для нового заголовка) ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    try:
        wks = spreadsheet.worksheet(name)
        try:
            current_header = wks.row_values(1)
            # --- ИЗМЕНЕНО: Проверка соответствия новому SCRIMS_HEADER ---
            if current_header != SCRIMS_HEADER:
                st.warning(f"Worksheet '{name}' header mismatch. Found {len(current_header)} cols, expected {len(SCRIMS_HEADER)}. "
                           f"Attempting to clear and recreate with correct header. BACKUP YOUR DATA FIRST!")
                # !!! ОСТОРОЖНО: Следующие строки удалят существующие данные и создадут новый лист !!!
                # !!! РАСКОММЕНТИРУЙТЕ ТОЛЬКО ЕСЛИ УВЕРЕНЫ !!!
                # spreadsheet.del_worksheet(wks)
                # st.info(f"Deleted worksheet '{name}' due to header mismatch.")
                # raise gspread.exceptions.WorksheetNotFound # Вызвать ошибку, чтобы создать заново
                # --- Вместо удаления, можно просто вывести предупреждение и не продолжать ---
                st.error("Header mismatch detected. Please manually fix the header or delete the sheet to allow recreation.")
                return None # Не возвращать лист, если заголовок неверный

        except Exception as header_exc:
             st.warning(f"Could not verify header for worksheet '{name}': {header_exc}")
             # Считаем, что заголовок может быть неверным
             st.error("Could not verify header. Please check the sheet manually.")
             return None

    except gspread.exceptions.WorksheetNotFound:
        try:
            cols_needed = len(SCRIMS_HEADER)
            # Создаем с запасом колонок
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=max(cols_needed, 40))
            wks.append_row(SCRIMS_HEADER, value_input_option='USER_ENTERED')
            wks.format(f'A1:{gspread.utils.rowcol_to_a1(1, cols_needed)}', {'textFormat': {'bold': True}})
            st.info(f"Created worksheet '{name}' with new structure.")
        except Exception as e:
            st.error(f"Error creating worksheet '{name}': {e}")
            return None
    except Exception as e:
        st.error(f"Error accessing worksheet '{name}': {e}")
        return None
    return wks

# --- GRID API Functions ---

# get_all_series (Без изменений, она работает корректно)
@st.cache_data(ttl=300)
def get_all_series(_debug_placeholder=None):
    internal_logs = []
    headers = {"x-api-key": GRID_API_KEY, "Content-Type": "application/json"}
    query = """
        query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
          allSeries(
            filter: $filter, first: $first, after: $after,
            orderBy: $orderBy, orderDirection: $orderDirection
          ) {
            totalCount,
            pageInfo { hasNextPage, endCursor },
            edges {
              node {
                id,
                startTimeScheduled
                # Можно добавить другие поля серии при необходимости, например, tournament { id name }
              }
            }
          }
        }
    """
    # Фильтр для LoL (ID 3) и только SCRIM
    start_thresh = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables = {
        "filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_thresh}},
        "first": 50, "orderBy": "StartTimeScheduled", "orderDirection": "DESC"
    }

    nodes = []
    next_pg, cursor, pg_num, max_pg = True, None, 1, 20 # Ограничение пагинации

    while next_pg and pg_num <= max_pg:
        curr_vars = variables.copy()
        if cursor: curr_vars["after"] = cursor
        try:
            resp = requests.post(f"{GRID_BASE_URL}central-data/graphql", headers=headers, json={"query": query, "variables": curr_vars}, timeout=20)
            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                st.error(f"GraphQL Error (Page {pg_num}): {data['errors']}")
                internal_logs.append(f"GraphQL Error (Page {pg_num}): {data['errors']}"); break

            s_data = data.get("data", {}).get("allSeries", {}); edges = s_data.get("edges", [])
            nodes.extend([s["node"] for s in edges if "node" in s])

            info = s_data.get("pageInfo", {}); next_pg = info.get("hasNextPage", False); cursor = info.get("endCursor");
            pg_num += 1; time.sleep(0.3) # Небольшая задержка между страницами
        except requests.exceptions.RequestException as e:
            st.error(f"Network error fetching series page {pg_num}: {e}")
            internal_logs.append(f"Network error fetching series page {pg_num}: {e}"); break
        except Exception as e:
             st.error(f"Unexpected error fetching series page {pg_num}: {e}")
             internal_logs.append(f"Unexpected error fetching series page {pg_num}: {e}"); break

    if internal_logs: st.warning("get_all_series encountered issues. Check logs.")
    # print(f"DEBUG: get_all_series finished. Total nodes retrieved: {len(nodes)}")
    return nodes

# --- НОВАЯ ФУНКЦИЯ: Для GraphQL запросов к Series State API ---
def post_graphql_request(api_key, query, endpoint="live-data-feed/series-state/graphql", variables=None, logs=None):
    """ Отправляет POST запрос с GraphQL query к указанному эндпоинту. """
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json"
    }
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    # Добавляем лог перед запросом
    if logs is not None:
        log_msg = f"GraphQL POST to {endpoint}. Query starts: {query[:100].strip()}..."
        if variables:
            log_msg += f" Vars: {variables}"
        logs.append(log_msg)

    try_count = 0
    while True:
        if try_count >= 3: # Уменьшаем количество попыток для GraphQL
            if logs is not None: logs.append(f"GraphQL Error: Request failed too many times ({endpoint})")
            return None
        try:
            response = requests.post(
                f"{GRID_BASE_URL}{endpoint}",
                headers=headers,
                json=payload, # Используем json=payload для requests
                timeout=20 # Увеличим таймаут для GraphQL
            )
            response.raise_for_status() # Проверяем HTTP ошибки
            data = response.json()

            if "errors" in data and data["errors"]:
                error_message = data["errors"][0].get("message", "Unknown GraphQL error")
                if logs is not None: logs.append(f"GraphQL Error response: {error_message}")
                # Проверяем специфичную ошибку "entity not found"
                if "could not find entity" in error_message.lower():
                     return {"data": None, "not_found": True} # Возвращаем флаг, что сущность не найдена
                return None # Другие GraphQL ошибки

            if logs is not None: logs.append(f"GraphQL Success ({endpoint})")
            return data # Возвращаем успешный результат

        except requests.exceptions.HTTPError as http_err:
            # Обработка 429 (Rate Limit) отдельно, если API его возвращает для POST
            if http_err.response.status_code == 429:
                retry_after = int(http_err.response.headers.get("Retry-After", "3"))
                if logs is not None: logs.append(f"GraphQL Warn: 429 Rate limit. Wait {retry_after}s")
                st.toast(f"Wait {retry_after}s...")
                time.sleep(retry_after)
                try_count += 1
                continue
            # Обработка 404 (Not Found)
            elif http_err.response.status_code == 404:
                 if logs is not None: logs.append(f"GraphQL Error: 404 Not Found for {endpoint}. Variables: {variables}")
                 return {"data": None, "not_found": True} # Возвращаем флаг
            else:
                 if logs is not None: logs.append(f"GraphQL HTTP Error: {http_err}")
                 # Можно добавить паузу перед повторной попыткой
                 time.sleep(1 + try_count)
                 try_count += 1
                 continue
        except requests.exceptions.RequestException as req_err:
            if logs is not None: logs.append(f"GraphQL Network Error: {req_err}")
            time.sleep(1 + try_count)
            try_count += 1
            continue
        except json.JSONDecodeError as json_err:
            if logs is not None: logs.append(f"GraphQL JSON Decode Error: {json_err}. Response text: {response.text[:200]}")
            return None # Не можем распарсить ответ
        except Exception as e:
            if logs is not None: logs.append(f"GraphQL Unexpected Error: {e}")
            return None # Неизвестная ошибка

# --- НОВАЯ ФУНКЦИЯ: Получение списка игр и их sequenceNumber ---
SERIES_STATE_GAMES_QUERY = """
    query GetSeriesGames($seriesId: ID!) {
        seriesState (
            id: $seriesId
        ) {
            id
            games {
                id
                sequenceNumber
                # Можно добавить finished: finished # если нужно фильтровать только завершенные игры
            }
        }
    }
"""
def get_game_details_from_series(series_id, api_key, logs):
    """ Получает список игр (id и sequenceNumber) для указанной серии. """
    variables = {"seriesId": str(series_id)} # Передаем ID как строку, на всякий случай
    response_data = post_graphql_request(api_key, SERIES_STATE_GAMES_QUERY, variables=variables, logs=logs)

    if response_data and "data" in response_data:
        if response_data.get("not_found"): # Проверяем флаг not_found
            logs.append(f"Info: Series State not found for series {series_id}. Skipping games.")
            return []
        series_state = response_data["data"].get("seriesState")
        if series_state and "games" in series_state:
            games_list = series_state["games"]
            # Оставляем только необходимые поля 'id' и 'sequenceNumber'
            # И проверяем их наличие и тип
            valid_games = []
            for game in games_list:
                game_id = game.get("id")
                seq_num = game.get("sequenceNumber")
                if game_id and isinstance(seq_num, int):
                     valid_games.append({"id": str(game_id), "sequenceNumber": seq_num})
                else:
                     logs.append(f"Warn: Invalid game data in series {series_id}: {game}")
            logs.append(f"Info: Found {len(valid_games)} valid games for series {series_id}")
            return valid_games
        else:
            logs.append(f"Warn: No 'games' found in seriesState for {series_id}. Response data: {response_data['data']}")
            return []
    elif response_data and response_data.get("not_found"):
         logs.append(f"Info: Series State explicitly not found (404 or specific error) for series {series_id}.")
         return []
    else:
        logs.append(f"Error: Failed to get games list for series {series_id}. Full response: {response_data}")
        return []

# --- ИЗМЕНЕНА: Функция скачивания данных игры (использует документированный эндпоинт) ---
def download_game_summary_data(series_id, sequence_number, api_key, logs, max_ret=3, delay_init=2):
    """ Скачивает файл статистики игры (/summary) по series_id и sequence_number. """
    endpoint = f"file-download/end-state/riot/series/{series_id}/games/{sequence_number}/summary"
    request_url = f"{GRID_BASE_URL}{endpoint}"
    headers = {"x-api-key": api_key}
    # Указываем Accept: application/json, так как ожидаем JSON
    headers["Accept"] = "application/json"

    log_prefix = f"GameDl (s:{series_id}, g#:{sequence_number})" # Префикс для логов

    for att in range(max_ret):
        try:
            # Увеличим таймаут для скачивания файлов
            resp = requests.get(request_url, headers=headers, timeout=30)

            if resp.status_code == 200:
                try:
                    # Пытаемся декодировать как JSON
                    game_data = resp.json()
                    logs.append(f"Success: {log_prefix} downloaded.")
                    return game_data
                except json.JSONDecodeError:
                    logs.append(f"Error: {log_prefix} JSONDecodeError. Content: {resp.text[:200]}...")
                    return None # Не удалось распарсить JSON
                except Exception as e:
                     logs.append(f"Error: {log_prefix} Parsing error {e}")
                     return None

            elif resp.status_code == 429:
                dly = delay_init * (2**att)
                retry_after_header = resp.headers.get("Retry-After")
                if retry_after_header:
                    try: dly = max(dly, int(retry_after_header))
                    except ValueError: pass # Используем расчетную задержку, если заголовок некорректен
                logs.append(f"Warn: {log_prefix} 429 Rate limit. Wait {dly}s")
                st.toast(f"Rate limit, wait {dly}s...")
                time.sleep(dly)
                continue # Повторная попытка

            elif resp.status_code == 404:
                logs.append(f"Info: {log_prefix} 404 Not Found.")
                # 404 для файла игры - это нормально, если игра еще не обработана или данных нет
                return None # Не считать ошибкой, просто данных нет

            elif resp.status_code == 403:
                logs.append(f"Error: {log_prefix} 403 Forbidden. Check API Key permissions.")
                st.error(f"Access Forbidden for {log_prefix}. Check API Key permissions.")
                return None # Прекращаем попытки для этой игры

            elif resp.status_code == 401:
                logs.append(f"Error: {log_prefix} 401 Unauthorized. Check API Key value.")
                st.error(f"Unauthorized for {log_prefix}. Check API Key.")
                return None # Прекращаем попытки для этой игры

            else:
                # Другие HTTP ошибки
                logs.append(f"Error: {log_prefix} HTTP {resp.status_code}. Content: {resp.text[:200]}...")
                # Добавляем небольшую паузу перед повторной попыткой для общих ошибок
                time.sleep(delay_init * (att + 1))
                continue # Повторная попытка

        except requests.exceptions.Timeout:
            logs.append(f"Warn: {log_prefix} Timeout on attempt {att + 1}/{max_ret}.")
            if att < max_ret - 1:
                time.sleep(delay_init * (2**att)) # Экспоненциальная задержка при таймауте
                continue
            else:
                st.error(f"Timeout error for {log_prefix} after {max_ret} attempts.")
                return None
        except requests.exceptions.RequestException as e:
            logs.append(f"Error: {log_prefix} Network error on attempt {att + 1}/{max_ret}: {e}")
            if att < max_ret - 1:
                time.sleep(delay_init * (2**att))
                continue
            else:
                st.error(f"Network error for {log_prefix} after {max_ret} attempts.")
                return None
        except Exception as e:
             logs.append(f"Error: {log_prefix} Unexpected error: {e}")
             return None # Непредвиденная ошибка

    logs.append(f"Error: {log_prefix} Failed after {max_ret} attempts.")
    return None # Если все попытки не удались

# --- СИЛЬНО ПЕРЕРАБОТАНА: Основная функция обновления данных ---
def update_scrims_data(worksheet, series_list, api_key, debug_logs, progress_bar):
    if not worksheet:
        st.error("Invalid Worksheet object.")
        return False
    if not series_list:
        st.info("No series found to process.")
        return False

    try:
        existing_data = worksheet.get_all_values()
        # --- ИЗМЕНЕНО: Используем индекс 1 для Match ID ---
        existing_ids = set(row[1] for row in existing_data[1:] if len(row) > 1 and row[1]) if len(existing_data) > 1 else set()
        debug_logs.append(f"Found {len(existing_ids)} existing Match IDs in the sheet.")
    except gspread.exceptions.APIError as api_err:
        st.error(f"GSpread API Error reading sheet: {api_err}")
        debug_logs.append(f"GSpread Error: {api_err}")
        return False
    except Exception as e:
        st.error(f"Error reading existing sheet data: {e}")
        debug_logs.append(f"Read Sheet Error: {e}")
        return False

    new_rows = []
    stats = defaultdict(int) # Упрощенная статистика
    stats["series_input"] = len(series_list)
    total_series = len(series_list)
    processed_games_count = 0 # Считаем обработанные игры, а не серии

    # --- НАЧАЛО ЦИКЛА ПО СЕРИЯМ ---
    for i, s_summary in enumerate(series_list):
        series_id = s_summary.get("id")
        if not series_id:
            stats["skipped_invalid_series_summary"] += 1
            continue

        prog = (i + 1) / total_series
        try:
            progress_bar.progress(prog, text=f"Processing Series {i+1}/{total_series} (ID: {series_id})")
        except Exception: pass

        # Добавляем задержку между обработкой серий
        if i > 0: time.sleep(API_REQUEST_DELAY * 0.5)

        stats["series_processed"] += 1
        debug_logs.append(f"--- Processing Series {series_id} ---")

        # 1. Получаем список игр (id и sequenceNumber) для этой серии
        games_in_series = get_game_details_from_series(series_id, api_key, debug_logs)

        if not games_in_series:
            stats["series_skipped_no_games"] += 1
            debug_logs.append(f"Info: No games found or error fetching games for series {series_id}.")
            continue # Переходим к следующей серии

        # --- НАЧАЛО ЦИКЛА ПО ИГРАМ ВНУТРИ СЕРИИ ---
        for game_details in games_in_series:
            # Добавляем блок try...except вокруг обработки каждой игры
            game_id = None # Инициализируем на случай ошибки до присваивания
            sequence_number = None
            try:
                game_id = game_details.get("id")
                sequence_number = game_details.get("sequenceNumber")

                if not game_id or sequence_number is None:
                    stats["games_skipped_invalid_details"] += 1
                    debug_logs.append(f"Warn: Invalid game details received for series {series_id}: {game_details}")
                    continue

                # Match ID для таблицы и проверки дубликатов
                m_id = str(game_id)

                # Проверяем, не обработана ли уже эта игра
                if m_id in existing_ids:
                    stats["games_skipped_duplicate"] += 1
                    # debug_logs.append(f"Debug: Skipping game {m_id} (already exists).")
                    continue

                stats["games_attempted"] += 1
                debug_logs.append(f"Attempting to download game {m_id} (Series: {series_id}, SeqNum: {sequence_number})")

                # 2. Скачиваем данные КОНКРЕТНОЙ ИГРЫ (/summary)
                # Добавляем задержку перед скачиванием данных игры
                time.sleep(API_REQUEST_DELAY)
                game_state_data = download_game_summary_data(series_id, sequence_number, api_key, debug_logs)

                # 3. Проверяем, успешно ли скачались данные
                if not game_state_data or not isinstance(game_state_data, dict):
                    stats["games_skipped_download_fail"] += 1
                    debug_logs.append(f"Warn: Failed to download or parse game summary for {m_id}.")
                    continue # Переходим к следующей игре

                # 4. Парсим game_state_data (это и есть аналог GameState)
                # Извлекаем базовую информацию об игре
                game_date_str = game_state_data.get("startedAt", s_summary.get("startTimeScheduled", "")) # Используем время старта игры, если есть
                date_formatted = "N/A"
                if game_date_str:
                     try: date_formatted = datetime.fromisoformat(game_date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
                     except ValueError:
                          try: date_formatted = datetime.strptime(game_date_str.split('.')[0], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                          except: pass # Оставляем "N/A" если не получилось

                duration_sec = game_state_data.get("duration") # Длительность в ISO 8601 duration format (e.g., PT29M38.235S)
                duration_formatted = "N/A"
                if duration_sec:
                     try:
                         # Простой парсинг PT{minutes}M{seconds}S
                         total_seconds = 0
                         if isinstance(duration_sec, (int, float)): # Если вдруг пришло числом секунд
                              total_seconds = int(duration_sec)
                         elif isinstance(duration_sec, str) and duration_sec.startswith('PT'):
                              time_part = duration_sec[2:]
                              minutes = 0
                              seconds = 0
                              if 'M' in time_part:
                                   parts = time_part.split('M')
                                   minutes = int(parts[0])
                                   time_part = parts[1]
                              if 'S' in time_part:
                                   seconds = float(time_part.replace('S','')) # Секунды могут быть дробными
                              total_seconds = int(minutes * 60 + seconds)

                         if total_seconds >= 0:
                              minutes_dur, seconds_dur = divmod(total_seconds, 60)
                              duration_formatted = f"{minutes_dur}:{seconds_dur:02d}"
                     except Exception as dur_e:
                          debug_logs.append(f"Warn: Error parsing duration '{duration_sec}' for game {m_id}: {dur_e}")


                teams_data = game_state_data.get("teams", [])
                if len(teams_data) < 2:
                    stats["games_skipped_invalid_teams"] += 1
                    debug_logs.append(f"Warn: Less than 2 teams found in game {m_id}.")
                    continue

                # --- НАЧАЛО ИЗМЕНЕННОГО БЛОКА ИДЕНТИФИКАЦИИ КОМАНД ---
                blue_team_data = None
                red_team_data = None

                # Добавим отладочный вывод структуры teams_data для первых нескольких игр
                if stats["games_attempted"] <= 2: # Логируем только для первых 2 попыток
                    debug_logs.append(f"Debug: Teams data for game {m_id}: {teams_data}")
                    try:
                         # Используем st.text вместо st.code, чтобы избежать потенциальных ошибок рендеринга больших объектов
                         st.text(f"Debug Teams Data (Game {m_id}):\n{json.dumps(teams_data, indent=2)}")
                    except Exception as log_e: # На случай если teams_data не сериализуется или st.text не сработает
                         st.text(f"Debug Teams Data (Game {m_id}):\nCould not serialize/display: {log_e}\nData: {teams_data}")


                # Попытка 1: Найти по side == "blue" / "red"
                blue_team_data = next((t for t in teams_data if t.get("side") == "blue"), None)
                red_team_data = next((t for t in teams_data if t.get("side") == "red"), None)

                # Попытка 2: Если не найдено, пробуем по ID или side == 100 / 200
                if not blue_team_data or not red_team_data:
                    debug_logs.append(f"Debug: Could not find teams by side='blue'/'red' for game {m_id}. Trying by ID/Side 100/200.")
                    # Сначала ищем по ID
                    temp_blue = next((t for t in teams_data if str(t.get("id")) == "100"), None)
                    temp_red = next((t for t in teams_data if str(t.get("id")) == "200"), None)
                    # Если не нашли по ID, пробуем по side == '100'/'200' (как строка)
                    if not temp_blue:
                         temp_blue = next((t for t in teams_data if str(t.get("side")) == "100"), None)
                    if not temp_red:
                         temp_red = next((t for t in teams_data if str(t.get("side")) == "200"), None)

                    # Присваиваем, только если нашли ОБЕ команды этим способом
                    if temp_blue and temp_red:
                         blue_team_data = temp_blue
                         red_team_data = temp_red
                         debug_logs.append(f"Debug: Found teams by ID/Side 100/200 for game {m_id}.")


                # Попытка 3: Если все еще не найдено и ровно 2 команды, пробуем по порядку (0=blue, 1=red)
                if (not blue_team_data or not red_team_data) and len(teams_data) == 2:
                     debug_logs.append(f"Warn: Could not find teams by side or ID for game {m_id}. Falling back to order (0=blue, 1=red). This might be incorrect!")
                     # Важно: Проверяем, не перезапишем ли мы одну из команд, если одна нашлась, а другая нет
                     if blue_team_data is None and red_team_data is None:
                          blue_team_data = teams_data[0]
                          red_team_data = teams_data[1]
                          debug_logs.append(f"Debug: Assigned teams by order 0 and 1 for game {m_id}.")
                     elif blue_team_data is None and red_team_data is not teams_data[0]: # Если red нашлась и она не первая
                          blue_team_data = teams_data[0]
                          debug_logs.append(f"Debug: Assigned blue team by order 0 for game {m_id}.")
                     elif red_team_data is None and blue_team_data is not teams_data[1]: # Если blue нашлась и она не вторая
                          red_team_data = teams_data[1]
                          debug_logs.append(f"Debug: Assigned red team by order 1 for game {m_id}.")
                     else:
                          debug_logs.append(f"Warn: Could not assign teams by order due to potential conflict/partial match for game {m_id}.")


                # Финальная проверка: Если так и не смогли определить обе команды, пропускаем игру
                if not blue_team_data or not red_team_data:
                    stats["games_skipped_invalid_sides"] += 1
                    debug_logs.append(f"Error: Failed to identify blue AND red teams definitively for game {m_id}. Skipping. Teams data logged above if enabled.")
                    continue # Пропускаем игру, если команды не идентифицированы
                # --- КОНЕЦ ИЗМЕНЕННОГО БЛОКА ---

                # Теперь продолжаем использовать blue_team_data и red_team_data
                blue_team_name = blue_team_data.get("name", "Blue Team")
                red_team_name = red_team_data.get("name", "Red Team")
                blue_team_id = str(blue_team_data.get("id", ""))
                red_team_id = str(red_team_data.get("id", ""))

                # Определяем результат для НАШЕЙ команды
                result = "N/A"
                is_our_blue = (blue_team_id == OUR_TEAM_ID)
                is_our_red = (red_team_id == OUR_TEAM_ID)

                if is_our_blue:
                    result = "Win" if blue_team_data.get("won") else "Loss"
                elif is_our_red:
                    result = "Win" if red_team_data.get("won") else "Loss"
                else:
                     debug_logs.append(f"Warn: Game {m_id} does not seem to involve OUR_TEAM_ID ({OUR_TEAM_ID}). Blue: {blue_team_id}, Red: {red_team_id}.")
                     result = "Draw/Unknown" # Или помечаем как ничью/неизвестно

                # Извлекаем драфт
                draft_actions = game_state_data.get("draftActions", [])
                b_bans, r_bans = ["N/A"]*5, ["N/A"]*5
                draft_picks_ordered = { "B1": "N/A", "R1": "N/A", "R2": "N/A", "B2": "N/A", "B3": "N/A", "R3": "N/A", "R4": "N/A", "B4": "N/A", "B5": "N/A", "R5": "N/A" }
                pick_map_seq_to_key = { 7: "B1", 8: "R1", 9: "R2", 10: "B2", 11: "B3", 12: "R3", 17: "R4", 18: "B4", 19: "B5", 20: "R5" }

                if draft_actions:
                    try: actions_sorted = sorted(draft_actions, key=lambda x: int(x.get("sequenceNumber", 99)))
                    except Exception: actions_sorted = draft_actions # Если sequenceNumber не число
                    bb, rb = 0, 0
                    processed_ban_seqs = set()
                    processed_pick_seqs = set()

                    for act in actions_sorted:
                        try:
                            seq_str = act.get("sequenceNumber")
                            if seq_str is None: continue
                            seq = int(seq_str)
                            act_type = act.get("type")
                            draftable = act.get("draftable")
                            champ = draftable.get("name", "N/A") if draftable else "N/A"
                            drafter_id = str(act.get("drafter", {}).get("id", ""))

                            if act_type == "ban" and champ != "N/A" and seq != -1 and seq not in processed_ban_seqs:
                                processed_ban_seqs.add(seq)
                                if drafter_id == blue_team_id:
                                    if bb < 5: b_bans[bb] = champ; bb += 1
                                elif drafter_id == red_team_id:
                                     if rb < 5: r_bans[rb] = champ; rb += 1
                                else:
                                     debug_logs.append(f"Warn: Ban seq {seq} in game {m_id} by unknown drafter {drafter_id}")

                            elif act_type == "pick" and champ != "N/A" and seq in pick_map_seq_to_key and seq not in processed_pick_seqs:
                                processed_pick_seqs.add(seq)
                                draft_picks_ordered[pick_map_seq_to_key[seq]] = champ

                        except (ValueError, TypeError) as parse_err:
                             debug_logs.append(f"Warn: Draft parse error seq {seq_str} in {m_id}: {parse_err}"); continue
                        except Exception as e:
                             debug_logs.append(f"Warn: Draft proc. error action {act.get('id')} in {m_id}: {e}"); continue

                # Извлекаем фактических чемпионов по ролям
                actual_champs = {"blue": {}, "red": {}}
                for role in ROLE_ORDER_FOR_SHEET:
                    role_short = role.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                    actual_champs["blue"][role_short] = "N/A"
                    actual_champs["red"][role_short] = "N/A"

                parse_success = True # Флаг успешного парсинга игроков/чемпионов
                for team_data in [blue_team_data, red_team_data]:
                    team_side = team_data.get("side") # Должен быть "blue" или "red", или None если определяли по порядку/ID
                    # Если side не 'blue'/'red', а например 100/200 или None, скорректируем
                    if str(team_data.get("id")) == blue_team_id or str(team_data.get("side")) == "100":
                         team_side_corrected = "blue"
                    elif str(team_data.get("id")) == red_team_id or str(team_data.get("side")) == "200":
                         team_side_corrected = "red"
                    else:
                         # Если определяли по порядку и side нет, нужно как-то решить
                         # Предположим, что blue_team_data это blue, red_team_data это red
                         if team_data is blue_team_data: team_side_corrected = "blue"
                         elif team_data is red_team_data: team_side_corrected = "red"
                         else: team_side_corrected = None # Не удалось определить

                    if not team_side_corrected:
                         debug_logs.append(f"Error: Could not determine corrected side for team {team_data.get('id')} in game {m_id}")
                         parse_success = False
                         continue # Пропускаем обработку этой команды

                    players_list = team_data.get("players", [])
                    target_champ_dict = actual_champs[team_side_corrected]
                    team_id = str(team_data.get("id", ""))
                    is_our_current_team = (team_id == OUR_TEAM_ID)

                    if is_our_current_team:
                        player_champion_map = {}
                        for p_state in players_list:
                            p_id = str(p_state.get("id", ""))
                            char_info = p_state.get("character")
                            champ_name = char_info.get("name", "N/A") if char_info and isinstance(char_info, dict) else "N/A"
                            if p_id: player_champion_map[p_id] = champ_name
                            if not char_info or champ_name == "N/A":
                                 debug_logs.append(f"Warn: Player {p_id} ({p_state.get('name')}) on OUR team in {m_id} missing character info.")
                                 # parse_success = False # Не будем считать это фатальной ошибкой парсинга

                        found_players_count = 0
                        for p_id_roster, role_full in PLAYER_ROLES_BY_ID.items():
                            role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                            if role_short in target_champ_dict:
                                champion = player_champion_map.get(p_id_roster, "N/A")
                                target_champ_dict[role_short] = champion
                                if p_id_roster in player_champion_map:
                                     found_players_count += 1
                                     if champion == "N/A":
                                          debug_logs.append(f"Warn: Our roster player {p_id_roster} found in game {m_id} but champ is N/A.")
                                          # parse_success = False
                                else:
                                     debug_logs.append(f"Warn: Our roster player {p_id_roster} NOT found in players list for game {m_id}. Side: {team_side_corrected}")
                                     parse_success = False # Считаем ошибкой, если наш игрок не найден

                        if found_players_count < 5:
                             debug_logs.append(f"Warn: Found only {found_players_count}/5 of our players in game {m_id}. Side: {team_side_corrected}")
                             parse_success = False # Считаем ошибкой, если не все наши игроки найдены

                    else: # Команда противника
                        player_role_map = {}
                        assigned_indices = set()
                        for idx, p_state in enumerate(players_list):
                             roles = p_state.get("roles", [])
                             char_info = p_state.get("character")
                             champ_name = char_info.get("name", "N/A") if char_info and isinstance(char_info, dict) else "N/A"
                             if not char_info or champ_name == "N/A":
                                  debug_logs.append(f"Warn: Opponent player {p_state.get('id')} ({p_state.get('name')}) in {m_id} missing character info.")

                             primary_role_found = False
                             for role_obj in roles:
                                  role_name_api = role_obj.get("name", "").upper()
                                  role_short = None
                                  if role_name_api == "TOP": role_short = "TOP"
                                  elif role_name_api == "JUNGLE": role_short = "JGL"
                                  elif role_name_api == "MID": role_short = "MID"
                                  elif role_name_api == "BOTTOM": role_short = "BOT"
                                  elif role_name_api == "SUPPORT": role_short = "SUP"
                                  elif role_name_api == "UTILITY": role_short = "SUP"

                                  if role_short and role_short in target_champ_dict and role_short not in player_role_map:
                                       player_role_map[role_short] = champ_name
                                       assigned_indices.add(idx)
                                       primary_role_found = True
                                       break
                             # Если роль не нашлась, игрок будет обработан по порядку

                        remaining_roles = [r.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL") for r in ROLE_ORDER_FOR_SHEET if r.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL") not in player_role_map]
                        role_idx = 0
                        for idx, p_state in enumerate(players_list):
                             if idx not in assigned_indices and role_idx < len(remaining_roles):
                                  char_info = p_state.get("character")
                                  champ_name = char_info.get("name", "N/A") if char_info and isinstance(char_info, dict) else "N/A"
                                  target_role = remaining_roles[role_idx]
                                  # Назначаем чемпиона на соответствующую роль в словаре этой команды
                                  target_champ_dict[target_role] = champ_name
                                  role_idx += 1
                                  if role_idx + len(player_role_map) >= 5: break # Если уже заполнили 5 ролей

                        # Объединяем роли, найденные явно, и роли, назначенные по порядку
                        for role_short, champ_name in player_role_map.items():
                             target_champ_dict[role_short] = champ_name

                        if len(target_champ_dict) < 5 or any(v == "N/A" for v in target_champ_dict.values()):
                             debug_logs.append(f"Warn: Could not assign all opponent roles/champions for game {m_id}. Side: {team_side_corrected}. Result: {target_champ_dict}")
                             # parse_success = False # Не будем считать фатальной ошибкой

                # Если парсинг нашей команды прошел неуспешно, пропускаем игру
                if not parse_success:
                    stats["games_skipped_parse_fail"] += 1
                    debug_logs.append(f"Error: Failed to parse OUR players/champions completely for game {m_id}. Skipping row.")
                    continue

                # Формируем строку для записи
                new_row_data = [
                    date_formatted, m_id, sequence_number, blue_team_name, red_team_name,
                    *b_bans, *r_bans,
                    draft_picks_ordered["B1"], draft_picks_ordered["R1"], draft_picks_ordered["R2"],
                    draft_picks_ordered["B2"], draft_picks_ordered["B3"], draft_picks_ordered["R3"],
                    draft_picks_ordered["R4"], draft_picks_ordered["B4"], draft_picks_ordered["B5"], draft_picks_ordered["R5"],
                    actual_champs["blue"]["TOP"], actual_champs["blue"]["JGL"], actual_champs["blue"]["MID"], actual_champs["blue"]["BOT"], actual_champs["blue"]["SUP"],
                    actual_champs["red"]["TOP"], actual_champs["red"]["JGL"], actual_champs["red"]["MID"], actual_champs["red"]["BOT"], actual_champs["red"]["SUP"],
                    duration_formatted, result
                ]

                if len(new_row_data) != len(SCRIMS_HEADER):
                    debug_logs.append(f"Error: Row length mismatch for game {m_id}. Expected {len(SCRIMS_HEADER)}, got {len(new_row_data)}. Row: {new_row_data}")
                    stats["games_skipped_row_mismatch"] += 1
                    continue

                new_rows.append(new_row_data)
                existing_ids.add(m_id)
                stats["games_processed_success"] += 1
                processed_games_count += 1
                debug_logs.append(f"Success: Prepared row for game {m_id}.")

            except Exception as parse_e:
                 # Общий обработчик ошибок парсинга для данной игры
                 stats["games_skipped_parse_fail"] += 1
                 # Используем repr(parse_e) для более детального вывода ошибки
                 debug_logs.append(f"Error: Failed to parse game {m_id} data: {repr(parse_e)}. Data keys: {list(game_state_data.keys()) if isinstance(game_state_data,dict) else 'N/A'}")
                 # Можно добавить вывод traceback для глубокой отладки, если потребуется
                 # import traceback
                 # debug_logs.append(f"Traceback: {traceback.format_exc()}")
                 continue # Переходим к следующей игре

        # --- КОНЕЦ ЦИКЛА ПО ИГРАМ ВНУТРИ СЕРИИ ---
        debug_logs.append(f"--- Finished Processing Series {series_id} ---")

    # --- КОНЕЦ ЦИКЛА ПО СЕРИЯМ ---

    # Вывод статистики
    progress_bar.progress(1.0, text="Update complete. Checking results...")
    summary = [
        f"\n--- Update Summary ---",
        f"Input Series: {stats['series_input']}",
        f"Series Processed: {stats['series_processed']}",
        f"Series Skipped (Invalid Summary): {stats['skipped_invalid_series_summary']}",
        f"Series Skipped (No Games Found): {stats['series_skipped_no_games']}",
        f"Total Games Attempted: {stats['games_attempted']}",
        f"Games Skipped (Duplicate): {stats['games_skipped_duplicate']}",
        f"Games Skipped (Invalid Details): {stats['games_skipped_invalid_details']}",
        f"Games Skipped (Download Fail): {stats['games_skipped_download_fail']}", # Исправлено имя ключа
        f"Games Skipped (Parse Fail): {stats['games_skipped_parse_fail']}", # Исправлено имя ключа
        f"Games Skipped (Invalid Teams/Sides): {stats['games_skipped_invalid_teams'] + stats['games_skipped_invalid_sides']}",
        f"Games Skipped (Row Length Mismatch): {stats['games_skipped_row_mismatch']}",
        f"Games Processed Successfully: {stats['games_processed_success']}",
        f"New Records Added to Sheet: {len(new_rows)}"
    ]
    if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
    st.session_state.scrims_update_logs.extend(debug_logs[-50:])
    st.session_state.scrims_update_logs.extend(summary)
    st.session_state.scrims_update_logs = st.session_state.scrims_update_logs[-100:]

    st.code("\n".join(summary), language=None)

    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            st.success(f"Added {len(new_rows)} new records to '{worksheet.title}'.")
            try: aggregate_scrims_data.clear()
            except AttributeError: pass
            return True
        except gspread.exceptions.APIError as api_err:
            error_msg = f"GSpread API Error appending rows: {api_err}"; debug_logs.append(error_msg); st.error(error_msg); st.error(f"Failed to add {len(new_rows)} rows.")
            return False
        except Exception as e:
            error_msg = f"Error appending rows: {e}"; debug_logs.append(error_msg); st.error(error_msg); st.error(f"Failed to add {len(new_rows)} rows.")
            return False
    else:
        st.info("No new valid records found to add.")
        if stats["series_processed"] > 0 and stats["games_processed_success"] == 0 and stats["games_attempted"] > 0 :
             st.warning(f"Attempted to process {stats['games_attempted']} games, but none were added successfully. Check logs for reasons (duplicates, download failures, parsing errors).")
        elif stats["series_processed"] == 0 and stats["series_input"] > 0:
              st.warning("No series were processed. Check logs for initial errors.")
        return False
# --- Конец функции update_scrims_data ---

# --- Функция агрегации (остается в основном без изменений, но проверим индексы колонок) ---
# @st.cache_data(ttl=600) # Можно вернуть кэширование, если нужно
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]), {}

    blue_stats = {"wins": 0, "losses": 0, "total": 0}
    red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0}))
    expected_cols = len(SCRIMS_HEADER) # Используем актуальный заголовок

    now = datetime.utcnow()
    time_threshold = None
    if time_filter != "All Time":
        weeks_map = {"1 Week": 1, "2 Weeks": 2, "3 Weeks": 3, "4 Weeks": 4}
        days_map = {"2 Months": 60}
        if time_filter in weeks_map: time_threshold = now - timedelta(weeks=weeks_map[time_filter])
        elif time_filter in days_map: time_threshold = now - timedelta(days=days_map[time_filter])

    try:
        data = worksheet.get_all_values()
    except Exception as e:
        st.error(f"Read error during aggregation: {e}")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]), {}

    if len(data) <= 1:
        st.info(f"No data in '{worksheet.title}' for aggregation matching '{time_filter}'.")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]), {}

    header = data[0]
    if header != SCRIMS_HEADER:
        st.error(f"Header mismatch in '{worksheet.title}' during aggregation.")
        st.code(f"Expected: {SCRIMS_HEADER}\nFound:    {header}", language=None)
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]), {}

    try:
        idx = {name: i for i, name in enumerate(SCRIMS_HEADER)}
    except Exception as e:
         st.error(f"Failed to create column index map: {e}")
         return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]), {}

    rows_processed_after_filter = 0
    for row_index, row in enumerate(data[1:], start=2):
        if len(row) != expected_cols: continue # Пропускаем строки неправильной длины
        try:
            date_str = row[idx["Date"]]
            if time_threshold and date_str != "N/A":
                try:
                    date_obj = datetime.strptime(date_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                    if date_obj < time_threshold: continue
                except ValueError: continue

            rows_processed_after_filter += 1

            b_team, r_team, res = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]
            is_our_blue = (b_team == TEAM_NAME)
            is_our_red = (r_team == TEAM_NAME)
            if not (is_our_blue or is_our_red): continue

            is_our_win = (is_our_blue and res == "Win") or (is_our_red and res == "Win")

            if is_our_blue:
                blue_stats["total"] += 1
                if res == "Win": blue_stats["wins"] += 1
                elif res == "Loss": blue_stats["losses"] += 1
            else:
                red_stats["total"] += 1
                if res == "Win": red_stats["wins"] += 1
                elif res == "Loss": red_stats["losses"] += 1

            # Статистика игроков (читаем из Actual_*)
            side_prefix = "Blue" if is_our_blue else "Red"
            for player_id_roster, role_full in PLAYER_ROLES_BY_ID.items():
                player_name = PLAYER_IDS.get(player_id_roster)
                if player_name:
                    role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                    actual_champ_col_name = f"Actual_{side_prefix}_{role_short}"
                    # --- ИЗМЕНЕНО: Проверяем наличие колонки в индексе ---
                    if actual_champ_col_name in idx:
                         champion = row[idx[actual_champ_col_name]]
                         if champion and champion != "N/A" and champion.strip() != "":
                             player_stats[player_name][champion]['games'] += 1
                             if is_our_win:
                                 player_stats[player_name][champion]['wins'] += 1
                    else:
                         st.warning(f"Column {actual_champ_col_name} not found in header index during aggregation.")


            # История матчей (используем иконки чемпионов из Actual_*)
            # Собираем HTML для пиков и банов из колонок Actual_* и Ban_*
            bb_html = " ".join(get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6) if idx.get(f"Blue Ban {i}") is not None and row[idx[f"Blue Ban {i}"]] != "N/A")
            rb_html = " ".join(get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6) if idx.get(f"Red Ban {i}") is not None and row[idx[f"Red Ban {i}"]] != "N/A")
            # Собираем пики из Actual_* колонок
            bp_html = ""
            rp_html = ""
            for role in ROLE_ORDER_FOR_SHEET:
                 role_short = role.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                 blue_col = f"Actual_Blue_{role_short}"
                 red_col = f"Actual_Red_{role_short}"
                 if blue_col in idx and row[idx[blue_col]] != "N/A":
                      bp_html += get_champion_icon_html(row[idx[blue_col]]) + " "
                 if red_col in idx and row[idx[red_col]] != "N/A":
                      rp_html += get_champion_icon_html(row[idx[red_col]]) + " "

            history_rows.append({
                "Date": date_str,
                "Blue Team": b_team,
                "B Bans": bb_html.strip(),
                "B Picks": bp_html.strip(), # Фактические пики
                "Result": res,
                "Duration": row[idx["Duration"]],
                "R Picks": rp_html.strip(), # Фактические пики
                "R Bans": rb_html.strip(),
                "Red Team": r_team,
                "Match ID": row[idx["Match ID"]],
                "Game SeqNum": row[idx["Game SeqNum"]] # Добавили номер игры
            })

        except IndexError as e_idx:
            st.warning(f"Skipping row {row_index} due to IndexError: {e_idx}. Check row length.")
            continue
        except Exception as e_inner:
            st.warning(f"Skipping row {row_index} due to error: {e_inner}")
            continue

    if rows_processed_after_filter == 0 and time_filter != "All Time":
        st.info(f"No scrim data found for the selected period: {time_filter}")
        return {}, {}, pd.DataFrame(columns=SCRIMS_HEADER), {} # Используем новый заголовок

    df_hist = pd.DataFrame(history_rows)
    if not df_hist.empty:
        try:
            df_hist['DT_temp'] = pd.to_datetime(df_hist['Date'], errors='coerce')
            df_hist.dropna(subset=['DT_temp'], inplace=True)
            df_hist = df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception as sort_ex:
             st.warning(f"Could not sort match history by date: {sort_ex}")

    final_player_stats = {player: dict(champions) for player, champions in player_stats.items()}
    for player in final_player_stats:
        final_player_stats[player] = dict(sorted(
            final_player_stats[player].items(),
            key=lambda item: item[1].get('games', 0),
            reverse=True
        ))

    if not final_player_stats and rows_processed_after_filter > 0:
         st.info(f"Processed {rows_processed_after_filter} scrims for '{time_filter}', but no player champion stats generated.")

    return blue_stats, red_stats, df_hist, final_player_stats
# --- Конец функции aggregate_scrims_data ---


# --- Основная функция страницы scrims_page (без существенных изменений, но передает api_key) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    if st.button("⬅️ Back to HLL Stats"): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()

    client = setup_google_sheets();
    if not client: st.error("GSheets client failed."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Sheet access error: {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: st.error(f"Worksheet access error or header mismatch prevents updates."); return # Не продолжаем, если лист некорректен

    with st.expander("Update Scrim Data", expanded=False):
        # Используем ключ API из констант
        api_key_to_use = GRID_API_KEY
        if not api_key_to_use:
             st.error("GRID_API_KEY is not set!")
        else:
            # Инициализируем логи в session_state при первом запуске или если их нет
            if 'scrims_update_logs' not in st.session_state:
                 st.session_state.scrims_update_logs = []

            if st.button("Download & Update from GRID API", key="update_scrims_btn"):
                # Очищаем старые логи перед новым запуском
                st.session_state.scrims_update_logs = []
                logs = st.session_state.scrims_update_logs # Ссылка на список логов
                with st.spinner("Fetching series list..."):
                    series_list = get_all_series(logs) # Передаем logs
                if series_list:
                    st.info(f"Found {len(series_list)} recent series. Processing games...")
                    progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting...")
                    try:
                        # --- ИЗМЕНЕНО: Передаем api_key и logs в update_scrims_data ---
                        data_added = update_scrims_data(wks, series_list, api_key_to_use, logs, progress_bar)
                        if data_added:
                            try: aggregate_scrims_data.clear() # Очищаем кэш аггрегации
                            except AttributeError: pass
                    except Exception as e:
                        st.error(f"Update process failed: {e}")
                        logs.append(f"FATAL UPDATE ERROR: {e}")
                    finally:
                        progress_bar_placeholder.empty()
                else: st.warning("No recent series found or failed to fetch series list.")

            # Отображаем логи ПОСЛЕ кнопки и возможного обновления
            if st.session_state.scrims_update_logs:
                st.subheader("Update Logs:")
                st.code("\n".join(st.session_state.scrims_update_logs[-150:]), language=None) # Показываем последние N логов
            else:
                 st.info("Logs will appear here after running the update.")


    st.divider(); st.subheader("Scrim Performance")
    time_f = st.selectbox("Filter by Time:", ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # Вызываем aggregate_scrims_data
    # Добавили обработку возможного None от check_if_scrims_worksheet_exists
    if wks:
         blue_s, red_s, df_hist, player_champ_stats = aggregate_scrims_data(wks, time_f)
    else: # Если лист некорректен, показываем пустые данные
         blue_s, red_s, df_hist, player_champ_stats = {}, {}, pd.DataFrame(), {}
         st.error("Cannot display statistics because the worksheet is missing or has an incorrect header.")


    # --- Отображение статистики и истории (без изменений) ---
    try:
        games_f = blue_s.get("total", 0) + red_s.get("total", 0) # Используем .get с default=0
        wins_f = blue_s.get("wins", 0) + red_s.get("wins", 0)
        loss_f = blue_s.get("losses", 0) + red_s.get("losses", 0)
        st.markdown(f"**Performance ({time_f})**"); co, cb, cr = st.columns(3)
        with co: wr = (wins_f / games_f * 100) if games_f > 0 else 0; st.metric("Total Games", games_f); st.metric("Overall WR", f"{wr:.1f}%", f"{wins_f}W-{loss_f}L")
        with cb: bwr = (blue_s.get("wins", 0) / blue_s.get("total", 1) * 100) if blue_s.get("total", 0) > 0 else 0; st.metric("Blue WR", f"{bwr:.1f}%", f"{blue_s.get('wins',0)}W-{blue_s.get('losses',0)}L ({blue_s.get('total',0)} G)")
        with cr: rwr = (red_s.get("wins", 0) / red_s.get("total", 1) * 100) if red_s.get("total", 0) > 0 else 0; st.metric("Red WR", f"{rwr:.1f}%", f"{red_s.get('wins',0)}W-{red_s.get('losses',0)}L ({red_s.get('total',0)} G)")
    except Exception as e: st.error(f"Error display summary: {e}")

    st.divider()

    tab1, tab2 = st.tabs(["📜 Match History", "📊 Player Champion Stats"])

    with tab1:
        st.subheader(f"Match History ({time_f})")
        if not df_hist.empty:
            # --- ИЗМЕНЕНО: Убедимся, что колонки для to_html существуют ---
            display_cols = ["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID", "Game SeqNum"]
            # Фильтруем df_hist, оставляя только существующие колонки из display_cols
            cols_to_display_in_df = [col for col in display_cols if col in df_hist.columns]
            st.markdown(df_hist[cols_to_display_in_df].to_html(escape=False, index=False, classes='compact-table history-table', justify='center'), unsafe_allow_html=True)
        else:
            st.info(f"No match history for {time_f}.")

    with tab2:
        st.subheader(f"Player Champion Stats ({time_f})")
        if not player_champ_stats:
             st.info(f"No player champion stats available for {time_f}.")
        else:
             player_order = [PLAYER_IDS[pid] for pid in ["26433", "25262", "25266", "20958", "21922"] if pid in PLAYER_IDS]
             player_cols = st.columns(len(player_order))

             for i, player_name in enumerate(player_order):
                 with player_cols[i]:
                     player_role = "Unknown"
                     for pid, role in PLAYER_ROLES_BY_ID.items():
                         if PLAYER_IDS.get(pid) == player_name:
                             player_role = role
                             break
                     st.markdown(f"**{player_name}** ({player_role})")

                     player_data = player_champ_stats.get(player_name, {})
                     stats_list = []
                     if player_data:
                         for champ, stats in player_data.items():
                             games = stats.get('games', 0)
                             if games > 0:
                                 wins = stats.get('wins', 0)
                                 win_rate = round((wins / games) * 100, 1)
                                 stats_list.append({
                                     'Icon': get_champion_icon_html(champ, 20, 20),
                                     'Champion': champ,
                                     'Games': games,
                                     'WR%': win_rate
                                 })

                     if stats_list:
                         df_player = pd.DataFrame(stats_list).sort_values("Games", ascending=False).reset_index(drop=True)
                         df_player['WR%'] = df_player['WR%'].apply(color_win_rate_scrims)
                         st.markdown(
                              df_player.to_html(escape=False, index=False, columns=['Icon', 'Games', 'WR%'], classes='compact-table player-stats', justify='center'),
                              unsafe_allow_html=True
                         )
                     else:
                         st.caption("No stats.")


# --- Keep __main__ block as is ---
if __name__ == "__main__":
     # Эта часть не будет выполняться при импорте в app.py
     # Оставьте pass или добавьте код для локального тестирования scrims.py, если нужно
     pass
