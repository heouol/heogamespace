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
API_REQUEST_DELAY = 1.0

# --- ВАЖНО: Карта для сопоставления Riot Никнеймов с GRID ID ---
# Вам нужно поддерживать этот словарь в актуальном состоянии!
# Ключ = Riot Никнейм (riotIdGameName/summonerName из API), Значение = GRID ID игрока из PLAYER_IDS
OUR_PLAYER_RIOT_NAMES_TO_GRID_ID = {
    # Aytekn
    "AyteknnnN777": "26433",
    "Aytekn": "26433",
    "GSMC Aytekn": "26433", # <--- ДОБАВЛЕНО
    # Pallet
    "KC Bo": "25262",
    "yiqunsb": "25262",
    "Pallet": "25262",
    "GSMC Pallet": "25262", # <--- ДОБАВЛЕНО
    # Tsiperakos
    "Tsiperakos": "25266",
    "Tsiper": "25266",
    "GSMC Tsiperakos": "25266", # <--- ДОБАВЛЕНО
    # Kenal
    "Kenal": "20958",
    "Kaneki Kenal": "20958",
    "GSMC Kenal": "20958", # <--- ДОБАВЛЕНО
    # Centu
    "ΣΑΝ ΚΡΟΥΑΣΑΝ": "21922",
    "Aim First": "21922",
    "CENTU": "21922",
    "GSMC CENTU": "21922" # <--- ДОБАВЛЕНО
}
# -------------------------------------------------------------

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
SCRIMS_HEADER = [
    "Date", "Match ID", "Blue Team", "Red Team",
    "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
    "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
    # Пики по порядку драфта
    "Draft_Pick_B1", "Draft_Pick_R1", "Draft_Pick_R2",
    "Draft_Pick_B2", "Draft_Pick_B3", "Draft_Pick_R3",
    "Draft_Pick_R4", "Draft_Pick_B4", "Draft_Pick_B5", "Draft_Pick_R5",
    # Фактические чемпионы по ролям (из g_data)
    "Actual_Blue_TOP", "Actual_Blue_JGL", "Actual_Blue_MID", "Actual_Blue_BOT", "Actual_Blue_SUP",
    "Actual_Red_TOP", "Actual_Red_JGL", "Actual_Red_MID", "Actual_Red_BOT", "Actual_Red_SUP",
    # Стандартные колонки в конце
    "Duration", "Result"
]

# --- DDRagon Helper Functions (Без изменений) ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1"
    except Exception: return "14.14.1"
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    ex = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"};
    if champ in ex: return ex[champ]
    return "".join(c for c in champ if c.isalnum())
def get_champion_icon_html(champion, width=25, height=25):
    patch_version = get_latest_patch_version(); norm = normalize_champion_name_for_ddragon(champion)
    if norm: url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{norm}.png"; return f'<img src="{url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return ""
def color_win_rate_scrims(value):
    try:
        v = float(value)
        # --- ИСПРАВЛЕННЫЙ БЛОК ---
        if 0 <= v < 48:
            return f'<span style="color:#FF7F7F; font-weight:bold;">{v:.1f}%</span>'
        elif 48 <= v <= 52:
            return f'<span style="color:#FFD700; font-weight:bold;">{v:.1f}%</span>'
        elif v > 52:
            return f'<span style="color:#90EE90; font-weight:bold;">{v:.1f}%</span>'
        else:
            # Handle potential edge cases or NaN if needed
            return f'{value}' # Return original value if outside defined ranges
        # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---
    except (ValueError, TypeError):
        return f'{value}'

# --- Google Sheets Setup (Без изменений) ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation (Без изменений) ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    """
    Проверяет существование листа и его заголовок.
    Создает лист с новым заголовком SCRIMS_HEADER, если он не найден.
    """
    try:
        wks = spreadsheet.worksheet(name)
        # Опционально: Проверка и обновление заголовка существующего листа
        try:
            current_header = wks.row_values(1)
            if current_header != SCRIMS_HEADER:
                st.warning(f"Worksheet '{name}' header mismatch. "
                           f"Expected {len(SCRIMS_HEADER)} columns, found {len(current_header)}. "
                           f"Data aggregation might be incorrect or fail. "
                           f"Consider updating the sheet header manually or deleting the sheet "
                           f"to allow recreation with the correct structure.")
                # Не пытаемся автоматически исправить заголовок, чтобы избежать потери данных
        except Exception as header_exc:
             st.warning(f"Could not verify header for worksheet '{name}': {header_exc}")

    except gspread.exceptions.WorksheetNotFound:
        try:
            cols_needed = len(SCRIMS_HEADER)
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=max(cols_needed, 26)) # Берем макс. на всякий случай
            wks.append_row(SCRIMS_HEADER, value_input_option='USER_ENTERED')
            # Форматируем заголовок жирным
            wks.format(f'A1:{gspread.utils.rowcol_to_a1(1, cols_needed)}', {'textFormat': {'bold': True}})
            st.info(f"Created worksheet '{name}' with new structure.")
        except Exception as e:
            st.error(f"Error creating worksheet '{name}': {e}")
            return None
    except Exception as e:
        st.error(f"Error accessing worksheet '{name}': {e}")
        return None
    return wks

# --- GRID API Functions (Без изменений) ---
# В файле scrims.py

# --- ИЗМЕНЕНА: get_all_series (добавлено games { id } в запрос) ---
# В файле scrims.py

# --- ИСПРАВЛЕНА: get_all_series (возвращен простой GraphQL запрос без games { id }) ---
@st.cache_data(ttl=300) # Кэшируем список серий на 5 минут
def get_all_series(_debug_placeholder=None):
    """
    Получает список ID и дат начала серий (скримов) за последние 180 дней.
    Используется простой GraphQL запрос для избежания ошибки 400 Bad Request.
    """
    internal_logs = [] # Логи для этой функции
    headers = {"x-api-key": GRID_API_KEY, "Content-Type": "application/json"}
    # !!! ИЗМЕНЕНИЕ: Возвращен простой запрос без games { id } !!!
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
                id,                 # ID Серии (s_id)
                startTimeScheduled
                # Поле games { id } убрано для исправления ошибки 400
              }
            }
          }
        }
    """
    # !!! КОНЕЦ ИЗМЕНЕНИЯ !!!
    start_thresh = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables = {
        "filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_thresh}},
        "first": 50, "orderBy": "StartTimeScheduled", "orderDirection": "DESC"
    }

    # Отладка переменных запроса (можно закомментировать)
    # print("--- DEBUG: get_all_series GraphQL Variables ---"); print(json.dumps(variables, indent=2)); print("---")

    nodes = []
    next_pg, cursor, pg_num, max_pg = True, None, 1, 20 # Ограничение пагинации

    while next_pg and pg_num <= max_pg:
        curr_vars = variables.copy()
        if cursor: curr_vars["after"] = cursor
        try:
            resp = requests.post(f"{GRID_BASE_URL}central-data/graphql", headers=headers, json={"query": query, "variables": curr_vars}, timeout=20)
            resp.raise_for_status() # Проверяем на HTTP ошибки (4xx, 5xx)
            data = resp.json()

            if "errors" in data:
                st.error(f"GraphQL Error (Page {pg_num}): {data['errors']}")
                internal_logs.append(f"GraphQL Error (Page {pg_num}): {data['errors']}"); break

            s_data = data.get("data", {}).get("allSeries", {}); edges = s_data.get("edges", [])
            total_count = s_data.get("totalCount", "N/A")

            # Отладка результатов (можно закомментировать)
            if pg_num == 1:
                print(f"--- DEBUG: get_all_series Results (Page 1) ---")
                print(f"Total series matching filters: {total_count}")
                print(f"First {len(edges)} nodes retrieved:")
                for i, edge in enumerate(edges[:5]): print(f"  Node {i+1}: {edge.get('node')}") # Теперь node не содержит games
                print(f"----------------------------------------------")

            # Извлекаем только 'id' и 'startTimeScheduled'
            nodes.extend([s["node"] for s in edges if "node" in s])

            info = s_data.get("pageInfo", {}); next_pg = info.get("hasNextPage", False); cursor = info.get("endCursor");
            pg_num += 1; time.sleep(0.3)
        except requests.exceptions.HTTPError as http_err:
             # Логируем конкретно HTTP ошибки, включая 400 Bad Request
             st.error(f"HTTP error fetching series page {pg_num}: {http_err}")
             internal_logs.append(f"HTTP error fetching series page {pg_num}: {http_err}"); break
        except requests.exceptions.RequestException as e:
            st.error(f"Network error fetching series page {pg_num}: {e}")
            internal_logs.append(f"Network error fetching series page {pg_num}: {e}"); break
        except Exception as e:
             st.error(f"Unexpected error fetching series page {pg_num}: {e}")
             internal_logs.append(f"Unexpected error fetching series page {pg_num}: {e}"); break

    if internal_logs: st.warning("get_all_series encountered issues. Check logs.")

    print(f"DEBUG: get_all_series finished. Total nodes retrieved: {len(nodes)}")
    return nodes
def download_series_data(sid, logs, max_ret=3, delay_init=2):
    hdr={"x-api-key":GRID_API_KEY}; url=f"https://api.grid.gg/file-download/end-state/grid/series/{sid}"
    for att in range(max_ret):
        try:
            resp=requests.get(url, headers=hdr, timeout=15);
            # --- ИСПРАВЛЕННЫЙ БЛОК ---
            if resp.status_code == 200:
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    logs.append(f"Err:JSON S {sid}")
                    return None
            elif resp.status_code == 429:
                dly = delay_init*(2**att)
                logs.append(f"Warn:429 S {sid}.Wait {dly}s")
                st.toast(f"Wait {dly}s...")
                time.sleep(dly)
                continue
            elif resp.status_code == 404:
                return None
            else:
                logs.append(f"Err:S {sid} St {resp.status_code}")
                resp.raise_for_status()
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---
        except requests.exceptions.RequestException as e:
            if att < max_ret-1:
                time.sleep(delay_init*(2**att))
            else:
                st.error(f"Net err S {sid}:{e}")
                return None
    return None

def download_game_data(gid, logs, max_ret=3, delay_init=2):
    hdr={"x-api-key":GRID_API_KEY}; url=f"https://api.grid.gg/file-download/end-state/grid/game/{gid}"
    for att in range(max_ret):
        try:
            resp=requests.get(url, headers=hdr, timeout=15);
            # --- ИСПРАВЛЕННЫЙ БЛОК ---
            if resp.status_code == 200:
                try:
                    return resp.json()
                except json.JSONDecodeError:
                    logs.append(f"Err:JSON G {gid}")
                    return None
            elif resp.status_code == 429:
                dly = delay_init*(2**att)
                logs.append(f"Warn:429 G {gid}.Wait {dly}s")
                st.toast(f"Wait {dly}s...")
                time.sleep(dly)
                continue
            elif resp.status_code == 404:
                return None
            else:
                logs.append(f"Err:G {gid} St {resp.status_code}")
                resp.raise_for_status()
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---
        except requests.exceptions.RequestException as e:
            if att < max_ret-1:
                time.sleep(delay_init*(2**att))
            else:
                st.error(f"Net err G {gid}:{e}")
                return None
    return None

# В файле scrims.py

# В файле scrims.py

# --- ПОЛНАЯ ФИНАЛЬНАЯ ВЕРСИЯ: update_scrims_data (с перепроверенными отступами) ---
# Убедись, что все нужные импорты и КОНСТАНТЫ определены ВЫШЕ
# (OUR_TEAM_ID, SCRIMS_HEADER, PLAYER_IDS, PLAYER_ROLES_BY_ID, API_REQUEST_DELAY и т.д.)
# В файле scrims.py

# --- ВРЕМЕННАЯ ОТЛАДОЧНАЯ ВЕРСИЯ update_scrims_data (Печать g_data) ---
# Убедись, что импорты json, time, gspread и т.д. есть, и константы определены ВЫШЕ
# В файле scrims.py

# В файле scrims.py

# Убедись, что все нужные импорты и КОНСТАНТЫ определены ВЫШЕ
# (OUR_TEAM_ID, SCRIMS_HEADER, PLAYER_IDS, PLAYER_ROLES_BY_ID, API_REQUEST_DELAY и т.д.)

def update_scrims_data(worksheet, series_list, api_key, debug_logs, progress_bar):
    if not worksheet:
        st.error("Invalid Worksheet object.")
        return False
    if not series_list:
        st.info("No series found to process.")
        return False

    try:
        existing_data = worksheet.get_all_values()
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
    stats = defaultdict(int)
    stats["series_input"] = len(series_list)
    total_series = len(series_list)
    processed_games_count = 0

    # Создаем обратный маппинг: Nickname -> GRID Player ID
    our_player_nicks_to_grid_id = {name: grid_id for grid_id, name in PLAYER_IDS.items()}
    # Создаем маппинг Riot Имя -> GRID ID (нужен для сопоставления)
    our_player_riot_names_to_grid_id = {}
    try:
         from app import team_rosters as app_team_rosters
         if "Gamespace" in app_team_rosters:
              for player_nick, player_data in app_team_rosters["Gamespace"].items():
                   grid_id = next((gid for gid, nick in PLAYER_IDS.items() if nick == player_nick), None)
                   if grid_id and "game_name" in player_data:
                        for game_name in player_data["game_name"]:
                             our_player_riot_names_to_grid_id[game_name] = grid_id
         debug_logs.append(f"Loaded {len(our_player_riot_names_to_grid_id)} Riot Name -> GRID ID mappings.")
         if not our_player_riot_names_to_grid_id:
              debug_logs.append("Warn: No Riot Name mappings found in app.py roster. Team identification might fail.")
    except Exception as roster_e:
         debug_logs.append(f"Warn: Error loading team_rosters: {roster_e}. Team identification might fail.")
         # Как fallback, можно попытаться сопоставить основные ники из PLAYER_IDS с riotIdGameName
         for name, grid_id in our_player_nicks_to_grid_id.items():
             our_player_riot_names_to_grid_id[name] = grid_id


    for i, s_summary in enumerate(series_list):
        series_id = s_summary.get("id")
        if not series_id:
            stats["skipped_invalid_series_summary"] += 1
            continue

        prog = (i + 1) / total_series
        try: progress_bar.progress(prog, text=f"Processing Series {i+1}/{total_series} (ID: {series_id})")
        except Exception: pass

        if i > 0: time.sleep(API_REQUEST_DELAY * 0.5)

        stats["series_processed"] += 1
        # debug_logs.append(f"--- Processing Series {series_id} ---") # Уменьшим логирование

        games_in_series = get_game_details_from_series(series_id, api_key, debug_logs)

        if not games_in_series:
            stats["series_skipped_no_games"] += 1
            # debug_logs.append(f"Info: No games found or error fetching games for series {series_id}.")
            continue

        for game_details in games_in_series:
            game_id = None; sequence_number = None
            try:
                game_id = game_details.get("id")
                sequence_number = game_details.get("sequenceNumber")

                if not game_id or sequence_number is None:
                    stats["games_skipped_invalid_details"] += 1
                    continue

                m_id = str(game_id)
                if m_id in existing_ids:
                    stats["games_skipped_duplicate"] += 1
                    continue

                stats["games_attempted"] += 1
                # debug_logs.append(f"Attempting to download game {m_id} (Series: {series_id}, SeqNum: {sequence_number})")

                time.sleep(API_REQUEST_DELAY)
                game_state_data = download_game_summary_data(series_id, sequence_number, api_key, debug_logs)

                if not game_state_data or not isinstance(game_state_data, dict):
                    stats["games_skipped_download_fail"] += 1
                    continue

                # --- НАЧАЛО ПАРСИНГА ---
                game_date_str = game_state_data.get("gameCreationDate", game_state_data.get("startedAt", s_summary.get("startTimeScheduled", "")))
                date_formatted = "N/A"
                if game_date_str:
                    if isinstance(game_date_str, (int, float)) and game_date_str > 1000000000000:
                         try: date_formatted = datetime.fromtimestamp(game_date_str / 1000).strftime("%Y-%m-%d %H:%M:%S")
                         except Exception as ts_e: pass
                    elif isinstance(game_date_str, str):
                         try: date_formatted = datetime.fromisoformat(game_date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
                         except ValueError:
                              try: date_formatted = datetime.strptime(game_date_str.split('.')[0], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S")
                              except: pass

                duration_sec_raw = game_state_data.get("gameDuration")
                duration_formatted = "N/A"
                if isinstance(duration_sec_raw, (int, float)) and duration_sec_raw >= 0:
                     try:
                         total_seconds = int(duration_sec_raw)
                         minutes_dur, seconds_dur = divmod(total_seconds, 60)
                         duration_formatted = f"{minutes_dur}:{seconds_dur:02d}"
                     except Exception as dur_e: pass
                elif isinstance(duration_sec_raw, str) and duration_sec_raw.startswith('PT'):
                     try:
                         total_seconds = 0; time_part = duration_sec_raw[2:]; minutes = 0; seconds = 0
                         if 'M' in time_part: parts = time_part.split('M'); minutes = int(parts[0]); time_part = parts[1]
                         if 'S' in time_part: seconds = float(time_part.replace('S',''))
                         total_seconds = int(minutes * 60 + seconds)
                         if total_seconds >= 0: minutes_dur, seconds_dur = divmod(total_seconds, 60); duration_formatted = f"{minutes_dur}:{seconds_dur:02d}"
                     except Exception as dur_e: pass

                teams_data = game_state_data.get("teams", [])
                if len(teams_data) < 2:
                    stats["games_skipped_invalid_teams"] += 1; continue

                blue_team_data = next((t for t in teams_data if t.get("teamId") == 100), None)
                red_team_data = next((t for t in teams_data if t.get("teamId") == 200), None)
                if not blue_team_data or not red_team_data:
                     if len(teams_data) == 2:
                         if teams_data[0].get("teamId") == 200 and teams_data[1].get("teamId") == 100: blue_team_data=teams_data[1]; red_team_data=teams_data[0]
                         elif teams_data[0].get("teamId") == 100 and teams_data[1].get("teamId") == 200: blue_team_data=teams_data[0]; red_team_data=teams_data[1]
                         else: blue_team_data=teams_data[0]; red_team_data=teams_data[1]
                     else: stats["games_skipped_invalid_sides"] += 1; continue

                blue_team_id_str = str(blue_team_data.get("teamId", "100"))
                red_team_id_str = str(red_team_data.get("teamId", "200"))

                participants_list = game_state_data.get("participants", [])
                if not participants_list:
                     stats["games_skipped_no_participants"] += 1; continue

                # Определяем, наша ли команда синяя/красная и имена команд
                is_our_blue = False; is_our_red = False
                blue_team_name_found = "Blue Team"; red_team_name_found = "Red Team"
                player_participant_id_to_grid_id = {} # Карта participantId -> GRID ID для НАШИХ игроков

                for p_state in participants_list:
                    p_team_id = str(p_state.get("teamId", ""))
                    p_riot_name = p_state.get("riotIdGameName", p_state.get("summonerName", ""))
                    p_participant_id = p_state.get("participantId")

                    team_tag = None; player_name_only = p_riot_name
                    if ' ' in p_riot_name:
                         parts = p_riot_name.split(' ', 1)
                         if parts[0].isupper() and len(parts[0]) <= 5: team_tag = parts[0]; player_name_only = parts[1]

                    # Находим GRID ID нашего игрока по его Riot имени
                    grid_id_found = our_player_riot_names_to_grid_id.get(p_riot_name)

                    if p_team_id == blue_team_id_str:
                        if blue_team_name_found == "Blue Team" and team_tag: blue_team_name_found = team_tag
                        if grid_id_found:
                             is_our_blue = True
                             if p_participant_id: player_participant_id_to_grid_id[p_participant_id] = grid_id_found
                    elif p_team_id == red_team_id_str:
                        if red_team_name_found == "Red Team" and team_tag: red_team_name_found = team_tag
                        if grid_id_found:
                             is_our_red = True
                             if p_participant_id: player_participant_id_to_grid_id[p_participant_id] = grid_id_found

                # Определяем результат
                result = "N/A"
                if is_our_blue: result = "Win" if blue_team_data.get("win") else "Loss"
                elif is_our_red: result = "Win" if red_team_data.get("win") else "Loss"
                elif blue_team_data.get("win") is not None: result = "Win" if blue_team_data.get("win") else "Loss"
                elif blue_team_data.get("win") is False and red_team_data.get("win") is False: result = "Draw"

                # Баны (только ID)
                b_bans, r_bans = ["N/A"]*5, ["N/A"]*5
                blue_bans_data = blue_team_data.get("bans", []); red_bans_data = red_team_data.get("bans", [])
                for i, ban_info in enumerate(blue_bans_data):
                    if i < 5: b_bans[i] = str(ban_info.get("championId", "N/A"))
                for i, ban_info in enumerate(red_bans_data):
                    if i < 5: r_bans[i] = str(ban_info.get("championId", "N/A"))

                # Пики по порядку недоступны
                draft_picks_ordered = ["N/A"] * 10

                # Заполняем фактических чемпионов
                actual_champs = {"blue": {}, "red": {}}
                for role in ROLE_ORDER_FOR_SHEET:
                    role_short = role.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                    actual_champs["blue"][role_short] = "N/A"; actual_champs["red"][role_short] = "N/A"

                participants_mapped_to_roles = 0
                assigned_roles_blue = set(); assigned_roles_red = set()

                for p_state in participants_list:
                    p_participant_id = p_state.get("participantId")
                    p_team_id = str(p_state.get("teamId", ""))
                    champ_name = p_state.get("championName", "N/A")
                    role_api = p_state.get("teamPosition", "").upper()
                    if not role_api: role_api = p_state.get("individualPosition", "").upper()

                    side = "blue" if p_team_id == blue_team_id_str else "red" if p_team_id == red_team_id_str else None
                    if not side: continue

                    role_short = None
                    is_player_ours = p_participant_id in player_participant_id_to_grid_id

                    # --- ЛОГИКА ОПРЕДЕЛЕНИЯ РОЛИ ---
                    if is_player_ours:
                        # Для НАШИХ игроков берем роль из PLAYER_ROLES_BY_ID
                        grid_id = player_participant_id_to_grid_id.get(p_participant_id)
                        if grid_id:
                             role_full = PLAYER_ROLES_BY_ID.get(grid_id)
                             if role_full:
                                  role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                             else: debug_logs.append(f"Warn: Could not find role in PLAYER_ROLES_BY_ID for our player GRID ID {grid_id} in game {m_id}.")
                        else: debug_logs.append(f"Warn: Could not find GRID ID mapping for our player participantId {p_participant_id} in game {m_id}.")
                    else:
                        # Для ОППОНЕНТОВ используем API роль
                        if role_api == "TOP": role_short = "TOP"
                        elif role_api == "JUNGLE": role_short = "JGL"
                        elif role_api == "MIDDLE": role_short = "MID"
                        elif role_api == "BOTTOM": role_short = "BOT"
                        elif role_api == "UTILITY": role_short = "SUP"
                        elif role_api == "AFK": pass # Игнорируем AFK для присвоения роли
                        elif role_api: debug_logs.append(f"Warn: Unknown API role '{role_api}' for opponent participantId {p_participant_id} on {side} in game {m_id}.")
                        # else: # Роль пустая, не можем присвоить

                    # --- ПРИСВАИВАЕМ ЧЕМПИОНА ---
                    if role_short:
                        assigned_roles = assigned_roles_blue if side == "blue" else assigned_roles_red
                        # Проверяем, что слот свободен или уже содержит этого же чемпиона (на случай дублей в API)
                        if role_short in actual_champs[side] and (actual_champs[side][role_short] == "N/A" or actual_champs[side][role_short] == champ_name):
                             if actual_champs[side][role_short] == "N/A": # Записываем только если был N/A
                                 actual_champs[side][role_short] = champ_name
                                 participants_mapped_to_roles += 1
                             # Если чемпион совпадает - все ок, не логируем
                             assigned_roles.add(role_short)
                        elif role_short in actual_champs[side]: # Конфликт ролей
                             debug_logs.append(f"Error: Role conflict for {role_short} on {side} side in game {m_id}. Slot occupied by '{actual_champs[side][role_short]}', tried to add '{champ_name}'. API role was '{role_api}'.")
                        else: # Роли нет в нашем стандарте
                             debug_logs.append(f"Warn: Role '{role_short}' (from API '{role_api}') not in standard role list for game {m_id}.")
                    # Если role_short is None (AFK или не определена), чемпион не присваивается

                # Итоговая проверка заполненности ролей (опционально)
                if len(assigned_roles_blue) < 5: debug_logs.append(f"Warn: Assigned only {len(assigned_roles_blue)}/5 blue roles for game {m_id}.")
                if len(assigned_roles_red) < 5: debug_logs.append(f"Warn: Assigned only {len(assigned_roles_red)}/5 red roles for game {m_id}.")


                # Имена команд
                blue_team_name_final = blue_team_name_found if blue_team_name_found != "Blue Team" else blue_team_data.get("name", "Blue Team")
                red_team_name_final = red_team_name_found if red_team_name_found != "Red Team" else red_team_data.get("name", "Red Team")

                # Формируем строку
                new_row_data = [
                    date_formatted, m_id, sequence_number, blue_team_name_final, red_team_name_final,
                    *b_bans, *r_bans, *draft_picks_ordered,
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
                # debug_logs.append(f"Success: Prepared row for game {m_id}.") # Убрано для краткости

            except Exception as parse_e:
                 stats["games_skipped_parse_fail"] += 1
                 debug_logs.append(f"Error: Failed to parse game {m_id} data: {repr(parse_e)}. Data keys: {list(game_state_data.keys()) if isinstance(game_state_data,dict) else 'N/A'}")
                 import traceback; debug_logs.append(f"Traceback: {traceback.format_exc()}")
                 continue

        # debug_logs.append(f"--- Finished Processing Series {series_id} ---") # Убрано для краткости

    # --- КОНЕЦ ЦИКЛА ПО СЕРИЯМ ---

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
        f"Games Skipped (Download Fail): {stats['games_skipped_download_fail']}",
        f"Games Skipped (Parse Fail): {stats['games_skipped_parse_fail']}",
        f"Games Skipped (Invalid Teams/Sides): {stats['games_skipped_invalid_teams'] + stats['games_skipped_invalid_sides']}",
        f"Games Skipped (No Participants): {stats['games_skipped_no_participants']}",
        f"Games Skipped (Row Length Mismatch): {stats['games_skipped_row_mismatch']}",
        f"Games Processed Successfully: {stats['games_processed_success']}",
        f"New Records Added to Sheet: {len(new_rows)}"
    ]
    if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
    combined_logs = debug_logs[-(100 - len(summary)):] + summary
    st.session_state.scrims_update_logs = combined_logs[-150:]
    st.code("\n".join(st.session_state.scrims_update_logs), language=None)

    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            st.success(f"Added {len(new_rows)} new records to '{worksheet.title}'.")
            try: aggregate_scrims_data.clear()
            except AttributeError: pass
            return True
        except gspread.exceptions.APIError as api_err:
            error_msg = f"GSpread API Error appending rows: {api_err}"; st.error(error_msg)
            if 'scrims_update_logs' in st.session_state: st.session_state.scrims_update_logs.append(f"GSPREAD APPEND ERROR: {api_err}")
            return False
        except Exception as e:
            error_msg = f"Error appending rows: {e}"; st.error(error_msg)
            if 'scrims_update_logs' in st.session_state: st.session_state.scrims_update_logs.append(f"APPEND ERROR: {e}")
            return False
    else:
        st.info("No new valid records found to add.")
        if stats["series_processed"] > 0 and stats["games_processed_success"] == 0 and stats["games_attempted"] > 0 :
             st.warning(f"Attempted to process {stats['games_attempted']} games, but none were added successfully. Check logs for reasons.")
        elif stats["series_processed"] == 0 and stats["series_input"] > 0:
              st.warning("No series were processed. Check logs for initial errors.")
        return False
# --- ВОССТАНОВЛЕННАЯ: aggregate_scrims_data (читает Actual_, без кэша) ---
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    """
    Агрегирует данные из Google Sheet, читая фактических чемпионов
    из колонок 'Actual_SIDE_ROLE'.
    Возвращает статистику по сторонам, историю матчей и статистику игроков.
    """
    # Отступ 1 (4 пробела)
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        # Возвращаем пустые структуры правильного типа
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}

    # Инициализация статистики
    blue_stats = {"wins": 0, "losses": 0, "total": 0}
    red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0}))
    # Ожидаемое количество колонок берем из константы
    expected_cols = len(SCRIMS_HEADER)

    # Настройка фильтра по времени
    now = datetime.utcnow()
    time_threshold = None
    if time_filter != "All Time":
        weeks_map = {"1 Week": 1, "2 Weeks": 2, "3 Weeks": 3, "4 Weeks": 4}
        days_map = {"2 Months": 60} # Примерно 2 месяца
        if time_filter in weeks_map:
            time_threshold = now - timedelta(weeks=weeks_map[time_filter])
        elif time_filter in days_map:
            time_threshold = now - timedelta(days=days_map[time_filter])

    # Чтение данных из таблицы
    try:
        data = worksheet.get_all_values()
    except gspread.exceptions.APIError as api_err:
        st.error(f"GSpread API Error reading sheet for aggregation: {api_err}")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}
    except Exception as e:
        st.error(f"Read error during aggregation: {e}")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}

    if len(data) <= 1: # Если только заголовок или пусто
        st.info(f"No data found in the sheet '{worksheet.title}' for aggregation matching the filter '{time_filter}'.")
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}

    header = data[0]
    # Проверяем заголовок на соответствие SCRIMS_HEADER
    if header != SCRIMS_HEADER:
        st.error(f"Header mismatch in '{worksheet.title}' during aggregation. Cannot proceed safely.")
        st.error(f"Expected {len(SCRIMS_HEADER)} cols, Found {len(header)} cols.")
        st.code(f"Expected: {SCRIMS_HEADER}\nFound:    {header}", language=None)
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}

    # Создаем индекс колонок на основе SCRIMS_HEADER
    try:
        idx = {name: i for i, name in enumerate(SCRIMS_HEADER)}
    except Exception as e:
         st.error(f"Failed to create column index map: {e}")
         return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}


    # Обработка строк данных
    rows_processed_after_filter = 0
    for row_index, row in enumerate(data[1:], start=2): # start=2 для нумерации строк в таблице
        # Отступ 2 (8 пробелов)
        # Пропускаем строки с неверным количеством колонок
        if len(row) != expected_cols:
            continue
        try:
            # Отступ 3 (12 пробелов)
            date_str = row[idx["Date"]]
            # Применяем фильтр по времени, если он активен
            if time_threshold and date_str != "N/A":
                try:
                    # Отступ 4 (16 пробелов)
                    date_obj = datetime.strptime(date_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                    if date_obj < time_threshold:
                        continue # Пропускаем строку, если она старше фильтра
                except ValueError:
                    continue # Пропускаем строки с неверной датой при активном фильтре

            # Если строка прошла фильтр по времени (или фильтр неактивен), увеличиваем счетчик
            rows_processed_after_filter += 1

            # Определяем команды и результат
            b_team, r_team, res = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]
            is_our_blue = (b_team == TEAM_NAME)
            is_our_red = (r_team == TEAM_NAME)
            # Пропускаем, если это не игра нашей команды
            if not (is_our_blue or is_our_red):
                continue

            # Определяем победу нашей команды
            is_our_win = (is_our_blue and res == "Win") or (is_our_red and res == "Win")

            # --- Обновление общей статистики по сторонам ---
            if is_our_blue:
                # Отступ 4 (16 пробелов)
                blue_stats["total"] += 1
                if res == "Win": blue_stats["wins"] += 1
                elif res == "Loss": blue_stats["losses"] += 1
            else: # Наша команда красная
                # Отступ 4 (16 пробелов)
                red_stats["total"] += 1
                if res == "Win": red_stats["wins"] += 1
                elif res == "Loss": red_stats["losses"] += 1

            # --- Подсчет статистики игроков по фактическим чемпионам ---
            side_prefix = "Blue" if is_our_blue else "Red"
            # Проходим по известным ролям нашей команды
            for player_id, role_full in PLAYER_ROLES_BY_ID.items():
                # Отступ 4 (16 пробелов)
                player_name = PLAYER_IDS.get(player_id) # Получаем имя игрока по его ID
                if player_name: # Если игрок найден в нашем ростере
                    # Отступ 5 (20 пробелов)
                    # Формируем короткое имя роли для ключа словаря/колонки
                    role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                    # Формируем имя колонки с фактическим чемпионом для нужной стороны и роли
                    actual_champ_col_name = f"Actual_{side_prefix}_{role_short}" # e.g., Actual_Blue_TOP

                    # Получаем чемпиона из ЭТОЙ колонки
                    champion = row[idx[actual_champ_col_name]]
                    # Обновляем статистику, если чемпион не "N/A" и не пустой
                    if champion and champion != "N/A" and champion.strip() != "":
                        # Отступ 6 (24 пробела)
                        player_stats[player_name][champion]['games'] += 1
                        if is_our_win: # Используем флаг победы нашей команды
                            player_stats[player_name][champion]['wins'] += 1

            # --- Подготовка строки для истории матчей ---
            # Используем пики из колонок Draft_Pick_* для отображения истории драфта
            bb_html = " ".join(get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6) if idx.get(f"Blue Ban {i}") is not None and row[idx[f"Blue Ban {i}"]] != "N/A")
            rb_html = " ".join(get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6) if idx.get(f"Red Ban {i}") is not None and row[idx[f"Red Ban {i}"]] != "N/A")
            bp_html = " ".join(get_champion_icon_html(row[idx[pick_key]]) for pick_key in ["Draft_Pick_B1","Draft_Pick_B2","Draft_Pick_B3","Draft_Pick_B4","Draft_Pick_B5"] if idx.get(pick_key) is not None and row[idx[pick_key]] != "N/A")
            rp_html = " ".join(get_champion_icon_html(row[idx[pick_key]]) for pick_key in ["Draft_Pick_R1","Draft_Pick_R2","Draft_Pick_R3","Draft_Pick_R4","Draft_Pick_R5"] if idx.get(pick_key) is not None and row[idx[pick_key]] != "N/A")
            history_rows.append({
                "Date": date_str,
                "Blue Team": b_team,
                "B Bans": bb_html,
                "B Picks": bp_html, # Пики драфта для истории
                "Result": res,
                "Duration": row[idx["Duration"]],
                "R Picks": rp_html, # Пики драфта для истории
                "R Bans": rb_html,
                "Red Team": r_team,
                "Match ID": row[idx["Match ID"]]
            })

        except IndexError as e_idx:
            # Отступ 3 (12 пробелов)
            # Логируем ошибку индекса, если нужно для отладки
            st.warning(f"Skipping row {row_index} due to IndexError: {e_idx}. Check row length vs header.")
            continue # Пропускаем строку
        except Exception as e_inner:
            # Отступ 3 (12 пробелов)
            # Логируем другие ошибки обработки строк
            st.warning(f"Skipping row {row_index} due to error: {e_inner}")
            continue # Пропускаем строку
    # --- Конец цикла for row in data[1:] ---

    # Если после фильтрации не осталось строк для обработки
    if rows_processed_after_filter == 0 and time_filter != "All Time":
        # Отступ 1 (4 пробела)
        st.info(f"No scrim data found for the selected period: {time_filter}")
        # Возвращаем пустые структуры
        return {}, {}, pd.DataFrame(columns=["Date", "Blue Team", "B Bans", "B Picks", "Result", "Duration", "R Picks", "R Bans", "Red Team", "Match ID"]), {}


    # --- Постобработка и возврат результатов ---
    df_hist = pd.DataFrame(history_rows)
    if not df_hist.empty:
        try:
            # Отступ 2 (8 пробелов)
            # Сортируем историю по дате (новые сверху)
            df_hist['DT_temp'] = pd.to_datetime(df_hist['Date'], errors='coerce')
            # Удаляем строки, где дата не распозналась, перед сортировкой
            df_hist.dropna(subset=['DT_temp'], inplace=True)
            df_hist = df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception as sort_ex:
             # Отступ 2 (8 пробелов)
             st.warning(f"Could not sort match history by date: {sort_ex}")
             # Не возвращаем ошибку, просто история будет не отсортирована

    # Конвертируем и сортируем статистику игроков
    final_player_stats = {player: dict(champions) for player, champions in player_stats.items()}
    for player in final_player_stats:
        # Отступ 2 (8 пробелов)
        # Сортируем чемпионов по количеству игр (убывание)
        final_player_stats[player] = dict(sorted(
            final_player_stats[player].items(),
            key=lambda item: item[1].get('games', 0), # Безопасный доступ к 'games'
            reverse=True
        ))

    # Добавляем проверку, если статистика пуста (например, из-за фильтра)
    if not final_player_stats and rows_processed_after_filter > 0:
         # Отступ 1 (4 пробела)
         st.info(f"Processed {rows_processed_after_filter} scrims for '{time_filter}', but no champion stats were generated (check data consistency or if players played on selected roles).")

    # Отступ 1 (4 пробела)
    return blue_stats, red_stats, df_hist, final_player_stats
# --- Конец функции aggregate_scrims_data ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    if st.button("⬅️ Back to HLL Stats"): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()

    client = setup_google_sheets();
    if not client: st.error("GSheets client failed."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Sheet access error: {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: st.error(f"Worksheet access error."); return

    with st.expander("Update Scrim Data", expanded=False):
        logs = [];
        if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
        if st.button("Download & Update from GRID API", key="update_scrims_btn"):
            st.session_state.scrims_update_logs = []; logs = st.session_state.scrims_update_logs
            with st.spinner("Fetching series..."): series_list = get_all_series(logs)
            if series_list:
                st.info(f"Processing {len(series_list)} series...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting...")
                try:
                    data_added = update_scrims_data(wks, series_list, logs, progress_bar)
                    if data_added: aggregate_scrims_data.clear() # Очищаем кэш если данные добавлены
                except Exception as e: st.error(f"Update error: {e}"); logs.append(f"FATAL: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No series found.")
        if st.session_state.scrims_update_logs: st.code("\n".join(st.session_state.scrims_update_logs), language=None)

    st.divider(); st.subheader("Scrim Performance")
    time_f = st.selectbox("Filter by Time:", ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # --- Вызываем aggregate_scrims_data, получаем 4 значения ---
    blue_s, red_s, df_hist, player_champ_stats = aggregate_scrims_data(wks, time_f)

    # --- Отображаем общую статистику (без изменений) ---
    try:
        games_f = blue_s["total"] + red_s["total"]; wins_f = blue_s["wins"] + red_s["wins"]; loss_f = blue_s["losses"] + red_s["losses"]
        st.markdown(f"**Performance ({time_f})**"); co, cb, cr = st.columns(3)
        with co: wr = (wins_f / games_f * 100) if games_f > 0 else 0; st.metric("Total Games", games_f); st.metric("Overall WR", f"{wr:.1f}%", f"{wins_f}W-{loss_f}L")
        with cb: bwr = (blue_s["wins"] / blue_s["total"] * 100) if blue_s["total"] > 0 else 0; st.metric("Blue WR", f"{bwr:.1f}%", f"{blue_s['wins']}W-{blue_s['losses']}L ({blue_s['total']} G)")
        with cr: rwr = (red_s["wins"] / red_s["total"] * 100) if red_s["total"] > 0 else 0; st.metric("Red WR", f"{rwr:.1f}%", f"{red_s['wins']}W-{red_s['losses']}L ({red_s['total']} G)")
    except Exception as e: st.error(f"Error display summary: {e}")

    st.divider()

    # --- ВКЛАДКИ ДЛЯ ИСТОРИИ И СТАТИСТИКИ ИГРОКОВ ---
    tab1, tab2 = st.tabs(["📜 Match History", "📊 Player Champion Stats"])

    with tab1:
        st.subheader(f"Match History ({time_f})")
        if not df_hist.empty:
            st.markdown(df_hist.to_html(escape=False, index=False, classes='compact-table history-table', justify='center'), unsafe_allow_html=True)
        else:
            st.info(f"No match history for {time_f}.")

    with tab2:
        st.subheader(f"Player Champion Stats ({time_f})")
        # st.caption("Note: Roles are assumed based on pick order (Top > Jg > Mid > Bot > Sup).") # Убрано примечание

        if not player_champ_stats:
             st.info(f"No player champion stats available for {time_f}.")
        else:
             # Используем PLAYER_IDS для получения имен игроков в нужном порядке
             player_order = [PLAYER_IDS[pid] for pid in ["26433", "25262", "25266", "20958", "21922"] if pid in PLAYER_IDS]
             player_cols = st.columns(len(player_order))

             for i, player_name in enumerate(player_order):
                 with player_cols[i]:
                     # Находим роль игрока для отображения
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
                                     'Champion': champ, # Оставляем имя для ясности
                                     'Games': games,
                                     'WR%': win_rate
                                 })

                     if stats_list:
                         df_player = pd.DataFrame(stats_list).sort_values("Games", ascending=False).reset_index(drop=True)
                         df_player['WR%'] = df_player['WR%'].apply(color_win_rate_scrims)
                         st.markdown(
                              # Убрали столбец Champion, Icon+WR% достаточно
                              df_player.to_html(escape=False, index=False, columns=['Icon', 'Games', 'WR%'], classes='compact-table player-stats', justify='center'),
                              unsafe_allow_html=True
                         )
                     else:
                         st.caption("No stats.")


# --- Keep __main__ block as is ---
if __name__ == "__main__": pass
