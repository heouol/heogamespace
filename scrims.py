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
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed"
SCRIMS_WORKSHEET_NAME = "Scrims"

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
@st.cache_data(ttl=300)
def get_all_series(_debug_placeholder):
    internal_logs = []
    headers = {"x-api-key": GRID_API_KEY,"Content-Type": "application/json"}
    query = """ query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) { allSeries( filter: $filter, first: $first, after: $after, orderBy: $orderBy, orderDirection: $orderDirection ) { totalCount, pageInfo { hasNextPage, endCursor }, edges { node { id, startTimeScheduled } } } } """
    start_thresh=(datetime.utcnow()-timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables = {"filter":{"titleId":3,"types":["SCRIM"],"startTimeScheduled":{"gte":start_thresh}},"first":50,"orderBy":"StartTimeScheduled","orderDirection":"DESC"}
    nodes,next_pg,cursor,pg_num,max_pg=[],True,None,1,20
    while next_pg and pg_num<=max_pg:
        curr_vars=variables.copy();
        if cursor:curr_vars["after"]=cursor
        try:
            resp=requests.post(f"{GRID_BASE_URL}central-data/graphql",headers=headers,json={"query":query,"variables":curr_vars},timeout=20); resp.raise_for_status(); data=resp.json()
            if "errors" in data: st.error(f"GraphQL Err:{data['errors']}"); break
            s_data=data.get("data",{}).get("allSeries",{}); edges=s_data.get("edges",[]); nodes.extend([s["node"] for s in edges if "node" in s]); info=s_data.get("pageInfo",{}); next_pg=info.get("hasNextPage",False); cursor=info.get("endCursor"); pg_num+=1; time.sleep(0.2)
        except Exception as e: st.error(f"Err fetch series pg {pg_num}:{e}"); return[]
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


# --- ИЗМЕНЕНА: update_scrims_data (Использует ID игрока для сопоставления пиков) ---
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    """
    Скачивает данные с GRID API, обрабатывает их и добавляет новые строки
    в Google Sheet согласно новой структуре SCRIMS_HEADER.
    """
    if not worksheet:
        st.error("Invalid Worksheet object provided to update_scrims_data.")
        return False
    if not series_list:
        st.info("No series found to process.")
        return False

    try:
        # Получаем ID существующих записей из второго столбца ('Match ID')
        existing_data = worksheet.get_all_values() # Получаем все данные один раз
        existing_ids = set(row[1] for row in existing_data[1:] if len(row) > 1) if len(existing_data) > 1 else set()
    except gspread.exceptions.APIError as api_err:
         st.error(f"GSpread API Error reading sheet: {api_err}")
         debug_logs.append(f"GSpread API Error reading sheet: {api_err}")
         return False
    except Exception as e:
        st.error(f"Error reading existing sheet data: {e}")
        debug_logs.append(f"Error reading existing sheet data: {e}")
        return False

    new_rows = []
    stats = {"gms_count": 0, "skip_dupes": 0, "processed": 0, "skipped_no_game_data": 0, "skipped_incomplete_map": 0}
    api_request_delay = 0.5 # Задержка между запросами к API в секундах
    total_series = len(series_list)

    for i, s_summary in enumerate(series_list):
        s_id = s_summary.get("id")
        if not s_id: continue

        # Обновляем прогресс-бар
        prog = (i + 1) / total_series
        try:
            progress_bar.progress(prog, text=f"Processing {i+1}/{total_series} ({s_id})")
        except Exception: pass # Игнорируем ошибки обновления UI

        # Задержка между обработкой серий (кроме первой)
        if i > 0: time.sleep(api_request_delay)

        # Проверяем ID до скачивания s_data
        m_id_potential = str(s_summary.get("matchId", s_id))
        if m_id_potential in existing_ids:
            stats["skip_dupes"] += 1
            continue

        # Скачиваем данные серии
        s_data = download_series_data(s_id, debug_logs=debug_logs)
        if not s_data: continue

        teams = s_data.get("teams")
        if not teams or len(teams) < 2: continue
        t0, t1 = teams[0], teams[1]
        t0_n, t1_n = t0.get("name", "N/A"), t1.get("name", "N/A")

        # Проверяем, наша ли это команда
        is_our_scrim = (TEAM_NAME == t0_n or TEAM_NAME == t1_n)
        if not is_our_scrim: continue
        stats["gms_count"] += 1

        m_id = str(s_data.get("matchId", s_id))
        # Повторная проверка ID после скачивания s_data
        if m_id in existing_ids:
            stats["skip_dupes"] += 1
            continue

        # Скачиваем данные игры (g_data)
        g_id, g_data = None, None
        potential_games = s_data.get("games", []) or (s_data.get("object", {}).get("games") if isinstance(s_data.get("object"), dict) else [])
        if isinstance(potential_games, list) and potential_games:
             game_info = potential_games[0] # Берем первую игру
             g_id = game_info.get("id") if isinstance(game_info, dict) else game_info if isinstance(game_info, str) else None

        if g_id:
            time.sleep(api_request_delay / 2) # Небольшая пауза перед скачиванием игры
            g_data = download_game_data(g_id, debug_logs=debug_logs)
        else:
             debug_logs.append(f"Warn: No game ID found for series {s_id}")
             stats["skipped_no_game_data"] += 1
             continue # Пропускаем, если нет ID игры

        # Проверяем наличие критически важных данных в g_data
        if not g_data or 'games' not in g_data or not g_data['games'] or 'teams' not in g_data['games'][0]:
            debug_logs.append(f"Warn: Skipping {s_id} (GameID: {g_id}) - Missing g_data or critical game structure (games/teams)")
            stats["skipped_no_game_data"] += 1
            continue

        # --- Извлечение базовой информации (Дата, Команды, Баны) ---
        date_f = "N/A"
        date_s = s_data.get("startTime", s_summary.get("startTimeScheduled", s_data.get("updatedAt")))
        if date_s and isinstance(date_s, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try:
                    date_f = datetime.strptime(date_s.split('+')[0], fmt.split('+')[0]).strftime("%Y-%m-%d %H:%M:%S") # Убираем таймзону для парсинга
                    break
                except ValueError: continue
        b_team_name, r_team_name = t0_n, t1_n

        draft_actions = g_data['games'][0].get("draftActions", [])
        b_bans, r_bans = ["N/A"]*5, ["N/A"]*5
        if draft_actions:
            try: actions_sorted = sorted(draft_actions, key=lambda x: int(x.get("sequenceNumber", 99)))
            except Exception: actions_sorted = draft_actions
            bb, rb = 0, 0
            processed_ban_seqs = set() # Для избежания дублирования банов с одинаковым sequenceNumber
            for act in actions_sorted:
                try:
                    seq = int(act.get("sequenceNumber", -1))
                    type = act.get("type")
                    champ = act.get("draftable", {}).get("name", "N/A")
                    if type == "ban" and champ != "N/A" and seq != -1 and seq not in processed_ban_seqs:
                         processed_ban_seqs.add(seq)
                         # Sequence numbers for bans: B1=1, R1=2, B2=3, R2=4, B3=5, R3=6 | B4=14, R4=13, B5=16, R5=15
                         if seq in [1, 3, 5, 14, 16]:
                              if bb < 5: b_bans[bb] = champ; bb += 1
                         elif seq in [2, 4, 6, 13, 15]:
                              if rb < 5: r_bans[rb] = champ; rb += 1
                except Exception as e: debug_logs.append(f"Warn: Ban processing error for seq {seq} in {s_id}: {e}"); continue

        # --- Извлечение пиков по порядку драфта ---
        draft_picks_ordered = {
            "B1": "N/A", "R1": "N/A", "R2": "N/A", "B2": "N/A", "B3": "N/A",
            "R3": "N/A", "R4": "N/A", "B4": "N/A", "B5": "N/A", "R5": "N/A"
        }
        pick_map_seq_to_key = {
            7: "B1", 8: "R1", 9: "R2", 10: "B2", 11: "B3",
            12: "R3", 17: "R4", 18: "B4", 19: "B5", 20: "R5"
        }
        processed_pick_seqs = set() # Для избежания дублирования пиков
        if draft_actions:
             for act in actions_sorted:
                 try:
                     seq = int(act.get("sequenceNumber", -1))
                     type = act.get("type")
                     champ = act.get("draftable", {}).get("name", "N/A")
                     if type == "pick" and champ != "N/A" and seq in pick_map_seq_to_key and seq not in processed_pick_seqs:
                          processed_pick_seqs.add(seq)
                          draft_picks_ordered[pick_map_seq_to_key[seq]] = champ
                 except Exception as e: debug_logs.append(f"Warn: Pick processing error for seq {seq} in {s_id}: {e}"); continue

        # --- Извлечение фактических данных игрок-чемпион по ролям ---
        actual_champs = {"blue": {}, "red": {}} # Ключи: TOP, JGL, MID, BOT, SUP
        for role in ROLE_ORDER_FOR_SHEET:
             role_short = role.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
             actual_champs["blue"][role_short] = "N/A"
             actual_champs["red"][role_short] = "N/A"

        game_teams_data = g_data['games'][0]['teams']
        found_all_our_players = True
        our_player_count = 0
        processed_teams = 0 # Счетчик обработанных команд в g_data

        for team_state in game_teams_data:
             processed_teams += 1
             team_name_in_game = team_state.get("name", "N/A")
             # Используем ID команды из s_data для надежного определения нашей команды
             is_our_team_in_game = (team_state.get("id") == t0.get("id") and t0_n == TEAM_NAME) or \
                                 (team_state.get("id") == t1.get("id") and t1_n == TEAM_NAME)

             team_side = team_state.get("side") # 'blue' или 'red'
             if team_side not in ["blue", "red"]:
                  debug_logs.append(f"Warn: Unknown side '{team_side}' for team {team_state.get('id')} in {s_id}"); continue

             target_champ_dict = actual_champs[team_side]
             players_list = team_state.get("players", [])

             if is_our_team_in_game:
                 player_champion_map = {} # ID -> Чемп для нашей команды
                 current_team_player_ids = set()
                 for player_state in players_list:
                     player_id = player_state.get("id")
                     champion_name = player_state.get("character", {}).get("name", "N/A")
                     if player_id in PLAYER_IDS:
                         player_champion_map[player_id] = champion_name
                         current_team_player_ids.add(player_id)
                 our_player_count = len(current_team_player_ids)

                 # Распределяем по ролям, используя PLAYER_ROLES_BY_ID
                 for p_id, role_full in PLAYER_ROLES_BY_ID.items():
                     role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                     if role_short in target_champ_dict:
                         champion = player_champion_map.get(p_id, "N/A")
                         target_champ_dict[role_short] = champion
                         if p_id not in current_team_player_ids or champion == "N/A":
                             # Отмечаем проблему, если ID из нашего ростера не найден в данных игры ИЛИ если чемпион не найден
                             found_all_our_players = False
                             if p_id not in current_team_player_ids:
                                 debug_logs.append(f"Warn: Our player {PLAYER_IDS.get(p_id)} (ID: {p_id}) not found in game data for {s_id}")
                             if champion == "N/A" and p_id in current_team_player_ids:
                                  debug_logs.append(f"Warn: Champion not found for our player {PLAYER_IDS.get(p_id)} (ID: {p_id}) in {s_id}")

             else: # Команда противника
                 # Предполагаем порядок: TOP, JGL, MID, BOT, SUP
                 # Убедимся, что у нас есть хотя бы 5 игроков в списке
                 if len(players_list) >= 5:
                     for i, player_state in enumerate(players_list[:5]): # Берем только первых 5
                          role_full = ROLE_ORDER_FOR_SHEET[i]
                          role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                          champion_name = player_state.get("character", {}).get("name", "N/A")
                          if role_short in target_champ_dict:
                               target_champ_dict[role_short] = champion_name
                 else:
                     debug_logs.append(f"Warn: Opponent team {team_name_in_game} has only {len(players_list)} players in g_data for {s_id}. Cannot map all roles.")
                     # Роли, для которых не хватило игроков, останутся "N/A"


        # --- Проверка на полноту данных нашей команды ---
        if not found_all_our_players or our_player_count < 5 or processed_teams < 2:
            details = f"Found our players: {our_player_count}/5. All mapped (by ID): {found_all_our_players}. Teams in g_data: {processed_teams}."
            debug_logs.append(f"Warn: Skipping {s_id} - Incomplete or missing data. {details}")
            stats["skipped_incomplete_map"] += 1
            continue

        # --- Результат и длительность ---
        duration_s = g_data['games'][0].get("clock", {}).get("currentSeconds")
        duration_f = "N/A"
        if isinstance(duration_s, (int, float)) and duration_s >= 0:
            minutes, seconds = divmod(int(duration_s), 60)
            duration_f = f"{minutes}:{seconds:02d}"
        res = "N/A"
        t0w = t0.get("won"); t1w = t1.get("won")
        # Используем ID команд для надежного определения результата
        if t0.get("id") == s_data.get("teams", [{}, {}])[0].get("id"): # t0 это blue
             if t0w is True: res = "Win" if t0_n == TEAM_NAME else "Loss"
             elif t1w is True: res = "Loss" if t0_n == TEAM_NAME else "Win" # Если t1 выиграла, то t0 проиграла
        elif t1.get("id") == s_data.get("teams", [{}, {}])[0].get("id"): # t1 это blue
             if t1w is True: res = "Win" if t1_n == TEAM_NAME else "Loss"
             elif t0w is True: res = "Loss" if t1_n == TEAM_NAME else "Win" # Если t0 выиграла, то t1 проиграла

        # Обработка ничьи, если она возможна и t0w/t1w оба False
        if res == "N/A" and t0w is False and t1w is False:
             res = "Tie" # Или оставить "N/A", если ничьи не ожидаются

        # --- Формирование строки для таблицы ---
        try:
            new_row = [
                date_f, m_id, b_team_name, r_team_name,
                *b_bans, *r_bans,
                # Пики драфта
                draft_picks_ordered["B1"], draft_picks_ordered["R1"], draft_picks_ordered["R2"],
                draft_picks_ordered["B2"], draft_picks_ordered["B3"], draft_picks_ordered["R3"],
                draft_picks_ordered["R4"], draft_picks_ordered["B4"], draft_picks_ordered["B5"], draft_picks_ordered["R5"],
                # Фактические чемпионы по ролям
                actual_champs["blue"]["TOP"], actual_champs["blue"]["JGL"], actual_champs["blue"]["MID"], actual_champs["blue"]["BOT"], actual_champs["blue"]["SUP"],
                actual_champs["red"]["TOP"], actual_champs["red"]["JGL"], actual_champs["red"]["MID"], actual_champs["red"]["BOT"], actual_champs["red"]["SUP"],
                # Конец строки
                duration_f, res
            ]
        except KeyError as ke:
             debug_logs.append(f"Error: KeyError constructing row for {s_id}: {ke}. Check dict keys (TOP/JGL/etc).")
             stats["skipped_incomplete_map"] += 1
             continue

        if len(new_row) != len(SCRIMS_HEADER):
             debug_logs.append(f"Error: Row length mismatch for {s_id}. Expected {len(SCRIMS_HEADER)}, got {len(new_row)}")
             stats["skipped_incomplete_map"] += 1
             continue

        new_rows.append(new_row)
        existing_ids.add(m_id) # Добавляем ID в множество обработанных
        stats["processed"] += 1
    # --- Конец цикла for ---

    progress_bar.progress(1.0, text="Update complete. Checking results...")
    summary = [
        f"\n--- Update Summary ---",
        f"Series Checked: {total_series}",
        f"Our Scrims Found (GRID): {stats['gms_count']}",
        f"Skipped (Already Exists): {stats['skip_dupes']}",
        f"Skipped (No/Bad Game Data): {stats['skipped_no_game_data']}",
        f"Skipped (Incomplete/Bad Map): {stats['skipped_incomplete_map']}",
        f"Processed Successfully: {stats['processed']}",
        f"New Records Added: {len(new_rows)}"
    ]
    debug_logs.extend(summary)
    st.code("\n".join(debug_logs[-10:]), language=None) # Показываем последние логи и итоги

    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            st.success(f"Added {len(new_rows)} new records to '{worksheet.title}'.")
            # Очищаем кэш агрегации ПОСЛЕ успешной записи, если он используется
            try: aggregate_scrims_data.clear()
            except AttributeError: pass # Игнорируем, если кэш не используется
            return True
        except gspread.exceptions.APIError as api_err:
            error_msg = f"GSpread API Error appending rows: {api_err}"
            debug_logs.append(error_msg)
            st.error(error_msg)
            st.error(f"Failed to add {len(new_rows)} rows. See logs above.")
            return False
        except Exception as e:
            error_msg = f"Error appending rows: {e}"
            debug_logs.append(error_msg)
            st.error(error_msg)
            st.error(f"Failed to add {len(new_rows)} rows. See logs above.")
            return False
    else:
        st.info("No new valid records found to add.")
        return False # Возвращаем False, если ничего не добавлено

# --- ИЗМЕНЕНА: aggregate_scrims_data (теперь возвращает и статистику игроков) ---
# В файле scrims.py

# --- aggregate_scrims_data (ИСПРАВЛЕНЫ ОТСТУПЫ) ---
@st.cache_data(ttl=600) # Кэшируем результат на 10 минут
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    """
    Агрегирует данные из Google Sheet, читая фактических чемпионов
    из колонок 'Actual_SIDE_ROLE'.
    Возвращает статистику по сторонам, историю матчей и статистику игроков.
    """
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        return {}, {}, pd.DataFrame(), {} # Возвращаем 4 значения: blue_stats, red_stats, df_history, player_champion_stats

    # Инициализация статистики
    blue_stats = {"wins": 0, "losses": 0, "total": 0}
    red_stats = {"wins": 0, "losses": 0, "total": 0}
    history_rows = []
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0}))
    expected_cols = len(SCRIMS_HEADER) # Ожидаемое количество колонок

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
        return {}, {}, pd.DataFrame(), {}
    except Exception as e:
        st.error(f"Read error during aggregation: {e}")
        return {}, {}, pd.DataFrame(), {}

    if len(data) <= 1: # Если только заголовок или пусто
        st.info("No data found in the sheet for aggregation.")
        return {}, {}, pd.DataFrame(), {}

    header = data[0]
    # Проверяем заголовок на соответствие SCRIMS_HEADER
    if header != SCRIMS_HEADER:
        st.error(f"Header mismatch in '{worksheet.title}' during aggregation. Cannot proceed safely.")
        st.error(f"Expected {len(SCRIMS_HEADER)} cols, Found {len(header)} cols.")
        st.code(f"Expected: {SCRIMS_HEADER}\nFound:    {header}", language=None)
        return {}, {}, pd.DataFrame(), {} # Останавливаем выполнение

    # Создаем индекс колонок на основе SCRIMS_HEADER
    idx = {name: i for i, name in enumerate(SCRIMS_HEADER)}

    # Обработка строк данных
    for row_index, row in enumerate(data[1:], start=2): # start=2 для нумерации строк в таблице
        # Пропускаем строки с неверным количеством колонок
        if len(row) != expected_cols:
            # st.warning(f"Skipping row {row_index} due to column count mismatch.") # Опционально для отладки
            continue
        try:
            date_str = row[idx["Date"]]
            # Применяем фильтр по времени, если он активен
            if time_threshold and date_str != "N/A":
                try:
                    # Парсим дату без учета миллисекунд и таймзоны
                    date_obj = datetime.strptime(date_str.split('.')[0], "%Y-%m-%d %H:%M:%S")
                    if date_obj < time_threshold:
                        continue # Пропускаем строку, если она старше фильтра
                except ValueError:
                    # st.warning(f"Skipping row {row_index} due to invalid date format: '{date_str}'") # Опционально
                    continue # Пропускаем строки с неверной датой при активном фильтре

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
                blue_stats["total"] += 1
                if res == "Win": blue_stats["wins"] += 1
                elif res == "Loss": blue_stats["losses"] += 1
            else: # Наша команда красная
                red_stats["total"] += 1
                if res == "Win": red_stats["wins"] += 1
                elif res == "Loss": red_stats["losses"] += 1

            # --- Подсчет статистики игроков по фактическим чемпионам ---
            side_prefix = "Blue" if is_our_blue else "Red"
            # Проходим по известным ролям нашей команды
            for player_id, role_full in PLAYER_ROLES_BY_ID.items():
                player_name = PLAYER_IDS.get(player_id) # Получаем имя игрока по его ID
                if player_name: # Если игрок найден в нашем ростере
                    # Формируем короткое имя роли для ключа словаря/колонки
                    role_short = role_full.replace("MIDDLE", "MID").replace("BOTTOM", "BOT").replace("UTILITY", "SUP").replace("JUNGLE","JGL")
                    # Формируем имя колонки с фактическим чемпионом для нужной стороны и роли
                    actual_champ_col_name = f"Actual_{side_prefix}_{role_short}" # e.g., Actual_Blue_TOP

                    # Получаем чемпиона из ЭТОЙ колонки
                    champion = row[idx[actual_champ_col_name]]
                    # Обновляем статистику, если чемпион не "N/A" и не пустой
                    if champion and champion != "N/A" and champion.strip() != "":
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
            # Логируем ошибку индекса, если нужно для отладки
            # st.warning(f"Skipping row {row_index} due to IndexError: {e_idx}. Check column count and idx dictionary.")
            continue # Пропускаем строку
        except Exception as e_inner:
            # Логируем другие ошибки обработки строк
            # st.warning(f"Skipping row {row_index} due to error: {e_inner}")
            continue # Пропускаем строку

    # --- Постобработка и возврат результатов ---
    df_hist = pd.DataFrame(history_rows)
    if not df_hist.empty:
        try:
            # Сортируем историю по дате (новые сверху)
            df_hist['DT_temp'] = pd.to_datetime(df_hist['Date'], errors='coerce')
            df_hist = df_hist.sort_values(by='DT_temp', ascending=False).drop(columns=['DT_temp'])
        except Exception:
             pass # Игнорируем ошибку сортировки, если формат даты некорректен

    # Конвертируем и сортируем статистику игроков
    final_player_stats = {player: dict(champions) for player, champions in player_stats.items()}
    for player in final_player_stats:
        # Сортируем чемпионов по количеству игр (убывание)
        final_player_stats[player] = dict(sorted(
            final_player_stats[player].items(),
            key=lambda item: item[1].get('games', 0), # Безопасный доступ к 'games'
            reverse=True
        ))

    return blue_stats, red_stats, df_hist, final_player_stats

# --- scrims_page (ИЗМЕНЕНА для использования новой aggregate_scrims_data) ---
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
