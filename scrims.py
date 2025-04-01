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
    try: wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26); header = ["Date","Match ID","Blue Team","Red Team","Blue Ban 1","Blue Ban 2","Blue Ban 3","Blue Ban 4","Blue Ban 5","Red Ban 1","Red Ban 2","Red Ban 3","Red Ban 4","Red Ban 5","Blue Pick 1","Blue Pick 2","Blue Pick 3","Blue Pick 4","Blue Pick 5","Red Pick 1","Red Pick 2","Red Pick 3","Red Pick 4","Red Pick 5","Duration","Result"]
            wks.append_row(header, value_input_option='USER_ENTERED')
        except Exception as e: st.error(f"Error creating worksheet '{name}': {e}"); return None
    except Exception as e: st.error(f"Error accessing worksheet '{name}': {e}"); return None
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
    if not worksheet: st.error("Invalid Sheet."); return False
    if not series_list: st.info("No series found."); return False
    try: existing_data = worksheet.get_all_values(); existing_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    except Exception as e: st.error(f"Read error: {e}"); return False

    new_rows, gms_count, skip_dupes, processed, skipped_no_game_data, skipped_incomplete_map = [], 0, 0, 0, 0, 0
    api_request_delay, total = 1.0, len(series_list)

    for i, s_summary in enumerate(series_list):
        s_id = s_summary.get("id");
        if not s_id: continue
        prog = (i + 1) / total;
        try: progress_bar.progress(prog, text=f"Processing {i+1}/{total}")
        except Exception: pass
        if i > 0: time.sleep(api_request_delay)

        s_data = download_series_data(s_id, debug_logs=debug_logs);
        if not s_data: continue
        teams = s_data.get("teams");
        if not teams or len(teams) < 2: continue
        t0, t1 = teams[0], teams[1]; t0_n, t1_n = t0.get("name", "N/A"), t1.get("name", "N/A")
        if TEAM_NAME not in [t0_n, t1_n]: continue
        gms_count += 1; m_id = str(s_data.get("matchId", s_id))
        if m_id in existing_ids: skip_dupes += 1; continue

        # --- Загружаем данные игры (g_data) ---
        g_id, g_data = None, None
        potential_games = s_data.get("games", []) or (s_data.get("object", {}).get("games") if isinstance(s_data.get("object"), dict) else [])
        if isinstance(potential_games, list) and potential_games: info = potential_games[0]; g_id = info.get("id") if isinstance(info, dict) else info if isinstance(info, str) else None
        if g_id: time.sleep(0.5); g_data = download_game_data(g_id, debug_logs=debug_logs)

        # --- !!! КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: Требуем g_data для определения пиков !!! ---
        if not g_data or 'games' not in g_data or not g_data['games'] or 'teams' not in g_data['games'][0]:
            debug_logs.append(f"Warn: Skipping {s_id} - Missing g_data or g_data['games'][0]['teams']")
            skipped_no_game_data += 1
            continue

        # --- Извлекаем дату ---
        date_s = s_data.get("startTime", s_summary.get("startTimeScheduled", s_data.get("updatedAt"))); date_f = "N/A"
        if date_s and isinstance(date_s, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try: date_f = datetime.strptime(date_s, fmt).strftime("%Y-%m-%d %H:%M:%S"); break
                except ValueError: continue
        b_team_name, r_team_name = t0_n, t1_n # Используем имена из s_data

        # --- Извлекаем баны из draftActions (могут быть и в s_data, и в g_data) ---
        draft_actions = g_data['games'][0].get("draftActions", []) # Берем из g_data
        b_bans, r_bans = ["N/A"]*5, ["N/A"]*5
        if draft_actions:
            try: actions_sorted = sorted(draft_actions, key=lambda x: int(x.get("sequenceNumber", 99)))
            except Exception: actions_sorted = draft_actions # Use unsorted if error
            bb, rb, seqs = 0, 0, set()
            for act in actions_sorted:
                try:
                    seq = int(act.get("sequenceNumber", -1));
                    if seq in seqs or seq == -1: continue; seqs.add(seq); type = act.get("type"); champ = act.get("draftable", {}).get("name", "N/A")
                    if type == "ban":
                        if seq in [1,3,5,14,16]: bb += 1; b_bans[bb-1] = champ if bb <= 5 else champ
                        elif seq in [2,4,6,13,15]: rb += 1; r_bans[rb-1] = champ if rb <= 5 else champ
                except Exception: continue

        # --- !!! ИЗВЛЕКАЕМ ПИКИ ИГРОКОВ ИЗ g_data['games'][0]['teams'] !!! ---
        player_champion_map = {} # {player_id: champion_name}
        our_team_side = None
        game_teams_data = g_data['games'][0]['teams'] # Список команд в данных игры

        for team_state in game_teams_data:
            team_id_in_game = team_state.get("id") # ID команды в данных игры
            # Сопоставляем с ID команды из s_data, чтобы понять, кто есть кто
            is_our_team_in_game = False
            original_team_id = None
            if t0.get("id") == team_id_in_game and t0_n == TEAM_NAME: is_our_team_in_game = True; original_team_id = t0.get("id")
            if t1.get("id") == team_id_in_game and t1_n == TEAM_NAME: is_our_team_in_game = True; original_team_id = t1.get("id")

            if is_our_team_in_game:
                our_team_side = team_state.get("side") # 'blue' или 'red'
                players_list = team_state.get("players", [])
                for player_state in players_list:
                    player_id = player_state.get("id")
                    champion_name = player_state.get("character", {}).get("name", "N/A")
                    if player_id in PLAYER_IDS: # Проверяем, что это игрок нашей команды
                        player_champion_map[player_id] = champion_name

        # --- Формируем списки пиков в правильном порядке ролей ---
        b_picks, r_picks = ["N/A"]*5, ["N/A"]*5
        picks_to_fill = b_picks if our_team_side == 'blue' else r_picks if our_team_side == 'red' else None

        if picks_to_fill is None:
             debug_logs.append(f"Warn: Could not determine side for team {TEAM_NAME} in {s_id}");
             skipped_incomplete_map += 1
             continue # Не можем заполнить пики, если сторона неизвестна

        found_all_players = True
        for i, role in enumerate(ROLE_ORDER_FOR_SHEET):
            player_id_for_role = None
            # Находим ID игрока для этой роли
            for p_id, r in PLAYER_ROLES_BY_ID.items():
                if r == role:
                    player_id_for_role = p_id
                    break
            if player_id_for_role:
                champion = player_champion_map.get(player_id_for_role, "N/A") # Получаем чемпа по ID
                if i < 5: # Защита от выхода за пределы списка
                     picks_to_fill[i] = champion
                     if champion == "N/A":
                         found_all_players = False # Отмечаем, если чемпион не найден для игрока
                         debug_logs.append(f"Warn: Champion not found for player {PLAYER_IDS.get(player_id_for_role)} (ID: {player_id_for_role}, Role: {role}) in {s_id}")
            else:
                found_all_players = False # Отмечаем, если игрок для роли не найден в ростере
                debug_logs.append(f"Warn: Player not found for role {role} in roster for {s_id}")

        # --- !!! Проверка на полноту карты игрок-чемпион !!! ---
        # Убрана проверка на "N/A" в picks_to_fill, т.к. мы ее уже сделали и залогировали
        if not found_all_players or len(player_champion_map) < 5:
            debug_logs.append(f"Warn: Skipping {s_id} - Incomplete player-champion map. Found: {player_champion_map}")
            skipped_incomplete_map += 1
            continue # Пропускаем, если не удалось найти всех игроков/чемпионов

        # --- Извлекаем пики противника (пока просто по порядку из draftActions, если нужно) ---
        # Можно оставить как есть или попытаться улучшить позже
        if our_team_side == 'blue':
             # Заполняем r_picks из draftActions, если нужно
             rp = 0
             for act in actions_sorted:
                 if act.get('type') == 'pick' and act.get('side') == 'red' and rp < 5:
                     r_picks[rp] = act.get('draftable', {}).get('name', 'N/A'); rp += 1
        elif our_team_side == 'red':
             # Заполняем b_picks из draftActions, если нужно
             bp = 0
             for act in actions_sorted:
                 if act.get('type') == 'pick' and act.get('side') == 'blue' and bp < 5:
                     b_picks[bp] = act.get('draftable', {}).get('name', 'N/A'); bp += 1


        # --- Определяем результат и длительность ---
        duration_s = None # Переопределяем, ищем в g_data
        if g_data and 'games' in g_data and g_data['games']:
             duration_s = g_data['games'][0].get("clock", {}).get("currentSeconds") # Ищем в данных игры

        duration_f = "N/A";
        if isinstance(duration_s, (int, float)) and duration_s >= 0:
            try:
                minutes = int(duration_s // 60)
                seconds = int(duration_s % 60)
                # Assign formatted duration ONLY if successful
                duration_f = f"{minutes}:{seconds:02d}"
            except Exception as e:
                # duration_f remains "N/A" if formatting fails
                pass

        res = "N/A" # Default result
        t0w = t0.get("won")
        t1w = t1.get("won")

        # Determine result based on win status
        if t0w is True:
            res = "Win" if t0_n == TEAM_NAME else "Loss"
        elif t1w is True:
            res = "Win" if t1_n == TEAM_NAME else "Loss"
        elif t0w is False and t1w is False:
             # Could check for t0.get("outcome") == "tie" if API provides it
             res = "Tie"

        # --- Собираем строку и добавляем ---
        new_row = [date_f, m_id, b_team_name, r_team_name, *b_bans, *r_bans, *b_picks, *r_picks, duration_f, res]
        if len(new_row) != 26: continue
        new_rows.append(new_row); existing_ids.add(m_id); processed += 1
    # --- Конец цикла for ---

    progress_bar.progress(1.0, text="Updating sheet...")
    summary = [f"\n--- Summary ---", f"Checked:{total}", f"{TEAM_NAME}:{gms_count}", f"Dupes:{skip_dupes}", f"Skip (No GameData):{skipped_no_game_data}", f"Skip (Incomplete Map):{skipped_incomplete_map}", f"Processed:{processed}", f"New:{len(new_rows)}"]
    debug_logs.extend(summary)

    if new_rows:
        try: worksheet.append_rows(new_rows, value_input_option='USER_ENTERED'); st.success(f"Added {len(new_rows)} new records."); return True
        except Exception as e: error_msg = f"Error appending rows: {e}"; debug_logs.append(error_msg); st.error(error_msg); return False
    else: st.info("No new valid records found."); return False


# --- ИЗМЕНЕНА: aggregate_scrims_data (теперь возвращает и статистику игроков) ---
# В файле scrims.py

# --- aggregate_scrims_data (ИСПРАВЛЕНЫ ОТСТУПЫ) ---
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    if not worksheet:
        st.error("Aggregate Error: Invalid worksheet object.")
        return {}, {}, pd.DataFrame(), {} # Добавляем пустой словарь для статы игроков

    blue_stats, red_stats, history_rows, expected_cols = {"wins":0,"losses":0,"total":0}, {"wins":0,"losses":0,"total":0}, [], 26
    player_stats = defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0})) # Для статистики игроков

    now, time_threshold = datetime.utcnow(), None
    if time_filter == "1 Week": time_threshold = now - timedelta(weeks=1)
    elif time_filter == "2 Weeks": time_threshold = now - timedelta(weeks=2)
    elif time_filter == "3 Weeks": time_threshold = now - timedelta(weeks=3)
    elif time_filter == "4 Weeks": time_threshold = now - timedelta(weeks=4)
    elif time_filter == "2 Months": time_threshold = now - timedelta(days=60)

    try: data = worksheet.get_all_values()
    except Exception as e: st.error(f"Read error agg: {e}"); return blue_stats, red_stats, pd.DataFrame(), {}
    if len(data) <= 1: return blue_stats, red_stats, pd.DataFrame(), {}

    header = data[0]
    try: idx = {name: header.index(name) for name in ["Date","Match ID","Blue Team","Red Team","Duration","Result","Blue Ban 1","Blue Ban 2","Blue Ban 3","Blue Ban 4","Blue Ban 5","Red Ban 1","Red Ban 2","Red Ban 3","Red Ban 4","Red Ban 5","Blue Pick 1","Blue Pick 2","Blue Pick 3","Blue Pick 4","Blue Pick 5","Red Pick 1","Red Pick 2","Red Pick 3","Red Pick 4","Red Pick 5"]}
    except ValueError as e: st.error(f"Header error agg: {e}."); return blue_stats, red_stats, pd.DataFrame(), {}

    # Получаем обратную карту Роль -> ID игрока
    # Убедимся, что PLAYER_ROLES_BY_ID определен где-то выше в файле
    try:
        role_to_player_id = {role_str: player_id for player_id, role_str in PLAYER_ROLES_BY_ID.items()}
    except NameError:
        st.error("Aggregate Error: PLAYER_ROLES_BY_ID not defined.")
        return blue_stats, red_stats, pd.DataFrame(), {}


    for row_index, row in enumerate(data[1:], start=2): # Добавим индекс для логгирования
        if len(row) < expected_cols: continue
        try:
            date_str = row[idx["Date"]]

            # Фильтрация по времени
            if time_threshold and date_str != "N/A":
                try:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if date_obj < time_threshold:
                        continue # Пропускаем строку, если она старше фильтра
                except ValueError:
                    # Пропускаем строку, если дата некорректна при активном фильтре
                    continue # Или можно не пропускать, если нужна вся история для игроков

            b_team, r_team, res = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]
            is_our, is_blue, our_picks_in_row = False, False, []
            is_our_win = False

            if b_team == TEAM_NAME:
                 is_our, is_blue = True, True
                 our_picks_in_row = [row[idx[f"Blue Pick {i}"]] for i in range(1,6)]
                 is_our_win = (res=="Win")
            elif r_team == TEAM_NAME:
                 is_our, is_blue = True, False
                 our_picks_in_row = [row[idx[f"Red Pick {i}"]] for i in range(1,6)]
                 is_our_win = (res=="Win")

            # --- Блок подсчета статистики с ИСПРАВЛЕННЫМИ ОТСТУПАМИ ---
            if is_our:
                win = (res == "Win") # Определяем победу один раз
                if is_blue:
                    # Отступ для блока if is_blue:
                    blue_stats["total"] += 1
                    if win:
                        # Отступ для блока if win:
                        blue_stats["wins"] += 1
                    elif res == "Loss":
                        # Отступ для блока elif res == "Loss":
                        blue_stats["losses"] += 1
                else: # Red side (На том же уровне отступа, что и if is_blue:)
                    red_stats["total"] += 1
                    if win:
                        # Отступ для блока if win:
                        red_stats["wins"] += 1
                    elif res == "Loss":
                        # Отступ для блока elif res == "Loss":
                        red_stats["losses"] += 1

                # --- Считаем стату игроков/чемпионов (на том же уровне отступа, что и if is_blue:/else:) ---
                for i, role in enumerate(ROLE_ORDER_FOR_SHEET):
                     player_id_for_role = None
                     # Ищем ID игрока для этой роли в нашем ростере
                     for p_id, r in PLAYER_ROLES_BY_ID.items():
                         if r == role:
                             player_id_for_role = p_id
                             break

                     # Убедимся, что PLAYER_IDS определен где-то выше
                     player_name = PLAYER_IDS.get(player_id_for_role) # Получаем имя игрока по ID
                     if player_name and i < len(our_picks_in_row):
                         champion = our_picks_in_row[i]
                         if champion != "N/A":
                             player_stats[player_name][champion]['games'] += 1
                             if is_our_win:
                                 player_stats[player_name][champion]['wins'] += 1
            # --- Конец блока if is_our: ---


            # --- Готовим строку для истории матчей ---
            bb_html=" ".join(get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6) if idx.get(f"Blue Ban {i}") is not None and row[idx[f"Blue Ban {i}"]] != "N/A")
            rb_html=" ".join(get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6) if idx.get(f"Red Ban {i}") is not None and row[idx[f"Red Ban {i}"]] != "N/A")
            bp_html=" ".join(get_champion_icon_html(row[idx[f"Blue Pick {i}"]]) for i in range(1, 6) if idx.get(f"Blue Pick {i}") is not None and row[idx[f"Blue Pick {i}"]] != "N/A")
            rp_html=" ".join(get_champion_icon_html(row[idx[f"Red Pick {i}"]]) for i in range(1, 6) if idx.get(f"Red Pick {i}") is not None and row[idx[f"Red Pick {i}"]] != "N/A")
            history_rows.append({"Date":date_str,"Blue Team":b_team,"B Bans":bb_html,"B Picks":bp_html,"Result":res,"Duration":row[idx["Duration"]],"R Picks":rp_html,"R Bans":rb_html,"Red Team":r_team,"Match ID":row[idx["Match ID"]]})
        except IndexError:
            # st.warning(f"Skipping row {row_index} due to IndexError") # Optional
            continue
        except Exception as e_inner:
            # st.warning(f"Skipping row {row_index} due to processing error: {e_inner}") # Optional
            continue # Пропускаем строку при любой ошибке обработки

    df_hist = pd.DataFrame(history_rows);
    try: df_hist['DT'] = pd.to_datetime(df_hist['Date'], errors='coerce'); df_hist = df_hist.sort_values(by='DT', ascending=False).drop(columns=['DT'])
    except Exception: pass

    # Конвертируем и сортируем статистику игроков
    final_player_stats = {player: dict(champions) for player, champions in player_stats.items()}
    for player in final_player_stats:
        final_player_stats[player] = dict(sorted(final_player_stats[player].items(), key=lambda item: item[1]['games'], reverse=True))

    return blue_stats, red_stats, df_hist, final_player_stats# Возвращаем 4 значения


# --- scrims_page (ИЗМЕНЕНА для использования новой aggregate_scrims_data) ---
def show_scrims_page():
    st.title(" scrims")

    # Используй значение по умолчанию или оставь поле пустым
    series_id = st.text_input(" scrims ID", "2783620") # Пример ID

    # Кнопка для запуска поиска данных
    if st.button(" scrims"):
        # Простая проверка, является ли введенный ID числом
        if not series_id.isdigit():
            st.warning(" scrims ID.")
            # Прерываем выполнение, если ID некорректен
            return

        # Вызов функции для получения данных с API
        # Убедись, что get_scrim_data обрабатывает ошибки и возвращает None в случае неудачи
        data = get_scrim_data(series_id)

        # Продолжаем, только если данные получены успешно, не равны None,
        # содержат ключ 'teams' и в нем ровно две команды.
        if data and data.get('teams') and len(data['teams']) == 2:
            # Отображение общей информации о серии игр
            # Используем .get() для безопасного доступа к вложенным данным
            title_info = data.get('title', {}) # Получаем словарь title или пустой словарь
            st.subheader(f" {title_info.get('nameShortened', 'N/A')} (ID: {series_id})")
            st.write(f" {data.get('format', 'N/A')}")
            # Можно добавить форматирование даты/времени, если нужно
            st.write(f" {data.get('updatedAt', 'N/A')}")

            # Создаем две колонки для отображения команд рядом
            col1, col2 = st.columns(2)

            # --- Обработка и отображение Команды 1 ---
            with col1:
                team1 = data['teams'][0]
                # Безопасно получаем данные команды с значениями по умолчанию
                team1_name = team1.get('name', 'Team 1')
                team1_score = team1.get('score', 0)
                team1_kills = team1.get('kills', 0)
                team1_won = team1.get('won', False)
                # Определяем строку статуса победы/поражения
                status1 = " ( )" if team1_won else " ( )" # Замени смайлики если нужно

                # Отображаем заголовок и статистику команды
                st.header(f"{team1_name}{status1}")
                st.write(f"Score: {team1_score}")
                st.write(f"Kills: {team1_kills}")
                st.markdown("---") # Визуальный разделитель

                # Проверяем, есть ли данные игроков в команде
                if 'players' in team1:
                    # Проходим по каждому игроку в команде
                    for player in team1['players']:
                        # Безопасно получаем данные игрока
                        player_name = player.get('name', 'Unknown Player')
                        player_role = player.get('role', 'Unknown Role').capitalize()

                        # --- ИСПРАВЛЕННАЯ ЛОГИКА ПОЛУЧЕНИЯ ЧЕМПИОНА ---
                        champion_name = "N/A" # Имя чемпиона по умолчанию
                        champion_image_url = None # URL изображения по умолчанию (None)

                        # Безопасно получаем словарь с данными чемпиона для этого игрока
                        champion_data = player.get('champion')
                        # Проверяем, существует ли этот словарь
                        if champion_data is not None:
                            # Безопасно получаем имя и URL изображения чемпиона
                            champion_name = champion_data.get('name', 'Unknown Champion')
                            champion_image_url = champion_data.get('image')
                        # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---

                        # Создаем колонки для информации об игроке и изображения чемпиона
                        player_col, champ_col = st.columns([3, 1]) # Соотношение ширины колонок

                        with player_col:
                            # Отображаем имя и роль игрока
                            st.write(f"**{player_name}** ({player_role})")
                            # Отображаем имя найденного чемпиона
                            st.write(f" {champion_name}")

                        with champ_col:
                            # Отображаем изображение чемпиона, только если URL действителен
                            if champion_image_url is not None:
                                st.image(champion_image_url, width=50)
                            else:
                                # Показываем заглушку, если URL изображения нет
                                st.caption("No Img")

                        # Визуальный разделитель между игроками
                        st.markdown("---")

            # --- Обработка и отображение Команды 2 (логика аналогична Команде 1) ---
            with col2:
                team2 = data['teams'][1]
                # Безопасно получаем данные команды
                team2_name = team2.get('name', 'Team 2')
                team2_score = team2.get('score', 0)
                team2_kills = team2.get('kills', 0)
                team2_won = team2.get('won', False)
                # Определяем статус победы/поражения
                status2 = " ( )" if team2_won else " ( )" # Замени смайлики если нужно

                # Отображаем заголовок и статистику команды
                st.header(f"{team2_name}{status2}")
                st.write(f"Score: {team2_score}")
                st.write(f"Kills: {team2_kills}")
                st.markdown("---") # Визуальный разделитель

                # Проверяем, есть ли данные игроков
                if 'players' in team2:
                     # Проходим по каждому игроку
                    for player in team2['players']:
                        # Безопасно получаем данные игрока
                        player_name = player.get('name', 'Unknown Player')
                        player_role = player.get('role', 'Unknown Role').capitalize()

                        # --- ИСПРАВЛЕННАЯ ЛОГИКА ПОЛУЧЕНИЯ ЧЕМПИОНА ---
                        champion_name = "N/A" # Имя чемпиона по умолчанию
                        champion_image_url = None # URL изображения по умолчанию (None)

                        # Безопасно получаем словарь с данными чемпиона
                        champion_data = player.get('champion')
                        # Проверяем, существует ли этот словарь
                        if champion_data is not None:
                           # Безопасно получаем имя и URL изображения
                           champion_name = champion_data.get('name', 'Unknown Champion')
                           champion_image_url = champion_data.get('image')
                        # --- КОНЕЦ ИСПРАВЛЕННОЙ ЛОГИКИ ---

                        # Создаем колонки для информации об игроке и изображения
                        player_col, champ_col = st.columns([3, 1])

                        with player_col:
                            # Отображаем имя и роль игрока
                            st.write(f"**{player_name}** ({player_role})")
                            # Отображаем имя найденного чемпиона
                            st.write(f" {champion_name}")

                        with champ_col:
                            # Отображаем изображение, только если URL действителен
                            if champion_image_url is not None:
                                st.image(champion_image_url, width=50)
                            else:
                                # Показываем заглушку, если URL нет
                                st.caption("No Img")

                        # Визуальный разделитель между игроками
                        st.markdown("---")

        # Обработка случаев, когда данные получены, но не соответствуют ожидаемой структуре
        elif data:
             st.warning(" scrims ID ( Teams).")
             # Можно раскомментировать следующую строку для отладки, чтобы увидеть полученные данные
             # st.json(data)
        # Случай, когда data равно None (ошибка при вызове API), обрабатывается неявно,
        # так как основной блок 'if data...' не выполнится.
        # Сообщение об ошибке должно было быть выведено функцией get_scrim_data.
        else:
             # Здесь дополнительных действий не требуется, если get_scrim_data обрабатывает ошибки
             pass


# --- Keep __main__ block as is ---
if __name__ == "__main__": pass
# --- END OF FILE scrims.py ---
