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

# Настройки
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63")
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed"
SCRIMS_WORKSHEET_NAME = "Scrims"

# --- DDRagon Helper Functions ---
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

# --- Google Sheets Setup ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    try: wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26); header = ["Date","Match ID","Blue Team","Red Team","Blue Ban 1","Blue Ban 2","Blue Ban 3","Blue Ban 4","Blue Ban 5","Red Ban 1","Red Ban 2","Red Ban 3","Red Ban 4","Red Ban 5","Blue Pick 1","Blue Pick 2","Blue Pick 3","Blue Pick 4","Blue Pick 5","Red Pick 1","Red Pick 2","Red Pick 3","Red Pick 4","Red Pick 5","Duration","Result"]
            wks.append_row(header, value_input_option='USER_ENTERED')
        except Exception as e: st.error(f"Error creating worksheet '{name}': {e}"); return None
    except Exception as e: st.error(f"Error accessing worksheet '{name}': {e}"); return None
    return wks

# --- GRID API Functions (Keep as is) ---
@st.cache_data(ttl=300)
def get_all_series(_debug_placeholder):
    # Use a separate list for internal logging if needed, don't rely on passed list for cache key
    internal_logs = []
    headers = {
        "x-api-key": GRID_API_KEY,
        "Content-Type": "application/json"
    }
    query = """
    query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
        allSeries(
            filter: $filter
            first: $first
            after: $after
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            totalCount
            pageInfo {
                hasNextPage
                endCursor
            }
            edges {
                node {
                    id
                    startTimeScheduled
                }
            }
        }
    }
    """
    lookback_days = 180
    start_date_threshold = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    variables = {
        "filter": {
            "titleId": 3,
            "types": ["SCRIM"],
            "startTimeScheduled": {"gte": start_date_threshold}
        },
        "first": 50,
        "orderBy": "StartTimeScheduled",
        "orderDirection": "DESC"
    }

    all_series_nodes = []
    has_next_page = True
    after_cursor = None
    page_number = 1
    max_pages = 20

    while has_next_page and page_number <= max_pages:
        current_variables = variables.copy()
        if after_cursor:
            current_variables["after"] = after_cursor

        try:
            response = requests.post(
                f"{GRID_BASE_URL}central-data/graphql",
                headers=headers,
                json={"query": query, "variables": current_variables},
                timeout=20
            )
            response.raise_for_status() # Raise HTTP errors

            data = response.json()

            # --- ВОССТАНОВЛЕННЫЙ БЛОК ---
            if "errors" in data:
                 internal_logs.append(f"GraphQL Error (Page {page_number}): {data['errors']}")
                 st.error(f"GraphQL Error: {data['errors']}") # Show error in UI
                 break # Stop pagination on error
            # --- КОНЕЦ ВОССТАНОВЛЕННОГО БЛОКА ---

            all_series_data = data.get("data", {}).get("allSeries", {})
            series_edges = all_series_data.get("edges", [])
            all_series_nodes.extend([s["node"] for s in series_edges if "node" in s])

            page_info = all_series_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")
            # internal_logs.append(f"GraphQL Page {page_number}: Fetched {len(series_edges)} series. HasNext: {has_next_page}") # Optional log

            page_number += 1
            time.sleep(0.2) # Small delay between pages

        except requests.exceptions.RequestException as e:
            internal_logs.append(f"Network error fetching GraphQL page {page_number}: {e}")
            st.error(f"Network error fetching series list: {e}")
            return [] # Return empty on error
        except Exception as e:
             internal_logs.append(f"Unexpected error fetching GraphQL page {page_number}: {e}")
             st.error(f"Unexpected error fetching series list: {e}")
             return []

    # if page_number > max_pages: st.warning(f"Reached max pages ({max_pages})") # Optional warning
    # internal_logs.append(f"Total series fetched: {len(all_series_nodes)}")
    # print("\n".join(internal_logs)) # Optional print for server logs
    return all_series_nodes

def download_series_data(series_id, debug_logs, max_retries=3, initial_delay=2):
    """Downloads end-state data for a specific series ID."""
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            # debug_logs.append(f"Series {series_id} Request: GET {url} -> Status {response.status_code}") # Optional logging

            # --- ИСПРАВЛЕННЫЙ БЛОК ---
            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError:
                    debug_logs.append(f"Error: Could not decode JSON for Series {series_id}. Content: {response.text[:200]}")
                    # st.warning(f"Invalid JSON received for Series {series_id}") # Less verbose
                    return None
            elif response.status_code == 429:
                delay = initial_delay * (2 ** attempt)
                debug_logs.append(f"Warn: 429 S {series_id}. Wait {delay}s (Att {attempt+1}/{max_retries})")
                st.toast(f"Rate limit hit, waiting {delay}s...")
                time.sleep(delay)
                continue # Retry
            elif response.status_code == 404:
                # debug_logs.append(f"Info: Series {series_id} 404.") # Less verbose logging for 404
                return None # Don't retry
            else:
                debug_logs.append(f"Error: API S {series_id} Status {response.status_code}, Resp: {response.text[:200]}")
                response.raise_for_status() # Raise other errors
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---

        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network S {series_id} (Att {attempt+1}): {e}")
            if attempt < max_retries - 1:
                 delay = initial_delay * (2 ** attempt)
                 time.sleep(delay)
            else:
                 st.error(f"Network error fetching series {series_id} after {max_retries} attempts: {e}")
                 return None

    debug_logs.append(f"Error: Failed S {series_id} download after {max_retries} attempts.")
    return None


def download_game_data(game_id, debug_logs, max_retries=3, initial_delay=2):
    """Downloads end-state data for a specific game ID."""
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/game/{game_id}"

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            # debug_logs.append(f"Game {game_id} Request: GET {url} -> Status {response.status_code}") # Optional logging

            # --- ИСПРАВЛЕННЫЙ БЛОК ---
            if response.status_code == 200:
                 try:
                    return response.json()
                 except json.JSONDecodeError:
                     debug_logs.append(f"Error: Could not decode JSON for Game {game_id}. Content: {response.text[:200]}")
                     # st.warning(f"Invalid JSON received for Game {game_id}") # Less verbose
                     return None
            elif response.status_code == 429:
                delay = initial_delay * (2 ** attempt)
                debug_logs.append(f"Warn: 429 G {game_id}. Wait {delay}s (Att {attempt+1}/{max_retries})")
                st.toast(f"Rate limit hit, waiting {delay}s...")
                time.sleep(delay)
                continue # Retry
            elif response.status_code == 404:
                # debug_logs.append(f"Info: Game {game_id} 404.") # Less verbose
                return None # Don't retry
            else:
                 debug_logs.append(f"Error: API G {game_id} Status {response.status_code}, Resp: {response.text[:200]}")
                 response.raise_for_status()
            # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---

        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network G {game_id} (Att {attempt+1}): {e}")
            if attempt < max_retries - 1:
                 delay = initial_delay * (2 ** attempt)
                 time.sleep(delay)
            else:
                 st.error(f"Network error fetching game {game_id} after {max_retries} attempts: {e}")
                 return None

    debug_logs.append(f"Error: Failed G {game_id} download after {max_retries} attempts.")
    return None


# --- update_scrims_data (Keep reverted version without strict pick check) ---
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not worksheet: st.error("Invalid Sheet."); return False
    if not series_list: st.info("No series found."); return False
    try: existing_data = worksheet.get_all_values(); existing_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    except Exception as e: st.error(f"Read error: {e}"); return False
    new_rows, gms_count, skip_dupes, processed = [], 0, 0, 0; delay, total = 1.0, len(series_list)
    for i, s_summary in enumerate(series_list):
        s_id = s_summary.get("id");
        if not s_id: continue
        prog = (i + 1) / total;
        try: progress_bar.progress(prog, text=f"Processing {i+1}/{total}")
        except Exception: pass
        if i > 0: time.sleep(delay)
        s_data = download_series_data(s_id, debug_logs=debug_logs);
        if not s_data: continue
        teams = s_data.get("teams");
        if not teams or len(teams) < 2: continue
        t0, t1 = teams[0], teams[1]; t0_n, t1_n = t0.get("name", "N/A"), t1.get("name", "N/A")
        if TEAM_NAME not in [t0_n, t1_n]: continue
        gms_count += 1; m_id = str(s_data.get("matchId", s_id))
        if m_id in existing_ids: skip_dupes += 1; continue
        date_s = s_data.get("startTime", s_summary.get("startTimeScheduled", s_data.get("updatedAt"))); date_f = "N/A"
        if date_s and isinstance(date_s, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try: date_f = datetime.strptime(date_s, fmt).strftime("%Y-%m-%d %H:%M:%S"); break
                except ValueError: continue
        b_team, r_team = t0_n, t1_n; g_id, g_data = None, None
        potential_games = s_data.get("games", []) or (s_data.get("object", {}).get("games") if isinstance(s_data.get("object"), dict) else [])
        if isinstance(potential_games, list) and potential_games: info = potential_games[0]; g_id = info.get("id") if isinstance(info, dict) else info if isinstance(info, str) else None
        if g_id: time.sleep(0.5); g_data = download_game_data(g_id, debug_logs=debug_logs)
        actions = []; duration_s = None
        if g_data: actions = g_data.get("draftActions", []); duration_s = g_data.get("clock", {}).get("currentSeconds", g_data.get("duration"))
        else:
            if isinstance(potential_games, list) and potential_games and isinstance(potential_games[0], dict): g_scrim = potential_games[0]; actions = g_scrim.get("draftActions", []); duration_s = g_scrim.get("clock", {}).get("currentSeconds", g_scrim.get("duration"))
            if duration_s is None: duration_s = s_data.get("duration")
        b_bans, r_bans, b_picks, r_picks = ["N/A"]*5, ["N/A"]*5, ["N/A"]*5, ["N/A"]*5
        if actions:
            try: actions.sort(key=lambda x: int(x.get("sequenceNumber", 99)))
            except Exception: pass
            bb, rb, bp, rp, seqs = 0, 0, 0, 0, set()
            for act in actions:
                try:
                    seq = int(act.get("sequenceNumber", -1));
                    if seq in seqs or seq == -1: continue; seqs.add(seq); type = act.get("type"); champ = act.get("draftable", {}).get("name", "N/A")
                    if type == "ban":
                        if seq in [1,3,5,14,16]: bb += 1; b_bans[bb-1] = champ if bb <= 5 else champ
                        elif seq in [2,4,6,13,15]: rb += 1; r_bans[rb-1] = champ if rb <= 5 else champ
                    elif type == "pick":
                        if seq in [7,10,11,18,19]: bp += 1; b_picks[bp-1] = champ if bp <= 5 else champ
                        elif seq in [8,9,12,17,20]: rp += 1; r_picks[rp-1] = champ if rp <= 5 else champ
                except Exception: continue
        duration_f = "N/A";
        if isinstance(duration_s, (int, float)) and duration_s >= 0:
            try:
                minutes = int(duration_s // 60)
                seconds = int(duration_s % 60)
                duration_f = f"{minutes}:{seconds:02d}" # Формат MM:SS
            except Exception as e:
                # Можно добавить логгирование ошибки форматирования, если нужно
                # debug_logs.append(f"Warn: Formatting duration {duration_s} failed: {e}")
                pass # Оставляем duration_f как "N/A" при ошибке форматирования
        # --- КОНЕЦ ИСПРАВЛЕННОГО БЛОКА ---

        res = "N/A"; t0w, t1w = t0.get("won"), t1.get("won")
        if t0w is True: res="Win" if t0_n==TEAM_NAME else "Loss"; elif t1w is True: res="Win" if t1_n==TEAM_NAME else "Loss"; elif t0w is False and t1w is False and t0.get("outcome")=="tie": res="Tie"
        new_row = [date_f, m_id, b_team, r_team, *b_bans, *r_bans, *b_picks, *r_picks, duration_f, res]
        if len(new_row) != 26: continue
        new_rows.append(new_row); existing_ids.add(m_id); processed += 1
    progress_bar.progress(1.0, text="Updating sheet...")
    summary = [f"\n--- Summary ---", f"Checked:{total}", f"{TEAM_NAME}:{gms_count}", f"Dupes:{skip_dupes}", f"Processed:{processed}", f"New:{len(new_rows)}"]
    debug_logs.extend(summary) # Use local debug_logs list
    if new_rows: try: worksheet.append_rows(new_rows, value_input_option='USER_ENTERED'); st.success(f"Added {len(new_rows)} scrims."); return True; except Exception as e: st.error(f"Append err:{e}"); return False
    else: st.info("No new scrims."); return False

# --- aggregate_scrims_data (УБРАНА ОТЛАДКА) ---
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    if not worksheet: return {}, {}, pd.DataFrame()
    blue_stats, red_stats, history_rows, expected_cols = {"wins":0,"losses":0,"total":0}, {"wins":0,"losses":0,"total":0}, [], 26
    now, time_threshold = datetime.utcnow(), None
    if time_filter == "1 Week": time_threshold = now - timedelta(weeks=1)
    elif time_filter == "2 Weeks": time_threshold = now - timedelta(weeks=2)
    elif time_filter == "3 Weeks": time_threshold = now - timedelta(weeks=3)
    elif time_filter == "4 Weeks": time_threshold = now - timedelta(weeks=4)
    elif time_filter == "2 Months": time_threshold = now - timedelta(days=60)
    try: data = worksheet.get_all_values()
    except Exception as e: st.error(f"Read error agg: {e}"); return blue_stats, red_stats, pd.DataFrame()
    if len(data) <= 1: return blue_stats, red_stats, pd.DataFrame()
    header = data[0]
    try: idx = {name: header.index(name) for name in ["Date", "Match ID", "Blue Team", "Red Team", "Duration", "Result", "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5", "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5", "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5", "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5"]}
    except ValueError as e: st.error(f"Header error agg: {e}."); return blue_stats, red_stats, pd.DataFrame()
    for row in data[1:]:
        if len(row) < expected_cols: continue
        try:
            date_str = row[idx["Date"]]
            if time_threshold and date_str != "N/A":
                try:
                    if datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") < time_threshold: continue
                except ValueError: continue
            blue_team, red_team, result = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]
            is_our, is_blue = False, False
            if blue_team == TEAM_NAME: is_our, is_blue = True, True
            elif red_team == TEAM_NAME: is_our, is_blue = True, False
            if is_our: # Changed from is_our_game to is_our
                win = (result == "Win")
                if is_blue:
                    blue_stats["total"]+=1
                    if win: blue_stats["wins"]+=1
                    elif result=="Loss": blue_stats["losses"]+=1
                else:
                    red_stats["total"]+=1
                    if win: red_stats["wins"]+=1
                    elif result=="Loss": red_stats["losses"]+=1
            bb_html=" ".join(get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6) if row[idx[f"Blue Ban {i}"]] != "N/A")
            rb_html=" ".join(get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6) if row[idx[f"Red Ban {i}"]] != "N/A")
            bp_html=" ".join(get_champion_icon_html(row[idx[f"Blue Pick {i}"]]) for i in range(1, 6) if row[idx[f"Blue Pick {i}"]] != "N/A")
            rp_html=" ".join(get_champion_icon_html(row[idx[f"Red Pick {i}"]]) for i in range(1, 6) if row[idx[f"Red Pick {i}"]] != "N/A")
            history_rows.append({"Date":date_str,"Blue Team":blue_team,"B Bans":bb_html,"B Picks":bp_html,"Result":result,"Duration":row[idx["Duration"]],"R Picks":rp_html,"R Bans":rb_html,"Red Team":red_team,"Match ID":row[idx["Match ID"]]})
        except Exception: continue
    df_history = pd.DataFrame(history_rows)
    try: df_history['DT'] = pd.to_datetime(df_history['Date'], errors='coerce'); df_history = df_history.sort_values(by='DT', ascending=False).drop(columns=['DT'])
    except Exception: pass
    return blue_stats, red_stats, df_history

# --- scrims_page (ДОБАВЛЕНА КНОПКА НАЗАД) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")

    # --- КНОПКА НАЗАД ---
    if st.button("⬅️ Back to HLL Stats"):
        st.session_state.current_page = "Hellenic Legends League Stats"
        st.rerun()
    # --- КОНЕЦ КНОПКИ НАЗАД ---

    client = setup_google_sheets();
    if not client: st.error("GSheets client failed."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Sheet access error: {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: st.error(f"Worksheet access error."); return

    with st.expander("Update Scrim Data", expanded=False):
        logs = [];
        # Use a different session state key if needed, or clear appropriately
        if 'scrims_update_logs' not in st.session_state: st.session_state.scrims_update_logs = []
        if st.button("Download & Update from GRID API", key="update_scrims_btn"):
            st.session_state.scrims_update_logs = []; logs = st.session_state.scrims_update_logs
            with st.spinner("Fetching series..."): series_list = get_all_series(logs) # Pass logs list
            if series_list:
                st.info(f"Processing {len(series_list)} series...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting...")
                try: data_added = update_scrims_data(wks, series_list, logs, progress_bar) # Pass logs list
                except Exception as e: st.error(f"Update error: {e}"); logs.append(f"FATAL: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No series found.")
        # Display logs if they exist in session state
        if st.session_state.scrims_update_logs:
             st.code("\n".join(st.session_state.scrims_update_logs), language=None)

    st.divider(); st.subheader("Scrim Performance")
    time_f = st.selectbox("Filter by Time:", ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # --- Call aggregate function (без отладки) ---
    blue_s, red_s, df_hist = aggregate_scrims_data(wks, time_f)

    # --- Display Summary Win Rates ---
    try:
        games_f = blue_s["total"] + red_s["total"]; wins_f = blue_s["wins"] + red_s["wins"]; loss_f = blue_s["losses"] + red_s["losses"]
        st.markdown(f"**Performance ({time_f})**"); co, cb, cr = st.columns(3)
        with co: wr = (wins_f / games_f * 100) if games_f > 0 else 0; st.metric("Total Games", games_f); st.metric("Overall WR", f"{wr:.1f}%", f"{wins_f}W-{loss_f}L")
        with cb: bwr = (blue_s["wins"] / blue_s["total"] * 100) if blue_s["total"] > 0 else 0; st.metric("Blue WR", f"{bwr:.1f}%", f"{blue_s['wins']}W-{blue_s['losses']}L ({blue_s['total']} G)")
        with cr: rwr = (red_s["wins"] / red_s["total"] * 100) if red_s["total"] > 0 else 0; st.metric("Red WR", f"{rwr:.1f}%", f"{red_s['wins']}W-{red_s['losses']}L ({red_s['total']} G)")
    except Exception as e: st.error(f"Error display summary: {e}")

    st.divider(); st.subheader(f"Match History ({time_f})")
    if not df_hist.empty: st.markdown(df_hist.to_html(escape=False, index=False, classes='compact-table history-table', justify='center'), unsafe_allow_html=True)
    else: st.info(f"No match history for {time_f}.")

# --- Keep __main__ block as is ---
if __name__ == "__main__": pass
# --- END OF FILE scrims.py ---
