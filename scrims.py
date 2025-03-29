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
from collections import defaultdict # Import defaultdict

# Настройки (Keep these specific to scrims)
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63") # Use env var or secrets
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed" # Use a specific name for the scrims sheet
SCRIMS_WORKSHEET_NAME = "Scrims" # Name of the worksheet within the sheet

# --- DDRagon Helper Functions (Copied/Adapted from app.py for self-containment) ---
@st.cache_data(ttl=3600) # Cache for 1 hour
def get_latest_patch_version():
    """Gets the latest LoL patch version from Data Dragon."""
    try:
        # st.info("Fetching patch version...") # Debugging call
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        response.raise_for_status()
        versions = response.json()
        if versions:
            return versions[0]
        st.warning("Could not determine latest patch version from Data Dragon, using default.")
        return "14.14.1" # Update default periodically
    except requests.exceptions.RequestException as e:
        # Check if running in Streamlit context before calling st.error
        try:
            st.error(f"Error fetching patch version: {e}. Using default.")
        except Exception: # If st command fails (e.g., running script directly)
             print(f"Error fetching patch version: {e}. Using default.")
        return "14.14.1" # Update default periodically

# !! УДАЛЕНА СТРОКА: PATCH_VERSION = get_latest_patch_version() !!

@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    """Normalizes champion name for Data Dragon URL."""
    if not champ or champ == "N/A":
        return None
    exceptions = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"}
    if champ in exceptions: return exceptions[champ]
    return "".join(c for c in champ if c.isalnum())

# !! ИЗМЕНЕНА ФУНКЦИЯ !!
def get_champion_icon_html(champion, width=25, height=25):
    """Generates HTML img tag for a champion icon."""
    # Получаем версию патча ЗДЕСЬ, когда функция вызывается
    patch_version = get_latest_patch_version() # <--- ИЗМЕНЕНИЕ
    normalized_champ = normalize_champion_name_for_ddragon(champion)
    if normalized_champ:
        # Используем полученную patch_version
        icon_url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{normalized_champ}.png"
        return f'<img src="{icon_url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return "" # Return empty string if champ is N/A or normalization fails

# Helper function to color win rate cells
def color_win_rate_scrims(value):
    try:
        val = float(value)
        if 0 <= val < 48: return f'<span style="color:#FF7F7F; font-weight: bold;">{val:.1f}%</span>'
        elif 48 <= val <= 52: return f'<span style="color:#FFD700; font-weight: bold;">{val:.1f}%</span>'
        elif val > 52: return f'<span style="color:#90EE90; font-weight: bold;">{val:.1f}%</span>'
        else: return f'{value}'
    except (ValueError, TypeError): return f'{value}'
# --- End of DDRagon Helpers ---


# --- Google Sheets Setup (Keep as is) ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS")
    if not json_creds_str:
        st.error("Google Sheets credentials (GOOGLE_SHEETS_CREDS) not found in environment variables/secrets for Scrims.")
        return None
    try:
        creds_dict = json.loads(json_creds_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        client.list_spreadsheet_files()
        return client
    except json.JSONDecodeError: st.error("Error decoding Google Sheets JSON credentials for Scrims."); return None
    except gspread.exceptions.APIError as e: st.error(f"Google Sheets API Error during Scrims setup: {e}."); return None
    except Exception as e: st.error(f"Unexpected error setting up Google Sheets for Scrims: {e}"); return None

# --- Worksheet Check/Creation (Keep as is) ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    try: wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            st.info(f"Worksheet '{name}' not found in '{spreadsheet.title}', creating...")
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26)
            header_row = ["Date","Match ID","Blue Team","Red Team","Blue Ban 1","Blue Ban 2","Blue Ban 3","Blue Ban 4","Blue Ban 5","Red Ban 1","Red Ban 2","Red Ban 3","Red Ban 4","Red Ban 5","Blue Pick 1","Blue Pick 2","Blue Pick 3","Blue Pick 4","Blue Pick 5","Red Pick 1","Red Pick 2","Red Pick 3","Red Pick 4","Red Pick 5","Duration","Result"]
            wks.append_row(header_row, value_input_option='USER_ENTERED')
            st.info(f"Worksheet '{name}' created with header.")
        except gspread.exceptions.APIError as e: st.error(f"API Error creating worksheet '{name}': {e}"); return None
    except gspread.exceptions.APIError as e: st.error(f"API Error checking/accessing worksheet '{name}': {e}"); return None
    return wks

# --- GRID API Functions (Keep get_all_series, download_series_data, download_game_data as is) ---
@st.cache_data(ttl=300)
def get_all_series(_debug_placeholder):
    internal_logs = []
    headers = {"x-api-key": GRID_API_KEY, "Content-Type": "application/json"}
    query = """
    query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
        allSeries( filter: $filter, first: $first, after: $after, orderBy: $orderBy, orderDirection: $orderDirection ) {
            totalCount, pageInfo { hasNextPage, endCursor }, edges { node { id, startTimeScheduled } }
        }
    }"""
    lookback_days = 180
    start_date_threshold = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables = {"filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_date_threshold}}, "first": 50, "orderBy": "StartTimeScheduled", "orderDirection": "DESC"}
    all_series_nodes, has_next_page, after_cursor, page_number, max_pages = [], True, None, 1, 20
    while has_next_page and page_number <= max_pages:
        current_variables = variables.copy();
        if after_cursor: current_variables["after"] = after_cursor
        try:
            response = requests.post(f"{GRID_BASE_URL}central-data/graphql", headers=headers, json={"query": query, "variables": current_variables}, timeout=20)
            response.raise_for_status(); data = response.json()
            if "errors" in data: internal_logs.append(f"GraphQL Error (Page {page_number}): {data['errors']}"); st.error(f"GraphQL Error: {data['errors']}"); break
            all_series_data = data.get("data", {}).get("allSeries", {}); series_edges = all_series_data.get("edges", [])
            all_series_nodes.extend([s["node"] for s in series_edges if "node" in s])
            page_info = all_series_data.get("pageInfo", {}); has_next_page = page_info.get("hasNextPage", False); after_cursor = page_info.get("endCursor")
            # internal_logs.append(f"GraphQL Page {page_number}: Fetched {len(series_edges)} series. HasNext: {has_next_page}");
            page_number += 1; time.sleep(0.2)
        except requests.exceptions.RequestException as e: internal_logs.append(f"Network error fetching GraphQL page {page_number}: {e}"); st.error(f"Network error fetching series list: {e}"); return []
        except Exception as e: internal_logs.append(f"Unexpected error fetching GraphQL page {page_number}: {e}"); st.error(f"Unexpected error fetching series list: {e}"); return []
    # if page_number > max_pages: st.warning(f"Reached maximum page limit ({max_pages}) for fetching series.")
    # internal_logs.append(f"Total series nodes fetched: {len(all_series_nodes)}")
    return all_series_nodes

def download_series_data(series_id, debug_logs, max_retries=3, initial_delay=2):
    headers = {"x-api-key": GRID_API_KEY}; url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15); # debug_logs.append(f"Series {series_id} Request: GET {url} -> Status {response.status_code}")
            if response.status_code == 200:
                try: return response.json()
                except json.JSONDecodeError: debug_logs.append(f"Error: Could not decode JSON for Series {series_id}. Content: {response.text[:200]}"); return None
            elif response.status_code == 429: delay = initial_delay * (2 ** attempt); debug_logs.append(f"Warning: Received 429 for Series {series_id}. Waiting {delay}s (Attempt {attempt+1}/{max_retries})"); st.toast(f"Rate limit hit, waiting {delay}s..."); time.sleep(delay); continue
            elif response.status_code == 404: debug_logs.append(f"Info: Series {series_id} not found (404). Skipping."); return None
            else: debug_logs.append(f"Error: API request for Series {series_id} failed. Status: {response.status_code}, Response: {response.text[:200]}"); response.raise_for_status()
        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network/Request Exception for Series {series_id} (Attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1: delay = initial_delay * (2 ** attempt); time.sleep(delay)
            else: st.error(f"Network error fetching series {series_id} after {max_retries} attempts: {e}"); return None
    debug_logs.append(f"Error: Failed to download data for Series {series_id} after {max_retries} attempts."); return None

def download_game_data(game_id, debug_logs, max_retries=3, initial_delay=2):
    headers = {"x-api-key": GRID_API_KEY}; url = f"https://api.grid.gg/file-download/end-state/grid/game/{game_id}"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15); # debug_logs.append(f"Game {game_id} Request: GET {url} -> Status {response.status_code}")
            if response.status_code == 200:
                 try: return response.json()
                 except json.JSONDecodeError: debug_logs.append(f"Error: Could not decode JSON for Game {game_id}. Content: {response.text[:200]}"); return None
            elif response.status_code == 429: delay = initial_delay * (2 ** attempt); debug_logs.append(f"Warning: Received 429 for Game {game_id}. Waiting {delay}s (Attempt {attempt+1}/{max_retries})"); st.toast(f"Rate limit hit, waiting {delay}s..."); time.sleep(delay); continue
            elif response.status_code == 404: debug_logs.append(f"Info: Game {game_id} not found (404). Skipping."); return None
            else: debug_logs.append(f"Error: API request for Game {game_id} failed. Status: {response.status_code}, Response: {response.text[:200]}"); response.raise_for_status()
        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network/Request Exception for Game {game_id} (Attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1: delay = initial_delay * (2 ** attempt); time.sleep(delay)
            else: st.error(f"Network error fetching game {game_id} after {max_retries} attempts: {e}"); return None
    debug_logs.append(f"Error: Failed to download data for Game {game_id} after {max_retries} attempts."); return None

# --- update_scrims_data (Keep previous version with the check for complete picks) ---
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not worksheet: debug_logs.append("Error: Invalid worksheet provided."); st.error("Invalid Google Sheet worksheet."); return False
    if not series_list: debug_logs.append("Info: Series list is empty."); st.info("No series found to process."); return False
    try:
        existing_data = worksheet.get_all_values(); existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
        debug_logs.append(f"Found {len(existing_match_ids)} existing Match IDs.")
    except Exception as e: debug_logs.append(f"Error fetching existing data: {e}"); st.error(f"Could not read existing data: {e}"); return False

    new_rows, gamespace_series_count, skipped_duplicates, processed_count, skipped_incomplete_picks = [], 0, 0, 0, 0
    api_request_delay, total_series_to_process = 1.0, len(series_list)
    progress_text_template = "Processing series {current}/{total} (ID: {series_id})"

    for i, series_summary in enumerate(series_list):
        series_id = series_summary.get("id");
        if not series_id: continue
        progress = (i + 1) / total_series_to_process; progress_text = progress_text_template.format(current=i+1, total=total_series_to_process, series_id=series_id)
        try: progress_bar.progress(progress, text=progress_text)
        except Exception: pass
        if i > 0: time.sleep(api_request_delay)
        scrim_data = download_series_data(series_id, debug_logs=debug_logs);
        if not scrim_data: continue
        teams = scrim_data.get("teams");
        if not teams or len(teams) < 2: continue
        team_0, team_1 = teams[0], teams[1]; team_0_name, team_1_name = team_0.get("name", "N/A"), team_1.get("name", "N/A")
        if TEAM_NAME not in [team_0_name, team_1_name]: continue
        gamespace_series_count += 1; match_id = str(scrim_data.get("matchId", series_id))
        if match_id in existing_match_ids: skipped_duplicates += 1; continue
        date_str = scrim_data.get("startTime", series_summary.get("startTimeScheduled", scrim_data.get("updatedAt"))); date_formatted = "N/A"
        if date_str and isinstance(date_str, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try: date_formatted = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d %H:%M:%S"); break
                except ValueError: continue
        blue_team, red_team = team_0_name, team_1_name; game_id, game_data = None, None
        potential_games_list = scrim_data.get("games", []) or (scrim_data.get("object", {}).get("games") if isinstance(scrim_data.get("object"), dict) else [])
        if isinstance(potential_games_list, list) and potential_games_list:
            first_game_info = potential_games_list[0]
            if isinstance(first_game_info, dict): game_id = first_game_info.get("id")
            elif isinstance(first_game_info, str): game_id = first_game_info
        if game_id: time.sleep(0.5); game_data = download_game_data(game_id, debug_logs=debug_logs)
        draft_actions = []; duration_seconds = None
        if game_data: draft_actions = game_data.get("draftActions", []); duration_seconds = game_data.get("clock", {}).get("currentSeconds", game_data.get("duration"))
        else:
            if isinstance(potential_games_list, list) and potential_games_list and isinstance(potential_games_list[0], dict):
                 game_data_from_scrim = potential_games_list[0]; draft_actions = game_data_from_scrim.get("draftActions", []); duration_seconds = game_data_from_scrim.get("clock", {}).get("currentSeconds", game_data_from_scrim.get("duration"))
            if duration_seconds is None: duration_seconds = scrim_data.get("duration")
        blue_bans, red_bans, blue_picks, red_picks = ["N/A"]*5, ["N/A"]*5, ["N/A"]*5, ["N/A"]*5
        if draft_actions:
            try: draft_actions.sort(key=lambda x: int(x.get("sequenceNumber", 99)))
            except (ValueError, TypeError): pass
            bb_idx, rb_idx, bp_idx, rp_idx, sequences = 0, 0, 0, 0, set()
            for action in draft_actions:
                try:
                    seq = int(action.get("sequenceNumber", -1));
                    if seq in sequences or seq == -1: continue; sequences.add(seq)
                    a_type = action.get("type"); champ = action.get("draftable", {}).get("name", "N/A")
                    if a_type == "ban":
                        if seq in [1, 3, 5, 14, 16]:
                            if bb_idx < 5: blue_bans[bb_idx] = champ; bb_idx += 1
                        elif seq in [2, 4, 6, 13, 15]:
                            if rb_idx < 5: red_bans[rb_idx] = champ; rb_idx += 1
                    elif a_type == "pick":
                        if seq in [7, 10, 11, 18, 19]:
                            if bp_idx < 5: blue_picks[bp_idx] = champ; bp_idx += 1
                        elif seq in [8, 9, 12, 17, 20]:
                            if rp_idx < 5: red_picks[rp_idx] = champ; rp_idx += 1
                except (ValueError, TypeError, KeyError): continue
        all_picks_present = "N/A" not in blue_picks and "N/A" not in red_picks
        if not all_picks_present: debug_logs.append(f"Info: Skipping {series_id} due to missing picks."); skipped_incomplete_picks += 1; continue
        duration_formatted = "N/A"
        if isinstance(duration_seconds, (int, float)) and duration_seconds >= 0:
            try: duration_formatted = f"{int(duration_seconds // 60)}:{int(duration_seconds % 60):02d}"
            except Exception: pass
        result = "N/A"; t0_won, t1_won = team_0.get("won"), team_1.get("won")
        if t0_won is True: result = "Win" if team_0_name == TEAM_NAME else "Loss"
        elif t1_won is True: result = "Win" if team_1_name == TEAM_NAME else "Loss"
        elif t0_won is False and t1_won is False and team_0.get("outcome") == "tie": result = "Tie"
        new_row = [date_formatted, match_id, blue_team, red_team, *blue_bans, *red_bans, *blue_picks, *red_picks, duration_formatted, result]
        if len(new_row) != 26: debug_logs.append(f"Error: Row length mismatch for {series_id}."); continue
        new_rows.append(new_row); existing_match_ids.add(match_id); processed_count += 1

    progress_bar.progress(1.0, text="Processing complete. Updating sheet...")
    summary_log = [f"\n--- Scrims Update Summary ---", f"Total series checked: {total_series_to_process}", f"Series involving {TEAM_NAME}: {gamespace_series_count}", f"Skipped duplicate Match IDs: {skipped_duplicates}", f"Skipped due to incomplete picks: {skipped_incomplete_picks}", f"Successfully processed & valid: {processed_count}", f"New rows to add: {len(new_rows)}" ]
    debug_logs.extend(summary_log)
    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            debug_logs.append(f"Success: Appended {len(new_rows)} new rows."); st.success(f"Successfully added {len(new_rows)} new scrim records.")
            aggregate_scrims_data.clear(); get_scrims_data_for_display.clear() # Clear cache
            return True
        except Exception as e: debug_logs.append(f"Error appending data: {str(e)}"); st.error(f"Error appending data: {str(e)}"); return False
    else: debug_logs.append("Info: No new valid rows."); st.info("No new valid scrim records found."); return False


# --- aggregate_scrims_data (For overall stats - Keep as is) ---
@st.cache_data(ttl=600)
def aggregate_scrims_data(worksheet):
    if not worksheet: return {}, {}, 0
    blue_side_stats, red_side_stats, expected_columns = {"wins": 0, "losses": 0, "total": 0}, {"wins": 0, "losses": 0, "total": 0}, 26
    try: data = worksheet.get_all_values();
    except Exception as e: st.error(f"Error reading sheet for summary: {e}"); return blue_side_stats, red_side_stats, 0
    if len(data) <= 1: return blue_side_stats, red_side_stats, 0
    header = data[0]
    try: blue_team_col, red_team_col, result_col = header.index("Blue Team"), header.index("Red Team"), header.index("Result")
    except ValueError as e: st.error(f"Missing header for summary: {e}."); return blue_side_stats, red_side_stats, 0
    total_valid_rows = 0
    for row in data[1:]:
        if len(row) < expected_columns: continue
        try:
            blue_team, red_team, result = row[blue_team_col], row[red_team_col], row[result_col]
            is_our_game = False; is_blue = False
            if blue_team == TEAM_NAME: is_our_game, is_blue = True, True
            elif red_team == TEAM_NAME: is_our_game, is_blue = True, False
            if is_our_game:
                total_valid_rows += 1; win = (result == "Win")
                if is_blue:
                    blue_side_stats["total"] += 1
                    if win: blue_side_stats["wins"] += 1
                    elif result == "Loss": blue_side_stats["losses"] += 1
                else:
                    red_side_stats["total"] += 1
                    if win: red_side_stats["wins"] += 1
                    elif result == "Loss": red_side_stats["losses"] += 1
        except Exception: continue
    return blue_side_stats, red_side_stats, total_valid_rows


# --- get_scrims_data_for_display (Keep previous version) ---
@st.cache_data(ttl=600)
def get_scrims_data_for_display(worksheet, time_filter="All Time"):
    if not worksheet: return pd.DataFrame(), {}
    all_rows, aggregated_stats, roles, expected_columns = [], defaultdict(lambda: defaultdict(lambda: {'games': 0, 'wins': 0})), ["Top", "Jungle", "Mid", "Bot", "Support"], 26
    now, time_threshold = datetime.utcnow(), None
    if time_filter == "1 Week": time_threshold = now - timedelta(weeks=1)
    elif time_filter == "2 Weeks": time_threshold = now - timedelta(weeks=2)
    elif time_filter == "3 Weeks": time_threshold = now - timedelta(weeks=3)
    elif time_filter == "4 Weeks": time_threshold = now - timedelta(weeks=4)
    elif time_filter == "2 Months": time_threshold = now - timedelta(days=60)
    try: data = worksheet.get_all_values();
    except Exception as e: st.error(f"Error reading sheet for display: {e}"); return pd.DataFrame(), {}
    if len(data) <= 1: return pd.DataFrame(), {}
    header = data[0]
    try:
        idx = {name: header.index(name) for name in ["Date", "Match ID", "Blue Team", "Red Team", "Duration", "Result", "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5", "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5", "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5", "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5"]}
    except ValueError as e: st.error(f"Display Error: Missing header: {e}."); return pd.DataFrame(), {}
    for row in data[1:]:
        if len(row) < expected_columns: continue
        try:
            date_str = row[idx["Date"]]
            if time_threshold and date_str != "N/A":
                try:
                    if datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S") < time_threshold: continue
                except ValueError: continue
            blue_team, red_team, result_str = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]
            blue_bans = [get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6)]; red_bans = [get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6)]
            blue_picks = [get_champion_icon_html(row[idx[f"Blue Pick {i}"]]) for i in range(1, 6)]; red_picks = [get_champion_icon_html(row[idx[f"Red Pick {i}"]]) for i in range(1, 6)]
            all_rows.append({"Date": date_str, "Blue Team": blue_team, "B Bans": " ".join(b for b in blue_bans if b), "B Picks": " ".join(p for p in blue_picks if p), "Result": result_str, "Duration": row[idx["Duration"]], "R Picks": " ".join(p for p in red_picks if p), "R Bans": " ".join(b for b in red_bans if b), "Red Team": red_team, "Match ID": row[idx["Match ID"]]})
            is_our_game, our_picks, our_result_is_win = False, [], False
            if blue_team == TEAM_NAME: is_our_game, our_picks, our_result_is_win = True, [row[idx[f"Blue Pick {i}"]] for i in range(1, 6)], (result_str == "Win")
            elif red_team == TEAM_NAME: is_our_game, our_picks, our_result_is_win = True, [row[idx[f"Red Pick {i}"]] for i in range(1, 6)], (result_str == "Win")
            if is_our_game:
                 for i, champion in enumerate(our_picks):
                     if champion != "N/A" and i < len(roles): role = roles[i]; aggregated_stats[role][champion]['games'] += 1;
                     if our_result_is_win: aggregated_stats[role][champion]['wins'] += 1
        except Exception as e: continue
    df_display = pd.DataFrame(all_rows)
    try: df_display['Date_DT'] = pd.to_datetime(df_display['Date'], errors='coerce'); df_display = df_display.sort_values(by='Date_DT', ascending=False).drop(columns=['Date_DT'])
    except Exception: pass
    return df_display, aggregated_stats


# --- scrims_page (Keep previous version with tabs) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    client = setup_google_sheets();
    if not client: st.error("Failed Google Sheets client init."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Error accessing spreadsheet '{SCRIMS_SHEET_NAME}': {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: st.error(f"Failed worksheet access '{SCRIMS_WORKSHEET_NAME}'."); return

    with st.expander("Update Scrim Data from API", expanded=False):
        debug_logs_scrims = [];
        if 'scrims_debug_logs' not in st.session_state: st.session_state.scrims_debug_logs = []
        if st.button("Download & Update Scrims Data from GRID API", key="update_scrims_btn"):
            st.session_state.scrims_debug_logs = []; debug_logs_scrims = st.session_state.scrims_debug_logs
            with st.spinner("Fetching series list..."): series_list = get_all_series(debug_logs_scrims)
            if series_list:
                st.info(f"Found {len(series_list)} potential series. Processing...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting processing...")
                try: data_added = update_scrims_data(wks, series_list, debug_logs_scrims, progress_bar)
                except Exception as e: st.error(f"Update process error: {e}"); debug_logs_scrims.append(f"FATAL ERROR: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No scrim series found in API.")
        if st.session_state.scrims_debug_logs: st.code("\n".join(st.session_state.scrims_debug_logs), language=None)

    st.divider()
    st.subheader("Scrim Performance Summary")
    time_filter = st.selectbox("Filter Stats by Time Range:", ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")
    df_history_display, aggregated_role_stats = get_scrims_data_for_display(wks, time_filter)

    try:
        blue_stats, red_stats, total_games_agg = aggregate_scrims_data(wks) # Gets overall stats
        st.markdown(f"**Overall Performance (All Time)**")
        col_ov, col_b, col_r = st.columns(3)
        with col_ov:
             total_wins = blue_stats["wins"] + red_stats["wins"]; total_losses = blue_stats["losses"] + red_stats["losses"]
             overall_wr = (total_wins / total_games_agg * 100) if total_games_agg > 0 else 0
             st.metric("Total Games", total_games_agg); st.metric("Overall WR", f"{overall_wr:.1f}%", f"{total_wins}W-{total_losses}L")
        with col_b:
             blue_wr = (blue_stats["wins"] / blue_stats["total"] * 100) if blue_stats["total"] > 0 else 0
             st.metric("Blue Side WR", f"{blue_wr:.1f}%", f"{blue_stats['wins']}W-{blue_stats['losses']}L ({blue_stats['total']} G)")
        with col_r:
             red_wr = (red_stats["wins"] / red_stats["total"] * 100) if red_stats["total"] > 0 else 0
             st.metric("Red Side WR", f"{red_wr:.1f}%", f"{red_stats['wins']}W-{red_stats['losses']}L ({red_stats['total']} G)")
    except Exception as e: st.error(f"Error displaying summary stats: {e}")

    st.divider()
    tab1, tab2 = st.tabs(["📜 Match History", "📊 Champion Stats by Role"])
    with tab1:
        st.subheader(f"Match History ({time_filter})")
        if not df_history_display.empty:
            st.markdown(df_history_display.to_html(escape=False, index=False, classes='compact-table history-table', justify='center'), unsafe_allow_html=True)
        else: st.info(f"No match history for {time_filter}.")
    with tab2:
        st.subheader(f"Champion Stats by Role ({time_filter})")
        st.caption("Note: Roles inferred from pick order (Top > Jg > Mid > Bot > Sup).")
        if not aggregated_role_stats: st.info(f"No champion stats for {time_filter}.")
        else:
             roles_to_display = ["Top", "Jungle", "Mid", "Bot", "Support"]; stat_cols = st.columns(len(roles_to_display))
             for i, role in enumerate(roles_to_display):
                 with stat_cols[i]:
                     st.markdown(f"**{role}**"); role_data = aggregated_role_stats.get(role, {}); stats_list = []
                     for champ, stats_dict in role_data.items():
                         games = stats_dict.get("games", 0)
                         if games > 0: wins = stats_dict.get("wins", 0); win_rate = round((wins / games) * 100, 1) if games > 0 else 0; stats_list.append({'Icon': get_champion_icon_html(champ, 20, 20), 'Champion': champ, 'Games': games, 'WR%': win_rate,})
                     if stats_list:
                         df_role_stats = pd.DataFrame(stats_list).sort_values("Games", ascending=False).reset_index(drop=True)
                         df_role_stats['WR%'] = df_role_stats['WR%'].apply(color_win_rate_scrims)
                         st.markdown(df_role_stats.to_html(escape=False, index=False, classes='compact-table role-stats', justify='center'), unsafe_allow_html=True)
                     else: st.caption("No stats.")

# --- Keep __main__ block as is ---
if __name__ == "__main__":
    st.warning("This page is intended to be run from the main app.py")
    pass
# --- END OF FILE scrims.py ---
