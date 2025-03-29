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

# Настройки
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63")
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed"
SCRIMS_WORKSHEET_NAME = "Scrims"

# --- DDRagon Helper Functions ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        response.raise_for_status(); versions = response.json()
        return versions[0] if versions else "14.14.1"
    except Exception: return "14.14.1"

@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    exceptions = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"}
    if champ in exceptions: return exceptions[champ]
    return "".join(c for c in champ if c.isalnum())

def get_champion_icon_html(champion, width=25, height=25):
    patch_version = get_latest_patch_version() # Get patch version inside function
    normalized_champ = normalize_champion_name_for_ddragon(champion)
    if normalized_champ:
        icon_url = f"https://ddragon.leagueoflegends.com/cdn/{patch_version}/img/champion/{normalized_champ}.png"
        return f'<img src="{icon_url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle; margin: 1px;">'
    return ""

# --- Google Sheets Setup ---
@st.cache_resource
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS not found."); return None
    try:
        creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

# --- Worksheet Check/Creation ---
def check_if_scrims_worksheet_exists(spreadsheet, name):
    try: wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26)
            header = ["Date","Match ID","Blue Team","Red Team","Blue Ban 1","Blue Ban 2","Blue Ban 3","Blue Ban 4","Blue Ban 5","Red Ban 1","Red Ban 2","Red Ban 3","Red Ban 4","Red Ban 5","Blue Pick 1","Blue Pick 2","Blue Pick 3","Blue Pick 4","Blue Pick 5","Red Pick 1","Red Pick 2","Red Pick 3","Red Pick 4","Red Pick 5","Duration","Result"]
            wks.append_row(header, value_input_option='USER_ENTERED')
        except Exception as e: st.error(f"Error creating worksheet '{name}': {e}"); return None
    except Exception as e: st.error(f"Error accessing worksheet '{name}': {e}"); return None
    return wks

# --- GRID API Functions (Keep as is) ---
@st.cache_data(ttl=300)
def get_all_series(_debug_placeholder):
    headers = {"x-api-key": GRID_API_KEY, "Content-Type": "application/json"}
    query = """query ($filter: SeriesFilter, $first: Int, $after: Cursor, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) { allSeries( filter: $filter, first: $first, after: $after, orderBy: $orderBy, orderDirection: $orderDirection ) { totalCount, pageInfo { hasNextPage, endCursor }, edges { node { id, startTimeScheduled } } } }"""
    start_date_threshold = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    variables = {"filter": {"titleId": 3, "types": ["SCRIM"], "startTimeScheduled": {"gte": start_date_threshold}}, "first": 50, "orderBy": "StartTimeScheduled", "orderDirection": "DESC"}
    all_series_nodes, has_next_page, after_cursor, page_number, max_pages = [], True, None, 1, 20
    while has_next_page and page_number <= max_pages:
        current_variables = variables.copy();
        if after_cursor: current_variables["after"] = after_cursor
        try:
            response = requests.post(f"{GRID_BASE_URL}central-data/graphql", headers=headers, json={"query": query, "variables": current_variables}, timeout=20)
            response.raise_for_status(); data = response.json()
            if "errors" in data: st.error(f"GraphQL Error: {data['errors']}"); break
            all_series_data = data.get("data", {}).get("allSeries", {}); series_edges = all_series_data.get("edges", [])
            all_series_nodes.extend([s["node"] for s in series_edges if "node" in s])
            page_info = all_series_data.get("pageInfo", {}); has_next_page = page_info.get("hasNextPage", False); after_cursor = page_info.get("endCursor")
            page_number += 1; time.sleep(0.2)
        except Exception as e: st.error(f"Error fetching series page {page_number}: {e}"); return []
    return all_series_nodes

def download_series_data(series_id, debug_logs, max_retries=3, initial_delay=2):
    headers = {"x-api-key": GRID_API_KEY}; url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15);
            if response.status_code == 200:
                try: return response.json()
                except json.JSONDecodeError: debug_logs.append(f"Err: JSON S {series_id}"); return None
            elif response.status_code == 429: delay = initial_delay * (2 ** attempt); debug_logs.append(f"Warn: 429 S {series_id}. Wait {delay}s"); st.toast(f"Wait {delay}s..."); time.sleep(delay); continue
            elif response.status_code == 404: return None # Don't log 404 as error
            else: debug_logs.append(f"Err: S {series_id} Status {response.status_code}"); response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1: time.sleep(initial_delay * (2 ** attempt))
            else: st.error(f"Network error S {series_id}: {e}"); return None
    return None

def download_game_data(game_id, debug_logs, max_retries=3, initial_delay=2):
    headers = {"x-api-key": GRID_API_KEY}; url = f"https://api.grid.gg/file-download/end-state/grid/game/{game_id}"
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15);
            if response.status_code == 200:
                 try: return response.json()
                 except json.JSONDecodeError: debug_logs.append(f"Err: JSON G {game_id}"); return None
            elif response.status_code == 429: delay = initial_delay * (2 ** attempt); debug_logs.append(f"Warn: 429 G {game_id}. Wait {delay}s"); st.toast(f"Wait {delay}s..."); time.sleep(delay); continue
            elif response.status_code == 404: return None
            else: debug_logs.append(f"Err: G {game_id} Status {response.status_code}"); response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1: time.sleep(initial_delay * (2 ** attempt))
            else: st.error(f"Network error G {game_id}: {e}"); return None
    return None


# --- update_scrims_data (Keep reverted version without strict pick check) ---
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not worksheet: st.error("Invalid Sheet."); return False
    if not series_list: st.info("No series found."); return False
    try: existing_data = worksheet.get_all_values(); existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    except Exception as e: st.error(f"Read error: {e}"); return False
    new_rows, gamespace_series_count, skipped_duplicates, processed_count = [], 0, 0, 0
    api_request_delay, total_series_to_process = 1.0, len(series_list)
    for i, series_summary in enumerate(series_list):
        series_id = series_summary.get("id");
        if not series_id: continue
        progress = (i + 1) / total_series_to_process;
        try: progress_bar.progress(progress, text=f"Processing {i+1}/{total_series_to_process}")
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
        duration_formatted = "N/A"
        if isinstance(duration_seconds, (int, float)) and duration_seconds >= 0:
            try: duration_formatted = f"{int(duration_seconds // 60)}:{int(duration_seconds % 60):02d}"
            except Exception: pass
        result = "N/A"; t0_won, t1_won = team_0.get("won"), team_1.get("won")
        if t0_won is True: result = "Win" if team_0_name == TEAM_NAME else "Loss"
        elif t1_won is True: result = "Win" if team_1_name == TEAM_NAME else "Loss"
        elif t0_won is False and t1_won is False and team_0.get("outcome") == "tie": result = "Tie"
        new_row = [date_formatted, match_id, blue_team, red_team, *blue_bans, *red_bans, *blue_picks, *red_picks, duration_formatted, result]
        if len(new_row) != 26: continue
        new_rows.append(new_row); existing_match_ids.add(match_id); processed_count += 1
    progress_bar.progress(1.0, text="Updating sheet...")
    summary_log = [f"\n--- Summary ---", f"Checked: {total_series_to_process}", f"{TEAM_NAME}: {gamespace_series_count}", f"Dupes: {skipped_duplicates}", f"Processed: {processed_count}", f"New: {len(new_rows)}" ]
    debug_logs.extend(summary_log)
    if new_rows:
        try:
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED'); st.success(f"Added {len(new_rows)} scrims.")
            # aggregate_scrims_data.clear() # Clear cache only if caching is re-enabled later
            return True
        except Exception as e: st.error(f"Append error: {str(e)}"); return False
    else: st.info("No new scrims."); return False

# --- aggregate_scrims_data (REVERTED: No Caching + Creates History DF) ---
# !! REMOVED @st.cache_data decorator !!
def aggregate_scrims_data(worksheet, time_filter="All Time"):
    if not worksheet:
        st.error("[AGG_DEBUG] Invalid worksheet provided.")
        return {}, {}, pd.DataFrame()

    st.info(f"[AGG_DEBUG] Starting aggregation for filter: {time_filter}") # Debug Start

    blue_stats, red_stats, history_rows, expected_cols = {"wins":0,"losses":0,"total":0}, {"wins":0,"losses":0,"total":0}, [], 26
    now, time_threshold = datetime.utcnow(), None
    if time_filter == "1 Week": time_threshold = now - timedelta(weeks=1)
    elif time_filter == "2 Weeks": time_threshold = now - timedelta(weeks=2)
    elif time_filter == "3 Weeks": time_threshold = now - timedelta(weeks=3)
    elif time_filter == "4 Weeks": time_threshold = now - timedelta(weeks=4)
    elif time_filter == "2 Months": time_threshold = now - timedelta(days=60)

    try:
        data = worksheet.get_all_values()
        st.info(f"[AGG_DEBUG] Read {len(data)} rows from worksheet.") # Debug Row Count
    except Exception as e:
        st.error(f"[AGG_DEBUG] Read error agg: {e}")
        return blue_stats, red_stats, pd.DataFrame()

    if len(data) <= 1:
        st.warning("[AGG_DEBUG] Worksheet is empty or contains only header.")
        return blue_stats, red_stats, pd.DataFrame()

    header = data[0]
    st.info(f"[AGG_DEBUG] Header found: {header}") # Debug Header

    try:
        # Define required columns for minimal functionality
        required_cols = ["Date", "Match ID", "Blue Team", "Red Team", "Duration", "Result"]
        # Define columns needed for icons
        icon_cols = [f"{side} {act} {i}" for side in ["Blue", "Red"] for act in ["Ban", "Pick"] for i in range(1, 6)]
        all_expected_cols = required_cols + icon_cols[0:10] + icon_cols[10:20] # Ensure all 26 are checked if needed by indices below

        # Get indices, raise error if any required column is missing
        idx = {name: header.index(name) for name in all_expected_cols}
        st.success("[AGG_DEBUG] All required header columns found.") # Debug Header Success
    except ValueError as e:
        st.error(f"[AGG_DEBUG] Header error agg: Column not found - {e}.")
        return blue_stats, red_stats, pd.DataFrame()

    processed_rows = 0
    skipped_incomplete = 0
    skipped_filtered = 0
    skipped_other_error = 0

    for i, row in enumerate(data[1:], start=2): # Start counting from row 2
        if len(row) < expected_cols:
            skipped_incomplete += 1
            continue # Skip incomplete rows
        try:
            date_str = row[idx["Date"]]
            # Apply time filter
            is_filtered_out = False
            if time_threshold and date_str != "N/A":
                try:
                    match_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if match_date < time_threshold:
                        is_filtered_out = True
                        skipped_filtered += 1
                        continue # Skip filtered rows
                except ValueError:
                    # Treat unparseable dates as potentially included unless strict filtering is needed
                    pass # Or: is_filtered_out = True; skipped_filtered += 1; continue

            # If not filtered out by time, proceed
            blue_team, red_team, result = row[idx["Blue Team"]], row[idx["Red Team"]], row[idx["Result"]]

            # Calculate side stats (only for TEAM_NAME)
            is_our, is_blue = False, False
            if blue_team == TEAM_NAME: is_our, is_blue = True, True
            elif red_team == TEAM_NAME: is_our, is_blue = True, False

            if is_our:
                win = (result == "Win")
                if is_blue:
                    blue_stats["total"]+=1
                    if win: blue_stats["wins"]+=1
                    elif result=="Loss": blue_stats["losses"]+=1
                else:
                    red_stats["total"]+=1
                    if win: red_stats["wins"]+=1
                    elif result=="Loss": red_stats["losses"]+=1

            # Prepare row for history display (always add if not filtered by time)
            bb_html=" ".join(get_champion_icon_html(row[idx[f"Blue Ban {i}"]]) for i in range(1, 6) if idx.get(f"Blue Ban {i}") is not None and row[idx[f"Blue Ban {i}"]] != "N/A")
            rb_html=" ".join(get_champion_icon_html(row[idx[f"Red Ban {i}"]]) for i in range(1, 6) if idx.get(f"Red Ban {i}") is not None and row[idx[f"Red Ban {i}"]] != "N/A")
            bp_html=" ".join(get_champion_icon_html(row[idx[f"Blue Pick {i}"]]) for i in range(1, 6) if idx.get(f"Blue Pick {i}") is not None and row[idx[f"Blue Pick {i}"]] != "N/A")
            rp_html=" ".join(get_champion_icon_html(row[idx[f"Red Pick {i}"]]) for i in range(1, 6) if idx.get(f"Red Pick {i}") is not None and row[idx[f"Red Pick {i}"]] != "N/A")

            history_rows.append({
                "Date":date_str,
                "Blue Team":blue_team,
                "B Bans":bb_html,
                "B Picks":bp_html,
                "Result":result,
                "Duration":row[idx["Duration"]],
                "R Picks":rp_html,
                "R Bans":rb_html,
                "Red Team":red_team,
                "Match ID":row[idx["Match ID"]]
            })
            processed_rows += 1

        except IndexError as e_idx:
            st.warning(f"[AGG_DEBUG] IndexError on row {i}: {e_idx}. Row data (partial): {row[:5]}...")
            skipped_other_error += 1
            continue
        except Exception as e_proc:
            st.warning(f"[AGG_DEBUG] Error processing row {i}: {e_proc}. Row data (partial): {row[:5]}...")
            skipped_other_error += 1
            continue

    st.info(f"[AGG_DEBUG] Processing Summary: Total Data Rows={len(data)-1}, Processed for History={processed_rows}, Skipped (Incomplete Col)={skipped_incomplete}, Skipped (Time Filter)={skipped_filtered}, Skipped (Other Error)={skipped_other_error}")

    df_history = pd.DataFrame(history_rows)
    # Sort history (most recent first)
    try:
        if not df_history.empty and 'Date' in df_history.columns:
            df_history['Date_DT'] = pd.to_datetime(df_history['Date'], errors='coerce')
            # Handle NaT values if any before sorting
            df_history_sorted = df_history.sort_values(by='Date_DT', ascending=False, na_position='last').drop(columns=['Date_DT'])
            st.info(f"[AGG_DEBUG] Successfully created and sorted DataFrame with {len(df_history_sorted)} rows.") # Debug DF creation
            return blue_stats, red_stats, df_history_sorted
        else:
             st.warning("[AGG_DEBUG] History DataFrame is empty or Date column missing after processing.")
             return blue_stats, red_stats, df_history # Return potentially unsorted empty/partial DF
    except Exception as e_sort:
        st.error(f"[AGG_DEBUG] Error during DataFrame sorting: {e_sort}")
        return blue_stats, red_stats, df_history

# --- scrims_page (Keep reverted version) ---
def scrims_page():
    st.title(f"Scrims Analysis - {TEAM_NAME}")
    client = setup_google_sheets();
    if not client: st.error("GSheets client failed."); return
    try: spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except Exception as e: st.error(f"Sheet access error: {e}"); return
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME);
    if not wks: st.error(f"Worksheet access error."); return

    with st.expander("Update Scrim Data", expanded=False):
        debug_logs_scrims = [];
        if 'scrims_debug_logs' not in st.session_state: st.session_state.scrims_debug_logs = []
        if st.button("Download & Update from GRID API", key="update_scrims_btn"):
            st.session_state.scrims_debug_logs = []; debug_logs_scrims = st.session_state.scrims_debug_logs
            with st.spinner("Fetching series..."): series_list = get_all_series(debug_logs_scrims)
            if series_list:
                st.info(f"Processing {len(series_list)} series...")
                progress_bar_placeholder = st.empty(); progress_bar = progress_bar_placeholder.progress(0, text="Starting...")
                try: data_added = update_scrims_data(wks, series_list, debug_logs_scrims, progress_bar)
                except Exception as e: st.error(f"Update error: {e}")
                finally: progress_bar_placeholder.empty()
            else: st.warning("No series found.")
        # if st.session_state.scrims_debug_logs: st.code("\n".join(st.session_state.scrims_debug_logs), language=None) # Keep logs minimal

    st.divider()
    st.subheader("Scrim Performance")
    time_filter = st.selectbox("Filter by Time:", ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"], key="scrims_time_filter")

    # --- Call reverted aggregate function ---
    blue_stats, red_stats, df_history = aggregate_scrims_data(wks, time_filter) # Removed caching here

    # --- Display Summary Win Rates ---
    try:
        total_games_f = blue_stats["total"] + red_stats["total"]; total_wins_f = blue_stats["wins"] + red_stats["wins"]; total_losses_f = blue_stats["losses"] + red_stats["losses"]
        st.markdown(f"**Performance ({time_filter})**")
        col_ov, col_b, col_r = st.columns(3)
        with col_ov: overall_wr = (total_wins_f / total_games_f * 100) if total_games_f > 0 else 0; st.metric("Total Games", total_games_f); st.metric("Overall WR", f"{overall_wr:.1f}%", f"{total_wins_f}W-{total_losses_f}L")
        with col_b: blue_wr = (blue_stats["wins"] / blue_stats["total"] * 100) if blue_stats["total"] > 0 else 0; st.metric("Blue WR", f"{blue_wr:.1f}%", f"{blue_stats['wins']}W-{blue_stats['losses']}L ({blue_stats['total']} G)")
        with col_r: red_wr = (red_stats["wins"] / red_stats["total"] * 100) if red_stats["total"] > 0 else 0; st.metric("Red WR", f"{red_wr:.1f}%", f"{red_stats['wins']}W-{red_stats['losses']}L ({red_stats['total']} G)")
    except Exception as e: st.error(f"Error display summary: {e}")

    st.divider()
    # --- Display Match History Table ---
    st.subheader(f"Match History ({time_filter})")
    if not df_history.empty:
        st.markdown(df_history.to_html(escape=False, index=False, classes='compact-table history-table', justify='center'), unsafe_allow_html=True)
    else: st.info(f"No match history for {time_filter}.")

# --- Keep __main__ block as is ---
if __name__ == "__main__": pass
# --- END OF FILE scrims.py ---
