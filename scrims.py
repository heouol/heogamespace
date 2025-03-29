# --- START OF FILE scrims.py ---

import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta
import time  # Для добавления задержек

# Настройки (Keep these specific to scrims)
GRID_API_KEY = os.getenv("GRID_API_KEY", "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63") # Use env var or secrets
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
# TOURNAMENT_NAME = "League of Legends Scrims" # Not used currently
SCRIMS_SHEET_NAME = "Scrims_GMS_Detailed" # Use a specific name for the scrims sheet
SCRIMS_WORKSHEET_NAME = "Scrims" # Name of the worksheet within the sheet

# --- ADD THIS FUNCTION DEFINITION BACK ---
# Настройка Google Sheets (Specific for Scrims context)
# Use @st.cache_resource if client should persist across reruns within scrims page
@st.cache_resource
def setup_google_sheets():
    """Sets up and authorizes the Google Sheets client using credentials."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS")
    if not json_creds_str:
        st.error("Google Sheets credentials (GOOGLE_SHEETS_CREDS) not found in environment variables/secrets for Scrims.")
        return None
    try:
        creds_dict = json.loads(json_creds_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        # Test connection
        client.list_spreadsheet_files()
        st.info("Google Sheets client authorized successfully for Scrims.") # Optional confirmation
        return client
    except json.JSONDecodeError:
        st.error("Error decoding Google Sheets JSON credentials for Scrims. Check the format.")
        return None
    except gspread.exceptions.APIError as e:
         st.error(f"Google Sheets API Error during Scrims setup: {e}. Check credentials/permissions.")
         return None
    except Exception as e:
        st.error(f"Unexpected error setting up Google Sheets for Scrims: {e}")
        return None
# --- END OF ADDED FUNCTION ---


# Make sure check_if_worksheets_exists creates enough columns (26)
def check_if_scrims_worksheet_exists(spreadsheet, name):
    """Checks for worksheet existence, creates with header if not found."""
    try:
        wks = spreadsheet.worksheet(name)
        # Optionally check header if needed
        # header = wks.row_values(1) ...
    except gspread.exceptions.WorksheetNotFound:
        try:
            st.info(f"Worksheet '{name}' not found in '{spreadsheet.title}', creating...")
            # Updated column count to 26
            wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26)
            # Add header row immediately upon creation
            header_row = [
                "Date", "Match ID", "Blue Team", "Red Team",
                "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
                "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
                "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5",
                "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5",
                "Duration", "Result"
            ]
            wks.append_row(header_row, value_input_option='USER_ENTERED')
            st.info(f"Worksheet '{name}' created with header.")
        except gspread.exceptions.APIError as e:
            st.error(f"API Error creating worksheet '{name}': {e}")
            return None
    except gspread.exceptions.APIError as e:
        st.error(f"API Error checking/accessing worksheet '{name}': {e}")
        return None
    return wks


# Функция для получения списка всех серий через GraphQL с пагинацией
# Use @st.cache_data for caching API results temporarily
@st.cache_data(ttl=300) # Cache for 5 minutes
def get_all_series(_debug_placeholder): # Pass a dummy arg if logs aren't needed in cached func
    """Fetches all scrim series IDs using GraphQL pagination."""
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
                    # Optional: Request teams here if needed early, but might slow down query
                    # teams { id name }
                }
            }
        }
    }
    """
    # Устанавливаем дату начала поиска (например, последние 6 месяцев)
    # Consider making the lookback period configurable
    lookback_days = 180
    start_date_threshold = (datetime.utcnow() - timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    variables = {
        "filter": {
            "titleId": 3,  # LoL
            "types": ["SCRIM"],  # Use list for types filter
            "startTimeScheduled": {
                "gte": start_date_threshold
            }
        },
        "first": 50,  # Max page size often 50 or 100
        "orderBy": "StartTimeScheduled",
        "orderDirection": "DESC" # Get most recent first
    }

    all_series_nodes = []
    has_next_page = True
    after_cursor = None
    page_number = 1
    max_pages = 20 # Safety break to prevent infinite loops

    while has_next_page and page_number <= max_pages:
        current_variables = variables.copy()
        if after_cursor:
            current_variables["after"] = after_cursor

        try:
            response = requests.post(
                f"{GRID_BASE_URL}central-data/graphql", # Ensure this endpoint is correct
                headers=headers,
                json={"query": query, "variables": current_variables},
                timeout=20 # Add timeout
            )
            response.raise_for_status() # Raise HTTP errors

            data = response.json()
            if "errors" in data:
                 internal_logs.append(f"GraphQL Error (Page {page_number}): {data['errors']}")
                 st.error(f"GraphQL Error: {data['errors']}") # Show error in UI
                 break # Stop pagination on error

            all_series_data = data.get("data", {}).get("allSeries", {})
            series_edges = all_series_data.get("edges", [])
            all_series_nodes.extend([s["node"] for s in series_edges if "node" in s])

            page_info = all_series_data.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            after_cursor = page_info.get("endCursor")
            internal_logs.append(f"GraphQL Page {page_number}: Fetched {len(series_edges)} series. HasNext: {has_next_page}")

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

    if page_number > max_pages:
         st.warning(f"Reached maximum page limit ({max_pages}) for fetching series.")

    internal_logs.append(f"Total series nodes fetched: {len(all_series_nodes)}")
    # print("\n".join(internal_logs)) # Print logs for debugging if needed
    return all_series_nodes


# Функция для загрузки данных серии (GRID-формат) с обработкой 429 и 404
# Use caching? Maybe not, as data might update.
def download_series_data(series_id, debug_logs, max_retries=3, initial_delay=2):
    """Downloads end-state data for a specific series ID."""
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}" # Verify endpoint

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            debug_logs.append(f"Series {series_id} Request: GET {url} -> Status {response.status_code}")

            if response.status_code == 200:
                try:
                    return response.json()
                except json.JSONDecodeError:
                     debug_logs.append(f"Error: Could not decode JSON for Series {series_id}. Content: {response.text[:200]}")
                     st.warning(f"Invalid JSON received for Series {series_id}")
                     return None
            elif response.status_code == 429:  # Too Many Requests
                delay = initial_delay * (2 ** attempt)
                debug_logs.append(f"Warning: Received 429 for Series {series_id}. Waiting {delay}s (Attempt {attempt+1}/{max_retries})")
                st.toast(f"Rate limit hit, waiting {delay}s...")
                time.sleep(delay)
                continue # Retry after delay
            elif response.status_code == 404:  # Not Found
                debug_logs.append(f"Info: Series {series_id} not found (404). Skipping.")
                return None # Don't retry on 404
            else:
                # Log other non-successful status codes
                debug_logs.append(f"Error: API request for Series {series_id} failed. Status: {response.status_code}, Response: {response.text[:200]}")
                response.raise_for_status() # Raise for other bad statuses (like 401, 403, 5xx) after logging

        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network/Request Exception for Series {series_id} (Attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                 delay = initial_delay * (2 ** attempt)
                 time.sleep(delay) # Wait before retrying network errors
            else:
                 st.error(f"Network error fetching series {series_id} after {max_retries} attempts: {e}")
                 return None # Failed after retries

    debug_logs.append(f"Error: Failed to download data for Series {series_id} after {max_retries} attempts.")
    return None


# Функция для загрузки данных игры (если есть game_id)
def download_game_data(game_id, debug_logs, max_retries=3, initial_delay=2):
    """Downloads end-state data for a specific game ID."""
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/game/{game_id}" # Verify endpoint

    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=15)
            debug_logs.append(f"Game {game_id} Request: GET {url} -> Status {response.status_code}")

            if response.status_code == 200:
                 try:
                    return response.json()
                 except json.JSONDecodeError:
                     debug_logs.append(f"Error: Could not decode JSON for Game {game_id}. Content: {response.text[:200]}")
                     st.warning(f"Invalid JSON received for Game {game_id}")
                     return None
            elif response.status_code == 429:
                delay = initial_delay * (2 ** attempt)
                debug_logs.append(f"Warning: Received 429 for Game {game_id}. Waiting {delay}s (Attempt {attempt+1}/{max_retries})")
                st.toast(f"Rate limit hit, waiting {delay}s...")
                time.sleep(delay)
                continue
            elif response.status_code == 404:
                debug_logs.append(f"Info: Game {game_id} not found (404). Skipping.")
                return None
            else:
                 debug_logs.append(f"Error: API request for Game {game_id} failed. Status: {response.status_code}, Response: {response.text[:200]}")
                 response.raise_for_status()

        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Error: Network/Request Exception for Game {game_id} (Attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                 delay = initial_delay * (2 ** attempt)
                 time.sleep(delay)
            else:
                 st.error(f"Network error fetching game {game_id} after {max_retries} attempts: {e}")
                 return None

    debug_logs.append(f"Error: Failed to download data for Game {game_id} after {max_retries} attempts.")
    return None


# Функция для обновления данных в Google Sheets
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not worksheet:
        debug_logs.append("Error: Invalid worksheet provided to update_scrims_data.")
        st.error("Cannot update scrims data: Invalid Google Sheet worksheet.")
        return False
    if not series_list:
        debug_logs.append("Info: Series list is empty. Nothing to update.")
        st.info("No series found to process.")
        return False # Nothing to add, but not necessarily an error

    try:
        # Fetch existing data efficiently (only match IDs)
        existing_data = worksheet.get_all_values() # Get all data once
        # Assuming Match ID is in the second column (index 1)
        existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
        debug_logs.append(f"Found {len(existing_match_ids)} existing Match IDs in the sheet.")
    except gspread.exceptions.APIError as e:
        debug_logs.append(f"Error: API Error fetching existing data from sheet '{worksheet.title}': {e}")
        st.error(f"Could not read existing data from Google Sheet: {e}")
        return False # Cannot proceed safely without knowing existing IDs
    except Exception as e:
        debug_logs.append(f"Error: Unexpected error fetching existing data: {e}")
        st.error(f"Unexpected error reading existing data: {e}")
        return False


    new_rows = []
    gamespace_series_count = 0
    skipped_duplicates = 0
    processed_count = 0
    api_request_delay = 1.0 # Delay between downloading data for each series

    total_series_to_process = len(series_list)
    progress_text_template = "Processing series {current}/{total} (ID: {series_id})"

    for i, series_summary in enumerate(series_list):
        series_id = series_summary.get("id")
        if not series_id:
            debug_logs.append(f"Warning: Skipping entry {i+1} due to missing series ID: {series_summary}")
            continue

        # --- Progress Bar Update ---
        progress = (i + 1) / total_series_to_process
        progress_text = progress_text_template.format(current=i+1, total=total_series_to_process, series_id=series_id)
        try:
            progress_bar.progress(progress, text=progress_text)
        except Exception as pb_e: # Catch potential errors if progress bar object becomes invalid
             debug_logs.append(f"Warning: Could not update progress bar: {pb_e}")
             # Continue processing without progress bar update

        # --- API Delay ---
        if i > 0: time.sleep(api_request_delay)

        # debug_logs.append(f"\n--- Processing Series {series_id} ---")
        scrim_data = download_series_data(series_id, debug_logs=debug_logs)
        if not scrim_data:
            # debug_logs.append(f"Info: Failed to download data for Series {series_id} or it was skipped.")
            continue # Skip to next series if data download failed

        # --- Check Team Participation ---
        teams = scrim_data.get("teams")
        if not teams or not isinstance(teams, list) or len(teams) < 2:
            debug_logs.append(f"Warning: Could not find 2 valid teams for Series {series_id}. Skipping. Data: {teams}")
            continue

        team_0 = teams[0]
        team_1 = teams[1]
        team_0_name = team_0.get("name", "Unknown_Team_0")
        team_1_name = team_1.get("name", "Unknown_Team_1")

        if TEAM_NAME not in [team_0_name, team_1_name]:
            # debug_logs.append(f"Info: Series {series_id} ({team_0_name} vs {team_1_name}) does not involve {TEAM_NAME}. Skipping.")
            continue # Skip if our team didn't play

        gamespace_series_count += 1
        # debug_logs.append(f"Info: Found series for {TEAM_NAME} (Series {series_id}): {team_0_name} vs {team_1_name}")

        # --- Check for Duplicates ---
        match_id = str(scrim_data.get("matchId", series_id)) # Use series_id as fallback
        if match_id in existing_match_ids:
            # debug_logs.append(f"Info: Series {series_id} already exists in sheet (Match ID: {match_id}). Skipping.")
            skipped_duplicates += 1
            continue # Skip duplicate entries

        # --- Extract Date ---
        date_str = scrim_data.get("startTime", series_summary.get("startTimeScheduled", scrim_data.get("updatedAt")))
        date_formatted = "N/A"
        if date_str and isinstance(date_str, str):
            # Try multiple common ISO formats
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    # Format without timezone info for consistency, assuming UTC input
                    date_formatted = date_obj.strftime("%Y-%m-%d %H:%M:%S")
                    break
                except ValueError:
                    continue
            if date_formatted == "N/A":
                 debug_logs.append(f"Warning: Could not parse date string '{date_str}' for series {series_id}")
        else:
             debug_logs.append(f"Warning: Date string not found or invalid for series {series_id}")

        # --- Assign Teams to Sides ---
        # GRID convention: Team 0 is usually Blue, Team 1 is Red
        blue_team = team_0_name
        red_team = team_1_name
        # blue_team_id = team_0.get("id") # Store IDs if needed for matching later
        # red_team_id = team_1.get("id")

        # --- Get Game Details (ID and Data) ---
        game_id = None
        game_data = None

        # Find game ID within scrim_data structure
        # It might be in scrim_data['games'][0]['id'] or scrim_data['object']['games'][0]['id']
        potential_games_list = scrim_data.get("games")
        if not potential_games_list and isinstance(scrim_data.get("object"), dict):
             potential_games_list = scrim_data["object"].get("games")

        if isinstance(potential_games_list, list) and potential_games_list:
            first_game_info = potential_games_list[0]
            if isinstance(first_game_info, dict):
                game_id = first_game_info.get("id")
            elif isinstance(first_game_info, str): # Sometimes just a list of IDs
                game_id = first_game_info

        # Fetch detailed game data if ID was found
        if game_id:
            # debug_logs.append(f"Info: Found game_id {game_id} for series {series_id}. Fetching details...")
            time.sleep(0.5) # Small delay before game data request
            game_data = download_game_data(game_id, debug_logs=debug_logs)
            # if game_data:
            #     debug_logs.append(f"Info: Successfully fetched game_data for {game_id}.")
            # else:
            #     debug_logs.append(f"Warning: Failed to fetch game_data for {game_id}. Will rely on series data.")
        # else:
        #     debug_logs.append(f"Info: game_id not found within series data for {series_id}.")

        # --- Extract Draft & Duration (Prioritize Game Data) ---
        draft_actions = []
        duration_seconds = None

        if game_data: # Prioritize detailed game data
            draft_actions = game_data.get("draftActions", [])
            clock_data = game_data.get("clock", {})
            # Prefer clock's currentSeconds, fallback to game's duration field
            duration_seconds = clock_data.get("currentSeconds", game_data.get("duration"))
            # debug_logs.append(f"Info: Using draft/duration from fetched game_data for game {game_id}.")
        else: # Fallback to series data (less reliable)
            # debug_logs.append(f"Info: Falling back to searching within series data for draft/duration (Series {series_id}).")
            if isinstance(potential_games_list, list) and potential_games_list and isinstance(potential_games_list[0], dict):
                 game_data_from_scrim = potential_games_list[0]
                 draft_actions = game_data_from_scrim.get("draftActions", [])
                 clock_data = game_data_from_scrim.get("clock", {})
                 duration_seconds = clock_data.get("currentSeconds", game_data_from_scrim.get("duration"))
            # If still no duration, check top-level series data
            if duration_seconds is None:
                 duration_seconds = scrim_data.get("duration")
                 # if duration_seconds is not None:
                 #     debug_logs.append("Info: Using duration from top-level series data.")

        # --- Process Draft Actions ---
        blue_bans = ["N/A"] * 5
        red_bans = ["N/A"] * 5
        blue_picks = ["N/A"] * 5
        red_picks = ["N/A"] * 5

        if not draft_actions:
             # debug_logs.append(f"Info: No draft actions found for series {series_id}.")
             pass # Keep picks/bans as N/A
        else:
            try:
                 # Sort by sequence number just in case
                 draft_actions.sort(key=lambda x: int(x.get("sequenceNumber", 99)))
            except (ValueError, TypeError):
                 debug_logs.append(f"Warning: Could not sort draft actions for series {series_id} due to invalid sequence numbers.")
                 # Proceed with unsorted data, might be incorrect

            blue_ban_idx, red_ban_idx, blue_pick_idx, red_pick_idx = 0, 0, 0, 0
            processed_sequences = set()

            for action in draft_actions:
                try:
                    # Ensure sequence is treated as int for reliable comparison
                    sequence = int(action.get("sequenceNumber", -1))
                    if sequence in processed_sequences or sequence == -1:
                         # debug_logs.append(f"Warning: Duplicate or invalid sequence number {sequence} in draft for {series_id}. Skipping action.")
                         continue # Skip duplicate or invalid sequences
                    processed_sequences.add(sequence)

                    action_type = action.get("type")
                    champion = action.get("draftable", {}).get("name", "N/A")

                    # Assign based on standard competitive draft sequence
                    if action_type == "ban":
                        if sequence in [1, 3, 5, 14, 16]: # Blue Bans
                            if blue_ban_idx < 5: blue_bans[blue_ban_idx] = champion; blue_ban_idx += 1
                        elif sequence in [2, 4, 6, 13, 15]: # Red Bans
                            if red_ban_idx < 5: red_bans[red_ban_idx] = champion; red_ban_idx += 1
                    elif action_type == "pick":
                        if sequence in [7, 10, 11, 18, 19]: # Blue Picks
                            if blue_pick_idx < 5: blue_picks[blue_pick_idx] = champion; blue_pick_idx += 1
                        elif sequence in [8, 9, 12, 17, 20]: # Red Picks
                            if red_pick_idx < 5: red_picks[red_pick_idx] = champion; red_pick_idx += 1

                except (ValueError, TypeError, KeyError) as e:
                     debug_logs.append(f"Warning: Error processing draft action for {series_id}. Action: {action}, Error: {e}")
                     continue # Skip problematic action

            # Log if not all picks/bans were filled
            # if blue_ban_idx < 5 or red_ban_idx < 5 or blue_pick_idx < 5 or red_pick_idx < 5:
            #     debug_logs.append(f"Warning: Incomplete draft processing for {series_id}. B:{blue_ban_idx}/5, {red_ban_idx}/5, P:{blue_pick_idx}/5, {red_pick_idx}/5")


        # --- Format Duration ---
        duration_formatted = "N/A"
        if isinstance(duration_seconds, (int, float)) and duration_seconds >= 0:
            try:
                minutes = int(duration_seconds // 60)
                seconds = int(duration_seconds % 60)
                duration_formatted = f"{minutes}:{seconds:02d}" # Format MM:SS
            except Exception as e:
                debug_logs.append(f"Warning: Error formatting duration {duration_seconds} for series {series_id}: {e}")
        # else: debug_logs.append(f"Info: Duration value unavailable or invalid for series {series_id}: {duration_seconds}")


        # --- Determine Result ---
        result = "N/A"
        # Check win status from team objects
        team_0_won = team_0.get("won") # Can be True, False, or None
        team_1_won = team_1.get("won")

        if team_0_won is True:
            result = "Win" if team_0_name == TEAM_NAME else "Loss"
        elif team_1_won is True:
             result = "Win" if team_1_name == TEAM_NAME else "Loss"
        elif team_0_won is False and team_1_won is False and team_0.get("outcome") == "tie":
             result = "Tie" # Handle ties if possible
        elif team_0_won is None or team_1_won is None:
             # Could try inferring from scores if available, but risky
             debug_logs.append(f"Warning: Win status missing or ambiguous for series {series_id}. Result set to N/A.")
        # If one team won and the other didn't lose (e.g., win=True, won=None), assume the win is correct
        elif team_0_won is True and team_1_won is not False:
             result = "Win" if team_0_name == TEAM_NAME else "Loss"
        elif team_1_won is True and team_0_won is not False:
             result = "Win" if team_1_name == TEAM_NAME else "Loss"


        # --- Assemble Row ---
        # Ensure order matches header: Date, MatchID, Blue, Red, 5xBBan, 5xRBan, 5xBPick, 5xRPick, Duration, Result
        new_row = [
            date_formatted, match_id, blue_team, red_team,
            *blue_bans, *red_bans, *blue_picks, *red_picks,
            duration_formatted, result
        ]

        # Validate row length
        expected_columns = 26
        if len(new_row) != expected_columns:
             debug_logs.append(f"Error: Generated row for Series {series_id} has {len(new_row)} columns, expected {expected_columns}. Row: {new_row}")
             st.error(f"Error: Generated row for Series {series_id} has {len(new_row)} columns, expected {expected_columns}.")
             continue # Skip adding malformed row

        new_rows.append(new_row)
        existing_match_ids.add(match_id) # Add to set to prevent adding duplicates within the same run
        processed_count += 1
        # --- End of loop for one series ---

    # --- Final Summary and Sheet Update ---
    progress_bar.progress(1.0, text="Processing complete. Updating sheet...")

    summary_log = [
        f"\n--- Scrims Update Summary ---",
        f"Total series fetched initially: {total_series_to_process}",
        f"Series involving {TEAM_NAME}: {gamespace_series_count}",
        f"Skipped duplicate Match IDs: {skipped_duplicates}",
        f"Successfully processed and ready to add: {processed_count}",
        f"New rows generated: {len(new_rows)}"
    ]
    debug_logs.extend(summary_log)
    # st.write("\n".join(summary_log)) # Display summary in Streamlit UI


    if new_rows:
        try:
            # Append all new rows at once
            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            debug_logs.append(f"Success: Appended {len(new_rows)} new rows to Google Sheet '{worksheet.title}'.")
            st.success(f"Successfully added {len(new_rows)} new scrim records to the sheet.")
            return True # Indicate that new data was added
        except gspread.exceptions.APIError as e:
            debug_logs.append(f"Error: API Error appending data to Google Sheets: {str(e)}")
            st.error(f"Error appending data to Google Sheets: {str(e)}")
            # Optionally: Attempt to add rows individually or in smaller batches on failure
            return False # Indicate failure
        except Exception as e:
            debug_logs.append(f"Error: Unexpected error appending data to Google Sheets: {str(e)}")
            st.error(f"Unexpected error appending data to Google Sheets: {str(e)}")
            return False
    else:
        debug_logs.append("Info: No new unique rows to add to the sheet.")
        st.info("No new scrim records found to add to the sheet.")
        return False # Indicate that no new data was added

# --- Aggregation Function (keep as is) ---
def aggregate_scrims_data(worksheet, time_filter="All"):
    if not worksheet:
        st.error("Cannot aggregate scrims data: Invalid worksheet.")
        return {}, {}, []

    blue_side_stats = {"wins": 0, "losses": 0, "total": 0}
    red_side_stats = {"wins": 0, "losses": 0, "total": 0}
    match_history = []
    expected_columns = 26 # Date, ID, B, R, 5xBB, 5xRB, 5xBP, 5xRP, Dur, Res

    # Determine time threshold
    now = datetime.utcnow()
    time_threshold = None
    if time_filter == "1 Week": time_threshold = now - timedelta(weeks=1)
    elif time_filter == "2 Weeks": time_threshold = now - timedelta(weeks=2)
    elif time_filter == "3 Weeks": time_threshold = now - timedelta(weeks=3)
    elif time_filter == "4 Weeks": time_threshold = now - timedelta(weeks=4)
    elif time_filter == "2 Months": time_threshold = now - timedelta(days=60) # Approx 2 months

    try:
        data = worksheet.get_all_values()
        if len(data) <= 1: # Only header or empty
             st.info("No scrim data found in the sheet for aggregation.")
             return blue_side_stats, red_side_stats, match_history
    except gspread.exceptions.APIError as e:
         st.error(f"API Error reading scrims data for aggregation: {e}")
         return blue_side_stats, red_side_stats, match_history
    except Exception as e:
         st.error(f"Unexpected error reading scrims data for aggregation: {e}")
         return blue_side_stats, red_side_stats, match_history


    header = data[0] # Assuming first row is header
    # Dynamically find column indices if needed, or rely on fixed order
    try:
        date_col = header.index("Date")
        match_id_col = header.index("Match ID")
        blue_team_col = header.index("Blue Team")
        red_team_col = header.index("Red Team")
        duration_col = header.index("Duration")
        result_col = header.index("Result")
    except ValueError as e:
        st.error(f"Missing expected column in Scrims sheet header: {e}. Cannot aggregate.")
        return blue_side_stats, red_side_stats, match_history


    for row in data[1:]: # Skip header row
        if len(row) < expected_columns:
            # st.warning(f"Skipping aggregation for incomplete row: {row}")
            continue # Skip rows that don't have enough columns

        try:
            date_str = row[date_col]
            match_id = row[match_id_col]
            blue_team = row[blue_team_col]
            red_team = row[red_team_col]
            duration = row[duration_col]
            result = row[result_col] # Should be "Win", "Loss", "Tie", or "N/A"

            # Time Filtering
            if time_threshold and date_str != "N/A":
                try:
                    match_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    if match_date < time_threshold:
                        continue # Skip if match is older than filter
                except ValueError:
                     # st.warning(f"Could not parse date '{date_str}' for filtering row: {row}")
                     continue # Skip if date format is wrong

            # Identify side and result for TEAM_NAME
            if blue_team == TEAM_NAME:
                is_blue_side = True
                is_our_game = True
            elif red_team == TEAM_NAME:
                is_blue_side = False
                is_our_game = True
            else:
                is_our_game = False # Skip if team name doesn't match exactly

            if is_our_game:
                win = (result == "Win")

                if is_blue_side:
                    blue_side_stats["total"] += 1
                    if win: blue_side_stats["wins"] += 1
                    else: blue_side_stats["losses"] += 1 # Increment losses only if not a win (handles Ties/N/A implicitly)
                else: # Red side
                    red_side_stats["total"] += 1
                    if win: red_side_stats["wins"] += 1
                    else: red_side_stats["losses"] += 1

                # Add to match history (can add picks/bans here if needed)
                match_history.append({
                    "Date": date_str,
                    "Match ID": match_id,
                    "Blue Team": blue_team,
                    "Red Team": red_team,
                    "Duration": duration,
                    "Result": result if result in ["Win", "Loss", "Tie"] else "N/A" # Standardize result
                })
        except IndexError:
             # st.warning(f"Index error processing row (expected {expected_columns} columns): {row}")
             continue # Skip row if index is out of bounds
        except Exception as e:
             # st.error(f"Unexpected error processing aggregation row: {row} - Error: {e}")
             continue # Skip row on other errors


    # Sort match history (most recent first)
    match_history.sort(key=lambda x: x.get("Date", "0"), reverse=True)

    return blue_side_stats, red_side_stats, match_history


# --- Main Streamlit Page Function for Scrims ---
def scrims_page():
    """Displays the Scrims statistics and update interface."""
    st.title(f"Scrims Analysis - {TEAM_NAME}")

    # --- Back Button ---
    # if st.button("⬅️ Back to HLL Stats"):
    #     st.session_state.current_page = "Hellenic Legends League Stats"
    #     st.rerun() # Rerun to switch page in app.py

    # --- Google Sheet Setup ---
    # Setup client (cached)
    client = setup_google_sheets() # Calls the function defined above in this file
    if not client:
        st.error("Failed to initialize Google Sheets client. Scrims features unavailable.")
        return

    # Open spreadsheet
    try:
        spreadsheet = client.open(SCRIMS_SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        st.error(f"Spreadsheet '{SCRIMS_SHEET_NAME}' not found.")
        st.info(f"Please ensure a Google Sheet named '{SCRIMS_SHEET_NAME}' exists and the service account has edit permissions.")
        # Optionally offer creation, but manual might be safer
        return
    except gspread.exceptions.APIError as e:
        st.error(f"API Error accessing spreadsheet '{SCRIMS_SHEET_NAME}': {e}")
        return
    except Exception as e:
        st.error(f"Unexpected error opening spreadsheet '{SCRIMS_SHEET_NAME}': {e}")
        return

    # Get or create worksheet
    wks = check_if_scrims_worksheet_exists(spreadsheet, SCRIMS_WORKSHEET_NAME)
    if not wks:
         st.error(f"Failed to get or create the '{SCRIMS_WORKSHEET_NAME}' worksheet.")
         return

    # --- Update Data Section ---
    st.subheader("Update Scrim Data")
    debug_logs_scrims = [] # Use a local list for logs specific to this run
    if 'scrims_debug_logs' not in st.session_state:
         st.session_state.scrims_debug_logs = [] # Initialize if needed

    if st.button("Download & Update Scrims Data from GRID API", key="update_scrims_btn"):
        st.session_state.scrims_debug_logs = [] # Clear previous logs
        debug_logs_scrims = st.session_state.scrims_debug_logs # Assign to session state list

        with st.spinner("Fetching series list from GRID API..."):
             series_list = get_all_series(debug_logs_scrims) # Fetch series IDs

        if series_list:
            st.info(f"Found {len(series_list)} potential scrim series. Processing...")
            # Placeholder for progress bar
            progress_bar_placeholder = st.empty()
            progress_bar = progress_bar_placeholder.progress(0, text="Starting processing...")

            try:
                # Pass the worksheet object, series list, log list, and progress bar
                data_added = update_scrims_data(wks, series_list, debug_logs_scrims, progress_bar)
                if data_added:
                     st.success("Scrims data update finished! New data was added.")
                     # Optionally clear aggregation cache here if needed
                     # aggregate_scrims_data.clear()
                else:
                     st.info("Scrims data update finished. No new unique data found or an error occurred.")
            except Exception as e:
                 st.error(f"An error occurred during the scrims update process: {e}")
                 debug_logs_scrims.append(f"FATAL ERROR during update: {e}")
            finally:
                 progress_bar_placeholder.empty() # Remove progress bar
        else:
             st.warning("No scrim series found matching the criteria in the API.")

    # --- Display Debug Logs ---
    if st.session_state.scrims_debug_logs:
         with st.expander("Show Scrims Update Debug Logs"):
             st.code("\n".join(st.session_state.scrims_debug_logs), language=None)


    st.divider()
    # --- Display Aggregated Statistics ---
    st.subheader("Scrim Performance Statistics")

    # Time Filter Selection
    time_filter = st.selectbox(
        "Filter Stats by Time Range:",
        ["All Time", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"],
        key="scrims_time_filter"
    )

    # Aggregate and Display
    try:
        # Pass the worksheet object and filter
        blue_stats, red_stats, history = aggregate_scrims_data(wks, time_filter.replace(" Time", "")) # Remove " Time" for function

        total_matches = blue_stats["total"] + red_stats["total"]
        total_wins = blue_stats["wins"] + red_stats["wins"]
        total_losses = blue_stats["losses"] + red_stats["losses"] # Sum calculated losses

        st.markdown(f"**Period: {time_filter}**")
        col_ov, col_b, col_r = st.columns(3)

        with col_ov:
             st.metric("Total Games", total_matches)
             overall_wr = (total_wins / total_matches * 100) if total_matches > 0 else 0
             st.metric("Overall Win Rate", f"{overall_wr:.1f}%", f"{total_wins}W - {total_losses}L")

        with col_b:
             st.metric("Blue Side Games", blue_stats["total"])
             blue_wr = (blue_stats["wins"] / blue_stats["total"] * 100) if blue_stats["total"] > 0 else 0
             st.metric("Blue Side Win Rate", f"{blue_wr:.1f}%", f"{blue_stats['wins']}W - {blue_stats['losses']}L")

        with col_r:
             st.metric("Red Side Games", red_stats["total"])
             red_wr = (red_stats["wins"] / red_stats["total"] * 100) if red_stats["total"] > 0 else 0
             st.metric("Red Side Win Rate", f"{red_wr:.1f}%", f"{red_stats['wins']}W - {red_stats['losses']}L")

        st.subheader("Match History")
        if history:
            df_history = pd.DataFrame(history)
            # Select columns to display
            df_display = df_history[["Date", "Blue Team", "Red Team", "Result", "Duration", "Match ID"]]
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info(f"No match history available for the selected time range ({time_filter}).")

    except Exception as e:
         st.error(f"An error occurred displaying scrim statistics: {e}")


# This check is mostly relevant if you were running scrims.py directly
# It's fine to keep it, but it won't execute when called from app.py
if __name__ == "__main__":
    # This part will not run when imported by app.py
    st.warning("This page is intended to be run from the main app.py")
    # You could potentially add logic here for standalone testing if needed
    # scrims_page()
    pass

# --- END OF FILE scrims.py ---
