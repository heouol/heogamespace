import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta
import time  # Для добавления задержек

# ... (Keep the rest of your code above this function as is) ...
# Настройки
GRID_API_KEY = "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63" # NOTE: Consider hiding this key using Streamlit secrets or environment variables
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
TOURNAMENT_NAME = "League of Legends Scrims" # Not currently used in logic, but good to have
SHEET_NAME = "Scrims_GMS_Detailed" # Ensure this matches your Google Sheet name

# --- START OF MODIFIED FUNCTION ---

# Функция для обновления данных в Google Sheets
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not series_list:
        debug_logs.append("Список серий пуст. Нечего обновлять.")
        st.write("Список серий пуст. Нечего обновлять.")
        return False

    existing_data = worksheet.get_all_values()
    # Assuming Match ID is in the second column (index 1)
    existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    new_rows = []
    gamespace_series_count = 0  # Счётчик серий для Gamespace MC
    skipped_duplicates = 0  # Счётчик пропущенных дубликатов
    processed_count = 0

    total_series_to_process = len(series_list)
    for i, series_summary in enumerate(series_list):
        series_id = series_summary.get("id")
        if not series_id:
            debug_logs.append(f"Skipping entry {i+1} due to missing series ID: {series_summary}")
            st.write(f"Skipping entry {i+1} due to missing series ID.")
            continue

        # --- Progress Bar Update ---
        progress = (i + 1) / total_series_to_process
        progress_bar.progress(progress, text=f"Processing series {i + 1}/{total_series_to_process} (ID: {series_id})")
        # --- End Progress Bar Update ---

        # Добавляем задержку между запросами к API серий (чтобы избежать 429)
        if i > 0:
            time.sleep(1.0) # 1 second delay between series requests

        debug_logs.append(f"\n--- Processing Series {series_id} ---")
        st.write(f"\n--- Processing Series {series_id} ---")
        scrim_data = download_series_data(series_id, debug_logs=debug_logs)
        if not scrim_data:
            debug_logs.append(f"Failed to download data for Series {series_id} or it was skipped (e.g., 404).")
            st.write(f"Failed to download data for Series {series_id} or it was skipped.")
            continue # Skip to next series if data download failed

        # Отладка: выводим весь scrim_data
        # debug_logs.append(f"Series {series_id} - RAW scrim_data: {json.dumps(scrim_data, indent=2)}")
        # st.write(f"Series {series_id} - RAW scrim_data: {json.dumps(scrim_data, indent=2)}") # Careful, can be very long

        # Проверяем, участвует ли Gamespace MC
        teams = scrim_data.get("teams", None)
        if not teams or len(teams) < 2:
            debug_logs.append(f"Could not find 2 teams for Series {series_id}. Skipping. Data: {teams}")
            st.write(f"Could not find 2 teams for Series {series_id}. Skipping.")
            continue

        team_0 = teams[0]
        team_1 = teams[1]
        team_0_name = team_0.get("name", "Unknown_Team_0")
        team_1_name = team_1.get("name", "Unknown_Team_1")

        if TEAM_NAME not in [team_0_name, team_1_name]:
            # debug_logs.append(f"Series {series_id} ({team_0_name} vs {team_1_name}) does not involve {TEAM_NAME}. Skipping.")
            # st.write(f"Series {series_id} ({team_0_name} vs {team_1_name}) does not involve {TEAM_NAME}. Skipping.")
            continue # Skip if our team didn't play

        gamespace_series_count += 1  # Увеличиваем счётчик найденных серий
        debug_logs.append(f"Found series for {TEAM_NAME} (Series {series_id}): {team_0_name} vs {team_1_name}")
        st.write(f"Found series for {TEAM_NAME} (Series {series_id}): {team_0_name} vs {team_1_name}")

        # Используем ID серии как запасной вариант, если matchId отсутствует
        match_id = str(scrim_data.get("matchId", series_id))
        if match_id in existing_match_ids:
            debug_logs.append(f"Series {series_id} already exists in the sheet (Match ID: {match_id}). Skipping.")
            st.write(f"Series {series_id} already exists in the sheet (Match ID: {match_id}). Skipping.")
            skipped_duplicates += 1
            continue # Skip duplicate entries

        # --- Дата ---
        date_str = scrim_data.get("startTime", series_summary.get("startTimeScheduled", scrim_data.get("updatedAt", "N/A")))
        date_formatted = "N/A"
        if date_str != "N/A" and isinstance(date_str, str):
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S+00:00"):
                try:
                    # Parse assuming UTC, then format
                    date_formatted = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d %H:%M:%S")
                    break # Stop trying formats once one works
                except ValueError:
                    continue # Try next format
            if date_formatted == "N/A":
                 debug_logs.append(f"Warning: Could not parse date string '{date_str}' for series {series_id}")
                 st.write(f"Warning: Could not parse date string '{date_str}' for series {series_id}")
        else:
             debug_logs.append(f"Warning: Date string not found or invalid for series {series_id}")
             st.write(f"Warning: Date string not found or invalid for series {series_id}")


        # --- Команды ---
        # GRID convention: Team 0 is usually Blue side, Team 1 is Red side
        blue_team = team_0_name
        red_team = team_1_name
        blue_team_id = team_0.get("id")
        red_team_id = team_1.get("id")

        # --- ИЗВЛЕКАЕМ ДАННЫЕ ИГРЫ ---
        game_id = None
        game_data = None

        # 1. Попытка найти game_id в scrim_data
        # Ищем список игр на верхнем уровне или внутри 'object'
        potential_games_list = scrim_data.get("games", [])
        if not potential_games_list and "object" in scrim_data and isinstance(scrim_data["object"], dict):
             potential_games_list = scrim_data["object"].get("games", [])

        if potential_games_list and isinstance(potential_games_list, list) and len(potential_games_list) > 0:
            # Берем первую игру из списка (обычно скримы состоят из одной игры в серии)
            game_info = potential_games_list[0]
            if isinstance(game_info, dict):
                game_id = game_info.get("id")
            elif isinstance(game_info, str): # Иногда может быть просто список ID
                game_id = game_info
            debug_logs.append(f"Series {series_id} - Found potential game_id in scrim_data: {game_id}")
            st.write(f"Series {series_id} - Found potential game_id in scrim_data: {game_id}")

        # 2. Если game_id найден, загружаем детальные данные игры
        if game_id:
            debug_logs.append(f"Series {series_id} - Attempting to fetch game data for game_id: {game_id}...")
            st.write(f"Series {series_id} - Attempting to fetch game data for game_id: {game_id}...")
            # Добавляем небольшую задержку перед запросом данных игры
            time.sleep(0.5) # 0.5 second delay before game data request
            game_data = download_game_data(game_id, debug_logs=debug_logs)

            if game_data:
                debug_logs.append(f"Series {series_id} - Successfully fetched game_data (from game endpoint). Keys: {list(game_data.keys())}")
                # st.write(f"Series {series_id} - game_data (from game endpoint): {json.dumps(game_data, indent=2)}") # Optional: Full dump
            else:
                debug_logs.append(f"Series {series_id} - Failed to fetch game data for game_id {game_id} (e.g., 404 or other error). Will rely on scrim_data if possible.")
                st.write(f"Series {series_id} - Failed to fetch game data for game_id {game_id}.")
        else:
            debug_logs.append(f"Series {series_id} - game_id not found within scrim_data.")
            st.write(f"Series {series_id} - game_id not found within scrim_data.")

        # --- Извлечение драфта и длительности ---
        draft_actions = []
        clock_data = {}
        duration_seconds = "N/A"

        # 3. Приоритет данным из game_data (если они были загружены)
        if game_data:
            draft_actions = game_data.get("draftActions", [])
            clock_data = game_data.get("clock", {})
            duration_seconds = clock_data.get("currentSeconds", game_data.get("duration", "N/A")) # Prefer clock, fallback to game duration
            debug_logs.append(f"Series {series_id} - Using data from fetched game_data.")
            st.write(f"Series {series_id} - Using data from fetched game_data.")
        else:
            # 4. Запасной вариант: Попытка найти данные в scrim_data (менее надежно)
            debug_logs.append(f"Series {series_id} - Falling back to searching within scrim_data (less reliable).")
            st.write(f"Series {series_id} - Falling back to searching within scrim_data (less reliable).")
            # Попробуем снова найти game_data_from_scrim, если вдруг структура там есть
            game_data_from_scrim = {}
            if potential_games_list and isinstance(potential_games_list, list) and len(potential_games_list) > 0:
                 if isinstance(potential_games_list[0], dict):
                     game_data_from_scrim = potential_games_list[0]

            if game_data_from_scrim:
                 draft_actions = game_data_from_scrim.get("draftActions", [])
                 clock_data = game_data_from_scrim.get("clock", {})
                 duration_seconds = clock_data.get("currentSeconds", game_data_from_scrim.get("duration", "N/A"))
                 debug_logs.append(f"Series {series_id} - Found fallback data in scrim_data. Keys: {list(game_data_from_scrim.keys())}")
                 st.write(f"Series {series_id} - Found fallback data in scrim_data.")
            else:
                 # Еще один запасной вариант - искать duration прямо в scrim_data
                 duration_seconds = scrim_data.get("duration", "N/A")
                 debug_logs.append(f"Series {series_id} - No game details found in scrim_data. Draft/Clock likely unavailable. Trying top-level duration.")
                 st.write(f"Series {series_id} - No game details found in scrim_data.")


        debug_logs.append(f"Series {series_id} - Final draftActions count: {len(draft_actions)}")
        st.write(f"Series {series_id} - Final draftActions count: {len(draft_actions)}")
        # st.write(f"Series {series_id} - draftActions: {json.dumps(draft_actions, indent=2)}") # Optional: dump draft actions

        # --- Обработка драфта ---
        blue_bans = ["N/A"] * 5
        red_bans = ["N/A"] * 5
        blue_picks = ["N/A"] * 5
        red_picks = ["N/A"] * 5

        blue_ban_idx = 0
        red_ban_idx = 0
        blue_pick_idx = 0
        red_pick_idx = 0

        # Standard competitive draft sequence numbers
        # Bans Phase 1: B1, R1, B2, R2, B3, R3 (seq 1-6)
        # Picks Phase 1: B1, R1, R2, B2, B3, R3 (seq 7-12)
        # Bans Phase 2: R4, B4, R5, B5 (seq 13-16)
        # Picks Phase 2: R4, B4, B5, R5 (seq 17-20)

        if not draft_actions:
            debug_logs.append(f"Series {series_id} - No draft actions found to process.")
            st.write(f"Series {series_id} - No draft actions found.")
        else:
            # Sort actions by sequence number just in case they are out of order
            draft_actions.sort(key=lambda x: int(x.get("sequenceNumber", 99)))

            for action in draft_actions:
                try:
                    # Use int for sequence comparison
                    sequence = int(action.get("sequenceNumber", -1))
                    action_type = action.get("type")
                    # drafter_team_id = action.get("drafter", {}).get("id") # We rely on sequence number now
                    champion = action.get("draftable", {}).get("name", "N/A")

                    if action_type == "ban":
                        # Blue Bans: 1, 3, 5 (Phase 1) | 14, 16 (Phase 2)
                        if sequence in [1, 3, 5, 14, 16]:
                            if blue_ban_idx < 5:
                                blue_bans[blue_ban_idx] = champion
                                blue_ban_idx += 1
                            else: debug_logs.append(f"Series {series_id} - Warning: More than 5 blue bans found (Seq {sequence})")
                        # Red Bans: 2, 4, 6 (Phase 1) | 13, 15 (Phase 2)
                        elif sequence in [2, 4, 6, 13, 15]:
                            if red_ban_idx < 5:
                                red_bans[red_ban_idx] = champion
                                red_ban_idx += 1
                            else: debug_logs.append(f"Series {series_id} - Warning: More than 5 red bans found (Seq {sequence})")
                        else:
                             debug_logs.append(f"Series {series_id} - Warning: Unexpected ban sequence number {sequence}")

                    elif action_type == "pick":
                        # Blue Picks: 7 (P1), 10, 11 (P1) | 18, 19 (P2)
                        if sequence in [7, 10, 11, 18, 19]:
                             if blue_pick_idx < 5:
                                blue_picks[blue_pick_idx] = champion
                                blue_pick_idx += 1
                             else: debug_logs.append(f"Series {series_id} - Warning: More than 5 blue picks found (Seq {sequence})")
                        # Red Picks: 8, 9 (P1), 12 (P1) | 17, 20 (P2)
                        elif sequence in [8, 9, 12, 17, 20]:
                             if red_pick_idx < 5:
                                red_picks[red_pick_idx] = champion
                                red_pick_idx += 1
                             else: debug_logs.append(f"Series {series_id} - Warning: More than 5 red picks found (Seq {sequence})")
                        else:
                             debug_logs.append(f"Series {series_id} - Warning: Unexpected pick sequence number {sequence}")

                except (ValueError, TypeError) as e:
                     debug_logs.append(f"Series {series_id} - Error processing draft action {action}: {e}")
                     st.write(f"Series {series_id} - Error processing draft action {action}: {e}")
                     continue # Skip this action if sequence number is invalid


        # --- Длительность ---
        duration = "N/A"
        if isinstance(duration_seconds, (int, float)) and duration_seconds >= 0:
            try:
                duration = f"{int(duration_seconds // 60)}:{int(duration_seconds % 60):02d}"  # Формат MM:SS
            except Exception as e:
                debug_logs.append(f"Series {series_id} - Error formatting duration {duration_seconds}: {e}")
                st.write(f"Series {series_id} - Error formatting duration {duration_seconds}: {e}")
        elif duration_seconds != "N/A":
             debug_logs.append(f"Series {series_id} - Invalid duration value found: {duration_seconds}")
             st.write(f"Series {series_id} - Invalid duration value found: {duration_seconds}")

        debug_logs.append(f"Series {series_id} - Final Duration: {duration}")
        st.write(f"Series {series_id} - Final Duration: {duration}")

        # --- Победа или поражение ---
        result = "N/A"
        if team_0.get("won") is True and team_0_name == TEAM_NAME:
            result = "Win"
        elif team_1.get("won") is True and team_1_name == TEAM_NAME:
            result = "Win"
        elif team_0.get("won") is False and team_0_name == TEAM_NAME:
             result = "Loss"
        elif team_1.get("won") is False and team_1_name == TEAM_NAME:
             result = "Loss"
        # Handle case where win status is missing or team names mismatch somehow
        elif team_0.get("won") is None or team_1.get("won") is None:
             debug_logs.append(f"Series {series_id} - Win status missing for one or both teams.")
             st.write(f"Series {series_id} - Win status missing for one or both teams.")
             # Could attempt to infer from scores if available, but keeping N/A is safer
        else:
             # This case implies our team name wasn't found correctly earlier, which shouldn't happen
              debug_logs.append(f"Series {series_id} - Could not determine result for {TEAM_NAME}.")
              st.write(f"Series {series_id} - Could not determine result for {TEAM_NAME}.")


        # --- Формируем строку ---
        # Ensure the order matches the header row:
        # "Date", "Match ID", "Blue Team", "Red Team",
        # "Blue Ban 1-5", "Red Ban 1-5", "Blue Pick 1-5", "Red Pick 1-5",
        # "Duration", "Result"
        new_row = [
            date_formatted, match_id, blue_team, red_team,
            *blue_bans,  # Unpack the list of 5 blue bans
            *red_bans,   # Unpack the list of 5 red bans
            *blue_picks, # Unpack the list of 5 blue picks
            *red_picks,  # Unpack the list of 5 red picks
            duration, result
        ]

        # Check row length (should be 1 + 1 + 1 + 1 + 5 + 5 + 5 + 5 + 1 + 1 = 26 columns based on current header setup)
        # Your original code mentioned 23 columns, let's double check the header in check_if_worksheets_exists and the header append below.
        # Ah, the header has 26 columns: Date, MatchID, Blue, Red, 5xBBan, 5xRBan, 5xBPick, 5xRPick, Duration, Result = 26
        if len(new_row) != 26:
             debug_logs.append(f"Error: Generated row for Series {series_id} has {len(new_row)} columns, expected 26. Row: {new_row}")
             st.error(f"Error: Generated row for Series {series_id} has {len(new_row)} columns, expected 26.")
             continue # Skip adding potentially malformed row

        new_rows.append(new_row)
        existing_match_ids.add(match_id) # Add to set to prevent adding duplicates within the same run
        processed_count += 1
        debug_logs.append(f"Series {series_id} processed successfully. Added to new rows.")
        st.write(f"Series {series_id} processed successfully.")
        # --- End of loop for one series ---

    # --- After processing all series ---
    progress_bar.progress(1.0, text="Processing complete. Updating sheet...")

    debug_logs.append(f"\n--- Summary ---")
    debug_logs.append(f"Total series fetched initially: {total_series_to_process}")
    debug_logs.append(f"Series involving {TEAM_NAME}: {gamespace_series_count}")
    debug_logs.append(f"Skipped duplicate Match IDs: {skipped_duplicates}")
    debug_logs.append(f"Successfully processed and ready to add: {processed_count}")
    debug_logs.append(f"New rows generated: {len(new_rows)}")

    st.write(f"\n--- Summary ---")
    st.write(f"Total series fetched initially: {total_series_to_process}")
    st.write(f"Series involving {TEAM_NAME}: {gamespace_series_count}")
    st.write(f"Skipped duplicate Match IDs: {skipped_duplicates}")
    st.write(f"Successfully processed and ready to add: {processed_count}")
    st.write(f"New rows generated: {len(new_rows)}")


    if new_rows:
        try:
            # Ensure header exists before appending (important for first run)
            if not existing_data: # If sheet was empty
                 wks.append_row([
                     "Date", "Match ID", "Blue Team", "Red Team",
                     "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
                     "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
                     "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5",
                     "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5",
                     "Duration", "Result"
                 ], value_input_option='USER_ENTERED') # Add header if sheet is empty
                 debug_logs.append("Added header row to empty sheet.")
                 st.write("Added header row to empty sheet.")

            worksheet.append_rows(new_rows, value_input_option='USER_ENTERED')
            debug_logs.append(f"Successfully appended {len(new_rows)} new rows to Google Sheet.")
            st.success(f"Successfully appended {len(new_rows)} new rows to Google Sheet.")
            return True # Indicate that new data was added
        except Exception as e:
            debug_logs.append(f"Error appending data to Google Sheets: {str(e)}")
            st.error(f"Error appending data to Google Sheets: {str(e)}")
            return False # Indicate failure
    else:
        debug_logs.append("No new unique rows to add to the sheet.")
        st.write("No new unique rows to add to the sheet.")
        return False # Indicate that no new data was added

# --- END OF MODIFIED FUNCTION ---

# ... (Keep the rest of your code below this function as is: setup_google_sheets, check_if_worksheets_exists, get_all_series, download_series_data, download_game_data, aggregate_scrims_data, scrims_page, if __name__ == "__main__":) ...

# Make sure check_if_worksheets_exists creates enough columns (26)
def check_if_worksheets_exists(spreadsheet, name):
    try:
        wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        # Updated column count to 26
        wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=26)
        # Add header row immediately upon creation
        wks.append_row([
            "Date", "Match ID", "Blue Team", "Red Team",
            "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
            "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
            "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5",
            "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5",
            "Duration", "Result"
        ], value_input_option='USER_ENTERED')
    return wks

# Ensure the main page logic calls check_if_worksheets_exists correctly
def scrims_page():
    st.title("Scrims - Gamespace MC")

    # ... (rest of the scrims_page function remains the same until sheet handling)

    client = setup_google_sheets()
    if not client:
        return

    try:
        spreadsheet = client.open(SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        st.info(f"Spreadsheet '{SHEET_NAME}' not found. Creating it.")
        spreadsheet = client.create(SHEET_NAME)
        # Share it so the service account can write (adjust sharing as needed)
        # spreadsheet.share('your-email@example.com', perm_type='user', role='writer') # Example sharing
        # Or share publicly if acceptable (less secure)
        spreadsheet.share('', perm_type='anyone', role='writer')
        st.info(f"Spreadsheet '{SHEET_NAME}' created. Please ensure the service account has edit permissions if not shared publicly.")
    except gspread.exceptions.APIError as e:
        st.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        return
    except Exception as e: # Catch other potential errors like network issues
        st.error(f"An unexpected error occurred while accessing Google Sheets: {str(e)}")
        return


    wks_name = "Scrims" # Define worksheet name
    wks = check_if_worksheets_exists(spreadsheet, wks_name) # Use the updated function

    # --- Button Logic ---
    debug_logs = []
    if 'debug_logs' not in st.session_state:
        st.session_state.debug_logs = [] # Initialize if not present

    if st.button("Download All Scrims Data"):
        st.session_state.debug_logs = [] # Clear previous logs on new run
        debug_logs = st.session_state.debug_logs # Use session state list
        with st.spinner("Downloading scrims data from GRID API... This may take a while."):
            # Placeholder for progress bar to be created inside update_scrims_data
            progress_bar_placeholder = st.empty()
            progress_bar = progress_bar_placeholder.progress(0, text="Starting...")

            series_list = get_all_series(debug_logs)
            if series_list:
                data_added = update_scrims_data(wks, series_list, debug_logs, progress_bar)
                if data_added:
                    st.success("Scrims data download and update process finished! New data was added.")
                else:
                    st.info("Scrims data download process finished. No new unique data found or an error occurred during update.")
                progress_bar_placeholder.empty()  # Remove progress bar
            else:
                st.warning("No series found matching the criteria.")
                progress_bar_placeholder.empty() # Remove progress bar

    # --- Display Section ---
    # Display logs from session state if button was pressed
    if st.session_state.debug_logs:
        with st.expander("Show Debug Logs"):
            st.code("\n".join(st.session_state.debug_logs), language=None)

    # ... (rest of the scrims_page function for filtering and display remains the same)
     # Выпадающий список для фильтрации по времени
    time_filter = st.selectbox(
        "Filter by Time Range",
        ["All", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"],
        key="time_filter_select" # Added key for potential state issues
    )

    # Агрегация и отображение с учётом фильтра
    try:
        blue_side_stats, red_side_stats, match_history = aggregate_scrims_data(wks, time_filter)
        total_matches = blue_side_stats["total"] + red_side_stats["total"]
        wins = blue_side_stats["wins"] + red_side_stats["wins"]
        losses = blue_side_stats["losses"] + red_side_stats["losses"]

        st.subheader(f"Overall Statistics ({time_filter})") # Indicate filter range
        win_rate = f"{wins/total_matches*100:.2f}%" if total_matches > 0 else "0.00%"
        st.markdown(f"**Total Matches:** {total_matches} | **Wins:** {wins} | **Losses:** {losses} | **Win Rate:** {win_rate}")
        blue_win_rate = f"{blue_side_stats['wins']/blue_side_stats['total']*100:.2f}%" if blue_side_stats['total'] > 0 else "0.00%"
        red_win_rate = f"{red_side_stats['wins']/red_side_stats['total']*100:.2f}%" if red_side_stats['total'] > 0 else "0.00%"
        st.markdown(f"**Blue Side:** {blue_side_stats['wins']}W / {blue_side_stats['losses']}L ({blue_side_stats['total']} Games, {blue_win_rate})")
        st.markdown(f"**Red Side:** {red_side_stats['wins']}W / {red_side_stats['losses']}L ({red_side_stats['total']} Games, {red_win_rate})")

        st.subheader(f"Match History ({time_filter})")
        if match_history:
            # Ensure DataFrame uses correct column names if aggregate_scrims_data provides them
            df_history = pd.DataFrame(match_history)
            # Select and reorder columns for display if needed
            # df_display = df_history[['Date', 'Match ID', 'Blue Team', 'Red Team', 'Duration', 'Result']]
            st.dataframe(df_history) # Use st.dataframe for better interactivity
            # Or use markdown table if preferred:
            # st.markdown(df_history.to_html(index=False, escape=False), unsafe_allow_html=True)
        else:
            st.write(f"No match history available for the selected time range ({time_filter}).")

    except gspread.exceptions.APIError as e:
        st.error(f"Error reading data from Google Sheets for aggregation: {e}")
    except Exception as e:
        st.error(f"An error occurred during data aggregation or display: {e}")


    # CSS для тёмной темы (Optional, Streamlit themes usually handle this)
    # st.markdown(""" ... css ... """, unsafe_allow_html=True)

# --- Main execution ---
if __name__ == "__main__":
    # Initialize session state for logs if it doesn't exist
    if 'debug_logs' not in st.session_state:
        st.session_state.debug_logs = []
    scrims_page()
