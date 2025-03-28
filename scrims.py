import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime

# Настройки
GRID_API_KEY = "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63"
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
TOURNAMENT_NAME = "League of Legends Scrims"
SHEET_NAME = "Scrims_GMS"

# Настройка Google Sheets
def setup_google_sheets():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds = os.getenv("GOOGLE_SHEETS_CREDS")
    if not json_creds:
        st.error("Не удалось загрузить учетные данные Google Sheets.")
        return None
    creds_dict = json.loads(json_creds)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    return client

def check_if_worksheets_exists(spreadsheet, name):
    try:
        wks = spreadsheet.worksheet(name)
    except gspread.exceptions.WorksheetNotFound:
        wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=10)
    return wks

# Функция для получения списка всех серий через GraphQL
def get_all_series():
    headers = {
        "x-api-key": GRID_API_KEY,
        "Content-Type": "application/json"
    }
    query = """
    query ($filter: SeriesFilter, $first: Int, $orderBy: SeriesOrderBy, $orderDirection: OrderDirection) {
        allSeries(
            filter: $filter
            first: $first
            orderBy: $orderBy
            orderDirection: $orderDirection
        ) {
            totalCount
            edges {
                node {
                    id
                    startTimeScheduled
                    tournament {
                        name
                    }
                }
            }
        }
    }
    """
    variables = {
        "filter": {
            "titleId": 3,  # LoL
            "types": "SCRIM"  # Ищем только скримы
        },
        "first": 100,  # Увеличиваем до 100
        "orderBy": "StartTimeScheduled",
        "orderDirection": "DESC"
    }

    try:
        response = requests.post(
            f"{GRID_BASE_URL}central-data/graphql",
            headers=headers,
            json={"query": query, "variables": variables}
        )
        if response.status_code == 200:
            data = response.json()
            st.write("GraphQL Response:", data)  # Отладочный вывод
            series = data.get("data", {}).get("allSeries", {}).get("edges", [])
            return [s["node"] for s in series]
        else:
            st.error(f"Ошибка GraphQL API: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения к GraphQL API: {str(e)}")
        return []

# Функция для загрузки данных серии (GRID-формат)
def download_series_data(series_id):
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Ошибка API для Series {series_id}: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения к GRID API: {str(e)}")
        return None

# Функция для обновления данных в Google Sheets
def update_scrims_data(worksheet, series_list):
    if not series_list:
        return False
    
    existing_data = worksheet.get_all_values()
    existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    new_rows = []
    
    for series in series_list:
        series_id = series.get("id")
        scrim_data = download_series_data(series_id)
        if not scrim_data:
            continue
        
        # Отладочный вывод
        st.write(f"Данные для Series {series_id}:", scrim_data)
        
        # Проверяем, участвует ли Gamespace MC
        teams = scrim_data.get("teams", [{}, {}])
        team_0_name = teams[0].get("name", "Unknown")
        team_1_name = teams[1].get("name", "Unknown")
        if TEAM_NAME not in [team_0_name, team_1_name]:
            continue
        
        match_id = str(scrim_data.get("matchId", scrim_data.get("id", series_id)))
        if match_id in existing_match_ids:
            continue
        
        is_blue_side = team_0_name == TEAM_NAME
        opponent = team_1_name if is_blue_side else team_0_name
        win = teams[0].get("won", False) if team_0_name == TEAM_NAME else teams[1].get("won", False)
        
        date = scrim_data.get("startTime", series.get("startTimeScheduled", scrim_data.get("updatedAt", "N/A")))
        if date != "N/A" and "T" in date:
            try:
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    date = "N/A"
        
        new_row = [date, match_id, opponent, "Blue" if is_blue_side else "Red", "Win" if win else "Loss"]
        
        new_rows.append(new_row)
        existing_match_ids.add(match_id)
    
    if new_rows:
        worksheet.append_rows(new_rows)
        return True
    return False

# Функция для агрегации данных из Google Sheets
def aggregate_scrims_data(worksheet):
    blue_side_stats = {"wins": 0, "losses": 0, "total": 0}
    red_side_stats = {"wins": 0, "losses": 0, "total": 0}
    match_history = []

    data = worksheet.get_all_values()
    if len(data) <= 1:
        return blue_side_stats, red_side_stats, match_history

    for row in data[1:]:
        if len(row) < 5:  # Минимально ожидаем Date, Match ID, Opponent, Side, Result
            continue
        
        date, match_id, opponent, side, result = row[:5]
        win = result == "Win"
        is_blue_side = side == "Blue"

        if is_blue_side:
            blue_side_stats["total"] += 1
            if win:
                blue_side_stats["wins"] += 1
            else:
                blue_side_stats["losses"] += 1
        else:
            red_side_stats["total"] += 1
            if win:
                red_side_stats["wins"] += 1
            else:
                red_side_stats["losses"] += 1

        match_history.append({
            "Date": date,
            "Opponent": opponent,
            "Side": side,
            "Result": result
        })

    return blue_side_stats, red_side_stats, match_history

# Основная функция страницы
def scrims_page():
    st.title("Scrims - Gamespace MC")

    if st.button("Back to Hellenic Legends League Stats"):
        st.session_state.current_page = "Hellenic Legends League Stats"
        st.rerun()

    client =植物_google_sheets()
    if not client:
        return

    try:
        spreadsheet = client.open(SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(SHEET_NAME)
        spreadsheet.share("", perm_type="anyone", role="writer")
    except gspread.exceptions.APIError as e:
        st.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        return

    wks = check_if_worksheets_exists(spreadsheet, "Scrims")
    if not wks.get_all_values():
        wks.append_row(["Date", "Match ID", "Opponent", "Side", "Result"])

    # Кнопка для загрузки всех серий
    if st.button("Download All Scrims Data"):
        with st.spinner("Downloading scrims data from GRID API..."):
            series_list = get_all_series()
            if series_list:
                if update_scrims_data(wks, series_list):
                    st.success("Scrims data downloaded and updated!")
                else:
                    st.warning("No new data added (possibly duplicates or error).")
            else:
                st.warning("No series found for Gamespace MC.")

    # Агрегация и отображение
    blue_side_stats, red_side_stats, match_history = aggregate_scrims_data(wks)
    total_matches = blue_side_stats["total"] + red_side_stats["total"]
    wins = blue_side_stats["wins"] + red_side_stats["wins"]
    losses = blue_side_stats["losses"] + red_side_stats["losses"]

    st.subheader("Overall Statistics")
    win_rate = f"{wins/total_matches*100:.2f}%" if total_matches > 0 else "0.00%"
    st.markdown(f"**Total Matches:** {total_matches} | **Wins:** {wins} | **Losses:** {losses} | **Win Rate:** {win_rate}")
    blue_win_rate = f"{blue_side_stats['wins']/blue_side_stats['total']*100:.2f}%" if blue_side_stats['total'] > 0 else "0.00%"
    red_win_rate = f"{red_side_stats['wins']/red_side_stats['total']*100:.2f}%" if red_side_stats['total'] > 0 else "0.00%"
    st.markdown(f"**Blue Side:** {blue_side_stats['wins']}/{blue_side_stats['total']} ({blue_win_rate})")
    st.markdown(f"**Red Side:** {red_side_stats['wins']}/{red_side_stats['total']} ({red_win_rate})")

    st.subheader("Match History")
    if match_history:
        df_history = pd.DataFrame(match_history)
        st.markdown(df_history.to_html(index=False, escape=False), unsafe_allow_html=True)
    else:
        st.write("No match history available.")

    st.markdown("""
        <style>
        table { width: 100%; border-collapse: collapse; margin: 10px 0; }
        th, td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        tr:hover { background-color: #f5f5f5; }
        </style>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    scrims_page()
