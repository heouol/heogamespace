import streamlit as st
import requests
import pandas as pd
from collections import defaultdict
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
            "types": "SCRIM",  # Только скримы
            "teams": TEAM_NAME
        },
        "first": 50,  # Максимум 50 серий за раз
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
            series = data.get("data", {}).get("allSeries", {}).get("edges", [])
            return [s["node"] for s in series]
        else:
            st.error(f"Ошибка GraphQL API: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения к GraphQL API: {str(e)}")
        return []

# Функция для загрузки данных серии
def get_series_data(series_id, game_number=1):
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/riot/series/{series_id}/games/{game_number}/summary"
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Ошибка API для Series {series_id}, Game {game_number}: {response.status_code} - {response.text}")
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
        # Пробуем загрузить данные для каждой игры (максимум 5 игр в серии)
        for game_number in range(1, 6):
            scrim_data = get_series_data(series_id, game_number)
            if not scrim_data:  # Если игра не существует (например, 400 ошибка), прерываем цикл
                break
            
            # Отладочный вывод
            st.write(f"Данные для Series {series_id}, Game {game_number}:", scrim_data)
            
            match_id = str(scrim_data.get("matchId", scrim_data.get("id", series_id + f"-{game_number}")))
            if match_id in existing_match_ids:
                continue
            
            teams = scrim_data.get("teams", [{}, {}])
            is_blue_side = teams[0].get("name") == TEAM_NAME
            opponent = teams[1].get("name", "Unknown") if is_blue_side else teams[0].get("name", "Unknown")
            win = scrim_data.get("winner", {}).get("name") == TEAM_NAME
            date = scrim_data.get("startTime", series.get("startTimeScheduled", "N/A"))
            if date != "N/A" and "T" in date:
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
            
            # Извлекаем патч из данных (предполагаем, что он есть в scrim_data)
            patch = scrim_data.get("gameVersion", "N/A").split(".")[:2]  # Например, "14.5.1" -> "14.5"
            patch = ".".join(patch) if patch != "N/A" else "N/A"
            
            new_row = [date, match_id, opponent, "Blue" if is_blue_side else "Red", "Win" if win else "Loss", "N/A", patch]
            
            # Пики
            participants = scrim_data.get("participants", [])
            picks = [f"{p.get('role', 'N/A')}:{p.get('champion', 'N/A')}" for p in participants if p.get("team") == TEAM_NAME]
            new_row.extend(picks)
            
            # Баны
            bans = scrim_data.get("bans", {}).get(TEAM_NAME, [])
            new_row.append(",".join(bans) if bans else "N/A")
            
            new_rows.append(new_row)
            existing_match_ids.add(match_id)
    
    if new_rows:
        worksheet.append_rows(new_rows)
        return True
    return False

# Функция для агрегации данных из Google Sheets
def aggregate_scrims_data(worksheet, selected_patch=None):
    role_stats = {
        "Top": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Jungle": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Mid": defaultdict(lambda: {"games": 0, "wins": 0}),
        "ADC": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Support": defaultdict(lambda: {"games": 0, "wins": 0})
    }
    blue_side_stats = {"wins": 0, "losses": 0, "total": 0}
    red_side_stats = {"wins": 0, "losses": 0, "total": 0}
    match_history = []
    patches = set()

    data = worksheet.get_all_values()
    if len(data) <= 1:
        return role_stats, blue_side_stats, red_side_stats, match_history, patches

    for row in data[1:]:
        if len(row) < 7:  # Учитываем новую колонку Patch
            continue
        
        date, match_id, opponent, side, result, vod, patch, *rest = row
        bans = row[-1] if len(row) > 7 else "N/A"
        
        # Фильтрация по патчу
        if selected_patch and patch != selected_patch:
            continue
        
        patches.add(patch)
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

        picks = row[7:-1] if len(row) > 7 else []
        for pick in picks:
            if ":" in pick:
                role, champion = pick.split(":", 1)
                if role in role_stats and champion != "N/A":
                    role_stats[role][champion]["games"] += 1
                    if win:
                        role_stats[role][champion]["wins"] += 1

        match_history.append({
            "Date": date,
            "Opponent": opponent,
            "Side": side,
            "Result": result,
            "VOD": vod,
            "Bans": bans
        })

    return role_stats, blue_side_stats, red_side_stats, match_history, patches

# Основная функция страницы
def scrims_page():
    st.title("Scrims - Gamespace MC")

    if st.button("Back to Hellenic Legends League Stats"):
        st.session_state.current_page = "Hellenic Legends League Stats"
        st.rerun()

    client = setup_google_sheets()
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
        wks.append_row(["Date", "Match ID", "Opponent", "Side", "Result", "VOD", "Patch", "Picks", "Bans"])

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
                st.warning("No series found.")

    # Получаем данные для фильтрации
    _, _, _, _, patches = aggregate_scrims_data(wks)
    patches = sorted([p for p in patches if p != "N/A"])
    
    # Выпадающий список для фильтрации по патчу
    selected_patch = st.selectbox("Filter by Patch", ["All"] + patches, index=0)
    selected_patch = None if selected_patch == "All" else selected_patch

    # Агрегация и отображение
    role_stats, blue_side_stats, red_side_stats, match_history, _ = aggregate_scrims_data(wks, selected_patch)
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

    st.subheader("Pick Statistics by Role")
    roles = ["Top", "Jungle", "Mid", "ADC", "Support"]
    cols = st.columns(len(roles))
    for i, role in enumerate(roles):
        with cols[i]:
            st.write(f"**{role}**")
            stats = []
            for champ, data in role_stats[role].items():
                win_rate = data["wins"] / data["games"] * 100 if data["games"] > 0 else 0
                stats.append({
                    "Champion": champ,
                    "Games": data["games"],
                    "Wins": data["wins"],
                    "Win Rate (%)": f"{win_rate:.2f}"
                })
            if stats:
                df = pd.DataFrame(stats).sort_values("Games", ascending=False)
                st.markdown(df.to_html(index=False, escape=False), unsafe_allow_html=True)
            else:
                st.write("No data available.")

    st.subheader("Match History")
    if match_history:
        df_history = pd.DataFrame(match_history)
        df_history["VOD"] = df_history["VOD"].apply(lambda x: f'<a href="{x}" target="_blank">Watch</a>' if x != "N/A" else "N/A")
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
