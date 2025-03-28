import streamlit as st
import requests
import pandas as pd
from collections import defaultdict
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime

# Настройки для API GRID (замени на свои реальные данные)
GRID_API_KEY = "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63"  # Замени на реальный ключ API GRID
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
TOURNAMENT_NAME = "League of Legends Scrims"
SHEET_NAME = "Scrims_GMS"  # Название Google Sheets для скримов

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

# Функция для получения данных из GRID API
def get_scrims_data(patch=None):
    headers = {"Authorization": f"Bearer {GRID_API_KEY}"}
    params = {"tournament": TOURNAMENT_NAME, "team": TEAM_NAME}
    if patch:
        params["patch"] = patch
    
    try:
        response = requests.get(f"{GRID_BASE_URL}/matches", headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Ошибка API: {response.status_code} - {response.text}")
            return []
    except requests.exceptions.RequestException as e:
        st.error(f"Ошибка подключения к GRID API: {str(e)}")
        return []

# Функция для обновления данных в Google Sheets
def update_scrims_data(worksheet, scrims_data):
    existing_data = worksheet.get_all_values()
    existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()
    
    new_rows = []
    for match in scrims_data:
        match_id = match.get("id", "N/A")
        if match_id not in existing_match_ids and match_id != "N/A":
            is_blue_side = match.get("team1", {}).get("name") == TEAM_NAME
            opponent = match.get("team2", {}).get("name") if is_blue_side else match.get("team1", {}).get("name")
            win = match.get("winner") == TEAM_NAME
            date = match.get("date", "N/A")
            if date != "N/A":
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S") if "T" in date else date
            
            new_rows.append([
                date,
                match_id,
                opponent,
                "Blue" if is_blue_side else "Red",
                "Win" if win else "Loss",
                match.get("vod_url", "N/A")
            ])
            
            # Добавляем пики и баны
            participants = match.get("participants", [])
            for player in participants:
                if player.get("team") == TEAM_NAME:
                    role = player.get("role", "N/A")
                    champion = player.get("champion", "N/A")
                    if role != "N/A" and champion != "N/A":
                        new_rows[-1].append(f"{role}:{champion}")
            bans = match.get("bans", {}).get(TEAM_NAME, [])
            new_rows[-1].append(",".join(bans) if bans else "N/A")

    if new_rows:
        worksheet.append_rows(new_rows)
    return new_rows

# Функция для агрегации данных из Google Sheets
def aggregate_scrims_data(worksheet):
    role_stats = {
        "Top": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Jungle": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Mid": defaultdict(lambda: {"games": 0, "wins": 0}),
        "ADC": defaultdict(lambda: {"games": 0, "wins": 0}),
        "Support": defaultdict(lambda: {"games": 0, "wins": 0})
    }
    bans_stats = defaultdict(int)
    blue_side_stats = {"wins": 0, "losses": 0, "total": 0}
    red_side_stats = {"wins": 0, "losses": 0, "total": 0}
    match_history = []

    data = worksheet.get_all_values()
    if len(data) <= 1:
        return role_stats, bans_stats, blue_side_stats, red_side_stats, match_history

    for row in data[1:]:
        if len(row) < 6:
            continue
        
        date, match_id, opponent, side, result, vod = row[:6]
        win = result == "Win"
        is_blue_side = side == "Blue"

        # Статистика по сторонам
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

        # Статистика пиков
        picks = row[6:-1] if len(row) > 6 else []
        for pick in picks:
            if ":" in pick:
                role, champion = pick.split(":", 1)
                if role in role_stats and champion != "N/A":
                    role_stats[role][champion]["games"] += 1
                    if win:
                        role_stats[role][champion]["wins"] += 1

        # Статистика банов
        bans = row[-1].split(",") if len(row) > 6 and row[-1] != "N/A" else []
        for ban in bans:
            if ban:
                bans_stats[ban] += 1

        # История матчей
        match_history.append({
            "Date": date,
            "Opponent": opponent,
            "Side": side,
            "Result": result,
            "VOD": vod
        })

    return role_stats, bans_stats, blue_side_stats, red_side_stats, match_history

# Основная функция страницы Scrims
def scrims_page():
    st.title("Scrims - Gamespace MC")

    # Кнопка возврата на главную страницу
    if st.button("Back to Hellenic Legends League Stats"):
        st.session_state.current_page = "Hellenic Legends League Stats"
        st.rerun()

    # Подключение к Google Sheets
    client = setup_google_sheets()
    if not client:
        return

    try:
        spreadsheet = client.open(SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        spreadsheet = client.create(SHEET_NAME)
        spreadsheet.share("", perm_type="anyone", role="writer")  # Открываем доступ, если нужно
    except gspread.exceptions.APIError as e:
        st.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        return

    # Создаем или получаем лист "Scrims"
    wks = check_if_worksheets_exists(spreadsheet, "Scrims")
    if not wks.get_all_values():
        wks.append_row(["Date", "Match ID", "Opponent", "Side", "Result", "VOD", "Picks", "Bans"])

    # Фильтр по патчу и обновление данных
    selected_patch = st.text_input("Filter by Patch (e.g., 14.5)", value="")
    if st.button("Update Scrims Data"):
        with st.spinner("Updating scrims data from GRID API..."):
            scrims_data = get_scrims_data(selected_patch if selected_patch else None)
            if scrims_data:
                update_scrims_data(wks, scrims_data)
                st.success("Scrims data updated!")
            else:
                st.warning("No new data to update.")

    # Агрегация данных
    role_stats, bans_stats, blue_side_stats, red_side_stats, match_history = aggregate_scrims_data(wks)
    total_matches = blue_side_stats["total"] + red_side_stats["total"]
    wins = blue_side_stats["wins"] + red_side_stats["wins"]
    losses = blue_side_stats["losses"] + red_side_stats["losses"]

    # Общая статистика
    st.subheader("Overall Statistics")
    st.markdown(f"**Total Matches:** {total_matches} | **Wins:** {wins} | **Losses:** {losses} | **Win Rate:** {wins/total_matches*100:.2f}%")
    st.markdown(f"**Blue Side:** {blue_side_stats['wins']}/{blue_side_stats['total']} ({blue_side_stats['wins']/blue_side_stats['total']*100:.2f}% if blue_side_stats['total'] > 0 else 0)")
    st.markdown(f"**Red Side:** {red_side_stats['wins']}/{red_side_stats['total']} ({red_side_stats['wins']/red_side_stats['total']*100:.2f}% if red_side_stats['total'] > 0 else 0)")

    # Статистика пиков по ролям
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

    # Статистика банов
    st.subheader("Ban Statistics")
    ban_stats = [{"Champion": champ, "Bans": count} for champ, count in bans_stats.items()]
    if ban_stats:
        df_bans = pd.DataFrame(ban_stats).sort_values("Bans", ascending=False)
        st.markdown(df_bans.to_html(index=False, escape=False), unsafe_allow_html=True)
    else:
        st.write("No ban data available.")

    # История матчей
    st.subheader("Match History")
    if match_history:
        df_history = pd.DataFrame(match_history)
        df_history["VOD"] = df_history["VOD"].apply(lambda x: f'<a href="{x}" target="_blank">Watch</a>' if x != "N/A" else "N/A")
        st.markdown(df_history.to_html(index=False, escape=False), unsafe_allow_html=True)
    else:
        st.write("No match history available.")

    # Стилизация таблиц
    st.markdown("""
        <style>
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
        }
        th, td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        </style>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    scrims_page()
