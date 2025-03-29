import streamlit as st
import requests
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import datetime, timedelta
import time  # Для добавления задержек

# Настройки
GRID_API_KEY = "kGPVB57xOjbFawMFqF18p1SzfoMdzWkwje4HWX63"
GRID_BASE_URL = "https://api.grid.gg/"
TEAM_NAME = "Gamespace MC"
TOURNAMENT_NAME = "League of Legends Scrims"
SHEET_NAME = "Scrims_GMS_Detailed"

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
        wks = spreadsheet.add_worksheet(title=name, rows=1200, cols=23)  # 23 столбца
    return wks

# Функция для получения списка всех серий через GraphQL с пагинацией
def get_all_series(debug_logs):
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
                    tournament {
                        name
                    }
                }
            }
        }
    }
    """
    # Устанавливаем дату начала поиска (например, последние 6 месяцев)
    six_months_ago = (datetime.utcnow() - timedelta(days=180)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    variables = {
        "filter": {
            "titleId": 3,  # LoL
            "types": "SCRIM",  # Фильтр для скримов
            "startTimeScheduled": {
                "gte": six_months_ago  # Ищем матчи за последние 6 месяцев
            }
        },
        "first": 50,  # Соответствует ограничению API
        "orderBy": "StartTimeScheduled",
        "orderDirection": "DESC"
    }

    all_series = []
    has_next_page = True
    after_cursor = None
    page_number = 1

    while has_next_page:
        if after_cursor:
            variables["after"] = after_cursor

        try:
            response = requests.post(
                f"{GRID_BASE_URL}central-data/graphql",
                headers=headers,
                json={"query": query, "variables": variables}
            )
            if response.status_code == 200:
                data = response.json()
                debug_logs.append(f"GraphQL Response (Page {page_number}): {json.dumps(data, indent=2)}")
                all_series_data = data.get("data", {}).get("allSeries", {})
                series = all_series_data.get("edges", [])
                all_series.extend([s["node"] for s in series])

                # Проверяем, есть ли следующая страница
                page_info = all_series_data.get("pageInfo", {})
                has_next_page = page_info.get("hasNextPage", False)
                after_cursor = page_info.get("endCursor", None)
                page_number += 1
            else:
                debug_logs.append(f"Ошибка GraphQL API: {response.status_code} - {response.text}")
                return []
        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Ошибка подключения к GraphQL API: {str(e)}")
            return []

    debug_logs.append(f"Всего серий получено: {len(all_series)}")
    return all_series

# Функция для загрузки данных серии (GRID-формат) с обработкой 429 и 404
def download_series_data(series_id, max_retries=3, initial_delay=5, debug_logs=None):
    headers = {"x-api-key": GRID_API_KEY}
    url = f"https://api.grid.gg/file-download/end-state/grid/series/{series_id}"
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:  # Too Many Requests
                delay = initial_delay * (2 ** attempt)  # Экспоненциальная задержка
                debug_logs.append(f"Ошибка 429 для Series {series_id}: слишком много запросов. Ждём {delay} секунд перед повторной попыткой...")
                time.sleep(delay)
                continue
            elif response.status_code == 404:  # Not Found
                debug_logs.append(f"Серия {series_id} не найдена (404). Пропускаем.")
                return None
            else:
                debug_logs.append(f"Ошибка API для Series {series_id}: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            debug_logs.append(f"Ошибка подключения к GRID API для Series {series_id}: {str(e)}")
            return None
    
    debug_logs.append(f"Не удалось загрузить данные для Series {series_id} после {max_retries} попыток.")
    return None

# Функция для обновления данных в Google Sheets
def update_scrims_data(worksheet, series_list, debug_logs, progress_bar):
    if not series_list:
        debug_logs.append("Список серий пуст. Нечего обновлять.")
        return False
    
    existing_data = worksheet.get_all_values()
    existing_match_ids = set(row[1] for row in existing_data[1:]) if len(existing_data) > 1 else set()  # Match ID в столбце 2
    new_rows = []
    gamespace_series_count = 0  # Счётчик серий для Gamespace MC
    skipped_duplicates = 0  # Счётчик пропущенных дубликатов
    
    total_series = len(series_list)
    for i, series in enumerate(series_list):
        # Обновляем прогресс-бар
        progress = (i + 1) / total_series
        progress_bar.progress(progress, text=f"Processing series {i + 1}/{total_series}")
        
        series_id = series.get("id")
        # Добавляем задержку между запросами (1 секунда)
        if i > 0:
            time.sleep(1.0)
        
        scrim_data = download_series_data(series_id, debug_logs=debug_logs)
        if not scrim_data:
            continue
        
        # Проверяем, участвует ли Gamespace MC
        teams = scrim_data.get("teams", None)
        if not teams or len(teams) < 2:
            debug_logs.append(f"Не удалось найти команды для Series {series_id}. Пропускаем. Данные: {scrim_data}")
            continue
        
        team_0_name = teams[0].get("name", "Unknown")
        team_1_name = teams[1].get("name", "Unknown")
        if TEAM_NAME not in [team_0_name, team_1_name]:
            continue
        
        gamespace_series_count += 1  # Увеличиваем счётчик
        debug_logs.append(f"Найдена серия для Gamespace MC (Series {series_id}): {team_0_name} vs {team_1_name}")
        
        match_id = str(scrim_data.get("matchId", scrim_data.get("id", series_id)))
        if match_id in existing_match_ids:
            debug_logs.append(f"Серия {series_id} уже существует в таблице (Match ID: {match_id}). Пропускаем.")
            skipped_duplicates += 1
            continue
        
        # Дата
        date = scrim_data.get("startTime", series.get("startTimeScheduled", scrim_data.get("updatedAt", "N/A")))
        if date != "N/A" and "T" in date:
            try:
                date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    date = datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    date = "N/A"
        
        # Команды
        blue_team = team_0_name  # Команда 0 — синяя сторона
        red_team = team_1_name   # Команда 1 — красная сторона
        
        # Баны и пики
        game_data = scrim_data.get("object", {}).get("games", [{}])[0]
        
        # Отладка: проверяем, что содержится в draftActions
        draft_actions = game_data.get("draftActions", [])
        debug_logs.append(f"Series {series_id} - draftActions: {json.dumps(draft_actions, indent=2)}")
        
        blue_bans = ["N/A"] * 5
        red_bans = ["N/A"] * 5
        blue_picks = ["N/A"] * 5
        red_picks = ["N/A"] * 5
        
        # Порядок драфт-фазы:
        # 1-6: ban blue, ban red, ban blue, ban red, ban blue, ban red
        # 7-12: pick blue, pick red, pick red, pick blue, pick blue, pick red
        # 13-16: ban red, ban blue, ban red, ban blue
        # 17-20: pick red, pick blue, pick blue, pick red
        blue_ban_idx = 0
        red_ban_idx = 0
        blue_pick_idx = 0
        red_pick_idx = 0
        
        for action in draft_actions:
            sequence = action.get("sequenceNumber")
            action_type = action.get("type")
            drafter_id = action.get("drafter", {}).get("id")
            champion = action.get("draftable", {}).get("name", "N/A")
            is_blue_team = drafter_id == teams[0].get("id")  # team0 — синяя сторона
            
            if action_type == "ban":
                if sequence in [1, 3, 5, 14, 16]:  # Баны синей команды
                    if blue_ban_idx < 5:
                        blue_bans[blue_ban_idx] = champion
                        blue_ban_idx += 1
                elif sequence in [2, 4, 6, 13, 15]:  # Баны красной команды
                    if red_ban_idx < 5:
                        red_bans[red_ban_idx] = champion
                        red_ban_idx += 1
            elif action_type == "pick":
                if sequence in [7, 10, 11, 18, 19]:  # Пики синей команды
                    if blue_pick_idx < 5:
                        blue_picks[blue_pick_idx] = champion
                        blue_pick_idx += 1
                elif sequence in [8, 9, 12, 17, 20]:  # Пики красной команды
                    if red_pick_idx < 5:
                        red_picks[red_pick_idx] = champion
                        red_pick_idx += 1
        
        # Длительность
        clock = game_data.get("clock", {})
        # Отладка: проверяем, что содержится в clock
        debug_logs.append(f"Series {series_id} - clock: {json.dumps(clock, indent=2)}")
        
        duration_seconds = clock.get("currentSeconds", "N/A")
        if isinstance(duration_seconds, (int, float)):
            duration = f"{int(duration_seconds // 60)}:{int(duration_seconds % 60):02d}"  # Переводим секунды в формат MM:SS
        else:
            duration = "N/A"
        
        # Победа или поражение
        win = teams[0].get("won", False) if team_0_name == TEAM_NAME else teams[1].get("won", False)
        result = "Win" if win else "Loss"
        
        # Формируем строку
        new_row = [
            date, match_id, blue_team, red_team,
            *blue_bans, *red_bans, *blue_picks, *red_picks,
            duration, result
        ]
        
        new_rows.append(new_row)
        existing_match_ids.add(match_id)
    
    debug_logs.append(f"Всего серий для Gamespace MC: {gamespace_series_count}")
    debug_logs.append(f"Пропущено дубликатов: {skipped_duplicates}")
    debug_logs.append(f"Новых строк для добавления: {len(new_rows)}")
    
    if new_rows:
        try:
            worksheet.append_rows(new_rows)
            debug_logs.append(f"Успешно добавлено {len(new_rows)} строк в Google Sheets.")
            return True
        except Exception as e:
            debug_logs.append(f"Ошибка при добавлении данных в Google Sheets: {str(e)}")
            return False
    return False

# Функция для агрегации данных из Google Sheets с фильтрацией по времени
def aggregate_scrims_data(worksheet, time_filter="All"):
    blue_side_stats = {"wins": 0, "losses": 0, "total": 0}
    red_side_stats = {"wins": 0, "losses": 0, "total": 0}
    match_history = []

    # Определяем временной диапазон
    current_date = datetime.utcnow()
    if time_filter == "1 Week":
        time_threshold = current_date - timedelta(weeks=1)
    elif time_filter == "2 Weeks":
        time_threshold = current_date - timedelta(weeks=2)
    elif time_filter == "3 Weeks":
        time_threshold = current_date - timedelta(weeks=3)
    elif time_filter == "4 Weeks":
        time_threshold = current_date - timedelta(weeks=4)
    elif time_filter == "2 Months":
        time_threshold = current_date - timedelta(days=60)
    else:
        time_threshold = None  # Без фильтра (All)

    data = worksheet.get_all_values()
    if len(data) <= 1:
        return blue_side_stats, red_side_stats, match_history

    for row in data[1:]:
        if len(row) < 23:  # Ожидаем 23 столбца
            continue
        
        date, match_id, blue_team, red_team, *_, duration, result = row
        
        # Фильтрация по времени
        if time_threshold:
            try:
                match_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                if match_date < time_threshold:
                    continue  # Пропускаем, если матч старше выбранного диапазона
            except ValueError:
                continue  # Пропускаем, если дата некорректна
        
        win = result == "Win"
        is_blue_side = blue_team == TEAM_NAME

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
            "Match ID": match_id,
            "Blue Team": blue_team,
            "Red Team": red_team,
            "Duration": duration,
            "Result": result
        })

    return blue_side_stats, red_side_stats, match_history

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
        wks.append_row([
            "Date", "Match ID", "Blue Team", "Red Team",
            "Blue Ban 1", "Blue Ban 2", "Blue Ban 3", "Blue Ban 4", "Blue Ban 5",
            "Red Ban 1", "Red Ban 2", "Red Ban 3", "Red Ban 4", "Red Ban 5",
            "Blue Pick 1", "Blue Pick 2", "Blue Pick 3", "Blue Pick 4", "Blue Pick 5",
            "Red Pick 1", "Red Pick 2", "Red Pick 3", "Red Pick 4", "Red Pick 5",
            "Duration", "Result"
        ])

    # Кнопка для загрузки всех серий
    debug_logs = []
    if st.button("Download All Scrims Data"):
        with st.spinner("Downloading scrims data from GRID API..."):
            series_list = get_all_series(debug_logs)
            if series_list:
                progress_bar = st.progress(0, text="Processing series 0/0")
                if update_scrims_data(wks, series_list, debug_logs, progress_bar):
                    st.success("Scrims data downloaded and updated!")
                else:
                    st.warning("No new data added (possibly duplicates or error).")
                progress_bar.empty()  # Убираем прогресс-бар после завершения
            else:
                st.warning("No series found for Gamespace MC.")

    # Отображаем отладочные сообщения
    if debug_logs:
        st.subheader("Debug Logs")
        for log in debug_logs:
            st.text(log)

    # Выпадающий список для фильтрации по времени
    time_filter = st.selectbox(
        "Filter by Time Range",
        ["All", "1 Week", "2 Weeks", "3 Weeks", "4 Weeks", "2 Months"]
    )

    # Агрегация и отображение с учётом фильтра
    blue_side_stats, red_side_stats, match_history = aggregate_scrims_data(wks, time_filter)
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
        st.write("No match history available for the selected time range.")

    # CSS для тёмной темы
    st.markdown("""
        <style>
        table { 
            width: 100%; 
            border-collapse: collapse; 
            margin: 10px 0; 
            background-color: #1e1e1e; 
            color: #ffffff; 
        }
        th, td { 
            padding: 8px; 
            text-align: left; 
            border-bottom: 1px solid #444444; 
        }
        th { 
            background-color: #333333; 
        }
        tr:hover { 
            background-color: #2a2a2a; 
        }
        </style>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    scrims_page()
