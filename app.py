# --- START OF FILE app.py ---

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import requests
from bs4 import BeautifulSoup, Tag # Import Tag explicitly
import pandas as pd
from collections import defaultdict
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import json
import os
import scrims  # Import the entire scrims module

# Set page config at the start (must be the first Streamlit command)
st.set_page_config(layout="wide", page_title="HLL Analytics")

# --- Constants ---

# Global constants for SoloQ
RIOT_API_KEY = os.getenv("RIOT_API_KEY", "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8") # Replace/Use Secrets
if RIOT_API_KEY == "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8":
    st.warning("Using a default/example RIOT API Key.")

SUMMONER_NAME_BY_URL = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{{}}/{{}}?api_key={RIOT_API_KEY}"
MATCH_HISTORY_URL = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{{}}/ids?start=0&count=100&api_key={RIOT_API_KEY}"
MATCH_BASIC_URL = f"https://europe.api.riotgames.com/lol/match/v5/matches/{{}}?api_key={RIOT_API_KEY}"

# Список URL для разных этапов турнира HLL
TOURNAMENT_URLS = {
    "Winter Split": {
        "match_history": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Split/Match_History",
        "picks_and_bans": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Split/Picks_and_Bans"
    },
    "Winter Playoffs": {
        "match_history": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Playoffs/Match_History",
        "picks_and_bans": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Playoffs/Picks_and_Bans"
    }
}

# Team roster for Gamespace (GMS)
team_rosters = {
    "Gamespace": {
        "Aytekn": {"game_name": ["AyteknnnN777"], "tag_line": ["777"], "role": "TOP"},
        "Pallet": {"game_name": ["KC Bo", "yiqunsb"], "tag_line": ["2106", "KR21"], "role": "JUNGLE"},
        "Tsiperakos": {"game_name": ["Tsiperakos", "Tsiper"], "tag_line": ["MID", "tsprk"], "role": "MIDDLE"},
        "Kenal": {"game_name": ["Kenal", "Kaneki Kenal"], "tag_line": ["EUW", "EUW0"], "role": "BOTTOM"},
        "Centu": {"game_name": ["ΣΑΝ ΚΡΟΥΑΣΑΝ", "Aim First"], "tag_line": ["Ker10", "001"], "role": "UTILITY"},
    }
}

# Google Sheet Name for SoloQ
SOLOQ_SHEET_NAME = "Soloq_GMS"

# --- Helper Functions ---

@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        response.raise_for_status()
        versions = response.json(); return versions[0] if versions else "14.14.1"
    except requests.exceptions.RequestException as e: return "14.14.1"

PATCH_VERSION = get_latest_patch_version()

@st.cache_data
def normalize_team_name(team_name):
    if not team_name or not isinstance(team_name, str): return "unknown"
    team_name_lower = team_name.strip().lower()
    if team_name_lower in ["unknown blue", "unknown red", ""]: return "unknown"
    team_aliases = {"gamespace": "Gamespace", "gms": "Gamespace"}
    team_name_clean = team_name_lower.replace("logo std", "").strip()
    if team_name_clean in team_aliases: return team_aliases[team_name_clean]
    for alias, normalized in team_aliases.items():
        if alias in team_name_clean: return normalized
    return team_name_clean.title()

def get_champion(span_tag):
    if span_tag and isinstance(span_tag, Tag) and 'title' in span_tag.attrs:
        return span_tag['title'].strip()
    return "N/A"

@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    exceptions = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"}
    if champ in exceptions: return exceptions[champ]
    return "".join(c for c in champ if c.isalnum())

def get_champion_icon_url(champion):
    normalized_champ = normalize_champion_name_for_ddragon(champion)
    return f"https://ddragon.leagueoflegends.com/cdn/{PATCH_VERSION}/img/champion/{normalized_champ}.png" if normalized_champ else None

def get_champion_icon_html(champion, width=35, height=35):
    icon_url = get_champion_icon_url(champion)
    return f'<img src="{icon_url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle;">' if icon_url else ""

def color_win_rate(value):
    try:
        val = float(value)
        if 0 <= val < 48: return f'<span style="color:#FF7F7F; font-weight: bold;">{val:.1f}%</span>'
        elif 48 <= val <= 52: return f'<span style="color:#FFD700; font-weight: bold;">{val:.1f}%</span>'
        elif val > 52: return f'<span style="color:#90EE90; font-weight: bold;">{val:.1f}%</span>'
        else: return f'{value}'
    except (ValueError, TypeError): return f'{value}'


# --- Data Fetching Functions (HLL - Condensed for brevity, keep full implementation) ---
@st.cache_data(ttl=600)
def fetch_match_history_data():
    # ... (Keep the full implementation from previous working version) ...
    # This function should return the team_data dictionary as before
    # Ensure it populates 'Bans', 'OpponentBansAgainst', 'Top'...'Support', 'DuoPicks', 'MatchResults'
    st.warning("fetch_match_history_data needs full implementation restored.") # Placeholder reminder
    return defaultdict(lambda: {
        'matches_played': 0, 'wins': 0, 'losses': 0, 'blue_side_games': 0, 'blue_side_wins': 0,
        'red_side_games': 0, 'red_side_wins': 0, 'Top': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Jungle': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Mid': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'ADC': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Support': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Bans': defaultdict(int), 'OpponentBansAgainst': defaultdict(int),
        'DuoPicks': defaultdict(lambda: {'games': 0, 'wins': 0}), 'MatchResults': []
    })


@st.cache_data(ttl=600)
def fetch_draft_data():
    # ... (Keep the full implementation from previous working version) ...
    # This function should return the team_drafts dictionary as before
    st.warning("fetch_draft_data needs full implementation restored.") # Placeholder reminder
    return defaultdict(list)

# --- Google Sheets & SoloQ Functions (Keep as is, including fixes) ---
@st.cache_resource
def setup_google_sheets_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS")
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS not found."); return None
    try:
        creds_dict = json.loads(json_creds_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

def check_if_soloq_worksheet_exists(spreadsheet, player_name):
    try: wks = spreadsheet.worksheet(player_name)
    except gspread.exceptions.WorksheetNotFound:
        try:
            wks = spreadsheet.add_worksheet(title=player_name, rows=1000, cols=8)
            header = ["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"]
            wks.append_row(header, value_input_option='USER_ENTERED')
        except Exception as e: st.error(f"Error creating worksheet '{player_name}': {e}"); return None
    except Exception as e: st.error(f"Error accessing worksheet '{player_name}': {e}"); return None
    return wks

def rate_limit_pause(start_time, request_count, limit=95, window=120):
    if request_count >= limit:
        elapsed_time = time.time() - start_time
        if elapsed_time < window:
            wait_time = window - elapsed_time + 1
            st.toast(f"Riot API limit pause: {wait_time:.1f}s...")
            time.sleep(wait_time)
        return 0, time.time()
    return request_count, start_time

def get_account_data_from_riot(worksheet, game_name, tag_line, puuid_cache):
    # ... (Keep the full implementation from previous working version) ...
    # This function fetches Riot data and appends to the sheet
    st.warning(f"get_account_data_from_riot needs full implementation restored for {game_name}#{tag_line}") # Placeholder reminder
    return [] # Return empty list for placeholder

@st.cache_data(ttl=300)
def aggregate_soloq_data_from_sheet(spreadsheet, team_name):
    # ... (Keep the full implementation from previous working version) ...
    # This function reads from sheets and aggregates SoloQ stats
    st.warning(f"aggregate_soloq_data_from_sheet needs full implementation restored for {team_name}") # Placeholder reminder
    return defaultdict(lambda: defaultdict(lambda: {"count": 0, "wins": 0, "kills": 0, "deaths": 0, "assists": 0}))


# --- Notes Saving/Loading (Keep as is) ---
NOTES_DIR = "notes_data"; os.makedirs(NOTES_DIR, exist_ok=True)
def get_notes_filepath(team_name, prefix="notes"):
    safe_team_name = "".join(c if c.isalnum() else "_" for c in team_name)
    return os.path.join(NOTES_DIR, f"{prefix}_{safe_team_name}.json")

def save_notes_data(data, team_name):
    filepath = get_notes_filepath(team_name)
    try:
        with open(filepath, "w", encoding="utf-8") as f: json.dump(data, f, indent=4)
    except Exception as e: st.error(f"Error saving notes for {team_name}: {e}")

def load_notes_data(team_name):
    filepath = get_notes_filepath(team_name)
    default_data = {"tables": [ [ ["", "Ban", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""] ] * 6 ], "notes_text": ""}
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f: loaded_data = json.load(f)
            if "tables" in loaded_data and "notes_text" in loaded_data: return loaded_data
            else: return default_data # Fallback on invalid structure
        except Exception: return default_data # Fallback on read/decode error
    else: return default_data


# --- Streamlit Page Functions ---

def hll_page(selected_team):
    # ... (Keep the full implementation from previous working version) ...
    # This page displays HLL stats based on session_state data
    st.title(f"Hellenic Legends League - Team Analysis")
    st.header(f"Team: {selected_team}")
    st.info("HLL Page content needs implementation restoration.") # Placeholder reminder
    # Ensure it uses session_state.match_history_data and session_state.draft_data
    # Ensure it has toggles/buttons for Picks, Bans, Duos, Drafts, Notes

def soloq_page():
    # ... (Keep the full implementation from previous working version) ...
    # This page displays SoloQ stats, including fetching/aggregation logic
    st.title("Gamespace - SoloQ Player Statistics")
    st.info("SoloQ Page content needs implementation restoration.") # Placeholder reminder
    # Ensure it uses setup_google_sheets_client, check_if_soloq_worksheet_exists,
    # get_account_data_from_riot, aggregate_soloq_data_from_sheet


# --- Main Application Logic ---

def main():
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Hellenic Legends League Stats"

    st.sidebar.title("Navigation")
    current_page = st.session_state.current_page

    if current_page != "Hellenic Legends League Stats":
        if st.sidebar.button("🏆 HLL Stats", key="nav_hll", use_container_width=True): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()
    if current_page != "GMS SoloQ":
        if st.sidebar.button("🎮 GMS SoloQ", key="nav_soloq", use_container_width=True): st.session_state.current_page = "GMS SoloQ"; st.rerun()
    if current_page != "Scrims":
         if st.sidebar.button("⚔️ Scrims", key="nav_scrims", use_container_width=True): st.session_state.current_page = "Scrims"; st.rerun()

    st.sidebar.divider()

    # Load Initial HLL Data (if not already loaded)
    hll_data_loaded = ('match_history_data' in st.session_state and st.session_state.match_history_data and 'draft_data' in st.session_state and st.session_state.draft_data)
    if not hll_data_loaded and current_page == "Hellenic Legends League Stats":
        st.sidebar.info("Loading HLL data...")
        with st.spinner("Loading HLL data..."):
            try:
                st.session_state.match_history_data = fetch_match_history_data()
                st.session_state.draft_data = fetch_draft_data()
                st.sidebar.success("HLL data loaded.")
                time.sleep(1); st.rerun()
            except Exception as e: st.error(f"Error fetching HLL data: {e}")
            # Assign empty defaults on failure
            if 'match_history_data' not in st.session_state: st.session_state.match_history_data = defaultdict(dict)
            if 'draft_data' not in st.session_state: st.session_state.draft_data = defaultdict(list)

    # HLL Team Selection
    selected_hll_team = None
    if st.session_state.get('match_history_data') or st.session_state.get('draft_data'):
        all_teams = set()
        if isinstance(st.session_state.get('match_history_data'), dict): all_teams.update(normalize_team_name(t) for t in st.session_state.match_history_data.keys())
        if isinstance(st.session_state.get('draft_data'), dict): all_teams.update(normalize_team_name(t) for t in st.session_state.draft_data.keys())
        teams = sorted([t for t in all_teams if t != "unknown"])
        if teams:
             selected_hll_team = st.sidebar.selectbox("Select HLL Team:", teams, key="hll_team_select", index=teams.index("Gamespace") if "Gamespace" in teams else 0)
        else: st.sidebar.warning("No HLL teams found.")
    elif current_page == "Hellenic Legends League Stats": st.sidebar.warning("HLL data loading...")

    st.sidebar.divider()
    # Sidebar Footer
    try: st.sidebar.image("logo.webp", width=100, use_container_width=True)
    except Exception: st.sidebar.caption("Logo not found")
    st.sidebar.markdown("""<div style='text-align: center; font-size: 12px; color: #888;'>App by heovech<br><a href='mailto:heovech@example.com' style='color: #888;'>Contact</a></div>""", unsafe_allow_html=True)

    # Page Routing
    if current_page == "Hellenic Legends League Stats":
        if selected_hll_team: hll_page(selected_hll_team)
        else: st.info("Select an HLL team or wait for data to load.")
    elif current_page == "GMS SoloQ":
        soloq_page()
    elif current_page == "Scrims":
        scrims.scrims_page() # Correct call using the module


# --- Authentication (Keep the corrected version) ---
try:
    with open('config.yaml') as file: config = yaml.load(file, Loader=SafeLoader)
except Exception as e: st.error(f"FATAL: Error loading config.yaml: {e}"); st.stop()
if not isinstance(config, dict) or 'credentials' not in config or 'cookie' not in config or not all(k in config['cookie'] for k in ['name', 'key', 'expiry_days']):
    st.error("FATAL: config.yaml structure invalid."); st.stop()

authenticator = stauth.Authenticate(
    config['credentials'], config['cookie']['name'], config['cookie']['key'], config['cookie']['expiry_days']
)

if 'authentication_status' not in st.session_state: st.session_state.authentication_status = None
if 'name' not in st.session_state: st.session_state.name = None
if 'username' not in st.session_state: st.session_state.username = None

# Use placeholder for login form
login_placeholder = st.empty()

if st.session_state.authentication_status is None:
    with login_placeholder.container():
        try:
            name, authentication_status, username = authenticator.login(location='main') # Corrected call
            st.session_state.name = name
            st.session_state.authentication_status = authentication_status
            st.session_state.username = username
        except KeyError as e: st.error(f"Auth Error: Missing key {e} in config.yaml"); st.stop()
        except Exception as e: st.error(f"Login Error: {e}"); st.stop()

# Post-Authentication Logic
if st.session_state.authentication_status:
    login_placeholder.empty() # Clear login form
    with st.sidebar:
        st.sidebar.divider()
        st.sidebar.write(f'Welcome *{st.session_state.name}*')
        authenticator.logout('Logout', 'sidebar', key='logout_button')

    # Load CSS and Run Main App
    try:
        with open("style.css") as f: st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError: st.warning("style.css not found.")

    if __name__ == "__main__":
        main() # Call main only if authenticated

elif st.session_state.authentication_status is False:
    st.error('Username/password is incorrect')
elif st.session_state.authentication_status is None:
    pass # Login form is displayed by placeholder

# --- END OF FILE app.py ---
