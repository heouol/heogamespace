

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import requests
from bs4 import BeautifulSoup
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
# Consider using Streamlit secrets for API keys
RIOT_API_KEY = os.getenv("RIOT_API_KEY", "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8") # Replace with your actual key or load from env/secrets
if RIOT_API_KEY == "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8":
    st.warning("Using a default/example RIOT API Key. Please replace it with your own key or set the RIOT_API_KEY environment variable/secret.")

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
    # Add more tournaments/stages as needed
}

# Team roster for Gamespace (GMS) - Ensure game names and tag lines are accurate
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

# Get the latest patch version from Data Dragon
@st.cache_data(ttl=3600) # Cache for 1 hour
def get_latest_patch_version():
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        response.raise_for_status() # Raise an exception for bad status codes
        versions = response.json()
        if versions:
            return versions[0]
        st.warning("Could not determine latest patch version from Data Dragon, using default.")
        return "14.5.1" # Default patch if API fails or returns empty list
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching patch version: {e}. Using default.")
        return "14.5.1" # Default patch on error

PATCH_VERSION = get_latest_patch_version()

# Normalize team names (case-insensitive, handles common variations)
@st.cache_data
def normalize_team_name(team_name):
    if not team_name or not isinstance(team_name, str):
        return "unknown"

    team_name_lower = team_name.strip().lower()

    if team_name_lower in ["unknown blue", "unknown red", ""]:
        return "unknown"

    # Specific known aliases for teams
    team_aliases = {
        "gamespace": "Gamespace",
        "gms": "Gamespace",
        # Add other known aliases for HLL teams here
        # "team pepega": "Team Pepega",
        # "tp": "Team Pepega",
    }

    # Remove common suffixes like 'logo std'
    team_name_clean = team_name_lower.replace("logo std", "").strip()

    # Check aliases first
    if team_name_clean in team_aliases:
        return team_aliases[team_name_clean]

    # General check if an alias is *part* of the cleaned name (e.g., "gamespace team")
    for alias, normalized in team_aliases.items():
        if alias in team_name_clean:
            return normalized

    # If no alias matches, return the cleaned name, capitalized
    return team_name_clean.title()


# Helper to extract champion name from sprite span
def get_champion(span_tag):
    if span_tag and isinstance(span_tag, Tag) and 'title' in span_tag.attrs:
        return span_tag['title'].strip()
    return "N/A" # Return consistent "N/A"

# Helper function to normalize champion names for Data Dragon URLs
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A":
        return None # Return None if no champion or N/A

    # Common exceptions where Data Dragon name differs significantly
    champion_exceptions = {
        "Nunu & Willump": "Nunu", # API uses NunuWillump, but icon uses Nunu
        "Wukong": "MonkeyKing",
        "Renata Glasc": "Renata",
        "K'Sante": "KSante",
    }
    if champ in champion_exceptions:
        return champion_exceptions[champ]

    # General replacements for characters not in Data Dragon names
    champ_normalized = champ.replace(" ", "").replace("'", "").replace(".", "").replace("&", "").replace("-", "")
    # Capitalize first letter, rest lower (usually works, but check edge cases)
    # A more robust way might involve checking against the actual champion list from DDRagon if needed
    # return champ_normalized[0].upper() + champ_normalized[1:].lower() if len(champ_normalized)>1 else champ_normalized.upper()
    return champ_normalized # Data Dragon seems to use PascalCase without spaces/special chars mostly


# Helper to get champion icon URL
def get_champion_icon_url(champion):
    if champion == "N/A":
        return None
    normalized_champ = normalize_champion_name_for_ddragon(champion)
    if normalized_champ:
        return f"https://ddragon.leagueoflegends.com/cdn/{PATCH_VERSION}/img/champion/{normalized_champ}.png"
    return None

# Helper to generate HTML for champion icon
def get_champion_icon_html(champion, width=35, height=35):
    icon_url = get_champion_icon_url(champion)
    if icon_url:
        # Added alt text for accessibility and title for hover info
        return f'<img src="{icon_url}" width="{width}" height="{height}" alt="{champion}" title="{champion}" style="vertical-align: middle;">'
    return "" # Return empty string if no icon

# Color win rate for display in tables
def color_win_rate(value):
    try:
        val = float(value)
        if 0 <= val < 48: # Slightly adjusted threshold
            # Lighter red for dark themes
            return f'<span style="color:#FF7F7F; font-weight: bold;">{val:.1f}%</span>'
        elif 48 <= val <= 52:
             # Yellowish/Orange
            return f'<span style="color:#FFD700; font-weight: bold;">{val:.1f}%</span>'
        elif val > 52:
             # Lighter green for dark themes
            return f'<span style="color:#90EE90; font-weight: bold;">{val:.1f}%</span>'
        else: # Handle NaN or unexpected values
             return f'{value}' # Display as is
    except (ValueError, TypeError):
        return f'{value}' # Display non-numeric as is


# --- Data Fetching Functions (HLL) ---

# Fetch match history data from Leaguepedia
@st.cache_data(ttl=600) # Cache for 10 minutes
def fetch_match_history_data():
    # Use a more specific User-Agent if possible
    headers = {'User-Agent': 'HLLAnalyticsApp/1.0 (Contact: your-email@example.com)'}
    team_data = defaultdict(lambda: {
        'matches_played': 0,
        'wins': 0,
        'losses': 0,
        'blue_side_games': 0,
        'blue_side_wins': 0,
        'red_side_games': 0,
        'red_side_wins': 0,
        'Top': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Jungle': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Mid': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'ADC': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Support': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Bans': defaultdict(int), # Bans made BY this team
        'OpponentBansAgainst': defaultdict(int), # Bans made BY OPPONENT against this team
        'DuoPicks': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'MatchResults': [] # Detailed list of matches
    })
    all_match_details = [] # Store raw details for joining later if needed

    from bs4 import Tag # Ensure Tag is imported if used for type checking

    # Roles order for pick extraction
    roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls.get("match_history")
        if not url:
            st.warning(f"Match history URL missing for {tournament_name}")
            continue

        try:
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status() # Check for HTTP errors
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to load {tournament_name} Match History page: {e}")
            continue # Skip this tournament if page load fails

        soup = BeautifulSoup(response.content, 'html.parser')
        match_history_tables = soup.select('.wikitable.mhgame.sortable') # Can be multiple tables
        if not match_history_tables:
            st.warning(f"Could not find match history table(s) for {tournament_name}")
            continue

        for match_history_table in match_history_tables:
            rows = match_history_table.select('tr')
            if len(rows) < 2: continue # Skip empty tables or header-only tables

            for row in rows[1:]: # Skip header row
                cols = row.select('td')
                if not cols or len(cols) < 9: # Expect at least 9 columns for basic data
                    # st.warning(f"Skipping row in {tournament_name} due to insufficient columns: {row.text[:50]}...")
                    continue

                # --- Extract Teams ---
                # Use more robust extraction, checking for 'a' tag and 'title'
                blue_team_cell = cols[2]
                red_team_cell = cols[3]

                blue_team_link = blue_team_cell.select_one('a[title]')
                red_team_link = red_team_cell.select_one('a[title]')

                # Prefer title attribute if link exists, otherwise fallback to text
                blue_team_raw = blue_team_link['title'].strip() if blue_team_link else blue_team_cell.get_text(strip=True)
                red_team_raw = red_team_link['title'].strip() if red_team_link else red_team_cell.get_text(strip=True)

                # Normalize names
                blue_team = normalize_team_name(blue_team_raw)
                red_team = normalize_team_name(red_team_raw)

                # Skip if normalization failed or resulted in 'unknown'
                if blue_team == "unknown" or red_team == "unknown":
                    # st.warning(f"Skipping row due to unknown teams: Blue='{blue_team_raw}', Red='{red_team_raw}'")
                    continue

                # --- Extract Winner ---
                # Winner can be indicated by text "1:0" or "0:1", or by a winning team link
                result_cell = cols[4]
                result_text = result_cell.get_text(strip=True)
                winner_link = result_cell.select_one('a[title]')

                winner_team = "unknown"
                if winner_link:
                    winner_team = normalize_team_name(winner_link['title'].strip())
                elif result_text == "1:0":
                    winner_team = blue_team
                elif result_text == "0:1":
                    winner_team = red_team
                # else: winner remains "unknown" - might happen for ongoing/invalid games

                # Determine win/loss for each team
                result_blue = 'Win' if winner_team == blue_team else 'Loss' if winner_team != "unknown" else 'N/A'
                result_red = 'Win' if winner_team == red_team else 'Loss' if winner_team != "unknown" else 'N/A'

                # Skip if result is unknown
                if result_blue == 'N/A':
                     # st.warning(f"Skipping row due to unknown result: {blue_team} vs {red_team}")
                     continue

                 # --- Update Team Stats ---
                team_data[blue_team]['matches_played'] += 1
                team_data[red_team]['matches_played'] += 1
                team_data[blue_team]['blue_side_games'] += 1
                team_data[red_team]['red_side_games'] += 1

                if result_blue == 'Win':
                    team_data[blue_team]['wins'] += 1
                    team_data[blue_team]['blue_side_wins'] += 1
                    team_data[red_team]['losses'] += 1
                else: # Blue Loss
                    team_data[blue_team]['losses'] += 1
                    team_data[red_team]['wins'] += 1
                    team_data[red_team]['red_side_wins'] += 1


                # --- Extract Bans ---
                # Columns 5 (Blue Bans) and 6 (Red Bans)
                blue_bans_elems = cols[5].select('span.sprite.champion-sprite') if len(cols) > 5 else []
                red_bans_elems = cols[6].select('span.sprite.champion-sprite') if len(cols) > 6 else [] # Note: Sometimes class is just champion-sprite

                blue_bans = [get_champion(ban) for ban in blue_bans_elems]
                red_bans = [get_champion(ban) for ban in red_bans_elems]

                for champ in blue_bans:
                    if champ != "N/A":
                        team_data[blue_team]['Bans'][champ] += 1
                        team_data[red_team]['OpponentBansAgainst'][champ] += 1
                for champ in red_bans:
                    if champ != "N/A":
                        team_data[red_team]['Bans'][champ] += 1
                        team_data[blue_team]['OpponentBansAgainst'][champ] += 1

                # --- Extract Picks ---
                # Columns 7 (Blue Picks) and 8 (Red Picks)
                blue_picks_elems = cols[7].select('span.sprite.champion-sprite') if len(cols) > 7 else []
                red_picks_elems = cols[8].select('span.sprite.champion-sprite') if len(cols) > 8 else []

                # Extract picks based on assumed role order in the table
                blue_picks = {role: get_champion(pick) for role, pick in zip(roles, blue_picks_elems)}
                red_picks = {role: get_champion(pick) for role, pick in zip(roles, red_picks_elems)}

                # Update pick stats for blue team
                for role, champion in blue_picks.items():
                    if champion and champion != "N/A":
                        team_data[blue_team][role][champion]['games'] += 1
                        if result_blue == 'Win':
                            team_data[blue_team][role][champion]['wins'] += 1
                    # else: Handle cases where pick might be missing for a role if needed

                # Update pick stats for red team
                for role, champion in red_picks.items():
                    if champion and champion != "N/A":
                        team_data[red_team][role][champion]['games'] += 1
                        if result_red == 'Win':
                            team_data[red_team][role][champion]['wins'] += 1
                    # else: Handle cases where pick might be missing for a role if needed

                # --- Update Duo Pick Stats ---
                # Define relevant duo combinations
                duo_pairs = [('Top', 'Jungle'), ('Jungle', 'Mid'), ('Mid', 'ADC'), ('ADC', 'Support'), ('Jungle', 'Support')] # Added Mid/ADC, Jg/Sup

                for team, picks, result in [(blue_team, blue_picks, result_blue), (red_team, red_picks, result_red)]:
                    for role1, role2 in duo_pairs:
                        champ1 = picks.get(role1, "N/A")
                        champ2 = picks.get(role2, "N/A")
                        # Only record if both picks are valid champions
                        if champ1 != "N/A" and champ2 != "N/A":
                            # Store duo key consistently (e.g., sorted alphabetically) to avoid duplicates like (A,B) and (B,A)
                            duo_key = tuple(sorted([(champ1, role1), (champ2, role2)]))
                            team_data[team]['DuoPicks'][duo_key]['games'] += 1
                            if result == 'Win':
                                team_data[team]['DuoPicks'][duo_key]['wins'] += 1

                # --- Store Match Result Details ---
                # Use a unique identifier if available (e.g., VOD link or game hash)
                # For now, using teams + tournament + sequential counter (less reliable for exact match linking)
                match_identifier = f"{tournament_name}_{blue_team}_vs_{red_team}_{team_data[blue_team]['matches_played']}"

                team_data[blue_team]['MatchResults'].append({
                    'match_id': match_identifier,
                    'opponent': red_team,
                    'side': 'blue',
                    'result': result_blue,
                    'tournament': tournament_name,
                    'blue_picks': blue_picks,
                    'red_picks': red_picks,
                    'blue_bans': blue_bans,
                    'red_bans': red_bans,
                })
                team_data[red_team]['MatchResults'].append({
                    'match_id': match_identifier,
                    'opponent': blue_team,
                    'side': 'red',
                    'result': result_red,
                    'tournament': tournament_name,
                    'blue_picks': blue_picks,
                    'red_picks': red_picks,
                    'blue_bans': blue_bans,
                    'red_bans': red_bans,
                })

    # Convert defaultdicts back to regular dicts for cleaner output if needed
    # return json.loads(json.dumps(team_data)) # Easy way to convert nested defaultdicts
    return dict(team_data)


# Fetch detailed draft data (including order) from Picks & Bans pages
@st.cache_data(ttl=600) # Cache for 10 minutes
def fetch_draft_data():
    headers = {'User-Agent': 'HLLAnalyticsApp/1.0 (Contact: your-email@example.com)'}
    team_drafts = defaultdict(list) # Store list of drafts per team
    # Keep track of matches within a tournament to assign game numbers
    match_counter = defaultdict(lambda: defaultdict(int)) # { tournament: { match_key: count } }

    from bs4 import Tag # Ensure Tag is imported

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls.get("picks_and_bans")
        if not url:
            st.warning(f"Picks and Bans URL missing for {tournament_name}")
            continue

        try:
            response = requests.get(url, headers=headers, timeout=20)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to load {tournament_name} Picks and Bans page: {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        # Target the specific table structure used on these pages
        draft_tables = soup.select('table.wikitable.plainlinks.hoverable-rows.column-show-hide-1')
        if not draft_tables:
            st.warning(f"Draft tables not found on the {tournament_name} page.")
            continue

        for table in draft_tables:
            rows = table.select('tr')
            if len(rows) < 2: continue # Skip header

            for row in rows[1:]:
                cols = row.select('td')
                # Expect a large number of columns for full draft details
                if len(cols) < 24:
                    # st.warning(f"Skipping draft row in {tournament_name} due to insufficient columns: {row.text[:50]}...")
                    continue

                # --- Extract Teams and Winner ---
                # Team names are usually in cols[1] (Blue) and cols[2] (Red)
                # Winner is often indicated by a 'pbh-winner' class on the team cell
                blue_team_cell = cols[1]
                red_team_cell = cols[2]

                # Try extracting from title attribute first
                blue_team_link = blue_team_cell.select_one('a[title], span[title]') # Sometimes it's a span
                red_team_link = red_team_cell.select_one('a[title], span[title]')

                blue_team_raw = blue_team_link['title'].strip() if blue_team_link else blue_team_cell.get_text(strip=True)
                red_team_raw = red_team_link['title'].strip() if red_team_link else red_team_cell.get_text(strip=True)

                blue_team = normalize_team_name(blue_team_raw)
                red_team = normalize_team_name(red_team_raw)

                if blue_team == "unknown" or red_team == "unknown":
                    # st.warning(f"Skipping draft row due to unknown teams: Blue='{blue_team_raw}', Red='{red_team_raw}'")
                    continue

                # Determine winner based on class
                winner_side = None
                if 'pbh-winner' in blue_team_cell.get('class', []):
                    winner_side = 'blue'
                elif 'pbh-winner' in red_team_cell.get('class', []):
                    winner_side = 'red'
                # else: Winner unknown or draw (unlikely in LoL)

                 # Skip if winner unknown (might be incomplete data)
                if winner_side is None:
                     # st.warning(f"Skipping draft row due to unknown winner: {blue_team} vs {red_team}")
                     continue

                # --- Assign Match Number ---
                match_key = tuple(sorted([blue_team, red_team]))
                match_counter[tournament_name][match_key] += 1
                match_number = match_counter[tournament_name][match_key]

                # --- Extract Draft Order (Bans and Picks) ---
                draft_actions = [] # List to store tuples: (action_type, side, champion)

                # Ban Phase 1 (BB1, RB1, BB2, RB2, BB3, RB3) - Cols 5 to 10
                ban_indices_p1 = range(5, 11)
                for i, col_idx in enumerate(ban_indices_p1):
                    side = 'blue' if i % 2 == 0 else 'red'
                    champ_span = cols[col_idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]') # Find span with title
                    champion = champ_span['title'].strip() if champ_span else "N/A"
                    draft_actions.append({'type': 'ban', 'phase': 1, 'side': side, 'champion': champion})

                # Pick Phase 1 (BP1, RP1, RP2, BP2, BP3, RP3) - Cols 11 to 14
                pick_order_p1 = [
                    (11, 0, 'blue'), (12, 0, 'red'), (12, 1, 'red'),
                    (13, 0, 'blue'), (13, 1, 'blue'), (14, 0, 'red')
                ]
                for col_idx, span_idx, side in pick_order_p1:
                    pick_spans = cols[col_idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                    champion = pick_spans[span_idx]['title'].strip() if len(pick_spans) > span_idx else "N/A"
                    draft_actions.append({'type': 'pick', 'phase': 1, 'side': side, 'champion': champion})

                # Ban Phase 2 (RB4, BB4, RB5, BB5) - Cols 15 to 18
                ban_indices_p2 = range(15, 19)
                for i, col_idx in enumerate(ban_indices_p2):
                    side = 'red' if i % 2 == 0 else 'blue' # Order is R, B, R, B
                    champ_span = cols[col_idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                    champion = champ_span['title'].strip() if champ_span else "N/A"
                    draft_actions.append({'type': 'ban', 'phase': 2, 'side': side, 'champion': champion})

                 # Pick Phase 2 (RP4, BP4, BP5, RP5) - Cols 19 to 21
                pick_order_p2 = [
                    (19, 0, 'red'), (20, 0, 'blue'), (20, 1, 'blue'), (21, 0, 'red')
                ]
                for col_idx, span_idx, side in pick_order_p2:
                     pick_spans = cols[col_idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                     champion = pick_spans[span_idx]['title'].strip() if len(pick_spans) > span_idx else "N/A"
                     draft_actions.append({'type': 'pick', 'phase': 2, 'side': side, 'champion': champion})

                # --- Extract VOD Link ---
                vod_link = "N/A"
                vod_cell = cols[23] # Assuming VOD is in the 24th column (index 23)
                vod_elem = vod_cell.select_one('a[href]')
                if vod_elem:
                    vod_link = vod_elem['href']


                # --- Store Draft Info ---
                draft_info = {
                    'tournament': tournament_name,
                    'match_key': match_key, # Tuple (teamA, teamB) sorted
                    'match_number': match_number, # Game number in the series
                    'blue_team': blue_team,
                    'red_team': red_team,
                    'winner_side': winner_side,
                    'draft_actions': draft_actions, # List of actions in order
                    'vod_link': vod_link
                }

                # Append the draft to both teams involved
                team_drafts[blue_team].append(draft_info)
                team_drafts[red_team].append(draft_info)

    return dict(team_drafts)


# --- Google Sheets & SoloQ Functions ---

# Setup Google Sheets client (requires GOOGLE_SHEETS_CREDS env var)
@st.cache_resource # Cache the client resource
def setup_google_sheets_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS")
    if not json_creds_str:
        st.error("Google Sheets credentials (GOOGLE_SHEETS_CREDS) not found in environment variables/secrets.")
        return None
    try:
        creds_dict = json.loads(json_creds_str)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        # Perform a simple test operation to check credentials
        client.list_spreadsheet_files()
        return client
    except json.JSONDecodeError:
        st.error("Error decoding Google Sheets JSON credentials. Check the format.")
        return None
    except gspread.exceptions.APIError as e:
         st.error(f"Google Sheets API Error during setup: {e}. Check credentials/permissions.")
         return None
    except Exception as e:
        st.error(f"Unexpected error setting up Google Sheets: {e}")
        return None

# Check if worksheet exists, create if not (specific for SoloQ sheet structure)
def check_if_soloq_worksheet_exists(spreadsheet, player_name):
    try:
        wks = spreadsheet.worksheet(player_name)
        # Optional: Check header row if worksheet exists
        header = wks.row_values(1)
        expected_header = ["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"]
        if header != expected_header:
             st.warning(f"Worksheet '{player_name}' exists but header is incorrect. Re-creating or manual fix needed.")
             # Decide on action: recreate, update header, or just warn
             # For now, just warning. Consider adding wks.update('A1:H1', [expected_header])
    except gspread.exceptions.WorksheetNotFound:
        try:
            st.info(f"Worksheet for '{player_name}' not found, creating...")
            # SoloQ sheets have 8 columns as defined in aggregate_soloq_data
            wks = spreadsheet.add_worksheet(title=player_name, rows=1000, cols=8)
             # Add header row immediately upon creation
            expected_header = ["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"]
            wks.append_row(expected_header, value_input_option='USER_ENTERED')
            st.info(f"Worksheet '{player_name}' created with header.")
        except gspread.exceptions.APIError as e:
             st.error(f"API Error creating worksheet '{player_name}': {e}")
             return None # Indicate error
    except gspread.exceptions.APIError as e:
        st.error(f"API Error checking/accessing worksheet '{player_name}': {e}")
        return None # Indicate error
    return wks


# Rate limiting helper for Riot API
def rate_limit_pause(start_time, request_count, limit=95, window=120):
    """Pauses execution if Riot API rate limit is approached."""
    # Use a slightly lower limit (e.g., 95) to be safe
    if request_count >= limit:
        elapsed_time = time.time() - start_time
        if elapsed_time < window:
            wait_time = window - elapsed_time + 1 # Add a small buffer
            st.warning(f"Approaching Riot API rate limit. Pausing for {wait_time:.1f} seconds...")
            time.sleep(wait_time)
        # Reset counter and timer after waiting or if window passed
        return 0, time.time()
    return request_count, start_time


# Get SoloQ match data for a player account from Riot API
def get_account_data_from_riot(worksheet, game_name, tag_line, puuid_cache):
    """Fetches recent matches for a player and adds new ones to the worksheet."""
    if not worksheet:
         st.error(f"Invalid worksheet provided for {game_name}#{tag_line}.")
         return [] # Return empty list if worksheet is invalid

    # --- 1. Get PUUID ---
    puu_id = puuid_cache.get(f"{game_name}#{tag_line}")
    request_count = 0
    start_time = time.time() # Rate limit timer starts here for this account update

    if not puu_id:
        try:
            # st.info(f"Fetching PUUID for {game_name}#{tag_line}...")
            url = SUMMONER_NAME_BY_URL.format(game_name, tag_line)
            response = requests.get(url, timeout=10)
            request_count += 1
            request_count, start_time = rate_limit_pause(start_time, request_count)
            response.raise_for_status()
            data = response.json()
            puu_id = data.get("puuid")
            if puu_id:
                 puuid_cache[f"{game_name}#{tag_line}"] = puu_id # Cache it
            else:
                 st.error(f"PUUID not found in response for {game_name}#{tag_line}.")
                 return []
        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching PUUID for {game_name}#{tag_line}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                 st.error(f"Response status: {e.response.status_code}, content: {e.response.text[:200]}")
            return [] # Cannot proceed without PUUID
        except Exception as e:
             st.error(f"Unexpected error fetching PUUID: {e}")
             return []


    # --- 2. Get Existing Match IDs from Sheet ---
    try:
        # Fetch only the second column (Match IDs) - more efficient
        existing_match_ids = set(worksheet.col_values(2)[1:]) # Skip header row
    except gspread.exceptions.APIError as e:
        st.error(f"Error fetching existing match IDs from sheet '{worksheet.title}': {e}")
        existing_match_ids = set() # Assume no existing matches if fetch fails
    except Exception as e:
         st.error(f"Unexpected error fetching existing match IDs: {e}")
         existing_match_ids = set()


    # --- 3. Get Recent Match History from Riot API ---
    try:
        # st.info(f"Fetching match history for {game_name}#{tag_line} (PUUID: ...{puu_id[-6:]})")
        url = MATCH_HISTORY_URL.format(puu_id)
        response = requests.get(url, timeout=15)
        request_count += 1
        request_count, start_time = rate_limit_pause(start_time, request_count)
        response.raise_for_status()
        recent_match_ids = response.json()
        if not isinstance(recent_match_ids, list):
             st.error(f"Unexpected format for match history for {game_name}#{tag_line}: {recent_match_ids}")
             return []
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching match history for {game_name}#{tag_line}: {e}")
        if hasattr(e, 'response') and e.response is not None:
             st.error(f"Response status: {e.response.status_code}, content: {e.response.text[:200]}")
        return []
    except Exception as e:
         st.error(f"Unexpected error fetching match history: {e}")
         return []


    # --- 4. Process New Matches ---
    new_data_to_add = []
    matches_to_fetch_details = [m_id for m_id in recent_match_ids if m_id not in existing_match_ids]

    if not matches_to_fetch_details:
        # st.info(f"No new matches found for {game_name}#{tag_line}.")
        return []

    st.info(f"Found {len(matches_to_fetch_details)} new matches for {game_name}#{tag_line}. Fetching details...")

    processed_count = 0
    for game_id in matches_to_fetch_details:
        try:
            # st.info(f"Fetching details for match {game_id}...")
            url = MATCH_BASIC_URL.format(game_id)
            response = requests.get(url, timeout=10)
            request_count += 1
            request_count, start_time = rate_limit_pause(start_time, request_count)
            response.raise_for_status()
            match_data = response.json()

            # Basic validation of response structure
            if not match_data or 'info' not in match_data or 'participants' not in match_data['info'] or 'metadata' not in match_data or 'participants' not in match_data['metadata']:
                st.warning(f"Incomplete data structure for match {game_id}. Skipping.")
                continue

            participants_puuids = match_data['metadata']['participants']
            if puu_id not in participants_puuids:
                 st.warning(f"Player PUUID {puu_id} not found in participants for match {game_id}. Skipping.")
                 continue

            player_index = participants_puuids.index(puu_id)
            player_data = match_data['info']['participants'][player_index]

            # Extract required fields safely using .get()
            champion_name = player_data.get('championName', 'UnknownChamp')
            kills = player_data.get('kills', 0)
            deaths = player_data.get('deaths', 0)
            assists = player_data.get('assists', 0)
            # Use 'individualPosition' or 'teamPosition' based on availability/preference
            # 'individualPosition' is usually more accurate (TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY)
            # 'teamPosition' can sometimes be empty or less specific
            position = player_data.get('individualPosition', player_data.get('teamPosition', 'UNKNOWN')).upper()
            is_win = 1 if player_data.get("win", False) else 0
            game_creation_ms = match_data['info'].get('gameCreation')

            if game_creation_ms:
                 game_datetime = datetime.fromtimestamp(game_creation_ms / 1000)
                 game_date_str = game_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                 game_date_str = "N/A" # Handle missing timestamp


            # Append data in the correct order for the sheet
            new_data_to_add.append([
                game_date_str,
                game_id,
                str(is_win), # Ensure win is stored as string '1' or '0'
                champion_name,
                position,
                str(kills), # Store stats as strings
                str(deaths),
                str(assists)
            ])
            processed_count += 1

        except requests.exceptions.RequestException as e:
            st.error(f"Error fetching details for match {game_id}: {e}")
            if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429:
                 st.warning("Hit rate limit fetching match details. Pausing and will retry later if needed.")
                 # Implement a longer pause or break if 429 occurs frequently
                 time.sleep(30) # Pause for 30s on 429
                 request_count = 0 # Reset count after pause
                 start_time = time.time()
            # else: Continue to next match on other errors

        except KeyError as e:
            st.error(f"Missing expected key '{e}' in data for match {game_id}. Skipping.")
        except Exception as e:
            st.error(f"Unexpected error processing match {game_id}: {e}")

    # --- 5. Append New Data to Worksheet ---
    if new_data_to_add:
        try:
            # Append rows in batches if necessary (though gspread handles reasonably large lists)
            worksheet.append_rows(new_data_to_add, value_input_option='USER_ENTERED')
            st.success(f"Successfully added {len(new_data_to_add)} new matches for {game_name}#{tag_line}.")
        except gspread.exceptions.APIError as e:
            st.error(f"Failed to append new matches to sheet '{worksheet.title}': {e}")
            # Optionally: save failed data locally for manual addition
        except Exception as e:
             st.error(f"Unexpected error appending matches to sheet: {e}")

    return new_data_to_add # Return the newly added data


# Aggregate SoloQ data from Google Sheets
@st.cache_data(ttl=300) # Cache aggregated data for 5 minutes
def aggregate_soloq_data_from_sheet(spreadsheet, team_name):
    """Aggregates player stats from their individual worksheets."""
    if not spreadsheet:
        st.error("Invalid spreadsheet object provided for SoloQ aggregation.")
        return {}

    aggregated_data = defaultdict(lambda: defaultdict(lambda: {
        "count": 0, "wins": 0, "kills": 0, "deaths": 0, "assists": 0
    }))
    players_config = team_rosters.get(team_name, {})

    if not players_config:
         st.warning(f"No roster found for team '{team_name}'. Cannot aggregate SoloQ data.")
         return {}

    for player, player_info in players_config.items():
        target_role = player_info.get("role", "UNKNOWN").upper() # Role defined in roster
        try:
            wks = spreadsheet.worksheet(player) # Get worksheet by player name
            # Fetch all data at once for efficiency
            # Use get_all_records for easier dictionary access if headers are consistent
            # records = wks.get_all_records() # Returns list of dicts
            all_values = wks.get_all_values() # Returns list of lists
            if len(all_values) <= 1: # Only header or empty
                 # st.info(f"No data found in worksheet for player '{player}'.")
                 continue

            header = all_values[0]
            # Find column indices dynamically - more robust than fixed indices
            try:
                win_col = header.index("Победа")
                champ_col = header.index("Чемпион")
                role_col = header.index("Роль")
                kills_col = header.index("Киллы")
                deaths_col = header.index("Смерти")
                assists_col = header.index("Ассисты")
            except ValueError as e:
                 st.error(f"Missing expected column in sheet '{player}': {e}. Skipping aggregation for this player.")
                 continue

            # Process rows, skipping header
            for row in all_values[1:]:
                 # Basic check for row length
                 if len(row) <= max(win_col, champ_col, role_col, kills_col, deaths_col, assists_col):
                     # st.warning(f"Skipping incomplete row in sheet '{player}': {row}")
                     continue

                 # Safely extract data
                 try:
                    win_str = row[win_col]
                    champion = row[champ_col]
                    role_in_game = row[role_col].upper() # Normalize role from sheet
                    kills_str = row[kills_col]
                    deaths_str = row[deaths_col]
                    assists_str = row[assists_col]

                    # Check if role matches the player's designated role
                    if role_in_game == target_role:
                        # Ensure stats are treated as numbers, handle potential errors
                        kills = int(kills_str) if kills_str.isdigit() else 0
                        deaths = int(deaths_str) if deaths_str.isdigit() else 0
                        assists = int(assists_str) if assists_str.isdigit() else 0
                        is_win = 1 if win_str == '1' else 0 # Check for '1' specifically

                        aggregated_data[player][champion]["wins"] += is_win
                        aggregated_data[player][champion]["count"] += 1
                        aggregated_data[player][champion]["kills"] += kills
                        aggregated_data[player][champion]["deaths"] += deaths
                        aggregated_data[player][champion]["assists"] += assists
                 except (ValueError, IndexError) as e:
                      # st.warning(f"Error processing row in sheet '{player}': {row} - Error: {e}. Skipping row.")
                      continue # Skip row if data conversion fails

        except gspread.exceptions.WorksheetNotFound:
            st.warning(f"Worksheet for player '{player}' not found in spreadsheet '{spreadsheet.title}'. Skipping.")
            continue # Skip player if sheet doesn't exist
        except gspread.exceptions.APIError as e:
             st.error(f"API Error accessing worksheet for player '{player}': {e}")
             continue # Skip player on API error
        except Exception as e:
             st.error(f"Unexpected error processing sheet for player '{player}': {e}")
             continue # Skip player on other errors


    # Sort champions by game count within each player's data
    for player in aggregated_data:
        aggregated_data[player] = dict(sorted(
            aggregated_data[player].items(),
            key=lambda item: item[1]["count"], # Sort primarily by count
            reverse=True
        ))

    return dict(aggregated_data) # Convert outer defaultdict


# --- Notes Saving/Loading ---
NOTES_DIR = "notes_data" # Directory to store notes JSON files
os.makedirs(NOTES_DIR, exist_ok=True) # Create directory if it doesn't exist

def get_notes_filepath(team_name, prefix="notes"):
    """Generates the filepath for a team's notes file."""
    # Sanitize team name for filename
    safe_team_name = "".join(c if c.isalnum() else "_" for c in team_name)
    return os.path.join(NOTES_DIR, f"{prefix}_{safe_team_name}.json")

def save_notes_data(data, team_name):
    """Saves notes data (draft templates and text) to a JSON file."""
    filepath = get_notes_filepath(team_name)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4) # Use indent for readability
    except IOError as e:
        st.error(f"Error saving notes data for {team_name}: {e}")
    except Exception as e:
         st.error(f"Unexpected error saving notes: {e}")


def load_notes_data(team_name):
    """Loads notes data from a JSON file, returning defaults if not found or invalid."""
    filepath = get_notes_filepath(team_name)
    # Define the default structure for notes
    default_data = {
        "tables": [
            # Default 10x3 table structure (Champ, Action, Champ)
            [ ["", "Ban", ""], ["", "Ban", ""], ["", "Ban", ""], # Phase 1 Bans
              ["", "Pick", ""], ["", "Pick", ""], ["", "Pick", ""], # Phase 1 Picks
              ["", "Ban", ""], ["", "Ban", ""],                      # Phase 2 Bans
              ["", "Pick", ""], ["", "Pick", ""]                     # Phase 2 Picks
            ] * 6 # Create 6 default tables
        ],
        "notes_text": "" # Default empty notes text
    }
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                loaded_data = json.load(f)
                # Basic validation: check if keys exist
                if "tables" in loaded_data and "notes_text" in loaded_data and isinstance(loaded_data["tables"], list):
                     # Further validation could check table dimensions/types if needed
                     return loaded_data
                else:
                     st.warning(f"Notes file for {team_name} has invalid structure. Loading defaults.")
                     return default_data
        except json.JSONDecodeError:
            st.error(f"Error decoding notes file for {team_name}. File might be corrupted. Loading defaults.")
            return default_data
        except IOError as e:
            st.error(f"Error reading notes file for {team_name}: {e}. Loading defaults.")
            return default_data
        except Exception as e:
            st.error(f"Unexpected error loading notes: {e}. Loading defaults.")
            return default_data
    else:
        # st.info(f"Notes file for {team_name} not found. Creating default notes.")
        return default_data # Return defaults if file doesn't exist


# --- Streamlit Page Functions ---

def hll_page(selected_team):
    """Displays HLL stats for the selected team."""
    st.title(f"Hellenic Legends League - Team Analysis")
    st.header(f"Team: {selected_team}")

    # Add a button to manually refresh HLL data
    if st.button("🔄 Refresh HLL Data", key="refresh_hll"):
        with st.spinner("Fetching latest HLL data from Leaguepedia..."):
            # Clear relevant cache entries before fetching
            fetch_match_history_data.clear()
            fetch_draft_data.clear()
            # Re-fetch data and update session state
            try:
                st.session_state.match_history_data = fetch_match_history_data()
                st.session_state.draft_data = fetch_draft_data() # Fetch draft data here
                st.success("HLL data refreshed!")
            except Exception as e:
                st.error(f"Failed to refresh HLL data: {e}")
                # Keep existing data in session state on failure
        st.rerun() # Rerun to reflect updated data immediately


    # --- Data Retrieval from Session State ---
    # Use .get() with defaults to prevent errors if data wasn't loaded
    match_history_data = st.session_state.get('match_history_data', {})
    draft_data_all_teams = st.session_state.get('draft_data', {})

    team_match_data = match_history_data.get(selected_team, {})
    team_draft_data = draft_data_all_teams.get(selected_team, [])

    if not team_match_data and not team_draft_data:
        st.warning(f"No HLL match or draft data found for team '{selected_team}'. Data might be loading or the team hasn't played.")
        # Optionally display overall league stats or return early
        # return

    # --- Display Summary Stats ---
    st.subheader("Overall Performance")
    col1, col2, col3, col4 = st.columns(4)
    total_games = team_match_data.get('matches_played', 0)
    total_wins = team_match_data.get('wins', 0)
    win_rate = (total_wins / total_games * 100) if total_games > 0 else 0
    blue_games = team_match_data.get('blue_side_games', 0)
    blue_wins = team_match_data.get('blue_side_wins', 0)
    blue_wr = (blue_wins / blue_games * 100) if blue_games > 0 else 0
    red_games = team_match_data.get('red_side_games', 0)
    red_wins = team_match_data.get('red_side_wins', 0)
    red_wr = (red_wins / red_games * 100) if red_games > 0 else 0

    col1.metric("Total Games", total_games)
    col2.metric("Win Rate", f"{win_rate:.1f}%", f"{total_wins}W - {total_games - total_wins}L")
    col3.metric("Blue Side WR", f"{blue_wr:.1f}%", f"{blue_wins}W - {blue_games - blue_wins}L ({blue_games} Games)")
    col4.metric("Red Side WR", f"{red_wr:.1f}%", f"{red_wins}W - {red_games - red_wins}L ({red_games} Games)")

    st.divider()

    # --- Section Toggles ---
    # Use checkboxes for a cleaner look than multiple buttons
    st.subheader("View Sections")
    show_picks = st.toggle("Show Champion Picks", key="toggle_picks", value=True)
    show_bans = st.toggle("Show Champion Bans", key="toggle_bans", value=False)
    show_duos = st.toggle("Show Duo Picks", key="toggle_duos", value=False)
    show_drafts = st.toggle("Show Detailed Drafts", key="toggle_drafts", value=False)
    show_notes = st.toggle("Show Notes & Templates", key="toggle_notes", value=False)
    st.divider()


    # --- Champion Picks Section ---
    if show_picks:
        st.subheader("Champion Picks by Role")
        roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']
        pick_cols = st.columns(len(roles))

        for i, role in enumerate(roles):
            with pick_cols[i]:
                st.markdown(f"**{role}**")
                role_pick_data = team_match_data.get(role, {})
                stats = []
                for champ, data in role_pick_data.items():
                    if champ != "N/A" and data.get('games', 0) > 0:
                        winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                        stats.append({
                            'Icon': get_champion_icon_html(champ, width=25, height=25),
                            # 'Champion': champ, # Icon implies champion
                            'Games': data['games'],
                            'WR%': winrate # Abbreviated header
                        })

                if stats:
                    df = pd.DataFrame(stats)
                    df = df.sort_values('Games', ascending=False).reset_index(drop=True)
                    # Apply coloring and format WR
                    df['WR%'] = df['WR%'].apply(color_win_rate)
                    # Display table without index, allow HTML
                    st.markdown(
                        df.to_html(escape=False, index=False, classes='compact-table', justify='center'),
                        unsafe_allow_html=True
                    )
                else:
                    st.caption("No picks data.")
        st.divider()


    # --- Champion Bans Section ---
    if show_bans:
        st.subheader("Champion Bans Analysis")
        bans_col1, bans_col2 = st.columns(2)

        with bans_col1:
            st.markdown("**Bans by Team**")
            team_bans_data = team_match_data.get('Bans', {})
            if team_bans_data:
                stats = [{'Icon': get_champion_icon_html(c, 25, 25), 'Count': n}
                         for c, n in team_bans_data.items() if c != "N/A"]
                if stats:
                     df = pd.DataFrame(stats).sort_values('Count', ascending=False).reset_index(drop=True)
                     st.markdown(df.to_html(escape=False, index=False, classes='compact-table'), unsafe_allow_html=True)
                else: st.caption("No bans data.")
            else: st.caption("No bans data.")

        with bans_col2:
            st.markdown("**Bans by Opponents**")
            opponent_bans_data = team_match_data.get('OpponentBansAgainst', {})
            if opponent_bans_data:
                stats = [{'Icon': get_champion_icon_html(c, 25, 25), 'Count': n}
                         for c, n in opponent_bans_data.items() if c != "N/A"]
                if stats:
                    df = pd.DataFrame(stats).sort_values('Count', ascending=False).reset_index(drop=True)
                    st.markdown(df.to_html(escape=False, index=False, classes='compact-table'), unsafe_allow_html=True)
                else: st.caption("No opponent bans data.")
            else: st.caption("No opponent bans data.")
        st.divider()


    # --- Duo Picks Section ---
    if show_duos:
        st.subheader("Duo Lane/Role Synergy")
        duo_picks_data = team_match_data.get('DuoPicks', {})
        # Define which pairs to show
        duo_pairs_to_display = {
             "Top / Jungle": ('Top', 'Jungle'),
             "Jungle / Mid": ('Jungle', 'Mid'),
             "Jungle / Support": ('Jungle', 'Support'),
             "Bot Lane (ADC / Support)": ('ADC', 'Support')
        }

        duo_cols = st.columns(len(duo_pairs_to_display))
        col_idx = 0

        for title, (role1_target, role2_target) in duo_pairs_to_display.items():
            with duo_cols[col_idx]:
                st.markdown(f"**{title}**")
                duo_stats = []
                for duo_key, data in duo_picks_data.items():
                    # Extract original roles and champs from the sorted key
                    (champ1, role1), (champ2, role2) = duo_key # Unpack the tuple key
                    # Check if this duo matches the target roles (in any order)
                    if {role1, role2} == {role1_target, role2_target}:
                        winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                         # Ensure roles are displayed in the target order for consistency
                        if role1 == role1_target:
                            icon1, icon2 = get_champion_icon_html(champ1, 25, 25), get_champion_icon_html(champ2, 25, 25)
                        else:
                             icon1, icon2 = get_champion_icon_html(champ2, 25, 25), get_champion_icon_html(champ1, 25, 25)
                        duo_stats.append({
                            f'{role1_target}': icon1,
                            f'{role2_target}': icon2,
                            'Games': data['games'],
                            'WR%': winrate
                        })

                if duo_stats:
                    df_duo = pd.DataFrame(duo_stats)
                    df_duo = df_duo.sort_values('Games', ascending=False).reset_index(drop=True)
                    df_duo['WR%'] = df_duo['WR%'].apply(color_win_rate)
                    st.markdown(
                        df_duo.to_html(escape=False, index=False, classes='compact-table', justify='center'),
                        unsafe_allow_html=True
                    )
                else:
                    st.caption("No duo data.")
            col_idx += 1
        st.divider()


    # --- Detailed Drafts Section ---
    if show_drafts:
        st.subheader("Detailed Game Drafts")
        if team_draft_data:
            # Group drafts by match (opponent and tournament)
            drafts_by_match = defaultdict(list)
            for draft in team_draft_data:
                # Use match_key (sorted tuple of teams) and tournament to group games in a series
                grouping_key = (draft['tournament'], draft['match_key'])
                drafts_by_match[grouping_key].append(draft)

            # Sort matches chronologically (using match_number within the series)
            # Display most recent matches first? Or oldest first? Let's do most recent.
            sorted_match_groups = sorted(
                drafts_by_match.items(),
                key=lambda item: (item[0][0], min(d['match_number'] for d in item[1])), # Sort by tournament, then first game number
                reverse=True # Show most recent tournaments/matches first
            )

            # Allow user to select which match to view drafts for
            match_options = [f"{t} - {mk[0]} vs {mk[1]}" for (t, mk), _ in sorted_match_groups]
            if not match_options:
                st.info("No completed drafts found for this team.")

            selected_match_str = st.selectbox("Select Match Series to View Drafts:", match_options, index=0 if match_options else None) # Default to first/most recent

            if selected_match_str:
                # Find the corresponding drafts for the selected match string
                selected_drafts = []
                for (t, mk), drafts in sorted_match_groups:
                     if f"{t} - {mk[0]} vs {mk[1]}" == selected_match_str:
                         selected_drafts = sorted(drafts, key=lambda d: d['match_number']) # Sort games within series
                         break

                if selected_drafts:
                    st.markdown(f"**{selected_match_str} (Game {selected_drafts[0]['match_number']} - {selected_drafts[-1]['match_number']})**")
                    draft_display_cols = st.columns(len(selected_drafts))

                    for i, draft in enumerate(selected_drafts):
                        with draft_display_cols[i]:
                            is_selected_team_blue = (draft['blue_team'] == selected_team)
                            opponent = draft['red_team'] if is_selected_team_blue else draft['blue_team']
                            result = "Win" if (draft['winner_side'] == 'blue' and is_selected_team_blue) or \
                                              (draft['winner_side'] == 'red' and not is_selected_team_blue) else "Loss"
                            result_color = "lightgreen" if result == "Win" else "lightcoral"

                            st.markdown(f"**Game {draft['match_number']}** (<span style='color:{result_color};'>{result}</span> vs {opponent})", unsafe_allow_html=True)
                            if draft['vod_link'] != "N/A":
                                st.link_button("Watch VOD", draft['vod_link'], use_container_width=True)

                            # Prepare data for the draft table display
                            actions = draft['draft_actions']
                            blue_picks_ordered = [a['champion'] for a in actions if a['type'] == 'pick' and a['side'] == 'blue']
                            red_picks_ordered = [a['champion'] for a in actions if a['type'] == 'pick' and a['side'] == 'red']
                            blue_bans_p1 = [a['champion'] for a in actions if a['type'] == 'ban' and a['phase'] == 1 and a['side'] == 'blue']
                            red_bans_p1 = [a['champion'] for a in actions if a['type'] == 'ban' and a['phase'] == 1 and a['side'] == 'red']
                            blue_bans_p2 = [a['champion'] for a in actions if a['type'] == 'ban' and a['phase'] == 2 and a['side'] == 'blue']
                            red_bans_p2 = [a['champion'] for a in actions if a['type'] == 'ban' and a['phase'] == 2 and a['side'] == 'red']

                            # Define the structure mirroring draft phase
                            draft_table_rows = []
                            # Phase 1 Bans (BB1, RB1, BB2, RB2, BB3, RB3)
                            for j in range(3):
                                bb = blue_bans_p1[j] if j < len(blue_bans_p1) else "N/A"
                                rb = red_bans_p1[j] if j < len(red_bans_p1) else "N/A"
                                draft_table_rows.append((get_champion_icon_html(bb, 20, 20), f"B{j+1}", get_champion_icon_html(rb, 20, 20)))
                            # Phase 1 Picks (BP1, RP1, RP2, BP2, BP3, RP3)
                            pick_map_p1 = {1:0, 4:1, 5:2} # Blue pick indices
                            pick_map_p1_r = {2:0, 3:1, 6:2} # Red pick indices
                            bp1 = blue_picks_ordered[pick_map_p1[1]] if 1 in pick_map_p1 and pick_map_p1[1] < len(blue_picks_ordered) else "N/A"
                            rp1 = red_picks_ordered[pick_map_p1_r[2]] if 2 in pick_map_p1_r and pick_map_p1_r[2] < len(red_picks_ordered) else "N/A"
                            rp2 = red_picks_ordered[pick_map_p1_r[3]] if 3 in pick_map_p1_r and pick_map_p1_r[3] < len(red_picks_ordered) else "N/A"
                            bp2 = blue_picks_ordered[pick_map_p1[4]] if 4 in pick_map_p1 and pick_map_p1[4] < len(blue_picks_ordered) else "N/A"
                            bp3 = blue_picks_ordered[pick_map_p1[5]] if 5 in pick_map_p1 and pick_map_p1[5] < len(blue_picks_ordered) else "N/A"
                            rp3 = red_picks_ordered[pick_map_p1_r[6]] if 6 in pick_map_p1_r and pick_map_p1_r[6] < len(red_picks_ordered) else "N/A"
                            draft_table_rows.extend([
                                (get_champion_icon_html(bp1, 20, 20), "P1", ""),
                                ("", "P1", get_champion_icon_html(rp1, 20, 20)),
                                ("", "P2", get_champion_icon_html(rp2, 20, 20)),
                                (get_champion_icon_html(bp2, 20, 20), "P2", ""),
                                (get_champion_icon_html(bp3, 20, 20), "P3", ""),
                                ("", "P3", get_champion_icon_html(rp3, 20, 20)),
                            ])
                             # Phase 2 Bans (RB4, BB4, RB5, BB5)
                            ban_map_p2_r = {1:0, 3:1} # Red ban indices (R B R B)
                            ban_map_p2_b = {2:0, 4:1} # Blue ban indices
                            rb4 = red_bans_p2[ban_map_p2_r[1]] if 1 in ban_map_p2_r and ban_map_p2_r[1] < len(red_bans_p2) else "N/A"
                            bb4 = blue_bans_p2[ban_map_p2_b[2]] if 2 in ban_map_p2_b and ban_map_p2_b[2] < len(blue_bans_p2) else "N/A"
                            rb5 = red_bans_p2[ban_map_p2_r[3]] if 3 in ban_map_p2_r and ban_map_p2_r[3] < len(red_bans_p2) else "N/A"
                            bb5 = blue_bans_p2[ban_map_p2_b[4]] if 4 in ban_map_p2_b and ban_map_p2_b[4] < len(blue_bans_p2) else "N/A"
                            draft_table_rows.extend([
                                 ("", "B4", get_champion_icon_html(rb4, 20, 20)),
                                 (get_champion_icon_html(bb4, 20, 20), "B4", ""),
                                 ("", "B5", get_champion_icon_html(rb5, 20, 20)),
                                 (get_champion_icon_html(bb5, 20, 20), "B5", ""),
                            ])
                            # Phase 2 Picks (RP4, BP4, BP5, RP5)
                            pick_map_p2_r = {1:3, 4:4} # Red pick indices (R B B R)
                            pick_map_p2_b = {2:3, 3:4} # Blue pick indices
                            rp4 = red_picks_ordered[pick_map_p2_r[1]] if 1 in pick_map_p2_r and pick_map_p2_r[1] < len(red_picks_ordered) else "N/A"
                            bp4 = blue_picks_ordered[pick_map_p2_b[2]] if 2 in pick_map_p2_b and pick_map_p2_b[2] < len(blue_picks_ordered) else "N/A"
                            bp5 = blue_picks_ordered[pick_map_p2_b[3]] if 3 in pick_map_p2_b and pick_map_p2_b[3] < len(blue_picks_ordered) else "N/A"
                            rp5 = red_picks_ordered[pick_map_p2_r[4]] if 4 in pick_map_p2_r and pick_map_p2_r[4] < len(red_picks_ordered) else "N/A"
                            draft_table_rows.extend([
                                ("", "P4", get_champion_icon_html(rp4, 20, 20)),
                                (get_champion_icon_html(bp4, 20, 20), "P4", ""),
                                (get_champion_icon_html(bp5, 20, 20), "P5", ""),
                                ("", "P5", get_champion_icon_html(rp5, 20, 20)),
                            ])

                            df_draft = pd.DataFrame(draft_table_rows, columns=[draft['blue_team'], "Action", draft['red_team']])
                            # Simple HTML table, consider adding styling later if needed
                            st.markdown(df_draft.to_html(escape=False, index=False, classes='compact-table draft-view', justify='center'), unsafe_allow_html=True)

                else:
                    st.info("No drafts found for the selected match series.")
        else:
            st.info(f"No detailed draft data available for {selected_team}.")
        st.divider()


    # --- Notes Section ---
    if show_notes:
        st.subheader("Draft Notes & Templates")
        notes_state_key = f'notes_data_{selected_team}'
        if notes_state_key not in st.session_state:
            st.session_state[notes_state_key] = load_notes_data(selected_team)

        notes_data = st.session_state[notes_state_key]
        col_templates, col_text_notes = st.columns([3, 1]) # Templates wider

        with col_templates:
            st.markdown("**Draft Templates**")
            num_templates = len(notes_data.get("tables", []))
            template_cols = st.columns(3) # Display 3 templates per row

            for i in range(num_templates):
                 with template_cols[i % 3]:
                    st.markdown(f"*Template {i+1}*")
                    # Ensure table data is a list of lists before passing to DataFrame
                    table_content = notes_data["tables"][i]
                    if not isinstance(table_content, list) or not all(isinstance(row, list) for row in table_content):
                         st.error(f"Invalid data structure for template {i+1}. Resetting to default.")
                         # Reset to default structure if invalid
                         table_content = [ ["", "Ban", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""] ]
                         notes_data["tables"][i] = table_content

                    df = pd.DataFrame(table_content, columns=["Team 1", "Action", "Team 2"])
                    editor_key = f"notes_table_{selected_team}_{i}"

                    # Use data_editor for interactive editing
                    edited_df = st.data_editor(
                        df,
                        num_rows="fixed", # Keep 10 rows
                        use_container_width=True,
                        key=editor_key,
                        height=385, # Adjust height to fit 10 rows comfortably
                        column_config={
                            # Allow editing champion names
                            "Team 1": st.column_config.TextColumn("Team 1 Champ", help="Enter champion name for Team 1"),
                            # Keep Action column read-only
                            "Action": st.column_config.TextColumn("Action", disabled=True),
                            "Team 2": st.column_config.TextColumn("Team 2 Champ", help="Enter champion name for Team 2"),
                        }
                    )
                    # Update session state immediately after edit
                    if not edited_df.equals(df): # Check if changes were made
                         st.session_state[notes_state_key]["tables"][i] = edited_df.values.tolist()
                         # Save data immediately on change (consider debouncing if performance is an issue)
                         save_notes_data(st.session_state[notes_state_key], selected_team)
                         # st.toast(f"Template {i+1} saved!") # Optional feedback


        with col_text_notes:
            st.markdown("**General Notes**")
            notes_text_key = f"notes_text_area_{selected_team}"
            notes_text = st.text_area(
                "Write general notes here:",
                value=notes_data.get("notes_text", ""),
                height=400, # Adjust height as needed
                key=notes_text_key,
                label_visibility="collapsed"
            )
            # Update session state and save if text changes
            if notes_text != notes_data.get("notes_text", ""):
                st.session_state[notes_state_key]["notes_text"] = notes_text
                save_notes_data(st.session_state[notes_state_key], selected_team)
                # st.toast("Notes saved!") # Optional feedback
        st.divider()


def soloq_page():
    """Displays SoloQ stats for the GMS team."""
    st.title("Gamespace - SoloQ Player Statistics")

    # --- Back Button ---
    # if st.button("⬅️ Back to HLL Stats"):
    #     st.session_state.current_page = "Hellenic Legends League Stats"
    #     st.rerun()

    # --- Google Sheets Client Setup ---
    gspread_client = setup_google_sheets_client()
    if not gspread_client:
        st.error("Failed to connect to Google Sheets. SoloQ features are unavailable.")
        return # Stop execution of this page if client fails

    # --- Open or Create Spreadsheet ---
    try:
        spreadsheet = gspread_client.open(SOLOQ_SHEET_NAME)
    except gspread.exceptions.SpreadsheetNotFound:
        st.warning(f"Spreadsheet '{SOLOQ_SHEET_NAME}' not found.")
        # Optionally offer to create it, but manual creation might be safer
        # Consider adding instructions for manual setup
        st.info(f"Please ensure a Google Sheet named '{SOLOQ_SHEET_NAME}' exists and the service account has edit permissions.")
        return # Stop if sheet not found
    except gspread.exceptions.APIError as e:
        st.error(f"API Error accessing spreadsheet '{SOLOQ_SHEET_NAME}': {e}")
        return
    except Exception as e:
        st.error(f"Unexpected error opening spreadsheet '{SOLOQ_SHEET_NAME}': {e}")
        return


    # --- Update Button ---
    # Cache for PUUIDs to reduce API calls
    if 'puuid_cache' not in st.session_state:
        st.session_state.puuid_cache = {}

    if st.button("🔄 Update SoloQ Data from Riot API", key="update_soloq"):
        total_new_matches = 0
        with st.spinner("Checking for new SoloQ games for GMS players..."):
            gms_roster = team_rosters.get("Gamespace", {})
            if not gms_roster:
                 st.error("Gamespace roster not found in configuration.")
            else:
                num_players = len(gms_roster)
                progress_bar = st.progress(0, text="Starting update...")
                players_updated = 0

                for player, player_info in gms_roster.items():
                    player_progress = (players_updated + 1) / num_players
                    progress_bar.progress(player_progress, text=f"Updating {player}...")

                    # Ensure worksheet exists for the player
                    wks = check_if_soloq_worksheet_exists(spreadsheet, player)
                    if not wks:
                        st.error(f"Could not get or create worksheet for {player}. Skipping update.")
                        continue # Skip player if worksheet fails

                    game_names = player_info.get("game_name", [])
                    tag_lines = player_info.get("tag_line", [])

                    # Check if lists have same length
                    if len(game_names) != len(tag_lines):
                         st.warning(f"Mismatch between game_name ({len(game_names)}) and tag_line ({len(tag_lines)}) count for player {player}. Skipping update for this player.")
                         continue

                    # Update for each Riot ID associated with the player
                    for game_name, tag_line in zip(game_names, tag_lines):
                         if game_name and tag_line: # Ensure both are present
                              # st.info(f"Checking account: {game_name}#{tag_line}")
                              new_matches_found = get_account_data_from_riot(wks, game_name, tag_line, st.session_state.puuid_cache)
                              total_new_matches += len(new_matches_found)
                         else:
                              st.warning(f"Missing game name or tag line for an account under player {player}.")
                    players_updated += 1
                progress_bar.progress(1.0, text="SoloQ update complete!")
                time.sleep(2) # Keep message visible briefly
                progress_bar.empty() # Clear progress bar

        if total_new_matches > 0:
            st.success(f"SoloQ data update finished. Added {total_new_matches} new matches.")
            # Clear cache for aggregated data to force re-aggregation
            aggregate_soloq_data_from_sheet.clear()
        else:
            st.info("SoloQ data update finished. No new matches found.")
        # No explicit rerun needed here, data display below will use latest from sheet


    # --- Display Aggregated Stats ---
    st.subheader("Player Statistics (Based on Sheets Data)")
    st.markdown("Select time range and view stats per player.")

    try:
        # Fetch and display aggregated data
        aggregated_soloq_data = aggregate_soloq_data_from_sheet(spreadsheet, "Gamespace")

        if not aggregated_soloq_data:
            st.warning("No aggregated SoloQ data available. Update data or check sheet content.")
        else:
            players = list(aggregated_soloq_data.keys())
            player_cols = st.columns(len(players) if players else 1)

            # Time filter selection (consider placing outside columns if space is tight)
            # Or apply filter *after* aggregation if performance allows
            # time_filter_soloq = st.selectbox("Filter Stats by Time:", ["All Time", "Last 7 Days", "Last 14 Days", "Last 30 Days"], key="soloq_time_filter")

            for i, player in enumerate(players):
                with player_cols[i]:
                    st.markdown(f"**{player}** ({team_rosters['Gamespace'][player]['role']})")
                    player_stats = aggregated_soloq_data.get(player, {})
                    stats_list = []
                    total_player_games = 0
                    total_player_wins = 0

                    for champ, stats_dict in player_stats.items():
                         games = stats_dict.get("count", 0)
                         if games > 0:
                            wins = stats_dict.get("wins", 0)
                            kills = stats_dict.get("kills", 0)
                            deaths = stats_dict.get("deaths", 1) # Avoid division by zero
                            assists = stats_dict.get("assists", 0)

                            total_player_games += games
                            total_player_wins += wins

                            win_rate = round((wins / games) * 100, 1) if games > 0 else 0
                            kda = round((kills + assists) / max(deaths, 1), 2)

                            stats_list.append({
                                'Icon': get_champion_icon_html(champ, 20, 20),
                                # 'Champion': champ,
                                'Games': games,
                                'WR%': win_rate,
                                'KDA': kda
                            })

                    # Display overall player win rate
                    player_wr = (total_player_wins / total_player_games * 100) if total_player_games > 0 else 0
                    st.caption(f"Overall: {total_player_wins}W-{total_player_games-total_player_wins}L ({player_wr:.1f}%)")


                    if stats_list:
                        df_stats = pd.DataFrame(stats_list).sort_values("Games", ascending=False).reset_index(drop=True)
                        df_stats['WR%'] = df_stats['WR%'].apply(color_win_rate)
                        # Display KDA with fixed format
                        df_stats['KDA'] = df_stats['KDA'].apply(lambda x: f"{x:.2f}")

                        st.markdown(
                             df_stats.to_html(escape=False, index=False, classes='compact-table soloq-stats', justify='center'),
                             unsafe_allow_html=True
                        )
                    else:
                        st.caption(f"No stats found.")

    except gspread.exceptions.APIError as e:
        st.error(f"API Error reading SoloQ data from Google Sheets: {e}")
    except Exception as e:
        st.error(f"An error occurred during SoloQ data aggregation or display: {e}")

    # --- Visualization Section (Optional) ---
    # Add visualizations like games played over time if needed
    # st.subheader("SoloQ Games Over Time") ... (code similar to original)


# --- Main Application Logic ---

def main():
    """Main function to handle page navigation and data loading."""

    # --- Initialize Session State ---
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Hellenic Legends League Stats" # Default page

    # --- Sidebar Navigation ---
    st.sidebar.title("Navigation")
    current_page = st.session_state.current_page

    # Conditional buttons for navigation
    if current_page != "Hellenic Legends League Stats":
        if st.sidebar.button("🏆 HLL Stats", key="nav_hll", use_container_width=True):
            st.session_state.current_page = "Hellenic Legends League Stats"
            st.rerun()
    if current_page != "GMS SoloQ":
        if st.sidebar.button("🎮 GMS SoloQ", key="nav_soloq", use_container_width=True):
            st.session_state.current_page = "GMS SoloQ"
            st.rerun()
    if current_page != "Scrims":
         if st.sidebar.button("⚔️ Scrims", key="nav_scrims", use_container_width=True):
            st.session_state.current_page = "Scrims"
            st.rerun()

    st.sidebar.divider()

    # --- Load Initial HLL Data (if not already loaded) ---
    # Check if data exists AND is not empty to avoid reloading on mere navigation
    hll_data_loaded = ('match_history_data' in st.session_state and st.session_state.match_history_data and
                       'draft_data' in st.session_state and st.session_state.draft_data)

    if not hll_data_loaded and current_page == "Hellenic Legends League Stats":
        st.sidebar.info("Loading HLL data...") # Show loading in sidebar
        with st.spinner("Loading initial HLL data from Leaguepedia..."):
            try:
                st.session_state.match_history_data = fetch_match_history_data()
                st.session_state.draft_data = fetch_draft_data()
                st.sidebar.success("HLL data loaded.")
                time.sleep(1) # Keep success message visible briefly
                st.rerun() # Rerun to update the main page content now data is available
            except requests.exceptions.RequestException as e:
                st.error(f"Network error fetching initial HLL data: {e}")
                # Assign empty defaults to prevent errors later
                st.session_state.match_history_data = defaultdict(dict)
                st.session_state.draft_data = defaultdict(list)
            except Exception as e:
                st.error(f"Error fetching initial HLL data: {e}")
                 # Assign empty defaults
                st.session_state.match_history_data = defaultdict(dict)
                st.session_state.draft_data = defaultdict(list)


    # --- HLL Team Selection (only if HLL data is available) ---
    if st.session_state.get('match_history_data') or st.session_state.get('draft_data'):
        all_teams = set()
        if isinstance(st.session_state.get('match_history_data'), dict):
            all_teams.update(normalize_team_name(team) for team in st.session_state.match_history_data.keys())
        if isinstance(st.session_state.get('draft_data'), dict):
            all_teams.update(normalize_team_name(team) for team in st.session_state.draft_data.keys())

        teams = sorted([team for team in all_teams if team != "unknown"])

        if teams:
             selected_hll_team = st.sidebar.selectbox(
                 "Select HLL Team:",
                 teams,
                 key="hll_team_select",
                 index=teams.index("Gamespace") if "Gamespace" in teams else 0 # Default to GMS if available
             )
        else:
             st.sidebar.warning("No HLL teams found in data.")
             selected_hll_team = None
    else:
        selected_hll_team = None
        if current_page == "Hellenic Legends League Stats":
             st.sidebar.warning("HLL data not loaded yet.")


    st.sidebar.divider()
    # --- Sidebar Footer ---
    try:
        st.sidebar.image("logo.webp", width=100, use_container_width=True)
    except Exception as img_err:
        st.sidebar.caption(f"Could not load logo.webp") # Use caption for less intrusive error

    st.sidebar.markdown(
        """<div style='text-align: center; font-size: 12px; color: #888;'>
           App by heovech
           <br>
           <a href='mailto:heovech@example.com' style='color: #888;'>Contact</a>
           </div>""", unsafe_allow_html=True
    )


    # --- Page Routing ---
    if current_page == "Hellenic Legends League Stats":
        if selected_hll_team:
            hll_page(selected_hll_team)
        else:
            st.info("Select an HLL team from the sidebar to view stats.")
            # Optionally show some default league-wide info here
    elif current_page == "GMS SoloQ":
        soloq_page()
    elif current_page == "Scrims":
        # Call the function from the imported scrims module
        scrims.scrims_page()


# --- Authentication ---
try:
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
except FileNotFoundError:
    st.error("FATAL: config.yaml not found. Authentication cannot proceed.")
    st.stop()
except yaml.YAMLError as e:
    st.error(f"FATAL: Error parsing config.yaml: {e}")
    st.stop()
except Exception as e:
    st.error(f"FATAL: Unexpected error loading config.yaml: {e}")
    st.stop()

# Basic validation of config structure needed for authenticator
if not isinstance(config, dict) or 'credentials' not in config or 'cookie' not in config or \
   not all(k in config['cookie'] for k in ['name', 'key', 'expiry_days']):
    st.error("FATAL: config.yaml is missing required sections ('credentials', 'cookie') or keys ('name', 'key', 'expiry_days').")
    st.stop()


authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# --- Authentication Flow ---
# Initialize session state keys if they don't exist
if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = None
if 'name' not in st.session_state:
    st.session_state.name = None
if 'username' not in st.session_state:
    st.session_state.username = None


# Attempt login only if status is None (initial state)
if st.session_state.authentication_status is None:
    try:
        # The login method returns name, status, username
        name, authentication_status, username = authenticator.login('Login', 'main')
        # Update session state with the results from authenticator.login
        st.session_state.name = name
        st.session_state.authentication_status = authentication_status
        st.session_state.username = username
    except KeyError as e:
         # This can happen if config.yaml structure is wrong (e.g., missing 'usernames')
         st.error(f"Authentication Error: Missing key {e} in config.yaml credentials. Please check the structure.")
         st.stop()
    except Exception as e:
         st.error(f"An unexpected error occurred during login: {e}")
         st.stop()


# --- Post-Authentication Logic ---
# Check the authentication status FROM session state
if st.session_state.authentication_status:
    # Clear login form placeholder if it exists (it shouldn't if logic is correct)
    # login_placeholder.empty() # Or similar if you used a placeholder

    # Display welcome message and logout button in the sidebar
    with st.sidebar:
        st.sidebar.divider()
        st.sidebar.write(f'Welcome *{st.session_state.name}*') # Use name from session state
        authenticator.logout('Logout', 'sidebar', key='logout_button') # Unique key for logout

    # --- Load CSS and Run Main App ---
    # Load custom CSS
    try:
        with open("style.css") as f:
             st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError:
         st.warning("style.css not found. Using default styles.")

    # Call the main application function only AFTER successful authentication
    if __name__ == "__main__":
        main()

elif st.session_state.authentication_status is False:
    st.error('Username/password is incorrect')
    # Optionally: Add forgot password link/functionality if supported by authenticator/backend
    # if st.button("Forgot Password?"): ...
elif st.session_state.authentication_status is None:
    # If still None after the login attempt, it means the form is waiting for input
    # st.warning('Please enter your username and password above.') # Message is implicit
    pass # The login form is already rendered by authenticator.login


