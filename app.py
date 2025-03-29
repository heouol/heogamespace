# --- START OF FILE app (4).py ---

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import requests
from bs4 import BeautifulSoup, Tag # <--- Убедимся, что Tag импортирован
import pandas as pd
from collections import defaultdict
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta
import json
import os
# from scrims import scrims_page  # <-- ЗАКОММЕНТИРОВАНА ИЛИ УДАЛЕНА СТАРАЯ СТРОКА
import scrims # <--- ИЗМЕНЕНИЕ: Импортируем весь модуль scrims

# Set page config at the start (must be the first Streamlit command)
st.set_page_config(layout="wide", page_title="HLL Analytics")

# Global constants for SoloQ
SUMMONER_NAME_BY_URL = "https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{}/{}?api_key=RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8"
MATCH_HISTORY_URL = "https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{}/ids?start=0&count=100&api_key=RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8"
MATCH_BASIC_URL = "https://europe.api.riotgames.com/lol/match/v5/matches/{}?api_key=RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8"

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

# Get the latest patch version from Data Dragon
#@st.cache_data(ttl=3600) # Убрано кэширование для избежания ошибки UnhashableParamError при импорте
def get_latest_patch_version():
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10)
        if response.status_code == 200:
            versions = response.json()
            return versions[0]
        print("Warning: Failed to get patch version, returning default.") # Changed to print
        return "14.14.1" # Update default periodically
    except Exception as e:
        print(f"Error fetching patch version: {e}. Returning default.") # Changed to print
        return "14.14.1"

PATCH_VERSION = get_latest_patch_version() # Вызов оставлен здесь

# Normalize team names
def normalize_team_name(team_name):
    if not team_name or not isinstance(team_name, str):
        return "unknown"

    team_name_lower = team_name.strip().lower()

    if team_name_lower in ["unknown blue", "unknown red", ""]:
        return "unknown"

    team_exceptions = {
        "gamespace": "Gamespace",
        "gms": "Gamespace",
        "gamespace logo std": "Gamespace",
        # Добавьте другие команды HLL при необходимости
    }

    team_name_clean = team_name_lower.replace("logo std", "").strip()

    for key, normalized_name in team_exceptions.items():
        if team_name_clean == key or key in team_name_clean:
            return normalized_name

    # Capitalize words if no exception matches
    return ' '.join(word.capitalize() for word in team_name_clean.split())


# Fetch match history data
# !! NO @st.cache_data !! - Removed due to lambda issue
def fetch_match_history_data():
    team_data = defaultdict(lambda: {
        'Top': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Jungle': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Mid': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'ADC': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Support': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Bans': defaultdict(int),
        'OpponentBansAgainst': defaultdict(int), # Renamed for clarity
        # 'OpponentBlueBans': defaultdict(int), # Keep one if needed
        # 'OpponentRedBans': defaultdict(int), # Keep one if needed
        'DuoPicks': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'MatchResults': [],
        'matches_played': 0, 'wins': 0, 'losses': 0, 'blue_side_games': 0,
        'blue_side_wins': 0, 'red_side_games': 0, 'red_side_wins': 0
    })

    match_counter = defaultdict(int) # Counter for games within a series vs same opponent

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls.get("match_history")
        if not url: continue
        headers = {'User-Agent': 'Mozilla/5.0'} # Consider a more specific user agent
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(f"Failed MH {tournament_name}: {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            # Select more robustly, handle multiple tables if present
            match_history_tables = soup.select('.wikitable.mhgame.sortable')
            if not match_history_tables:
                 st.error(f"No MH table for {tournament_name}")
                 continue
            match_history_table = match_history_tables[0] # Process first table found
        except IndexError:
            st.error(f"Could not find MH table for {tournament_name}")
            continue

        for row in match_history_table.select('tr')[1:]: # Skip header
            cols = row.select('td')
            if not cols or len(cols) < 9: # Need at least basic columns
                continue

            # Robust team name extraction
            blue_elem = cols[2].select_one('a[title]')
            red_elem = cols[3].select_one('a[title]')
            blue_raw = blue_elem['title'].strip() if blue_elem and 'title' in blue_elem.attrs else cols[2].get_text(strip=True)
            red_raw = red_elem['title'].strip() if red_elem and 'title' in red_elem.attrs else cols[3].get_text(strip=True)
            blue_team = normalize_team_name(blue_raw)
            red_team = normalize_team_name(red_raw)
            if blue_team == "unknown" or red_team == "unknown": continue

            # Robust winner extraction
            winner_team = "unknown"
            result_elem = cols[4].select_one('a[title]')
            result_text = cols[4].get_text(strip=True)
            if result_elem and 'title' in result_elem.attrs:
                winner_team = normalize_team_name(result_elem['title'].strip())
            elif result_text == "1:0": winner_team = blue_team
            elif result_text == "0:1": winner_team = red_team

            result_blue = 'Win' if winner_team == blue_team else 'Loss' if winner_team != "unknown" else 'N/A'
            result_red = 'Win' if winner_team == red_team else 'Loss' if winner_team != "unknown" else 'N/A'
            if result_blue == 'N/A': continue # Skip games with unknown results

            # Update overall stats
            team_data[blue_team]['matches_played'] += 1; team_data[red_team]['matches_played'] += 1
            team_data[blue_team]['blue_side_games'] += 1; team_data[red_team]['red_side_games'] += 1
            if result_blue == 'Win':
                team_data[blue_team]['wins'] += 1; team_data[blue_team]['blue_side_wins'] += 1; team_data[red_team]['losses'] += 1
            else:
                team_data[blue_team]['losses'] += 1; team_data[red_team]['wins'] += 1; team_data[red_team]['red_side_wins'] += 1

            # Match counter for series
            match_key = tuple(sorted([blue_team, red_team]))
            match_counter[match_key] += 1
            match_number = match_counter[match_key]

            # Bans
            blue_bans_elem = cols[5].select('span.sprite.champion-sprite') if len(cols) > 5 else []
            red_bans_elem = cols[6].select('span.sprite.champion-sprite') if len(cols) > 6 else [] # Adjusted selector slightly
            blue_bans = [get_champion(ban) for ban in blue_bans_elem]
            red_bans = [get_champion(ban) for ban in red_bans_elem]

            for champ in blue_bans:
                if champ != "N/A": team_data[blue_team]['Bans'][champ] += 1; team_data[red_team]['OpponentBansAgainst'][champ] += 1
            for champ in red_bans:
                if champ != "N/A": team_data[red_team]['Bans'][champ] += 1; team_data[blue_team]['OpponentBansAgainst'][champ] += 1

            # Picks
            blue_picks_elem = cols[7].select('span.sprite.champion-sprite') if len(cols) > 7 else []
            red_picks_elem = cols[8].select('span.sprite.champion-sprite') if len(cols) > 8 else []
            roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']
            blue_picks = {role: get_champion(pick) for role, pick in zip(roles, blue_picks_elem)}
            red_picks = {role: get_champion(pick) for role, pick in zip(roles, red_picks_elem)}

            # Update pick stats
            for team, picks, result in [(blue_team, blue_picks, result_blue), (red_team, red_picks, result_red)]:
                for role in roles:
                    champion = picks.get(role, "N/A") # Use N/A consistently
                    if champion != "N/A":
                        team_data[team][role][champion]['games'] += 1
                        if result == 'Win': team_data[team][role][champion]['wins'] += 1
                    # else: # Optional: Handle N/A picks if needed, e.g., count games with missing picks
                    #     if role not in team_data[team] or "N/A" not in team_data[team][role]:
                    #          team_data[team][role]["N/A"] = {'games': 0, 'wins': 0}
                    #     team_data[team][role]["N/A"]['games'] += 1
                    #     if result == 'Win': team_data[team][role]["N/A"]['wins'] += 1

                # Duo Picks - Store consistently sorted key
                duo_pairs = [('Top', 'Jungle'), ('Jungle', 'Mid'), ('Mid', 'ADC'), ('ADC', 'Support'), ('Jungle','Support')]
                for r1, r2 in duo_pairs:
                    champ1, champ2 = picks.get(r1, "N/A"), picks.get(r2, "N/A")
                    if champ1 != "N/A" and champ2 != "N/A":
                        duo_key = tuple(sorted([(champ1, r1), (champ2, r2)])) # Sort by champ name then role
                        team_data[team]['DuoPicks'][duo_key]['games'] += 1
                        if result == 'Win': team_data[team]['DuoPicks'][duo_key]['wins'] += 1

            # Match Results List
            m_id = f"{tournament_name}_{match_key[0]}_{match_key[1]}_{match_number}" # More specific ID
            team_data[blue_team]['MatchResults'].append({'match_id': m_id, 'opponent': red_team, 'side': 'blue', 'result': result_blue, 'tournament': tournament_name, 'blue_picks': blue_picks, 'red_picks': red_picks, 'blue_bans': blue_bans, 'red_bans': red_bans})
            team_data[red_team]['MatchResults'].append({'match_id': m_id, 'opponent': blue_team, 'side': 'red', 'result': result_red, 'tournament': tournament_name, 'blue_picks': blue_picks, 'red_picks': red_picks, 'blue_bans': blue_bans, 'red_bans': red_bans})

    return dict(team_data) # Convert outer defaultdict

# Fetch first bans data - NO LONGER NEEDED as it's covered in fetch_draft_data
# def fetch_first_bans_data(): ...

# Fetch draft data
# !! NO @st.cache_data !! - Removed due to lambda issue
def fetch_draft_data():
    team_drafts = defaultdict(list)
    match_counter = defaultdict(lambda: defaultdict(int)) # Needs lambda for nested default

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls.get("picks_and_bans") # Use the picks_and_bans URL
        if not url: continue
        headers = {'User-Agent': 'Mozilla/5.0'} # Use specific agent
        try:
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            st.error(f"Failed PB {tournament_name}: {e}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        # Find the correct table - class might vary slightly, be flexible
        draft_tables = soup.select('table.wikitable.plainlinks.hoverable-rows.column-show-hide-1, table.wikitable.plainlinks.column-show-hide-1')
        if not draft_tables:
            st.warning(f"Draft tables not found on {tournament_name}.")
            continue

        for table in draft_tables:
            rows = table.select('tr')
            if len(rows) < 2: continue # Skip header row

            # Process rows - might need adjustment based on actual table structure
            for row in rows[1:]:
                cols = row.select('td')
                if len(cols) < 24: # Expect many columns for draft
                    continue

                # Extract teams (similar logic to fetch_match_history_data)
                blue_cell, red_cell = cols[1], cols[2]
                blue_link = blue_cell.select_one('a[title], span[title]') # Check span too
                red_link = red_cell.select_one('a[title], span[title]')
                blue_raw = blue_link['title'].strip() if blue_link else blue_cell.get_text(strip=True)
                red_raw = red_link['title'].strip() if red_link else red_cell.get_text(strip=True)
                blue_team, red_team = normalize_team_name(blue_raw), normalize_team_name(red_raw)
                if blue_team == "unknown" or red_team == "unknown": continue

                # Extract winner based on class
                winner_side = None
                if 'pbh-winner' in blue_cell.get('class', []): winner_side = 'blue'
                elif 'pbh-winner' in red_cell.get('class', []): winner_side = 'red'
                # if winner_side is None: continue # Skip if winner unknown

                # Assign match number
                match_key = tuple(sorted([blue_team, red_team]))
                match_counter[tournament_name][match_key] += 1
                match_number = match_counter[tournament_name][match_key]

                # Extract draft actions (Bans and Picks in order)
                draft_actions = []
                # Ban Phase 1 (BB1->RB3, cols 5-10)
                for i, idx in enumerate(range(5, 11)):
                    side = 'blue' if i % 2 == 0 else 'red'
                    champ_span = cols[idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                    champ = champ_span['title'].strip() if champ_span else "N/A"
                    draft_actions.append({'type': 'ban', 'phase': 1, 'side': side, 'champion': champ, 'sequence': i + 1})

                # Pick Phase 1 (BP1->RP3, cols 11-14) - order BP1, RP1, RP2, BP2, BP3, RP3
                pick_order_p1 = [(11, 0, 'blue', 7), (12, 0, 'red', 8), (12, 1, 'red', 9), (13, 0, 'blue', 10), (13, 1, 'blue', 11), (14, 0, 'red', 12)]
                for col_idx, span_idx, side, seq in pick_order_p1:
                    pick_spans = cols[col_idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                    champ = pick_spans[span_idx]['title'].strip() if len(pick_spans) > span_idx else "N/A"
                    draft_actions.append({'type': 'pick', 'phase': 1, 'side': side, 'champion': champ, 'sequence': seq})

                # Ban Phase 2 (RB4->BB5, cols 15-18) - order RB4, BB4, RB5, BB5
                ban_order_p2 = [(15, 'red', 13), (16, 'blue', 14), (17, 'red', 15), (18, 'blue', 16)]
                for col_idx, side, seq in ban_order_p2:
                    champ_span = cols[col_idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                    champ = champ_span['title'].strip() if champ_span else "N/A"
                    draft_actions.append({'type': 'ban', 'phase': 2, 'side': side, 'champion': champ, 'sequence': seq})

                # Pick Phase 2 (RP4->RP5, cols 19-21) - order RP4, BP4, BP5, RP5
                pick_order_p2 = [(19, 0, 'red', 17), (20, 0, 'blue', 18), (20, 1, 'blue', 19), (21, 0, 'red', 20)]
                for col_idx, span_idx, side, seq in pick_order_p2:
                     pick_spans = cols[col_idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]')
                     champ = pick_spans[span_idx]['title'].strip() if len(pick_spans) > span_idx else "N/A"
                     draft_actions.append({'type': 'pick', 'phase': 2, 'side': side, 'champion': champ, 'sequence': seq})

                # Extract VOD link
                vod_link = "N/A"; vod_elem = cols[23].select_one('a[href]')
                if vod_elem: vod_link = vod_elem['href']

                # Store draft info for both teams
                draft_info = {'tournament': tournament_name, 'match_key': match_key, 'match_number': match_number, 'blue_team': blue_team, 'red_team': red_team, 'winner_side': winner_side, 'draft_actions': draft_actions, 'vod_link': vod_link}
                team_drafts[blue_team].append(draft_info); team_drafts[red_team].append(draft_info)

    return dict(team_drafts)


# --- Google Sheets & SoloQ Functions (Полная реализация) ---
@st.cache_resource
def setup_google_sheets_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]; json_creds_str = os.getenv("GOOGLE_SHEETS_CREDS");
    if not json_creds_str: st.error("GOOGLE_SHEETS_CREDS missing."); return None
    try: creds_dict = json.loads(json_creds_str); creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope); client = gspread.authorize(creds); client.list_spreadsheet_files(); return client
    except Exception as e: st.error(f"GSheets setup error: {e}"); return None

def check_if_soloq_worksheet_exists(spreadsheet, player_name):
    try: wks = spreadsheet.worksheet(player_name)
    except gspread.exceptions.WorksheetNotFound:
        try: wks = spreadsheet.add_worksheet(title=player_name, rows=1000, cols=8); header = ["Дата матча","Матч_айди","Победа","Чемпион","Роль","Киллы","Смерти","Ассисты"]; wks.append_row(header, value_input_option='USER_ENTERED')
        except Exception as e: st.error(f"Err create sheet '{player_name}': {e}"); return None
    except Exception as e: st.error(f"Err access sheet '{player_name}': {e}"); return None
    return wks

def rate_limit_pause(start_time, req_count, limit=95, window=120):
    if req_count >= limit: elapsed = time.time() - start_time;
    if elapsed < window: wait = window - elapsed + 1; st.toast(f"Riot pause: {wait:.1f}s..."); time.sleep(wait); return 0, time.time()
    return req_count, start_time

def get_account_data_from_riot(worksheet, game_name, tag_line, puuid_cache):
    if not worksheet: st.error(f"Invalid worksheet {game_name}#{tag_line}."); return []
    puu_id = puuid_cache.get(f"{game_name}#{tag_line}"); rc, stm = 0, time.time()
    if not puu_id:
        try: url = SUMMONER_NAME_BY_URL.format(game_name, tag_line); resp = requests.get(url, timeout=10); rc += 1; rc, stm = rate_limit_pause(stm, rc); resp.raise_for_status(); data = resp.json(); puu_id = data.get("puuid");
        if puu_id: puuid_cache[f"{game_name}#{tag_line}"] = puu_id; else: st.error(f"PUUID not found {game_name}#{tag_line}."); return []
        except Exception as e: st.error(f"Err PUUID {game_name}#{tag_line}: {e}"); return []
    try: existing = set(worksheet.col_values(2)[1:])
    except Exception as e: st.error(f"Err get existing matches: {e}"); existing = set()
    try: url = MATCH_HISTORY_URL.format(puu_id); resp = requests.get(url, timeout=15); rc += 1; rc, stm = rate_limit_pause(stm, rc); resp.raise_for_status(); recent = resp.json();
    if not isinstance(recent, list): st.error(f"Bad history format {game_name}"); return []
    except Exception as e: st.error(f"Err fetch history {game_name}: {e}"); return []
    new_rows = []; fetch_list = [m for m in recent if m not in existing]
    if not fetch_list: return []
    # st.info(f"Fetch {len(fetch_list)} matches for {game_name}#{tag_line}...") # Less verbose
    for g_id in fetch_list:
        try:
            url = MATCH_BASIC_URL.format(g_id); resp = requests.get(url, timeout=10); rc += 1; rc, stm = rate_limit_pause(stm, rc); resp.raise_for_status(); match = resp.json()
            if not match or 'info' not in match or 'participants' not in match['info'] or 'metadata' not in match or 'participants' not in match['metadata']: continue
            puuids = match['metadata']['participants'];
            if puu_id not in puuids: continue
            p_idx = puuids.index(puu_id); p_data = match['info']['participants'][p_idx]
            champ = p_data.get('championName','Unk'); k,d,a = p_data.get('kills',0), p_data.get('deaths',0), p_data.get('assists',0); pos = p_data.get('individualPosition', p_data.get('teamPosition','UNK')).upper(); win = 1 if p_data.get("win", False) else 0; created = match['info'].get('gameCreation')
            date_s = datetime.fromtimestamp(created / 1000).strftime('%Y-%m-%d %H:%M:%S') if created else "N/A"
            new_rows.append([date_s, g_id, str(win), champ, pos, str(k), str(d), str(a)])
        except requests.exceptions.RequestException as e: st.error(f"Err detail {g_id}:{e}");
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 429: time.sleep(30); rc=0; stm=time.time()
        except Exception as e: st.error(f"Err process {g_id}:{e}")
    if new_rows: try: worksheet.append_rows(new_rows, value_input_option='USER_ENTERED'); # st.success(f"Added {len(new_rows)} for {game_name}.") # Less verbose
    except Exception as e: st.error(f"Fail append {game_name}:{e}")
    return new_rows

@st.cache_data(ttl=300)
def aggregate_soloq_data_from_sheet(spreadsheet, team_name):
    if not spreadsheet: return {}
    agg = defaultdict(lambda: defaultdict(lambda: {"count":0,"wins":0,"kills":0,"deaths":0,"assists":0})); cfg = team_rosters.get(team_name,{});
    if not cfg: return {}
    for p, info in cfg.items():
        role = info.get("role","UNK").upper()
        try:
            wks = spreadsheet.worksheet(p); vals = wks.get_all_values();
            if len(vals) <= 1: continue
            hdr = vals[0];
            try: wc,cc,rc,kc,dc,ac = hdr.index("Победа"), hdr.index("Чемпион"), hdr.index("Роль"), hdr.index("Киллы"), hdr.index("Смерти"), hdr.index("Ассисты")
            except ValueError as e: st.error(f"Miss col '{p}':{e}"); continue
            for row in vals[1:]:
                 if len(row) <= max(wc,cc,rc,kc,dc,ac): continue
                 try:
                    ws,ch,rl = row[wc], row[cc], row[rc].upper(); ks,ds,ass = row[kc], row[dc], row[ac]
                    if rl == role: k = int(ks) if ks.isdigit() else 0; d = int(ds) if ds.isdigit() else 0; a = int(ass) if ass.isdigit() else 0; win = 1 if ws=='1' else 0;
                    agg[p][ch]["wins"]+=win; agg[p][ch]["count"]+=1; agg[p][ch]["kills"]+=k; agg[p][ch]["deaths"]+=d; agg[p][ch]["assists"]+=a
                 except (ValueError, IndexError): continue
        except gspread.exceptions.WorksheetNotFound: continue
        except Exception as e: st.error(f"Err process sheet '{p}':{e}"); continue
    for p in agg: agg[p] = dict(sorted(agg[p].items(), key=lambda item: item[1]["count"], reverse=True))
    return dict(agg)

# --- Notes Saving/Loading ---
NOTES_DIR = "notes_data"; os.makedirs(NOTES_DIR, exist_ok=True)
def get_notes_filepath(t_name, pfx="notes"): safe = "".join(c if c.isalnum() else "_" for c in t_name); return os.path.join(NOTES_DIR, f"{pfx}_{safe}.json")
def save_notes_data(data, t_name): path = get_notes_filepath(t_name); try: f=open(path,"w",encoding="utf-8"); json.dump(data,f,indent=4); f.close(); except Exception as e: st.error(f"Err save notes {t_name}: {e}")
def load_notes_data(t_name):
    path=get_notes_filepath(t_name); default={"tables":[[["", "Ban", ""],["", "Ban", ""],["", "Ban", ""],["", "Pick", ""],["", "Pick", ""],["", "Pick", ""],["", "Ban", ""],["", "Ban", ""],["", "Pick", ""],["", "Pick", ""]]*6],"notes_text":""}
    if os.path.exists(path): try: f=open(path,"r",encoding="utf-8"); loaded=json.load(f); f.close(); return loaded if "tables" in loaded and "notes_text" in loaded else default; except Exception: return default
    else: return default

# --- Streamlit Page Functions ---

# !!! ПОЛНАЯ hll_page !!!
def hll_page(selected_team):
    st.title(f"Hellenic Legends League - Team Analysis"); st.header(f"Team: {selected_team}")
    if st.button("🔄 Refresh HLL Data", key="refresh_hll"):
        with st.spinner("Fetching HLL data..."):
            try: st.session_state.match_history_data = fetch_match_history_data(); st.session_state.draft_data = fetch_draft_data(); st.success("HLL refreshed!")
            except Exception as e: st.error(f"Failed refresh: {e}")
        st.rerun()
    match_data = st.session_state.get('match_history_data', {}); draft_data_all = st.session_state.get('draft_data', {})
    team_match = match_data.get(selected_team, {}); team_draft = draft_data_all.get(selected_team, [])
    if not team_match and not team_draft: st.warning(f"No HLL data for '{selected_team}'."); return
    st.subheader("Overall Performance"); cols = st.columns(4); tg, tw = team_match.get('matches_played', 0), team_match.get('wins', 0); wr = (tw / tg * 100) if tg > 0 else 0
    bg, bw = team_match.get('blue_side_games', 0), team_match.get('blue_side_wins', 0); bwr = (bw / bg * 100) if bg > 0 else 0
    rg, rw = team_match.get('red_side_games', 0), team_match.get('red_side_wins', 0); rwr = (rw / rg * 100) if rg > 0 else 0
    cols[0].metric("Games", tg); cols[1].metric("Win Rate", f"{wr:.1f}%", f"{tw}W-{tg-tw}L"); cols[2].metric("Blue WR", f"{bwr:.1f}%", f"{bw}W-{bg-bw}L ({bg}G)"); cols[3].metric("Red WR", f"{rwr:.1f}%", f"{rw}W-{rg-rw}L ({rg}G)"); st.divider()
    st.subheader("View Sections"); show_picks = st.toggle("Picks", key="toggle_picks", value=True); show_bans = st.toggle("Bans", key="toggle_bans"); show_duos = st.toggle("Duos", key="toggle_duos"); show_drafts = st.toggle("Drafts", key="toggle_drafts"); show_notes = st.toggle("Notes", key="toggle_notes"); st.divider()
    if show_picks:
        st.subheader("Picks by Role"); roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']; p_cols = st.columns(len(roles))
        for i, r in enumerate(roles):
            with p_cols[i]: st.markdown(f"**{r}**"); picks = team_match.get(r, {}); stats = []
            for c, d in picks.items():
                if c != "N/A" and d.get('games', 0) > 0: wrate = (d['wins'] / d['games'] * 100) if d['games'] > 0 else 0; stats.append({'Icon': get_champion_icon_html(c, 25, 25), 'Games': d['games'], 'WR%': wrate})
            if stats: df = pd.DataFrame(stats).sort_values('Games', ascending=False).reset_index(drop=True); df['WR%'] = df['WR%'].apply(color_win_rate); st.markdown(df.to_html(escape=False, index=False, classes='compact-table', justify='center'), unsafe_allow_html=True)
            else: st.caption("N/A.")
        st.divider()
    if show_bans:
        st.subheader("Bans Analysis"); b_cols = st.columns(2)
        with b_cols[0]: st.markdown("**Bans by Team**"); bans = team_match.get('Bans', {}); stats = [{'Icon': get_champion_icon_html(c, 25, 25), 'Count': n} for c, n in bans.items() if c != "N/A"] if bans else [];
        if stats: df = pd.DataFrame(stats).sort_values('Count', ascending=False).reset_index(drop=True); st.markdown(df.to_html(escape=False, index=False, classes='compact-table'), unsafe_allow_html=True); else: st.caption("N/A.")
        with b_cols[1]: st.markdown("**Bans by Opponents**"); o_bans = team_match.get('OpponentBansAgainst', {}); stats = [{'Icon': get_champion_icon_html(c, 25, 25), 'Count': n} for c, n in o_bans.items() if c != "N/A"] if o_bans else [];
        if stats: df = pd.DataFrame(stats).sort_values('Count', ascending=False).reset_index(drop=True); st.markdown(df.to_html(escape=False, index=False, classes='compact-table'), unsafe_allow_html=True); else: st.caption("N/A.")
        st.divider()
    if show_duos:
        st.subheader("Duo Synergy"); duos = team_match.get('DuoPicks', {}); pairs = {"Top/Jg": ('Top', 'Jungle'), "Jg/Mid": ('Jungle', 'Mid'), "Jg/Sup": ('Jungle', 'Support'), "Bot": ('ADC', 'Support')}; d_cols = st.columns(len(pairs)); c_idx = 0
        for title, (r1t, r2t) in pairs.items():
            with d_cols[c_idx]: st.markdown(f"**{title}**"); stats = []
            for key, data in duos.items():
                (c1, r1), (c2, r2) = key;
                if {r1, r2} == {r1t, r2t}: wrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0; i1, i2 = (get_champion_icon_html(c1, 25, 25), get_champion_icon_html(c2, 25, 25)) if r1 == r1t else (get_champion_icon_html(c2, 25, 25), get_champion_icon_html(c1, 25, 25)); stats.append({f'{r1t}': i1, f'{r2t}': i2, 'Games': data['games'], 'WR%': wrate})
            if stats: df = pd.DataFrame(stats).sort_values('Games', ascending=False).reset_index(drop=True); df['WR%'] = df['WR%'].apply(color_win_rate); st.markdown(df.to_html(escape=False, index=False, classes='compact-table', justify='center'), unsafe_allow_html=True)
            else: st.caption("N/A.")
            c_idx += 1
        st.divider()
    if show_drafts:
        st.subheader("Detailed Drafts");
        if team_draft:
            grouped = defaultdict(list);
            for d in team_draft: grouped[(d['tournament'], d['match_key'])].append(d)
            s_matches = sorted(grouped.items(), key=lambda item: (item[0][0], min(d['match_number'] for d in item[1])), reverse=True); opts = [f"{t} - {mk[0]} vs {mk[1]}" for (t, mk), _ in s_matches]
            if not opts: st.info("No drafts.");
            sel_match = st.selectbox("Select Match:", opts, index=0 if opts else None)
            if sel_match:
                s_drafts = [];
                for (t, mk), drfts in s_matches:
                     if f"{t} - {mk[0]} vs {mk[1]}" == sel_match: s_drafts = sorted(drfts, key=lambda d: d['match_number']); break
                if s_drafts:
                    st.markdown(f"**{sel_match} (G{s_drafts[0]['match_number']} - G{s_drafts[-1]['match_number']})**"); d_cols = st.columns(len(s_drafts))
                    for i, d in enumerate(s_drafts):
                        with d_cols[i]:
                            is_b = (d['blue_team'] == selected_team); opp = d['red_team'] if is_b else d['blue_team']; res = "Win" if (d['winner_side'] == 'blue' and is_b) or (d['winner_side'] == 'red' and not is_b) else "Loss"; color = "lightgreen" if res == "Win" else "lightcoral"
                            st.markdown(f"**G{d['match_number']}** (<span style='color:{color};'>{res}</span> vs {opp})", unsafe_allow_html=True);
                            if d['vod_link'] != "N/A": st.link_button("VOD", d['vod_link'], use_container_width=True)
                            actions = sorted(d['draft_actions'], key=lambda x: x.get('sequence', 99)) # Sort by sequence
                            rows = []; # B1, R1, B2, R2, B3, R3 | P1, P2, P3, P4, P5 | B4, R4, B5, R5 | P6, P7, P8, P9, P10
                            blue_row, red_row = [""]*12, [""]*12 # 6 bans + 6 picks ~
                            # Try to reconstruct based on sequence (adjust indices as needed)
                            for action in actions:
                                seq = action.get('sequence', 0); icon = get_champion_icon_html(action['champion'], 20, 20)
                                if action['type'] == 'ban':
                                    idx = [0,1,2,6,7][([1,3,5,14,16].index(seq) if seq in [1,3,5,14,16] else -1) if action['side']=='blue' else ([2,4,6,13,15].index(seq) if seq in [2,4,6,13,15] else -1)] if 'index' in locals() and idx != -1 else -1 # Complex ban index mapping (might need simplification)
                                    if idx != -1:
                                        if action['side'] == 'blue': blue_row[idx] = icon
                                        else: red_row[idx] = icon
                                elif action['type'] == 'pick':
                                     idx = [3,4,5,8,9][([7,10,11,18,19].index(seq) if seq in [7,10,11,18,19] else -1) if action['side']=='blue' else ([8,9,12,17,20].index(seq) if seq in [8,9,12,17,20] else -1)] if 'index' in locals() and idx != -1 else -1
                                     if idx != -1:
                                         if action['side'] == 'blue': blue_row[idx] = icon
                                         else: red_row[idx] = icon

                            df_d = pd.DataFrame([blue_row, red_row], index=[d['blue_team'], d['red_team']], columns=["B1","B2","B3","P1","P2","P3","B4","B5","P4","P5","",""]) # Simplified cols
                            st.markdown(df_d.to_html(escape=False, classes='compact-table draft-view', justify='center'), unsafe_allow_html=True)
        else: st.info(f"No draft data for {selected_team}.")
        st.divider()
    if show_notes:
        st.subheader("Notes & Templates"); key = f'notes_data_{selected_team}';
        if key not in st.session_state: st.session_state[key] = load_notes_data(selected_team)
        data = st.session_state[key]; cols_t, col_n = st.columns([3, 1])
        with cols_t:
            st.markdown("**Templates**"); num = len(data.get("tables", [])); t_cols = st.columns(3)
            for i in range(num):
                 with t_cols[i % 3]:
                    st.markdown(f"*T{i+1}*"); tbl = data["tables"][i];
                    if not isinstance(tbl, list) or not all(isinstance(row, list) for row in tbl): tbl = [ ["", "Ban", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Pick", ""], ["", "Ban", ""], ["", "Ban", ""], ["", "Pick", ""], ["", "Pick", ""] ]; data["tables"][i] = tbl
                    df = pd.DataFrame(tbl, columns=["T1", "Act", "T2"]); e_key = f"notes_table_{selected_team}_{i}"
                    edited = st.data_editor(df, num_rows="fixed", use_container_width=True, key=e_key, height=385, column_config={"T1": st.column_config.TextColumn("T1"), "Act": st.column_config.TextColumn(disabled=True), "T2": st.column_config.TextColumn("T2")})
                    if not edited.equals(df): st.session_state[key]["tables"][i] = edited.values.tolist(); save_notes_data(st.session_state[key], selected_team)
        with col_n:
            st.markdown("**Notes**"); n_key = f"notes_text_area_{selected_team}"; txt = st.text_area("N:", value=data.get("notes_text", ""), height=400, key=n_key, label_visibility="collapsed")
            if txt != data.get("notes_text", ""): st.session_state[key]["notes_text"] = txt; save_notes_data(st.session_state[key], selected_team)
        st.divider()

# !!! ПОЛНАЯ soloq_page !!!
def soloq_page():
    st.title("Gamespace - SoloQ Player Statistics"); client = setup_google_sheets_client();
    if not client: st.error("GSheets connect failed."); return
    try: sheet = client.open(SOLOQ_SHEET_NAME)
    except Exception as e: st.error(f"Sheet access error: {e}"); return
    if 'puuid_cache' not in st.session_state: st.session_state.puuid_cache = {}
    if st.button("🔄 Update SoloQ Data", key="update_soloq"):
        new_matches_count = 0
        with st.spinner("Updating SoloQ..."):
            roster = team_rosters.get("Gamespace", {});
            if not roster: st.error("GMS roster missing.")
            else:
                n_players = len(roster); pb = st.progress(0, text="Starting..."); updated = 0
                for player, p_info in roster.items():
                    pb.progress((updated + 1) / n_players, text=f"Updating {player}..."); wks = check_if_soloq_worksheet_exists(sheet, player)
                    if not wks: st.error(f"Worksheet fail {player}."); continue
                    names, tags = p_info.get("game_name", []), p_info.get("tag_line", [])
                    if len(names) != len(tags): st.warning(f"Name/Tag mismatch {player}."); continue
                    for name, tag in zip(names, tags):
                         if name and tag: new = get_account_data_from_riot(wks, name, tag, st.session_state.puuid_cache); new_matches_count += len(new)
                    updated += 1
                pb.progress(1.0, text="Done!"); time.sleep(2); pb.empty()
        if new_matches_count > 0: st.success(f"Added {new_matches_count} matches."); aggregate_soloq_data_from_sheet.clear() # Clear cache
        else: st.info("No new matches.")
    st.subheader("Player Stats (Sheets Data)")
    try:
        agg_data = aggregate_soloq_data_from_sheet(sheet, "Gamespace"); # Use cached data
        if not agg_data: st.warning("No aggregated SoloQ data.")
        else:
            players = list(agg_data.keys()); p_cols = st.columns(len(players) if players else 1)
            for i, p in enumerate(players):
                with p_cols[i]:
                    st.markdown(f"**{p}** ({team_rosters['Gamespace'][p]['role']})"); p_stats = agg_data.get(p, {}); stats = []; tg, tw = 0, 0
                    for c, s_dict in p_stats.items():
                         games = s_dict.get("count", 0)
                         if games > 0:
                            wins, k, d, a = s_dict.get("wins", 0), s_dict.get("kills", 0), s_dict.get("deaths", 1), s_dict.get("assists", 0)
                            tg += games; tw += wins; wr = round((wins / games) * 100, 1) if games > 0 else 0; kda = round((k + a) / max(d, 1), 2)
                            stats.append({'Icon': get_champion_icon_html(c, 20, 20), 'Games': games, 'WR%': wr, 'KDA': kda})
                    p_wr = (tw / tg * 100) if tg > 0 else 0; st.caption(f"Overall: {tw}W-{tg-tw}L ({p_wr:.1f}%)")
                    if stats:
                        df = pd.DataFrame(stats).sort_values("Games", ascending=False).reset_index(drop=True); df['WR%'] = df['WR%'].apply(color_win_rate); df['KDA'] = df['KDA'].apply(lambda x: f"{x:.2f}")
                        st.markdown(df.to_html(escape=False, index=False, classes='compact-table soloq-stats', justify='center'), unsafe_allow_html=True)
                    else: st.caption(f"N/A.")
    except Exception as e: st.error(f"Error display SoloQ: {e}")

# --- Main Application Logic ---
def main():
    if 'current_page' not in st.session_state: st.session_state.current_page = "Hellenic Legends League Stats"
    st.sidebar.title("Navigation"); current_page = st.session_state.current_page
    if current_page != "Hellenic Legends League Stats":
        if st.sidebar.button("🏆 HLL Stats", key="nav_hll", use_container_width=True): st.session_state.current_page = "Hellenic Legends League Stats"; st.rerun()
    if current_page != "GMS SoloQ":
        if st.sidebar.button("🎮 GMS SoloQ", key="nav_soloq", use_container_width=True): st.session_state.current_page = "GMS SoloQ"; st.rerun()
    if current_page != "Scrims":
         if st.sidebar.button("⚔️ Scrims", key="nav_scrims", use_container_width=True): st.session_state.current_page = "Scrims"; st.rerun()
    st.sidebar.divider()
    hll_data_loaded = ('match_history_data' in st.session_state and st.session_state.match_history_data and 'draft_data' in st.session_state and st.session_state.draft_data)
    if not hll_data_loaded and current_page == "Hellenic Legends League Stats":
        st.sidebar.info("Loading HLL data...")
        with st.spinner("Loading HLL data..."):
            try: st.session_state.match_history_data = fetch_match_history_data(); st.session_state.draft_data = fetch_draft_data(); st.sidebar.success("HLL loaded."); time.sleep(1); st.rerun()
            except Exception as e: st.error(f"Error fetch HLL: {e}")
            if 'match_history_data' not in st.session_state: st.session_state.match_history_data = defaultdict(dict)
            if 'draft_data' not in st.session_state: st.session_state.draft_data = defaultdict(list)
    selected_hll_team = None
    if st.session_state.get('match_history_data') or st.session_state.get('draft_data'):
        all_teams=set(); teams=[]
        if isinstance(st.session_state.get('match_history_data'), dict): all_teams.update(normalize_team_name(t) for t in st.session_state.match_history_data.keys())
        if isinstance(st.session_state.get('draft_data'), dict): all_teams.update(normalize_team_name(t) for t in st.session_state.draft_data.keys())
        teams = sorted([t for t in all_teams if t != "unknown"])
        if teams: selected_hll_team = st.sidebar.selectbox("Select HLL Team:", teams, key="hll_team_select", index=teams.index("Gamespace") if "Gamespace" in teams else 0)
        else: st.sidebar.warning("No HLL teams.")
    elif current_page == "Hellenic Legends League Stats": st.sidebar.warning("HLL loading...")
    st.sidebar.divider();
    try: st.sidebar.image("logo.webp", width=100)
    except Exception: st.sidebar.caption("Logo missing")
    st.sidebar.markdown("""<div style='text-align:center; font-size:12px; color:#888;'>App by heovech<br><a href='#' style='color:#888;'>Contact</a></div>""", unsafe_allow_html=True)
    if current_page == "Hellenic Legends League Stats":
        if selected_hll_team: hll_page(selected_hll_team)
        else: st.info("Select HLL team or wait.")
    elif current_page == "GMS SoloQ": soloq_page()
    elif current_page == "Scrims":
        scrims.scrims_page() # <--- ИЗМЕНЕНИЕ: Вызываем функцию через модуль

# --- Authentication ---
try:
    with open('config.yaml') as file: config = yaml.load(file, Loader=SafeLoader)
except Exception as e: st.error(f"FATAL: config.yaml error: {e}"); st.stop()
if not isinstance(config, dict) or 'credentials' not in config or 'cookie' not in config or not all(k in config['cookie'] for k in ['name', 'key', 'expiry_days']):
    st.error("FATAL: config.yaml invalid."); st.stop()
authenticator = stauth.Authenticate(config['credentials'], config['cookie']['name'], config['cookie']['key'], config['cookie']['expiry_days'])
if 'authentication_status' not in st.session_state: st.session_state.authentication_status = None
if 'name' not in st.session_state: st.session_state.name = None
if 'username' not in st.session_state: st.session_state.username = None
login_placeholder = st.empty()
if st.session_state.authentication_status is None:
    with login_placeholder.container():
        try: name, authentication_status, username = authenticator.login(location='main')
        st.session_state.name, st.session_state.authentication_status, st.session_state.username = name, authentication_status, username
        except KeyError as e: st.error(f"Auth Error key {e}"); st.stop()
        except Exception as e: st.error(f"Login Error: {e}"); st.stop()
if st.session_state.authentication_status:
    login_placeholder.empty();
    with st.sidebar: st.sidebar.divider(); st.sidebar.write(f'Welcome *{st.session_state.name}*'); authenticator.logout('Logout', 'sidebar', key='logout_button')
    try:
        with open("style.css") as f: st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError: pass
    if __name__ == "__main__": main()
elif st.session_state.authentication_status is False: st.error('Username/password incorrect')
elif st.session_state.authentication_status is None: pass

# --- END OF FILE app.py ---
