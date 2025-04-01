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
from scrims import scrims_page
import scrims# Импорт функции scrims_page из файла scrims.py

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
def get_latest_patch_version():
    try:
        response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json")
        if response.status_code == 200:
            versions = response.json()
            return versions[0]
        return "14.5.1"
    except:
        return "14.5.1"

PATCH_VERSION = get_latest_patch_version()

# Normalize team names
def normalize_team_name(team_name):
    if not team_name or team_name.lower() == "unknown blue" or team_name.lower() == "unknown red":
        return "unknown"
    
    team_exceptions = {
        "gamespace": "Gamespace",
        "gms": "Gamespace",
        "gamespace logo std": "Gamespace",
        # Добавьте другие команды HLL при необходимости
    }

    team_name_clean = team_name.lower().replace("logo std", "").strip()
    
    for key, normalized_name in team_exceptions.items():
        if team_name_clean == key or key in team_name_clean:
            return normalized_name
    
    return team_name_clean

# Fetch match history data
def fetch_match_history_data():
    team_data = defaultdict(lambda: {
        'Top': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Jungle': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Mid': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'ADC': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Support': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'Bans': defaultdict(int),
        'OpponentBlueBans': defaultdict(int),
        'OpponentRedBans': defaultdict(int),
        'DuoPicks': defaultdict(lambda: {'games': 0, 'wins': 0}),
        'MatchResults': []
    })

    match_counter = defaultdict(int)

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls["match_history"]
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            st.error(f"Failed to load {tournament_name} Match History page (code {response.status_code})")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        try:
            match_history_table = soup.select('.wikitable.mhgame.sortable')[0]
        except IndexError:
            st.error(f"Could not find match history table for {tournament_name}")
            continue
        
        for row in match_history_table.select('tr')[1:]:
            cols = row.select('td')
            if not cols:
                continue

            blue_team_elem = cols[2].select_one('a[title]') if len(cols) > 2 else None
            red_team_elem = cols[3].select_one('a[title]') if len(cols) > 3 else None
            
            blue_team = (blue_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0] if blue_team_elem and 'title' in blue_team_elem.attrs 
                        else blue_team_elem.text.strip().lower() if blue_team_elem 
                        else cols[2].text.strip().lower() if len(cols) > 2 else "unknown blue")
            red_team = (red_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0] if red_team_elem and 'title' in red_team_elem.attrs 
                       else red_team_elem.text.strip().lower() if red_team_elem 
                       else cols[3].text.strip().lower() if len(cols) > 3 else "unknown red")

            blue_team = normalize_team_name(blue_team)
            red_team = normalize_team_name(red_team)

            if blue_team == "unknown" or red_team == "unknown":
                continue

            winner_team = "unknown"
            result_elem = cols[4].select_one('a[title]') if len(cols) > 4 else None
            if result_elem and 'title' in result_elem.attrs:
                winner_team = normalize_team_name(result_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0])
            else:
                result_text = cols[4].text.strip().lower() if len(cols) > 4 else ""
                if result_text == "1:0":
                    winner_team = blue_team
                elif result_text == "0:1":
                    winner_team = red_team

            if winner_team == "unknown":
                result_blue = 'Loss'
                result_red = 'Loss'
            else:
                result_blue = 'Win' if winner_team == blue_team else 'Loss'
                result_red = 'Win' if winner_team == red_team else 'Loss'

            match_key = tuple(sorted([blue_team, red_team]))
            match_counter[match_key] += 1
            match_number = match_counter[match_key]
            team_data[blue_team]['MatchResults'].append({
                'match_key': match_key,
                'match_number': match_number,
                'side': 'blue',
                'opponent': red_team,
                'win': result_blue == 'Win',
                'tournament': tournament_name
            })
            team_data[red_team]['MatchResults'].append({
                'match_key': match_key,
                'match_number': match_number,
                'side': 'red',
                'opponent': blue_team,
                'win': result_red == 'Win',
                'tournament': tournament_name
            })

            blue_bans_elem = cols[5].select('span.sprite.champion-sprite') if len(cols) > 5 else []
            red_bans_elem = cols[6].select('span.champion-sprite') if len(cols) > 6 else []

            for team, bans in [(blue_team, blue_bans_elem), (red_team, red_bans_elem)]:
                for ban in bans:
                    champion = get_champion(ban)
                    if champion:
                        team_data[team]['Bans'][champion] += 1

            for team, opponent, opponent_bans in [(blue_team, red_team, red_bans_elem), (red_team, blue_team, blue_bans_elem)]:
                for ban in opponent_bans[:3]:
                    champion = get_champion(ban)
                    if champion:
                        if team == blue_team:
                            team_data[team]['OpponentBlueBans'][champion] += 1
                        else:
                            team_data[team]['OpponentRedBans'][champion] += 1

            blue_picks_elem = cols[7].select('span.sprite.champion-sprite') if len(cols) > 7 else []
            red_picks_elem = cols[8].select('span.sprite.champion-sprite') if len(cols) > 8 else []

            roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']
            blue_picks = {role: get_champion(pick) for role, pick in zip(roles, blue_picks_elem) if pick}
            red_picks = {role: get_champion(pick) for role, pick in zip(roles, red_picks_elem) if pick}

            for team, picks, result in [(blue_team, blue_picks, result_blue), (red_team, red_picks, result_red)]:
                for role in roles:
                    champion = picks.get(role, "")
                    if champion:
                        team_data[team][role][champion]['games'] += 1
                        if result == 'Win':
                            team_data[team][role][champion]['wins'] += 1
                    else:
                        if role not in team_data[team] or not any(data['games'] > 0 for data in team_data[team][role].values()):
                            team_data[team][role]["N/A"]['games'] += 1
                            if result == 'Win':
                                team_data[team][role]["N/A"]['wins'] += 1

                duo_pairs = [('Top', 'Jungle'), ('Jungle', 'Mid'), ('Jungle', 'Support'), ('ADC', 'Support')]
                for role1, role2 in duo_pairs:
                    champ1 = picks.get(role1, "N/A")
                    champ2 = picks.get(role2, "N/A")
                    if champ1 != "N/A" and champ2 != "N/A":
                        duo_key = (champ1, champ2, role1, role2)
                        team_data[team]['DuoPicks'][duo_key]['games'] += 1
                        if result == 'Win':
                            team_data[team]['DuoPicks'][duo_key]['wins'] += 1

    return dict(team_data)

# Fetch first bans data
def fetch_first_bans_data():
    team_data = defaultdict(lambda: {
        'BlueFirstBans': defaultdict(int),
        'RedFirstBans': defaultdict(int)
    })

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls["picks_and_bans"]
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            st.error(f"Failed to load {tournament_name} Picks and Bans page (code {response.status_code})")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        picks_bans_tables = soup.select('.wikitable.plainlinks.hoverable-rows.column-show-hide-1')
        if not picks_bans_tables:
            st.warning(f"Picks and Bans table not found for {tournament_name}")
            continue

        picks_bans_table = picks_bans_tables[0]
        for row in picks_bans_table.select('tr')[1:]:
            cols = row.select('td')
            if not cols or len(cols) < 11:
                continue

            blue_team = "unknown blue"
            red_team = "unknown red"

            if len(cols) > 1 and 'title' in cols[1].attrs:
                blue_team = cols[1]['title'].strip().lower()
            if len(cols) > 2 and 'title' in cols[2].attrs:
                red_team = cols[2]['title'].strip().lower()

            if blue_team == "unknown blue":
                blue_team_elem = cols[1].select_one('.to_hasTooltip') if len(cols) > 1 else None
                if blue_team_elem and 'title' in blue_team_elem.attrs:
                    blue_team = blue_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0]
                elif blue_team_elem:
                    blue_team = blue_team_elem.text.strip().lower()

            if red_team == "unknown red":
                red_team_elem = cols[2].select_one('.to_hasTooltip') if len(cols) > 2 else None
                if red_team_elem and 'title' in red_team_elem.attrs:
                    red_team = red_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0]
                elif red_team_elem:
                    red_team = red_team_elem.text.strip().lower()

            if blue_team == "unknown blue":
                blue_team_img = cols[1].select_one('img') if len(cols) > 1 else None
                if blue_team_img and 'alt' in blue_team_img.attrs:
                    blue_team = blue_team_img['alt'].strip().lower()

            if red_team == "unknown red":
                red_team_img = cols[2].select_one('img') if len(cols) > 2 else None
                if red_team_img and 'alt' in red_team_img.attrs:
                    red_team = red_team_img['alt'].strip().lower()

            if blue_team == "unknown blue":
                blue_team = cols[1].text.strip().lower() if len(cols) > 1 else "unknown blue"
            if red_team == "unknown red":
                red_team = cols[2].text.strip().lower() if len(cols) > 2 else "unknown red"

            if not blue_team or blue_team.isspace():
                blue_team = "unknown blue"
            if not red_team or red_team.isspace():
                red_team = "unknown red"

            blue_team = normalize_team_name(blue_team)
            red_team = normalize_team_name(red_team)

            if blue_team == "unknown" or red_team == "unknown":
                continue

            ban_columns = ['BB1', 'RB1', 'BB2', 'RB2', 'BB3', 'RB3']
            for i, ban_col in enumerate(ban_columns):
                col_index = 5 + i
                ban_elem = cols[col_index].select_one('span.sprite.champion-sprite') if len(cols) > col_index else None
                champion = get_champion(ban_elem) if ban_elem else None
                if champion and champion != "N/A":
                    if ban_col.startswith('BB'):
                        team_data[blue_team]['BlueFirstBans'][champion] += 1
                    elif ban_col.startswith('RB'):
                        team_data[red_team]['RedFirstBans'][champion] += 1

    return dict(team_data)

# Fetch draft data
def fetch_draft_data():
    team_drafts = defaultdict(list)
    match_counter = defaultdict(int)
    team_wins = defaultdict(int)

    for tournament_name, urls in TOURNAMENT_URLS.items():
        url = urls["picks_and_bans"]
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            st.error(f"Failed to load {tournament_name} Picks and Bans page (code {response.status_code})")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')
        draft_tables = soup.select('table.wikitable.plainlinks.hoverable-rows.column-show-hide-1')
        if not draft_tables:
            st.warning(f"Draft tables not found on the {tournament_name} page.")
            continue
        
        for table in draft_tables:
            rows = table.select('tr')[1:]
            rows = list(reversed(rows))
            
            for row in rows:
                cols = row.select('td')
                if len(cols) < 24:
                    continue

                blue_team = "unknown blue"
                red_team = "unknown red"

                if len(cols) > 1 and 'title' in cols[1].attrs:
                    blue_team = cols[1]['title'].strip().lower()
                if len(cols) > 2 and 'title' in cols[2].attrs:
                    red_team = cols[2]['title'].strip().lower()

                if blue_team == "unknown blue":
                    blue_team_elem = cols[1].select_one('.to_hasTooltip') if len(cols) > 1 else None
                    if blue_team_elem and 'title' in blue_team_elem.attrs:
                        blue_team = blue_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0]
                    elif blue_team_elem:
                        blue_team = blue_team_elem.text.strip().lower()

                if red_team == "unknown red":
                    red_team_elem = cols[2].select_one('.to_hasTooltip') if len(cols) > 2 else None
                    if red_team_elem and 'title' in red_team_elem.attrs:
                        red_team = red_team_elem['title'].strip().lower().replace("||tooltip:", "").split("||")[0]
                    elif red_team_elem:
                        red_team = red_team_elem.text.strip().lower()

                if blue_team == "unknown blue":
                    blue_team_img = cols[1].select_one('img') if len(cols) > 1 else None
                    if blue_team_img and 'alt' in blue_team_img.attrs:
                        blue_team = blue_team_img['alt'].replace('logo std', '').strip().lower()

                if red_team == "unknown red":
                    red_team_img = cols[2].select_one('img') if len(cols) > 2 else None
                    if red_team_img and 'alt' in red_team_img.attrs:
                        red_team = red_team_img['alt'].replace('logo std', '').strip().lower()

                if blue_team == "unknown blue":
                    blue_team = cols[1].text.strip().lower() if len(cols) > 1 else "unknown blue"
                if red_team == "unknown red":
                    red_team = cols[2].text.strip().lower() if len(cols) > 2 else "unknown red"

                if not blue_team or blue_team.isspace():
                    blue_team = "unknown blue"
                if not red_team or red_team.isspace():
                    red_team = "unknown red"

                blue_team = normalize_team_name(blue_team)
                red_team = normalize_team_name(red_team)

                if blue_team == "unknown" or red_team == "unknown":
                    continue

                winner_team = red_team if cols[2].get('class') and 'pbh-winner' in cols[2]['class'] else blue_team if cols[1].get('class') and 'pbh-winner' in cols[1]['class'] else None
                winner_side = 'red' if winner_team == red_team else 'blue' if winner_team == blue_team else None

                match_key = tuple(sorted([blue_team, red_team]))
                match_counter[match_key] += 1
                match_number = match_counter[match_key]
                if winner_side == 'blue':
                    team_wins[blue_team] += 1
                elif winner_side == 'red':
                    team_wins[red_team] += 1

                blue_wins = team_wins[blue_team]
                red_wins = team_wins[red_team]

                ban_indices = [5, 6, 7, 8, 9, 10, 15, 16, 17, 18]
                blue_bans = []
                red_bans = []
                for i, idx in enumerate(ban_indices):
                    champ_span = cols[idx].select_one('.pbh-cn')
                    if champ_span:
                        nested_span = champ_span.select_one('.sprite.champion-sprite')
                        champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    else:
                        champ_span_alt = cols[idx].select_one('span.champion-sprite')
                        champ = champ_span_alt.get('title', 'N/A') if champ_span_alt else "N/A"
                    if i % 2 == 0:
                        blue_bans.append(champ)
                    else:
                        red_bans.append(champ)

                roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']
                blue_picks = []
                red_picks = []
                pick_spans = cols[11].select('.pbh-cn')
                if pick_spans:
                    champ_span = pick_spans[0]
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    blue_picks.append((champ, roles[0]))
                rp1_2 = cols[12].select('.pbh-cn')
                for i, champ_span in enumerate(rp1_2[:2]):
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    red_picks.append((champ, roles[i]))
                bp2_3 = cols[13].select('.pbh-cn')
                for i, champ_span in enumerate(bp2_3[:2]):
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    blue_picks.append((champ, roles[1 + i]))
                pick_spans = cols[14].select('.pbh-cn')
                if pick_spans:
                    champ_span = pick_spans[0]
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    red_picks.append((champ, roles[2]))
                pick_spans = cols[19].select('.pbh-cn')
                if pick_spans:
                    champ_span = pick_spans[0]
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    red_picks.append((champ, roles[3]))
                bp4_5 = cols[20].select('.pbh-cn')
                for i, champ_span in enumerate(bp4_5[:2]):
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    blue_picks.append((champ, roles[3 + i]))
                pick_spans = cols[21].select('.pbh-cn')
                if pick_spans:
                    champ_span = pick_spans[0]
                    nested_span = champ_span.select_one('.sprite.champion-sprite')
                    champ = nested_span['title'] if nested_span and 'title' in nested_span.attrs else champ_span.get('data-champion', 'N/A')
                    red_picks.append((champ, roles[4]))

                while len(blue_picks) < 5:
                    blue_picks.append(("N/A", roles[len(blue_picks)]))
                while len(red_picks) < 5:
                    red_picks.append(("N/A", roles[len(red_picks)]))

                vod_elem = cols[23].select_one('a')
                vod_link = vod_elem['href'] if vod_elem and 'href' in vod_elem.attrs else "N/A"

                draft_blue = {
                    'opponent': red_team,
                    'blue_team': blue_team,
                    'red_team': red_team,
                    'blue_bans': blue_bans,
                    'red_bans': red_bans,
                    'blue_picks': blue_picks,
                    'red_picks': red_picks,
                    'winner_side': winner_side,
                    'blue_wins': blue_wins,
                    'red_wins': red_wins,
                    'match_key': match_key,
                    'match_number': match_number,
                    'vod_link': vod_link,
                    'tournament': tournament_name
                }
                team_drafts[blue_team].append(draft_blue)

                draft_red = {
                    'opponent': blue_team,
                    'blue_team': blue_team,
                    'red_team': red_team,
                    'blue_bans': blue_bans,
                    'red_bans': red_bans,
                    'blue_picks': blue_picks,
                    'red_picks': red_picks,
                    'winner_side': winner_side,
                    'blue_wins': blue_wins,
                    'red_wins': red_wins,
                    'match_key': match_key,
                    'match_number': match_number,
                    'vod_link': vod_link,
                    'tournament': tournament_name
                }
                team_drafts[red_team].append(draft_red)

    return dict(team_drafts)

# Helper functions
def get_champion(span_tag):
    if span_tag and 'title' in span_tag.attrs:
        return span_tag['title']
    return "N/A"

def get_role_from_sprite(role_sprite):
    style = role_sprite['style']
    if "background-position:-32px -16px" in style:
        return "Top"
    elif "background-position:-32px -0px" in style:
        return "Jungle"
    elif "background-position:-48px -0px" in style:
        return "Mid"
    elif "background-position:-16px -0px" in style:
        return "ADC"
    elif "background-position:-16px -16px" in style:
        return "Support"
    return "Unknown"

def normalize_champion_name(champ):
    if champ == "N/A":
        return "N/A"
    champion_exceptions = {
        "Nunu & Willump": "Nunu",
        "Xin Zhao": "XinZhao",
        "Miss Fortune": "MissFortune",
        "Kai'Sa": "Kaisa",
        "Kha'Zix": "Khazix",
        "LeBlanc": "Leblanc",
        "Wukong": "MonkeyKing",
        "Cho'Gath": "Chogath",
        "Jarvan IV": "JarvanIV",
        "Ivern": "Ivern",
        "K'Sante": "KSante",
        "Renata Glasc": "Renata"
    }
    champ_clean = champ.strip().lower()
    for full_name, normalized_name in champion_exceptions.items():
        if champ_clean == full_name.lower() or champ_clean.replace(" ", "").replace("&", "").replace("'", "") == full_name.lower().replace(" ", "").replace("&", "").replace("'", ""):
            return normalized_name
    champ_normalized = champ.replace(" ", "").replace("'", "").replace(".", "").replace("&", "").replace("-", "")
    return ''.join(word.capitalize() for word in champ_normalized.split())

def get_champion_icon(champion):
    if champion == "N/A":
        return "N/A"
    normalized_champ = normalize_champion_name(champion)
    icon_url = f"https://ddragon.leagueoflegends.com/cdn/{PATCH_VERSION}/img/champion/{normalized_champ}.png"
    return f'<img src="{icon_url}" width="35" height="35" style="vertical-align: middle;">'

def color_win_rate(value):
    if 0 <= value < 50:
        return f'<span style="color:rgb(255, 251, 251)">{value:.2f}</span>'
    elif 50 <= value <= 53:
        return f'<span style="color:rgb(204, 204, 31)">{value:.2f}</span>'
    else:
        return f'<span style="color:rgb(245, 26, 11)">{value:.2f}</span>'

# NEW: SoloQ functions
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

def rate_limit_pause(start_time, request_count):
    REQUEST_LIMIT = 100
    TIME_WINDOW = 120
    if request_count >= REQUEST_LIMIT:
        elapsed_time = time.time() - start_time
        if elapsed_time < TIME_WINDOW:
            time.sleep(TIME_WINDOW - elapsed_time)
        return 0, time.time()
    return request_count, start_time

def get_account_data(worksheet, game_name, tag_line):
    game_ids = set(worksheet.col_values(2))
    request_count = 0
    start_time = time.time()

    response = requests.get(SUMMONER_NAME_BY_URL.format(game_name, tag_line))
    request_count += 1
    request_count, start_time = rate_limit_pause(start_time, request_count)

    if response.status_code == 200:
        data = response.json()
        puu_id = data["puuid"]
        match_history_response = requests.get(MATCH_HISTORY_URL.format(puu_id))
        request_count += 1
        request_count, start_time = rate_limit_pause(start_time, request_count)

        if match_history_response.status_code == 200:
            matches = match_history_response.json()
            new_data = []

            for game_id in matches:
                if game_id not in game_ids:
                    match_info_response = requests.get(MATCH_BASIC_URL.format(game_id))
                    request_count += 1
                    request_count, start_time = rate_limit_pause(start_time, request_count)

                    if match_info_response.status_code == 200:
                        match_data = match_info_response.json()
                        participants = match_data['metadata']['participants']
                        player_index = participants.index(puu_id)
                        player_data = match_data['info']['participants'][player_index]
                        champion_name = player_data['championName']
                        kills = player_data['kills']
                        deaths = player_data['deaths']
                        assists = player_data['assists']
                        position = player_data['teamPosition']
                        is_win = 1 if player_data["win"] else 0
                        game_creation = datetime.fromtimestamp(match_data['info']['gameCreation'] / 1000)

                        new_data.append([
                            game_creation.strftime('%Y-%m-%d %H:%M:%S'),
                            game_id,
                            is_win,
                            champion_name,
                            position,
                            kills,
                            deaths,
                            assists
                        ])

            if new_data:
                worksheet.append_rows(new_data)
            return new_data
    return None

def aggregate_soloq_data(spreadsheet, team_name):
    data = defaultdict(lambda: defaultdict(lambda: {
        "count": 0, "wins": 0, "kills": 0, "deaths": 0, "assists": 0
    }))
    players = team_rosters.get(team_name, {})

    for player, player_data in players.items():
        wks = check_if_worksheets_exists(spreadsheet, player)
        full_data = wks.get_all_values()
        if not full_data:
            wks.append_row(["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"])
            continue
        for game_data in full_data[1:]:
            if len(game_data) >= 8:
                _, _, win, champion, role, kills, deaths, assists = game_data
                if champion and role == player_data["role"]:
                    if win == "1": data[player][champion]["wins"] += 1
                    data[player][champion]["count"] += 1
                    data[player][champion]["kills"] += int(kills)
                    data[player][champion]["deaths"] += int(deaths)
                    data[player][champion]["assists"] += int(assists)

    for player in data:
        data[player] = dict(sorted(data[player].items(), key=lambda x: (x[1]["count"], x[1]["wins"]), reverse=True))

    return data

# Main Streamlit function with button navigation
def main():
    if 'current_page' not in st.session_state:
        st.session_state.current_page = "Hellenic Legends League Stats"

    st.sidebar.title("Navigation")
    
    if 'match_history_data' not in st.session_state or 'first_bans_data' not in st.session_state or 'draft_data' not in st.session_state:
        with st.spinner("Loading data from Leaguepedia..."):
            st.session_state.match_history_data = fetch_match_history_data()
            st.session_state.first_bans_data = fetch_first_bans_data()
            st.session_state.draft_data = fetch_draft_data()

    all_teams = set()
    for team in st.session_state.match_history_data.keys():
        all_teams.add(normalize_team_name(team))
    for team in st.session_state.first_bans_data.keys():
        all_teams.add(normalize_team_name(team))
    for team in st.session_state.draft_data.keys():
        all_teams.add(normalize_team_name(team))
    
    teams = sorted(list(all_teams))
    if not teams:
        st.warning("No teams found in the data.")
        return

    selected_team = st.sidebar.selectbox("Select a Hellenic Legends League Team", teams, key="hll_team_select")

    # Добавляем кнопки навигации
    if st.session_state.current_page == "Hellenic Legends League Stats":
        if st.sidebar.button("Go to GMS SoloQ"):
            st.session_state.current_page = "GMS SoloQ"
            st.rerun()
        if st.sidebar.button("Go to Scrims"):
            st.session_state.current_page = "Scrims"
            st.rerun()

    st.sidebar.markdown("<hr style='border: 1px solid #333; margin: 20px 0;'>", unsafe_allow_html=True)
    st.sidebar.image("logo.webp", width=100, use_container_width=True)
    st.sidebar.markdown(
        """
        <div style="text-align: center; font-size: 14px; color: #888;">
            by heovech
        </div>
        """,
        unsafe_allow_html=True
    )

    # Выбор страницы
    if st.session_state.current_page == "Hellenic Legends League Stats":
        hll_page(selected_team)
    elif st.session_state.current_page == "GMS SoloQ":
        soloq_page()
    elif st.session_state.current_page == "Scrims":
        scrims_page()  # Вызов функции из scrims.py

def save_notes_data(data, team_name, filename_prefix="notes_data"):
    filename = f"{filename_prefix}_{team_name}.json"
    with open(filename, "w") as f:
        json.dump(data, f)

def load_notes_data(team_name, filename_prefix="notes_data"):
    filename = f"{filename_prefix}_{team_name}.json"
    default_data = {
        "tables": [
            [
                ["", "Ban", ""],
                ["", "Ban", ""],
                ["", "Ban", ""],
                ["", "Pick", ""],
                ["", "Pick", ""],
                ["", "Pick", ""],
                ["", "Ban", ""],
                ["", "Ban", ""],
                ["", "Pick", ""],
                ["", "Pick", ""]
            ] for _ in range(6)
        ],
        "notes_text": ""
    }
    if os.path.exists(filename):
        with open(filename, "r") as f:
            return json.load(f)
    return default_data

def hll_page(selected_team):
    st.title("Hellenic Legends League 2025 Winter - Pick & Ban Statistics")

    normalized_selected_team = normalize_team_name(selected_team)

    st.header(f"Team: {selected_team}")
    if st.button("Update Data"):
        with st.spinner("Updating data..."):
            st.session_state.match_history_data = fetch_match_history_data()
            st.session_state.first_bans_data = fetch_first_bans_data()
            if 'draft_data' not in st.session_state:
                st.session_state.draft_data = {}
            st.session_state.draft_data[normalized_selected_team] = fetch_draft_data()
        st.success("Data updated!")

    if 'show_picks' not in st.session_state:
        st.session_state.show_picks = False
    if 'show_bans' not in st.session_state:
        st.session_state.show_bans = False
    if 'show_duo_picks' not in st.session_state:
        st.session_state.show_duo_picks = False
    if 'show_drafts' not in st.session_state:
        st.session_state.show_drafts = False
    if 'show_notes' not in st.session_state:
        st.session_state.show_notes = False

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        if st.button("Picks", key="picks_btn"):
            st.session_state.show_picks = not st.session_state.show_picks
    with col2:
        if st.button("Bans", key="bans_btn"):
            st.session_state.show_bans = not st.session_state.show_bans
    with col3:
        if st.button("Duo Picks", key="duo_picks_btn"):
            st.session_state.show_duo_picks = not st.session_state.show_duo_picks
    with col4:
        if st.button("Drafts", key="drafts_btn"):
            st.session_state.show_drafts = not st.session_state.show_drafts
    with col5:
        if st.button("Notes", key="notes_btn"):
            st.session_state.show_notes = not st.session_state.show_notes

    team_info = st.session_state.match_history_data.get(normalized_selected_team, {})
    first_bans_info = st.session_state.first_bans_data.get(normalized_selected_team, {'BlueFirstBans': defaultdict(int), 'RedFirstBans': defaultdict(int)})
    roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']

    if st.session_state.show_picks:
        st.subheader("Picks")
        st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)
        columns = st.columns(len(roles))
        for i, role in enumerate(roles):
            with columns[i]:
                st.subheader(f"{role}")
                role_data = team_info.get(role, {})
                if role_data:
                    stats = []
                    for champ, data in role_data.items():
                        if champ != "N/A":
                            winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                            stats.append({
                                'Icon': get_champion_icon(champ),
                                'Champion': champ,
                                'Matches': data['games'],
                                'Win Rate (%)': winrate
                            })
                    if stats:
                        df = pd.DataFrame(stats)
                        df = df.sort_values('Matches', ascending=False)
                        df['Win Rate (%)'] = df['Win Rate (%)'].apply(color_win_rate)
                        html = df.to_html(escape=False, index=False, classes='styled-table')
                        st.markdown(html, unsafe_allow_html=True)
                else:
                    st.write("No data for this role.")

    if st.session_state.show_bans:
        st.subheader("Bans")
        st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)
        col1, col2, divider_col, col3, col4 = st.columns([1, 1, 0.1, 1, 1])

        with col1:
            st.subheader("First 3 Bans (Blue Side)")
            blue_bans_data = first_bans_info['BlueFirstBans']
            if blue_bans_data:
                blue_bans_stats = []
                for champ, count in blue_bans_data.items():
                    blue_bans_stats.append({
                        'Icon': get_champion_icon(champ),
                        'Champion': champ,
                        'Count': count
                    })
                df_blue_bans = pd.DataFrame(blue_bans_stats)
                df_blue_bans = df_blue_bans.sort_values('Count', ascending=False)
                html_blue_bans = df_blue_bans.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(html_blue_bans, unsafe_allow_html=True)
            else:
                st.write("No data on first three bans on the blue side.")

        with col2:
            st.subheader("First 3 Bans (Red Side)")
            red_bans_data = first_bans_info['RedFirstBans']
            if red_bans_data:
                red_bans_stats = []
                for champ, count in red_bans_data.items():
                    red_bans_stats.append({
                        'Icon': get_champion_icon(champ),
                        'Champion': champ,
                        'Count': count
                    })
                df_red_bans = pd.DataFrame(red_bans_stats)
                df_red_bans = df_red_bans.sort_values('Count', ascending=False)
                html_red_bans = df_red_bans.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(html_red_bans, unsafe_allow_html=True)
            else:
                st.write("No data on first three bans on the red side.")

        with divider_col:
            st.markdown(
                """
                <div style='height: 100%; border-left: 2px solid #333; margin: 0 10px;'></div>
                """,
                unsafe_allow_html=True
            )

        with col3:
            st.subheader("Opponent's First 3 Bans (Blue Side)")
            opponent_blue_bans_data = team_info.get('OpponentBlueBans', {})
            if opponent_blue_bans_data:
                opponent_blue_bans_stats = []
                for champ, count in opponent_blue_bans_data.items():
                    opponent_blue_bans_stats.append({
                        'Icon': get_champion_icon(champ),
                        'Champion': champ,
                        'Count': count
                    })
                df_opponent_blue_bans = pd.DataFrame(opponent_blue_bans_stats)
                df_opponent_blue_bans = df_opponent_blue_bans.sort_values('Count', ascending=False)
                html_opponent_blue_bans = df_opponent_blue_bans.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(html_opponent_blue_bans, unsafe_allow_html=True)
            else:
                st.write("No data on opponent's first three bans on the blue side.")

        with col4:
            st.subheader("Opponent's First 3 Bans (Red Side)")
            opponent_red_bans_data = team_info.get('OpponentRedBans', {})
            if opponent_red_bans_data:
                opponent_red_bans_stats = []
                for champ, count in opponent_red_bans_data.items():
                    opponent_red_bans_stats.append({
                        'Icon': get_champion_icon(champ),
                        'Champion': champ,
                        'Count': count
                    })
                df_opponent_red_bans = pd.DataFrame(opponent_red_bans_stats)
                df_opponent_red_bans = df_opponent_red_bans.sort_values('Count', ascending=False)
                html_opponent_red_bans = df_opponent_red_bans.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(html_opponent_red_bans, unsafe_allow_html=True)
            else:
                st.write("No data on opponent's first three bans on the red side.")

    if st.session_state.show_duo_picks:
        st.subheader("Duo Picks")
        st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)
        duo_picks_data = team_info.get('DuoPicks', {})
        duo_pairs = [('Top', 'Jungle'), ('Jungle', 'Mid'), ('Jungle', 'Support'), ('ADC', 'Support')]

        col1, col2 = st.columns(2)
        with col1:
            duo_stats = []
            for (champ1, champ2, r1, r2), data in duo_picks_data.items():
                if data['games'] > 0 and ((r1 == 'Top' and r2 == 'Jungle') or (r1 == 'Jungle' and r2 == 'Top')):
                    winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                    if r1 == 'Top' and r2 == 'Jungle':
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ1), champ1, get_champion_icon(champ2), champ2
                    else:
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ2), champ2, get_champion_icon(champ1), champ1
                    duo_stats.append({
                        'Icon1': icon1,
                        'Champion1': champ1_name,
                        'Icon2': icon2,
                        'Champion2': champ2_name,
                        'Matches': data['games'],
                        'Win Rate (%)': winrate
                    })
            if duo_stats:
                df_duo = pd.DataFrame(duo_stats)
                df_duo = df_duo.sort_values('Matches', ascending=False)
                df_duo['Win Rate (%)'] = df_duo['Win Rate (%)'].apply(color_win_rate)
                html_duo = df_duo.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Top-Jungle Duo Picks</h4>
                        {html_duo}
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Top-Jungle Duo Picks</h4>
                        <p>No data on duo picks for Top-Jungle.</p>
                    </div>
                """, unsafe_allow_html=True)

        with col2:
            duo_stats = []
            for (champ1, champ2, r1, r2), data in duo_picks_data.items():
                if data['games'] > 0 and ((r1 == 'Jungle' and r2 == 'Mid') or (r1 == 'Mid' and r2 == 'Jungle')):
                    winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                    if r1 == 'Jungle' and r2 == 'Mid':
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ1), champ1, get_champion_icon(champ2), champ2
                    else:
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ2), champ2, get_champion_icon(champ1), champ1
                    duo_stats.append({
                        'Icon1': icon1,
                        'Champion1': champ1_name,
                        'Icon2': icon2,
                        'Champion2': champ2_name,
                        'Matches': data['games'],
                        'Win Rate (%)': winrate
                    })
            if duo_stats:
                df_duo = pd.DataFrame(duo_stats)
                df_duo = df_duo.sort_values('Matches', ascending=False)
                df_duo['Win Rate (%)'] = df_duo['Win Rate (%)'].apply(color_win_rate)
                html_duo = df_duo.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Jungle-Mid Duo Picks</h4>
                        {html_duo}
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Jungle-Mid Duo Picks</h4>
                        <p>No data on duo picks for Jungle-Mid.</p>
                    </div>
                """, unsafe_allow_html=True)

        col3, col4 = st.columns(2)
        with col3:
            duo_stats = []
            for (champ1, champ2, r1, r2), data in duo_picks_data.items():
                if data['games'] > 0 and ((r1 == 'Jungle' and r2 == 'Support') or (r1 == 'Support' and r2 == 'Jungle')):
                    winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                    if r1 == 'Jungle' and r2 == 'Support':
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ1), champ1, get_champion_icon(champ2), champ2
                    else:
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ2), champ2, get_champion_icon(champ1), champ1
                    duo_stats.append({
                        'Icon1': icon1,
                        'Champion1': champ1_name,
                        'Icon2': icon2,
                        'Champion2': champ2_name,
                        'Matches': data['games'],
                        'Win Rate (%)': winrate
                    })
            if duo_stats:
                df_duo = pd.DataFrame(duo_stats)
                df_duo = df_duo.sort_values('Matches', ascending=False)
                df_duo['Win Rate (%)'] = df_duo['Win Rate (%)'].apply(color_win_rate)
                html_duo = df_duo.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Jungle-Support Duo Picks</h4>
                        {html_duo}
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>Jungle-Support Duo Picks</h4>
                        <p>No data on duo picks for Jungle-Support.</p>
                    </div>
                """, unsafe_allow_html=True)

        with col4:
            duo_stats = []
            for (champ1, champ2, r1, r2), data in duo_picks_data.items():
                if data['games'] > 0 and ((r1 == 'ADC' and r2 == 'Support') or (r1 == 'Support' and r2 == 'ADC')):
                    winrate = (data['wins'] / data['games'] * 100) if data['games'] > 0 else 0
                    if r1 == 'ADC' and r2 == 'Support':
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ1), champ1, get_champion_icon(champ2), champ2
                    else:
                        icon1, champ1_name, icon2, champ2_name = get_champion_icon(champ2), champ2, get_champion_icon(champ1), champ1
                    duo_stats.append({
                        'Icon1': icon1,
                        'Champion1': champ1_name,
                        'Icon2': icon2,
                        'Champion2': champ2_name,
                        'Matches': data['games'],
                        'Win Rate (%)': winrate
                    })
            if duo_stats:
                df_duo = pd.DataFrame(duo_stats)
                df_duo = df_duo.sort_values('Matches', ascending=False)
                df_duo['Win Rate (%)'] = df_duo['Win Rate (%)'].apply(color_win_rate)
                html_duo = df_duo.to_html(escape=False, index=False, classes='styled-table')
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>ADC-Support Duo Picks</h4>
                        {html_duo}
                    </div>
                """, unsafe_allow_html=True)
            else:
                st.markdown(f"""
                    <div style="display: flex; justify-content: center;">
                        <h4>ADC-Support Duo Picks</h4>
                        <p>No data on duo picks for ADC-Support.</p>
                    </div>
                """, unsafe_allow_html=True)

    if st.session_state.show_drafts:
        st.subheader("Drafts")
        st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)
        draft_data = st.session_state.draft_data.get(normalized_selected_team, [])
        if draft_data:
            drafts_by_match = {}
            for draft in draft_data:
                if draft['blue_team'] == normalized_selected_team or draft['red_team'] == normalized_selected_team:
                    match_key = draft['match_key']
                    if match_key not in drafts_by_match:
                        drafts_by_match[match_key] = []
                    drafts_by_match[match_key].append(draft)

            sorted_matches = sorted(drafts_by_match.items(), key=lambda x: min(d['match_number'] for d in x[1]))

            for match_key, match_drafts in sorted_matches:
                blue_team = match_drafts[0]['blue_team']
                red_team = match_drafts[0]['red_team']
                st.subheader(f"{blue_team} vs {red_team}")

                for draft in match_drafts:
                    game_key = f"show_game_{match_key}_{draft['match_number']}"
                    if game_key not in st.session_state:
                        st.session_state[game_key] = False

                num_games = len(match_drafts)
                game_cols = st.columns(num_games)
                for i, draft in enumerate(match_drafts):
                    with game_cols[i]:
                        game_key = f"show_game_{match_key}_{draft['match_number']}"
                        if st.button(f"Game {draft['match_number']}", key=f"game_btn_{match_key}_{draft['match_number']}"):
                            st.session_state[game_key] = not st.session_state[game_key]

                active_games = [draft for draft in match_drafts if st.session_state[f"show_game_{match_key}_{draft['match_number']}"]]
                if active_games:
                    active_cols = st.columns(len(active_games))
                    for i, draft in enumerate(active_games):
                        with active_cols[i]:
                            result = "Win" if (draft['winner_side'] == 'blue' and draft['blue_team'] == normalized_selected_team) or (draft['winner_side'] == 'red' and draft['red_team'] == normalized_selected_team) else "Loss"
                            st.write(f"Game {draft['match_number']}")
                            st.write(f"Result: {result}")

                            is_selected_team_blue = (draft['blue_team'] == normalized_selected_team)
                            team_side = "Blue" if is_selected_team_blue else "Red"
                            left_team = normalized_selected_team if is_selected_team_blue else draft['blue_team']
                            right_team = draft['red_team'] if is_selected_team_blue else normalized_selected_team

                            if is_selected_team_blue:
                                left_bans = draft['blue_bans']
                                right_bans = draft['red_bans']
                                left_picks = [champ for champ, _ in draft['blue_picks']]
                                right_picks = [champ for champ, _ in draft['red_picks']]
                            else:
                                left_bans = draft['red_bans']
                                right_bans = draft['blue_bans']
                                left_picks = [champ for champ, _ in draft['red_picks']]
                                right_picks = [champ for champ, _ in draft['blue_picks']]

                            vod_link = draft['vod_link']
                            vod = f'<a href="{vod_link}" target="_blank">VOD</a>' if vod_link != "N/A" else ""

                            table_data = [
                                (f"{get_champion_icon(left_bans[0])} {left_bans[0]}" if left_bans[0] != "N/A" else "", "Ban", f"{get_champion_icon(right_bans[0])} {right_bans[0]}" if right_bans[0] != "N/A" else "", vod),
                                (f"{get_champion_icon(left_bans[1])} {left_bans[1]}" if left_bans[1] != "N/A" else "", "Ban", f"{get_champion_icon(right_bans[1])} {right_bans[1]}" if right_bans[1] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_bans[2])} {left_bans[2]}" if left_bans[2] != "N/A" else "", "Ban", f"{get_champion_icon(right_bans[2])} {right_bans[2]}" if right_bans[2] != "N/A" else "", result),
                                (f"{get_champion_icon(left_picks[0])} {left_picks[0]}" if left_picks[0] != "N/A" else "", "Pick", f"{get_champion_icon(right_picks[0])} {right_picks[0]}" if right_picks[0] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_picks[1])} {left_picks[1]}" if left_picks[1] != "N/A" else "", "Pick", f"{get_champion_icon(right_picks[1])} {right_picks[1]}" if right_picks[1] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_picks[2])} {left_picks[2]}" if left_picks[2] != "N/A" else "", "Pick", f"{get_champion_icon(right_picks[2])} {right_picks[2]}" if right_picks[2] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_bans[3])} {left_bans[3]}" if left_bans[3] != "N/A" else "", "Ban", f"{get_champion_icon(right_bans[3])} {right_bans[3]}" if right_bans[3] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_bans[4])} {left_bans[4]}" if left_bans[4] != "N/A" else "", "Ban", f"{get_champion_icon(right_bans[4])} {right_bans[4]}" if right_bans[4] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_picks[3])} {left_picks[3]}" if left_picks[3] != "N/A" else "", "Pick", f"{get_champion_icon(right_picks[3])} {right_picks[3]}" if right_picks[3] != "N/A" else "", ""),
                                (f"{get_champion_icon(left_picks[4])} {left_picks[4]}" if left_picks[4] != "N/A" else "", "Pick", f"{get_champion_icon(right_picks[4])} {right_picks[4]}" if right_picks[4] != "N/A" else "", ""),
                            ]

                            df_draft = pd.DataFrame(table_data, columns=[left_team, "Action", right_team, "VOD"])

                            def highlight_cells(row):
                                styles = [''] * len(row)
                                if row['Action'] == "Ban":
                                    styles[0] = 'background-color: red'
                                    styles[2] = 'background-color: red'
                                if row['VOD'] == "Win":
                                    styles[3] = 'background-color: green'
                                elif row['VOD'] == "Loss":
                                    styles[3] = 'background-color: red'
                                return styles

                            styled_df = df_draft.style.apply(highlight_cells, axis=1)
                            html_draft = styled_df.to_html(escape=False, index=False, classes='styled-table drafts-table')
                            st.markdown(html_draft, unsafe_allow_html=True)

    if st.session_state.show_notes:
        st.subheader("Notes")
        st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)

        if f'notes_data_{normalized_selected_team}' not in st.session_state:
            st.session_state[f'notes_data_{normalized_selected_team}'] = load_notes_data(normalized_selected_team)

        col_left, col_right = st.columns([3, 1])

        with col_left:
            st.subheader("Draft Templates")
            table_cols = st.columns(3)
            for i in range(6):
                with table_cols[i % 3]:
                    st.write(f"Draft Template {i + 1}")
                    columns = ["Team 1", "Action", "Team 2"]
                    df = pd.DataFrame(st.session_state[f'notes_data_{normalized_selected_team}']["tables"][i], columns=columns)
                    edited_df = st.data_editor(
                        df,
                        num_rows="fixed",
                        use_container_width=True,
                        key=f"notes_table_{normalized_selected_team}_{i}",
                        column_config={
                            "Team 1": st.column_config.TextColumn("Team 1"),
                            "Action": st.column_config.TextColumn("Action", disabled=True),
                            "Team 2": st.column_config.TextColumn("Team 2"),
                        }
                    )
                    st.session_state[f'notes_data_{normalized_selected_team}']["tables"][i] = edited_df.values.tolist()

        with col_right:
            st.subheader("Additional Notes")
            notes_text = st.text_area(
                "Write your notes here:",
                value=st.session_state[f'notes_data_{normalized_selected_team}']["notes_text"],
                height=400,
                key=f"notes_text_area_{normalized_selected_team}"
            )
            st.session_state[f'notes_data_{normalized_selected_team}']["notes_text"] = notes_text

        save_notes_data(st.session_state[f'notes_data_{normalized_selected_team}'], normalized_selected_team)

def soloq_page():
    st.title("Gamespace 2025 SoloQ Statistics")

    if st.button("Back to Hellenic Legends League Stats"):
        st.session_state.current_page = "Hellenic Legends League Stats"
        st.rerun()

    client = setup_google_sheets()
    if not client:
        return

    try:
        spreadsheet = client.open("Soloq_GMS")
    except gspread.exceptions.APIError as e:
        st.error(f"Ошибка подключения к Google Sheets: {str(e)}")
        return

    if 'soloq_data' not in st.session_state:
        st.session_state.soloq_data = aggregate_soloq_data(spreadsheet, "Gamespace")

    if st.button("Update Soloq"):
        with st.spinner("Updating SoloQ data..."):
            for player, player_data in team_rosters["Gamespace"].items():
                wks = check_if_worksheets_exists(spreadsheet, player)
                for game_name, tag_line in zip(player_data["game_name"], player_data["tag_line"]):
                    get_account_data(wks, game_name, tag_line)
            st.session_state.soloq_data = aggregate_soloq_data(spreadsheet, "Gamespace")
        st.success("SoloQ data updated!")

    st.subheader("SoloQ Player Statistics")
    st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)
    soloq_data = st.session_state.soloq_data
    players = team_rosters["Gamespace"].keys()
    cols = st.columns(5)
    for i, player in enumerate(players):
        with cols[i]:
            st.subheader(f"{player} Stats")
            wks = check_if_worksheets_exists(spreadsheet, player)
            data = wks.get_all_values()
            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"])
                df["Дата матча"] = pd.to_datetime(df["Дата матча"], errors='coerce')
                time_filter = st.selectbox(f"Filter {player}", ["All", "1 week", "2 weeks", "4 weeks"], key=f"time_filter_{player}")
                if time_filter != "All":
                    days = {"1 week": 7, "2 weeks": 14, "4 weeks": 28}[time_filter]
                    cutoff = datetime.now() - timedelta(days=days)
                    df = df[df["Дата матча"] >= cutoff]
                player_data = defaultdict(lambda: {"count": 0, "wins": 0, "kills": 0, "deaths": 0, "assists": 0})
                for _, row in df.iterrows():
                    if row["Роль"] == team_rosters["Gamespace"][player]["role"]:
                        champion = row["Чемпион"]
                        if row["Победа"] == "1": player_data[champion]["wins"] += 1
                        player_data[champion]["count"] += 1
                        player_data[champion]["kills"] += int(row["Киллы"])
                        player_data[champion]["deaths"] += int(row["Смерти"])
                        player_data[champion]["assists"] += int(row["Ассисты"])
                stats = []
                for champ, stats_dict in player_data.items():
                    if stats_dict["count"] > 0:
                        win_rate = round(stats_dict["wins"] / stats_dict["count"] * 100, 2)
                        kda = round((stats_dict["kills"] + stats_dict["assists"]) / max(stats_dict["deaths"], 1), 2)
                        stats.append({"Champion": champ, "Games": stats_dict["count"], "Win Rate (%)": win_rate, "KDA": kda})
                if stats:
                    df_stats = pd.DataFrame(stats).sort_values("Games", ascending=False)
                    df_stats["Win Rate (%)"] = df_stats["Win Rate (%)"].apply(color_win_rate)
                    html = df_stats.to_html(escape=False, index=False, classes='styled-table')
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.write(f"No SoloQ data for {player}.")
            else:
                st.write(f"No SoloQ data for {player}.")

    st.subheader("SoloQ Games Over Time")
    st.markdown("<hr style='border: 2px solid #333; margin: 10px 0;'>", unsafe_allow_html=True)

    selected_player = st.selectbox("Select Player for Visualization", players, key="viz_player")
    aggregation_type = st.selectbox("Aggregate by", ["Day", "Week", "Month"], key="agg_type")

    wks = check_if_worksheets_exists(spreadsheet, selected_player)
    try:
        data = wks.get_all_values()
        if len(data) <= 1:
            st.write("No data available for visualization.")
            return

        df = pd.DataFrame(data[1:], columns=["Дата матча", "Матч_айди", "Победа", "Чемпион", "Роль", "Киллы", "Смерти", "Ассисты"])
        df["Дата матча"] = pd.to_datetime(df["Дата матча"], errors='coerce')
        df = df.dropna(subset=["Дата матча"])

        if aggregation_type == "Day":
            df_agg = df.groupby(df["Дата матча"].dt.date).size().reset_index(name="Games")
            df_agg.columns = ["Дата", "Количество игр"]
            title = f"Games Played per Day by {selected_player}"
            st.bar_chart(df_agg.set_index("Дата")["Количество игр"])
        
        elif aggregation_type == "Week":
            df_agg = df.groupby(df["Дата матча"].dt.to_period("W")).size().reset_index(name="Games")
            df_agg["Дата матча"] = df_agg["Дата матча"].apply(lambda x: x.start_time)
            df_agg.columns = ["Дата", "Количество игр"]
            title = f"Games Played per Week by {selected_player}"
            st.bar_chart(df_agg.set_index("Дата")["Количество игр"])
        
        elif aggregation_type == "Month":
            df_agg = df.groupby(df["Дата матча"].dt.to_period("M")).size().reset_index(name="Games")
            df_agg["Дата матча"] = df_agg["Дата матча"].apply(lambda x: x.start_time)
            df_agg.columns = ["Дата", "Количество игр"]
            title = f"Games Played per Month by {selected_player}"
            st.bar_chart(df_agg.set_index("Дата")["Количество игр"])

        if not df_agg.empty:
            st.write(f"**{title}**")
        else:
            st.write(f"No data available for visualization for {selected_player}.")

    except gspread.exceptions.APIError as e:
        st.error(f"Ошибка API Google Sheets при загрузке данных: {str(e)}")

# Аутентификация
with open('config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

if 'authentication_status' not in st.session_state:
    st.session_state.authentication_status = None
    st.session_state.name = None
    st.session_state.username = None

if st.session_state.authentication_status is None:
    login_result = authenticator.login(key='Login')
    if login_result is not None:
        st.session_state.name, st.session_state.authentication_status, st.session_state.username = login_result

name = st.session_state.name
authentication_status = st.session_state.authentication_status
username = st.session_state.username

if authentication_status:
    with st.sidebar:
        authenticator.logout('Logout', 'sidebar')
        st.write(f'Welcome *Coach*')
    
    if __name__ == "__main__":
        main()

elif authentication_status == False:
    st.error('Username/password is incorrect')
elif authentication_status is None:
    st.warning('Please enter your username and password')

