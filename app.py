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
RIOT_API_KEY = os.getenv("RIOT_API_KEY", "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8") # Replace/Use Secrets
if RIOT_API_KEY == "RGAPI-2364bf09-8116-4d02-9dde-e2ed7cde4af8": st.warning("Using default RIOT API Key.")
SUMMONER_NAME_BY_URL = f"https://europe.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{{}}/{{}}?api_key={RIOT_API_KEY}"
MATCH_HISTORY_URL = f"https://europe.api.riotgames.com/lol/match/v5/matches/by-puuid/{{}}/ids?start=0&count=100&api_key={RIOT_API_KEY}"
MATCH_BASIC_URL = f"https://europe.api.riotgames.com/lol/match/v5/matches/{{}}?api_key={RIOT_API_KEY}"
TOURNAMENT_URLS = {
    "Winter Split": {"match_history": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Split/Match_History", "picks_and_bans": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Split/Picks_and_Bans"},
    "Winter Playoffs": {"match_history": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Playoffs/Match_History", "picks_and_bans": "https://lol.fandom.com/wiki/Hellenic_Legends_League/2025_Season/Winter_Playoffs/Picks_and_Bans"}
}
team_rosters = {
    "Gamespace": { "Aytekn": {"game_name": ["AyteknnnN777"], "tag_line": ["777"], "role": "TOP"}, "Pallet": {"game_name": ["KC Bo", "yiqunsb"], "tag_line": ["2106", "KR21"], "role": "JUNGLE"}, "Tsiperakos": {"game_name": ["Tsiperakos", "Tsiper"], "tag_line": ["MID", "tsprk"], "role": "MIDDLE"}, "Kenal": {"game_name": ["Kenal", "Kaneki Kenal"], "tag_line": ["EUW", "EUW0"], "role": "BOTTOM"}, "Centu": {"game_name": ["ΣΑΝ ΚΡΟΥΑΣΑΝ", "Aim First"], "tag_line": ["Ker10", "001"], "role": "UTILITY"} }
}
SOLOQ_SHEET_NAME = "Soloq_GMS"

# --- Helper Functions ---
@st.cache_data(ttl=3600)
def get_latest_patch_version():
    try: response = requests.get("https://ddragon.leagueoflegends.com/api/versions.json", timeout=10); response.raise_for_status(); versions = response.json(); return versions[0] if versions else "14.14.1"
    except Exception: return "14.14.1"
PATCH_VERSION = get_latest_patch_version()
@st.cache_data
def normalize_team_name(team_name):
    if not team_name or not isinstance(team_name, str): return "unknown"
    tnl = team_name.strip().lower();
    if tnl in ["unknown blue", "unknown red", ""]: return "unknown"
    aliases = {"gamespace": "Gamespace", "gms": "Gamespace"}; clean = tnl.replace("logo std", "").strip()
    if clean in aliases: return aliases[clean]
    for alias, norm in aliases.items():
        if alias in clean: return norm
    return clean.title()
def get_champion(span): return span['title'].strip() if span and isinstance(span, Tag) and 'title' in span.attrs else "N/A"
@st.cache_data
def normalize_champion_name_for_ddragon(champ):
    if not champ or champ == "N/A": return None
    ex = {"Nunu & Willump": "Nunu", "Wukong": "MonkeyKing", "Renata Glasc": "Renata", "K'Sante": "KSante"};
    if champ in ex: return ex[champ]
    return "".join(c for c in champ if c.isalnum())
def get_champion_icon_url(champ): norm = normalize_champion_name_for_ddragon(champ); return f"https://ddragon.leagueoflegends.com/cdn/{PATCH_VERSION}/img/champion/{norm}.png" if norm else None
def get_champion_icon_html(champ, w=35, h=35): url = get_champion_icon_url(champ); return f'<img src="{url}" width="{w}" height="{h}" alt="{champ}" title="{champ}" style="vertical-align: middle;">' if url else ""
def color_win_rate(val):
    try: v = float(val);
    if 0<=v<48: return f'<span style="color:#FF7F7F; font-weight:bold;">{v:.1f}%</span>'
    elif 48<=v<=52: return f'<span style="color:#FFD700; font-weight:bold;">{v:.1f}%</span>'
    elif v>52: return f'<span style="color:#90EE90; font-weight:bold;">{v:.1f}%</span>'
    else: return f'{val}'
    except (ValueError, TypeError): return f'{val}'

# --- Data Fetching Functions (HLL - Полная реализация, без кэширования) ---
# !! NO @st.cache_data !!
def fetch_match_history_data():
    headers = {'User-Agent': 'HLLAnalyticsApp/1.0'}
    team_data = defaultdict(lambda: {'matches_played': 0, 'wins': 0, 'losses': 0, 'blue_side_games': 0, 'blue_side_wins': 0, 'red_side_games': 0, 'red_side_wins': 0, 'Top': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Jungle': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Mid': defaultdict(lambda: {'games': 0, 'wins': 0}), 'ADC': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Support': defaultdict(lambda: {'games': 0, 'wins': 0}), 'Bans': defaultdict(int), 'OpponentBansAgainst': defaultdict(int), 'DuoPicks': defaultdict(lambda: {'games': 0, 'wins': 0}), 'MatchResults': [] })
    roles = ['Top', 'Jungle', 'Mid', 'ADC', 'Support']
    for tour, urls in TOURNAMENT_URLS.items():
        url = urls.get("match_history");
        if not url: continue
        try: resp = requests.get(url, headers=headers, timeout=20); resp.raise_for_status()
        except requests.exceptions.RequestException as e: st.error(f"Failed MH {tour}: {e}"); continue
        soup = BeautifulSoup(resp.content, 'html.parser'); tables = soup.select('.wikitable.mhgame.sortable')
        if not tables: continue
        for table in tables:
            rows = table.select('tr');
            if len(rows) < 2: continue
            for row in rows[1:]:
                cols = row.select('td');
                if not cols or len(cols) < 9: continue
                bl, rl = cols[2].select_one('a[title]'), cols[3].select_one('a[title]')
                br, rr = (bl['title'].strip() if bl else cols[2].get_text(strip=True)), (rl['title'].strip() if rl else cols[3].get_text(strip=True))
                bt, rt = normalize_team_name(br), normalize_team_name(rr)
                if bt == "unknown" or rt == "unknown": continue
                restxt, winl = cols[4].get_text(strip=True), cols[4].select_one('a[title]')
                winner = "unknown";
                if winl: winner = normalize_team_name(winl['title'].strip())
                elif restxt == "1:0": winner = bt; elif restxt == "0:1": winner = rt
                resb, resr = ('Win' if winner==bt else 'Loss' if winner!="unknown" else 'N/A'), ('Win' if winner==rt else 'Loss' if winner!="unknown" else 'N/A')
                if resb == 'N/A': continue
                team_data[bt]['matches_played'] += 1; team_data[rt]['matches_played'] += 1; team_data[bt]['blue_side_games'] += 1; team_data[rt]['red_side_games'] += 1
                if resb == 'Win': team_data[bt]['wins'] += 1; team_data[bt]['blue_side_wins'] += 1; team_data[rt]['losses'] += 1
                else: team_data[bt]['losses'] += 1; team_data[rt]['wins'] += 1; team_data[rt]['red_side_wins'] += 1
                bbe, rbe = (cols[5].select('span.sprite.champion-sprite') if len(cols)>5 else []), (cols[6].select('span.sprite.champion-sprite') if len(cols)>6 else [])
                bbans, rbans = [get_champion(b) for b in bbe], [get_champion(b) for b in rbe]
                for c in bbans:
                    if c!="N/A": team_data[bt]['Bans'][c]+=1; team_data[rt]['OpponentBansAgainst'][c]+=1
                for c in rbans:
                    if c!="N/A": team_data[rt]['Bans'][c]+=1; team_data[bt]['OpponentBansAgainst'][c]+=1
                bpe, rpe = (cols[7].select('span.sprite.champion-sprite') if len(cols)>7 else []), (cols[8].select('span.sprite.champion-sprite') if len(cols)>8 else [])
                bpicks, rpicks = {r: get_champion(p) for r,p in zip(roles, bpe)}, {r: get_champion(p) for r,p in zip(roles, rpe)}
                for r, c in bpicks.items():
                    if c!="N/A": team_data[bt][r][c]['games']+=1;
                    if resb=='Win': team_data[bt][r][c]['wins']+=1
                for r, c in rpicks.items():
                    if c!="N/A": team_data[rt][r][c]['games']+=1;
                    if resr=='Win': team_data[rt][r][c]['wins']+=1
                duos = [('Top','Jungle'), ('Jungle','Mid'), ('Mid','ADC'), ('ADC','Support'), ('Jungle','Support')]
                for team, picks, res in [(bt, bpicks, resb), (rt, rpicks, resr)]:
                    for r1, r2 in duos:
                        c1,c2 = picks.get(r1,"N/A"), picks.get(r2,"N/A");
                        if c1!="N/A" and c2!="N/A": key=tuple(sorted([(c1,r1),(c2,r2)])); team_data[team]['DuoPicks'][key]['games']+=1;
                        if res=='Win': team_data[team]['DuoPicks'][key]['wins']+=1
                mid = f"{tour}_{bt}_vs_{rt}_{team_data[bt]['matches_played']}"
                mrb = {'match_id':mid,'opponent':rt,'side':'blue','result':resb,'tournament':tour,'blue_picks':bpicks,'red_picks':rpicks,'blue_bans':bbans,'red_bans':rbans}
                mrr = {'match_id':mid,'opponent':bt,'side':'red','result':resr,'tournament':tour,'blue_picks':bpicks,'red_picks':rpicks,'blue_bans':bbans,'red_bans':rbans}
                team_data[bt]['MatchResults'].append(mrb); team_data[rt]['MatchResults'].append(mrr)
    return dict(team_data)

# !! NO @st.cache_data !!
def fetch_draft_data():
    headers = {'User-Agent': 'HLLAnalyticsApp/1.0'}
    team_drafts = defaultdict(list); match_counter = defaultdict(lambda: defaultdict(int))
    for tour, urls in TOURNAMENT_URLS.items():
        url = urls.get("picks_and_bans");
        if not url: continue
        try: resp = requests.get(url, headers=headers, timeout=20); resp.raise_for_status()
        except requests.exceptions.RequestException as e: st.error(f"Failed PB {tour}: {e}"); continue
        soup = BeautifulSoup(resp.content, 'html.parser'); tables = soup.select('table.wikitable.plainlinks.hoverable-rows.column-show-hide-1')
        if not tables: continue
        for table in tables:
            rows = table.select('tr');
            if len(rows) < 2: continue
            for row in rows[1:]:
                cols = row.select('td');
                if len(cols) < 24: continue
                bc, rc = cols[1], cols[2]; bl, rl = bc.select_one('a[title], span[title]'), rc.select_one('a[title], span[title]')
                br, rr = (bl['title'].strip() if bl else bc.get_text(strip=True)), (rl['title'].strip() if rl else rc.get_text(strip=True))
                bt, rt = normalize_team_name(br), normalize_team_name(rr)
                if bt == "unknown" or rt == "unknown": continue
                winner = None;
                if 'pbh-winner' in bc.get('class', []): winner = 'blue'; elif 'pbh-winner' in rc.get('class', []): winner = 'red'
                if winner is None: continue
                key = tuple(sorted([bt, rt])); match_counter[tour][key] += 1; num = match_counter[tour][key]
                actions = []
                ban1 = range(5, 11);
                for i, idx in enumerate(ban1): side = 'blue' if i%2==0 else 'red'; span = cols[idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]'); champ = span['title'].strip() if span else "N/A"; actions.append({'type':'ban','phase':1,'side':side,'champion':champ})
                pick1 = [(11,0,'blue'), (12,0,'red'), (12,1,'red'), (13,0,'blue'), (13,1,'blue'), (14,0,'red')]
                for idx, sidx, side in pick1: spans = cols[idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]'); champ = spans[sidx]['title'].strip() if len(spans)>sidx else "N/A"; actions.append({'type':'pick','phase':1,'side':side,'champion':champ})
                ban2 = range(15, 19);
                for i, idx in enumerate(ban2): side = 'red' if i%2==0 else 'blue'; span = cols[idx].select_one('.pbh-cn .champion-sprite[title], span.champion-sprite[title]'); champ = span['title'].strip() if span else "N/A"; actions.append({'type':'ban','phase':2,'side':side,'champion':champ})
                pick2 = [(19,0,'red'), (20,0,'blue'), (20,1,'blue'), (21,0,'red')]
                for idx, sidx, side in pick2: spans = cols[idx].select('.pbh-cn .champion-sprite[title], span.champion-sprite[title]'); champ = spans[sidx]['title'].strip() if len(spans)>sidx else "N/A"; actions.append({'type':'pick','phase':2,'side':side,'champion':champ})
                vod = "N/A"; elem = cols[23].select_one('a[href]');
                if elem: vod = elem['href']
                info = {'tournament':tour,'match_key':key,'match_number':num,'blue_team':bt,'red_team':rt,'winner_side':winner,'draft_actions':actions,'vod_link':vod}
                team_drafts[bt].append(info); team_drafts[rt].append(info)
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


# --- Notes Saving/Loading (Keep as is) ---
NOTES_DIR = "notes_data"; os.makedirs(NOTES_DIR, exist_ok=True)
def get_notes_filepath(t_name, pfx="notes"): safe = "".join(c if c.isalnum() else "_" for c in t_name); return os.path.join(NOTES_DIR, f"{pfx}_{safe}.json")
def save_notes_data(data, t_name): path = get_notes_filepath(t_name); try: f=open(path,"w",encoding="utf-8"); json.dump(data,f,indent=4); f.close(); except Exception as e: st.error(f"Err save notes {t_name}: {e}")
def load_notes_data(t_name):
    path=get_notes_filepath(t_name); default={"tables":[[["", "Ban", ""],["", "Ban", ""],["", "Ban", ""],["", "Pick", ""],["", "Pick", ""],["", "Pick", ""],["", "Ban", ""],["", "Ban", ""],["", "Pick", ""],["", "Pick", ""]]*6],"notes_text":""}
    if os.path.exists(path): try: f=open(path,"r",encoding="utf-8"); loaded=json.load(f); f.close(); return loaded if "tables" in loaded and "notes_text" in loaded else default; except Exception: return default
    else: return default


# --- Streamlit Page Functions ---

# !!! ВОССТАНОВЛЕНА ПОЛНАЯ hll_page !!!
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
                            actions = d['draft_actions']
                            # Simplified display using actions directly
                            draft_table_rows = []
                            for action in actions:
                                icon = get_champion_icon_html(action['champion'], 20, 20)
                                phase_tag = f"P{action['phase']}" if 'phase' in action else ""
                                action_tag = "B" if action['type'] == 'ban' else "P" if action['type'] == 'pick' else "?"
                                blue_cell = icon if action['side'] == 'blue' else ""
                                red_cell = icon if action['side'] == 'red' else ""
                                draft_table_rows.append((blue_cell, f"{action_tag}", red_cell)) # Simplified action display
                            df_d = pd.DataFrame(draft_table_rows, columns=[d['blue_team'], "Action", d['red_team']])
                            st.markdown(df_d.to_html(escape=False, index=False, classes='compact-table draft-view', justify='center'), unsafe_allow_html=True)
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

# !!! ВОССТАНОВЛЕНА ПОЛНАЯ soloq_page !!!
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
        if new_matches_count > 0: st.success(f"Added {new_matches_count} matches."); aggregate_soloq_data_from_sheet.clear() # Clear cache on update
        else: st.info("No new matches.")
    st.subheader("Player Stats (Sheets Data)")
    try:
        agg_data = aggregate_soloq_data_from_sheet(sheet, "Gamespace"); # Use cached data if available and not cleared
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

# --- Main Application Logic (Keep as previously corrected) ---
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
            try:
                 # Fetch data without caching here if functions changed
                 st.session_state.match_history_data = fetch_match_history_data();
                 st.session_state.draft_data = fetch_draft_data();
                 st.sidebar.success("HLL loaded."); time.sleep(1); st.rerun()
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
    st.sidebar.markdown("""<div style='text-align: center; font-size: 12px; color: #888;'>App by heovech<br><a href='#' style='color: #888;'>Contact</a></div>""", unsafe_allow_html=True)
    if current_page == "Hellenic Legends League Stats":
        if selected_hll_team: hll_page(selected_hll_team)
        else: st.info("Select HLL team or wait.")
    elif current_page == "GMS SoloQ": soloq_page()
    elif current_page == "Scrims": scrims.scrims_page()

# --- Authentication (Keep as previously corrected) ---
try:
    with open('config.yaml') as file: config = yaml.load(file, Loader=SafeLoader)
except Exception as e: st.error(f"FATAL: config.yaml error: {e}"); st.stop()
if not isinstance(config, dict) or 'credentials' not in config or 'cookie' not in config or not all(k in config['cookie'] for k in ['name', 'key', 'expiry_days']):
    st.error("FATAL: config.yaml invalid."); st.stop()
authenticator = stauth.Authenticate(config['credentials'], config['cookie']['name'], config['cookie']['key'], config['cookie']['expiry_days'])
if 'authentication_status' not in st.session_state: st.session_state.authentication_status = None
if 'name' not in st.session_state: st.session_state.name = None
if 'username' not in st.session_state: st.session_state.username = None
login_placeholder = st.empty() # Define placeholder BEFORE using it
if st.session_state.authentication_status is None:
    with login_placeholder.container():
        try: name, authentication_status, username = authenticator.login(location='main') # Corrected call
        st.session_state.name, st.session_state.authentication_status, st.session_state.username = name, authentication_status, username
        except KeyError as e: st.error(f"Auth Error key {e}"); st.stop()
        except Exception as e: st.error(f"Login Error: {e}"); st.stop()
if st.session_state.authentication_status:
    login_placeholder.empty(); # Clear login form
    with st.sidebar: st.sidebar.divider(); st.sidebar.write(f'Welcome *{st.session_state.name}*'); authenticator.logout('Logout', 'sidebar', key='logout_button')
    try:
        with open("style.css") as f: st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)
    except FileNotFoundError: pass
    if __name__ == "__main__": main()
elif st.session_state.authentication_status is False: st.error('Username/password incorrect')
elif st.session_state.authentication_status is None: pass

# --- END OF FILE app.py ---
