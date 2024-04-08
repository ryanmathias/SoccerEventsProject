import psycopg
import json

# Connect to your PostgreSQL database
conn = psycopg.connect(
    "dbname=dbexport user=postgres "
    "password=Timpw4mSQLA! host=localhost port=5432"
)
cursor = conn.cursor()
match_ids = []

# Read JSON data from file
try:        
    with open('competitions.json', encoding="utf-8") as file5:
        json_competitions_data = json.load(file5)

except Exception as e:
    print(f"Failed to read JSON file: {e}")
    cursor.close()
    conn.close()
    exit(1)

# Delete and rebuild the tables    
cursor.execute("DROP TABLE competitions, matches, lineups, events, pass, ball_receipt, ball_recovery, foul_committed, fifty_fifty, block, carry, "
               "clearance, dribble, dribbled_past, duel, foul_won, goalkeeper, half_end, half_start, injury_stoppage, interception, miscontrol, "
               "player_off, pressure, substitution, shot, tactics, shot_freeze_frame")
    
cursor.execute("CREATE TABLE Competitions (competition_id INT, "
               "season_id INT, "
               "country_name TEXT, "
               "competition_name TEXT, "
               "competition_gender TEXT, "
               "competition_youth TEXT, "
               "competition_international TEXT, "
               "season_name TEXT);")

cursor.execute("CREATE TABLE Matches (match_id INT PRIMARY KEY, "
               "match_date DATE NOT NULL, "
               "kick_off TIME NOT NULL, "
               "country_name TEXT, "
               "competition_id INT, "
               "competition_name TEXT, "
               "season_id INT, "
               "season_name TEXT, "
               "home_team_id INT, "
               "home_team_name TEXT, "
               "home_team_gender TEXT, "
               "home_team_group TEXT, "
               "home_team_country_id INT, "
               "home_team_country_name TEXT, "
               "home_team_manager_id INT, "
               "home_team_manager_dob DATE, "
               "home_team_manager_name VARCHAR(255), "
               "home_team_manager_country_id INT, "
               "home_team_manager_country_name TEXT, "
               "home_team_manager_nickname TEXT, "
               "away_team_id INT, "
               "away_team_name TEXT, "
               "away_team_gender TEXT, "
               "away_team_group TEXT, "
               "away_team_country_id INT, "
               "away_team_country_name TEXT, "
               "away_team_manager_id INT, "
               "away_team_manager_dob DATE, "
               "away_team_manager_name VARCHAR(255), "
               "away_team_manager_country_id INT, "
               "away_team_manager_country_name TEXT, "
               "away_team_manager_nickname TEXT, "
               "home_score INT, "
               "away_score INT, "
               "match_week INT, "
               "competition_stage_id INT, "
               "competition_stage_name TEXT, "
               "stadium_id INT, "
               "stadium_name TEXT, "
               "stadium_country_id INT, "
               "stadium_country_name TEXT, "
               "referee_id INT, "
               "referee_name TEXT, "
               "referee_country_id INT, "
               "referee_country_name TEXT);")

cursor.execute("CREATE TABLE Lineups (team_id INT, "
               "match_id INT REFERENCES Matches(match_id), "
               "team_name TEXT, "
               "lineup_player_id INT, "
               "lineup_player_name TEXT, "
               "lineup_player_nickname TEXT, "
               "lineup_jersey_number INT, "
               "lineup_country_id INT, "
               "lineup_country_name TEXT, "
               "cards_time TEXT, "
               "cards_card_type TEXT, "
               "cards_reason TEXT, "
               "cards_period INT, "
               "positions_position_id INT, "
               "positions_position TEXT, "
               "positions_from TEXT, "
               "positions_to TEXT, "
               "positions_from_period INT, "
               "positions_to_period INT, "
               "positions_start_reason TEXT, "
               "positions_end_reason TEXT);")

cursor.execute("CREATE TABLE Pass (event_id TEXT UNIQUE, pass_recipient_id INT, "
               "pass_recipient_name TEXT, "
               "pass_length DOUBLE PRECISION, "
               "pass_angle DOUBLE PRECISION, "
               "pass_height_id INT, "
               "pass_height_name TEXT, "
               "pass_end_location REAL[], "
               "assisted_shot_id TEXT, "
               "backheel BOOLEAN, "
               "deflected BOOLEAN, "
               "miscommunication BOOLEAN, "
               "pass_cross BOOLEAN, "
               "cut_back BOOLEAN, "
               "switch BOOLEAN, "
               "shot_assist BOOLEAN, "
               "goal_assist BOOLEAN, "
               "pass_body_part_id INT, "
               "pass_body_part_name TEXT, "
               "pass_type_id INT, "
               "pass_type_name TEXT, "
               "pass_outcome_id INT, "
               "pass_outcome_name TEXT, "
               "pass_technique_id INT, "
               "pass_technique_name TEXT);")

cursor.execute("CREATE TABLE Ball_Receipt (event_id TEXT UNIQUE, ball_receipt_outcome_id INT, "
               "ball_receipt_outcome_name TEXT);")

cursor.execute("CREATE TABLE Foul_Committed (event_id TEXT UNIQUE, counterpress BOOLEAN, foul_card_id INT, foul_card_name TEXT, foul_advantage BOOLEAN, "
               "foul_offensive BOOLEAN, penalty BOOLEAN, foul_type_id INT, foul_type_name TEXT);")

cursor.execute("CREATE TABLE Fifty_Fifty (event_id TEXT UNIQUE, fifty_fifty_outcome_id INT, fifty_fifty_outcome_name TEXT, counterpress BOOLEAN);")

cursor.execute("CREATE TABLE Ball_Recovery (event_id TEXT UNIQUE, offensive BOOLEAN, recovery_failure BOOLEAN);")

cursor.execute("CREATE TABLE Block (event_id TEXT UNIQUE, deflection BOOLEAN, offensive BOOLEAN, save_block BOOLEAN, counterpress BOOLEAN);")

cursor.execute("CREATE TABLE Carry (event_id TEXT UNIQUE, end_location REAL[]);")

cursor.execute("CREATE TABLE Clearance (event_id TEXT UNIQUE, aerial_won BOOLEAN, body_part_id INT, body_part_name TEXT);")

cursor.execute("CREATE TABLE Dribble (event_id TEXT UNIQUE, overrun BOOLEAN, nutmeg BOOLEAN, outcome_id INT, outcome_name TEXT, no_touch BOOLEAN);")

cursor.execute("CREATE TABLE Dribbled_Past (event_id TEXT UNIQUE, counterpress BOOLEAN);")

cursor.execute("CREATE TABLE Duel (event_id TEXT UNIQUE, counterpress BOOLEAN, type_id INT, type_name TEXT, outcome_id INT, outcome_name TEXT);")

cursor.execute("CREATE TABLE Foul_Won (event_id TEXT UNIQUE, defensive BOOLEAN, advantage BOOLEAN, penalty BOOLEAN);")

cursor.execute("CREATE TABLE Goalkeeper (event_id TEXT UNIQUE, end_location REAL[], position_id INT, position_name TEXT, technique_id INT, technique_name TEXT, "
               "body_part_id INT, body_part_name TEXT, type_id INT, type_name TEXT, outcome_id INT, outcome_name TEXT);")

cursor.execute("CREATE TABLE Half_End (event_id TEXT UNIQUE, early_video_end BOOLEAN, match_suspended BOOLEAN);")

cursor.execute("CREATE TABLE Half_Start (event_id TEXT UNIQUE, late_video_start BOOLEAN);")

cursor.execute("CREATE TABLE Injury_Stoppage (event_id TEXT UNIQUE, in_chain BOOLEAN);")

cursor.execute("CREATE TABLE Interception (event_id TEXT UNIQUE, outcome_id INT, outcome_name TEXT);")

cursor.execute("CREATE TABLE Miscontrol (event_id TEXT UNIQUE, aerial_won BOOLEAN);")

cursor.execute("CREATE TABLE Player_Off (event_id TEXT UNIQUE, permanent BOOLEAN);")

cursor.execute("CREATE TABLE Pressure (event_id TEXT UNIQUE, counterpress BOOLEAN);")

cursor.execute("CREATE TABLE Substitution (event_id TEXT UNIQUE, replacement_id INT, replacement_name TEXT, outcome_id INT, outcome_name TEXT);")

cursor.execute("CREATE TABLE Shot (event_id TEXT UNIQUE, "
               "match_id INT REFERENCES Matches(match_id), "
               "key_pass_id TEXT, "
               "end_location REAL[], "
               "aerial_won BOOLEAN, "
               "follows_dribble BOOLEAN, "
               "first_time BOOLEAN, "
               "open_goal BOOLEAN, "
               "statsbomb_xg REAL, "
               "deflected BOOLEAN, "
               "technique_id INT, "
               "technique_name TEXT, "
               "body_part_id INT, "
               "body_part_name TEXT, "
               "type_id INT, "
               "type_name TEXT, "
               "outcome_id INT, "
               "outcome_name TEXT);")

cursor.execute("CREATE TABLE Events (id TEXT UNIQUE, "
               "match_id INT REFERENCES Matches(match_id), "
               "index INT, "
               "period INT, "
               "timestamp TEXT, "
               "minute INT, "
               "second INT, "
               "type_id INT, "
               "type_name TEXT, "
               "possession INT, "
               "possession_team_id INT, "
               "possession_team_name TEXT, "
               "play_pattern_id INT, "
               "play_pattern_name TEXT, "
               "team_id INT, "
               "team_name TEXT, "
               "player_id INT, "
               "player_name TEXT, "
               "position_id INT, "
               "position_name TEXT, "
               "duration REAL, "
               "location INT[], "
               "related_events TEXT[], "
               "under_pressure BOOLEAN, "
               "off_camera BOOLEAN, "
               "out BOOLEAN);")

cursor.execute("CREATE TABLE Tactics (event_id TEXT, "
               "tactics_formation INT, "
               "tactics_lineup_player_id INT, "
               "tactics_lineup_player_name TEXT, "
               "tactics_lineup_position_id INT, "
               "tactics_lineup_position_name TEXT, "
               "tactics_lineup_jersey_number INT);")

cursor.execute("CREATE TABLE Shot_Freeze_Frame (event_id TEXT, "
               "freeze_frame_location REAL[], "
               "freeze_frame_player_id INT, "
               "freeze_frame_player_name TEXT, "
               "freeze_frame_position_id INT, "
               "freeze_frame_position_name TEXT, "
               "freeze_frame_teammate BOOLEAN);")

# COMPETITION INFORMATION
for data in json_competitions_data:
    season_id = json.dumps(data['season_id'])
    if int(season_id) in [90, 42, 4, 44]:
        competition_id = json.dumps(data['competition_id'])
        country_name = json.dumps(data['country_name'])
        competition_name = json.dumps(data['competition_name'])
        competition_gender = json.dumps(data['competition_gender'])
        competition_youth = json.dumps(data['competition_youth'])
        competition_international = json.dumps(data['competition_international'])
        season_name = json.dumps(data['season_name'])
        
        cursor.execute("INSERT INTO Competitions (competition_id, season_id, country_name, competition_name, "
                       "competition_gender, competition_youth, competition_international, season_name)"
                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                       (competition_id, season_id, country_name, competition_name, 
                        competition_gender, competition_youth, competition_international, season_name)) 
        
# MATCH TABLE INFORMATION
seasons = [90, 42, 4, 44]

season_file_paths = ['Seasons\\' + str(seasons[0]) + '.json', 'Seasons\\' + str(seasons[1]) + '.json', 'Seasons\\' + str(seasons[2]) 
              + '.json', 'Seasons\\' + str(seasons[3]) + '.json']

for file_path in season_file_paths:
    with open(file_path, encoding="utf-8") as file:
        json_matches_data = json.load(file)
        
    for data in json_matches_data:
        country_name = json.dumps(data['competition']['country_name'])
        competition_id = json.dumps(data['competition']['competition_id'])
        competition_name = json.dumps(data['competition']['competition_name'])
        season_id = json.dumps(data['season']['season_id'])
        season_name = json.dumps(data['season']['season_name'])
        
        home_team_id = data['home_team']['home_team_id']
        home_team_name = data['home_team']['home_team_name']
        home_team_gender = data['home_team']['home_team_gender']
        home_team_group = data['home_team']['home_team_group']
        home_team_country_id = json.dumps(data['home_team']['country']['id'])
        home_team_country_name = json.dumps(data['home_team']['country']['name'])
        
        away_team_id = data['away_team']['away_team_id']
        away_team_name = data['away_team']['away_team_name']
        away_team_gender = data['away_team']['away_team_gender']
        away_team_group = data['away_team']['away_team_group']
        
        # Checking missing home team manager
        home_team_manager = data.get('home_team').get('managers')
        if home_team_manager is not None:
            home_team_manager_id = json.dumps(data['home_team']['managers'][0]['id'])
            home_team_manager_dob = json.dumps(data['home_team']['managers'][0]['dob'])
            home_team_manager_name = json.dumps(data['home_team']['managers'][0]['name'])
            home_team_manager_country_id = json.dumps(data['home_team']['managers'][0]['country']['id'])
            home_team_manager_country_name = json.dumps(data['home_team']['managers'][0]['country']['name'])
            home_team_manager_nickname = json.dumps(data['home_team']['managers'][0]['nickname'])
        else:
            home_team_manager_id = None
            home_team_manager_dob = None
            home_team_manager_name = None
            home_team_manager_country_id = None
            home_team_manager_country_name = None
            home_team_manager_nickname = None
        
        # Checking missing away team manager
        away_team_manager = data.get('away_team').get('managers')
        if away_team_manager is not None:
            away_team_country_id = json.dumps(data['away_team']['country']['id'])
            away_team_country_name = json.dumps(data['away_team']['country']['name'])
            away_team_manager_id = json.dumps(data['away_team']['managers'][0]['id'])
            away_team_manager_dob = json.dumps(data['away_team']['managers'][0]['dob'])
            away_team_manager_name = json.dumps(data['away_team']['managers'][0]['name'])
            away_team_manager_country_id = json.dumps(data['away_team']['managers'][0]['country']['id'])
            away_team_manager_country_name = json.dumps(data['away_team']['managers'][0]['country']['name'])
            away_team_manager_nickname = json.dumps(data['away_team']['managers'][0]['nickname'])
        else:
            away_team_country_id = None
            away_team_country_name = None
            away_team_manager_id = None
            away_team_manager_dob = None
            away_team_manager_name = None
            away_team_manager_country_id = None
            away_team_manager_country_name = None
            away_team_manager_nickname = None
        
        competition_stage_id = json.dumps(data['competition_stage']['id'])
        competition_stage_name = json.dumps(data['competition_stage']['name'])
        
        # Checking missing stadium
        stadium = data.get('stadium')
        if stadium is not None:
            stadium_id = json.dumps(data['stadium']['id'])
            stadium_name = json.dumps(data['stadium']['name'])
            stadium_country_id = json.dumps(data['stadium']['country']['id'])
            stadium_country_name = json.dumps(data['stadium']['country']['name']) 
        else:
            stadium_id = None
            stadium_name = None
            stadium_country_id = None
            stadium_country_name = None
        
        # If referee is null
        referee_data = data.get('referee')
        if referee_data is not None:
            referee_id = json.dumps(referee_data.get('id'))
            referee_name = json.dumps(referee_data.get('name'))
            referee_country_id = json.dumps(referee_data['country'].get('id'))
            referee_country_name = json.dumps(referee_data['country'].get('name'))
        else:
            referee_id = None
            referee_name = None
            referee_country_id = None
            referee_country_name = None

        cursor.execute("INSERT INTO Matches (match_id, match_date, kick_off, country_name, competition_id, "
                        "competition_name, season_id, season_name, home_team_id, home_team_name, home_team_gender, home_team_group, home_team_country_id, home_team_country_name, "
                        "home_team_manager_id, home_team_manager_dob, home_team_manager_name, home_team_manager_country_id, home_team_manager_country_name, "
                        "home_team_manager_nickname, away_team_id, away_team_name, away_team_gender, away_team_group, away_team_country_id, away_team_country_name, "
                        "away_team_manager_id, away_team_manager_dob, "
                        "away_team_manager_name, away_team_manager_country_id, away_team_manager_country_name, away_team_manager_nickname, "
                        "home_score, away_score, match_week, competition_stage_id, competition_stage_name, "
                        "stadium_id, stadium_name, stadium_country_id, stadium_country_name, referee_id, referee_name, referee_country_id, referee_country_name) "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (data['match_id'], data['match_date'], data['kick_off'],
                            country_name, competition_id, competition_name, season_id, season_name, home_team_id, home_team_name, home_team_gender, 
                            home_team_group, home_team_country_id, home_team_country_name, 
                            home_team_manager_id, home_team_manager_dob, home_team_manager_name, home_team_manager_country_id, 
                            home_team_manager_country_name, home_team_manager_nickname, away_team_id, away_team_name, away_team_gender, away_team_group, 
                            away_team_country_id, away_team_country_name,
                            away_team_manager_id, away_team_manager_dob, away_team_manager_name, away_team_manager_country_id, 
                            away_team_manager_country_name, away_team_manager_nickname, data['home_score'], data['away_score'], data['match_week'],
                            competition_stage_id, competition_stage_name, stadium_id, stadium_name, stadium_country_id, stadium_country_name,
                            referee_id, referee_name, referee_country_id, referee_country_name))
        
        match_ids.append(data['match_id'])

# LINEUPS TABLE        
lineups_path = 'open-data-0067cae166a56aa80b2ef18f61e16158d6a7359a\open-data-0067cae166a56aa80b2ef18f61e16158d6a7359a\data\lineups\\'
match_ids_list_path = 'MatchIds.txt'
       
with open(match_ids_list_path, "r") as file:
    # Read each line of the text file to get the json file names
    for line in file:
        
        # Strip any leading/trailing whitespace and newline characters
        line = line.strip()
        
        with open(lineups_path + line, "r", encoding="utf-8") as json_file: # + line
            try:
                lineup_data = json.load(json_file)
                
            except Exception as e:
                print(f"Failed to read JSON file: {e}")
                cursor.close()
                conn.close()
                exit(1)
                
            # Iterate over each team in the JSON data
            for team_data in lineup_data:
                team_id = team_data["team_id"]
                team_name = team_data["team_name"]
                
                # Iterate over each lineup object in the team's lineup array
                for lineup_obj in team_data["lineup"]:
                    lineup_player_id = lineup_obj["player_id"]
                    lineup_player_name = lineup_obj["player_name"]
                    lineup_player_nickname = lineup_obj["player_nickname"]
                    lineup_jersey_number = lineup_obj["jersey_number"]
                    
                    # Extract country information
                    country_data = lineup_obj["country"]
                    country_id = country_data["id"]
                    country_name = country_data["name"]
                
                    cards_empty = len(lineup_obj["cards"]) == 0
                    if not cards_empty:
                        lineup_cards_time = json.dumps(lineup_obj['cards'][0]['time'])
                        lineup_cards_card_type = json.dumps(lineup_obj['cards'][0]['card_type'])
                        lineup_cards_reason = json.dumps(lineup_obj['cards'][0]['reason'])
                        lineup_cards_period = json.dumps(lineup_obj['cards'][0]['period'])
                    else:
                        lineup_cards_time = None
                        lineup_cards_card_type = None
                        lineup_cards_reason = None
                        lineup_cards_period = None
                        
                    positions_empty = len(lineup_obj["positions"]) == 0
                    if not positions_empty:
                        positions_position_id = json.dumps(lineup_obj['positions'][0]['position_id'])
                        positions_position = json.dumps(lineup_obj['positions'][0]['position'])
                        positions_from = json.dumps(lineup_obj['positions'][0]['from'])
                        positions_to = json.dumps(lineup_obj['positions'][0]['to'])
                        positions_from_period = json.dumps(lineup_obj['positions'][0]['from_period'])
                        positions_to_period = json.dumps(lineup_obj['positions'][0]['to_period'])
                        if positions_to_period == "null":
                            positions_to_period = None
                
                        positions_start_reason = json.dumps(lineup_obj['positions'][0]['start_reason'])
                        positions_end_reason = json.dumps(lineup_obj['positions'][0]['end_reason'])
                        
                    else:
                        positions_position_id = None
                        positions_position = None
                        positions_from = None
                        positions_to = None
                        positions_from_period = None
                        positions_to_period = None
                        positions_start_reason = None
                        positions_end_reason = None
                        
                    newLineMatchIdLineups = line.replace('.json', '')
                    
                    cursor.execute("INSERT INTO Lineups (team_id, match_id, team_name, lineup_player_id, lineup_player_name, lineup_player_nickname, "
                                    "lineup_jersey_number, lineup_country_id, lineup_country_name, cards_time, cards_card_type, cards_reason, "
                                    "cards_period, positions_position_id, positions_position, positions_from, positions_to, positions_from_period, "
                                    "positions_to_period, positions_start_reason, positions_end_reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                                    (team_id, newLineMatchIdLineups, team_name, lineup_player_id, lineup_player_name, lineup_player_nickname, 
                                    lineup_jersey_number, country_id, country_name, lineup_cards_time, lineup_cards_card_type, lineup_cards_reason, 
                                    lineup_cards_period, positions_position_id, positions_position, positions_from, positions_to, positions_from_period, 
                                    positions_to_period, positions_start_reason, positions_end_reason))

               
# EVENTS TABLE        
events_path = 'open-data-0067cae166a56aa80b2ef18f61e16158d6a7359a\open-data-0067cae166a56aa80b2ef18f61e16158d6a7359a\data\events\\'
match_ids_list_path = 'MatchIds.txt'
       
with open(match_ids_list_path, "r") as file:
    # Read each line of the text file to get the json file names
    for line in file:
        
        # Strip any leading/trailing whitespace and newline characters
        line = line.strip()
        
        with open(events_path + line, "r", encoding="utf-8") as json_file: # "3773386.json"
            try:
                event_data = json.load(json_file)
                print(line)
                
            except Exception as e:
                print(f"Failed to read JSON file: {e}")
                cursor.close()
                conn.close()
                exit(1)
                
            # Iterate over each team in the JSON data
            for data in event_data:
                id = data["id"]
                index = data["index"]
                period = data["period"]
                timestamp = data["timestamp"]
                minute = data["minute"]
                second = data["second"]
                type_id = data["type"]["id"]
                type_name = data["type"]["name"]
                possession = data["possession"]
                possession_team_id = data["possession_team"]["id"]
                possession_team_name = data["possession_team"]["name"]
                play_pattern_id = data["play_pattern"]["id"]
                play_pattern_name = data["play_pattern"]["name"]
                team_id = data["team"]["id"]
                team_name = data["team"]["name"]
                
                player = data.get("player")
                if player is not None:
                    player_id = data['player']['id']
                    player_name = data['player']['name']
                else:
                    player_id = None
                    player_name = None
                    
                position = data.get("position")
                if position is not None:
                    position_id = data['position']['id']
                    position_name = data['position']['name']
                else:
                    position_id = None
                    position_name = None
                
                duration = data.get("duration")
                if duration is not None:
                    duration = data["duration"]
                else:
                    duration = None
                    
                location = data.get("location")
                if location is not None:
                    location = data["location"]
                else:
                    location = None
                    
                related_events = data.get("related_events")
                if related_events is not None:
                    related_events = data["related_events"]
                else:
                    related_events = None
                    
                under_pressure = data.get("under_pressure")
                if under_pressure is not None:
                    under_pressure = data["under_pressure"]
                else:
                    under_pressure = None
                    
                out = data.get("out")
                if out is not None:
                    out = data["out"]
                else:
                    out = None
                    
                off_camera = data.get("off_camera")
                if off_camera is not None:
                    off_camera = data["off_camera"]
                else:
                    off_camera = None
                    
                newLineMatchId = line.replace('.json', '')
                
                cursor.execute("INSERT INTO Events (id, match_id, index, period, timestamp, "
                    "minute, second, type_id, type_name, possession, possession_team_id, "
                    "possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, "
                    "player_id, player_name, position_id, position_name, duration, location, related_events, under_pressure, off_camera, out) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO NOTHING;", 
                    (id, newLineMatchId, index, period, timestamp, minute, second, type_id, type_name, possession, possession_team_id, 
                    possession_team_name, play_pattern_id, play_pattern_name, team_id, team_name, player_id, player_name, position_id, position_name, 
                    duration, location, related_events, under_pressure, off_camera, out))
                
                    
                carry_end_location = data.get("carry")
                carry_end_location = None
                if carry_end_location is not None:
                    carry_end_location = data["carry"]["end_location"]
                    
                    cursor.execute("INSERT INTO Carry (event_id, end_location) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (id, carry_end_location))
                    
                if type_name == "Carry":
                    cursor.execute("INSERT INTO Carry (event_id, end_location) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (id, carry_end_location))
                    
                counterpress = data.get("counterpress")
                if counterpress is not None:
                    counterpress = data["counterpress"]
                
                else:
                    counterpress = None
                    
                fifty_fifty = data.get("50_50")
                fifty_fifty_outcome_id = None
                fifty_fifty_outcome_name = None
                if fifty_fifty is not None:
                    fifty_fifty_outcome_id = data["50_50"]["outcome"]["id"]
                    fifty_fifty_outcome_name = data["50_50"]["outcome"]["name"]
                                  
                else:
                    fifty_fifty_outcome_id = None
                    fifty_fifty_outcome_name = None
                    
                if fifty_fifty is not None:
                    cursor.execute("INSERT INTO Fifty_Fifty (event_id, fifty_fifty_outcome_id, fifty_fifty_outcome_name, counterpress) VALUES "
                                   "(%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING;", (id, fifty_fifty_outcome_id, fifty_fifty_outcome_name, counterpress))
                    
                if type_name == "50/50":
                    cursor.execute("INSERT INTO Fifty_Fifty (event_id, fifty_fifty_outcome_id, fifty_fifty_outcome_name, counterpress) VALUES "
                                   "(%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING;", (id, fifty_fifty_outcome_id, fifty_fifty_outcome_name, counterpress))
                
                foul_advantage = None
                foul_offensive = None
                foul_committed = data.get("foul_committed")
                card_id = None
                card_name = None
                penalty = None
                foul_type_id = None
                foul_type_name = None
                
                if foul_committed is not None and foul_committed.get("advantage") is not None:
                    foul_advantage = data["foul_committed"]["advantage"]
                            
                else:
                    foul_advantage = None
                    
                    
                if foul_committed is not None and foul_committed.get("offensive") is not None:
                    foul_offensive = data["foul_committed"]["offensive"]
                            
                else:
                    foul_offensive = None
                            
                            
                if foul_committed is not None and foul_committed.get("card") is not None:
                    card_id = data["foul_committed"]["card"]["id"]
                    card_name = data["foul_committed"]["card"]["name"]
                    
                else:
                    card_id = None
                    card_name = None
                    
                if foul_committed is not None and foul_committed.get("penalty") is not None:
                    penalty = data["foul_committed"]["penalty"]
                    
                else:
                    penalty = None
                    
                if foul_committed is not None and foul_committed.get("type") is not None:
                    foul_type_id = data["foul_committed"]["type"]["id"]
                    foul_type_name = data["foul_committed"]["type"]["name"]
                    
                else:
                    foul_type_id = None
                    foul_type_name = None
                    
                    
                if foul_committed is not None:
                    cursor.execute("INSERT INTO Foul_Committed (event_id, counterpress, foul_card_id, foul_card_name, foul_advantage, foul_offensive, penalty, "
                                   "foul_type_id, foul_type_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                   "ON CONFLICT (event_id) DO NOTHING;", 
                                   (id, counterpress, card_id, card_name, foul_advantage, foul_offensive, penalty, foul_type_id, foul_type_name))
                    
                if type_name == "Foul Committed":
                    cursor.execute("INSERT INTO Foul_Committed (event_id, counterpress, foul_card_id, foul_card_name, foul_advantage, foul_offensive, penalty, "
                                   "foul_type_id, foul_type_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                   "ON CONFLICT (event_id) DO NOTHING;", 
                                   (id, counterpress, card_id, card_name, foul_advantage, foul_offensive, penalty, foul_type_id, foul_type_name))
                    
                ball_receipt = data.get("ball_receipt")
                ball_receipt_outcome_id = None
                ball_receipt_outcome_name = None 
                if ball_receipt is not None:
                    ball_receipt_outcome_id = data["ball_receipt"]["outcome"]["id"]
                    ball_receipt_outcome_name = data["ball_receipt"]["outcome"]["name"]
                    
                else:
                    ball_receipt_outcome_id = None
                    ball_receipt_outcome_name = None
                    
                if ball_receipt is not None:
                    cursor.execute("INSERT INTO Ball_receipt (event_id, ball_receipt_outcome_id, ball_receipt_outcome_name) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING;", 
                                   (id, ball_receipt_outcome_id, ball_receipt_outcome_name))
                    
                if type_name == "Ball Receipt":
                    cursor.execute("INSERT INTO Ball_receipt (event_id, ball_receipt_outcome_id, ball_receipt_outcome_name) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING;", 
                                   (id, ball_receipt_outcome_id, ball_receipt_outcome_name))
                    
                ball_recovery = data.get("ball_recovery")
                ball_recovery_offensive = None
                ball_recovery_failure = None
                if ball_recovery is not None:
                    if ball_recovery.get("offensive") is not None:
                        ball_recovery_offensive = data["ball_recovery"]["offensive"]
                    else:
                        ball_recovery_offensive = None
                        
                    if ball_recovery.get("recovery_failure") is not None:
                        ball_recovery_failure = data["ball_recovery"]["recovery_failure"]
                        
                    else:
                        ball_recovery_failure = None
                        
                    cursor.execute("INSERT INTO Ball_Recovery (event_id, offensive, recovery_failure) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, ball_recovery_offensive, ball_recovery_failure))
                    
                if type_name == "Ball Recovery":
                    cursor.execute("INSERT INTO Ball_Recovery (event_id, offensive, recovery_failure) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, ball_recovery_offensive, ball_recovery_failure))
                    
                block = data.get("block")
                block_deflection = None
                block_offensive = None
                save_block = None
                if block is not None:
                    if block.get("deflection") is not None:
                        block_deflection = data["block"]["deflection"]
                    else:
                        block_deflection = None
                        
                    if block.get("offensive") is not None:
                        block_offensive = data["block"]["offensive"]
                        
                    else:
                        block_offensive = None
                        
                    if block.get("save_block") is not None:
                        save_block = data["block"]["save_block"]
                    
                    else:
                        save_block = None
                        
                    cursor.execute("INSERT INTO Block (event_id, deflection, offensive, counterpress) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, block_deflection,
                                    block_offensive, counterpress))
                    
                if type_name == "Block":
                    cursor.execute("INSERT INTO Block (event_id, deflection, offensive, counterpress) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, block_deflection,
                                    block_offensive, counterpress))
                    
                clearance = data.get("clearance")
                if clearance is not None:
                    clearance_aerial_won = clearance.get("aerial_won")
                    clearance_body_part = clearance.get("body_part")
                    if clearance_body_part is not None:
                        clearance_body_part_id = clearance_body_part["id"]
                        clearance_body_part_name = clearance_body_part["name"]
                    else:
                        clearance_body_part_id = None
                        clearance_body_part_name = None
                        
                    cursor.execute("INSERT INTO Clearance (event_id, aerial_won, body_part_id, body_part_name) VALUES (%s, %s, %s, %s) "
                                    "ON CONFLICT (event_id) DO NOTHING", (id, clearance_aerial_won,
                                    clearance_body_part_id, clearance_body_part_name))
                    
                dribble = data.get("dribble")
                dribble_overrun = None
                dribble_nutmeg = None
                dribble_no_touch = None
                dribble_outcome_id = None
                dribble_outcome_name = None
                if dribble is not None:
                    if dribble.get("overrun") is not None:
                        dribble_overrun = data["dribble"]["overrun"]
                    else:
                        dribble_overrun = None
                        
                    if dribble.get("nutmeg") is not None:
                        dribble_nutmeg = data["dribble"]["nutmeg"]
                    else:
                        dribble_nutmeg = None
                        
                    if dribble.get("no_touch") is not None:
                        dribble_no_touch = data["dribble"]["no_touch"]
                    else:
                        dribble_no_touch = None
                        
                    dribble_outcome_id = data["dribble"]["outcome"]["id"]
                    dribble_outcome_name = data["dribble"]["outcome"]["name"]
                    
                    cursor.execute("INSERT INTO Dribble (event_id, overrun, nutmeg, outcome_id, outcome_name, no_touch) VALUES (%s, %s, %s, %s, %s, %s) "
                                   "ON CONFLICT (event_id) DO NOTHING", (id, dribble_overrun, dribble_nutmeg, dribble_outcome_id, dribble_outcome_name, dribble_no_touch))
                    
                if type_name == "Dribbled Past": # Dribbled Past
                    cursor.execute("INSERT INTO Dribbled_Past (event_id, counterpress) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (id, counterpress))
                    
                    # cursor.execute("INSERT INTO Dribble (event_id, overrun, nutmeg, outcome_id, outcome_name, no_touch) VALUES (%s, %s, %s, %s, %s, %s) "
                    #                "ON CONFLICT (event_id) DO NOTHING", (id, dribble_overrun, dribble_nutmeg, dribble_outcome_id, dribble_outcome_name, dribble_no_touch))
                    
                duel = data.get("duel")
                duel_type_id = None
                duel_type_name = None
                duel_outcome_id = None
                duel_outcome_name = None
                if duel is not None:
                    duel_type_id = data["duel"]["type"]["id"]
                    duel_type_name = data["duel"]["type"]["name"]
                    
                    if duel.get("outcome") is not None:
                        duel_outcome_id = data["duel"]["outcome"]["id"]
                        duel_outcome_name = data["duel"]["outcome"]["name"]
                    else:
                        duel_outcome_id = None
                        duel_outcome_name = None
                    
                    cursor.execute("INSERT INTO Duel (event_id, counterpress, type_id, type_name, outcome_id, outcome_name) VALUES (%s, "
                        "%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (id, counterpress, duel_type_id, duel_type_name, duel_outcome_id, duel_outcome_name))
                    
                if type_name == "Duel":
                    cursor.execute("INSERT INTO Duel (event_id, counterpress, type_id, type_name, outcome_id, outcome_name) VALUES (%s, "
                        "%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (id, counterpress, duel_type_id, duel_type_name, duel_outcome_id, duel_outcome_name))
                    
                foul_won = data.get("foul_won")
                foul_won_advantage = None
                foul_won_defensive = None
                foul_won_penalty = None
                if foul_won is not None:
                    if foul_won.get("advantage") is not None:
                        foul_won_advantage = data["foul_won"]["advantage"]
                    else:
                        foul_won_advantage = None
                        
                    if foul_won.get("defensive") is not None:
                        foul_won_defensive = data["foul_won"]["defensive"]
                    else:
                        foul_won_defensive = None
                        
                    if foul_won.get("penalty") is not None:
                        foul_won_penalty = data["foul_won"]["penalty"]
                    else:
                        foul_won_penalty = None
                        
                    cursor.execute("INSERT INTO Foul_Won (event_id, defensive, advantage, penalty) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (
                        id, foul_won_defensive, foul_won_advantage, foul_won_penalty))
                    
                if type_name == "Foul Won":
                    cursor.execute("INSERT INTO Foul_Won (event_id, defensive, advantage, penalty) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (
                        id, foul_won_defensive, foul_won_advantage, foul_won_penalty))
                    
                goalkeeper = data.get("goalkeeper")
                if goalkeeper is not None:
                    if goalkeeper.get("position") is not None:
                        goalkeeper_position_id = data["goalkeeper"]["position"]["id"]
                        goalkeeper_position_name = data["goalkeeper"]["position"]["name"]
                    else:
                        goalkeeper_position_id = None
                        goalkeeper_position_name = None
                        
                    if goalkeeper.get("end_location") is not None:
                        goalkeeper_end_location = data["goalkeeper"]["end_location"]
                    else:
                        goalkeeper_end_location = None
                        
                    if goalkeeper.get("technique") is not None:
                        goalkeeper_technique_id = data["goalkeeper"]["technique"]["id"]
                        goalkeeper_technique_name = data["goalkeeper"]["technique"]["name"]
                    else:
                        goalkeeper_technique_id = None
                        goalkeeper_technique_name = None
                        
                    if goalkeeper.get("body_part") is not None:
                        goalkeeper_body_part_id = data["goalkeeper"]["body_part"]["id"]
                        goalkeeper_body_part_name = data["goalkeeper"]["body_part"]["name"]
                    else:
                        goalkeeper_body_part_id = None
                        goalkeeper_body_part_name = None
                        
                    if goalkeeper.get("type") is not None:
                        goalkeeper_type_id = data["goalkeeper"]["type"]["id"]
                        goalkeeper_type_name = data["goalkeeper"]["type"]["name"]
                    else:
                        goalkeeper_type_id = None
                        goalkeeper_type_name = None
                        
                    if goalkeeper.get("outcome") is not None:
                        goalkeeper_outcome_id = data["goalkeeper"]["outcome"]["id"]
                        goalkeeper_outcome_name = data["goalkeeper"]["outcome"]["name"]
                    else:
                        goalkeeper_outcome_id = None
                        goalkeeper_outcome_name = None
                        
                    cursor.execute("INSERT INTO Goalkeeper (event_id, end_location, position_id, position_name, technique_id, technique_name, body_part_id, "
                                   "body_part_name, type_id, type_name, outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                   "ON CONFLICT (event_id) DO NOTHING", (id, goalkeeper_end_location, goalkeeper_position_id, goalkeeper_position_name, goalkeeper_technique_id, 
                                    goalkeeper_technique_name, goalkeeper_body_part_id, goalkeeper_body_part_name, goalkeeper_type_id, goalkeeper_type_name, goalkeeper_outcome_id, 
                                    goalkeeper_outcome_name))
                    
                half_end = data.get("half_end")
                half_end_video = None
                half_end_suspended = None
                if half_end is not None:
                    if half_end.get("early_video_end") is not None:
                        half_end_video = data["half_end"]["early_video_end"]
                    else:
                        half_end_video = None
                        
                    if half_end.get("match_suspended") is not None:
                        half_end_suspended = data["half_end"]["match_suspended"]
                    else:
                        half_end_suspended = None
                        
                    cursor.execute("INSERT INTO Half_End (event_id, early_video_end, match_suspended) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, half_end_video, half_end_suspended))
                    
                if type_name == "Half End":
                    cursor.execute("INSERT INTO Half_End (event_id, early_video_end, match_suspended) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, half_end_video, half_end_suspended))
                    
                half_start = data.get("half_start")
                half_start_video = None
                if half_start is not None:
                    if half_start.get("late_video_start") is not None:
                        half_start_video = data["half_start"]["late_video_start"]
                    else:
                        half_start_video = None
                    
                    cursor.execute("INSERT INTO Half_Start (event_id, late_video_start) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (
                        id, half_start_video))
                    
                if type_name == "Half Start":
                    cursor.execute("INSERT INTO Half_Start (event_id, late_video_start) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (
                        id, half_start_video))
                    
                injury_stoppage = data.get("injury_stoppage")
                injury_in_chain = None
                if injury_stoppage is not None:
                    if injury_stoppage.get("in_chain") is not None:
                        injury_in_chain = data["injury_stoppage"]["in_chain"]
                    else:
                        injury_in_chain = None
                    
                    cursor.execute("INSERT INTO Injury_Stoppage (event_id, in_chain) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, injury_in_chain))
                    
                if type_name == "Injury Stoppage":
                    cursor.execute("INSERT INTO Injury_Stoppage (event_id, in_chain) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, injury_in_chain))
                    
                interception = data.get("interception")
                interception_outcome_id = None
                interception_outcome_name = None
                if interception is not None:
                    interception_outcome_id = data["interception"]["outcome"]["id"]
                    interception_outcome_name = data["interception"]["outcome"]["name"]
                    
                    cursor.execute("INSERT INTO Interception (event_id, outcome_id, outcome_name) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, interception_outcome_id, interception_outcome_name))
                    
                if type_name == "Interception":
                    cursor.execute("INSERT INTO Interception (event_id, outcome_id, outcome_name) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, interception_outcome_id, interception_outcome_name))
                
                miscontrol_aerial_won = None    
                miscontrol = data.get("miscontrol")
                if miscontrol is not None:
                    miscontrol_aerial_won = data["miscontrol"]["aerial_won"]
                    
                    cursor.execute("INSERT INTO Miscontrol (event_id, aerial_won) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, miscontrol_aerial_won))
                    
                if type_name == "Miscontrol":
                    cursor.execute("INSERT INTO Miscontrol (event_id, aerial_won) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, miscontrol_aerial_won))
                    
                passEvent = data.get("pass")
                if passEvent is not None:
                    recipient = data["pass"].get("recipient")
                    if recipient is not None:
                        pass_recipient_id = data["pass"]["recipient"]["id"]
                        pass_recipient_name = data["pass"]["recipient"]["name"]
                    else:
                        pass_recipient_id = None
                        pass_recipient_name = None
                    
                    pass_length = data["pass"]["length"]
                    pass_angle = data["pass"]["angle"]
                    pass_height_id = data["pass"]["height"]["id"]
                    pass_height_name = data["pass"]["height"]["name"]
                    pass_end_location = data["pass"]["end_location"]
                    
                    if passEvent.get("assisted_shot_id") is not None:
                        pass_assisted_shot_id = data["pass"]["assisted_shot_id"]
                    else:
                        pass_assisted_shot_id = None
                        
                    if passEvent.get("backheel") is not None:
                        pass_backheel = data["pass"]["backheel"]
                    else:
                        pass_backheel = None    
                        
                    if passEvent.get("deflected") is not None:
                        pass_deflected = data["pass"]["deflected"]
                    else:
                        pass_deflected = None
                        
                    if passEvent.get("miscommunication") is not None:
                        pass_miscommunication = data["pass"]["miscommunication"]
                    else:
                        pass_miscommunication = None
                        
                    if passEvent.get("cross") is not None:
                        pass_cross = data["pass"]["cross"]
                    else:
                        pass_cross = None
                        
                    if passEvent.get("cut_back") is not None:
                        pass_cut_back = data["pass"]["cut_back"]
                    else:
                        pass_cut_back = None
                        
                    if passEvent.get("switch") is not None:
                        pass_switch = data["pass"]["switch"]
                    else:
                        pass_switch = None
                        
                    if passEvent.get("shot_assist") is not None:
                        pass_shot_assist = data["pass"]["shot_assist"]
                    else:
                        pass_shot_assist = None
                        
                    if passEvent.get("goal_assist") is not None:
                        pass_goal_assist = data["pass"]["goal_assist"]
                    else:
                        pass_goal_assist = None
                    
                    pass_body_part = data["pass"].get("body_part")
                    if pass_body_part is not None:
                        pass_body_part_id = data["pass"]["body_part"]["id"]
                        pass_body_part_name = data["pass"]["body_part"]["name"]
                        
                    else:
                        pass_body_part_id = None
                        pass_body_part_name = None
                        
                    if passEvent.get("type") is not None:
                        pass_type_id = data["pass"]["type"]["id"]
                        pass_type_name = data["pass"]["type"]["name"]
                    else:
                        pass_type_id = None
                        pass_type_name = None
                        
                    pass_outcome = data["pass"].get("outcome")
                    if pass_outcome is not None:
                        pass_outcome_id = data["pass"]["outcome"]["id"]
                        pass_outcome_name = data["pass"]["outcome"]["name"]
                        
                    else:
                        pass_outcome_id = None
                        pass_outcome_name = None
                        
                    if passEvent.get("technique") is not None:
                        pass_technique_id = data["pass"]["technique"]["id"]
                        pass_technique_name = data["pass"]["technique"]["name"]
                    else:
                        pass_technique_id = None
                        pass_technique_name = None
                    
                else:
                    pass_recipient_id = None
                    pass_recipient_name = None
                    pass_length = None
                    pass_angle = None
                    pass_height_id = None
                    pass_height_name = None
                    pass_end_location = None
                    pass_body_part_id = None
                    pass_body_part_name = None
                    pass_outcome_id = None
                    pass_outcome_name = None
                    
                if passEvent is not None:
                    # Pass Table Insert
                    cursor.execute("INSERT INTO Pass (event_id, pass_recipient_id, pass_recipient_name, "
                                    "pass_length, pass_angle, pass_height_id, pass_height_name, pass_end_location, assisted_shot_id, "
                                    "backheel, deflected, miscommunication, pass_cross, cut_back, switch, shot_assist, goal_assist, "
                                    "pass_body_part_id, pass_body_part_name, pass_type_id, pass_type_name, "
                                    "pass_outcome_id, pass_outcome_name, pass_technique_id, pass_technique_name) VALUES (%s, %s, %s, %s, %s, %s, %s, "
                                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                    "%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                    (id, pass_recipient_id, pass_recipient_name, 
                                    pass_length, pass_angle, pass_height_id, pass_height_name, pass_end_location, pass_assisted_shot_id, pass_backheel, 
                                    pass_deflected, pass_miscommunication, pass_cross, pass_cut_back, pass_switch, pass_shot_assist, pass_goal_assist, 
                                    pass_body_part_id, pass_body_part_name, pass_type_id, pass_type_name, pass_outcome_id, pass_outcome_name,
                                    pass_technique_id, pass_technique_name))
                    
                player_off = data.get("player_off")
                player_off_permanent = None
                if player_off is not None:
                    player_off_permanent = data["player_off"]["permanent"]
                    
                    cursor.execute("INSERT INTO Player_Off (event_id, permanent) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, player_off_permanent))
                    
                if type_name == "Player Off":
                    cursor.execute("INSERT INTO Player_Off (event_id, permanent) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, player_off_permanent))
                    
                pressure = data.get("pressure")
                if pressure is not None:
                    cursor.execute("INSERT INTO Pressure (event_id, counterpress) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, counterpress))
                    
                if type_name == "Pressure":
                    cursor.execute("INSERT INTO Pressure (event_id, counterpress) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", 
                                   (id, counterpress))
                    
                substitution = data.get("substitution")
                substitution_replacement_id = None
                substitution_replacement_name = None
                substitution_outcome_id = None
                substitution_outcome_name = None
                if substitution is not None:
                    substitution_replacement_id = data["substitution"]["replacement"]["id"]
                    substitution_replacement_name = data["substitution"]["replacement"]["name"]
                    substitution_outcome_id = data["substitution"]["outcome"]["id"]
                    substitution_outcome_name = data["substitution"]["outcome"]["name"]
                    
                    cursor.execute("INSERT INTO Substitution (event_id, replacement_id, replacement_name, outcome_id, outcome_name) VALUES (%s, "
                                   "%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (id, substitution_replacement_id, substitution_replacement_name, 
                                    substitution_outcome_id, substitution_outcome_name))
                    
                if type_name == "Substitution":
                    cursor.execute("INSERT INTO Substitution (event_id, replacement_id, replacement_name, outcome_id, outcome_name) VALUES (%s, "
                                   "%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (id, substitution_replacement_id, substitution_replacement_name, 
                                    substitution_outcome_id, substitution_outcome_name))
                    
                #Tactics info is processed after this giant loop
                    
                shot = data.get("shot")
                if shot is not None:
                    if shot.get("key_pass_id") is not None:
                        key_pass_id = data["shot"]["key_pass_id"]
                    else:
                        key_pass_id = None
                        
                    shot_end_location = data["shot"]["end_location"]
                    statsbomb_xg = data["shot"]["statsbomb_xg"]
                    
                    if shot.get("aerial_won") is not None:
                        shot_aerial_won = data["shot"]["aerial_won"]
                    else:
                        shot_aerial_won = None
                        
                    if shot.get("follows_dribble") is not None:
                        shot_follows_dribble = data["shot"]["follows_dribble"]
                    else:
                        shot_follows_dribble = None
                        
                    if shot.get("first_time") is not None:
                        shot_first_time = data["shot"]["first_time"]
                    else:
                        shot_first_time = None
                        
                    if shot.get("open_goal") is not None:
                        shot_open_goal = data["shot"]["open_goal"]
                    else:
                        shot_open_goal = None
                        
                    if shot.get("deflected") is not None:
                        shot_deflected = data["shot"]["deflected"]
                    else:
                        shot_deflected = None
                        
                    shot_technique_id = data["shot"]["technique"]["id"]
                    shot_technique_name = data["shot"]["technique"]["name"]
                    shot_body_part_id = data["shot"]["body_part"]["id"]
                    shot_body_part_name = data["shot"]["body_part"]["name"]
                    shot_type_id = data["shot"]["type"]["id"]
                    shot_type_name = data["shot"]["type"]["name"]
                    shot_outcome_id = data["shot"]["outcome"]["id"]
                    shot_outcome_name = data["shot"]["outcome"]["name"]
                        
                    cursor.execute("INSERT INTO Shot (event_id, match_id, key_pass_id, end_location, aerial_won, follows_dribble, first_time, "
                                    "open_goal, statsbomb_xg, deflected, technique_id, technique_name, body_part_id, body_part_name, type_id, type_name, "
                                    "outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                    "ON CONFLICT (event_id) DO NOTHING;", (id, newLineMatchId, key_pass_id, shot_end_location, shot_aerial_won, shot_follows_dribble, shot_first_time, 
                                    shot_open_goal, statsbomb_xg, shot_deflected, shot_technique_id, shot_technique_name, 
                                    shot_body_part_id, shot_body_part_name, shot_type_id, shot_type_name, shot_outcome_id, shot_outcome_name))
                    
                if type_name == "Shot":
                    cursor.execute("INSERT INTO Shot (event_id, match_id, key_pass_id, end_location, aerial_won, follows_dribble, first_time, "
                                    "open_goal, statsbomb_xg, deflected, technique_id, technique_name, body_part_id, body_part_name, type_id, type_name, "
                                    "outcome_id, outcome_name) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                                    "ON CONFLICT (event_id) DO NOTHING;", (id, newLineMatchId, key_pass_id, shot_end_location, shot_aerial_won, shot_follows_dribble, shot_first_time, 
                                    shot_open_goal, statsbomb_xg, shot_deflected, shot_technique_id, shot_technique_name, 
                                    shot_body_part_id, shot_body_part_name, shot_type_id, shot_type_name, shot_outcome_id, shot_outcome_name))
                        
                tactics = data.get('tactics')
                if tactics is not None:
                    tactics_formation = tactics["formation"]
                    lineup_data = []  # Accumulate lineup data

                    for info in tactics['lineup']:
                        tactics_player_id = info["player"]["id"]
                        tactics_player_name = info["player"]["name"]
                        tactics_position_id = info["position"]["id"]
                        tactics_position_name = info["position"]["name"]
                        tactics_jersey_number = info["jersey_number"]

                        # Check if the player is already in the table for the given event_id and formation
                        cursor.execute("SELECT COUNT(*) FROM Tactics WHERE event_id = %s AND tactics_formation = %s AND tactics_lineup_player_name = %s",
                                    (id, tactics_formation, tactics_player_name))
                        existing_count = cursor.fetchone()[0]

                        # Insert data only if the player is not already in the table
                        if existing_count == 0:
                            lineup_data.append((id, tactics_formation, tactics_player_id, tactics_player_name,
                                                tactics_position_id, tactics_position_name, tactics_jersey_number))

                    # Insert lineup data
                    if lineup_data:
                        cursor.executemany("INSERT INTO Tactics (event_id, tactics_formation, tactics_lineup_player_id, "
                                        "tactics_lineup_player_name, tactics_lineup_position_id, tactics_lineup_position_name, "
                                        "tactics_lineup_jersey_number) VALUES (%s, %s, %s, %s, %s, %s, %s)", lineup_data)
                        
                shot = data.get("shot")
                if shot is not None:
                    if shot.get("freeze_frame") is not None:
                        freeze_frame_info = shot["freeze_frame"]
                        for freeze_frame in freeze_frame_info:
                            freeze_frame_location = freeze_frame["location"]
                            freeze_frame_player_id = freeze_frame["player"]["id"]
                            freeze_frame_player_name = freeze_frame["player"]["name"]
                            freeze_frame_position_id = freeze_frame["position"]["id"]
                            freeze_frame_position_name = freeze_frame["position"]["name"]
                            freeze_frame_teammate = freeze_frame["teammate"]
                            
                            # Check if the player is already in the table for the given event_id and name
                            cursor.execute("SELECT COUNT(*) FROM Shot_Freeze_Frame WHERE event_id = %s AND freeze_frame_player_name = %s",
                                        (id, freeze_frame_player_name))
                            existing_count = cursor.fetchone()[0]

                            # Insert data only if the player is not already in the table
                            if existing_count == 0:
                                cursor.execute("INSERT INTO Shot_Freeze_Frame (event_id, freeze_frame_location, "
                                            "freeze_frame_player_id, freeze_frame_player_name, freeze_frame_position_id, freeze_frame_position_name, freeze_frame_teammate) "
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s) ", (id, freeze_frame_location, freeze_frame_player_id, freeze_frame_player_name, 
                                                freeze_frame_position_id, freeze_frame_position_name, freeze_frame_teammate))
                        
                        



                        
                    
                        
                    
                    
                                
            
                
        
# Commit the transaction
conn.commit()

# Close the cursor and connection
cursor.close()
conn.close()