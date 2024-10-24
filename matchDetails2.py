import json
import pandas as pd
from collections import defaultdict

# Load the JSON files
with open("game-changers/games/2024/val_03c7dfd8-5928-4e3d-8a03-bc61594e7aa9.json", "r") as new_game_data_file:
    game_events = json.load(new_game_data_file)

with open("game-changers/esports-data/mapping_data.json", "r") as mapping_file:
    mapping_data = json.load(mapping_file)

with open("game-changers/esports-data/players.json", "r") as players_file:
    players_data = json.load(players_file)

with open("game-changers/esports-data/teams.json", "r") as teams_file:
    teams_data = json.load(teams_file)

# Load the agent mapping file
with open("game-changers/esports-data/agent.txt", "r") as agent_file:
    agent_mapping = json.load(agent_file)

# Identify the relevant game ID and extract the necessary mappings
game_id = 'val:03c7dfd8-5928-4e3d-8a03-bc61594e7aa9'
game_mappings = next((item for item in mapping_data if item["platformGameId"] == game_id), None)

# Extract participantMapping and teamMapping for the game
if game_mappings:
    participant_mapping = game_mappings.get('participantMapping', {})
    team_mapping = game_mappings.get('teamMapping', {})

# Convert mappings to DataFrames
participants_df = pd.DataFrame(list(participant_mapping.items()), columns=['playerId', 'mappedId'])
players_df = pd.DataFrame(players_data)
teams_df = pd.DataFrame(teams_data)

team_ids = list(team_mapping.values())

# Get team names dynamically by matching the IDs from team_mapping with the teams_data DataFrame
team_names = []
for team_id in team_ids:
    team_name = teams_df[teams_df['id'] == team_id]['name'].values[0] if team_id in teams_df['id'].values else 'Unknown'
    team_names.append(team_name)

# Assuming the match is between two teams
team1_name = team_names[0] if len(team_names) > 0 else 'Unknown'
team2_name = team_names[1] if len(team_names) > 1 else 'Unknown'

# Merge player data with participants data
players_in_game = participants_df.merge(players_df, left_on='mappedId', right_on='id', how='left')

# Remove duplicates and select only 10 unique players based on their 'id'
players_in_game_unique = players_in_game.drop_duplicates(subset=['id']).head(10)

# Initialize a dictionary to store player stats, including agent info
player_stats = defaultdict(lambda: {'kills': 0, 'deaths': 0, 'assists': 0, 'damage': 0, 'revives': 0, 'ability_uses': 0,
                                    'first_bloods': 0, 'first_deaths': 0, 'agent': 'Unknown'})

# Track whether a round has started and ensure first blood is recorded only after roundStarted
round_active = False
first_kill_recorded = False

# Function to map agents to players from configuration events
def map_agents_to_players(event):
    #print(agent_mapping)
    if 'configuration' in event:
        # Extract teams and players from the configuration
        configuration = event['configuration']
        # Loop through each player in the configuration
        for player in configuration['players']:
            player_id = player['playerId']['value']
            agent_guid = player['selectedAgent']['fallback']['guid']  # Extract the agent GUID
            # Directly look up the agent name using the agent GUID
            agent_name = agent_mapping.get(agent_guid.lower(), 'Unknown')   # 'Unknown' if GUID not found
            player_stats[player_id]['agent'] = agent_name



# Function to update player stats from events
def update_player_stats(event):
    if 'playerDied' in event:
        deceased = event['playerDied']['deceasedId']['value']
        killer = event['playerDied']['killerId']['value']
        assistants = event['playerDied'].get('assistants', [])

        player_stats[killer]['kills'] += 1
        player_stats[deceased]['deaths'] += 1

        for assistant in assistants:
            assistant_id = assistant['assistantId']['value']
            player_stats[assistant_id]['assists'] += 1

    elif 'damageEvent' in event:
        causer = event['damageEvent']['causerId']['value']
        damage_amount = event['damageEvent']['damageAmount']
        player_stats[causer]['damage'] += damage_amount

    elif 'playerRevived' in event:
        revived_by = event['playerRevived']['revivedById']['value']
        player_stats[revived_by]['revives'] += 1

    elif 'abilityUsed' in event:
        player = event['abilityUsed']['playerId']['value']
        player_stats[player]['ability_uses'] += 1

# Function to track first-blood and first-death after round start
def track_first_bloods(events, current_round):
    global round_active, first_kill_recorded
    for event in events:
        if 'roundStarted' in event:  # Adjusted to use 'roundStarted'
            # Reset the round status when a new round starts
            round_active = True
            first_kill_recorded = False  # Reset the first kill flag for the new round
            current_round = event['roundStarted']['roundNumber']  # Adjust to match the round structure

        elif 'playerDied' in event and round_active and not first_kill_recorded:
            # Only count the first kill after roundStart
            killer = event['playerDied']['killerId']['value']
            victim = event['playerDied']['deceasedId']['value']

            # Track first-blood and first-death
            player_stats[killer]['first_bloods'] += 1
            player_stats[victim]['first_deaths'] += 1

            # Mark that the first kill has been recorded for this round
            first_kill_recorded = True

# Process all events in the game data
current_round = 0
for event in game_events:
      # Map agents to players
    track_first_bloods([event], current_round)
    update_player_stats(event)
    map_agents_to_players(event)

# Convert player stats to a DataFrame
player_stats_df = pd.DataFrame.from_dict(player_stats, orient='index').reset_index()
player_stats_df.columns = ['playerId', 'kills', 'deaths', 'assists', 'damage', 'revives', 'ability_uses', 'first_bloods', 'first_deaths', 'agent']

# Convert playerId to string to match the type in players_in_game_unique
player_stats_df['playerId'] = player_stats_df['playerId'].astype(str)

# Merge player stats with player details
combined_stats = players_in_game_unique.merge(player_stats_df, left_on='playerId', right_on='playerId', how='left')

# Select and reorder columns for display, including agent
display_columns = ['handle', 'first_name', 'last_name', 'home_team_id', 'agent', 'kills', 'deaths', 'assists', 'damage', 'revives', 'ability_uses', 'first_bloods', 'first_deaths']
final_stats = combined_stats[display_columns].sort_values('kills', ascending=False)

# Display the final combined stats
print(f"Match between {team1_name} and {team2_name}")
print("Combined Player Stats with Agent:")
print(final_stats)

# If you want to save this to a CSV file:
# final_stats.to_csv("combined_player_stats_with_agents.csv", index=False)
