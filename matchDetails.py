import json
import pandas as pd
from collections import defaultdict

# Load the JSON files
with open("game-changers/games/2024/val:03c7dfd8-5928-4e3d-8a03-bc61594e7aa9.json", "r") as new_game_data_file:
    game_events = json.load(new_game_data_file)

with open("game-changers/esports-data/mapping_data.json", "r") as mapping_file:
    mapping_data = json.load(mapping_file)

with open("game-changers/esports-data/players.json", "r") as players_file:
    players_data = json.load(players_file)

with open("game-changers/esports-data/teams.json", "r") as teams_file:
    teams_data = json.load(teams_file)

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

# Merge player data with participants data
players_in_game = participants_df.merge(players_df, left_on='mappedId', right_on='id', how='left')

# Remove duplicates and select only 10 unique players based on their 'id'
players_in_game_unique = players_in_game.drop_duplicates(subset=['id']).head(10)

# Initialize a dictionary to store player stats
player_stats = defaultdict(lambda: {'kills': 0, 'deaths': 0, 'assists': 0, 'damage': 0, 'revives': 0, 'ability_uses': 0})

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

# Process all events in the game data
for event in game_events:
    update_player_stats(event)

# Convert player stats to a DataFrame
player_stats_df = pd.DataFrame.from_dict(player_stats, orient='index').reset_index()
player_stats_df.columns = ['playerId', 'kills', 'deaths', 'assists', 'damage', 'revives', 'ability_uses']

# Convert playerId to string to match the type in players_in_game_unique
player_stats_df['playerId'] = player_stats_df['playerId'].astype(str)

# Merge player stats with player details
combined_stats = players_in_game_unique.merge(player_stats_df, left_on='playerId', right_on='playerId', how='left')

# Select and reorder columns for display
display_columns = ['handle', 'first_name', 'last_name', 'home_team_id', 'kills', 'deaths', 'assists', 'damage', 'revives', 'ability_uses']
final_stats = combined_stats[display_columns].sort_values('kills', ascending=False)

# Display the final combined stats
print("Combined Player Stats:")
print(final_stats)

# If you want to save this to a CSV file:
# final_stats.to_csv("combined_player_stats.csv", index=False)