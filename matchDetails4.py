import json 
import pandas as pd
import os
import sys
from collections import defaultdict

# Command-line arguments for year and event
year = sys.argv[1]  # Example: "2024"
event = sys.argv[2]  # Example: "vct-international"

# Path to the games directory
games_dir = f"{event}/games/{year}/"

# Load the necessary static data files
with open(f"{event}/esports-data/mapping_data.json", "r") as mapping_file:
    mapping_data = json.load(mapping_file)

with open(f"{event}/esports-data/players.json", "r", encoding="utf-8") as players_file:
    players_data = json.load(players_file)

with open(f"{event}/esports-data/teams.json", "r") as teams_file:
    teams_data = json.load(teams_file)

with open(f"{event}/esports-data/agent.txt", "r") as agent_file:
    agent_mapping = json.load(agent_file)

# Agent type classification
agent_type_mapping = {
    "Duelist": ["Jett", "Reyna", "Raze", "Yoru", "Phoenix", "Neon", "Iso"],
    "Controller": ["Omen", "Brimstone", "Astra", "Viper", "Harbor"],
    "Sentinel": ["Killjoy", "Cypher", "Sage", "Chamber", "Deadlock"],
    "Initiator": ["Breach", "Skye", "Sova", "Kayo", "Fade", "Gekko"]
}

# Function to determine agent type
def get_agent_type(agent_name):
    for agent_type, agents in agent_type_mapping.items():
        if agent_name in agents:
            return agent_type
    return 'Unknown'  # Default if agent type is not found

# Iterate over all game JSON files in the directory
for game_file in os.listdir(games_dir):
    if game_file.endswith(".json"):
        # Construct the game_id from the file name
        game_id = f"val:{game_file.split('_')[1].replace('.json', '')}"
        print(f"Processing game ID: {game_id}")

        # Load the game events
        with open(os.path.join(games_dir, game_file), "r") as new_game_data_file:
            game_events = json.load(new_game_data_file)

        # Find the relevant game mappings based on the game_id
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

        # Initialize a dictionary to store player stats
        player_stats = defaultdict(lambda: {'kills': 0, 'deaths': 0, 'assists': 0, 'damage': 0, 'ability_uses': 0,
                                            'first_bloods': 0, 'first_deaths': 0, 'agent': 'Unknown', 'agent_type': 'Unknown', 'tier': 'VCT International'})

        # Function to map agents to players from configuration events
        def map_agents_to_players(event):
            if 'configuration' in event:
                # Extract teams and players from the configuration
                configuration = event.get('configuration', {})
                # Loop through each player in the configuration
                for player in configuration.get('players', []):
                    player_id = player.get('playerId', {}).get('value', None)
                    agent_guid = player.get('selectedAgent', {}).get('fallback', {}).get('guid', '').lower()  # Extract the agent GUID safely
                    if player_id is not None and agent_guid:
                        # Directly look up the agent name using the agent GUID
                        agent_name = agent_mapping.get(agent_guid, 'Unknown')  # 'Unknown' if GUID not found
                        player_stats[player_id]['agent'] = agent_name
                        # Assign agent type using the mapping
                        player_stats[player_id]['agent_type'] = get_agent_type(agent_name)

        def update_player_stats(event):
            if 'playerDied' in event:
                deceased = event.get('playerDied', {}).get('deceasedId', {}).get('value', None)
                killer = event.get('playerDied', {}).get('killerId', {}).get('value', None)
                assistants = event.get('playerDied', {}).get('assistants', [])

                if killer is not None:
                    player_stats[killer]['kills'] += 1
                if deceased is not None:
                    player_stats[deceased]['deaths'] += 1

                for assistant in assistants:
                    assistant_id = assistant.get('assistantId', {}).get('value', None)
                    if assistant_id is not None:
                        player_stats[assistant_id]['assists'] += 1

            elif 'damageEvent' in event:
                causer = event.get('damageEvent', {}).get('causerId', {}).get('value', None)
                damage_amount = event.get('damageEvent', {}).get('damageAmount', 0)

                if causer is not None:
                    player_stats[causer]['damage'] += damage_amount

            elif 'abilityUsed' in event:
                player = event.get('abilityUsed', {}).get('playerId', {}).get('value', None)
                if player is not None:
                    player_stats[player]['ability_uses'] += 1

        def track_first_bloods(events, current_round):
            global round_active, first_kill_recorded
            for event in events:
                if 'roundStarted' in event:
                    round_active = True
                    first_kill_recorded = False
                    current_round = event.get('roundStarted', {}).get('roundNumber', current_round)

                elif 'playerDied' in event and round_active and not first_kill_recorded:
                    killer = event.get('playerDied', {}).get('killerId', {}).get('value', None)
                    victim = event.get('playerDied', {}).get('deceasedId', {}).get('value', None)

                    if killer is not None:
                        player_stats[killer]['first_bloods'] += 1
                    if victim is not None:
                        player_stats[victim]['first_deaths'] += 1

                    first_kill_recorded = True

        # Process all events in the game data
        current_round = 0
        for event in game_events:
            map_agents_to_players(event)
            track_first_bloods([event], current_round)
            update_player_stats(event)

        # Convert player stats to a DataFrame (without revives)
        player_stats_df = pd.DataFrame.from_dict(player_stats, orient='index').reset_index()
        player_stats_df.columns = ['playerId', 'kills', 'deaths', 'assists', 'damage', 'ability_uses', 'first_bloods', 'first_deaths', 'agent', 'agent_type', 'tier']

        # Convert playerId to string to match the type in players_in_game_unique
        player_stats_df['playerId'] = player_stats_df['playerId'].astype(str)

        # Merge player stats with player details
        combined_stats = players_in_game_unique.merge(player_stats_df, left_on='playerId', right_on='playerId', how='left')

        # Sort players by team (home_team_id) and within team by kills
        combined_stats = combined_stats.sort_values(by=['home_team_id', 'kills'], ascending=[True, False])

        # Select and reorder columns for display
        display_columns = ['handle', 'first_name', 'last_name', 'home_team_id', 'agent', 'agent_type', 'tier', 'kills', 'deaths', 'assists', 'damage', 'ability_uses', 'first_bloods', 'first_deaths']
        final_stats = combined_stats[display_columns]

        # Save the final sorted combined stats to JSON
        game_file_name = os.path.basename(game_file)
        folder_name = os.path.splitext(game_file_name)[0]

        # Create the directory structure if it doesn't exist
        output_directory = "processedData"
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        # Define the JSON output path
        output_json = os.path.join(output_directory, f"{folder_name}.json")

        # Save the final sorted combined stats to JSON
        final_stats.to_json(output_json, orient="records", indent=4)

        print(f"Data has been saved to {output_json}")
