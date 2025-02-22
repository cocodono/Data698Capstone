import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import glob

# List to track request timestamps for rate limiting
request_times = []

def rate_limit():
    """Ensures we do not exceed the rate limit of 19 requests per minute."""
    global request_times
    current_time = time.time()
    
    # Remove timestamps older than 60 seconds
    request_times = [t for t in request_times if current_time - t < 60]
    
    if len(request_times) >= 19:
        wait_time = 60 - (current_time - request_times[0])
        print(f"Rate limit reached. Waiting for {wait_time:.2f} seconds...")
        time.sleep(wait_time)
    
    request_times.append(current_time)

def fetch_players_for_season(season):
    """
    Fetches player names, links, and IDs from the WNBA season totals page.

    Args:
        season (int): The season year to fetch data for.

    Returns:
        list: A list of dictionaries with 'Season', 'Player', 'Player Link', and 'Player ID'.
    """
    url = f"https://www.basketball-reference.com/wnba/years/{season}_totals.html"
    print(f"Fetching player list for {season} from: {url}")

    # Ensure we respect the rate limit
    rate_limit()

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to fetch player list for {season}. Status code: {response.status_code}")
        return []

    # Parse the HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Locate the player totals table
    table = soup.find('table', id='totals')
    if not table:
        print(f"No player totals table found for {season}.")
        return []

    print(f"Player totals table found for {season}. Extracting data...")

    # Extract player data
    player_data = []
    rows = table.find('tbody').find_all('tr', class_='full_table')  # Targeting player rows

    for row in rows:
        player_cell = row.find('th', {'data-stat': 'player'})
        if player_cell:
            player_link = player_cell.find('a')
            if player_link and 'href' in player_link.attrs:
                player_name = player_link.text
                player_url = "https://www.basketball-reference.com" + player_link['href']
                player_id = player_link['href'].split('/')[-1].replace('.html', '')  # Extract player ID
                
                player_data.append({
                    'Season': season,
                    'Player': player_name,
                    'Player Link': player_url,
                    'Player ID': player_id
                })
    
    print(f"Extracted {len(player_data)} players for {season}.")
    return player_data

# Loop through seasons from 1997 to 2024 and collect player data
all_players = []

for year in range(1997, 2025):  # From 1997 to 2024
    season_data = fetch_players_for_season(year)
    all_players.extend(season_data)  # Add to main list

# Convert to DataFrame
df_players = pd.DataFrame(all_players)

df_players.to_csv('df_players.csv')

# Display first few rows
print(df_players.head())


# Function to scrape a player's game logs
def fetch_game_logs(player_name, player_link, player_id, year):
    base_url = player_link.replace(".html", f"/gamelog/{year}/")
    print(f"Fetching game logs for {player_name} ({year}): {base_url}")
    
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"Failed to fetch data for {player_name} ({year}). Status code: {response.status_code}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')
    table_div = soup.find("div", id="div_wnba_pgl_basic")
    
    if not table_div:
        print(f"No game log table found for {player_name} ({year})")
        return None

    table = table_div.find("table")
    if not table:
        print(f"No table found within div for {player_name} ({year})")
        return None

    df = pd.read_html(str(table))[0]  # Convert table to DataFrame
    df["Player"] = player_name
    df["Player ID"] = player_id  # Add Player ID column
    df["Year"] = year
    return df


# Initialize list to store all game logs
all_game_logs = []

def fetch_game_logs_by_year_range(df_players, year):
    
    all_game_logs = []

    # Filter players within the specified year range
    df_filtered = df_players[df_players["Season"] == year]

    # Loop through the filtered players
    for _, row in df_filtered.iterrows():
        player_name = row["Player"]
        player_link = row["Player Link"]
        player_id = row["Player ID"]  # Fix: Include player_id
        year = row["Season"]

        # Fetch the game logs
        game_log_df = fetch_game_logs(player_name, player_link, player_id, year)  # Fix: Add player_id
        if game_log_df is not None:
            all_game_logs.append(game_log_df)

        # Sleep for 4 seconds after each request to avoid hitting the rate limit
        time.sleep(4)

    # Combine all collected game logs into a single DataFrame
    if all_game_logs:
        df_game_logs = pd.concat(all_game_logs, ignore_index=True)
        return df_game_logs
    else:
        print(f"No game logs found for {year}.")
        return None

for year in range(2024, 1996, -1):  # Looping from 2024 down to 1997
    df = fetch_game_logs_by_year_range(df_players, year)
    if df is not None:
        df.to_csv(f"wnba_game_logs_{year}.csv", index=False)
    
    time.sleep(random.randint(60, 68))

# Find all CSV files that start with "wnba_game_logs"
csv_files = glob.glob("wnba_game_logs*.csv")

# Read and combine them into a single DataFrame
df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)

df = df[df["Rk"] != "Rk"]

df['Home_Away'] = df["Unnamed: 4"].apply(lambda x: "Away" if x == '@' else 'Home')
df[['W_L', 'game_differential']] = df['Unnamed: 6'].str.extract(r'([WL])\s*\(([-+]?\d+)\)')

df = df.drop(['Unnamed: 4','Unnamed: 6'], axis = 1)

df.to_csv('wnba_player_gamelogs.csv')
