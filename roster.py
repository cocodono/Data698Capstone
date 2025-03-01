import requests
import pandas as pd
import random
import time
from bs4 import BeautifulSoup

df_team_years = pd.read_csv('df_team_years.csv')
df_team_years = df_team_years.drop('Unnamed: 0', axis =1)
df_team_years = df_team_years[df_team_years['Year'] != 2025]

# Function to scrape player data from a single URL
def scrape_roster_data(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find the roster table by its id
    roster_table = soup.find('table', id='roster')
    
    # Extract the year and team abbreviation from the URL
    year = url.split('/')[-1].replace('.html', '')
    team_abbreviation = url.split('/')[-2]
    
    # List to store individual player data
    player_data = []

    # Loop through each row in the roster table to extract player ID
    rows = roster_table.find('tbody').find_all('tr')
    for row in rows:
        player_cell = row.find('td', {'data-stat': 'player'})
        if player_cell and player_cell.find('a'):
            player_link = player_cell.find('a')['href']
            player_id = player_link.split('/')[-1].replace('.html', '')  # Extract player ID
            player_data.append([year, team_abbreviation, player_id])

    return player_data

# Create an empty list to store all the data for each year
all_years_data = []

# Loop over each team in df_team_years
for year in df_team_years['Year'].unique():
    print(f"Starting scraping for year: {year}")
    
    # Create an empty list to collect all the player data for this year
    year_player_data = []
    
    # Filter the team URLs for the current year
    team_urls_for_year = df_team_years[df_team_years['Year'] == year]['Team URL']
    
    for team_url in team_urls_for_year:
        print(f"Scraping data from: {team_url}")
        
        # Scrape data from the team URL
        player_data = scrape_roster_data(team_url)
        year_player_data.extend(player_data)  # Append new data to the year list
        
        # Wait for 3 to 4 seconds before scraping the next link
        time.sleep(random.randint(3, 4))
    
    # After scraping all URLs for the current year, save to CSV
    year_df = pd.DataFrame(year_player_data, columns=['Year', 'Team Abbreviation', 'Player ID'])
    year_df.to_csv(f'roster_{year}.csv', index=False)
    all_years_data.append(year_player_data)  # Append the year data to the final list
    
    # Wait for 30 to 35 seconds before moving to the next year's data
    print(f"Finished scraping for year {year}, waiting for the next batch...")
    time.sleep(random.randint(30, 35))

# Define the path pattern for your CSV files (adjust the path if needed)
csv_files = glob.glob("roster_*.csv")

# Read all the CSV files and store them in a list of DataFrames
dataframes = [pd.read_csv(file) for file in csv_files]

# Concatenate all the DataFrames into a single DataFrame
merged_df = pd.concat(dataframes, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged_rosters.csv', index=False)
