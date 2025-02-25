import requests
import pandas as pd
from bs4 import BeautifulSoup
import time  # Import the time module to use sleep

# Function to scrape the main table (active and defunct tables)
def scrape_main_table(table_id, status):
    url = 'https://www.basketball-reference.com/wnba/teams/'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the table by ID
    table = soup.find('table', {'id': table_id})
    
    # Extract the rows (skip the header row)
    table_data = []
    for row in table.find_all('tr')[1:]:  # Skip header row
        columns = row.find_all('td')
        if len(columns) > 0:
            # Extract Team name and URL from the first column (which contains a link)
            team_cell = row.find('th', {'data-stat': 'team_name'})
            if team_cell:
                team_link = team_cell.find('a', href=True)
                team_name = team_link.text.strip() if team_link else 'N/A'
                team_url = f"https://www.basketball-reference.com{team_link['href']}" if team_link else 'N/A'
            else:
                team_name = 'N/A'
                team_url = 'N/A'
            
            # Extract 'From' and 'To' years from the respective columns
            from_year = columns[0].text.strip()
            to_year = columns[1].text.strip()

            table_data.append({
                'Team': team_name,
                'Team URL': team_url,
                'From': from_year,
                'To': to_year,
                'Status': status  # Add the status (Active or Defunct)
            })
    
    return table_data

# Function to scrape the Year and Team information from the team's individual page
def scrape_team_data(team_url):
    # Send a GET request to fetch the HTML content of the team's page
    response = requests.get(team_url)
    
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Extract the table ID from the URL (the part after the final '/')
    table_id = team_url.split('/')[-2]  # This assumes the last part of the URL is the team identifier
    
    # Find the table by the ID that matches the extracted string
    table = soup.find('table', {'id': table_id})

    # Initialize list to hold team and year data
    team_year_data = []

    # If the table exists, extract data from it
    if table:
        # Loop through the rows in the table, skipping the header row
        for row in table.find_all('tr')[1:]:  # Skip header row
            columns = row.find_all('td')
            
            if len(columns) > 0:
                # Extract the Year (likely in the first column with 'data-stat="year_id"')
                year = row.find('th', {'data-stat': 'year_id'}).text.strip()  # Extract year from 'th' tag
                
                # Extract the Team (from the <a> tag in the second column with 'data-stat="team_name"')
                team_tag = columns[0].find('a')  # Assuming team link is in the first column's <a> tag
                if team_tag:
                    team_name = team_tag.text.strip()  # Team name
                    team_link = f"https://www.basketball-reference.com{team_tag['href']}"  # Full URL for the team
                else:
                    team_name = 'N/A'
                    team_link = 'N/A'

                # Add the extracted data to the list
                team_year_data.append({
                    'Year': year,
                    'Team': team_name,
                    'Team URL': team_link
                })
    
    return team_year_data

# Scrape the 'active' table and label as 'Active'
active_teams = scrape_main_table('active', 'Active')

# Scrape the 'defunct' table and label as 'Defunct'
defunct_teams = scrape_main_table('defunct', 'Defunct')

# Combine the results from both active and defunct tables
combined_teams = active_teams + defunct_teams

# Create a DataFrame from the combined list
df_teams = pd.DataFrame(combined_teams)

# Remove rows where the team name is 'N/A'
df_teams = df_teams[df_teams['Team'] != 'N/A']

# Scrape the individual team data for each team URL in the dataframe
scraped_data = []

for index, row in df_teams.iterrows():
    team_url = row['Team URL']
    
    if team_url != 'N/A':
        team_data = scrape_team_data(team_url)
        
        # Append the data to the list
        for data in team_data:
            scraped_data.append(data)
    
    # Implementing a 4-second delay between requests
    time.sleep(4)

# Convert the scraped data into a DataFrame
df_team_years = pd.DataFrame(scraped_data)

# Display the final DataFrame with Year and Team columns
print(df_team_years)
