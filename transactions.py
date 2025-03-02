import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
import glob

df_team_years = pd.read_csv('df_team_years.csv')
df_team_years = df_team_years.drop('Unnamed: 0', axis =1)
df_team_years = df_team_years[df_team_years['Year'] != 2025]

# Fu# Function to scrape transaction data for a given team URL
def scrape_transactions(year, team_abbreviation, transaction_url):
    response = requests.get(transaction_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Locate the ul tag with class 'page_index'
    ul_tag = soup.find('ul', class_='page_index')

    transactions = []
    max_links = 0  # Track max number of links in any row

    if ul_tag:
        for li in ul_tag.find_all('li'):
            # Extract the date
            span_tag = li.find('span')
            date = span_tag.get_text(strip=True) if span_tag else None

            # Extract action and player links
            p_tag = li.find('p')
            if p_tag:
                # Extract action text while keeping spacing intact
                action_parts = []
                for s in p_tag.contents:
                    if isinstance(s, str):
                        action_parts.append(s.strip())
                    elif s.name == 'a':
                        action_parts.append(s.get_text(strip=True))  # Only add the text of the <a> tag
                action = " ".join(action_parts)
                action = re.sub(r'\s+\.', '.', action)  # Remove spaces before periods

                # Extract all links and IDs, determine type
                links = [f"https://www.basketball-reference.com{a['href']}" for a in p_tag.find_all('a')]
                ids = []
                types = []

                for link in links:
                    if "players" in link:
                        types.append("player")
                        player_id = link.split("/")[-1].replace(".html", "")
                        ids.append(player_id)
                    elif "teams" in link:
                        types.append("team")
                        team_id = link.split("/")[-2]  # ID is between "/" and the year
                        ids.append(team_id)
                    else:
                        types.append(None)
                        ids.append(None)

                # Update max number of links found
                max_links = max(max_links, len(links))

                # Append transaction data
                transactions.append({
                    'Year': year,
                    'Date': date,
                    'Action': action,
                    'Links': links,
                    'IDs': ids,
                    'Types': types,
                    'Team Abbreviation': team_abbreviation
                })

    # Expand transactions to ensure consistent columns
    for transaction in transactions:
        num_links = len(transaction['Links'])
        for i in range(max_links):
            transaction[f'Link_{i+1}'] = transaction['Links'][i] if i < num_links else None
            transaction[f'ID_{i+1}'] = transaction['IDs'][i] if i < num_links else None
            transaction[f'Type_{i+1}'] = transaction['Types'][i] if i < num_links else None
        
        # Remove original lists
        del transaction['Links']
        del transaction['IDs']
        del transaction['Types']

    return transactions

# Group dataframe by 'Year' to process all teams of a given year together
grouped_years = df_team_years.groupby('Year')

for year, group in grouped_years:
    all_transactions = []  # Store all transactions for this year

    print(f"\nProcessing transactions for Year {year}...")

    for _, row in group.iterrows():
        team_url = row['Team URL']

        # Extract team abbreviation from URL
        match = re.search(r'/teams/([A-Z]+)/\d{4}', team_url)
        team_abbreviation = match.group(1) if match else None

        if not team_abbreviation:
            print(f"  ⚠️ Skipping invalid URL: {team_url}")
            continue

        # Modify URL for transactions page
        transaction_url = team_url.replace('.html', '_transactions.html')

        print(f"  - Scraping {team_abbreviation} ({transaction_url})")
        
        # Scrape transaction data
        transactions = scrape_transactions(year, team_abbreviation, transaction_url)
        all_transactions.extend(transactions)

        # Wait 3-4 seconds before moving to the next team
        time.sleep(random.uniform(3, 4))

    # Convert to DataFrame and save CSV for this year
    df_transactions = pd.DataFrame(all_transactions)
    csv_filename = f"transactions_{year}.csv"
    df_transactions.to_csv(csv_filename, index=False)
    print(f"  ✅ Saved {csv_filename} with {len(df_transactions)} transactions.")

    # Wait 30-35 seconds before processing the next year
    wait_time = random.uniform(30, 35)
    print(f"  ⏳ Waiting {wait_time:.2f} seconds before processing next year...\n")
    time.sleep(wait_time)

# Define the path pattern for your CSV files (adjust the path if needed)
csv_files = glob.glob("transactions_*.csv")

# Read all the CSV files and store them in a list of DataFrames
dataframes = [pd.read_csv(file) for file in csv_files]

# Concatenate all the DataFrames into a single DataFrame
merged_df = pd.concat(dataframes, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged_transactions.csv', index=False)
