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

# Function to scrape transaction data for a given team URL
def scrape_transactions(year, team_abbreviation, transaction_url):
    response = requests.get(transaction_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Locate the ul tag with class 'page_index'
    ul_tag = soup.find('ul', class_='page_index')

    transactions = []

    if ul_tag:
        for li in ul_tag.find_all('li'):
            # Extract the date
            span_tag = li.find('span')
            date = span_tag.get_text(strip=True) if span_tag else None

            # Extract action and player link
            p_tag = li.find('p')
            if p_tag:
                action = p_tag.contents[0].strip() if p_tag.contents else None
                a_tag = p_tag.find('a')
                player_link = f"https://www.basketball-reference.com{a_tag['href']}" if a_tag else None
                player_id = player_link.split("/")[-1].replace(".html", "") if player_link else None

                # Append transaction data
                transactions.append({
                    'Year': year,
                    'Date': date,
                    'Action': action,
                    'Player Link': player_link,
                    'Player ID': player_id,
                    'Team Abbreviation': team_abbreviation
                })

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
