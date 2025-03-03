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

def scrape_transactions(year, team_abbreviation, transaction_url):
    response = requests.get(transaction_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    ul_tag = soup.find('ul', class_='page_index')
    transactions = []
    max_links = 0

    if ul_tag:
        for li in ul_tag.find_all('li'):
            span_tag = li.find('span')
            date = span_tag.get_text(strip=True) if span_tag else None
            p_tag = li.find('p')
            
            if p_tag:
                description_parts = []
                links, ids, types, names = [], [], [], []
                
                for s in p_tag.contents:
                    if isinstance(s, str):
                        description_parts.append(s.strip())
                    elif s.name == 'a':
                        link = f"https://www.basketball-reference.com{s['href']}"
                        name = s.get_text(strip=True)
                        
                        if "players" in link:
                            item_type = "player"
                            item_id = link.split("/")[-1].replace(".html", "")
                        elif "teams" in link:
                            item_type = "team"
                            item_id = link.split("/")[-2]
                        else:
                            item_type, item_id = None, None
                        
                        links.append(link)
                        ids.append(item_id)
                        types.append(item_type)
                        names.append(name)
                        description_parts.append(name)
                
                description_text = " ".join(description_parts).strip()
                description_text = re.sub(r'\s+\.', '.', description_text)
                max_links = max(max_links, len(links))
                
                actions = description_text.split(";")
                for desc in actions:
                    action = desc.strip().split(" ")[0] if desc.strip() else None
                    traded, traded_to, traded_for = None, None, None
                    
                    if action == "Traded":
                        traded_match = re.search(r'Traded (.*?) to the', desc)
                        traded_to_match = re.search(r'to the (.*?) for', desc)
                        traded_for_match = re.search(r'for (.*)', desc)
                        
                        if traded_for_match:
                            traded_for_names = traded_for_match.group(1).split(', ')
                            print(f"Traded For Match: {traded_for_match.group(1)}")  # Print matched names

                            # Remove periods and handle case-insensitivity when comparing names
                            traded_for = []
                            for name in traded_for_names:
                                # Remove any trailing periods and strip spaces
                                normalized_name = name.strip().lower().rstrip('.')
                                for i, player_name in enumerate(names):
                                    # Remove any trailing periods and strip spaces from player names
                                    player_normalized = player_name.strip().lower().rstrip('.')
                                    if player_normalized == normalized_name:
                                        traded_for.append(ids[i])
                                        print(f"Matched {name} to ID {ids[i]}")
                            
                            # If no match was found for a name, debug
                            if not traded_for:
                                print(f"Could not find ID for: {traded_for_names}")

                        if traded_match:
                            traded_names = traded_match.group(1).split(', ')
                            traded = ", ".join([ids[i] for i, name in enumerate(names) if name in traded_names])
                        if traded_to_match:
                            traded_to_name = traded_to_match.group(1)
                            traded_to = next((ids[i] for i, name in enumerate(names) if name == traded_to_name), None)

                    transaction = {
                        'Year': year,
                        'Date': date,
                        'Action': action,
                        'Description': desc.strip(),
                        'Traded': traded,
                        'Traded_To': traded_to,
                        'Traded_For': ", ".join(traded_for) if traded_for else None,
                        'Team Abbreviation': team_abbreviation
                    }
                    
                    for i in range(max_links):
                        transaction[f'Name_{i+1}'] = names[i] if i < len(names) else None
                        transaction[f'ID_{i+1}'] = ids[i] if i < len(ids) else None
                        transaction[f'Link_{i+1}'] = links[i] if i < len(links) else None
                    
                    transactions.append(transaction)
    
    return transactions

grouped_years = df_team_years.groupby('Year')

for year, group in grouped_years:
    all_transactions = []
    print(f"\nProcessing transactions for Year {year}...")

    for _, row in group.iterrows():
        team_url = row['Team URL']
        match = re.search(r'/teams/([A-Z]+)/\d{4}', team_url)
        team_abbreviation = match.group(1) if match else None

        if not team_abbreviation:
            print(f"  ⚠️ Skipping invalid URL: {team_url}")
            continue

        transaction_url = team_url.replace('.html', '_transactions.html')
        print(f"  - Scraping {team_abbreviation} ({transaction_url})")
        
        transactions = scrape_transactions(year, team_abbreviation, transaction_url)
        all_transactions.extend(transactions)
        time.sleep(random.uniform(3, 4))

    df_transactions = pd.DataFrame(all_transactions)
    csv_filename = f"transactions_{year}.csv"
    df_transactions.to_csv(csv_filename, index=False)
    print(f"  ✅ Saved {csv_filename} with {len(df_transactions)} transactions.")
    time.sleep(random.uniform(30, 35))

# Define the path pattern for your CSV files (adjust the path if needed)
csv_files = glob.glob("transactions_*.csv")

# Read all the CSV files and store them in a list of DataFrames
dataframes = [pd.read_csv(file) for file in csv_files]

# Concatenate all the DataFrames into a single DataFrame
merged_df = pd.concat(dataframes, ignore_index=True)

# Save the merged DataFrame to a new CSV file
merged_df.to_csv('merged_transactions.csv', index=False)
