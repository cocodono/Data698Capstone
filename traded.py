import pandas as pd
import numpy as np

# Read the CSV file using pandas
df = pd.read_csv("merged_transactions.csv")

df.loc[df['Description'].str.lower().str.contains('traded', na=False), 'Action'] = 'Traded'

traded = df[df['Action'] == 'Traded']

# Function to process each row
def filter_names_not_in_description(row):
    description = str(row["Description"]).lower()  # Convert to string and lowercase for matching
    for col in row.index:
        if col.startswith("Name_"):  
            suffix = col.split("_")[-1]  # Extract the integer suffix
            name_value = str(row[col]).strip().lower() if pd.notna(row[col]) else ""

            if name_value and name_value not in description:  
                # Set corresponding Name, Link, and ID columns to None
                row[f"Name_{suffix}"] = None
                row[f"Link_{suffix}"] = None
                row[f"ID_{suffix}"] = None
    return row

# Apply function to dataframe
traded_1 = traded.apply(filter_names_not_in_description, axis=1)

def condense_columns(df):
    core_columns = ['Year', 'Date', 'Action', 'Description', 'Team Abbreviation']  # Keep these as they are
    
    # Extract all "Name", "ID", and "Link" columns
    name_cols = [col for col in df.columns if col.startswith("Name_")]
    id_cols = [col for col in df.columns if col.startswith("ID_")]
    link_cols = [col for col in df.columns if col.startswith("Link_")]

    condensed_data = []
    
    for _, row in df.iterrows():
        core_data = row[core_columns].tolist()  # Keep Year, Date, Action, etc.
        
        # Extract (Name, ID, Link) triplets where ID is not empty
        triplets = [(row[name], row[id_col], row[link]) 
                    for name, id_col, link in zip(name_cols, id_cols, link_cols) 
                    if pd.notna(row[id_col]) and row[id_col] != ""]
        
        # Flatten the valid triplets and store them
        flattened_triplets = [item for triplet in triplets for item in triplet]
        condensed_data.append(core_data + flattened_triplets)

    # Create new condensed dataframe with reduced columns
    max_cols = max(len(row) for row in condensed_data)  # Determine the max number of columns needed
    new_column_names = core_columns + [f"{col}_{i+1}" for i in range((max_cols - len(core_columns)) // 3) for col in ["Name", "ID", "Link"]]

    condensed_df = pd.DataFrame(condensed_data, columns=new_column_names)
    
    return condensed_df

# Apply function to condense the dataframe
traded_2 = condense_columns(traded_1)

# Function to count the number of ID columns that are exactly three-character strings
def count_three_char_ids(row):
    id_cols = [col for col in traded_2.columns if col.startswith("ID_")]
    return sum(pd.notna(row[col]) and isinstance(row[col], str) and len(row[col]) == 3 for col in id_cols)

# Apply the function and filter rows where the count is 3 or more
traded_3 = traded_2[traded_2.apply(count_three_char_ids, axis=1) >= 3]

def remove_duplicate_name_id_link(df):
    # Identify relevant columns
    name_cols = sorted([col for col in df.columns if col.startswith("Name_")], key=lambda x: int(x.split("_")[1]))
    id_cols = sorted([col for col in df.columns if col.startswith("ID_")], key=lambda x: int(x.split("_")[1]))
    link_cols = sorted([col for col in df.columns if col.startswith("Link_")], key=lambda x: int(x.split("_")[1]))

    def process_row(row):
        seen_combinations = set()
        
        for i in range(len(name_cols)):
            name, id_val, link = row[name_cols[i]], row[id_cols[i]], row[link_cols[i]]
            
            if (name, id_val, link) in seen_combinations:
                # If duplicate, make them blank
                row[name_cols[i]] = np.nan
                row[id_cols[i]] = np.nan
                row[link_cols[i]] = np.nan
            else:
                # Otherwise, mark as seen
                seen_combinations.add((name, id_val, link))
        
        return row

    # Apply function to each row
    df = df.apply(process_row, axis=1)
    
    return df

traded_4 = remove_duplicate_name_id_link(traded_3)

traded_5 = condense_columns(traded_4)

def extract_trade_info(df):
    # Identify relevant columns
    name_cols = sorted([col for col in df.columns if col.startswith("Name_")], key=lambda x: int(x.split("_")[1]))
    id_cols = [col.replace("Name", "ID") for col in name_cols]  # Get corresponding ID columns
    
    def process_row(row):
        description = row["Description"].lower() if isinstance(row["Description"], str) else ""
        name_to_id = {}
        
        # Extract name-ID mappings
        for name_col, id_col in zip(name_cols, id_cols):
            name = row[name_col]
            id_val = row[id_col]

            if isinstance(name, str) and isinstance(id_val, str) and name.lower() in description:
                name_to_id[name.lower()] = id_val  # Store ID with name key

        # Preserve order of appearance in the description
        ordered_ids = [name_to_id[name] for name in sorted(name_to_id.keys(), key=lambda n: description.find(n))]

        # Assign trade columns
        three_char_ids = [id_val for id_val in ordered_ids if len(id_val) == 3]
        non_three_char_ids = [id_val for id_val in ordered_ids if len(id_val) != 3]

        row["Traded_from"] = three_char_ids[0] if three_char_ids else None
        row["Traded"] = ", ".join(non_three_char_ids) if non_three_char_ids else None
        row["Traded_to"] = three_char_ids[-1] if three_char_ids else None

        return row

    # Apply function to each row
    df = df.apply(process_row, axis=1)
    
    return df

traded_6 = extract_trade_info(traded_5)

traded_6 = traded_6[['Year', 'Date', 'Action', 'Traded_from', 'Traded', 'Traded_to']].drop_duplicates()

# Get the difference while keeping traded_2
traded_7 = traded_2.merge(traded_3, how='left', indicator=True)
traded_7 = traded_7[traded_7['_merge'] == 'left_only'].drop(columns=['_merge'])
traded_7 = condense_columns(traded_7)

# Define function to process each row
def process_trades(row):
    description = row['Description'].lower()
    
    # Extract relevant names and IDs that are in the description
    id_map = {}
    for i in range(1, 8):  # Iterate through Name_1 to Name_7
        name = row.get(f'Name_{i}', None)
        id_value = row.get(f'ID_{i}', None)
        
        if isinstance(name, str) and name.lower() in description:
            id_map[name.lower()] = id_value

    # Split description based on " for " 
    parts = description.split(" for ")
    
    # Traded_from is simply the team abbreviation
    traded_from = row['Team Abbreviation']
    
    # Find Traded_to (First three-character ID associated with a name in the description)
    traded_to = next((id_val for id_val in id_map.values() if len(id_val) == 3), None)
    
    # If "for" exists in description, process traded/traded_for separately
    traded, traded_for = [], []
    
    if len(parts) > 1:
        before_for, after_for = parts[0], parts[1]
        
        for name, id_val in id_map.items():
            if len(id_val) != 3:  # Only non-three-character IDs
                if name in before_for:
                    traded.append(id_val)
                elif name in after_for:
                    traded_for.append(id_val)
    
    # Convert lists to comma-separated strings
    traded = ', '.join(traded) if traded else None
    traded_for = ', '.join(traded_for) if traded_for else None

    return pd.Series([traded_from, traded, traded_for, traded_to], 
                     index=['Traded_from', 'Traded', 'Traded_for', 'Traded_to'])

# Apply function to dataframe
traded_7[['Traded_from', 'Traded', 'Traded_for', 'Traded_to']] = traded_7.apply(process_trades, axis=1)

traded_8 = traded_7[['Year', 'Date', 'Action','Traded_from', 'Traded', 'Traded_to']].drop_duplicates()

# Ensure Traded is a string before splitting
traded_8['Traded'] = traded_8['Traded'].astype(str).str.split(', ')

# Explode the Traded column into separate rows
traded_9 = traded_8.explode('Traded')

# Reset index after exploding
traded_9 = traded_9.reset_index(drop=True)

# Ensure Traded is a string before splitting
traded_6['Traded'] = traded_6['Traded'].astype(str).str.split(', ')

# Explode the Traded column into separate rows
traded_10 = traded_6.explode('Traded')

# Reset index after exploding
traded_10 = traded_10.reset_index(drop=True)

# Combine two dataframes vertically
combined_df = pd.concat([traded_9, traded_10], ignore_index=True)

# Clean up the Traded column
combined_df['Traded'] = combined_df['Traded'].astype(str).str.replace(r"[\[\]']", "", regex=True)

combined_df = combined_df.drop_duplicates()

combined_df = combined_df[combined_df['Traded'] !='None']

df1 = combined_df[['Year', 'Date', 'Action', 'Traded_from', 'Traded']]
df1.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df1['Start_Stop'] = 'stop'
df2 = combined_df[['Year', 'Date', 'Action', 'Traded_to', 'Traded']]
df2.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df2['Start_Stop'] = 'start'

# Use pd.concat() instead of append()
traded_final = pd.concat([df1, df2], ignore_index=True)

traded_final.to_csv('traded_final.csv')
