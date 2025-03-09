import pandas as pd

df = pd.read_csv('wnba_player_gamelogs.csv')
df = df.drop('Unnamed: 0', axis =1)

df = df[['Date', 'Year', 'Tm', 'Opp',
         'MP', 'FG', 'FGA', 
         '3P', '3PA', 
         'FT', 'FTA', 
         'ORB', 'DRB', 'TRB', 
         'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]

df[['MPm', 'MPs']] = df['MP'].str.split(':', n=1, expand=True)
df['MPm'] = df['MPm'].astype(np.float64)
df['MPs'] = df['MPs'].astype(np.float64)

df['MP'] = round(df['MPm'] + (df['MPs'] / 60.0))

df = df.drop(['MPs','MPm'], axis = 1)

# Define the columns to group by
group_by_columns = ['Date', 'Year', 'Tm', 'Opp']

# Define the columns to sum
sum_columns = ['MP', 'FG', 'FGA', '3P', '3PA', 'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']

# Group by the specified columns and sum the other columns
df = df.groupby(group_by_columns)[sum_columns].sum().reset_index()

df['FG%'] = df['FG']/df['FGA']
df['3P%'] = df['3P']/df['3PA']
df['FT%'] = df['FT']/df['FTA']

# Ensure Date is in string format (if not already)
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')

# Sort Tm and Opp alphabetically and create GameID
df['GameID'] = df.apply(lambda row: '_'.join(sorted([row['Tm'], row['Opp']])) + '_' + row['Date'], axis=1)

df = df[['GameID', 'Date', 'Year', 'Tm', 'Opp', 'MP', 'FG', 'FGA', 'FG%', 
         '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB',
         'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]

df_copy = df.copy()

df_copy.rename(columns={'Tm': 'Opp', 'Opp': 'Tm'}, inplace=True)

df_copy = df_copy[['GameID', 'Tm', 'MP', 'FG', 'FGA', 'FG%', '3P',
       '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL',
       'BLK', 'TOV', 'PF', 'PTS']]

# Merge on GameID and Tm
merged_df = df.merge(df_copy, on=['GameID', 'Tm'], suffixes=('_Tm', '_Opp'))

# possession estimate

merged_df['Poss'] = 0.5 * (
    (merged_df['FGA_Tm'] + 0.4 * merged_df['FTA_Tm'] - 
     1.07 * (merged_df['ORB_Tm'] / (merged_df['ORB_Tm'] + merged_df['DRB_Opp'])) * 
     (merged_df['FGA_Tm'] - merged_df['FG_Tm']) + merged_df['TOV_Tm']) +
    (merged_df['FGA_Opp'] + 0.4 * merged_df['FTA_Opp'] - 
     1.07 * (merged_df['ORB_Opp'] / (merged_df['DRB_Tm'] + merged_df['ORB_Opp'])) * 
     (merged_df['FGA_Opp'] - merged_df['FG_Opp']) + merged_df['TOV_Opp'])
)

merged_df.loc[df['MP'].isin([53, 56, 49, 209, 201, 199, 202, 198, 195, 197, 203, 192, 194, 196, 193, 189]), 'MP'] = 200
merged_df.loc[df['MP'].isin([219, 220, 223, 222, 226, 224, 227, 221]), 'MP'] = 225
merged_df.loc[df['MP'].isin([251, 249, 245, 246, 252, 244, 241, 240, ]), 'MP'] = 250
merged_df.loc[df['MP'].isin([269, 272, 273, 271, 274, 276]), 'MP'] = 275

merged_df.to_csv('team_gamelogs.csv')
