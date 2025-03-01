import pandas as pd

df = pd.read_csv('wnba_player_gamelogs.csv')
df = df.drop('Unnamed: 0', axis =1)

# Create the 'Home' column based on the value of 'Home_Away'
df['Home'] = df.apply(lambda row: row['Tm'] if row['Home_Away'] == 'Home' else row['Opp'], axis=1)

# Create the 'Away' column based on the opposite of 'Home'
df['Away'] = df.apply(lambda row: row['Opp'] if row['Home_Away'] == 'Home' else row['Tm'], axis=1)

df = df[['Date', 'Year', 'Tm', 'Home', 'Away',
         'FG', 'FGA', 
         '3P', '3PA', 
         'FT', 'FTA', 
         'ORB', 'DRB', 'TRB', 
         'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]

# Define the columns to group by
group_by_columns = ['Date', 'Year', 'Tm', 'Home', 'Away']

# Define the columns to sum
sum_columns = ['FG', 'FGA', '3P', '3PA', 'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']

# Group by the specified columns and sum the other columns
df = df.groupby(group_by_columns)[sum_columns].sum().reset_index()

df['FG%'] = df['FG']/df['FGA']
df['3P%'] = df['3P']/df['3PA']
df['FT%'] = df['FT']/df['FTA']
df['GameID'] = df['Home']+df['Away']+df['Date']

df = df[['GameID', 'Date', 'Year', 'Tm', 'Home', 'Away', 'FG', 'FGA', 'FG%', 
         '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB',
         'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']]

# Define the columns for Home and Away statistics
stat_columns = ['FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 
                'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']

# Create Home team dataframe by selecting rows where 'Tm' is equal to 'Home' and renaming columns
home_df = df[df['Tm'] == df['Home']].copy()  # Check if 'Tm' equals 'Home' team
home_df = home_df[['GameID', 'Date', 'Year', 'Home', 'Away'] + stat_columns]
home_df.columns = ['GameID', 'Date', 'Year', 'Home', 'Away'] + ['Home_' + col for col in stat_columns]

# Create Away team dataframe by selecting rows where 'Tm' is equal to 'Away' and renaming columns
away_df = df[df['Tm'] == df['Away']].copy()  # Check if 'Tm' equals 'Away' team
away_df = away_df[['GameID', 'Date', 'Year', 'Home', 'Away'] + stat_columns]
away_df.columns = ['GameID', 'Date', 'Year', 'Home', 'Away'] + ['Away_' + col for col in stat_columns]

# Merge the Home and Away dataframes on 'GameID', 'Date', 'Year', 'Tm', 'Home', 'Away'
result_df = pd.merge(home_df, away_df, on=['GameID', 'Date', 'Year', 'Home', 'Away'], how='inner')

result_df['W_L'] = ['W' if home > away else 'L' for home, away in zip(result_df['Home_PTS'], result_df['Away_PTS'])]
result_df['PT_Diff'] = result_df['Home_PTS'] - result_df['Away_PTS']
