import pandas as pd
import numpy as np

team = pd.read_csv('team_gamelogs.csv')
team = team.drop('Unnamed: 0', axis = 1)
player = pd.read_csv('wnba_player_gamelogs.csv')
player = player.drop('Unnamed: 0', axis = 1)
player = player.drop_duplicates()

# Ensure Date is in string format (if not already)
player['Date'] = pd.to_datetime(player['Date']).dt.strftime('%Y-%m-%d')

# Sort Tm and Opp alphabetically and create GameID
player['GameID'] = player.apply(lambda row: '_'.join(sorted([row['Tm'], row['Opp']])) + '_' + row['Date'], axis=1)

df = player.merge(team, on=['GameID', 'Tm'], how='left')

df = df[['Rk', 'Date_x', 'Age', 'Tm', 'Opp_x', 'GS', 'MP_x', 'FG', 'FGA', 'FG%',
       '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST',
       'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 'Player', 'Player ID',
       'Year_x', 'Home_Away', 'W_L', 'game_differential', 'GameID',
       'MP_Tm', 'FG_Tm', 'FGA_Tm', 'FG%_Tm', '3P_Tm',
       '3PA_Tm', '3P%_Tm', 'FT_Tm', 'FTA_Tm', 'FT%_Tm', 'ORB_Tm', 'DRB_Tm',
       'TRB_Tm', 'AST_Tm', 'STL_Tm', 'BLK_Tm', 'TOV_Tm', 'PF_Tm', 'PTS_Tm',
       'MP_Opp', 'FG_Opp', 'FGA_Opp', 'FG%_Opp', '3P_Opp', '3PA_Opp',
       '3P%_Opp', 'FT_Opp', 'FTA_Opp', 'FT%_Opp', 'ORB_Opp', 'DRB_Opp',
       'TRB_Opp', 'AST_Opp', 'STL_Opp', 'BLK_Opp', 'TOV_Opp', 'PF_Opp',
       'PTS_Opp', 'Poss', 'MP_y']]

df.columns = ['Rk', 'Date', 'Age', 'Tm', 'Opp', 'GS', 'Player_MP', 'FG', 'FGA', 'FG%',
       '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST',
       'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 'Player', 'PlayerID',
       'Year', 'Home_Away', 'W_L', 'game_differential', 'GameID', 'MP_Tm',
       'FG_Tm', 'FGA_Tm', 'FG%_Tm', '3P_Tm', '3PA_Tm', '3P%_Tm', 'FT_Tm',
       'FTA_Tm', 'FT%_Tm', 'ORB_Tm', 'DRB_Tm', 'TRB_Tm', 'AST_Tm', 'STL_Tm',
       'BLK_Tm', 'TOV_Tm', 'PF_Tm', 'PTS_Tm', 'MP_Opp', 'FG_Opp', 'FGA_Opp',
       'FG%_Opp', '3P_Opp', '3PA_Opp', '3P%_Opp', 'FT_Opp', 'FTA_Opp',
       'FT%_Opp', 'ORB_Opp', 'DRB_Opp', 'TRB_Opp', 'AST_Opp', 'STL_Opp',
       'BLK_Opp', 'TOV_Opp', 'PF_Opp', 'PTS_Opp', 'Poss', 'Game_MP']

df[['MPm', 'MPs']] = df['Player_MP'].str.split(':', n=1, expand=True)
df['MPm'] = df['MPm'].astype(np.float64)
df['MPs'] = df['MPs'].astype(np.float64)
df['Player_MP'] = round(df['MPm'] + (df['MPs'] / 60.0))

df.drop(columns=['MPs', 'MPm'], inplace=True)

df[df['Game_MP'].isin([53,56,49]) & df['PlayerID']!='dalesst01w']['Player_MP'] = df[df['Game_MP']==53]['Player_MP'] * 4

df['Date'] = pd.to_datetime(df['Date'])

# A_TO

df['A_TO'] = df['AST']/df['TOV']

# AST%

df['AST%'] = df['AST'] / (((df['Player_MP'] / (df['Game_MP'] / 5)) * df['FG_Tm']) - df['FG'])

# BLK%

df['BLK%'] = (df['BLK'] * (df['Game_MP'] / 5)) / (df['Player_MP'] * (df['FGA_Opp'] - df['3PA_Opp']))

# DRB%

df['BLK%'] = (df['DRB'] * (df['Game_MP'] / 5)) / (df['Player_MP'] * (df['DRB_Tm'] + df['ORB_Opp']))

# eFG%

df['eFG%'] = (df['FG'] + (0.5 * df['3P'])) / df['FGA']

# ORB%

df['BLK%'] = (df['ORB'] * (df['Game_MP'] / 5)) / (df['Player_MP'] * (df['ORB_Tm'] + df['DRB_Opp']))

# STL%

df['STL%'] = (df['STL'] * (df['Game_MP'] / 5)) / (df['Player_MP'] * df['Poss'])

# TOV%

df['TOV%'] = df['TOV'] / (df['FGA'] + 0.44 * df['FTA'] + df['TOV'])

# TRB%

df['TRB%'] = (df['TRB'] * (df['Game_MP'] / 5)) / (df['Player_MP'] * (df['TRB_Tm'] + df['TRB_Opp']))

# TS%

TSA = df['FGA'] + 0.44 * df['FTA']
df['TS%'] = df['PTS'] / (2 * TSA)

# Usg%

df['Usg%'] = ((df['FGA'] + 0.44 * df['FTA'] + df['TOV']) * (df['Game_MP'] / 5)) / (df['Player_MP'] * (df['FGA_Tm'] + 0.44 * df['FTA_Tm'] + df['TOV_Tm']))

df = df[['Rk', 'GameID', 'Date', 'Age', 'Player', 'PlayerID', 'Year', 'Tm',
       'Opp', 'GS', 'Home_Away', 'Player_MP', 'FG', 'FGA', 'FG%', '3P', '3PA',
       '3P%', 'FT', 'FTA', 'FT%', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK',
       'TOV', 'PF', 'PTS', 'GmSc', 'A_TO', 'AST%', 'BLK%', 'eFG%', 'STL%',
       'TOV%', 'TRB%', 'TS%', 'Usg%', 'W_L', 'game_differential', 'FG_Tm',
       'FGA_Tm', 'FG%_Tm', '3P_Tm', '3PA_Tm', '3P%_Tm', 'FT_Tm', 'FTA_Tm',
       'FT%_Tm', 'ORB_Tm', 'DRB_Tm', 'TRB_Tm', 'AST_Tm', 'STL_Tm', 'BLK_Tm',
       'TOV_Tm', 'PF_Tm', 'PTS_Tm', 'FG_Opp', 'FGA_Opp', 'FG%_Opp', '3P_Opp',
       '3PA_Opp', '3P%_Opp', 'FT_Opp', 'FTA_Opp', 'FT%_Opp', 'ORB_Opp',
       'DRB_Opp', 'TRB_Opp', 'AST_Opp', 'STL_Opp', 'BLK_Opp', 'TOV_Opp',
       'PF_Opp', 'PTS_Opp', 'Poss', 'Game_MP']]

# Ensure Date is in datetime format
df['Date'] = pd.to_datetime(df['Date'])

# Sort by PlayerID, Year, and Date
df = df.sort_values(by=['PlayerID', 'Year', 'Date'])

# Define the columns for rolling averages
rolling_cols = [
    'Player_MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%', 'FT', 'FTA', 'FT%', 
    'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'GmSc', 
    'A_TO', 'AST%', 'BLK%', 'eFG%', 'STL%', 'TOV%', 'TRB%', 'TS%', 'Usg%', 
    'game_differential', 'FG_Tm', 'FGA_Tm', 'FG%_Tm', '3P_Tm', '3PA_Tm', 
    '3P%_Tm', 'FT_Tm', 'FTA_Tm', 'FT%_Tm', 'ORB_Tm', 'DRB_Tm', 'TRB_Tm', 
    'AST_Tm', 'STL_Tm', 'BLK_Tm', 'TOV_Tm', 'PF_Tm', 'PTS_Tm', 'FG_Opp', 
    'FGA_Opp', 'FG%_Opp', '3P_Opp', '3PA_Opp', '3P%_Opp', 'FT_Opp', 'FTA_Opp', 
    'FT%_Opp', 'ORB_Opp', 'DRB_Opp', 'TRB_Opp', 'AST_Opp', 'STL_Opp', 
    'BLK_Opp', 'TOV_Opp', 'PF_Opp', 'PTS_Opp', 'Poss'
]

# Compute rolling averages for the last 6 games within each PlayerID-Year group
df[rolling_cols] = df.groupby(['PlayerID', 'Year'])[rolling_cols].transform(lambda x: x.rolling(6, min_periods=1).mean())

df.head()
