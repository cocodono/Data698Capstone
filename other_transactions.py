import pandas as pd

# Read the CSV file using pandas
df = pd.read_csv("merged_transactions.csv")

df.loc[df['Description'].str.lower().str.contains('resign', na=False), 'Action'] = 'Resigned'
df.loc[df['Description'].str.lower().str.contains('retire', na=False), 'Action'] = 'Retired'
df.loc[df['Description'].str.lower().str.contains('traded', na=False), 'Action'] = 'Traded'

other_trans = df[df['Action'] != 'Traded']

other_trans = other_trans[['Year', 'Date', 'Action', 'Description', 'Team Abbreviation', 'Name_1',
       'ID_1', 'Link_1', 'Name_2', 'ID_2', 'Link_2']]

start = other_trans[other_trans['Action'].isin(['Hired', 'Claimed'])]

start_wo_draftsign = start[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
start_wo_draftsign.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
start_wo_draftsign['Start_Stop'] = 'start'  # Now it's a copy, so no warning

start_wo_draftsign['Date'] = pd.to_datetime(start_wo_draftsign['Date'])

stop = other_trans[other_trans['Action'].isin(['Waived', 'Resigned', 'Fired', 'Suspended', 'Retired', 'Released', 'Released', 'Reassigned'])]

stop_wo_draftlost = start[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
stop_wo_draftlost.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
stop_wo_draftlost['Start_Stop'] = 'stop'  # Now it's a copy, so no warning

stop_wo_draftlost['Date'] = pd.to_datetime(stop_wo_draftlost['Date'])

# Drafted

drafted = other_trans[other_trans['Action'] == 'Drafted']

# Drafted_1: No ID_2
drafted_1 = drafted[drafted['ID_2'].isna()].copy()

drafted_1a = drafted_1[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
drafted_1a.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
drafted_1a['Start_Stop'] = 'start'  # No warning since it's a copy

# Drafted_2: Has ID_2
drafted_2 = drafted[drafted['ID_2'].notna()].copy()

df1 = drafted_2[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
df1.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df1['Start_Stop'] = 'start'

df2 = drafted_2[['Year', 'Date', 'Action', 'ID_2', 'ID_1']].copy()
df2.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df2['Start_Stop'] = 'stop'

# Use pd.concat() instead of append()
drafted_final = pd.concat([df1, df2, drafted_1a], ignore_index=True)
drafted_final['Date'] = pd.to_datetime(drafted_final['Date'])

# Lost

lost = other_trans[other_trans['Action'] == 'Lost']

df1 = lost[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
df1.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df1['Start_Stop'] = 'stop'

df2 = lost[['Year', 'Date', 'Action', 'ID_2', 'ID_1']].copy()
df2.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
df2['Start_Stop'] = 'start'

# Use pd.concat() instead of append()
lost_final = pd.concat([df1, df2], ignore_index=True)
lost_final['Date'] = pd.to_datetime(lost_final['Date'])

signed = other_trans[other_trans['Action']=='Signed']

signed_with_day = signed[(signed['Action'] == 'Signed') & (signed['Description'].str.contains('day', case=False, na=False))].copy()

signed_wo_day = signed.merge(signed_with_day, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
signed_wo_day_1 = signed_wo_day[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
signed_wo_day_1.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
signed_wo_day_1['Start_Stop'] = 'start'
signed_wo_day_1['Date'] = pd.to_datetime(signed_wo_day_1['Date'])

signed_with_day_start = signed_with_day[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
signed_with_day_start.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
signed_with_day_start['Start_Stop'] = 'start'
signed_with_day_start['Date'] = pd.to_datetime(signed_with_day_start['Date'])

signed_with_day_stop = signed_with_day.copy()

# Convert Date column to datetime
signed_with_day_stop['Date'] = pd.to_datetime(signed_with_day_stop['Date'])

# Add 7 days
signed_with_day_stop['Date'] = signed_with_day_stop['Date'] + pd.Timedelta(days=7)

signed_with_day_stop['Action'] = 'End of 7-day contract'

signed_with_day_stop = signed_with_day_stop[['Year', 'Date', 'Action', 'Team Abbreviation', 'ID_1']].copy()
signed_with_day_stop.columns = ['Year', 'Date', 'Action', 'Team', 'ID']
signed_with_day_stop['Start_Stop'] = 'stop'

signed_final = pd.concat([signed_wo_day_1, signed_with_day_start, signed_with_day_stop], ignore_index = True)

traded_final = pd.read_csv('traded_final.csv').drop('Unnamed: 0', axis = 1)
traded_final['Date'] = pd.to_datetime(traded_final['Date'])

transactions_final = pd.concat([lost_final, drafted_final, traded_final, stop_wo_draftlost, start_wo_draftsign, signed_final], ignore_index = True).drop_duplicates()

transactions_final.to_csv('transactions_final.csv')
