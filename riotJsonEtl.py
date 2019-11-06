'''\
riotJsonEtl.py

Deserialize and import JSON extracted from Riot Games API.

 +---------------+   Riot Games API   +------+   riotJsonEtl.py     +--------+
 | Riot Games DB | -----------------> | JSON | -------------------> | SQL DB |
 +---------------+      Extract       +------+  Transform & Load    +--------+

The JSON file extracted from is: test.json

THIS VERSION:
    0. Load JSON
    1. Find "participantIdentities" & create a DataFrame on this 

'''

# module to manipulate json
import json

# pandas = panel data analysis ~ DataFrame object
import pandas as pd

# SQL Alchemy to assist with loading a DataFrame to an SQL table
# import sqlalchemy

#instead importing sqlite3 to assist loading DataFrame to SQL table
import sqlite3

JSON_EXTRACT_FILE = 'test.json'

# "standard method" to use the json.load() method from the module: json
with open(JSON_EXTRACT_FILE, 'r') as json_read:
    jsd = json.load(json_read)

# jsd = "json dictionary"
#type(jsd)
# returns "dict"

# what are the keys (a dict has "key : value" pairs):
#print(jsd.keys())
""" returns a list of the keys:
[ 'seasonId', 'queueId', 'gameId', 'participantIdentities'
   , 'gameVersion', 'platformId', 'gameMode', 'mapId', 'gameType'
   , 'teams', 'participants', 'gameDuration', 'gameCreation'
]
"""
''' placed for visual reference
df_players = pd.DataFrame( {} ) - make empty data frame
for item in partIds: - place info row by row
    df_players = df_players.append(pd.DataFrame( item['player'], index=[ item['participantId'] ] ) )
'''

# Notice that one of the keys is: participantIdentities
# Use the key "participantIdentnties" to extract/isolate its dict value/"payload"
partIds = jsd['participantIdentities']
'''
#expanded upon above command to include other keys
seaId = jsd['seasonId']
df_seaId = pd.DataFrame( {} )
#not list, no keys print(seaId[0].keys())
queId = jsd['queueId']
df_queId = pd.DataFrame( {} )
#not list, no keys print(queId[0].keys())
gameId = jsd['gameId']
df_gameId = pd.DataFrame( {} )
#not list, no keys print(gameId[0].keys())
gVers = jsd['gameVersion']
df_gVers = pd.DataFrame( {} )
#not list, no keys print(gVers[0].keys())
platId = jsd['platformId']
df_platId = pd.DataFrame( {} )
#not list, no keys print(platId[0].keys())
gMode = jsd['gameMode']
df_gMode = pd.DataFrame( {} )
#not list, no keys print(gMode[0].keys())
mapId = jsd['mapId']
df_mapId = pd.DataFrame( {} )
print(mapId[0].keys())
gType = jsd['gameType']
df_gType = pd.DataFrame( {} )
print(gType[0].keys())
teams = jsd['teams']
df_teams = pd.DataFrame( {} )
print(teams[0].keys())
partics = jsd['participants']
df_partics = pd.DataFrame( {} )
print(partics[0].keys())
gDura = jsd['gameDuration']
df_gDura = pd.DataFrame( {} )
print(gDura[0].keys())
gCrea = jsd['gameCreation']
df_gCrea = pd.DataFrame( {} )
print(gCrea[0].keys())
'''
#type(partIds)
# list

#print(len(partIds))
# 10

# 
#print(partIds[0].keys())
# 'player' & 'participantId'

#type(partIds[0]['player'])
# dict

#type( partIds[0]['participantId'] )
# int

#partIds[0]

# initialize empty DataFrame to accumulate players row-by-row:
df_players = pd.DataFrame( {} )
for item in partIds:
#    print(item['participantId'])
#    print('\t', item['player'], '\n')
    df_players = df_players.append(pd.DataFrame( item['player'], index=[item['participantId']] ) )

#print('RESULT:')
#print(df_players)

#print('Printing the DataFrame column-by-column:')

#for colname in df_players.columns:
#    print('column:', colname)
#    print(df_players[colname])
#    print()


#partIds = jsd['participantIdentities']
#create sqlite3 database/table
conn = sqlite3.connect('etlproject.db')
c = conn.cursor()

c.execute("CREATE TABLE IF NOT EXISTS participantIdentities (player text, participantId number)")
conn.commit()

df_players.to_sql('participantIdentities', conn, if_exists='replace', index=True, index_label = 'participantId')

#c.execute('''
#SELECT * FROM participantIdentities
#        ''')

#for row in c.fetchall():
#    print (row)
