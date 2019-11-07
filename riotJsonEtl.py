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

#expanded upon above command to include other keys
teams = jsd['teams']
df_bans = pd.DataFrame( {'Team':0, 'pickTurn':0, 'championID':0} )
df_teams = pd.DataFrame( {} )

#print(teams[0].keys())

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

#print(type(teams[0]['firstDragon']))
#print(teams)

for item in teams:
    df_teams = df_teams.append(pd.DataFrame(item))
#nest bans w/ columns (team, picturn, champid)
for item in df_teams['firstDragon']:
    if item == True:
        df_teams['firstDragon'] = 1
#        print (df_teams['firstDragon'])
    elif item == False:
        df_teams['firstDragon'] = 0
#        print(df_teams['firstDragon'])

for item in df_teams['firstInhibitor']:
    if item == True:
        df_teams['firstInhibitor'] = 1
#        print (df_teams['firstInhibitor'])
    elif item == False:
        df_teams['firstInhibitor'] = 0
#        print(df_teams['firstInhibitor'])

for item in df_teams['firstRiftHerald']:
    if item == True:
        df_teams['firstRiftHerald'] = 1
#        print (df_teams['firstRiftHerald'])
    elif item == False:
        df_teams['firstRiftHerald'] = 0
#        print(df_teams['firstRiftHerald'])

for item in df_teams['firstBaron']:
    if item == True:
        df_teams['firstBaron'] = 1
#        print (df_teams['firstBaron'])
    elif item == False:
        df_teams['firstBaron'] = 0
#        print(df_teams['firstBaron'])

for item in df_teams['firstBlood']:
    if item == True:
        df_teams['firstBlood'] = 1
#        print (df_teams['firstBlood'])
    elif item == False:
        df_teams['firstBlood'] = 0
#        print(df_teams['firstBlood'])

for item in df_teams['firstTower']:
    if item == True:
        df_teams['firstTower'] = 1
#        print (df_teams['firstTower'])
    elif item == False:
        df_teams['firstTower'] = 0
#        print(df_teams['firstTower'])


#        teams['firstDragon'] = 1
#print(teams[teamId])

#print('RESULT:'firstDragon)
#print(df_players)

#print('Printing the DataFrame column-by-column:')

#for colname in df_players.columns,item[]:
#    print('column:', colname)
#    print(df_players[colname])
#    print()


#partIds = jsd['participantIdentities']
#create sqlite3 database/tablprint(partIds[0].keyprint(partIds[0].keys())s())e
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
#donn = sqlite3.connect('etlproject.db')
#d = donn.cursor()
#c.execute("CREATE TABLE IF NOT EXISTS teams (firstDragon text, bans text, firstInhibitor text, win text, firstRiftHerald text, firstBaron text, baronKills integer, riftHeraldKills integer, firstBlood text, teamId integer, firstTower text, vilemawKills integer, inhibitorKills integer, towerKills integer, dominionVictoryScore integer, dragonKills integer)")
c.execute("CREATE TABLE IF NOT EXISTS teams (firstDragon integer, bans text, firstInhibitor integer, win text, firstRiftHerald integer, firstBaron integer, baronKills integer, riftHeraldKills integer, firstBlood integer, teamId integer, firstTower integer, vilemawKills integer, inhibitorKills integer, towerKills integer, dominionVictoryScore integer, dragonKills integer)")

conn.commit()

#for item in df_teams:
#    conn.execute("INSERT INTO teams", teams[item])

#df_teams.to_sql('teams',conn,if_exists='replace',index=True)
#print (df_teams.firstDragon)
for item in df_teams:
    print(item)
    print(df_teams[item])
