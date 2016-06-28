#!/usr/bin/env python
# -*- coding: utf8 -*-

import sqlite3
import time
import xml.etree.ElementTree as etree
import os

print (time.strftime("%H:%M:%S"))
start_time = time.time()

conn = sqlite3.connect('sample.sqlite')		
cur = conn.cursor()	
	
cur.execute('DROP TABLE IF EXISTS Node ')
cur.execute('DROP TABLE IF EXISTS Key ')
cur.execute('DROP TABLE IF EXISTS Val ')
cur.execute('DROP TABLE IF EXISTS Node_tags ')
cur.execute('DROP TABLE IF EXISTS Way ')
cur.execute('DROP TABLE IF EXISTS Way_tags ')
cur.execute('DROP TABLE IF EXISTS Relation ')
cur.execute('DROP TABLE IF EXISTS Relation_tags ')
cur.execute('DROP TABLE IF EXISTS Ref ')
	
cur.executescript('''
	CREATE TABLE Node
		(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		nodeID INTEGER UNIQUE,
		lat INTEGER,
		lon INTEGER);	
		
	CREATE TABLE Key
		(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		k TEXT UNIQUE);
		
	CREATE TABLE Val
		(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		v TEXT UNIQUE);
			
	CREATE TABLE Node_tags
		(val_id INTEGER,
		key_id INTEGER,
		node_id INTEGER,
		PRIMARY KEY (val_id,key_id,node_id));
		
	CREATE TABLE Way
		(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		wayID INTEGER UNIQUE);
		
	CREATE TABLE Way_tags
		(way_id INTEGER,
		val_id INTEGER,
		key_id INTEGER,
		PRIMARY KEY (val_id,key_id,way_id));	

	CREATE TABLE Relation
		(id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		relationID INTEGER UNIQUE);
		
	CREATE TABLE Relation_tags
		(relation_id INTEGER,
		val_id INTEGER,
		key_id INTEGER,
		PRIMARY KEY (val_id,key_id,relation_id));
		
	CREATE TABLE Ref
		(refID INTEGER ,
		way_id PRIMARY KEY )		
		''')
count=0
for name in os.listdir("D:\Torrent\FDM\DE"): 
        if name.endswith('.osm'):
		context=etree.iterparse(file)
		for event, elem in context:
			if elem.tag=='node':
				nodeID=elem.get('id')
				lat=elem.get('lat')
				lon=elem.get('lon')		
				cur.execute('INSERT OR IGNORE INTO Node (nodeID, lat, lon) VALUES ( ?, ?, ? )',( nodeID, lat, lon ) )
				cur.execute('SELECT id FROM Node WHERE nodeID = ? ', (nodeID, ))
				node_id = cur.fetchone()[0]							
				for child in elem:
					key=child.get('k')	
					val=child.get('v')			
					if key is None or val is None:	None
					else: 
						cur.execute('INSERT OR IGNORE INTO Key ( k ) VALUES ( ? )',( key, ) )
						cur.execute('SELECT id FROM Key WHERE k = ? ', (key, ))
						key_id = cur.fetchone()[0]		
							
						cur.execute('INSERT OR IGNORE INTO Val ( v ) VALUES ( ? )',( val, ) )
						cur.execute('SELECT id FROM Val WHERE v = ? ', (val, ))
						val_id = cur.fetchone()[0]
							
						cur.execute('INSERT OR IGNORE INTO Node_tags ( val_id,key_id,node_id ) VALUES ( ?,?,? )',( val_id,key_id,node_id, ) )	
					
			if elem.tag=='way':
				wayID=elem.get('id')
				cur.execute('INSERT OR IGNORE INTO Way (wayID) VALUES ( ? )',( wayID, ) )
				cur.execute('SELECT id FROM Way WHERE wayID = ? ', (wayID, ))
				way_id = cur.fetchone()[0]			
				for child in elem:
					ref=child.get('ref')
					key=child.get('k')	
					val=child.get('v')
					if ref is None :	None
					else:
						cur.execute('INSERT OR IGNORE INTO Ref ( refID,way_id ) VALUES ( ?,? )',( ref,way_id, ) )
					if key is None or val is None:	None
					else: 
						cur.execute('INSERT OR IGNORE INTO Key ( k ) VALUES ( ? )',( key, ) )
						cur.execute('SELECT id FROM Key WHERE k = ? ', (key, ))
						key_id = cur.fetchone()[0]		
							
						cur.execute('INSERT OR IGNORE INTO Val ( v ) VALUES ( ? )',( val, ) )
						cur.execute('SELECT id FROM Val WHERE v = ? ', (val, ))
						val_id = cur.fetchone()[0]
						
						cur.execute('INSERT OR IGNORE INTO Way_tags ( val_id,key_id,way_id ) VALUES ( ?,?,? )',( val_id,key_id,way_id, ) )
			
			if elem.tag=='relation':
				relationID=elem.get('id')
				cur.execute('INSERT OR IGNORE INTO Relation (relationID) VALUES ( ? )',( relationID, ) )
				cur.execute('SELECT id FROM Relation WHERE relationID = ? ', (relationID, ))
				relation_id = cur.fetchone()[0]	
				for child in elem:			
					key=child.get('k')	
					val=child.get('v')	
					if key is None or val is None:	None
					else: 
						cur.execute('INSERT OR IGNORE INTO Key ( k ) VALUES ( ? )',( key, ) )
						cur.execute('SELECT id FROM Key WHERE k = ? ', (key, ))
						key_id = cur.fetchone()[0]		
						
						cur.execute('INSERT OR IGNORE INTO Val ( v ) VALUES ( ? )',( val, ) )
						cur.execute('SELECT id FROM Val WHERE v = ? ', (val, ))
						val_id = cur.fetchone()[0]
							
						cur.execute('INSERT OR IGNORE INTO Relation_tags ( val_id,key_id,relation_id ) VALUES ( ?,?,? )',( val_id,key_id,relation_id, ) )
					
			count+=1
				
			if count %300000==0:
				print count,("--- %s seconds ---" % (time.time() - start_time))					
				conn.commit()
conn.close()

print 'ALL DONE!',("--- %s seconds ---" % (time.time() - start_time))
