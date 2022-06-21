# 1. PURPOSE

Sparkify data collection and management relies on 2 JSON files: 
- logs on user activity on the app
- metadata on the songs in the app

Such approach prevents from easy data quering.
Since Sparkify analytics team is interesed in understanding traffic on their app
and particulary preferences of the users (popularity of each song), it has been decided 
to transfer the data to Posgres database.

# 2. MANUAL

In order to perform inteded actions, as describe in Purpose section, 2 python files 
need to be executed by the user and STRICTLY in the following order: create_tables.py and etl.py. 
To do so:
1. Open command line terminal
2. Navigate the terminal to the directory where the script is located using the cd command.
3. Type "python create_tables.py" in the terminal to execute the script.
4. Confirm create_tables.py did not return any errors.
5. Type "python etl.py" in the terminal to execute the script and make sure no errors were returned.

# 3. STRUCTURE

The repository consists of the following:

## 3.1 Data folder

This is where JSON files are being stored for logs on users activity (log_data subfolder) and 
medatadata on songs (songs_data subfolder).

## 3.2 sql_queries.py

This python script stores all PostgreSQL queries that are being called by create_tables.py and etl.py.
The scope covers creation of tables, inserting the data from JSON files and joining/filtering tables.

## 3.3 create_tables.py

The script is ultimately responsible for creation of tables defined in sql_queries.
To do so however, it first:
- drops the existing database (if it exists)
- creates new sparkify database
- establishes connection with sparkify database and gets cursor to it
- drops the tables


Finally required and defined tables are created and connection is closed.

## 3.4 etl.py

ETL scripts extracts data from JSON files, transforms it and loads into previously created PosgreSQL tables.

## 3.5 etl.ipynb

ETL script in the format of Jupyter Notebook, allowing for easy understanding of the ETL pipeline process.

## 3.6 test.ipynb

Jupyter Notebook with predefined tests.
There are 2 sections in it:
1. Tables view - Displays first few rows of each table, allowing for quick data checkup. 
2. Sanity tests - Basic tests for data format & consistency evaluation. 

## 3.7 README.md

Well... this file :). 


# 4. SCHEMA & PIPELINE

Sparkify PosgreSQL database Schema is of a Star type, having in mind Analytics team as the main user.
This architecture shall allow for fast aggregations and simple queries (also those not yet foreseen).
The schema consists of:
- centre Fact table (songplays)
- 4 dimension tables (songs, artists, users, time)

## 4.1 SONGPLAYS 

 | `songplay_id`       | `start_time'        | 'user_id'     | 'level' | 'song_id' | 'artist_id' | 'session_id' | 'location' | 'user_agent' |
 |---------------------| --------------------| --------------| --------|-----------|-------------|--------------|------------|--------------|  
 | SERIAL PRIMARY KEY  | timestamp NOT NULL  | int NOT NULL  | varchar | varchar   | varchar     | int          | varchar    | varchar      |
 
 
## 4.2 USERS
 
  | 'user_id'                | 'first_name' | 'last_name' | 'gender' | 'level' |
  |--------------------------|--------------|-------------|----------|---------|
  | int NOT NULL PRIMARY KEY | varchar      | varchar     | varchar  | varchar |
  
## 4.3 SONGS
 
  | 'song_id'                    | 'title'           | 'artist_id' | 'year  ' | 'duration'       |
  |------------------------------|-------------------|-------------|----------|------------------|
  | varchar NOT NULL PRIMARY KEY | varchar NOT NULL  | varchar     | int      | numeric NOT NULL |
  
## 4.4 ARTISTS
 
  | 'artist_id'                  | 'name'           | 'location' | 'latitude' | 'longitude' |
  |------------------------------|------------------|------------|------------|-------------|
  | varchar NOT NULL PRIMARY KEY | varchar NOT NULL | varchar    | float      | float       |
  
## 4.5 TIME

  | 'start_time'                   | 'hour'  | 'day' | 'week' | 'month' | 'year' | 'weekday' |
  |--------------------------------|---------|-------|--------|---------|--------|-----------|
  | timestamp NOT NULL PRIMARY KEY | int     | int   | int    | int     | int    | int       |
  
  
 
# 5. EXAMPLE QUERIES

# 5.1 Getting song title and count of its plays

  try:
    cur.execute("""SELECT title, COUNT(*) FROM songplays JOIN songs ON songplays.song_id = songs.song_id GROUP BY title""")
except psycopg2.Error as e:
    print("Error: selecting count of played songs")
    print(e)
    
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()   