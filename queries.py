# Created by Gabriel Martell

'''
Version 1.2 (04/13/2024)
=========================================================
queries.py (Carleton University COMP3005 - Database Management Student Template Code)

This is the template code for the COMP3005 Database Project 1, and must be accomplished on an Ubuntu Linux environment.
Your task is to ONLY write your SQL queries within the prompted space within each Q_# method (where # is the question number).

You may modify code in terms of testing purposes (commenting out a Qn method), however, any alterations to the code, such as modifying the time, 
will be flagged for suspicion of cheating - and thus will be reviewed by the staff and, if need be, the Dean. 

To review the Integrity Violation Attributes of Carleton University, please view https://carleton.ca/registrar/academic-integrity/ 

=========================================================
'''

# Imports
import psycopg
import csv
import subprocess
import os
import re

# Connection Information
''' 
The following is the connection information for this project. These settings are used to connect this file to the autograder.
You must NOT change these settings - by default, db_host, db_port and db_username are as follows when first installing and utilizing psql.
For the user "postgres", you must MANUALLY set the password to 1234.

This can be done with the following snippet:

sudo -u postgres psql
\password postgres

'''
root_database_name = "project_database"
query_database_name = "query_database"
db_username = 'postgres'
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# Directory Path - Do NOT Modify
dir_path = os.path.dirname(os.path.realpath(__file__))

# Loading the Database after Drop - Do NOT Modify
#================================================
def load_database(conn):
    drop_database(conn)

    cursor = conn.cursor()
    # Create the Database if it DNE
    try:
        conn.autocommit = True
        cursor.execute(f"CREATE DATABASE {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)

    finally:
        cursor.close()
        conn.autocommit = False
    conn.close()
    
    # Connect to this query database.
    dbname = query_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    # Import the dbexport.sql database data into this database
    try:
        command = f'psql -h {host} -U {user} -d {query_database_name} -a -f "{os.path.join(dir_path, "dbexport.sql")}" > /dev/null 2>&1'
        env = {'PGPASSWORD': password}
        subprocess.run(command, shell=True, check=True, env=env)

    except Exception as error:
        print(f"An error occurred while loading the database: {error}")
    
    # Return this connection.
    return conn    

# Dropping the Database after Query n Execution - Do NOT Modify
#================================================
def drop_database(conn):
    # Drop database if it exists.

    cursor = conn.cursor()

    try:
        conn.autocommit = True
        cursor.execute(f"DROP DATABASE IF EXISTS {query_database_name};")
        conn.commit()

    except Exception as error:
        print(error)
        pass

    finally:
        cursor.close()
        conn.autocommit = False

# Reconnect to Root Database - Do NOT Modify
#================================================
def reconnect():
    dbname = root_database_name
    user = db_username
    password = db_password
    host = db_host
    port = db_port
    return psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# Getting the execution time of the query through EXPLAIN ANALYZE - Do NOT Modify
#================================================
def get_time(cursor, sql_query):
    # Prefix your query with EXPLAIN ANALYZE
    explain_query = f"EXPLAIN ANALYZE {sql_query}"

    try:
        # Execute the EXPLAIN ANALYZE query
        cursor.execute(explain_query)
        
        # Fetch all rows from the cursor
        explain_output = cursor.fetchall()
        
        # Convert the output tuples to a single string
        explain_text = "\n".join([row[0] for row in explain_output])
        
        # Use regular expression to find the execution time
        # Look for the pattern "Execution Time: <time> ms"
        match = re.search(r"Execution Time: ([\d.]+) ms", explain_text)
        if match:
            execution_time = float(match.group(1))
            return f"Execution Time: {execution_time} ms"
        else:
            print("Execution Time not found in EXPLAIN ANALYZE output.")
            return f"NA"
        
    except Exception as error:
        print(f"[ERROR] Error getting time.\n{error}")


# Write the results into some Q_n CSV. If the is an error with the query, it is a INC result - Do NOT Modify
#================================================
def write_csv(execution_time, cursor, i):
    # Collect all data into this csv, if there is an error from the query execution, the resulting time is INC.
    try:
        colnames = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        filename = f"{dir_path}/Q_{i}.csv"

        with open(filename, 'w', encoding='utf-8', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            
            # Write column names to the CSV file
            csvwriter.writerow(colnames)
            
            # Write data rows to the CSV file
            csvwriter.writerows(rows)

    except Exception as error:
        execution_time[i-1] = "INC"
        print(error)
    
#================================================
        
'''
The following 10 methods, (Q_n(), where 1 < n < 10) will be where you are tasked to input your queries.
To reiterate, any modification outside of the query line will be flagged, and then marked as potential cheating.
Once you run this script, these 10 methods will run and print the times in order from top to bottom, Q1 to Q10 in the terminal window.
'''
def Q_1(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, AVG(s.statsbomb_xg) as average_xg
                FROM Events e
                JOIN Shot s ON e.id = s.event_id
                JOIN Matches m ON s.match_id = m.match_id
                WHERE s.statsbomb_xg > 0
                AND m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                GROUP BY e.player_name
                ORDER BY average_xg DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[0] = (time_val)

    write_csv(execution_time, cursor, 1)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_2(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, count(e.player_id) AS numShots
                FROM Events e
                JOIN Shot s ON s.event_id = e.id
                JOIN Matches m ON s.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"' 
                GROUP BY e.player_name
                HAVING COUNT(e.player_id) > 0
                ORDER BY numShots DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[1] = (time_val)

    write_csv(execution_time, cursor, 2)

    cursor.close()
    new_conn.close()

    return reconnect()
    
def Q_3(conn, execution_time):

    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, count(e.player_id) AS numfirsttimeshots
                FROM Events e
                JOIN Shot s ON s.event_id = e.id
                JOIN Matches m ON s.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                OR m.season_name = '"2019/2020"'
                OR m.season_name = '"2018/2019"'
                AND s.first_time = 'True'
                GROUP BY e.player_name
                HAVING COUNT(e.player_id) > 0
                ORDER BY numfirsttimeshots DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[2] = (time_val)

    write_csv(execution_time, cursor, 3)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_4(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.team_name, count(e.team_name) AS numpasses
                FROM Events e
                JOIN Pass p ON p.event_id = e.id
                JOIN Matches m ON e.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"' 
                GROUP BY e.team_name
                HAVING COUNT(e.team_name) > 0
                ORDER BY numpasses DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[3] = (time_val)

    write_csv(execution_time, cursor, 4)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_5(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT p.pass_recipient_name, count(p.pass_recipient_name) AS passedtonum
                FROM Pass p
                JOIN Events e ON e.id = p.event_id
                JOIN Matches m ON m.match_id = e.match_id
                WHERE m.competition_name = '"Premier League"'
                AND m.season_name = '"2003/2004"' 
                GROUP BY p.pass_recipient_name
                HAVING COUNT(p.pass_recipient_name) > 0
                ORDER BY passedtonum DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[4] = (time_val)

    write_csv(execution_time, cursor, 5)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_6(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.team_name, count(e.team_name) AS teamnumshots
                FROM Events e
                JOIN Shot s ON s.event_id = e.id
                JOIN Matches m ON m.match_id = e.match_id
                WHERE m.competition_name = '"Premier League"'
                AND m.season_name = '"2003/2004"' 
                GROUP BY e.team_name
                HAVING COUNT(e.team_name) > 0
                ORDER BY teamnumshots DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[5] = (time_val)

    write_csv(execution_time, cursor, 6)

    cursor.close()
    new_conn.close()

    return reconnect()


def Q_7(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, count(e.player_name) as numthroughballs
                FROM Events e
                JOIN Pass p ON p.event_id = e.id 
                JOIN Matches m ON e.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                AND p.pass_technique_name = 'Through Ball'
                GROUP BY e.player_name
                HAVING count(e.player_name) > 0
                ORDER BY numthroughballs DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[6] = (time_val)

    write_csv(execution_time, cursor, 7)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_8(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.team_name AS teamname, count(e.team_name) as numteamthroughballs
                FROM Events e
                JOIN Pass p ON p.event_id = e.id
                JOIN Matches m ON e.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                AND p.pass_technique_name = 'Through Ball'
                GROUP BY e.team_name
                HAVING count(e.team_name) > 0
                ORDER BY numteamthroughballs DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[7] = (time_val)

    write_csv(execution_time, cursor, 8)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_9(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, count(e.player_name) AS numcompletedribbles
                FROM Events e
                JOIN Dribble d ON d.event_id = e.id
                JOIN Matches m ON e.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                OR m.season_name = '"2019/2020"'
                OR m.season_name = '"2018/2019"'
                AND d.outcome_name = 'Completed'
                GROUP BY e.player_name
                HAVING COUNT(e.player_name) > 0
                ORDER BY numcompletedribbles DESC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[8] = (time_val)

    write_csv(execution_time, cursor, 9)

    cursor.close()
    new_conn.close()

    return reconnect()

def Q_10(conn, execution_time):
    new_conn = load_database(conn)
    cursor = new_conn.cursor()

    #==========================================================================
    # Enter QUERY within the quotes:

    query = """SELECT e.player_name AS playername, count(e.player_name) AS numdribbledpast
                FROM Events e
                JOIN Dribbled_Past dp ON dp.event_id = e.id
                JOIN Matches m ON e.match_id = m.match_id
                WHERE m.competition_name = '"La Liga"'
                AND m.season_name = '"2020/2021"'
                AND e.type_name = 'Dribbled Past'
                GROUP BY e.player_name
                HAVING COUNT(e.player_name) > 0
                ORDER BY numdribbledpast ASC;"""

    #==========================================================================

    time_val = get_time(cursor, query)
    cursor.execute(query)
    execution_time[9] = (time_val)

    write_csv(execution_time, cursor, 10)

    cursor.close()
    new_conn.close()

    return reconnect()

# Running the queries from the Q_n methods - Do NOT Modify
#=====================================================
def run_queries(conn):

    execution_time = [0,0,0,0,0,0,0,0,0,0]

    conn = Q_1(conn, execution_time)
    conn = Q_2(conn, execution_time)
    conn = Q_3(conn, execution_time)
    conn = Q_4(conn, execution_time)
    conn = Q_5(conn, execution_time)
    conn = Q_6(conn, execution_time)
    conn = Q_7(conn, execution_time)
    conn = Q_8(conn, execution_time)
    conn = Q_9(conn, execution_time)
    conn = Q_10(conn, execution_time)

    for i in range(10):
        print(execution_time[i])

''' MAIN '''
try:
    if __name__ == "__main__":

        dbname = root_database_name
        user = db_username
        password = db_password
        host = db_host
        port = db_port

        conn = psycopg.connect(dbname=dbname, user=user, password=password, host=host, port=port)
        
        run_queries(conn)
except Exception as error:
    print(error)
    #print("[ERROR]: Failure to connect to database.")
#_______________________________________________________
