import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    #  this specific function  goes over the list of load queries that load  are loaded from the s3 bucket using  what we like to call the copy command
    for query in copy_table_queries:
        print(f"Loading: {query.split()[1]}") 
        cur.execute(query)# as stated early in the  create tables  these are the cursor and connection variables that are used to commit and execute the  information that is given to us
        conn.commit()
        

def insert_tables(cur, conn):
    print("insert_tables function has started") 
    #  this is the inserting table query which is used to  insert the  stage tables and to create a final table 
    for query in insert_table_queries:
        print(f"Inserting: {query.split()[3]}")
        cur.execute(query)# as stated early in the  create tables  these are the cursor and connection variables that are used to commit and execute the  information that is given to us
        conn.commit()


def main(): # this starts the  pogram to give us a smooth transition, which i like to call the main focal point of the   program after creating
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging_events;")
    print("Rows in staging_events:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM staging_songs;")
    print("Rows in staging_songs:", cur.fetchone()[0])
    
    load_staging_tables(cur, conn)
    
    print("All Queries have been loaded")

    print("Now starting  to insert tables")
    print("3")
    print("2")
    print("1")
    insert_tables(cur, conn)

    conn.close()
    print("it worked")

if __name__ == "__main__":
    main()