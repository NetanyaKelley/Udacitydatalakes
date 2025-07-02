import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    #This  is  going over all of the drop existing tables and making sure none of them are skipped
    
    for query in drop_table_queries:
        print(f"Dropping: {query.split()[4]}")  #added  print statements for debugging purposes
         # the cursor variable is  executing the query"
        cur.execute(query)
        # and this is the connection variable and it is  making use the connection is commit
        conn.commit()

def create_tables(cur, conn):
    #just like above this is iterating all over the tables but this one is making sure they are  creating not dropped
    for query in create_table_queries:
       print(f"creating: {query.split()[5]}")  #added  print statements for debugging purposes
       cur.execute(query)
       conn.commit()


def main():
    # making sure that the  password and the  security portion of this is   working properly
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    print("query completion complete and should be able to visualize")


if __name__ == "__main__":
    main()