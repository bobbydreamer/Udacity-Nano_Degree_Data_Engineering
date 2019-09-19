import psycopg2
from sql_queries import create_table_queries, drop_table_queries

import sql_queries
from importlib import reload
reload(sql_queries)

msgcount = 0

# Below function is not much useful, initially thought statusmessage & rowcount will be useful. Most of the time rowcount = -1 and statusmessage just tells statement executed.
# One +ve tells whats all been executed
def display_status(cur, message):    
    """ 
    Summary line. 
    Below function is not much useful, initially thought statusmessage & rowcount will be useful. 
    Most of the time rowcount = -1 and statusmessage just tells statement executed.
    
    One +ve tells whats all been executed
  
    Parameters: 
    arg1 (cursor): Cursor thats been executed
    arg2 (string): String message
  
    Returns: 
    Does not return any value
    """
    global msgcount
    msgcount+=1
    if hasattr(cur, 'statusmessage'):        
        print("{}. {} : statusmessage({}), rowcount({})".format(msgcount, message, cur.statusmessage, cur.rowcount))
    else:
        print("{}. {}".format(msgcount, message))


def create_database():
    """ 
    Summary line.   
    Drop & Create database sparkifydb
  
    Parameters: 
    None
  
    Returns: 
    cur : Cursor
    conn: Connection
    """    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    display_status(cur, 'Connected to Postgres')
    
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    display_status(cur, 'DROP sparkifydb')
    
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
    display_status(cur, 'CREATE sparkifydb')

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    display_status(cur, 'Connected to sparkifydb')
    return cur, conn


def drop_tables(cur, conn):
    """ 
    Summary line.   
    Drop tables in loop
  
    Parameters: 
    arg1 : cur : Cursor
    arg2 : conn: Connection
  
    Return: 
    None
    """        
    
    for query in drop_table_queries:
        cur.execute(query)
        display_status(cur, 'Executing drop_tables')
        conn.commit()


def create_tables(cur, conn):
    """ 
    Summary line.   
    Create tables in loop
  
    Parameters: 
    arg1 : cur : Cursor
    arg2 : conn: Connection
  
    Return: 
    None
    """        
        
    for query in create_table_queries:
        cur.execute(query)
        display_status(cur, 'Executing create_tables')
        conn.commit()


def main():    
    """ 
    Summary line.   
    Main function
  
    Parameters:
    None
  
    Return: 
    None
    """            
    cur, conn = create_database()
    display_status(cur, 'In main()')
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    display_status(conn, 'In main() - Connection closed')

    print('Testing docstrings')
    #Method 1
    print (create_database.__doc__)    
    #Method 2
    help(display_status)
    

if __name__ == "__main__":
    main()