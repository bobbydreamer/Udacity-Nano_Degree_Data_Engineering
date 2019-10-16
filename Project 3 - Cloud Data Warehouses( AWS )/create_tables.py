import configparser
import psycopg2
from importlib import reload
import sql_queries
reload(sql_queries)
from sql_queries import schema_queries, create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all the table present in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    try:
        print(schema_queries[2]['message'])
        cur.execute(schema_queries[2]['query'])
    except:
        print('{:5}Since schema itself is not there. Guessing table wont be there as well.'.format(''))
        conn.commit()
        return
        
        
    for o in drop_table_queries:
        print(o['message'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()

            
def create_tables(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    print(schema_queries[2]['message'])
    cur.execute(schema_queries[2]['query'])

    for o in create_table_queries:
        print(o['message'])
        try:
            cur.execute(o['query'])
            conn.commit()
        except psycopg2.Error as e:
            print(e)
            conn.close()

            
def create_schema(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    print(schema_queries[0]['message'])    

    try:
        cur.execute(schema_queries[0]['query'])
        conn.commit()
    except psycopg2.Error as e:
            print(e)
            conn.close()

            
def drop_schema(cur, conn):
    """
    Create table in the Redshift cluster
    :param cur: cursor connexion object on Redshift
    :param conn: connection object on Redshift
    """
    print(schema_queries[1]['message'])    
    try:        
        cur.execute(schema_queries[1]['query'])
        conn.commit()
    except psycopg2.Error as e:
            print(e)
            conn.close()

            
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Reading config & initializing variables
    details_dict = {}
    new_dict = {}
    for section in config.sections():
        dic = dict(config.items(section))
        if len(dic) != 0 :
            #print( '{} - {} : {}'.format(len(dic), section, dic) )
            details_dict.update(dic)

    for k, v in details_dict.items():
        k = k.upper()
        #print('{} : {}'.format(k,v))
        new_dict[k] = v

    #print(new_dict)
    
    for k, v in new_dict.items():
        globals()[k] = v

    #print(' LOG_LOCAL_DATA = {}'.format(LOG_LOCAL_DATA))

    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format( HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT ))
    cur = conn.cursor()

    drop_tables(cur, conn)
    drop_schema(cur, conn)
    
    create_schema(cur, conn)    
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()