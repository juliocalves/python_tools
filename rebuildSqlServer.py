import pyodbc
from configparser import ConfigParser

def get_database_config():
    try:
        config = ConfigParser()
        config.read('database.ini')
        return (
            config['DATABASE']['IPADDRESS'],
            config['DATABASE']['USER'],
            config['DATABASE']['PASSWORD'],
            config['DATABASE']['PORT'],
            config['DATABASE']['NAME_DATABASE']
        )
    except:
        pass

def get_db_connection():
    data = get_database_config()
    return pyodbc.connect(
        DRIVER='{SQL Server}',
        SERVER=data[0],
        DATABASE=data[4],
        UID=data[1],
        PWD=data[2]
    )

def execute_select(SQL):
    db = get_db_connection()
    cur = db.cursor()
    cur.execute(SQL)
    columns = [column[0] for column in cur.description]
    query_result = cur.fetchall()
    data = []
    for row in query_result:
        data.append(dict(zip(columns, row)))
    db.close()
    return data if data else False

def rebuild_indexes():
    results = execute_select("""
        select i.name, OBJECT_NAME(i.OBJECT_id) AS TableName
        From sys.indexes i
        where i.name is not null AND is_primary_key = 0
        AND I.object_id IN (SELECT object_id FROM sys.tables)
    """)
    if results:
        db = get_db_connection()
        cur = db.cursor()
        for result in results:
            cur.execute(f"ALTER INDEX {result['name']} ON {result['TableName']} REBUILD")
            db.commit()
        db.close()

rebuild_indexes()
