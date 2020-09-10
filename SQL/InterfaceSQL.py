import psycopg2
import pandas as pd

def createExecData(dbname):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE execdata_train AS SELECT * FROM train;')
            cur.execute('CREATE TABLE execdata_test AS SELECT * FROM test;')
    
    print("DONE.")


def readColumns(dbname, tablename):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + tablename + "' ORDER BY ordinal_position;")

            cols = cur.fetchall()
            df_columns = pd.DataFrame(cols, columns=['cols'])
    
    return df_columns

def selectExecData(dbname, tableName, clmns = None):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            if clmns is None:
            #全テーブルデータ取得
                cur.execute('SELECT * FROM ' + tableName)
                rows = cur.fetchall()
                df = pd.DataFrame(rows)
                colnames = [col.name for col in cur.description]
                df.columns = colnames
            
            else:
            #指定したcolumnの全レコード取得
                #検索するcolumn列名を成形
                qrySnpt = ""
                for clnm in clmns.cols:
                    qrySnpt += clnm + ","
                qrySnpt = qrySnpt[:-1]
                
                cur.execute("SELECT " + qrySnpt + " FROM " + tableName)
                rows = cur.fetchall()
                df = pd.DataFrame(rows)
                colnames = [col.name for col in cur.description]
                df.columns = colnames

    return df
                