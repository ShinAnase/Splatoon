import psycopg2
import pandas as pd
from tqdm import tqdm

#ExecDataの作成
#Input：DB名
#Output：なし
def createExecData(dbname):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE TABLE execdata_train AS SELECT * FROM train;')
            cur.execute('CREATE TABLE execdata_test AS SELECT * FROM test;')
    
    print("DONE.")



#ExecData削除
#Input：DB名
#Output：なし
def DeleteExecData(dbname):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute('DROP TABLE execdata_train, execdata_test;')
    
    print("DONE.")



#列名の配列抽出
#Input：DB名、テーブル名
#Output：列名(dataframe)
def readColumns(dbname, tablename):
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '" + tablename + "' ORDER BY ordinal_position;")

            cols = cur.fetchall()
            df_columns = pd.DataFrame(cols, columns=['cols'])
    
    return df_columns



#DataTableの読み込み
#Input：DB名、テーブル名、抽出するcolumn名(指定しなければ前列を抽出する)
#Output：指定した列のテーブル(dataframe)
def selectDataTable(dbname, tableName, clmns = None):
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



###　頓挫(updateの方針を変えたため)：列削除で使えそうなので残しておく ###
#指定列のUpdate(列ごと挿げ替え)
#Input：DB名、テーブル名、挿げ替えるtable(dataframe)
#Output：なし
def exchangeExecData(dbname, tableName, df):
    #列名取得
    clmnNns = df.columns
    
    #挿げ替える列名削除用query作成
    qrySnpt = ""
    for nm in clmnNns:
        qrySnpt = qrySnpt + "DROP COLUMN " + nm + ", "
    qrySnpt = qrySnpt[:-2]
    qrySnpt += ";"
    Query = "ALTER TABLE " + tableName +  " " + qrySnpt
    
    #挿げ替える前の列削除
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            cur.execute(Query)
    
    #列挿げ替え用のquery作成
    
    return




#指定列のUpdate
#Input：DB名、テーブル名、更新するtable(dataframe)、主キーとなるtable(dataframe)
#Output：なし
def updateFeatures(dbname, tableName, updDf, pkeyDf):
    #queryの枕詞
    iniQrySnpt = "update " + tableName + " set "
    
    #指定列のUpdate実行
    with psycopg2.connect("host=localhost port=5432 dbname=" + dbname + " user=tidal password=tidalryoku") as conn:
        with conn.cursor() as cur:
            for recNum in tqdm(range(pkeyDf.shape[0])):
                #updateするクエリ
                updQrySnpt = ""
                for clmnNm in updDf:
                    updQrySnpt = updQrySnpt + clmnNm + "='" + str(updDf[clmnNm][recNum]) +"', "
                updQrySnpt = updQrySnpt[:-2] + " "
                #条件クエリ
                whrQrySnpt = "where "
                for clmnNm in pkeyDf:
                    whrQrySnpt = whrQrySnpt + clmnNm + "='" + str(pkeyDf[clmnNm][recNum]) + "' and "
                whrQrySnpt = whrQrySnpt[:-5] + ";"
                #query結合
                execQry = iniQrySnpt + updQrySnpt + whrQrySnpt
                #query実行
                cur.execute(execQry)
    #print(execQry)
    print("Done.")




    