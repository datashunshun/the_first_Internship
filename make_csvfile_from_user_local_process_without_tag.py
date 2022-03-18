import os
from pandas.core.dtypes.missing import notnull
from pandas.core.series import Series
from pandas.io.pytables import dropna_doc
import psycopg2
import pandas as pd
import re
from glob import glob
import MeCab 
from gensim.models.doc2vec import TaggedDocument
from gensim.models.doc2vec import Doc2Vec
import numpy as np
from sqlalchemy import create_engine

db_userlocal = os.environ.get("DATABASE_URL")
db_userlocal_create_engine = os.environ.get("DATABASE_URL_CREATE_ENGINE")
db_userlocal_new = os.environ.get("DATABASE_URL_NEW_0304")
db_userlocal_create_engine_new = os.environ.get("DATABASE_URL_CREATE_ENGINE_NEW_0304")

query_1 = "SELECT source_entry_id,(SELECT url FROM user_local_process WHERE similar_article.source_entry_id = user_local_process.source_id) AS source_url,(SELECT group_1 FROM user_local_process WHERE similar_article.source_entry_id = user_local_process.source_id) AS source_title,similar_no1_entry_id,similar_no1_degree,(SELECT url FROM user_local_process WHERE similar_article.similar_no1_entry_id = user_local_process.source_id) AS similar_no1_url,(SELECT group_1 FROM user_local_process WHERE similar_article.similar_no1_entry_id = user_local_process.source_id) AS similar_no1_title,similar_no2_entry_id,similar_no2_degree,(SELECT url FROM user_local_process WHERE similar_article.similar_no2_entry_id = user_local_process.source_id) AS similar_no2_url,(SELECT group_1 FROM user_local_process WHERE similar_article.similar_no2_entry_id = user_local_process.source_id) AS similar_no2_title,similar_no3_entry_id,similar_no3_degree,(SELECT url FROM user_local_process WHERE similar_article.similar_no3_entry_id = user_local_process.source_id) AS similar_no3_url,(SELECT group_1 FROM user_local_process WHERE similar_article.similar_no3_entry_id = user_local_process.source_id) AS similar_no3_title,similar_no4_entry_id,similar_no4_degree,(SELECT url FROM user_local_process WHERE similar_article.similar_no4_entry_id = user_local_process.source_id) AS similar_no4_url,(SELECT group_1 FROM user_local_process WHERE similar_article.similar_no4_entry_id = user_local_process.source_id) AS similar_no4_title, similar_no5_entry_id, similar_no5_degree,(SELECT url FROM user_local_process WHERE similar_article.similar_no5_entry_id = user_local_process.source_id) AS similar_no5_url,(SELECT group_1 FROM user_local_process WHERE similar_article.similar_no5_entry_id = user_local_process.source_id) AS similar_no5_title FROM similar_article;"

def get_connection(string):
    return psycopg2.connect(string)

conn = get_connection(db_userlocal)
cur = conn.cursor()

cur.close
conn.close

##作成したdfをtable article_similartity_showがおかれているdbにimportするコードを作成した
df_0 = pd.read_sql(sql = query_1,con = conn)
def string_cutter(string):
    list_cutter_string = []
    if not string:
        return string
    else :
        list_cutter_string = re.split('/',string)
    ##list_cutter_string_size = int(len(list_cutter_string))
    ##print(list_cutter_string_size)
        id_string_pre_YYYY = list_cutter_string[4]
        ##print(id_string_pre_YYYY)
        id_string_pre_MMDD = list_cutter_string[5]
        ##print(id_string_pre_MMDD)
        id_string_pre_MMDD_temp = id_string_pre_MMDD[:4]
        ##print(id_string_pre_MMDD_temp)
        id_string = id_string_pre_YYYY + id_string_pre_MMDD_temp
        return id_string

##similar_noX_publish_date のカラム作成
##similar_no1_publish_date のレコード作成

list_url_1 = df_0['similar_no1_url']
similar_no1_publish_date = []

M_1 = int(len(list_url_1))

for i in range(0,M_1):
    temp = string_cutter(list_url_1[i])
    similar_no1_publish_date.append(temp)

Series_similar_no1_publish_date = pd.Series(similar_no1_publish_date)
df_1 = pd.DataFrame(Series_similar_no1_publish_date)
df_1.rename(columns = {0:'similar_no1_publish_date'},inplace = True)
#no2

list_url_2 = df_0['similar_no2_url']
similar_no2_publish_date = []

M_2 = int(len(list_url_2))

for i in range(0,M_2):
    temp = string_cutter(list_url_2[i])
    similar_no2_publish_date.append(temp)

Series_similar_no2_publish_date = pd.Series(similar_no2_publish_date)
df_2 = pd.DataFrame(Series_similar_no2_publish_date)
df_2.rename(columns = {0:'similar_no2_publish_date'},inplace = True)
##no3

list_url_3 = df_0['similar_no3_url']
similar_no3_publish_date = []

M_3 = int(len(list_url_3))

for i in range(0,M_3):
    temp = string_cutter(list_url_3[i])
    similar_no3_publish_date.append(temp)

Series_similar_no3_publish_date = pd.Series(similar_no3_publish_date)
df_3 = pd.DataFrame(Series_similar_no3_publish_date)
df_3.rename(columns = {0:'similar_no3_publish_date'},inplace = True)
##no4

list_url_4 = df_0['similar_no4_url']
similar_no4_publish_date = []

M_4 = int(len(list_url_4))

for i in range(0,M_4):
    temp = string_cutter(list_url_4[i])
    similar_no4_publish_date.append(temp)

Series_similar_no4_publish_date = pd.Series(similar_no4_publish_date)
df_4 = pd.DataFrame(Series_similar_no4_publish_date)
df_4.rename(columns = {0:'similar_no4_publish_date'},inplace = True)
##no5

list_url_5 = df_0['similar_no5_url']
similar_no5_publish_date = []

M_5 = int(len(list_url_5))

for i in range(0,M_5):
    temp = string_cutter(list_url_5[i])
    similar_no5_publish_date.append(temp)

Series_similar_no5_publish_date = pd.Series(similar_no5_publish_date)
df_5 = pd.DataFrame(Series_similar_no5_publish_date)
df_5.rename(columns = {0:'similar_no5_publish_date'},inplace = True)
##dataframeの結合
df = pd.concat([df_0,df_1,df_2,df_3,df_4,df_5],axis = 1)

COLUMNS =  ["source_entry_id","source_url","source_title","similar_no1_entry_id","similar_no1_degree","similar_no1_url","similar_no1_title","similar_no1_publish_date","similar_no2_entry_id","similar_no2_degree","similar_no2_url","similar_no2_title","similar_no2_publish_date","similar_no3_entry_id","similar_no3_degree","similar_no3_url","similar_no3_title","similar_no3_publish_date","similar_no4_entry_id","similar_no4_degree","similar_no4_url","similar_no4_title","similar_no4_publish_date","similar_no5_entry_id","similar_no5_degree","similar_no5_url","similar_no5_title","similar_no5_publish_date"]

df_new = df.reindex(columns = COLUMNS)

engine = create_engine(db_userlocal_create_engine_new)
conn = get_connection(db_userlocal_new)
cur = conn.cursor()

cur.execute('DELETE FROM article_similarity_show')

conn.commit()
cur.close
conn.close

print("cursor has got closed!")
df_new.to_csv("output_sql.csv",index = False)
df_new.to_sql('article_similarity_show', con=engine, if_exists='append', index=False)
print("importing is complete!")
