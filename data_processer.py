import os
from pandas.core.dtypes.missing import notnull
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
db_userlocal_new = os.environ.get("DATABASE_URL")
db_userlocal_create_engine_new = os.environ.get("DATABASE_URL_CREATE_ENGINE")

##データベースにアクセスして必要なデータをとってきてデータフレームに格納する
condition_1 = 'mt_entry.entry_id = mt_objecttag.objecttag_object_id'
condition_2 = 'mt_objecttag.objecttag_tag_id = mt_tag.tag_id'

COLUMNS = 'mt_entry.entry_id,mt_entry.entry_title,mt_entry.entry_text,mt_entry.entry_text_more,mt_entry.entry_blog_id,mt_entry.entry_status,mt_tag.tag_id,mt_tag.tag_name'

query_1 = f'SELECT {COLUMNS} FROM mt_entry,mt_tag,mt_objecttag  where {condition_1} AND {condition_2}'

path = "./result/result.csv"

db_mt_entry_url = os.environ.get("DATABASE_URL_TEST")
db_similar_article_url = os.environ.get("DATABASE_URL_TEST")
db_similar_article_url_create_engine = os.environ.get("DATABASE_URL_CREATE_ENGINE_TEST")

print(db_similar_article_url)
print(db_similar_article_url_create_engine)

def get_connection(string):
    return psycopg2.connect(string)

conn = get_connection(db_mt_entry_url)
cur = conn.cursor()

cur.close
conn.close

df = pd.read_sql(sql = query_1,con = conn)

engine = create_engine(db_userlocal_create_engine_new)
conn = get_connection(db_userlocal_new)
cur = conn.cursor()

cur.execute('DELETE FROM mt_tag_objecttag_entry')

conn.commit()
cur.close
conn.close

print("cursor has got closed!")

df.to_csv("output_sql.csv",index = False)
df.to_sql('mt_tag_objecttag_entry', con=engine, if_exists='append', index=False)

print("importing is complete!")