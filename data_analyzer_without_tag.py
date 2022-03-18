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

##データベースにアクセスして必要なデータをとってきてデータフレームに格納する
condition_1 = 'mt_entry.entry_blog_id = 7 OR mt_entry.entry_blog_id = 11'
condition_2 = 'mt_entry.entry_status  = 2'
condition_3 = 'mt_entry.entry_id >= 50000'

COLUMNS = 'entry_id,entry_title,entry_text,entry_text_more,entry_blog_id,entry_status'

query_1 = f'SELECT {COLUMNS} FROM mt_entry where {condition_1} AND {condition_2} AND {condition_3}'

path = "./result/result.csv"
##環境変数を読み込むように書き直した

db_mt_entry_url = os.environ.get("DATABASE_URL")
db_similar_article_url = os.environ.get("DATABASE_URL_NEW_0304")
db_similar_article_url_create_engine = os.environ.get("DATABASE_URL_CREATE_ENGINE_NEW_0304")

print(db_similar_article_url)
print(db_similar_article_url_create_engine)

def get_connection(string):
    return psycopg2.connect(string)

conn = get_connection(db_mt_entry_url)
cur = conn.cursor()

cur.close
conn.close

df = pd.read_sql(sql = query_1,con = conn)

##entry_textとentry_text_moreを結合する．

N = int(len(df['entry_text']))
print(N)

drop_index = []

for i in range(0,N):
    if(pd.isnull(df['entry_text'][i]) ==True and pd.isnull(df['entry_text_more'][i]) == True):
        drop_index.append(i)

##print(drop_index)
df = df.drop(drop_index)
M = int(len(df['entry_text']))

df = df.reset_index()
out_entry_text = [0]*M

for i in range(0,M):
    if((pd.notnull(df['entry_text'][i]) == True) and (pd.notnull(df['entry_text_more'][i]) == True)):
        out_entry_text[i] = df['entry_text'][i] + df['entry_text_more'][i]
    elif(pd.notnull(df['entry_text'][i]) == True):
        out_entry_text[i] = df['entry_text'][i]
    else:
        out_entry_text[i] = df['entry_text_more'][i]

##文章からタグを取り除く操作
cleanr = re.compile('<.*?>')
for i in range(0,M):
    out_entry_text[i] = re.sub(cleanr,'',out_entry_text[i])
for i in range(0,M):
    out_entry_text[i] = out_entry_text[i].lstrip()

df_temp_1 = pd.DataFrame(out_entry_text)
df_temp_1.rename(columns={0:'entry_text'},inplace = True)
df_temp_2 = df[['entry_id','entry_title','entry_blog_id','entry_status']]
df_new = pd.concat([df_temp_1,df_temp_2],axis = 1)

##記事を形態素解析する

m = MeCab.Tagger("-Ochasen")
text_wakati= []
##↓記事が空のレコードにnull文字列を追加する↓   
def listsplitting(string):
    listtemp = []    
    for d in m.parse(string).splitlines():
        # print(d.split())
        # print(pd.notnull(d.split()[0]))
        if(int(len(d.split())) != 0):
            listtemp.append(d.split()[0])
        else:
            continue
    return listtemp

for i in range(0,M):
    text_wakati.append(listsplitting(df_new['entry_text'][i]))
##類似度算出する
Displayed_number = 5
COLUMNS = ['source_entry_id','source_entry_blog_id','similar_no1_entry_id','similar_no1_degree','similar_no2_entry_id','similar_no2_degree','similar_no3_entry_id','similar_no3_degree','similar_no4_entry_id','similar_no4_degree','similar_no5_entry_id','similar_no5_degree']
df_output = pd.DataFrame(columns = COLUMNS)

cnt = 0
doc_train = []
for words in text_wakati:
    doc_train.append(TaggedDocument(words,[cnt]))
    cnt += 1

print('text_ok')

model = Doc2Vec(doc_train,dm=1,vector_size = 200, min_count=10, epochs=20)

def list_decomposer(number,sample,size_of_list):
    list_2D = sample.dv.most_similar(number)
    list_target = [0]*size_of_list
    list_ruiji = [0]*size_of_list
    for i in range(0,size_of_list):
        list_target[i] = list_2D[i][0] 
        list_ruiji[i] = list_2D[i][1]
    return list_ruiji,list_target

def record_research_entryid(index):
    entryid = df_new['entry_id'][index]
    return int(entryid)

def record_research_entry_blog_id(index):
    entry_blog_id = df_new['entry_blog_id'][index]
    return int(entry_blog_id)

def record_research_entry_text(index):
    entry_text = df_new['entry_text'][index]
    return entry_text

def list_merging(LIST_1,LIST_2):
    LIST = pd.Series(index = COLUMNS)
    # ##ソース記事id格納
    LIST['source_entry_id'] = int(LIST_1[0])
    LIST['source_entry_blog_id'] = LIST_1[1]

    # ##類似記事のid格納
    LIST['similar_no1_entry_id'] = int(LIST_2[0][0])
    LIST['similar_no2_entry_id'] = int(LIST_2[1][0])
    LIST['similar_no3_entry_id'] = int(LIST_2[2][0])
    LIST['similar_no4_entry_id'] = int(LIST_2[3][0])
    LIST['similar_no5_entry_id'] = int(LIST_2[4][0])

    # ##類似記事の類似度格納
    LIST['similar_no1_degree'] = LIST_2[0][1]
    LIST['similar_no2_degree'] = LIST_2[1][1]
    LIST['similar_no3_degree'] = LIST_2[2][1]
    LIST['similar_no4_degree'] = LIST_2[3][1]
    LIST['similar_no5_degree'] = LIST_2[4][1]

    return LIST
print('function_ok')

for i in range(0,M):
    ##リストの初期化
    LIST_source_text = [0]*2
    LIST_target_text = []
    list_similarity,list_index = list_decomposer(i,model,Displayed_number)
    LIST_source_text[0] = int(record_research_entryid(i))
    LIST_source_text[1] = record_research_entry_blog_id(i)
    for j in range(0,Displayed_number):
        LIST_target_text.append([int(record_research_entryid(list_index[j])),list_similarity[j]])
    list_temp = list_merging(LIST_source_text,LIST_target_text)
    df_output = df_output.append(list_temp,ignore_index= True)

df_output['source_entry_id'] = df_output['source_entry_id'].astype(np.int64)
df_output['source_entry_blog_id'] = df_output['source_entry_blog_id'].astype(np.int64)
df_output['similar_no1_entry_id'] = df_output['similar_no1_entry_id'].astype(np.int64)
df_output['similar_no2_entry_id'] = df_output['similar_no2_entry_id'].astype(np.int64)
df_output['similar_no3_entry_id'] = df_output['similar_no3_entry_id'].astype(np.int64)
df_output['similar_no4_entry_id'] = df_output['similar_no4_entry_id'].astype(np.int64)
df_output['similar_no5_entry_id'] = df_output['similar_no5_entry_id'].astype(np.int64)

print("DataFrame is created!")

##データベースにアクセスしてデータを置き換える
print(db_similar_article_url_create_engine)

engine = create_engine(db_similar_article_url_create_engine)
conn = get_connection(db_similar_article_url)
cur = conn.cursor()

cur.execute('DELETE FROM similar_article')

conn.commit()
cur.close
conn.close

print("cursor has got closed!")

df_output.to_sql('similar_article', con=engine, if_exists='append', index=False)