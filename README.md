# similar_article_improved
## with tag case process
1. mt_entry,mt_tag,mt_objecttagからdataを抽出加工
    - data_processer.py
2. 加工したデータをmt_gat_objecttag_entryへimportする
    - data_processe.py
3. mt_tag_objecttag_entryからdata抽出・加工
    - data_processer.py
4. 類似度算出
    - data_analyzer_with_tag.py
5. similar_articleへimportする
    - data_analyzer_with_tag.py
6. userloal,similar_articleからdata抽出
    - make_user_local_process.py
7. user_local_processへimport
    - make_user_local_process
8. user_local_process,similar_articleからdata抽出
    - make_csvfile_from_user_local_process_with_tag
9. article_similar_showへimport
    - make_csvfile_from_user_local_process_with_tag

## witout tag case process
1. mt_entryから抽出加工
    - data_analyzer_without_tag
2. 類似度算出
    - data_analyzer_without_tag
3. similar_articleへimportする
    - data_analyzer_without_tag
4. userlocal,similar_articleからdata抽出・加工
    - make_user_local_process
5. user_local_processへimport
    - make_user_local_process
6. user_local_process,similar_articleからdata抽出
    - make_csvfile_from_user_local_process_without_tag
7. artivle_similar_showへインポート
    - make_csvfile_from_user_local_process_without_tag