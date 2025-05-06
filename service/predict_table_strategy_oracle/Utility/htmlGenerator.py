# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang 
# CREATED ON: 2020/3/5 下午1:17
# LAST MODIFIED ON:
# AIM: 用来生成html
import pandas as pd
from typing import List, Tuple, Dict, Union


def g_begining() -> str:
    out = '''
<html lang="cn">
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"></head>
        <body class="awr"><title></title>
    '''
    return out


def g_ending() -> str:
    out = '''
    </body>
</html>
    '''
    return out


def g_css() -> str:
    out = '''
<style type="text/css">
body.awr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black; background:White;}
pre.awr  {font:8pt Courier;color:black; background:White;}
h1.awr   {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:White;border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}
h2.awr   {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:White;margin-top:4pt; margin-bottom:0pt;}
h3.awr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#336699;background-color:White;margin-top:4pt; margin-bottom:0pt;}
li.awr {font: 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}
th.awrnobg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}
th.awrbg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; background:#0066CC;padding-left:4px; padding-right:4px;padding-bottom:2px}
td.awrnc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;}
td.awrc    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;}
td.awrnclb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-left: thin solid black;}
td.awrncbb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-left: thin solid black;border-right: thin solid black;}
td.awrncrb {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-right: thin solid black;}
td.awrcrb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-right: thin solid black;}
td.awrclb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-left: thin solid black;}
td.awrcbb    {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-left: thin solid black;border-right: thin solid black;}
a.awr {font:bold 8pt Arial,Helvetica,sans-serif;color:#663300; vertical-align:top;margin-top:0pt; margin-bottom:0pt;}
td.awrnct {font:8pt Arial,Helvetica,Geneva,sans-serif;border-top: thin solid black;color:black;background:White;vertical-align:top;}
td.awrct   {font:8pt Arial,Helvetica,Geneva,sans-serif;border-top: thin solid black;color:black;background:#FFFFCC; vertical-align:top;}
td.awrnclbt  {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-top: thin solid black;border-left: thin solid black;}
td.awrncbbt  {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-left: thin solid black;border-right: thin solid black;border-top: thin solid black;}
td.awrncrbt {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:White;vertical-align:top;border-top: thin solid black;border-right: thin solid black;}
td.awrcrbt     {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-right: thin solid black;}
td.awrclbt     {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-left: thin solid black;}
td.awrcbbt   {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;background:#FFFFCC; vertical-align:top;border-top: thin solid black;border-left: thin solid black;border-right: thin solid black;}
table.tdiff {  border_collapse: collapse; }
.hidden   {position:absolute;left:-10000px;top:auto;width:1px;height:1px;overflow:hidden;}
.pad   {margin-left:10px;}
.doublepad {margin-left:34px;}
</style>
    '''
    return out


def g_head(head: str) -> str:
    return '<h3 class="awr">{0}</h3>'.format(head)


def g_verbose(words: str) -> str:
    return '<p>{0}</p>'.format(words)


def g_list(words: str, link: str = None) -> str:
    if link:
        return '<li class="awr"> <a class="awr" href="#{0}">{1}</a></li>'.format(link, words)
    return '<li class="awr">{0}</li>'.format(words)


def g_table_pd(tab: pd.DataFrame) -> str:
    out = '''
<p>
    <table border="0" width="600" class="tdiff" summary="auto generate">
        <tbody>
    '''
    out += add_table_header(tab.columns)
    for e, id in enumerate(tab.index):
        if e % 2:
            out += add_table_row(tab.loc[id].values, 'awrnc')
        else:
            out += add_table_row(tab.loc[id].values, 'awrc')
    out += '''
         </tbody>
    </table>
</p>
<p></p>
    '''''
    return out


def g_table_dict_h(tab: Dict[str, Union[str, List[str]]], **kwargs) -> str:
    try:
        linked = kwargs['linked']
    except:
        linked = None
    out = '''
<p>
    <table border="0" width="600" class="tdiff" summary="auto generate">
        <tbody>
        '''
    for id, key in enumerate(tab):
        value = tab[key]
        out += add_tale_row_dict(key, value, id, name=linked)
    out += '''
         </tbody>
    </table>
</p>
<p></p>
        '''''
    return out


def add_tale_row_dict(key: str, row: Union[List[str], str], id: int, name: str = None) -> str:
    indent = '\t\t\t'
    out = indent + '<tr>\n'
    if isinstance(row, list):
        out += indent + '\t<th rowspan="{0}" class="awrbg" scope ="col"><pre>{1}</pre></th>\n'.format(len(row), key)
        if id:
            out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrnc', row[0])
        else:
            out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrc', row[0])
        out += indent + '</tr>\n'
        id += 1
        for i, e_row in enumerate(row[1::]):
            out += indent + '<tr>\n'
            if (i + id) % 2:
                out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrnc', e_row)
            else:
                out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrc', e_row)
            out += indent + '</tr>\n'
    else:
        out += indent + '\t<th class="awrbg" scope="col"><pre>{0}</pre></th>\n'.format(key)
        if name and key== name:
            value = '<a name="{0}">{1}</a>'.format(row,row)
        else:
            value = row
        if id:
            out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrnc', value)
        else:
            out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format('awrc', value)
        out += indent + '</tr>\n'
    return out


def add_table_header(row: List[str]) -> str:
    indent = '\t\t\t'
    out = indent + '<tr>\n'
    for element in row:
        out += indent + '\t<th class="awrbg" scope="col"><pre>{0}</pre></th>\n'.format(element)
    out += indent + '</tr>\n'
    return out


def add_table_row(row: List[object], c: str = 'awrc') -> str:
    indent = '\t\t\t'
    out = indent + '<tr>\n'
    out += indent + '\t<td scope="row" class="{0}"><pre>{1}</pre></td>\n'.format(c, row[0])
    for element in row[1::]:
        out += indent + '\t<td align="left" class="{0}"><pre>{1}</pre></td>\n'.format(c, element)
    out += indent + '</tr>\n'
    return out


if __name__ == '__main__':
    # ---- row data ---- #
    df = pd.DataFrame({'表名': ["APP_TRANS", "H_APP_TRANS", "ACK_TRANS", "ARK_BAL_FUND_HIS", "H_ACK_TRANS", "CFG_FUND",
                              "ARK_BAL_FUND", "SALE_ACCT_MONEYACCOUNT", "SALE_APP_BACKBALANCE", "ACK_ACCT"],
                       '出现次数(次/天)': [455, 424, 272, 198, 184, 82, 17, 13, 6, 6]})

    ddl = ['CREATE INDEX ARKDATA.APP_TRANS_INDEX0 ON APP_TRANS(TAACCOUNTID) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.APP_TRANS_INDEX1 ON APP_TRANS(CUSTFULLNAME,APPSHEETSERIALNO) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.H_APP_TRANS_INDEX0 ON H_APP_TRANS(CUSTFULLNAME) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.ACK_TRANS_INDEX0 ON ACK_TRANS(SNO) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.ACK_TRANS_INDEX1 ON ACK_TRANS(CONFIRMEDVOL,CONFIRMEDAMOUNT) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.ARK_BAL_FUND_HIS_INDEX0 ON ARK_BAL_FUND_HIS(BAL_FUND) ONLINE COMPUTE STATISTICS;',
           'CREATE INDEX ARKDATA.H_ACK_TRANS_INDEX0 ON H_ACK_TRANS(CUSTNO) ONLINE COMPUTE STATISTICS;']
    over_all = [{'表名': 'ark.ARKDATA.APP_TRANS',
                 '构建索引': [' ARKDATA.APP_TRAN0 (TAACCOUNTID)', ' ARKDATA.APP_TRAN1 (CUSTFULLNAME,APPSHEETSERIALNO)',
                          'aaaa', 'bbbb'],
                 '性能最多提高 99.99%': '其中有 2.96% 的sql性能提高, 96.90% 的sql性能不变, 0.14% 的sql性能变差'},
                {'表名': 'ark.ARKDATA.APP_TRANS',
                 '构建索引': [' ARKDATA.APP_TRAN0 (TAACCOUNTID)', ' ARKDATA.APP_TRAN1 (CUSTFULLNAME,APPSHEETSERIALNO)',
                          'aaaa', 'bbbb'],
                 '性能最多提高 99.99%': '其中有 2.96% 的sql性能提高, 96.90% 的sql性能不变, 0.14% 的sql性能变差'}]

    detial_list = [{'表名': 'ark.ARKDATA.H_APP_TRANS',
                    '对应的sql id': ['8qx6ph55d9bbw', '1t78nzzrqht48', '5nmfd3jpn4cf7', '7j9gtt88jxm4f', '92rucz19dzqyw']}]

    detial_sql = [{'sql_id': '8qx6ph55d9bbw',
                   'sql_text': '''
    select count(1) tmpCount 
	from app_trans a left join cfg_fund c on a.fundcode = c.fundcode
        where a.transactiondate >= '20170515'         
	and a.is_delete = '0'                      
	and a.transactiondate >= :1
   	and a.transactiondate<= :2
        and a.cancelflag= 3
        and a.custfullname like '%'||:4 ||'%'
        union all
	select count(1) tmpCount 
	from h_app_trans b left join cfg_fund d on b.fundcode = d.fundcode                  
	where b.transactiondate <= '20170514'
	and b.transactiondate  >= :5  
	and b.transactiondate  <= :6 
	and b.cancelflag=:7                 
                               ''',
                   '原始执行执行计划': '''
    Plan hash value: 1134915831

---------------------------------------------------------------------------------------------------------------------------
| Id  | Operation                       | Name                    | Rows  | Bytes | Cost (%CPU)| Time     | Pstart| Pstop |
---------------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                |                         |     1 |    13 |   107K  (1)| 00:21:31 |       |       |
|   1 |  SORT AGGREGATE                 |                         |     1 |    13 |            |          |       |       |
|   2 |   VIEW                          |                         |     2 |    26 |   107K  (1)| 00:21:31 |       |       |
|   3 |    UNION-ALL                    |                         |       |       |            |          |       |       |
|   4 |     SORT AGGREGATE              |                         |     1 |    27 |            |          |       |       |
|*  5 |      TABLE ACCESS BY INDEX ROWID| APP_TRANS               |  1442 | 38934 |  3955   (1)| 00:00:48 |       |       |
|*  6 |       INDEX RANGE SCAN          | IDX_APP_TRANS_DATE_CODE | 10383 |       |  1310   (1)| 00:00:16 |       |       |
|   7 |     SORT AGGREGATE              |                         |     1 |    25 |            |          |       |       |
|   8 |      PARTITION RANGE AND        |                         |   275 |  6875 |   103K  (1)| 00:20:44 |KEY(AP)|KEY(AP)|
|*  9 |       TABLE ACCESS FULL         | H_APP_TRANS             |   275 |  6875 |   103K  (1)| 00:20:44 |KEY(AP)|KEY(AP)|
---------------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   5 - filter("A"."CANCELFLAG"=:3 AND "A"."CUSTFULLNAME" LIKE '%'||:4||'%' AND "A"."IS_DELETE"='0')
   6 - access("A"."TRANSACTIONDATE">='20170515' AND "A"."TRANSACTIONDATE"<=:2)
       filter("A"."TRANSACTIONDATE">=:1)
   9 - filter("B"."TRANSACTIONDATE">=:5 AND "B"."TRANSACTIONDATE"<=:6 AND "B"."CANCELFLAG"=:7 AND
              "B"."CUSTFULLNAME" LIKE '%'||:8||'%' AND "B"."TRANSACTIONDATE"<='20170514')
                   ''',
                   '加入索引后的执行计划': '''
    Plan hash value: 911165992

----------------------------------------------------------------------------------------------------------------------
| Id  | Operation                              | Name        | Rows  | Bytes | Cost (%CPU)| Time     | Pstart| Pstop |
----------------------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT                       |             |     1 |    13 |    20   (0)| 00:00:01 |       |       |
|   1 |  SORT AGGREGATE                        |             |     1 |    13 |            |          |       |       |
|   2 |   VIEW                                 |             |     2 |    26 |    20   (0)| 00:00:01 |       |       |
|   3 |    UNION-ALL                           |             |       |       |            |          |       |       |
|   4 |     SORT AGGREGATE                     |             |     1 |    27 |            |          |       |       |
|*  5 |      TABLE ACCESS BY INDEX ROWID       | APP_TRANS   |  1442 | 38934 |    10   (0)| 00:00:01 |       |       |
|*  6 |       INDEX RANGE SCAN                 | APP_TRAN1   |   207K|       |     2   (0)| 00:00:01 |       |       |
|   7 |     SORT AGGREGATE                     |             |     1 |    25 |            |          |       |       |
|*  8 |      TABLE ACCESS BY GLOBAL INDEX ROWID| H_APP_TRANS |   275 |  6875 |    10   (0)| 00:00:01 | ROWID | ROWID |
|*  9 |       INDEX RANGE SCAN                 | H_APP_TRAN0 | 39947 |       |     2   (0)| 00:00:01 |       |       |
----------------------------------------------------------------------------------------------------------------------

Predicate Information (identified by operation id):
---------------------------------------------------

   5 - filter("A"."TRANSACTIONDATE">=:1 AND "A"."TRANSACTIONDATE"<=:2 AND "A"."CANCELFLAG"=:3 AND
              "A"."IS_DELETE"='0' AND "A"."TRANSACTIONDATE">='20170515')
   6 - access("A"."CUSTFULLNAME" LIKE '%'||:4||'%')
       filter("A"."CUSTFULLNAME" LIKE '%'||:4||'%')
   8 - filter("B"."TRANSACTIONDATE">=:5 AND "B"."TRANSACTIONDATE"<=:6 AND "B"."CANCELFLAG"=:7 AND
              "B"."TRANSACTIONDATE"<='20170514')
   9 - access("B"."CUSTFULLNAME" LIKE '%'||:8||'%')
       filter("B"."CUSTFULLNAME" LIKE '%'||:8||'%')
    '''},
                  {'sql_id': '8qx6ph55d9bbw',
                   'sql_text': '''
                      select count(1) tmpCount 
                  	from app_trans a left join cfg_fund c on a.fundcode = c.fundcode
                          where a.transactiondate >= '20170515'         
                  	and a.is_delete = '0'                      
                  	and a.transactiondate >= :1
                     	and a.transactiondate<= :2
                          and a.cancelflag= 3
                          and a.custfullname like '%'||:4 ||'%'
                          union all
                  	select count(1) tmpCount 
                  	from h_app_trans b left join cfg_fund d on b.fundcode = d.fundcode                  
                  	where b.transactiondate <= '20170514'
                  	and b.transactiondate  >= :5  
                  	and b.transactiondate  <= :6 
                  	and b.cancelflag=:7                 
                                                 ''',
                   '原始执行执行计划': '''
                      Plan hash value: 1134915831

                  ---------------------------------------------------------------------------------------------------------------------------
                  | Id  | Operation                       | Name                    | Rows  | Bytes | Cost (%CPU)| Time     | Pstart| Pstop |
                  ---------------------------------------------------------------------------------------------------------------------------
                  |   0 | SELECT STATEMENT                |                         |     1 |    13 |   107K  (1)| 00:21:31 |       |       |
                  |   1 |  SORT AGGREGATE                 |                         |     1 |    13 |            |          |       |       |
                  |   2 |   VIEW                          |                         |     2 |    26 |   107K  (1)| 00:21:31 |       |       |
                  |   3 |    UNION-ALL                    |                         |       |       |            |          |       |       |
                  |   4 |     SORT AGGREGATE              |                         |     1 |    27 |            |          |       |       |
                  |*  5 |      TABLE ACCESS BY INDEX ROWID| APP_TRANS               |  1442 | 38934 |  3955   (1)| 00:00:48 |       |       |
                  |*  6 |       INDEX RANGE SCAN          | IDX_APP_TRANS_DATE_CODE | 10383 |       |  1310   (1)| 00:00:16 |       |       |
                  |   7 |     SORT AGGREGATE              |                         |     1 |    25 |            |          |       |       |
                  |   8 |      PARTITION RANGE AND        |                         |   275 |  6875 |   103K  (1)| 00:20:44 |KEY(AP)|KEY(AP)|
                  |*  9 |       TABLE ACCESS FULL         | H_APP_TRANS             |   275 |  6875 |   103K  (1)| 00:20:44 |KEY(AP)|KEY(AP)|
                  ---------------------------------------------------------------------------------------------------------------------------

                  Predicate Information (identified by operation id):
                  ---------------------------------------------------

                     5 - filter("A"."CANCELFLAG"=:3 AND "A"."CUSTFULLNAME" LIKE '%'||:4||'%' AND "A"."IS_DELETE"='0')
                     6 - access("A"."TRANSACTIONDATE">='20170515' AND "A"."TRANSACTIONDATE"<=:2)
                         filter("A"."TRANSACTIONDATE">=:1)
                     9 - filter("B"."TRANSACTIONDATE">=:5 AND "B"."TRANSACTIONDATE"<=:6 AND "B"."CANCELFLAG"=:7 AND
                                "B"."CUSTFULLNAME" LIKE '%'||:8||'%' AND "B"."TRANSACTIONDATE"<='20170514')
                                     ''',
                   '加入索引后的执行计划': '''
                      Plan hash value: 911165992

                  ----------------------------------------------------------------------------------------------------------------------
                  | Id  | Operation                              | Name        | Rows  | Bytes | Cost (%CPU)| Time     | Pstart| Pstop |
                  ----------------------------------------------------------------------------------------------------------------------
                  |   0 | SELECT STATEMENT                       |             |     1 |    13 |    20   (0)| 00:00:01 |       |       |
                  |   1 |  SORT AGGREGATE                        |             |     1 |    13 |            |          |       |       |
                  |   2 |   VIEW                                 |             |     2 |    26 |    20   (0)| 00:00:01 |       |       |
                  |   3 |    UNION-ALL                           |             |       |       |            |          |       |       |
                  |   4 |     SORT AGGREGATE                     |             |     1 |    27 |            |          |       |       |
                  |*  5 |      TABLE ACCESS BY INDEX ROWID       | APP_TRANS   |  1442 | 38934 |    10   (0)| 00:00:01 |       |       |
                  |*  6 |       INDEX RANGE SCAN                 | APP_TRAN1   |   207K|       |     2   (0)| 00:00:01 |       |       |
                  |   7 |     SORT AGGREGATE                     |             |     1 |    25 |            |          |       |       |
                  |*  8 |      TABLE ACCESS BY GLOBAL INDEX ROWID| H_APP_TRANS |   275 |  6875 |    10   (0)| 00:00:01 | ROWID | ROWID |
                  |*  9 |       INDEX RANGE SCAN                 | H_APP_TRAN0 | 39947 |       |     2   (0)| 00:00:01 |       |       |
                  ----------------------------------------------------------------------------------------------------------------------

                  Predicate Information (identified by operation id):
                  ---------------------------------------------------

                     5 - filter("A"."TRANSACTIONDATE">=:1 AND "A"."TRANSACTIONDATE"<=:2 AND "A"."CANCELFLAG"=:3 AND
                                "A"."IS_DELETE"='0' AND "A"."TRANSACTIONDATE">='20170515')
                     6 - access("A"."CUSTFULLNAME" LIKE '%'||:4||'%')
                         filter("A"."CUSTFULLNAME" LIKE '%'||:4||'%')
                     8 - filter("B"."TRANSACTIONDATE">=:5 AND "B"."TRANSACTIONDATE"<=:6 AND "B"."CANCELFLAG"=:7 AND
                                "B"."TRANSACTIONDATE"<='20170514')
                     9 - access("B"."CUSTFULLNAME" LIKE '%'||:8||'%')
                         filter("B"."CUSTFULLNAME" LIKE '%'||:8||'%')
                      ''',
                '性能':'提升90%'}
                  ]
    # --- generate html --- #
    out = g_begining()
    out += g_css()
    out += g_head('报告')
    out += g_verbose('发现35张表,前10个出现频率最高的表为')
    out += g_table_pd(df)
    out += g_verbose('其中17.14%(6个)表有足够的统计意义可以构建索引策略')
    out += g_head('DDL')
    out += g_verbose('')
    for e in ddl:
        out += g_list(e)
    out += g_verbose('')
    out += g_head('Overall Performance')
    out += g_verbose('')
    for e in over_all:
        out += g_table_dict_h(e)
    out += g_head('Performance in Detail')
    for info in detial_list:
        for key in info:
            value = info[key]
            if isinstance(value, str):
                out += g_verbose(value + ' 对应的sql id:')
            elif isinstance(value, list):
                for sql_id in value:
                    out += g_list(words=sql_id, link=sql_id)
    for e in detial_sql:
        out += g_table_dict_h(e, linked='sql_id')
        out += g_verbose("")
    out += g_ending()

    with open('test.html', 'w') as f:
        f.write(out)
