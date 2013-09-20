SET SCHEMA = SZPX;

CREATE PROCEDURE SZPX.P_SZPX_SJDL_ALL (
    IN IN_XMLX    VARCHAR(20),
    IN IN_JZBH    VARCHAR(40),
    OUT OUT_JG    VARCHAR(5000),
    OUT OP_V_ERR_MSG    VARCHAR(3000) )
  SPECIFIC SQL130906173844600
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN

    DECLARE V_RES      INTEGER;               --导入条数
    DECLARE V_JZBH     VARCHAR(40);           --加载编号
    DECLARE V_XMLX     VARCHAR(20);           --项目类型
    DECLARE V_XZZDT    VARCHAR(500);          --动态新增字段(临时,不转换)
    DECLARE V_XZZDTE   VARCHAR(500);          --动态新增字段(临时,要转换)
    DECLARE V_XZZDR    VARCHAR(2000);         --动态新增字段(结果,INSERT后以及查询结果)
    DECLARE V_XZZDR_A  VARCHAR(2000);         --动态新增字段(A表结果,INSERT后以及查询结果)
    DECLARE V_XZZDR_B  VARCHAR(2000);         --动态新增字段(B表结果,INSERT后以及查询结果)
    DECLARE V_XZZDRE   VARCHAR(2000);         --动态新增字段(结果,查询数据处需转换类型)
    DECLARE EXE_SQL    VARCHAR(20000);        --动态SQL
    DECLARE V_KSXH     INTEGER;               --开始序号
    DECLARE V_JSXH     INTEGER;               --结果序号
    DECLARE V_SQLSTATE CHAR(5);
    DECLARE I_SQLCODE  INTEGER; 
    DECLARE SQLSTATE   CHAR(5); 
    DECLARE SQLCODE    INTEGER; 

    --定义游标
    DECLARE c0 CURSOR for s0;
    DECLARE c1 CURSOR with return for s1;

    --异常处理
    DECLARE exit HANDLER FOR SqlException
        BEGIN 
            VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
            SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
                ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
                ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
            ; 
            SET OUT_JG=EXE_SQL;
    END;
    

--主成份临时结果表    
DECLARE GLOBAL TEMPORARY TABLE SESSION.ZCFJGB(
       ID      INTEGER,
       GPJWJM  VARCHAR(500),
       GPWJM   VARCHAR(500),
       GPMXMC  VARCHAR(255),
       GPMXID  INTEGER,
       JM_YC   VARCHAR(200),
       YPID    VARCHAR(100),
       SCSJ    TIMESTAMP
)with replace on commit preserve rows NOT LOGGED;
    
--新增字段
DECLARE GLOBAL TEMPORARY TABLE SESSION.XZZD(
    XZZDRE  VARCHAR(1500),
    XZZDR_A VARCHAR(800),
    XZZDR_B VARCHAR(800),
    XZZDR   VARCHAR(400)
)with replace on commit preserve rows NOT LOGGED;    
    
    SET V_JZBH=IN_JZBH;                       --判断加载编号(用于多用户执行导入操作时的区分)
    SET V_XMLX=LCASE(IN_XMLX);                --判断项目类型(raw:原料,medium:中间件,product:产品)

    IF V_XMLX NOT IN ('raw','medium','product','RAW','MEDIUM','PRODUCT','productQuality','productquality','middle','MIDDLE')
    THEN 
    SET OP_V_ERR_MSG='输入参数错误';
    RETURN;
    END IF;
    
    
    IF V_XMLX='raw' THEN 
    SET OUT_JG='开始导入原料数据';
    
    SET OUT_JG='开始插入(或更新)原料基础信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_JCXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('GPSL','SCSJ','YPID', 'YPBS','SYBH','ZDYSYBH', 'SEWM', 'SFJC', 'XMID', 'YLNF', 'SYNF', 'SYYF', 'SYRQ', 'SFID', 'CSID', 'XZ', 'YYLBID', 'YYXTID', 'YYBWMC', 'YYDDJMC', 'YYDJID', 'YYPZMC', 'YYYSMC', 'PC','JZBH','SJYTID')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
    --导入基础信息表
    SET EXE_SQL='merge INTO SZPX.T_SZPX_YL_JCXXB as A  
    using (
 select A.YPID as YPID,
           ''YL''||RTRIM(CHAR(A.YPID)) AS YPBS,
         A.SFJC,
         A.SEWM,
         CASE WHEN A.SYBH IS NULL THEN VALUE(A.SFJC,''YL'')||''-''||VALUE(A.YYBWMC,''QT'')||''-''||RTRIM(CHAR(A.YPID)) ELSE A.SYBH END AS SYBH,
         A.ZDYSYBH,
         A.XMMC,
         A.XMID,
         CAST (A.YLNF AS DECIMAL(4,0)) AS YLNF,
         CAST (A.SYNF AS DECIMAL(4,0)) AS SYNF,
         CAST (A.SYYF AS DECIMAL(2,0)) AS SYYF,
         CAST (A.SYRQ AS DECIMAL(2,0)) AS SYRQ,
         A.SFMC,
         A.SFID,
         A.CSMC,
         A.CSID,
         A.XZ,
         A.LBMC,
         A.YYLBID,
         A.XTMC,
         A.YYXTID,
         A.YYBWMC,
         A.YYDDJMC,
         A.DJMC,
         A.YYDJID,
         A.YYPZMC,
         A.YYYSMC,
         INT(LTRIM(RTRIM(A.PC))) AS PC,
         A.SCSJ,
         A.SJYTID
        '||V_XZZDRE||'
    from ( SELECT ROW_NUMBER()OVER()+VALUE(B.ID,0) as YPID,
        A.JZXH, 
        A.JZBH, 
        A.SYBH, 
        A.ZDYSYBH,
        A.SEWM,
        A.XMMC, 
        A.YLNF, 
        A.SYNF, 
        A.SYYF, 
        A.SYRQ, 
        A.SFMC, 
        A.CSMC, 
        A.XZ, 
        A.LBMC, 
        A.XTMC, 
        A.YYBWMC, 
        A.YYDDJMC, 
        A.DJMC, 
        A.YYPZMC, 
        A.YYYSMC,
        A.PC,
        A.SFJC,
        A.XMID,
        A.SFID,
        A.CSID,
        A.YYLBID,
        A.YYXTID,
        A.YYDJID,
        A.SCSJ,
        A.SJYTID
        '||V_XZZDR_A||'
        
FROM (SELECT    A.JZXH, 
           A.JZBH, 
           A.SYBH, 
           A.ZDYSYBH,
           A.SEWM,
           A.XMMC, 
           H.YYNF AS YLNF, 
           A.SYNF, 
           A.SYYF, 
           A.SYRQ, 
           A.SFMC, 
           H.YYCDMC AS CSMC, 
           A.XZ, 
           H.YYLBMC AS LBMC, 
           H.YYKBMC AS XTMC, 
           A.YYBWMC, 
           A.YYDDJMC, 
           H.YYDJMC AS DJMC, 
           A.YYPZMC, 
           A.YYYSMC,
           A.PC,
           C.SFJC,
           B.ID AS XMID,
           C.ID AS SFID,
           D.ID AS CSID,
           E.ID AS YYLBID,
           F.ID AS YYXTID,
           G.ID AS YYDJID,
           A.SCSJ,
           M.ID AS SJYTID
           '||V_XZZDR_A||'
      FROM     SZPX.T_SZPX_YL_JCXXB_DL AS A
      LEFT JOIN YYZY.T_YYZY_YYZDBMX AS H
      ON A.SEWM=H.YYDM 
    LEFT JOIN SZPX.T_DIM_SZPX_XMB AS B
      ON A.XMMC=B.XMMC
     AND LCASE(B.XMLX)=''raw''
    LEFT JOIN SZPX.T_DIM_SZPX_YL_SFB AS C
      ON H.DCDMC=C.SFMC
     AND C.JSRQ>CURRENT DATE
     AND C.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_CSB AS D
      ON H.YYCDMC=D.CSMC
     AND H.DCDMC= D.SFMC
     AND D.JSRQ>CURRENT DATE
     AND D.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_LBB AS E
      ON H.YYLBMC=E.LBMC
     AND E.JSRQ>CURRENT DATE
     AND E.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_XTB AS F
      ON H.YYKBMC=F.XTMC
     AND F.JSRQ>CURRENT DATE
     AND F.ZYBJ=1
    LEFT JOIN (SELECT A.LBID,B.LBMC,A.ID,A.DJMC FROM SZPX.T_DIM_SZPX_YL_DJB AS A,SZPX.T_DIM_SZPX_YL_LBB AS B WHERE A.LBID=B.ID AND A.ZYBJ=1 AND B.ZYBJ=1 AND A.JSRQ>CURRENT DATE AND B.JSRQ>CURRENT DATE) AS G
      ON H.YYDJMC=G.DJMC
     AND H.YYLBMC=G.LBMC
     LEFT JOIN SZPX.T_DIM_SZPX_SJYTB AS M
    ON A.SJYTMC = M.SJYTMC
     WHERE A.SEWM IS NOT NULL
      AND A.JZBH='''||V_JZBH||'''
     UNION ALL
     SELECT  A.JZXH, 
           A.JZBH, 
           A.SYBH, 
           A.ZDYSYBH,
           A.SEWM,
           A.XMMC, 
           CAST(A.YLNF AS INTEGER) AS YLNF, 
           A.SYNF, 
           A.SYYF, 
           A.SYRQ, 
           A.SFMC, 
           A.CSMC, 
           A.XZ, 
           A.LBMC, 
           A.XTMC, 
           A.YYBWMC, 
           A.YYDDJMC, 
           A.DJMC, 
           A.YYPZMC, 
           A.YYYSMC,
           A.PC,
           C.SFJC,
           B.ID AS XMID,
           C.ID AS SFID,
           D.ID AS CSID,
           E.ID AS YYLBID,
           F.ID AS YYXTID,
           G.ID AS YYDJID,
            A.SCSJ,
           M.ID AS SJYTID            
            '||V_XZZDR_A||'
     FROM SZPX.T_SZPX_YL_JCXXB_DL AS A
    LEFT JOIN SZPX.T_DIM_SZPX_XMB AS B
      ON A.XMMC=B.XMMC
     AND LCASE(B.XMLX)=''raw''
    LEFT JOIN SZPX.T_DIM_SZPX_YL_SFB AS C
      ON A.SFMC=C.SFMC
     AND C.JSRQ>CURRENT DATE
     AND C.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_CSB AS D
      ON A.CSMC=D.CSMC
     AND (CASE WHEN A.SFMC IS NOT NULL AND A.CSMC IS NOT NULL THEN A.SFMC END )= D.SFMC
     AND D.JSRQ>CURRENT DATE
     AND D.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_LBB AS E
      ON A.LBMC=E.LBMC
     AND E.JSRQ>CURRENT DATE
     AND E.ZYBJ=1
    LEFT JOIN SZPX.T_DIM_SZPX_YL_XTB AS F
      ON A.XTMC=F.XTMC
     AND F.JSRQ>CURRENT DATE
     AND F.ZYBJ=1
    LEFT JOIN (SELECT A.LBID,B.LBMC,A.ID,A.DJMC FROM SZPX.T_DIM_SZPX_YL_DJB AS A,SZPX.T_DIM_SZPX_YL_LBB AS B WHERE A.LBID=B.ID AND A.ZYBJ=1 AND B.ZYBJ=1 AND A.JSRQ>CURRENT DATE AND B.JSRQ>CURRENT DATE) AS G
      ON A.DJMC=G.DJMC
     AND A.LBMC=G.LBMC
     LEFT JOIN SZPX.T_DIM_SZPX_SJYTB AS M
     ON A.SJYTMC = M.SJYTMC
      WHERE A.SEWM IS NULL
      AND A.JZBH='''||V_JZBH||''') AS A
      JOIN (SELECT MAX(YPID) AS ID from SZPX.T_SZPX_YL_JCXXB) AS B 
      ON 1=1
      order by a.sybh,a.jzxh) AS A
      ) as B
   on (A.SYBH=B.SYBH)
 WHEN matched then 
      update set (A.ZDYSYBH,A.SFJC,A.SEWM,A.XMID,A.PC,A.YLNF,A.SYNF, A.SYYF, A.SYRQ, A.SFID, A.CSID, A.XZ, A.YYLBID, A.YYXTID, A.YYBWMC, A.YYDDJMC, A.YYDJID, A.YYPZMC, A.YYYSMC,A.SJYTID '||V_XZZDR_A||') =
                    (case when B.ZDYSYBH is not null then B.ZDYSYBH else a.ZDYSYBH end,B.SFJC,B.SEWM,B.XMID,B.PC,B.YLNF,B.SYNF, B.SYYF, B.SYRQ, B.SFID, B.CSID, B.XZ, B.YYLBID, B.YYXTID, B.YYBWMC, B.YYDDJMC, B.YYDJID, B.YYPZMC, B.YYYSMC,
                 CASE WHEN B.SJYTID IS NULL THEN A.SJYTID ELSE B.SJYTID END  '||V_XZZDR_B||' )
 when not matched then  
           insert(a.jzbh,A.YPID,A.YPBS,A.SYBH,A.ZDYSYBH,A.PC,A.SEWM,A.SFJC,A.XMID,A.YLNF,A.SYNF, A.SYYF, A.SYRQ, A.SFID, A.CSID, A.XZ, A.YYLBID, A.YYXTID, A.YYBWMC, A.YYDDJMC, A.YYDJID, A.YYPZMC, A.YYYSMC,A.SCSJ,A.SJYTID '||V_XZZDR_A||')
           values('''||V_JZBH||''',B.YPID,B.YPBS,B.SYBH,B.ZDYSYBH,B.PC,B.SEWM,B.SFJC,B.XMID,B.YLNF,B.SYNF, B.SYYF, B.SYRQ, B.SFID, B.CSID, B.XZ, B.YYLBID, B.YYXTID, B.YYBWMC, B.YYDDJMC, B.YYDJID, B.YYPZMC, B.YYYSMC,CURRENT TIMESTAMP,VALUE(B.SJYTID,1)'||V_XZZDR_B||')';
    PREPARE s0 FROM EXE_SQL;
    EXECUTE s0;
    COMMIT;
    
    SET OUT_JG='开始导入原料指标表数据';
    
    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_ZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'YTLX', 'YLJD', 'YLWD', 'YLHB', 'YLGY', 'YLGYCS', 'YLGYCL', 'YLYPCL')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
    
    SET EXE_SQL='
    merge INTO SZPX.T_SZPX_YL_ZBB as A  
    using (
    SELECT ID, YPID, SYBH, YTLX, YLJD, YLWD, YLHB, YLGYCL, YLGY, YLGYCS, YLYPCL,SCSJ '||V_XZZDR||' FROM (
SELECT ROW_NUMBER()OVER()+VALUE(bid,0) as ID,
       YPID,
       SYBH,
       YTLX, 
       YLJD, 
       YLWD, 
       YLHB, 
       YLGYCL, 
       YLGY, 
       YLGYCS, 
       YLYPCL,
       SCSJ
       '||V_XZZDR||'
  FROM 
  (
  SELECT b.id as bid,
       C.YPID,
       c.SYBH,
       A.YTLX, 
       A.YLJD, 
       A.YLWD, 
       A.YLHB, 
       A.YLGYCL, 
       A.YLGY, 
       A.YLGYCS, 
       A.YLYPCL,
       A.SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_YL_ZBB_DL as A
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
    ON (A.SYBH=C.SYBH OR A.SYBH=C.ZDYSYBH)
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_ZBB) AS B
    ON 1=1
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh))) AS B
        ON A.SYBH=B.SYBH
      when matched then 
    update set (A.YTLX, A.YLJD, A.YLWD, A.YLHB, A.YLGYCL, A.YLGY, A.YLGYCS, A.YLYPCL '||V_XZZDR_A||')= 
               (B.YTLX, B.YLJD, B.YLWD, B.YLHB, B.YLGYCL, B.YLGY, B.YLGYCS, B.YLYPCL '||V_XZZDR_B||')
      when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.YTLX, A.YLJD, A.YLWD, A.YLHB, A.YLGYCL, A.YLGY, A.YLGYCS, A.YLYPCL,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.YTLX, B.YLJD, B.YLWD, B.YLHB, B.YLGYCL, B.YLGY, B.YLGYCS, B.YLYPCL,CURRENT TIMESTAMP '||V_XZZDR_B||')';

    PREPARE s0 FROM EXE_SQL;
    EXECUTE s0;
    COMMIT;
    
--插入增评吸指标表(老版)数据
SET OUT_JG='开始导入新增原料评吸指标表(老版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

--删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'PXLX', 'GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX', 'MX', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'ZF', 'ZJXX', 'NXX', 'PY', 'XF', 'CY', 'ND', 'JT',  'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ', 'PXR', 'PXSJ', 'BZ')
       AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_YL_PXZBB_XBDL' AND TBCREATOR='SZPX')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='
 INSERT INTO SZPX.T_SZPX_YL_PXZBB (ID, YPID, SYBH, PXLX, PXR, PXSJ, BZ,SCSJ '||V_XZZDR||' )    
          SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
                      YPID,
                 SYBH,
                 PXLX,
                    PXR,
                    PXSJ,
                    BZ,
                 SCSJ
                 '||V_XZZDR||'
              FROM(
            SELECT b.id as  BID,
                      C.YPID,
                 c.SYBH,
                 2 AS PXLX,
                    A.PXR,
                    A.PXSJ,
                    A.BZ,
                 CURRENT TIMESTAMP AS SCSJ
                 '||V_XZZDRE||'
              FROM
            SZPX.T_SZPX_YL_PXZBB_LBDL AS A
       LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_PXZBB) AS B
              ON 1=1
       LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
              ON (A.SYBH=C.SYBH OR A.SYBH=C.ZDYSYBH)
           WHERE 1=1
             AND A.JZBH='''||V_JZBH||'''
             order by a.sybh,a.jzxh) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--插入评吸指标表(新版)数据
SET OUT_JG='开始导入新增原料评吸指标表(新版)数据';
SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'PXLX', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF', 'PXR', 'PXSJ', 'BZ')
       AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_YL_PXZBB_LBDL' AND TBCREATOR='SZPX')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

    
    
SET EXE_SQL='    
 INSERT INTO SZPX.T_SZPX_YL_PXZBB (ID, YPID, SYBH, PXLX, PXR, PXSJ, BZ,SCSJ '||V_XZZDR||' )    
          SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
                      YPID,
                 SYBH,
                 PXLX,
                    PXR,
                    PXSJ,
                    BZ,
                 SCSJ
                 '||V_XZZDR||'
              FROM
         (SELECT b.id as BID,
                      C.YPID,
                 c.SYBH,
                 1 AS PXLX,
                    A.PXR,
                    A.PXSJ,
                    A.BZ,
                 CURRENT TIMESTAMP AS SCSJ
                 '||V_XZZDRE||'
              FROM 
         SZPX.T_SZPX_YL_PXZBB_XBDL AS A
       LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_PXZBB) AS B
              ON 1=1
       LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS C
              ON (A.SYBH=C.SYBH OR A.SYBH=C.ZDYSYBH)
           WHERE 1=1
             AND A.JZBH='''||V_JZBH||'''
             order by a.sybh,a.jzxh) '; 
    
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
--插入光谱信息表数据.
SET OUT_JG='开始导入新增(修改)光谱信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_GPXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'XH', 'GPJWJM', 'GPWJM', 'ZDYGPWJM','YPBS', 'XMMC', 'GPLXID', 'SFCZGPWJ','JCSJ', 'JCJXID', 'JCR', 'SFCZGPWJ', 'BZ')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
    
--光谱文件为空
    SET EXE_SQL='
merge INTO SZPX.T_SZPX_YL_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN
            SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM 
       (SELECT b.id as bid,
             F.YPID,
            F.SYBH,
            C.XH,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_YL_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_YL_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (A.SYBH=F.SYBH OR A.SYBH=F.ZDYSYBH)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, B.BZ '||V_XZZDR_B||')
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--光谱文件不为空
SET EXE_SQL='
merge INTO SZPX.T_SZPX_YL_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN
            SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM, 
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM 
       (SELECT b.id as BID,
             F.YPID,
            F.SYBH,
            C.XH,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_YL_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_YL_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (A.SYBH=F.SYBH OR A.SYBH=F.ZDYSYBH)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NOT NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ '||V_XZZDR_B||') 
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
           

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新基础信息表对应光谱数量
SET OUT_JG='开始更新基础信息表对应光谱数量';
UPDATE SZPX.T_SZPX_YL_JCXXB A
SET A.GPSL=(SELECT COUNT(1) FROM SZPX.T_SZPX_YL_GPXXB B WHERE A.SYBH=B.SYBH)
WHERE A.SYBH IN (SELECT SYBH FROM SZPX.T_SZPX_YL_GPXXB_DL C WHERE C.JZBH=V_JZBH);
--WHERE A.JZBH=V_JZBH;
COMMIT;
SET OUT_JG='更新基础信息表对应光谱数量完成';
--更新完成    
    
    
--插入主成分表数据
SET OUT_JG='开始导入原料主成分表';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_ZCFB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'GPID', 'GPJWJM', 'GPWJM', 'GPMXID', 'JM_YC')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;    
    
--先导入主成份与光谱的关联数据(主成份ID为人工序号生成,因此需特别注意关联表与主成份表的ID生成结果必须完全一致)
set EXE_SQL=' 
 INSERT INTO SZPX.T_SZPX_YL_ZCFGPDYB (ID,ZCFID,GPID) 
with YSSJ(ID,GPWJM) as (
           select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
           A.GPWJM
          from SZPX.T_SZPX_YL_ZCFB_DL AS A
     left join (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_YL_ZCFB) as b
            on 1=1
         where A.JZBH='''||V_JZBH||'''
      Order by JZXH), 
temp(ID,GPWJM) as (
           SELECT ID, GPWJM FROM YSSJ
         UNION ALL
         SELECT ID, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM FROM TEMP AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
CFJG(ID,GPWJM) as (
         SELECT ID,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM FROM TEMP WHERE GPWJM <> '''' )

     select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
             A.ID AS ZCFID,
            C.ID AS GPID
       from CFJG AS A
  left join (select MAX(ID) AS ID FROM SZPX.T_SZPX_YL_ZCFGPDYB) AS B
         on 1=1
  left join SZPX.T_SZPX_YL_GPXXB AS C
         ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPWJM))
         OR LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPJWJM))
      where A.ID IS NOT NULL   --主成份ID
        and C.ID IS NOT NULL   --光谱ID
        and (A.ID,C.ID) NOT IN (SELECT ZCFID,GPID FROM SZPX.T_SZPX_YL_ZCFGPDYB) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--删除临时表数据
DELETE FROM SESSION.ZCFJGB;

--导入主成份临时结果数据
set EXE_SQL='
INSERT INTO SESSION.ZCFJGB(ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ)
 with WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC
          from SZPX.T_SZPX_YL_ZCFB_DL AS A
         where JZBH='''||V_JZBH||'''
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           SELECT GPJWJM,gpwjm,GPMXMC,JM_YC FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC FROM WJMCZ_TMP1 AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
         SELECT GPJWJM,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM,GPMXMC,JM_YC FROM WJMCZ_TMP1 WHERE GPWJM <> '''' 
)
,
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, B.GPWJM||''/''))),''</A>'',''''),''<A>'','''') as GPWJM
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_YL_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by a.gpjwjm,a.GPMXMC,a.JM_YC)
)
,
WJMCZ2(GPJWJM,GPMXMC,JM_YC,YPID) as (
      select  GPJWJM,GPMXMC,JM_YC,LEFT(ypid,LENGTH(ypid)-1) ypid  from (
      SELECT GPJWJM,GPMXMC,JM_YC,REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, trim(char(ypid))||''/''))),''</A>'',''''),''<A>'','''') ypid from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                b.ypid
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_YL_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by b.ypid,a.gpjwjm,a.GPMXMC,a.JM_YC)
       group by gpjwjm,GPMXMC,JM_YC)
),
WJMCZ (GPJWJM,GPWJM,GPMXMC,JM_YC,YPID) as (
      select a.GPJWJM,a.GPWJM,a.GPMXMC,a.JM_YC,b.YPID
      from WJMCZ1 a join WJMCZ2 b
      on a.gpjwjm=b.gpjwjm and a.gpmxmc=b.gpmxmc and a.jm_yc=b.jm_yc
)
,
RESULTS AS (
        select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
               GPJWJM, 
               GPWJM,
               GPMXMC,
               GPMXID, 
               JM_YC,
               YPID,
               SCSJ
              
             from 
          (select b.id as bid,
               A.GPWJM as GPJWJM, 
               D.GPWJM,
               D.YPID,
               A.GPMXMC,
               C.ID AS GPMXID, 
               A.JM_YC,
               A.SCSJ
             
             from
          SZPX.T_SZPX_YL_ZCFB_DL AS A
     left join (select max(ID) AS ID from SZPX.T_SZPX_YL_ZCFB) AS B
            on 1=1
     left join SZPX.T_DIM_SZPX_GPMXB AS C
            on A.GPMXMC=C.GPMXMC
           and C.ZYBJ=1
     left join WJMCZ AS D
            ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(D.GPJWJM))
           and A.GPMXMC=D.GPMXMC
           AND A.JM_YC=D.JM_YC
         where JZBH='''||V_JZBH||'''
         order by a.jzxh))
SELECT ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ FROM RESULTS';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
--导入主成份表数据GPWJM,GPMXMC,JM_YC三项全部相同的更新，不同的插入
set EXE_SQL='
MERGE INTO SZPX.T_SZPX_YL_ZCFB AS A
USING(
    SELECT b.id,a.gpwjm as gpjwjm,b.gpwjm as gpwjm,b.ypid,a.jm_yc,b.gpmxid,B.SCSJ '||V_XZZDRE||'
    FROM SESSION.ZCFJGB b
    JOIN 
    SZPX.T_SZPX_YL_ZCFB_DL AS a
    ON A.gpwjm=b.gpjwjm
    and a.gpmxmc=b.gpmxmc
    and a.jm_yc=b.jm_yc
    where a.JZBH='''||V_JZBH||'''
    ) AS B
ON (a.gpwjm,a.jm_yc,a.gpmxid)=(b.gpwjm,b.jm_yc,b.gpmxid)
when matched then
update set(A.JM_YC '||V_XZZDR_A||')=(B.JM_YC '||V_XZZDR_B||')
when not matched then 
insert (A.ID, A.GPJWJM,A.GPWJM,A.YPID, A.GPMXID, A.JM_YC,A.SCSJ '||V_XZZDR_A||')
values(B.ID,B.GPJWJM,B.GPWJM,B.YPID,B.GPMXID,B.JM_YC,CURRENT TIMESTAMP '||V_XZZDR_B||')'
;
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
--插入检测指标表数据
SET OUT_JG='开始导入检测指标表';
SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_YL_JCZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'JCLXID', 'GPWJM', 'GPJWJM', 'GPID', 'YJ', 'ZT', 'HYT', 'ZD', 'ZJ', 'ZL', 'JCR', 'JCJXID', 'JCSJ', 'BZ' )
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;    

--插入检测类型为 光谱检测 时关联到光谱信息表    
set EXE_SQL='
INSERT INTO SZPX.T_SZPX_YL_JCZBB (ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||')
  SELECT ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||' FROM (
       select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
              JCLXMC,
              JCLXID,
              GPID,
              SYBH,
              GPJWJM,
              YPID,
              GPWJM, 
              JCR,
              JCJXMC,
              JCJXID, 
              JCSJ, 
              BZ, 
              YJ, 
              ZT, 
              HYT, 
              ZD, 
              ZJ, 
              ZL,
              SCSJ
              '||V_XZZDR||'
         FROM 
         (select b.id as bid,
              A.JCLXMC,
              C.ID AS JCLXID,
              E.ID AS GPID,
              A.GPWJM as GPJWJM,
              f.sybh as SYBH,
              case when a.JCLXMC  like ''%光谱%'' then e.ypid else f.ypid end as ypid,
              E.GPWJM, 
              A.JCR,
              A.JCJXMC,
              D.ID AS JCJXID, 
              A.JCSJ, 
              A.BZ, 
              CURRENT TIMESTAMP AS SCSJ,
              DOUBLE(A.YJ)  AS YJ, 
              DOUBLE(A.ZT)  AS ZT, 
              DOUBLE(A.HYT) AS HYT, 
              DOUBLE(A.ZD)  AS ZD, 
              DOUBLE(A.ZJ)  AS ZJ, 
              DOUBLE(A.ZL)  AS ZL
              '||V_XZZDRE||'
         FROM
         SZPX.T_SZPX_YL_JCZBB_DL AS A
    LEFT JOIN (select max(ID) AS ID from SZPX.T_SZPX_YL_JCZBB) AS B
           ON 1=1
    LEFT JOIN SZPX.T_DIM_SZPX_JCLXB AS C
           ON A.JCLXMC=C.JCLXMC
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS D
           ON A.JCJXMC=D.JCJXMC
    LEFT JOIN SZPX.T_SZPX_YL_GPXXB AS E
           ON A.GPWJM=E.GPWJM
           OR A.GPWJM=E.GPJWJM
    LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_YL_JCXXB t 
              left join SZPX.T_SZPX_YL_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
           ON (A.SYBH=F.SYBH OR A.SYBH=F.ZDYSYBH)
        WHERE A.JZBH='''||V_JZBH||''' 
        order by e.ypid,a.gpwjm,f.sybh,a.jzxh)) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新光谱旧文件名为null,避免插入主成份，检测指标表时由于gpjwjm出现问题
update SZPX.T_SZPX_YL_GPXXB
set gpjwjm=null;
commit;

--原料数据加载完成
    END IF ;
    SET OUT_JG='原料数据导入完成';
    
    IF V_XMLX='product' THEN 
    
    SET OUT_JG='开始导入产品基础信息表数据';
    
    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_JCXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('GPSL','SCSJ','YPID', 'YPBS', 'PPJC', 'SYBH','ZDYSYBH', 'XMID', 'THTXBS','XHTXBS', 'SYNF', 'SYYF', 'SYRQ', 'SCCJ', 'CPPHID', 'CPYHID', 'CPGG', 'CPNF', 'CPJLID', 'HBJY', 'HBCO', 'HBYJ','JZBH')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
     
SET EXE_SQL='
merge into SZPX.T_SZPX_CP_JCXXB as A  
    using (
    SELECT A.YPID as YPID,
           ''CP''||RTRIM(CHAR(A.YPID)) AS YPBS,
           CASE WHEN SYBH IS NULL THEN VALUE(A.PPJC,''QT'')||RTRIM(CHAR(A.YPID)) ELSE A.SYBH END SYBH,
           A.ZDYSYBH,
           A.JZBH,
           A.XMMC,
           A.XMID,
           A.XHTXBS,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.PPJC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.CPGG,
           A.CPNF,
           A.JLMC,
           A.CPJLID,
           A.HBJY,
           A.HBYJ,
           A.HBCO,
           A.SCSJ
           '||V_XZZDRE||'
      FROM (
    SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as YPID,
           A.JZBH,
           A.SYBH,
           A.ZDYSYBH,
           A.XMMC,
           A.XMID,
           A.XHTXBS,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.PPJC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.CPGG,
           A.CPNF,
           A.JLMC,
           A.CPJLID,
           A.HBJY,
           A.HBYJ,
           A.HBCO,
           A.SCSJ
           '||V_XZZDRE||'
      FROM(
      SELECT b.id as bid,
           A.JZBH,
           A.SYBH,
           A.ZDYSYBH,
           A.XMMC,
           A.XMID,
           A.XHTXBS,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.PPJC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.CPGG,
           A.CPNF,
           A.JLMC,
           A.CPJLID,
           A.HBJY,
           A.HBYJ,
           A.HBCO,
           A.SCSJ
           '||V_XZZDRE||'
      FROM
      (
SELECT A.JZBH,
       A.JZXH,
       A.SYBH,
       A.ZDYSYBH,
       A.XMMC,
       B.ID AS XMID, 
       A.XHTXBS,
       CAST (CHAR(A.SYNF,4) AS DECIMAL(4,0)) AS SYNF, 
       CAST (CHAR(A.SYYF,4) AS DECIMAL(2,0)) AS SYYF,
       CAST (CHAR(A.SYRQ,4) AS DECIMAL(2,0)) AS SYRQ,
       A.SCCJ, 
       C.PPMC,
       VALUE(C.PPJC,''CP'') AS PPJC,
       C.ID AS CPPHID,
       A.YHMC,
       D.ID AS CPYHID, 
       A.CPGG,
       CAST (CHAR(A.CPNF,4) AS DECIMAL(4,0)) AS CPNF, 
       E.JLMC,
       E.ID AS CPJLID,
       A.SCSJ,
       CAST(A.HBJY AS DECIMAL(18,2)) AS HBJY,
       CAST(A.HBCO AS DECIMAL(18,2)) AS HBCO,
       CAST(A.HBYJ AS DECIMAL(18,2)) AS HBYJ
       '||V_XZZDRE||'
  FROM SZPX.T_SZPX_CP_JCXXB_DL      AS A
  LEFT JOIN SZPX.T_DIM_SZPX_XMB     AS B
    ON A.XMMC = B.XMMC
   AND B.XMLX=''product''
  LEFT JOIN SZPX.T_DIM_SZPX_CP_PPB  AS C
    ON A.PPMC=C.PPMC
   AND C.ZYBJ=1
   AND C.JSRQ>CURRENT DATE
  LEFT JOIN SZPX.T_DIM_SZPX_CP_YHB  AS D
    ON A.YHMC = D.YHMC
   AND D.ZYBJ = 1
  LEFT JOIN SZPX.T_DIM_SZPX_CP_JLB  AS E
    ON A.JLMC=E.JLMC
   AND E.ZYBJ=1 
   AND E.JSRQ>CURRENT DATE
 WHERE A.JZBH='''||V_JZBH||''' 
   AND A.XHTXBS IS NULL
UNION ALL
SELECT A.JZBH,
       A.JZXH,
       A.SYBH,
       A.ZDYSYBH,
       A.XMMC,
       B.ID AS XMID, 
       A.XHTXBS,
       CAST (CHAR(A.SYNF,4) AS DECIMAL(4,0)) AS SYNF, 
       CAST (CHAR(A.SYYF,4) AS DECIMAL(2,0)) AS SYYF,
       CAST (CHAR(A.SYRQ,4) AS DECIMAL(2,0)) AS SYRQ,
       C.SCQYMC as SCCJ, 
       C.PPMC,
       VALUE(C.PPJC,''CP'') AS PPJC,
       C.PPDM AS CPPHID,
       A.YHMC,
       D.ID AS CPYHID, 
       C.GJJJYMC as CPGG,
       CAST (CHAR(A.CPNF,4) AS DECIMAL(4,0)) AS CPNF, 
       C.JLMC,
       C.JLDM  AS CPJLID,
       A.SCSJ,
       C.JYHL  AS HBJY, 
       C.YQCOL AS HBCO,
       C.YQYJL AS HBYJ
       '||V_XZZDRE||'
  FROM SZPX.T_SZPX_CP_JCXXB_DL      AS A
  LEFT JOIN SZPX.T_DIM_SZPX_XMB     AS B
    ON A.XMMC = B.XMMC
   AND B.XMLX=''product''
  LEFT JOIN SZPX.T_SZPX_CP_GJJJY    AS C
    ON A.XHTXBS=C.XHTXBS
  LEFT JOIN SZPX.T_DIM_SZPX_CP_YHB  AS D
    ON A.YHMC = D.YHMC
   AND D.ZYBJ = 1
 WHERE A.JZBH='''||V_JZBH||''' 
   AND A.XHTXBS IS NOT NULL) AS A
 LEFT JOIN (SELECT MAX(YPID) AS ID FROM SZPX.T_SZPX_CP_JCXXB) AS B
        ON 1=1
        order by a.sybh,a.jzxh) AS A
        ) AS A 
    ) AS B
   ON (A.SYBH=B.SYBH)
  when matched then 
       update set (A.ZDYSYBH,A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.SCCJ,A.XHTXBS, A.CPPHID, A.CPYHID, A.CPGG, A.CPNF, A.CPJLID , A.HBJY, A.HBCO, A.HBYJ  '||V_XZZDR_A||')= 
                    (case when B.ZDYSYBH is not null then B.ZDYSYBH else A.ZDYSYBH end,B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.SCCJ,B.XHTXBS, B.CPPHID, B.CPYHID, B.CPGG, B.CPNF, B.CPJLID , B.HBJY, B.HBCO, B.HBYJ  '||V_XZZDR_B||')
  when not matched then
            insert(a.jzbh,A.YPID, A.YPBS, A.SYBH,A.ZDYSYBH, A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.SCCJ,A.XHTXBS, A.CPPHID, A.CPYHID, A.CPGG, A.CPNF, A.CPJLID, A.HBJY, A.HBCO, A.HBYJ,A.SCSJ  '||V_XZZDR_A||')
            values('''||V_JZBH||''' ,B.YPID, B.YPBS, B.SYBH,B.ZDYSYBH, B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.SCCJ,B.XHTXBS, B.CPPHID, B.CPYHID, B.CPGG, B.CPNF, B.CPJLID, B.HBJY, B.HBCO, B.HBYJ,CURRENT TIMESTAMP  '||V_XZZDR_B||') ';

    PREPARE s0 FROM EXE_SQL;
    EXECUTE s0;
    COMMIT;
    
--插入增评吸指标表(老版)数据
SET OUT_JG='(产品)开始导入产品评吸指标表(老版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
        AND NAME NOT IN ( 'SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX', 'MX', 'ZF', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'ZJXX', 'NXX', 'PY', 'XF', 'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ')
        AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_CP_PXZBB_XBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;    

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM 
  (SELECT b.id as bid,
       C.YPID,
       c.SYBH,
       2 AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ ,
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_CP_PXZBB_LBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CP_PXZBB) as b
         ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CP_JCXXB t 
              left join SZPX.T_SZPX_CP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
         on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
    
--插入增评吸指标表(新版)数据
SET OUT_JG='(产品)开始导入新增评吸指标表(新版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
         AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF')
         AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_CP_PXZBB_LBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM (SELECT b.id as bid,
       C.YPID,
       c.SYBH,
       1    AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ ,
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_CP_PXZBB_XBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CP_PXZBB) as b
    ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CP_JCXXB t 
              left join SZPX.T_SZPX_CP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
    on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--插入光谱信息表数据.
SET OUT_JG='(产品)开始插入产品光谱信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_GPXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'XH', 'GPJWJM', 'GPWJM', 'ZDYGPWJM','YPBS', 'XMMC', 'GPLXID', 'SFCZGPWJ','JCSJ', 'JCJXID', 'JCR', 'SFCZGPWJ', 'BZ')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;        
    
--光谱文件名不为空
SET EXE_SQL='
merge INTO SZPX.T_SZPX_CP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN
            SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM 
       (SELECT b.id as bid,
             F.YPID,
            F.SYBH,
            c.xh,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_CP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_CP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CP_JCXXB t 
              left join SZPX.T_SZPX_CP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=F.sybh or a.sybh=F.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NOT NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ '||V_XZZDR_B||')
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--光谱文件名为空    
SET EXE_SQL='
merge INTO SZPX.T_SZPX_CP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN 
            SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM(
           SELECT b.id as BID,
             F.YPID,
            f.SYBH,
            C.XH,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_CP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_CP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CP_JCXXB t 
              left join SZPX.T_SZPX_CP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR,  A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR,  B.BZ '||V_XZZDR_B||')   
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新基础信息表对应光谱数量
SET OUT_JG='开始更新基础信息表对应光谱数量';
UPDATE SZPX.T_SZPX_CP_JCXXB A
SET A.GPSL=(SELECT COUNT(1) FROM SZPX.T_SZPX_CP_GPXXB B WHERE A.SYBH=B.SYBH)
WHERE A.SYBH IN (SELECT SYBH FROM SZPX.T_SZPX_CP_GPXXB_DL C WHERE C.JZBH=V_JZBH);
--WHERE A.JZBH=V_JZBH;
COMMIT;
SET OUT_JG='更新基础信息表对应光谱数量完成';
--更新完成            
    
--插入主成分表数据
SET OUT_JG='开始导入产品主成分表';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_ZCFB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'GPID', 'GPJWJM', 'GPWJM', 'GPMXID', 'JM_YC')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--先导入主成份与光谱的关联数据(主成份ID为人工序号生成,因此需特别注意关联表与主成份表的ID生成结果必须完全一致)
set EXE_SQL=' 
 INSERT INTO SZPX.T_SZPX_CP_ZCFGPDYB (ID,ZCFID,GPID) 
with YSSJ(ID,GPWJM) as (
           select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
           A.GPWJM
          from SZPX.T_SZPX_CP_ZCFB_DL AS A
     left join (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CP_ZCFB) as b
            on 1=1
         where A.JZBH='''||V_JZBH||'''
      Order by JZXH), 
temp(ID,GPWJM) as (
           SELECT ID, GPWJM FROM YSSJ
         UNION ALL
         SELECT ID, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM FROM TEMP AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
CFJG(ID,GPWJM) as (
         SELECT ID,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM FROM TEMP WHERE GPWJM <> '''' )

     select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
             A.ID AS ZCFID,
            C.ID AS GPID
       from CFJG AS A
  left join (select MAX(ID) AS ID FROM SZPX.T_SZPX_CP_ZCFGPDYB) AS B
         on 1=1
  left join SZPX.T_SZPX_CP_GPXXB AS C
         ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPWJM))
         OR LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPJWJM))
      where A.ID IS NOT NULL   --主成份ID
        and C.ID IS NOT NULL   --光谱ID
        and (A.ID,C.ID) NOT IN (SELECT ZCFID,GPID FROM SZPX.T_SZPX_CP_ZCFGPDYB) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--删除临时表数据
DELETE FROM SESSION.ZCFJGB;

--导入主成份临时结果数据
set EXE_SQL='
INSERT INTO SESSION.ZCFJGB(ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ)
 with WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC
          from SZPX.T_SZPX_CP_ZCFB_DL AS A
         where JZBH='''||V_JZBH||'''
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           SELECT GPJWJM,gpwjm,GPMXMC,JM_YC FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC FROM WJMCZ_TMP1 AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
         SELECT GPJWJM,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM,GPMXMC,JM_YC FROM WJMCZ_TMP1 WHERE GPWJM <> '''' 
)
,
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, B.GPWJM||''/''))),''</A>'',''''),''<A>'','''') as GPWJM
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_CP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by a.gpjwjm,a.GPMXMC,a.JM_YC)
)
,
WJMCZ2(GPJWJM,GPMXMC,JM_YC,YPID) as (
      select  GPJWJM,GPMXMC,JM_YC,LEFT(ypid,LENGTH(ypid)-1) ypid  from (
      SELECT GPJWJM,GPMXMC,JM_YC,REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, trim(char(ypid))||''/''))),''</A>'',''''),''<A>'','''') ypid from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                b.ypid
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_CP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by b.ypid,a.gpjwjm,a.GPMXMC,a.JM_YC)
       group by gpjwjm,GPMXMC,JM_YC)
),
WJMCZ (GPJWJM,GPWJM,GPMXMC,JM_YC,YPID) as (
      select a.GPJWJM,a.GPWJM,a.GPMXMC,a.JM_YC,b.YPID
      from WJMCZ1 a join WJMCZ2 b
      on a.gpjwjm=b.gpjwjm and a.gpmxmc=b.gpmxmc and a.jm_yc=b.jm_yc
)
,
RESULTS AS (
        select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
               GPJWJM, 
               GPWJM,
               YPID,
               GPMXMC,
               GPMXID, 
               JM_YC,
               SCSJ
              
             from 
          (select b.id as bid,
               A.GPWJM as GPJWJM, 
               D.GPWJM,
               D.YPID,
               A.GPMXMC,
               C.ID AS GPMXID, 
               A.JM_YC,
               A.SCSJ
             
             from
          SZPX.T_SZPX_CP_ZCFB_DL AS A
     left join (select max(ID) AS ID from SZPX.T_SZPX_CP_ZCFB) AS B
            on 1=1
     left join SZPX.T_DIM_SZPX_GPMXB AS C
            on A.GPMXMC=C.GPMXMC
           and C.ZYBJ=1
     left join WJMCZ AS D
            ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(D.GPJWJM))
           and A.GPMXMC=D.GPMXMC
           AND A.JM_YC=D.JM_YC
         where JZBH='''||V_JZBH||'''
         order by a.jzxh))
SELECT ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ FROM RESULTS';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
--导入主成份表数据GPWJM,GPMXMC,JM_YC三项全部相同的更新，不同的插入
set EXE_SQL='
MERGE INTO SZPX.T_SZPX_CP_ZCFB AS A
USING(
    SELECT b.id,a.gpwjm as gpjwjm,b.gpwjm as gpwjm,b.ypid,a.jm_yc,b.gpmxid,B.SCSJ '||V_XZZDRE||'
    FROM SESSION.ZCFJGB b
    JOIN 
    SZPX.T_SZPX_CP_ZCFB_DL AS a
    ON A.gpwjm=b.gpjwjm
    and a.gpmxmc=b.gpmxmc
    and a.jm_yc=b.jm_yc
    where a.JZBH='''||V_JZBH||'''
    ) AS B
ON (a.gpwjm,a.jm_yc,a.gpmxid)=(b.gpwjm,b.jm_yc,b.gpmxid)
when matched then
update set(A.JM_YC '||V_XZZDR_A||')=(B.JM_YC '||V_XZZDR_B||')
when not matched then 
insert (A.ID, A.GPJWJM,A.GPWJM,A.YPID, A.GPMXID, A.JM_YC,A.SCSJ '||V_XZZDR_A||')
values(B.ID,B.GPJWJM,B.GPWJM,B.YPID,B.GPMXID,B.JM_YC,CURRENT TIMESTAMP '||V_XZZDR_B||')'
;
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
--插入检测指标表数据
SET OUT_JG='开始导入检测指标表';
SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CP_JCZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'JCLXID', 'GPWJM', 'GPJWJM', 'GPID', 'YJ', 'ZT', 'HYT', 'ZD', 'ZJ', 'ZL', 'JCR', 'JCJXID', 'JCSJ', 'BZ' )
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--插入检测类型为 光谱检测 时关联到光谱信息表
set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CP_JCZBB (ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||')
  SELECT ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||' FROM (
       select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
              JCLXMC,
              JCLXID,
              GPID,
              GPJWJM,
              SYBH,
              YPID,
              GPWJM, 
              JCR,
              JCJXMC,
              JCJXID, 
              JCSJ, 
              BZ, 
              YJ, 
              ZT, 
              HYT, 
              ZD, 
              ZJ, 
              ZL,
              SCSJ
              '||V_XZZDR||'
         FROM (select b.id as BID,
              A.JCLXMC,
               f.sybh as SYBH,
              C.ID AS JCLXID,
              E.ID AS GPID,
              A.GPWJM as GPJWJM,
              case when a.JCLXMC  like ''%光谱%'' then e.ypid else f.ypid end as ypid,
              E.GPWJM, 
              A.JCR,
              A.JCJXMC,
              D.ID AS JCJXID, 
              A.JCSJ, 
              A.BZ, 
              CURRENT TIMESTAMP AS SCSJ,
              DOUBLE(A.YJ)  AS YJ, 
              DOUBLE(A.ZT)  AS ZT, 
              DOUBLE(A.HYT) AS HYT, 
              DOUBLE(A.ZD)  AS ZD, 
              DOUBLE(A.ZJ)  AS ZJ, 
              DOUBLE(A.ZL)  AS ZL
              '||V_XZZDRE||'
         FROM
         SZPX.T_SZPX_CP_JCZBB_DL AS A
    LEFT JOIN (select max(ID) AS ID from SZPX.T_SZPX_CP_JCZBB) AS B
           ON 1=1
    LEFT JOIN SZPX.T_DIM_SZPX_JCLXB AS C
           ON A.JCLXMC=C.JCLXMC
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS D
           ON A.JCJXMC=D.JCJXMC
    LEFT JOIN SZPX.T_SZPX_CP_GPXXB AS E
           ON A.GPWJM=E.GPWJM
           OR A.GPWJM=E.GPJWJM
    LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CP_JCXXB t 
              left join SZPX.T_SZPX_CP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
           ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
        WHERE A.JZBH='''||V_JZBH||''' 
        order by e.ypid,a.gpwjm,f.sybh,a.jzxh)) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
        
--更新光谱旧文件名为null,避免插入主成份，检测指标表时由于gpjwjm出现问题
update SZPX.T_SZPX_CP_GPXXB
set gpjwjm=null;
commit;

    SET OUT_JG='产品数据导入完成';
    
    END IF ;
    
    IF V_XMLX='medium' THEN 
    SET OUT_JG='开始导入掺配品数据';
    
        SET OUT_JG='(掺配品)开始更新基础信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_JCXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
      AND NAME NOT IN ('GPSL','SCSJ','YPID', 'SYBH','ZDYSYBH', 'YPBS', 'XMID', 'SYNF', 'SYYF', 'SYRQ', 'CPPLXID', 'CPPZLXMC', 'CPPZTMC', 'CPPBL', 'CPPGY', 'CPPGD', 'CPPCL', 'CPPCLGY', 'CPPCLCS','JZBH')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD; 
    
    SET EXE_SQL='
    merge into SZPX.T_SZPX_CPP_JCXXB as A  
    using (
    SELECT A.YPID AS YPID,
           A.JZBH,
           A.JZXH,
           ''CPP''||RTRIM(CHAR(A.YPID)) AS YPBS,
           CASE WHEN A.SYBH IS NULL THEN VALUE(A.CPPLXJC,''CPP'')||RTRIM(CHAR(A.YPID)) ELSE A.SYBH END AS SYBH,
           A.ZDYSYBH,
           A.XMMC,
           A.XMID,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.LXMC,
           A.CPPLXID,
           A.CPPZLXMC,
           A.CPPZTMC,
           A.CPPBL,
           A.CPPGY,
           A.CPPGD,        
           A.CPPCL,        
           A.CPPCLGY,        
           A.CPPCLCS,
           A.SCSJ
           '||V_XZZDRE||'
      FROM (           
    select ROW_NUMBER()OVER()+VALUE(BID,0) as YPID,
           JZBH, 
           JZXH, 
           XMMC, 
           XMID,
           SYBH, 
           ZDYSYBH,
           SYNF, 
           SYYF, 
           SYRQ, 
           LXMC, 
           CPPLXJC,
           CPPLXID,       
           CPPZLXMC,       
           CPPZTMC,        
           CPPBL,        
           CPPGY,        
           CPPGD,        
           CPPCL,        
           CPPCLGY,        
           CPPCLCS,
           SCSJ
           '||V_XZZDR||'
      FROM (select b.id as bid,
           A.JZBH, 
           A.JZXH, 
           A.XMMC, 
           D.ID AS XMID,
           A.SYBH, 
           A.ZDYSYBH,
           CAST (CHAR(SYNF,4) AS DECIMAL(4,0)) AS SYNF, 
           CAST (CHAR(SYYF,4) AS DECIMAL(2,0)) AS SYYF, 
           CAST (CHAR(SYRQ,4) AS DECIMAL(2,0)) AS SYRQ, 
           A.LXMC, 
           C.LXJC AS CPPLXJC,
           C.ID AS CPPLXID,       
           A.CPPZLXMC,       
           A.CPPZTMC,        
           A.CPPBL,        
           A.CPPGY,        
           A.CPPGD,        
           A.CPPCL,        
           A.CPPCLGY,        
           A.CPPCLCS,
           A.SCSJ
           '||V_XZZDRE||'
      FROM
      SZPX.T_SZPX_CPP_JCXXB_DL      AS A
      LEFT JOIN (SELECT MAX(YPID) AS ID FROM SZPX.T_SZPX_CPP_JCXXB) AS B
        ON 1=1
      LEFT JOIN SZPX.T_DIM_SZPX_XMB      AS D
        ON A.XMMC = D.XMMC
       AND D.XMLX = ''medium''
      LEFT JOIN SZPX.T_DIM_SZPX_CPP_LXB  AS C
        ON A.LXMC = C.LXMC
       AND C.ZYBJ = 1
     WHERE A.JZBH = '''||V_JZBH||''' 
     order by a.sybh,a.jzxh)
     ) AS A ) AS B
        ON (A.SYBH)=(B.SYBH)
      when matched then 
    update set (A.ZDYSYBH,A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.CPPLXID, A.CPPZLXMC, A.CPPZTMC, A.CPPBL, A.CPPGY, A.CPPGD, A.CPPCL, A.CPPCLGY, A.CPPCLCS '||V_XZZDR_A||')= 
               (case when B.ZDYSYBH is not null then B.ZDYSYBH else A .ZDYSYBH end,B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.CPPLXID, B.CPPZLXMC, B.CPPZTMC, B.CPPBL, B.CPPGY, B.CPPGD, B.CPPCL, B.CPPCLGY, B.CPPCLCS '||V_XZZDR_B||')
      when not matched then
         insert(a.jzbh,A.YPID, A.YPBS, A.SYBH,A.ZDYSYBH, A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.CPPLXID, A.CPPZLXMC, A.CPPZTMC, A.CPPBL, A.CPPGY, A.CPPGD, A.CPPCL, A.CPPCLGY, A.CPPCLCS,A.SCSJ '||V_XZZDR_A||')
         values('''||V_JZBH||''',B.YPID, B.YPBS, B.SYBH,B.ZDYSYBH, B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.CPPLXID, B.CPPZLXMC, B.CPPZTMC, B.CPPBL, B.CPPGY, B.CPPGD, B.CPPCL, B.CPPCLGY, B.CPPCLCS,CURRENT TIMESTAMP '||V_XZZDR_B||') ';

    PREPARE s0 FROM EXE_SQL;
    EXECUTE s0;
    COMMIT;
    
    --插入增评吸指标表(老版)数据
SET OUT_JG='(掺配品)开始导入掺配品评吸指标表(老版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ( 'SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX', 'MX', 'ZF', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'ZJXX', 'NXX', 'PY', 'XF', 'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ')
       AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_CPP_PXZBB_XBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD; 

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CPP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM (SELECT b.id as BID,
       C.YPID,
       C.SYBH,
       2 AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ, 
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_CPP_PXZBB_LBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_PXZBB) as b
         ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CPP_JCXXB t 
              left join SZPX.T_SZPX_CPP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
         on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
    
--插入增评吸指标表(新版)数据
SET OUT_JG='(掺配品)开始导入新增评吸指标表(新版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF')
           AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_CPP_PXZBB_LBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CPP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM (SELECT b.id as BID,
       C.YPID,
       c.SYBH,
       1    AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ ,
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_CPP_PXZBB_XBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_PXZBB) as b
    ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CPP_JCXXB t 
              left join SZPX.T_SZPX_CPP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
    on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
    --插入光谱信息表数据.
SET OUT_JG='(掺配品)开始插入掺配品光谱信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_GPXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'XH', 'GPJWJM', 'GPWJM','ZDYGPWJM', 'YPBS', 'XMMC', 'GPLXID', 'SFCZGPWJ','JCSJ', 'JCJXID', 'JCR', 'SFCZGPWJ', 'BZ')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
--光谱文件为空
    SET EXE_SQL='
merge INTO SZPX.T_SZPX_CPP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN
            SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM (SELECT b.id as BID,
             F.YPID,
            F.SYBH,
            c.xh,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_CPP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_CPP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CPP_JCXXB t 
              left join SZPX.T_SZPX_CPP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.BZ '||V_XZZDR_A||')= 
                 (CASE WHEN B.ZDYGPWJM IS NOT NULL THEN B.ZDYGPWJM ELSE A.ZDYGPWJM END ,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, B.BZ '||V_XZZDR_B||')
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--光谱文件不为空
    SET EXE_SQL='
merge INTO SZPX.T_SZPX_CPP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN 
            SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''_''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM(
             SELECT b.id as BID,
             F.YPID,
            F.SYBH,
            C.XH,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_CPP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_CPP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CPP_JCXXB t 
              left join SZPX.T_SZPX_CPP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NOT NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ '||V_XZZDR_A||')= 
                 (CASE WHEN B.ZDYGPWJM IS NOT NULL THEN B.ZDYGPWJM ELSE A.ZDYGPWJM END,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ '||V_XZZDR_B||') 
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新基础信息表对应光谱数量
SET OUT_JG='开始更新基础信息表对应光谱数量';
UPDATE SZPX.T_SZPX_CPP_JCXXB A
SET A.GPSL=(SELECT COUNT(1) FROM SZPX.T_SZPX_CPP_GPXXB B WHERE A.SYBH=B.SYBH)
WHERE A.SYBH IN (SELECT SYBH FROM SZPX.T_SZPX_CPP_GPXXB_DL C WHERE C.JZBH=V_JZBH);
--WHERE A.JZBH=V_JZBH;
COMMIT;
SET OUT_JG='更新基础信息表对应光谱数量完成';
--更新完成        
    
    --插入主成分表数据
SET OUT_JG='开始导入掺配品主成分表';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_ZCFB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'GPID', 'GPJWJM', 'GPWJM', 'GPMXID', 'JM_YC')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--先导入主成份与光谱的关联数据(主成份ID为人工序号生成,因此需特别注意关联表与主成份表的ID生成结果必须完全一致)
set EXE_SQL=' 
 INSERT INTO SZPX.T_SZPX_CPP_ZCFGPDYB (ID,ZCFID,GPID) 
with YSSJ(ID,GPWJM) as (
           select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
           A.GPWJM
          from SZPX.T_SZPX_CPP_ZCFB_DL AS A
     left join (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_ZCFB) as b
            on 1=1
         where A.JZBH='''||V_JZBH||'''
      Order by JZXH), 
temp(ID,GPWJM) as (
           SELECT ID, GPWJM FROM YSSJ
         UNION ALL
         SELECT ID, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM FROM TEMP AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
CFJG(ID,GPWJM) as (
         SELECT ID,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM FROM TEMP WHERE GPWJM <> '''' )

     select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
             A.ID AS ZCFID,
            C.ID AS GPID
       from CFJG AS A
  left join (select MAX(ID) AS ID FROM SZPX.T_SZPX_CPP_ZCFGPDYB) AS B
         on 1=1
  left join SZPX.T_SZPX_CPP_GPXXB AS C
         ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPWJM))
         OR LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPJWJM))
      where A.ID IS NOT NULL   --主成份ID
        and C.ID IS NOT NULL   --光谱ID
        and (A.ID,C.ID) NOT IN (SELECT ZCFID,GPID FROM SZPX.T_SZPX_CPP_ZCFGPDYB) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--删除临时表数据
DELETE FROM SESSION.ZCFJGB;

--导入主成份临时结果数据
set EXE_SQL='
INSERT INTO SESSION.ZCFJGB(ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ)
 with WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC
          from SZPX.T_SZPX_CPP_ZCFB_DL AS A
         where JZBH='''||V_JZBH||'''
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           SELECT GPJWJM,gpwjm,GPMXMC,JM_YC FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC FROM WJMCZ_TMP1 AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
         SELECT GPJWJM,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM,GPMXMC,JM_YC FROM WJMCZ_TMP1 WHERE GPWJM <> '''' 
)
,
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, B.GPWJM||''/''))),''</A>'',''''),''<A>'','''') as GPWJM
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_CPP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by a.gpjwjm,a.GPMXMC,a.JM_YC)
)
,
WJMCZ2(GPJWJM,GPMXMC,JM_YC,YPID) as (
      select  GPJWJM,GPMXMC,JM_YC,LEFT(ypid,LENGTH(ypid)-1) ypid  from (
      SELECT GPJWJM,GPMXMC,JM_YC,REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, trim(char(ypid))||''/''))),''</A>'',''''),''<A>'','''') ypid from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                b.ypid
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_CPP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by b.ypid,a.gpjwjm,a.GPMXMC,a.JM_YC)
       group by gpjwjm,GPMXMC,JM_YC)
),
WJMCZ (GPJWJM,GPWJM,GPMXMC,JM_YC,YPID) as (
      select a.GPJWJM,a.GPWJM,a.GPMXMC,a.JM_YC,b.YPID
      from WJMCZ1 a join WJMCZ2 b
      on a.gpjwjm=b.gpjwjm and a.gpmxmc=b.gpmxmc and a.jm_yc=b.jm_yc
)
,
RESULTS AS (
        select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
               GPJWJM, 
               GPWJM,
               ypid,
               GPMXMC,
               GPMXID, 
               JM_YC,
               SCSJ
              
             from 
          (select b.id as bid,
               A.GPWJM as GPJWJM, 
               D.GPWJM,
               D.ypid,
               A.GPMXMC,
               C.ID AS GPMXID, 
               A.JM_YC,
               A.SCSJ
             
             from
          SZPX.T_SZPX_CPP_ZCFB_DL AS A
     left join (select max(ID) AS ID from SZPX.T_SZPX_CPP_ZCFB) AS B
            on 1=1
     left join SZPX.T_DIM_SZPX_GPMXB AS C
            on A.GPMXMC=C.GPMXMC
           and C.ZYBJ=1
     left join WJMCZ AS D
            ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(D.GPJWJM))
           and A.GPMXMC=D.GPMXMC
           AND A.JM_YC=D.JM_YC
         where JZBH='''||V_JZBH||'''
         order by a.jzxh))
SELECT ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ FROM RESULTS';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
--导入主成份表数据GPWJM,GPMXMC,JM_YC三项全部相同的更新，不同的插入
set EXE_SQL='
MERGE INTO SZPX.T_SZPX_CPP_ZCFB AS A
USING(
    SELECT b.id,a.gpwjm as gpjwjm,b.gpwjm as gpwjm,b.ypid,a.jm_yc,b.gpmxid,B.SCSJ '||V_XZZDRE||'
    FROM SESSION.ZCFJGB b
    JOIN 
    SZPX.T_SZPX_CPP_ZCFB_DL AS a
    ON A.gpwjm=b.gpjwjm
    and a.gpmxmc=b.gpmxmc
    and a.jm_yc=b.jm_yc
    where a.JZBH='''||V_JZBH||'''
    ) AS B
ON (a.gpwjm,a.jm_yc,a.gpmxid)=(b.gpwjm,b.jm_yc,b.gpmxid)
when matched then
update set(A.JM_YC '||V_XZZDR_A||')=(B.JM_YC '||V_XZZDR_B||')
when not matched then 
insert (A.ID, A.GPJWJM,A.GPWJM,a.ypid,A.GPMXID, A.JM_YC,A.SCSJ '||V_XZZDR_A||')
values(B.ID,B.GPJWJM,B.GPWJM,b.ypid,B.GPMXID,B.JM_YC,CURRENT TIMESTAMP '||V_XZZDR_B||')'
;
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
--插入检测指标表数据
SET OUT_JG='开始导入掺配品检测指标表';
SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_CPP_JCZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'JCLXID', 'GPWJM', 'GPJWJM', 'GPID', 'YJ', 'ZT', 'HYT', 'ZD', 'ZJ', 'ZL', 'JCR', 'JCJXID', 'JCSJ', 'BZ' )
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--插入检测类型为 光谱检测 时关联到光谱信息表    
set EXE_SQL='
INSERT INTO SZPX.T_SZPX_CPP_JCZBB (ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH,YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||')
  SELECT ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||' FROM (
       select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
              JCLXMC,
              SYBH,
              JCLXID,
              GPID,
              GPJWJM,
              YPID,
              GPWJM, 
              JCR,
              JCJXMC,
              JCJXID, 
              JCSJ, 
              BZ, 
              SCSJ,
              YJ, 
              ZT, 
              HYT, 
              ZD, 
              ZJ, 
              ZL
              '||V_XZZDR||'
         FROM (
         select b.id as BID,
              A.JCLXMC,
              f.sybh as SYBH,
              C.ID AS JCLXID,
              E.ID AS GPID,
              A.GPWJM as GPJWJM,
              case when a.JCLXMC  like ''%光谱%'' then e.ypid else f.ypid end as ypid,
              E.GPWJM, 
              A.JCR,
              A.JCJXMC,
              D.ID AS JCJXID, 
              A.JCSJ, 
              A.BZ, 
              CURRENT TIMESTAMP AS SCSJ,
              DOUBLE(A.YJ)  AS YJ, 
              DOUBLE(A.ZT)  AS ZT, 
              DOUBLE(A.HYT) AS HYT, 
              DOUBLE(A.ZD)  AS ZD, 
              DOUBLE(A.ZJ)  AS ZJ, 
              DOUBLE(A.ZL)  AS ZL
              '||V_XZZDRE||'
         FROM
         SZPX.T_SZPX_CPP_JCZBB_DL AS A
    LEFT JOIN (select max(ID) AS ID from SZPX.T_SZPX_CPP_JCZBB) AS B
           ON 1=1
    LEFT JOIN SZPX.T_DIM_SZPX_JCLXB AS C
           ON A.JCLXMC=C.JCLXMC
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS D
           ON A.JCJXMC=D.JCJXMC
    LEFT JOIN SZPX.T_SZPX_CPP_GPXXB AS E
           ON A.GPWJM=E.GPWJM
           OR A.GPWJM=E.GPJWJM
    LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_CPP_JCXXB t 
              left join SZPX.T_SZPX_CPP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
           ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
        WHERE A.JZBH='''||V_JZBH||''' 
        order by e.ypid,a.gpwjm,f.sybh,a.jzxh)) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新光谱旧文件名为null,避免插入主成份，检测指标表时由于gpjwjm出现问题
update SZPX.T_SZPX_CPP_GPXXB
set gpjwjm=null;
commit;

SET OUT_JG='掺配品数据导入完成';
    
END IF;

IF V_XMLX='productquality' THEN 
    SET OUT_JG='开始导入产品质量数据';
    SET OUT_JG='开始插入样品信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_YPXXB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('GPSL','YPID','YPBH','ZDYYPBH','PPMC','CPGGMC','YZGGMC','CJMC','ZSPHMC','JJPC',
       'ZSPC','HBCO','HBJY','HBYJ','SCSJ','BZ','CP27WM','JZBH','CJR','CJSJ','ZHGXR','ZHGXSJ')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='MERGE INTO SYPT.T_SYPT_CPZL_YPXXB AS A  
USING (
   SELECT B.ID+A.YPID YPID,
           CASE WHEN A.YPBH IS NOT NULL THEN A.YPBH ELSE VALUE(C.PPJC,''CP'')||''-''||TRIM(CHAR(B.ID+A.YPID)) END AS YPBH,
           A.ZDYYPBH,
           C.ID PPID,
           D.ID CPGGDM,
           E.ID YZGGDM,
           F.ID CJDM,
           G.ID ZSPHDM,
           A.JJPC,
           A.ZSPC,
           A.HBCO,
           A.HBJY,
           A.HBYJ,
           A.SCSJ,
           A.BZ,
           A.CP27WM,
           A.JZBH,
           A.CJR,
           A.CJSJ,
           A.ZHGXR,
           A.ZHGXSJ 
           '||V_XZZDR_A||'
    FROM (SELECT ROW_NUMBER() OVER(ORDER BY A.YPBH,A.JZXH) YPID,
           A.YPBH,
           A.ZDYYPBH,
           A.PPMC,
           A.CJMC,
           A.CPGGMC,
           A.YZGGMC,
           A.ZSPHMC,       
           A.JJPC,
           A.ZSPC,
           DOUBLE(A.HBCO) HBCO,
           DOUBLE(A.HBJY) HBJY,
           DOUBLE(A.HBYJ) HBYJ,
           A.SCSJ,
           A.BZ,
           A.CP27WM,
           A.JZBH,
           A.CJR,
           A.CJSJ,
           A.ZHGXR,
           A.ZHGXSJ
           '||V_XZZDRE||'
      FROM SYPT.T_SYPT_CPZL_YPXXB_DL A
      WHERE JZBH='''||V_JZBH||''') A 
      JOIN 
      (SELECT VALUE(MAX(YPID),0) ID FROM SYPT.T_SYPT_CPZL_YPXXB) B
      ON 1=1
      LEFT JOIN SZPX.T_DIM_SZPX_CP_PPB C
      ON A.PPMC=C.PPMC AND C.ZYBJ=1 AND CURRENT DATE BETWEEN C.KSRQ AND C.JSRQ
      LEFT JOIN SYPT.T_DIM_SYPT_CPZL_CPGG D
      ON A.CPGGMC=D.CPGGMC AND D.PPID=C.ID AND D.ZYBJ=1 AND CURRENT DATE BETWEEN D.KSRQ AND D.JSRQ
      LEFT JOIN SYPT.T_DIM_SYPT_CPZL_YZGG E
      ON A.YZGGMC=E.YZGGMC AND E.ZYBJ=1 AND CURRENT DATE BETWEEN E.KSRQ AND E.JSRQ
      LEFT JOIN SYPT.T_DIM_SYPT_CPZL_SCCJ F
      ON A.CJMC=F.CJMC AND F.ZYBJ=1 AND CURRENT DATE BETWEEN F.KSRQ AND F.JSRQ
      LEFT JOIN SYPT.T_DIM_SYPT_CPZL_ZSPH G
      ON A.ZSPHMC=G.ZSPHMC AND G.PPID=C.ID AND G.ZYBJ=1 AND CURRENT DATE BETWEEN G.KSRQ AND G.JSRQ
      ) B
      ON A.YPBH=B.YPBH
      WHEN MATCHED THEN UPDATE
      SET  (A.ZDYYPBH,A.PPMC,A.CJMC,A.CPGGMC,A.YZGGMC,A.ZSPHMC,A.JJPC,A.ZSPC,A.HBCO,A.HBJY,A.HBYJ,A.SCSJ,A.BZ,A.CP27WM,A.JZBH,A.ZHGXR,A.ZHGXSJ '||V_XZZDR_A||')=
           (CASE WHEN B.ZDYYPBH IS NOT NULL THEN B.ZDYYPBH ELSE A.ZDYYPBH END,B.PPID,B.CJDM,B.CPGGDM,B.YZGGDM,B.ZSPHDM,B.JJPC,B.ZSPC,B.HBCO,B.HBJY,B.HBYJ,B.SCSJ,B.BZ,B.CP27WM,B.JZBH,B.ZHGXR,CURRENT TIMESTAMP '||V_XZZDR_B||')
      WHEN NOT MATCHED THEN 
      INSERT (A.JZBH,A.YPID,A.YPBH,A.ZDYYPBH,A.PPMC,A.CJMC,A.CPGGMC,A.YZGGMC,A.ZSPHMC,A.JJPC,A.ZSPC,A.HBCO,A.HBJY,A.HBYJ,A.SCSJ,A.BZ,A.CP27WM,A.CJR,A.CJSJ,A.ZHGXR,A.ZHGXSJ '||V_XZZDR_A||')
      VALUES ('''||V_JZBH||''',B.YPID,B.YPBH,B.ZDYYPBH,B.PPID,B.CJDM,B.CPGGDM,B.YZGGDM,B.ZSPHDM,B.JJPC,B.ZSPC,B.HBCO,B.HBJY,B.HBYJ,B.SCSJ,B.BZ,B.CP27WM,B.CJR,CURRENT TIMESTAMP,B.ZHGXR,CURRENT TIMESTAMP '||V_XZZDR_B||')';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

SET OUT_JG='样品信息数据导入完成';

SET OUT_JG='开始插入感官信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_GGB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('ID', 'JZBH', 'YPBH', 'YPID', 'GZ', 'XQ', 'XT', 'ZQ', 'CJ', 'YW', 'HJ','JCBM', 'PXSJ', 'BZ', 'CJR', 'CJSJ', 'ZHGXR', 'ZHGXSJ')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='INSERT INTO SYPT.T_SYPT_CPZL_GGB(ID, JZBH, YPBH, YPID, GZ, XQ, XT, ZQ, CJ, YW, HJ,JCBM, PXSJ, BZ, CJR, CJSJ, ZHGXR, ZHGXSJ '||V_XZZDR||')
SELECT B.ID+A.ID ID,
       A.JZBH,
       D.YPBH,
       D.YPID,
       DOUBLE(A.GZ) GZ,
       DOUBLE(A.XQ) XQ,
       DOUBLE(A.XT) XT,
       DOUBLE(A.ZQ) ZQ,
       DOUBLE(A.CJ) CJ,
       DOUBLE(A.YW) YW,
       DOUBLE(A.HJ) HJ,
       C.ID JCBMDM,
       DATE(A.PXSJ) PXSJ,
       A.BZ,
       A.CJR,
       CURRENT TIMESTAMP,
       A.ZHGXR,
       CURRENT TIMESTAMP
       '||V_XZZDRE||'
FROM 
(SELECT ROW_NUMBER() OVER(ORDER BY YPBH,JZXH) ID,
       JZXH, 
       JZBH, 
       YPBH, 
       GZ, 
       XQ, 
       XT, 
       ZQ, 
       CJ, 
       YW, 
       HJ, 
       JCBMMC, 
       PXSJ, 
       BZ, 
       CJR, 
       CJSJ, 
       ZHGXR, 
       ZHGXSJ
       '||V_XZZDR||'
    FROM  SYPT.T_SYPT_CPZL_GGB_DL 
    WHERE JZBH='''||V_JZBH||''') A
    LEFT JOIN (SELECT VALUE(MAX(ID),0) ID FROM SYPT.T_SYPT_CPZL_GGB) B
    ON 1=1
    LEFT JOIN SYPT.T_DIM_SYPT_CPZL_JCBM C
    ON A.JCBMMC=C.JCBMMC AND C.ZYBJ=1 AND CURRENT DATE BETWEEN C.KSRQ AND C.JSRQ
    LEFT JOIN (SELECT D.YPID,D.YPBH,E.ZDYYPBH FROM SYPT.T_SYPT_CPZL_YPXXB D
                    LEFT JOIN SYPT.T_SYPT_CPZL_YPXXB_DL E 
                    ON D.YPBH=E.YPBH AND E.JZBH='''||V_JZBH||''') D
    ON A.YPBH=D.YPBH OR A.YPBH=D.ZDYYPBH
    ORDER BY D.YPID,D.YPBH,A.JZXH
       ';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

SET OUT_JG='感官信息数据导入完成';
SET OUT_JG='开始插入烟气信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_YQB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('ID','JZBH','YPBH','YPID','XYJX','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='INSERT INTO SYPT.T_SYPT_CPZL_YQB(ID, JZBH, YPBH, YPID,XYJX, JCBM, JCSJ, BZ, CJR, CJSJ, ZHGXR, ZHGXSJ '||V_XZZDR||')
SELECT B.ID+A.ID ID,
       A.JZBH,
       D.YPBH,
       D.YPID,
       F.ID AS XYJX,
       C.ID JCBMDM,
       A.JCSJ,
       A.BZ,
       A.CJR,
       CURRENT TIMESTAMP,
       A.ZHGXR,
       CURRENT TIMESTAMP
       '||V_XZZDRE||'
FROM 
(SELECT ROW_NUMBER() OVER(ORDER BY YPBH,JZXH) ID,
       JZXH, 
       JZBH, 
       YPBH, 
       JCJXMC AS XYJX, 
       JCBMMC, 
       JCSJ, 
       BZ, 
       CJR, 
       CJSJ, 
       ZHGXR, 
       ZHGXSJ
       '||V_XZZDR||'
    FROM  SYPT.T_SYPT_CPZL_YQB_DL 
    WHERE JZBH='''||V_JZBH||'''
    ) A
    LEFT JOIN (SELECT VALUE(MAX(ID),0) ID FROM SYPT.T_SYPT_CPZL_YQB) B
    ON 1=1
    LEFT JOIN SYPT.T_DIM_SYPT_CPZL_JCBM C
    ON A.JCBMMC=C.JCBMMC AND C.ZYBJ=1 AND CURRENT DATE BETWEEN C.KSRQ AND C.JSRQ
    LEFT JOIN (SELECT D.YPID,D.YPBH,E.ZDYYPBH FROM SYPT.T_SYPT_CPZL_YPXXB D
                    LEFT JOIN SYPT.T_SYPT_CPZL_YPXXB_DL E 
                    ON D.YPBH=E.YPBH AND E.JZBH='''||V_JZBH||''') D
    ON A.YPBH=D.YPBH OR A.YPBH=D.ZDYYPBH
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX F
    ON F.JCJXMC=A.XYJX AND F.ZYBJ=1 AND F.JXLB=1
    ORDER BY D.YPID,D.YPBH,A.JZXH';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

SET OUT_JG='烟气信息数据导入完成';

SET OUT_JG='开始插入化学信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_HXB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('ID','JZBH','YPBH','YPID','JCBM','JCSB','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='INSERT INTO SYPT.T_SYPT_CPZL_HXB(ID, JZBH, YPBH, YPID, JCBM, JCSB, JCSJ, BZ, CJR, CJSJ, ZHGXR, ZHGXSJ '||V_XZZDR||')

SELECT B.ID+A.ID ID,
       A.JZBH,
       D.YPBH,
       D.YPID,
       C.ID JCBMDM,
       F.ID JCSBDM,
       A.JCSJ,
       A.BZ,
       A.CJR,
       CURRENT TIMESTAMP,
       A.ZHGXR,
       CURRENT TIMESTAMP
       '||V_XZZDRE||'
FROM 
(SELECT ROW_NUMBER() OVER(ORDER BY YPBH,JZXH) ID,
       JZXH,
       JZBH, 
       YPBH, 
       JCBMMC, 
       JCJXMC AS JCSBMC, 
       JCSJ, 
       BZ, 
       CJR, 
       CJSJ, 
       ZHGXR, 
       ZHGXSJ
       '||V_XZZDR||'
    FROM  SYPT.T_SYPT_CPZL_HXB_DL 
    WHERE JZBH='''||V_JZBH||'''
    ) A
    LEFT JOIN (SELECT VALUE(MAX(ID),0) ID FROM SYPT.T_SYPT_CPZL_HXB) B
    ON 1=1
    LEFT JOIN SYPT.T_DIM_SYPT_CPZL_JCBM C
    ON A.JCBMMC=C.JCBMMC AND C.ZYBJ=1 AND CURRENT DATE BETWEEN C.KSRQ AND C.JSRQ
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX  F
    ON A.JCSBMC=F.JCJXMC AND F.ZYBJ=1 AND F.JXLB=2
    LEFT JOIN (SELECT D.YPID,D.YPBH,E.ZDYYPBH FROM SYPT.T_SYPT_CPZL_YPXXB D
                    LEFT JOIN SYPT.T_SYPT_CPZL_YPXXB_DL E 
                    ON D.YPBH=E.YPBH AND E.JZBH='''||V_JZBH||''') D
    ON A.YPBH=D.YPBH OR A.YPBH=D.ZDYYPBH
    ORDER BY D.YPID,D.YPBH,A.JZXH';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

SET OUT_JG='化学信息数据导入完成';

SET OUT_JG='开始插入物理信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_WLB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ( 'ID','JZBH','YPBH','YPID','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ')),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='INSERT INTO SYPT.T_SYPT_CPZL_WLB(ID,JZBH,YPBH,YPID,JCBM,JCSJ,BZ,CJR,CJSJ,ZHGXR,ZHGXSJ '||V_XZZDR||')
SELECT B.ID+A.ID ID,
       A.JZBH,
       D.YPBH,
       D.YPID,
       C.ID JCBMDM,
       A.JCSJ,
       A.BZ,
       A.CJR,
       CURRENT TIMESTAMP CJSJ,
       A.ZHGXR,
       CURRENT TIMESTAMP ZHGXSJ
       '||V_XZZDRE||'
FROM 
(SELECT ROW_NUMBER() OVER(ORDER BY YPBH,JZXH) ID,
        JZXH,
        JZBH,
        YPBH,
        JCBMMC,
        JCSJ,
        BZ,
        CJR,
        CJSJ,
        ZHGXR,
        ZHGXSJ
       '||V_XZZDR||'
    FROM  SYPT.T_SYPT_CPZL_WLB_DL 
    WHERE JZBH='''||V_JZBH||'''
    ) A
    LEFT JOIN (SELECT VALUE(MAX(ID),0) ID FROM SYPT.T_SYPT_CPZL_WLB) B
    ON 1=1
    LEFT JOIN SYPT.T_DIM_SYPT_CPZL_JCBM C
    ON A.JCBMMC=C.JCBMMC AND C.ZYBJ=1 AND CURRENT DATE BETWEEN C.KSRQ AND C.JSRQ
    LEFT JOIN (SELECT D.YPID,D.YPBH,E.ZDYYPBH FROM SYPT.T_SYPT_CPZL_YPXXB D
                    LEFT JOIN SYPT.T_SYPT_CPZL_YPXXB_DL E 
                    ON D.YPBH=E.YPBH AND E.JZBH='''||V_JZBH||''') D
    ON A.YPBH=D.YPBH OR A.YPBH=D.ZDYYPBH
    ORDER BY D.YPID,D.YPBH,A.JZXH';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

SET OUT_JG='物理信息数据导入完成';

SET OUT_JG='开始插入光谱信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SYPT_CPZL_GPB' 
       AND TBCREATOR='SYPT'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('ID','JZBH','YPBH','YPID','GPMC','ZDYGPMC','GPWJ','GPLX','JCYQXH','JCBM','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
    
set EXE_SQL='MERGE INTO SYPT.T_SYPT_CPZL_GPB AS A   
USING ( SELECT BID+ROW_NUMBER() OVER() ID,
               JZBH,
               YPBH,
               YPID,
               CASE WHEN GPMC IS NOT NULL THEN GPMC
                WHEN ETLUSR.F_SZPX_WJHZ1(ZDYGPMC) IS NOT NULL THEN YPBH||''-''||TRIM(CHAR(ROW_NUMBER() OVER(PARTITION BY YPBH ORDER BY ZDYGPMC)+XH))||ETLUSR.F_SZPX_WJHZ1(ZDYGPMC) 
               ELSE YPBH||''-''||TRIM(CHAR(ROW_NUMBER() OVER(PARTITION BY YPBH ORDER BY ZDYGPMC)+XH))
               END GPMC,
               ZDYGPMC,
               GPWJ,
               GPLXID,
               JCYQXHDM,
               JCBMDM,
               BZ,
               CJR,
               CURRENT TIMESTAMP CJSJ,
               ZHGXR,
               CURRENT TIMESTAMP ZHGXSJ
               '||V_XZZDR||'
        FROM  (SELECT B.BID,
               A.JZXH, 
               VALUE(G.XH,0) XH,
               A.JZBH, 
               F.YPBH,
               F.YPID,
               A.GPMC, 
               A.ZDYGPMC, 
               A.GPWJ, 
               A.GPLXMC, 
               C.ID GPLXID,
               A.JCJXMC,
               D.ID JCYQXHDM,               
               A.JCBMMC, 
               E.ID JCBMDM,
               A.BZ, 
               A.CJR, 
               A.CJSJ, 
               A.ZHGXR, 
               A.ZHGXSJ
               '||V_XZZDRE||'
        FROM SYPT.T_SYPT_CPZL_GPB_DL A
        LEFT JOIN (SELECT VALUE(MAX(ID),0) BID FROM SYPT.T_SYPT_CPZL_GPB) B
        ON 1=1
        LEFT JOIN SZPX.T_DIM_SZPX_GPLXB    C
        ON A.GPLXMC=C.GPLXMC AND C.ZYBJ=1
        LEFT JOIN SZPX.T_DIM_SZPX_JCJX D
        ON D.JCJXMC=A.JCJXMC AND D.ZYBJ=1 AND D.JXLB=2
        LEFT JOIN SYPT.T_DIM_SYPT_CPZL_JCBM E
        ON E.JCBMMC=A.JCBMMC AND E.ZYBJ=1 AND CURRENT DATE BETWEEN E.KSRQ AND E.JSRQ
        LEFT JOIN (SELECT VALUE(COUNT(YPBH),0) XH,YPBH FROM SYPT.T_SYPT_CPZL_GPB GROUP BY YPBH) G
        ON G.YPBH=A.YPBH
        LEFT JOIN(SELECT F.YPID,F.YPBH,G.ZDYYPBH FROM SYPT.T_SYPT_CPZL_YPXXB F
                    LEFT JOIN SYPT.T_SYPT_CPZL_YPXXB_DL G
                    ON F.YPBH=G.YPBH AND G.JZBH='''||V_JZBH||''') F
        ON A.YPBH=F.YPBH OR A.YPBH=F.ZDYYPBH
        WHERE A.JZBH='''||V_JZBH||'''
        ORDER BY A.YPBH,A.JZXH) A
) B
ON A.GPMC=B.GPMC
WHEN MATCHED THEN UPDATE 
SET(A.JZBH,A.ZDYGPMC, A.GPWJ, A.GPLX, A.JCYQXH, A.JCBM, A.BZ,A.ZHGXR, A.ZHGXSJ '||V_XZZDR_A||')=
    ('''||V_JZBH||''',CASE WHEN B.ZDYGPMC IS NOT NULL THEN B.ZDYGPMC ELSE A.ZDYGPMC END,CASE WHEN B.GPWJ IS NOT NULL THEN BLOB(B.GPWJ) ELSE BLOB(A.GPWJ) END,B.GPLXID,B.JCYQXHDM,B.JCBMDM,B.BZ,B.ZHGXR,B.ZHGXSJ '||V_XZZDR_B||')
WHEN NOT MATCHED THEN 
INSERT (A.ID,A.JZBH,A.YPBH,A.YPID,A.GPMC,A.ZDYGPMC,A.GPWJ,A.GPLX,A.JCYQXH,A.JCBM,A.BZ,A.CJR,A.CJSJ,A.ZHGXR,A.ZHGXSJ '||V_XZZDR_A||')
VALUES(B.ID,'''||V_JZBH||''',B.YPBH,B.YPID,B.GPMC,B.ZDYGPMC,BLOB(B.GPWJ),B.GPLXID,B.JCYQXHDM,B.JCBMDM,B.BZ,B.CJR,B.CJSJ,B.ZHGXR,B.ZHGXSJ '||V_XZZDR_B||')';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

--更新基础信息表对应光谱数量
SET OUT_JG='开始更新基础信息表对应光谱数量';
UPDATE SYPT.T_SYPT_CPZL_YPXXB A
SET A.GPSL=(SELECT COUNT(1) FROM SYPT.T_SYPT_CPZL_GPB B WHERE A.YPBH=B.YPBH)
WHERE A.YPBH IN (SELECT YPBH FROM SYPT.T_SYPT_CPZL_GPB_DL C WHERE C.JZBH=V_JZBH);
--WHERE A.JZBH=V_JZBH;
COMMIT;
SET OUT_JG='更新基础信息表对应光谱数量完成';
--更新完成        

SET OUT_JG='光谱信息数据导入完成';
END IF;
-----------------------------------------中间品----------------------------------------------
    IF V_XMLX='middle' THEN 
    
    SET OUT_JG='开始导入中间品基础信息表数据';
    
    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_JCXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('GPSL','YPID', 'YPBS','SYBH','LXID','ZDYSYBH','XMID', 'SYNF', 'SYYF','SYRQ', 'SCCJ', 'CPPHID', 'CPYHID','CJSJ','JZBH','SCSJ')
       ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;
     
SET EXE_SQL='
merge into SZPX.T_SZPX_ZJP_JCXXB as A  
    using (
    SELECT A.YPID as YPID,
           ''ZJP''||RTRIM(CHAR(A.YPID)) AS YPBS,
           CASE WHEN SYBH IS NULL THEN VALUE(A.LXJC,''ZJP'')||''-''||RTRIM(CHAR(A.YPID)) ELSE A.SYBH END SYBH,
           A.ZDYSYBH,
           A.JZBH,
           A.XMMC,
           A.XMID,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.LXMC,
           A.LXID,
           A.LXJC,
           A.SCSJ
           '||V_XZZDRE||'
      FROM (
    SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as YPID,
           A.JZBH,
           A.SYBH,
           A.ZDYSYBH,
           A.XMMC,
           A.XMID,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.LXJC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.SCSJ,
           A.LXMC,
           A.LXID
           '||V_XZZDRE||'
      FROM(
      SELECT b.id as bid,
           A.JZBH,
           A.SYBH,
           A.ZDYSYBH,
           A.XMMC,
           A.XMID,
           A.SYNF,
           A.SYYF,
           A.SYRQ,
           A.SCCJ,
           A.PPMC,
           A.LXJC,
           A.CPPHID,
           A.YHMC,
           A.CPYHID,
           A.SCSJ,
           A.LXMC,
           A.LXID
           '||V_XZZDRE||'
      FROM
      (
SELECT A.JZBH,
       A.JZXH,
       A.SYBH,
       A.ZDYSYBH,
       A.XMMC,
       B.ID AS XMID, 
       CAST (CHAR(A.SYNF,4) AS DECIMAL(4,0)) AS SYNF, 
       CAST (CHAR(A.SYYF,4) AS DECIMAL(2,0)) AS SYYF,
       CAST (CHAR(A.SYRQ,4) AS DECIMAL(2,0)) AS SYRQ,
       A.SCCJ, 
       C.PPMC,
       VALUE(E.LXJC,''ZJP'') AS LXJC,
       C.ID AS CPPHID,
       A.YHMC,
       D.ID AS CPYHID, 
       A.SCSJ,
       E.ID LXID,
       E.LXMC LXMC
       '||V_XZZDRE||'
  FROM SZPX.T_SZPX_ZJP_JCXXB_DL      AS A
  LEFT JOIN SZPX.T_DIM_SZPX_XMB     AS B
    ON A.XMMC = B.XMMC
   AND B.XMLX=''middle''
  LEFT JOIN SZPX.T_DIM_SZPX_CP_PPB  AS C
    ON A.PPMC=C.PPMC
   AND C.ZYBJ=1
   AND C.JSRQ>CURRENT DATE
  LEFT JOIN SZPX.T_DIM_SZPX_CP_YHB  AS D
    ON A.YHMC = D.YHMC
   AND D.ZYBJ = 1
  LEFT JOIN SZPX.T_SZPX_DIM_ZJP_LXB  AS E
   ON E.LXMC=A.LXMC
   AND E.ZYBJ=1 AND CURRENT DATE BETWEEN E.KSRQ AND E.JSRQ
 WHERE A.JZBH='''||V_JZBH||''' 
) AS A
 LEFT JOIN (SELECT MAX(YPID) AS ID FROM SZPX.T_SZPX_ZJP_JCXXB) AS B
        ON 1=1
        order by a.sybh,a.jzxh) AS A
        ) AS A 
    ) AS B
   ON (A.SYBH=B.SYBH)
  when matched then 
       update set (A.ZDYSYBH,A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.SCCJ,A.CPPHID, A.CPYHID,A.LXID  '||V_XZZDR_A||')= 
                    (case when B.ZDYSYBH is not null then B.ZDYSYBH else A.ZDYSYBH end,B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.SCCJ,B.CPPHID, B.CPYHID,B.LXID'||V_XZZDR_B||')
  when not matched then
            insert(a.jzbh,A.YPID, A.YPBS, A.SYBH,A.ZDYSYBH, A.XMID, A.SYNF, A.SYYF, A.SYRQ, A.SCCJ,A.CPPHID, A.CPYHID,A.SCSJ,A.LXID  '||V_XZZDR_A||')
            values('''||V_JZBH||''' ,B.YPID, B.YPBS, B.SYBH,B.ZDYSYBH, B.XMID, B.SYNF, B.SYYF, B.SYRQ, B.SCCJ,B.CPPHID, B.CPYHID,CURRENT TIMESTAMP,B.LXID  '||V_XZZDR_B||') ';

    PREPARE s0 FROM EXE_SQL;
    EXECUTE s0;
    COMMIT;
    
--插入增评吸指标表(老版)数据
SET OUT_JG='(中间品)开始导入中间品评吸指标表(老版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
        AND NAME NOT IN ( 'SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX', 'MX', 'ZF', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'ZJXX', 'NXX', 'PY', 'XF', 'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ')
        AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_ZJP_PXZBB_XBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;    

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_ZJP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM 
  (SELECT b.id as bid,
       C.YPID,
       c.SYBH,
       2 AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ ,
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_ZJP_PXZBB_LBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_PXZBB) as b
         ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_ZJP_JCXXB t 
              left join SZPX.T_SZPX_ZJP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
         on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    
    
--插入增评吸指标表(新版)数据
SET OUT_JG='(中间品)开始导入新增评吸指标表(新版)数据';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)    
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_PXZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
         AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF')
         AND NAME NOT IN (SELECT NAME FROM sysibm.syscolumns WHERE TBNAME='T_SZPX_ZJP_PXZBB_LBDL' AND TBCREATOR='SZPX')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

set EXE_SQL='
INSERT INTO SZPX.T_SZPX_ZJP_PXZBB (ID,YPID,SYBH,PXLX,PXR,PXSJ,BZ,SCSJ '||V_XZZDR||')    
SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
       YPID,
       SYBH,
       PXLX,
       PXR, 
       PXSJ, 
       BZ ,
       SCSJ
       '||V_XZZDR||'
  FROM (SELECT b.id as bid,
       C.YPID,
       c.SYBH,
       1    AS PXLX,
       A.PXR, 
       A.PXSJ, 
       A.BZ ,
       CURRENT TIMESTAMP AS SCSJ
       '||V_XZZDRE||'
  FROM
  SZPX.T_SZPX_ZJP_PXZBB_XBDL as a
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_PXZBB) as b
    ON 1=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_ZJP_JCXXB t 
              left join SZPX.T_SZPX_ZJP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) as C
    on (a.sybh=c.sybh or a.sybh=c.zdysybh)
 WHERE A.JZBH='''||V_JZBH||'''
 order by a.sybh,a.jzxh) ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--插入光谱信息表数据.
SET OUT_JG='(中间品)开始插入中间品光谱信息表数据';

    SET V_XZZDRE='';     
    SET V_XZZDR='';
    SET V_XZZDR_A='';
    SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_GPXXB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID','YPID', 'SYBH', 'XH', 'GPJWJM', 'GPWJM', 'ZDYGPWJM','YPBS', 'XMMC', 'GPLXID', 'SFCZGPWJ','JCSJ', 'JCJXID', 'JCR', 'SFCZGPWJ', 'BZ')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;        
    
--光谱文件名不为空
SET EXE_SQL='
merge INTO SZPX.T_SZPX_ZJP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN
            SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0))) 
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM 
       (SELECT b.id as bid,
             F.YPID,
            F.SYBH,
            c.xh,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_ZJP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_ZJP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_ZJP_JCXXB t 
              left join SZPX.T_SZPX_ZJP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=F.sybh or a.sybh=F.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NOT NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ '||V_XZZDR_B||')
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
 
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
    
--光谱文件名为空    
SET EXE_SQL='
merge INTO SZPX.T_SZPX_ZJP_GPXXB as A  
    using (
     SELECT ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
             YPID,
            SYBH,
            ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0) as XH,
            GPJWJM, 
            CASE WHEN GPWJM IS NOT NULL THEN GPWJM 
            WHEN F_SZPX_WJHZ1(GPJWJM) IS NOT NULL THEN 
            SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))||F_SZPX_WJHZ1(GPJWJM)
            ELSE SYBH||''-''||RTRIM(CHAR(ROW_NUMBER()OVER(PARTITION BY SYBH ORDER BY GPJWJM)+VALUE(XH,0)))
            END AS GPWJM,
            ZDYGPWJM,
            GPLXMC,
            GPLXID,
            JCSJ, 
            JCJXMC,
            JCJXID,
            JCR,
            SFCZGPWJ, 
            BZ,
            SCSJ
            '||V_XZZDR||'
       FROM(
           SELECT b.id as BID,
             F.YPID,
            f.SYBH,
            C.XH,
            A.GPWJM,
            A.ZDYGPWJM,
            A.ZDYGPWJM as GPJWJM, 
            A.GPLXMC,
            D.ID AS GPLXID,
            A.JCSJ, 
            A.JCJXMC,
            E.ID AS JCJXID,
            A.JCR,
            A.SFCZGPWJ, 
            A.BZ,
            A.SCSJ
            '||V_XZZDRE||'
       FROM
       SZPX.T_SZPX_ZJP_GPXXB_DL AS A
  LEFT JOIN (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_GPXXB) AS B
         ON 1=1
  LEFT JOIN (SELECT MAX(XH) AS XH,SYBH FROM SZPX.T_SZPX_ZJP_GPXXB GROUP BY SYBH) AS C
         ON 1=1
        AND A.SYBH=C.SYBH
  LEFT JOIN SZPX.T_DIM_SZPX_GPLXB AS D
         ON A.GPLXMC=D.GPLXMC
        AND D.ZYBJ=1
  LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS E
         ON A.JCJXMC=E.JCJXMC
        AND E.ZYBJ=1
  LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_ZJP_JCXXB t 
              left join SZPX.T_SZPX_ZJP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
         ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
      WHERE 1=1
        AND A.JZBH='''||V_JZBH||'''
        AND A.SFCZGPWJ IS NULL
   ORDER BY a.sybh,a.JZXH)) AS B
    ON A.GPWJM=B.GPWJM
    OR A.GPWJM=B.GPJWJM
  when matched then 
        update set (A.ZDYGPWJM,A.GPJWJM,A.GPLXID, A.JCSJ, A.JCJXID, A.JCR,  A.BZ '||V_XZZDR_A||')= 
                 (case when B.ZDYGPWJM is not null then B.ZDYGPWJM else A.ZDYGPWJM end,B.GPJWJM,B.GPLXID, B.JCSJ, B.JCJXID, B.JCR,  B.BZ '||V_XZZDR_B||')   
  when not matched then
           insert(A.ID, A.YPID, A.SYBH, A.XH, A.GPWJM,A.ZDYGPWJM, A.GPJWJM, A.GPLXID, A.JCSJ, A.JCJXID, A.JCR, A.SFCZGPWJ      , A.BZ,A.SCSJ '||V_XZZDR_A||')
           values(B.ID, B.YPID, B.SYBH, B.XH, B.GPWJM,B.ZDYGPWJM, B.GPJWJM, B.GPLXID, B.JCSJ, B.JCJXID, B.JCR, BLOB(B.SFCZGPWJ), B.BZ,CURRENT TIMESTAMP '||V_XZZDR_B||') ';
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--更新基础信息表对应光谱数量
SET OUT_JG='开始更新基础信息表对应光谱数量';
UPDATE SZPX.T_SZPX_ZJP_JCXXB A
SET A.GPSL=(SELECT COUNT(1) FROM SZPX.T_SZPX_ZJP_GPXXB B WHERE A.SYBH=B.SYBH)
WHERE A.SYBH IN (SELECT SYBH FROM SZPX.T_SZPX_ZJP_GPXXB_DL C WHERE C.JZBH=V_JZBH);
--WHERE A.JZBH=V_JZBH;
COMMIT;
SET OUT_JG='更新基础信息表对应光谱数量完成';
--更新完成            
    
--插入主成分表数据
SET OUT_JG='开始导入中间品主成分表';

SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';
    
    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_ZCFB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'GPID', 'GPJWJM', 'GPWJM', 'GPMXID', 'JM_YC')
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--先导入主成份与光谱的关联数据(主成份ID为人工序号生成,因此需特别注意关联表与主成份表的ID生成结果必须完全一致)
set EXE_SQL=' 
 INSERT INTO SZPX.T_SZPX_ZJP_ZCFGPDYB (ID,ZCFID,GPID) 
with YSSJ(ID,GPWJM) as (
           select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
           A.GPWJM
          from SZPX.T_SZPX_ZJP_ZCFB_DL AS A
     left join (SELECT MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_ZCFB) as b
            on 1=1
         where A.JZBH='''||V_JZBH||'''
      Order by JZXH), 
temp(ID,GPWJM) as (
           SELECT ID, GPWJM FROM YSSJ
         UNION ALL
         SELECT ID, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM FROM TEMP AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
CFJG(ID,GPWJM) as (
         SELECT ID,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM FROM TEMP WHERE GPWJM <> '''' )

     select ROW_NUMBER()OVER()+VALUE(B.ID,0) as ID,
             A.ID AS ZCFID,
            C.ID AS GPID
       from CFJG AS A
  left join (select MAX(ID) AS ID FROM SZPX.T_SZPX_ZJP_ZCFGPDYB) AS B
         on 1=1
  left join SZPX.T_SZPX_ZJP_GPXXB AS C
         ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPWJM))
         OR LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(C.GPJWJM))
      where A.ID IS NOT NULL   --主成份ID
        and C.ID IS NOT NULL   --光谱ID
        and (A.ID,C.ID) NOT IN (SELECT ZCFID,GPID FROM SZPX.T_SZPX_ZJP_ZCFGPDYB) ';

PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;

--删除临时表数据
DELETE FROM SESSION.ZCFJGB;

--导入主成份临时结果数据
set EXE_SQL='
INSERT INTO SESSION.ZCFJGB(ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ)
 with WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC
          from SZPX.T_SZPX_ZJP_ZCFB_DL AS A
         where JZBH='''||V_JZBH||'''
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
           SELECT GPJWJM,gpwjm,GPMXMC,JM_YC FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE(''/'', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC FROM WJMCZ_TMP1 AS A WHERE LOCATE(''/'', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
         SELECT GPJWJM,  CASE WHEN LOCATE(''/'', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE(''/'', GPWJM) - 1) ELSE GPWJM END AS GPWJM,GPMXMC,JM_YC FROM WJMCZ_TMP1 WHERE GPWJM <> '''' 
)
,
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, B.GPWJM||''/''))),''</A>'',''''),''<A>'','''') as GPWJM
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_ZJP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by a.gpjwjm,a.GPMXMC,a.JM_YC)
)
,
WJMCZ2(GPJWJM,GPMXMC,JM_YC,YPID) as (
      select  GPJWJM,GPMXMC,JM_YC,LEFT(ypid,LENGTH(ypid)-1) ypid  from (
      SELECT GPJWJM,GPMXMC,JM_YC,REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, trim(char(ypid))||''/''))),''</A>'',''''),''<A>'','''') ypid from (
         SELECT A.GPJWJM,
                a.GPMXMC,
                a.JM_YC,
                b.ypid
           FROM WJMCZ_TMP2 AS A
      LEFT JOIN SZPX.T_SZPX_ZJP_GPXXB AS B
             ON A.GPWJM=B.GPWJM
             OR A.GPWJM=B.GPJWJM
       group by b.ypid,a.gpjwjm,a.GPMXMC,a.JM_YC)
       group by gpjwjm,GPMXMC,JM_YC)
),
WJMCZ (GPJWJM,GPWJM,GPMXMC,JM_YC,YPID) as (
      select a.GPJWJM,a.GPWJM,a.GPMXMC,a.JM_YC,b.YPID
      from WJMCZ1 a join WJMCZ2 b
      on a.gpjwjm=b.gpjwjm and a.gpmxmc=b.gpmxmc and a.jm_yc=b.jm_yc
)
,
RESULTS AS (
        select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
               GPJWJM, 
               GPWJM,
               YPID,
               GPMXMC,
               GPMXID, 
               JM_YC,
               SCSJ
              
             from 
          (select b.id as bid,
               A.GPWJM as GPJWJM, 
               D.GPWJM,
               D.YPID,
               A.GPMXMC,
               C.ID AS GPMXID, 
               A.JM_YC,
               A.SCSJ
             from
          SZPX.T_SZPX_ZJP_ZCFB_DL AS A
     left join (select max(ID) AS ID from SZPX.T_SZPX_ZJP_ZCFB) AS B
            on 1=1
     left join SZPX.T_DIM_SZPX_GPMXB AS C
            on A.GPMXMC=C.GPMXMC
           and C.ZYBJ=1
     left join WJMCZ AS D
            ON LTRIM(RTRIM(A.GPWJM))=LTRIM(RTRIM(D.GPJWJM))
           and A.GPMXMC=D.GPMXMC
           AND A.JM_YC=D.JM_YC
         where JZBH='''||V_JZBH||'''
         order by a.jzxh))
SELECT ID,GPJWJM,GPWJM,GPMXMC,GPMXID,JM_YC,YPID,SCSJ FROM RESULTS';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
--导入主成份表数据GPWJM,GPMXMC,JM_YC三项全部相同的更新，不同的插入
set EXE_SQL='
MERGE INTO SZPX.T_SZPX_ZJP_ZCFB AS A
USING(
    SELECT b.id,a.gpwjm as gpjwjm,b.gpwjm as gpwjm,b.ypid,a.jm_yc,b.gpmxid,B.SCSJ '||V_XZZDRE||'
    FROM SESSION.ZCFJGB b
    JOIN 
    SZPX.T_SZPX_ZJP_ZCFB_DL AS a
    ON A.gpwjm=b.gpjwjm
    and a.gpmxmc=b.gpmxmc
    and a.jm_yc=b.jm_yc
    where a.JZBH='''||V_JZBH||'''
    ) AS B
ON (a.gpwjm,a.jm_yc,a.gpmxid)=(b.gpwjm,b.jm_yc,b.gpmxid)
when matched then
update set(A.JM_YC '||V_XZZDR_A||')=(B.JM_YC '||V_XZZDR_B||')
when not matched then 
insert (A.ID, A.GPJWJM,A.GPWJM,A.YPID, A.GPMXID, A.JM_YC,A.SCSJ '||V_XZZDR_A||')
values(B.ID,B.GPJWJM,B.GPWJM,B.YPID,B.GPMXID,B.JM_YC,CURRENT TIMESTAMP '||V_XZZDR_B||')'
;
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;    

--插入检测指标表数据
SET OUT_JG='开始导入检测指标表';
SET V_XZZDRE='';     
SET V_XZZDR='';
SET V_XZZDR_A='';
SET V_XZZDR_B='';

    --删除临时表数据
 DELETE FROM SESSION.XZZD;
--拼新增字段
 INSERT INTO SESSION.XZZD(XZZDRE,XZZDR_A,XZZDR_B,XZZDR)        
    WITH COLS(COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
       ( SELECT case when COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT') then ', DOUBLE(A.'||NAME||') as '||NAME
            else ', A.'||NAME end  AS COLNAME_S,
            ', A.'||NAME AS COLNAME_A,
            ', B.'||NAME AS COLNAME_B,
            ROWNUMBER() OVER(ORDER BY COLNO) AS XH,
            ','||NAME AS NAME
      FROM sysibm.syscolumns 
     WHERE TBNAME='T_SZPX_ZJP_JCZBB' 
       AND TBCREATOR='SZPX'
       AND NAME IS NOT NULL
       AND NAME NOT IN ('SCSJ','ID', 'YPID', 'SYBH', 'JCLXID', 'GPWJM', 'GPJWJM', 'GPID', 'YJ', 'ZT', 'HYT', 'ZD', 'ZJ', 'ZL', 'JCR', 'JCJXID', 'JCSJ', 'BZ' )
           ),
     COLS_P (COLNAME_S,COLNAME_A,COLNAME_B,XH,NAME) AS
         (SELECT cast(COLNAME_S as long varchar),cast(COLNAME_A as long varchar),cast(COLNAME_B as long varchar),XH,cast(NAME as long varchar)  FROM COLS WHERE XH=1
            UNION ALL
          SELECT cast(B.COLNAME_S||A.COLNAME_S as long varchar),cast(B.COLNAME_A||A.COLNAME_A as long varchar),cast(B.COLNAME_B||A.COLNAME_B as long varchar),
              B.XH,cast(B.NAME||A.NAME as long varchar)
          FROM COLS_P A,COLS B
          WHERE A.XH+1=B.XH)
     SELECT cast(COLNAME_S as varchar(1500)),cast(COLNAME_A as varchar(800)),cast(COLNAME_B as varchar(800)),cast(NAME as varchar(400)) FROM COLS_P A,(SELECT MAX(XH) XH FROM COLS_P) B
    WHERE A.XH=B.XH;
         
    SELECT XZZDRE,XZZDR_A,XZZDR_B,XZZDR INTO V_XZZDRE,V_XZZDR_A,V_XZZDR_B,V_XZZDR FROM SESSION.XZZD;

--插入检测类型为 光谱检测 时关联到光谱信息表
set EXE_SQL='
INSERT INTO SZPX.T_SZPX_ZJP_JCZBB (ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||')
  SELECT ID, YPID, JCLXID, GPID, GPJWJM, GPWJM,SYBH, YJ, ZT, HYT, ZD, ZJ, ZL, JCR, JCJXID, JCSJ, BZ,SCSJ '||V_XZZDR||' FROM (
       select ROW_NUMBER()OVER()+VALUE(BID,0) as ID,
              JCLXMC,
              JCLXID,
              GPID,
              GPJWJM,
              SYBH,
              YPID,
              GPWJM, 
              JCR,
              JCJXMC,
              JCJXID, 
              JCSJ, 
              BZ, 
              YJ, 
              ZT, 
              HYT, 
              ZD, 
              ZJ, 
              ZL,
              SCSJ
              '||V_XZZDR||'
         FROM (select b.id as BID,
              A.JCLXMC,
               f.sybh as SYBH,
              C.ID AS JCLXID,
              E.ID AS GPID,
              A.GPWJM as GPJWJM,
              case when a.JCLXMC  like ''%光谱%'' then e.ypid else f.ypid end as ypid,
              E.GPWJM, 
              A.JCR,
              A.JCJXMC,
              D.ID AS JCJXID, 
              A.JCSJ, 
              A.BZ, 
              CURRENT TIMESTAMP AS SCSJ,
              DOUBLE(A.YJ)  AS YJ, 
              DOUBLE(A.ZT)  AS ZT, 
              DOUBLE(A.HYT) AS HYT, 
              DOUBLE(A.ZD)  AS ZD, 
              DOUBLE(A.ZJ)  AS ZJ, 
              DOUBLE(A.ZL)  AS ZL
              '||V_XZZDRE||'
         FROM
         SZPX.T_SZPX_ZJP_JCZBB_DL AS A
    LEFT JOIN (select max(ID) AS ID from SZPX.T_SZPX_ZJP_JCZBB) AS B
           ON 1=1
    LEFT JOIN SZPX.T_DIM_SZPX_JCLXB AS C
           ON A.JCLXMC=C.JCLXMC
    LEFT JOIN SZPX.T_DIM_SZPX_JCJX AS D
           ON A.JCJXMC=D.JCJXMC
    LEFT JOIN SZPX.T_SZPX_ZJP_GPXXB AS E
           ON A.GPWJM=E.GPWJM
           OR A.GPWJM=E.GPJWJM
    LEFT JOIN (select t.ypid,t.sybh,d.zdysybh from SZPX.T_SZPX_ZJP_JCXXB t 
              left join SZPX.T_SZPX_ZJP_JCXXB_DL d on t.sybh=d.sybh and d.jzbh='''||V_JZBH||'''
             ) AS F
           ON (a.sybh=f.sybh or a.sybh=f.zdysybh)
        WHERE A.JZBH='''||V_JZBH||''' 
        order by e.ypid,a.gpwjm,f.sybh,a.jzxh)) ';
   
PREPARE s0 FROM EXE_SQL;
EXECUTE s0;
COMMIT;
        
--更新光谱旧文件名为null,避免插入主成份，检测指标表时由于gpjwjm出现问题
update SZPX.T_SZPX_ZJP_GPXXB
set gpjwjm=null;
commit;

    SET OUT_JG='中间品数据导入完成';
    
    END IF ;
-----------------------------------------------------------------------------------------------
DELETE FROM SZPX.T_SZPX_YL_GPXXB_DL       WHERE JZBH=V_JZBH;     --删除光谱信息表(原料)
DELETE FROM SZPX.T_SZPX_CP_GPXXB_DL       WHERE JZBH=V_JZBH;     --删除光谱信息表(产品)
DELETE FROM SZPX.T_SZPX_CPP_GPXXB_DL      WHERE JZBH=V_JZBH;     --删除光谱信息表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_GPXXB_DL       WHERE JZBH=V_JZBH;     --删除光谱信息表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_JCXXB_DL  WHERE JZBH=V_JZBH;     --删除基础信息表(原料)
DELETE FROM SZPX.T_SZPX_CP_JCXXB_DL  WHERE JZBH=V_JZBH;     --删除基础信息表(产品)
DELETE FROM SZPX.T_SZPX_CPP_JCXXB_DL      WHERE JZBH=V_JZBH;     --删除基础信息表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_JCXXB_DL  WHERE JZBH=V_JZBH;     --删除基础信息表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_JCZBB_DL  WHERE JZBH=V_JZBH;     --删除检测指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_JCZBB_DL  WHERE JZBH=V_JZBH;     --删除检测指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_JCZBB_DL      WHERE JZBH=V_JZBH;     --删除检测指标表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_JCZBB_DL      WHERE JZBH=V_JZBH;     --删除检测指标表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_PXZBB_LBDL  WHERE JZBH=V_JZBH;     --删除老版评吸指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_PXZBB_LBDL  WHERE JZBH=V_JZBH;     --删除老版评吸指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_PXZBB_LBDL      WHERE JZBH=V_JZBH;     --删除老版评吸指标表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_PXZBB_LBDL      WHERE JZBH=V_JZBH;     --删除老版评吸指标表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_PXZBB_XBDL  WHERE JZBH=V_JZBH;     --删除新版评吸指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_PXZBB_XBDL  WHERE JZBH=V_JZBH;     --删除新版评吸指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_PXZBB_XBDL      WHERE JZBH=V_JZBH;     --删除新版评吸指标表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_PXZBB_XBDL      WHERE JZBH=V_JZBH;     --删除新版评吸指标表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_ZCFB_DL     WHERE JZBH=V_JZBH;     --删除主成分表(原料)
DELETE FROM SZPX.T_SZPX_CP_ZCFB_DL     WHERE JZBH=V_JZBH;     --删除主成分表(产品)
DELETE FROM SZPX.T_SZPX_CPP_ZCFB_DL         WHERE JZBH=V_JZBH;     --删除主成分表(掺配品)
DELETE FROM SZPX.T_SZPX_ZJP_ZCFB_DL         WHERE JZBH=V_JZBH;     --删除主成分表(中间品)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_ZBB_DL      WHERE JZBH=V_JZBH;     --删除指标表(原料)

COMMIT;

DELETE FROM SYPT.T_SYPT_CPZL_YPXXB_DL WHERE JZBH=V_JZBH; --删除样品信息表（产品质量）
DELETE FROM SYPT.T_SYPT_CPZL_GGB_DL WHERE JZBH=V_JZBH;   --删除感官中转表（产品质量）
DELETE FROM SYPT.T_SYPT_CPZL_YQB_DL WHERE  JZBH=V_JZBH;  --删除烟气中转表（产品质量）
DELETE FROM SYPT.T_SYPT_CPZL_HXB_DL WHERE  JZBH=V_JZBH;  --删除化学中转表（产品质量）
DELETE FROM SYPT.T_SYPT_CPZL_WLB_DL WHERE JZBH=V_JZBH;   --删除物理中转表（产品质量）
DELETE FROM SYPT.T_SYPT_CPZL_GPB_DL WHERE JZBH=V_JZBH;   --删除光谱中转表（产品质量）
COMMIT;

    SET OUT_JG='1';
    SET OP_V_ERR_MSG='1';
    --SET OP_V_ERR_MSG=EXE_SQL;
    --加载程序执行完成
    END;
