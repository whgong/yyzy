SET SCHEMA = SZPX;

CREATE PROCEDURE SZPX.P_SZPX_SJJY_ALL (
    IN IN_XMLX  VARCHAR(20),
    IN IN_JZBH  VARCHAR(40),
    OUT OUT_JG  VARCHAR(3000),
    OUT OP_V_ERR_MSG  VARCHAR(3000) )
  SPECIFIC SQL130906173820600
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN  
  /*
   20130815 SUNCC 删除项目表数据时ＳＱＬ　BUG处理　（４１２９行）;
  */

--定义部分
DECLARE V_SFXZZD    INTEGER;             --是否有新增字段
DECLARE V_JZBH      VARCHAR(40);         --加载编号
DECLARE V_XMLX      VARCHAR(20);         --项目类型
DECLARE V_TBNAME_CT VARCHAR(200);        --动态模式名+表名
DECLARE V_TBNAME_T  VARCHAR(200);        --动态表名
DECLARE V_COLNAME   VARCHAR(200);        --动态字段名
DECLARE V_KSXH      INTEGER;             --开始序号(用于校验新增字段)
DECLARE V_JSXH      INTEGER;             --结束序号(用于校验新增字段)
DECLARE EXE_SQL     VARCHAR(20000);      --动态SQL
DECLARE V_RES       INTEGER;             --检测有共有多少条错误数据

--定义错误输出部分
DECLARE V_SQLSTATE  CHAR(5); 
DECLARE I_SQLCODE   INTEGER;  
DECLARE SQLSTATE    CHAR(5); 
DECLARE SQLCODE     INTEGER; 

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
END;

--检测结果表  
DECLARE GLOBAL TEMPORARY TABLE SESSION.JGB(
     TNAME   VARCHAR(50),
       RNUM    INTEGER,
     CNAME   VARCHAR(50),
     JCJG    VARCHAR(200),
     JZBH    VARCHAR(40)
)with replace on commit preserve rows NOT LOGGED;

--检测新增字段
DECLARE GLOBAL TEMPORARY TABLE SESSION.XZZD(
     XH        INTEGER,
     TBNAME_CT VARCHAR(200),
     TBNAME_T  VARCHAR(200),
     COLNAME   VARCHAR(200)
)with replace on commit preserve rows NOT LOGGED;

--定义光谱信息表
DECLARE GLOBAL TEMPORARY TABLE SESSION.GPXXB(
     ID      INTEGER,
     SYBH    VARCHAR(255),
     GPWJM   VARCHAR(255)
)with replace on commit preserve rows NOT LOGGED;

--自定样品编号表
DECLARE GLOBAL TEMPORARY TABLE SESSION.ZDYSYBH(
     ZDYSYBH    VARCHAR(255),
     sybh    varchar(255)
)with replace on commit preserve rows NOT LOGGED;

--定义基础信息表
DECLARE GLOBAL TEMPORARY TABLE SESSION.JCXXB(
     SYBH        VARCHAR(255),
     XMMC        VARCHAR(200),
     XMID        INTEGER,
     XMLX        VARCHAR(200)
)with replace on commit preserve rows NOT LOGGED;

SET V_JZBH=IN_JZBH;                       --判断加载编号(用于多用户执行导入操作时的区分)
SET V_XMLX=LCASE(IN_XMLX);                --判断项目类型(raw:原料,medium:中间件,product:产品)

--清空结果表中对应加载编号的检测数据
DELETE FROM SZPX.T_SZPX_DLJG WHERE JZBH=V_JZBH;
COMMIT;

IF V_XMLX NOT IN ('raw','medium','product','RAW','MEDIUM','PRODUCT','productQuality','productquality','MIDDLE','middle')
THEN 
SET OP_V_ERR_MSG='输入参数错误';
RETURN;
END IF;


--如果项目名称不存在.则新增该项目名称
INSERT INTO SZPX.T_DIM_SZPX_XMB (XMMC,XMLX)
SELECT DISTINCT XMMC ,V_XMLX FROM SZPX.T_SZPX_YL_JCXXB_DL where (xmmc,v_xmlx) not in (SELECT xmmc,xmlx FROM SZPX.T_DIM_SZPX_XMB) and xmmc is not null and JZBH=V_JZBH
UNION ALL
SELECT DISTINCT XMMC ,V_XMLX FROM SZPX.T_SZPX_CP_JCXXB_DL where (xmmc,v_xmlx) not in (SELECT xmmc,xmlx FROM SZPX.T_DIM_SZPX_XMB) and xmmc is not null and JZBH=V_JZBH
UNION ALL
SELECT DISTINCT XMMC ,V_XMLX FROM SZPX.T_SZPX_CPP_JCXXB_DL where (xmmc,v_xmlx) not in (SELECT xmmc,xmlx FROM SZPX.T_DIM_SZPX_XMB) and xmmc is not null and JZBH=V_JZBH
UNION ALL
SELECT DISTINCT XMMC ,V_XMLX FROM SZPX.T_SZPX_ZJP_JCXXB_DL where (xmmc,v_xmlx) not in (SELECT xmmc,xmlx FROM SZPX.T_DIM_SZPX_XMB) and xmmc is not null and JZBH=V_JZBH;
COMMIT;
 

--检测项目名称
IF V_XMLX='raw' or V_XMLX='RAW' THEN 

DELETE FROM SESSION.JCXXB;
DELETE FROM SESSION.GPXXB;
DELETE FROM SESSION.ZDYSYBH;

--基础信息
INSERT INTO SESSION.JCXXB (SYBH,XMMC,XMID,XMLX)
SELECT DISTINCT SYBH,XMMC,XMID,XMLX FROM (
SELECT A.SYBH, B.XMMC, B.ID as XMID,B.XMLX
  from SZPX.T_SZPX_YL_JCXXB AS A,
       SZPX.T_DIM_SZPX_XMB AS B
 WHERE A.XMID=B.ID);
COMMIT;

--自定样品编号

INSERT INTO SESSION.ZDYSYBH(ZDYSYBH,sybh)
SELECT A.ZDYSYBH,a.sybh
  from SZPX.T_SZPX_YL_JCXXB_DL AS A
  where a.JZBH=V_JZBH
;
COMMIT;

--光谱信息
INSERT INTO SESSION.GPXXB (ID,SYBH,GPWJM)
select A.YPID,A.SYBH,B.GPWJM
  from SZPX.T_SZPX_YL_JCXXB AS A,
       SZPX.T_SZPX_YL_GPXXB AS B
 WHERE A.SYBH=B.SYBH
   AND B.GPWJM IS NOT NULL
   AND B.GPWJM <>''
    UNION ALL
 select CAST (NULL AS INTEGER) AS ID,B.SYBH,B.ZDYGPWJM
  from SZPX.T_SZPX_YL_GPXXB_DL AS B
 WHERE JZBH=V_JZBH
   and B.ZDYGPWJM IS NOT NULL
   AND B.ZDYGPWJM <>'';
COMMIT;
----------------------------------开始检测原料基础信息表----------------------------------------------
SET OUT_JG='开始检测原料基础信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     XMMC, 
     PC,
     SYBH,
     ZDYSYBH,
     YLNF,
     SYNF,
     SYYF,
     SYRQ,
     SFMC,
     CSMC,
     LBMC,
     XTMC,
     SEWM,
     DJMC,
     JZBH,
     YYBWMC,
     SJYTMC
  from SZPX.T_SZPX_YL_JCXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_rq as
 (
SELECT  XH,
     SYNF,
     XMMC,
       SYYF,
     SYRQ,
     JZBH
  from yssj
 WHERE TRANSLATE(SYNF,'','0123456789') ='' And TRANSLATE(SYRQ,'','0123456789') =''  and TRANSLATE(SYYF,'','0123456789') =''),
  yssj_ylnf as (
      select  XH,
            XMMC,
           ylnf,
       JZBH
    from yssj
    where TRANSLATE(ylnf,'','0123456789') =''
 ),
 yssj_sybhnull(jzbh,bj,nullc) as (
   select max(jzbh),'N',COUNT(*) nullc from yssj where sybh is not null
   union all
   select max(jzbh),'Y',count(*) nullc from yssj where sybh is null
 ),
 yssj_sybhnull2 as (select SYBH,xh,JZBH,(select nullc from yssj_sybhnull b where b.jzbh=a.jzbh and b.bj='N') as FKD,
                 (select nullc from yssj_sybhnull c where c.jzbh=a.jzbh and c.bj='Y') as KD
                  from yssj a
  ),
yssj_zdysybh as (
    select   SYBH,xh,JZBH,zdysybh,count(zdysybh) over(partition by zdysybh) as cou from yssj     
  ),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测自定样品编号
 select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定义样品编号重复',jzbh from yssj_zdysybh where cou>=2
union all 
--自定义样品编号与系统样品编号不能相同
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定义样品编号与系统样品编号相同',jzbh from yssj_zdysybh a
where exists(select 1 from session.jcxxb as b where a.zdysybh=b.sybh)
 union all 
 --检测批次
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'PC' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(PC,'','0123456789') <>''
union all
--检测项目名称
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'XMMC' as cname,'项目名称为空',JZBH from yssj where XMMC IS NULL OR XMMC=''
union all 
--检测样品编号在落地表重复
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号重复',JZBH 
from 
(select xh,jzbh,count(*) over(partition by sybh) as counts from yssj where sybh is not null)
 where counts>=2
union all 
--检测样品编号在落地表存在(样品编号不为空时)
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在落地表不存在',JZBH from yssj 
 where SYBH IS NOT NULL and sybh not in (select sybh from SZPX.T_SZPX_YL_JCXXB)
 union all 
 --检测原料年份
 select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'YLNF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(YLNF,'','0123456789') <>''
 union all
 --检测试验年份
 select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYNF,'','0123456789') <>''
 union all 
 --检测试验月份
 select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYYF,'','0123456789') <>''
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'试验年份为空',JZBH from yssj where (SYNF IS NULL OR SYNF='') AND SYYF IS NOT NULL
union all
--检测试验日期
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYRQ,'','0123456789') <>''
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'试验月份为空',JZBH from yssj where (SYYF IS NULL OR SYYF='') AND SYRQ IS NOT NULL
--检测省份名称
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SFMC' as cname,'无法找到该省份',JZBH from yssj where SFMC NOT IN (SELECT SFMC FROM SZPX.T_DIM_SZPX_YL_SFB WHERE JSRQ>CURRENT DATE AND ZYBJ=1) AND SFMC IS NOT NULL
--检测城市名称
union all
select 'T_SZPX_YL_JCXXB_DL' AS xmmc,xh,'CSMC' as cname,'无法找到该城市',JZBH from yssj where CSMC NOT IN (SELECT CSMC FROM SZPX.T_DIM_SZPX_YL_CSB WHERE JSRQ>CURRENT DATE AND ZYBJ=1) AND CSMC IS NOT NULL
--检测类别名称
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'LBMC' as cname,'无法找到该类别',JZBH from yssj where LBMC NOT IN (SELECT LBMC FROM SZPX.T_DIM_SZPX_YL_LBB WHERE JSRQ>CURRENT DATE AND ZYBJ=1) AND LBMC IS NOT NULL
--检测形态名称
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'XTMC' as cname,'无法找到该形态',JZBH from yssj where XTMC NOT IN (SELECT XTMC FROM SZPX.T_DIM_SZPX_YL_XTB WHERE JSRQ>CURRENT DATE AND ZYBJ=1) AND XTMC IS NOT NULL
--检测等级名称
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'DJMC' as cname,'无法找到该等级',JZBH from yssj where DJMC NOT IN (SELECT DJMC FROM SZPX.T_DIM_SZPX_YL_DJB WHERE JSRQ>CURRENT DATE AND ZYBJ=1) AND DJMC IS NOT NULL
--检测12位码
union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SEWM' as cname,'十二位码无效',JZBH FROM YSSJ A
where NOT EXISTS (SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM ) AND SEWM IS  NOT NULL
union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'DJMC' as cname,'等级名称无法与十二位码关联',JZBH FROM YSSJ A
WHERE NOT EXISTS(SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM AND A.DJMC=B.YYDJMC)
 AND (DJMC IS NOT NULL OR DJMC<>'') AND (SEWM IS NOT NULL OR SEWM <> '')
 union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'YLNF' as cname,'原料年份无法与十二位码关联',JZBH FROM YSSJ A
WHERE NOT EXISTS(SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM AND A.YLNF=trim(char(B.YYNF)))
 AND (YLNF IS NOT NULL OR YLNF<>'') AND (SEWM IS NOT NULL OR SEWM <> '')
  union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'LBMC' as cname,'类别名称无法与十二位码关联',JZBH FROM YSSJ A
WHERE NOT EXISTS(SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM AND A.LBMC=B.YYLBMC)
 AND (LBMC IS NOT NULL OR LBMC<>'') AND (SEWM IS NOT NULL OR SEWM <> '')
   union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'XTMC' as cname,'形态名称无法与十二位码关联',JZBH FROM YSSJ A
WHERE NOT EXISTS(SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM AND A.XTMC=B.YYKBMC)
 AND (XTMC IS NOT NULL OR XTMC<>'') AND (SEWM IS NOT NULL OR SEWM <> '')
    union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'CSMC' as cname,'城市名称无法与十二位码关联',JZBH FROM YSSJ A
WHERE NOT EXISTS(SELECT 1 FROM YYZY.T_YYZY_YYZDBMX B WHERE A.SEWM=B.YYDM AND A.CSMC=B.YYCDMC)
 AND (CSMC IS NOT NULL OR CSMC<>'') AND (SEWM IS NOT NULL OR SEWM <> '')
 union all
SELECT 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'YYBWMC' as cname,'烟叶部位名称填写错误，不是 B，C，X',JZBH FROM YSSJ A
WHERE YYBWMC IS NOT  NULL AND YYBWMC NOT IN('B','C','X')
union all
--检测试验日期是否有效
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
WHERE  integer(SYYF)=2 and (mod(integer(synf),4)>0 or (mod(integer(synf),400)<>0 and mod(integer(synf),100)=0)) and integer(SYRQ) not between 1 and 28 
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF)=2 and mod(integer(synf),4)=0 and mod(integer(synf),100)<>0 and integer(SYRQ) not between 1 and 29
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(4),(6),(9),(11)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 30
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(1),(3),(5),(6),(8),(10),(12)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 31
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'实验月份无效',JZBH from yssj_rq
where integer(syyf) not between 1 and 12
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'年份小于1980',JZBH
from yssj_rq where integer(synf)<1980
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'YLNF' as cname,'年份小于1980',JZBH
from yssj_ylnf where integer(YLNF)<1980
union all 
 select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH  from yssj_sybhnull2 b
 where FKD>0 and kd>0 and sybh is null
 UNION ALL
  select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SJYTMC' as cname,'数据用途无效',JZBH  from yssj b
 where SJYTMC NOT IN (SELECT A.SJYTMC FROM SZPX.T_DIM_SZPX_SJYTB A WHERE A.ZYBJ=1) AND SJYTMC IS NOT NULL
 )
--将结果插入结果临时表
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

----------------------------------开始检测原料指标表----------------------------------------------
SET OUT_JG='开始检测原料指标表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj_tmp as (
SELECT JZXH, 
     SYBH,
     JZBH
  from SZPX.T_SZPX_YL_ZBB_DL
 WHERE JZBH=V_JZBH),
 yssj as (
 SELECT JZXH AS XH,
     SYBH,
     JZBH,
     count(sybh) over(partition by sybh) as cs
  from yssj_tmp),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
  --检测指标表
select 'T_SZPX_YL_ZBB_DL' AS SYBH,xh,'SYBH' as cname,'样品编号无效',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select zdysybh from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'')
 UNION all
 select 'T_SZPX_YL_ZBB_DL' AS SYBH,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj 
 WHERE SYBH IS NULL OR SYBH=''
 --样品编号是否重复
 union all
 select 'T_SZPX_YL_ZBB_DL' AS SYBH,xh,'SYBH' as cname,SYBH||' : 样品编号重复',JZBH from yssj 
where cs>=2)
--将结果插入结果临时表  
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

----------------------------------开始检测原料光谱信息表----------------------------------------------
SET OUT_JG='开始检测原料光谱信息表';


insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
       GPWJM,
     ZDYGPWJM,
     JCJXMC,
     GPLXMC,
     JZXH as xh,
     JZBH
  from SZPX.T_SZPX_YL_GPXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_cfgp as
 (
  SELECT XH,
     GPWJM,
     JZBH
  from yssj
 where (gpwjm is not null or gpwjm <>'') 
 ),
 yssj_zdygpwjm as 
 (select xh,jzbh,zdygpwjm,count(zdygpwjm) over(partition by zdygpwjm) as cou from yssj),
yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
--自定义光谱文件名不能在同文档内不允许重复
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'自定义光谱文件名重复',JZBH from  yssj_zdygpwjm where cou>=2
union all
--检测光谱文件名
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,'系统光谱文件名不存在',JZBH from yssj a where  not exists (select 1 from SZPX.T_SZPX_YL_GPXXB b where a.gpwjm=b.gpwjm) and a.gpwjm is not null
union all
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'系统光谱文件名和自定义光谱文件名不能同时为空',JZBH from yssj where gpwjm is null and zdygpwjm is  null
union all
--检测样品编号
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh is null
union all
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在原料基础信息表不存在',JZBH from yssj 
 where sybh NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'') and sybh is not null
--检测 仪器名称
union all
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测仪器',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 AND JXLB=2) AND JCJXMC IS NOT NULL
--检测 光谱类型
union all
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'GPLXMC' as cname,'无法找到该光谱类型',JZBH from yssj where GPLXMC NOT IN (SELECT GPLXMC FROM SZPX.T_DIM_SZPX_GPLXB WHERE ZYBJ=1) AND GPLXMC IS NOT NULL
--检测光谱文件名是否重复
union all
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,GPWJM||' :光谱文件名重复',JZBH from yssj_cfgp 
 where (GPWJM) IN (select GPWJM from yssj_cfgp group by gpwjm having count(*)>1))
 
 --将结果插入结果临时表
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


----------------------------------开始检测原料检测指标表--------------------------------------------
SET OUT_JG='开始检测原料检测指标表';
insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
       gpwjm, 
       JCLXMC,
     JCJXMC,
     JZXH as xh,
     JZBH 
  from SZPX.T_SZPX_YL_JCZBB_DL 
 WHERE JZBH=V_JZBH),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名
select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名无效',JZBH from yssj
 where JCLXMC LIKE '%光谱%' and gpwjm NOT IN (select gpwjm from SESSION.GPXXB)
 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号不存在',JZBH  from yssj
 where JCLXMC not LIKE '%光谱%' and sybh NOT IN (select sybh from SESSION.jcXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'')
  --检测试验编号是否为空
  union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号为空',JZBH from yssj where JCLXMC not LIKE '%光谱%' and (SYBH IS NULL OR SYBH='')
 --检测光谱文件名是否为空
 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where JCLXMC LIKE '%光谱%' and (GPWJM IS NULL OR GPWJM='')

 --检测 检测类型
 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'检测类型为空',JZBH from yssj where (jclxmc is null or jclxmc ='')
 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'无法找到该检测类型',JZBH from yssj where JCLXMC NOT IN (SELECT JCLXMC FROM SZPX.T_DIM_SZPX_JCLXB WHERE ZYBJ=1) AND JCLXMC IS NOT NULL
 --检测 仪器名称
union all
select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测仪器',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 /*AND JXLB=2*/) AND JCJXMC IS NOT NULL
--检测检测仪器是否和检测类型关联
union all
select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'该检测仪器与该检测类型不对应',JZBH from yssj where JCJXMC 
NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A,SZPX.T_DIM_SZPX_JCLXB B WHERE A.JCLX=B.ID AND A.ZYBJ=1 
/*AND A.JXLB=2*/ AND B.ZYBJ=1 AND B.JCLXMC=YSSJ.JCLXMC) AND JCJXMC IS NOT NULL AND JCLXMC IS NOT NULL
 
--光谱检测时 光谱文件名必填并且正确，SYBH可以不填，如果填写必须正确
--非光谱检测时 ＳＹＢＨ必填并且正确，ＧＰＷＪＭ可以不填，如果填必须是该SYBH下的GPWJM

 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,CASE WHEN JCLXMC not LIKE '%光谱%' THEN 'GPWJM' ELSE 'SYBH' END as cname,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN '光谱文件名在该试验编号下无效' ELSE '试验编号与该光谱文件名无关联' END AS JCJG,JZBH 
 from yssj where (SYBH IS NOT NULL OR SYBH <>'') AND (GPWJM IS NOT NULL OR GPWJM<>'') 
 and not exists (select 1 from SESSION.GPXXB as A where a.gpwjm=yssj.gpwjm and (a.sybh=yssj.sybh or exists(select 1 from session.zdysybh c where (c.zdysybh=yssj.sybh or yssj.sybh=c.sybh) and (a.sybh=c.sybh or a.sybh=c.zdysybh) and (zdysybh is not null or zdysybh<>''))))
 
/* union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名在该试验编号下无效',JZBH from yssj where JCLXMC not LIKE '%光谱%' AND (GPWJM IS NOT NULL OR GPWJM<>'') and not exists (select 1 from SESSION.GPXXB as A where a.gpwjm=yssj.gpwjm and a.sybh=yssj.sybh)
 
 union all
 select 'T_SZPX_YL_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号与该光谱文件名无关联',JZBH from yssj where JCLXMC  LIKE '%光谱%' AND (GPWJM IS NOT NULL OR GPWJM<>'') and not exists (select 1 from SESSION.GPXXB as A where a.sybh=yssj.sybh AND a.gpwjm=yssj.gpwjm )*/

 )
 --查询结果插入结果临时表
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

----------------------------------开始检测原料主成份表--------------------------------------------
SET OUT_JG='开始检测原料主成份表';


insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM, 
     GPMXMC,
     JM_YC,
     JZBH
  from SZPX.T_SZPX_YL_ZCFB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as 
 (
     SELECT XH,
     GPWJM,
     JZBH
  from yssj
 WHERE (GPWJM IS not NULL OR GPWJM<>'')
 ),
 yssj_gpdg as
 (
  select xh,
       gpwjm,
       jzbh
    from yssj_gp
    where locate('/',gpwjm)>0
 ) ,
 yssj_gpdgks(xh,gpwjm,jzbh) as (
 SELECT XH,
     GPWJM,
     JZBH
  from yssj_gpdg
  union all
  select xh,substr(gpwjm,locate('/',gpwjm)+1) as gpwjm,jzbh
  from yssj_gpdgks
  where locate('/',gpwjm)>0),
 yssj_gpdgjs (xh,gpwjm,jzbh) as
 (select xh,case when locate('/',gpwjm)>0
            then substr(gpwjm,1,locate('/',gpwjm)-1)
            else gpwjm
            end as gpwjm,jzbh
  from yssj_gpdgks
  where gpwjm<>''),
 yssj_gpwj(xh,gpwjm,jzbh) as
 (
  select xh,gpwjm,jzbh from yssj_gp
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
    and locate('/',gpwjm)=0
  union all
  select xh,gpwjm,jzbh from yssj_gpdgjs
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
 ),
 yssj_cf as (
  select xh,
       GPWJM, 
         GPMXMC,
         JM_YC,
         JZBH,
       count(*) over(partition by GPWJM,GPMXMC,JM_YC) as counts
    from yssj
 ),
 --------------------------------------------------------------------------
 WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC,a.jzxh as xh,a.jzbh
          from SZPX.T_SZPX_YL_ZCFB_DL AS A
     where a.JZBH=V_JZBH
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,gpwjm,GPMXMC,JM_YC,xh,jzbh FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE('/', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 AS A WHERE LOCATE('/', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,  CASE WHEN LOCATE('/', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE('/', GPWJM) - 1) ELSE GPWJM END AS GPWJM,
     GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 WHERE GPWJM <> '' 
),
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh from (
     SELECT A.GPJWJM,
        a.GPMXMC,
        a.JM_YC,
        trim(char(REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, C.GPWJM||'/'))),'</A>',''),'<A>',''))) as GPWJM,
        a.xh,
        a.jzbh
       FROM WJMCZ_TMP2 AS A
    LEFT JOIN SZPX.T_SZPX_YL_GPXXB_DL AS C
           on a.gpwjm=c.gpwjm
       or a.gpwjm=c.zdygpwjm
     WHERE C.GPWJM IS NOT NULL AND C.ZDYGPWJM IS NOT NULL
     group by a.gpjwjm,a.GPMXMC,a.JM_YC,a.xh,a.jzbh)
),
 ---------------------------------------------------------------------------
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名，光谱模型名称，建模或预测不允许重复
 SELECT 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH 
 from (SELECT XH,JZBH,COUNT(*) OVER(PARTITION BY GPWJM,GPMXMC,JM_YC) AS COU FROM WJMCZ1) as t
 WHERE COU>=2
 union all
  --检测建模预测是否为空
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'JM_YC' as cname,'建模预测为空',JZBH from yssj where JM_YC IS NULL OR JM_YC=''
union all
   --检测光谱模型名称是否为空
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'光谱模型名称为空',JZBH from yssj where GPMXMC IS NULL OR GPMXMC=''
union all
 --检测光谱文件名是否为空
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where GPWJM IS NULL OR GPWJM=''
--检测 光谱模型
union all
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'无法找到该光谱模型',JZBH from yssj where GPMXMC NOT IN (SELECT GPMXMC FROM SZPX.T_DIM_SZPX_GPMXB WHERE ZYBJ=1) AND GPMXMC IS NOT NULL
--检测光谱文件名是否存在
union all
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gp
where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB) and locate('/',gpwjm)=0
union all
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gpdgjs 
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB)
union all
--检测光谱文件名，光谱模型名称，建模或预测不允许重复
select 'T_SZPX_YL_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH from yssj_cf
where counts>=2      
        )
--查询结构导入结果临时表
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;
  
----------------------------------开始检测原料评吸指标表(老版)-------------------------------------------
SET OUT_JG='开始检测原料评吸指标表(老版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
     JZXH as xh,
     JZBH
  from SZPX.T_SZPX_YL_PXZBB_LBDL
 WHERE JZBH=V_JZBH
 ),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as
(
 --检测样品编号
select 'T_SZPX_YL_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj 
where sybh is null or sybh =''
union all
--检测样品编号
select 'T_SZPX_YL_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where SYBH NOT IN (select SYBH from SESSION.JCXXB) AND (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'') )
 --将结果插入临时表
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
commit;

----------------------------------开始检测原料评吸指标表(新版)------------------------------------------
SET OUT_JG='开始检测原料评吸指标表(新版)';

--检测样品编号
insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj_tmp as (
SELECT SYBH,
     JZXH,
     JZBH
  from SZPX.T_SZPX_YL_PXZBB_XBDL
 WHERE JZBH=V_JZBH),
 yssj as (
 SELECT JZXH AS XH,
     SYBH,
     JZBH
  from yssj_tmp)
select 'T_SZPX_YL_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where SYBH NOT IN (select SYBH from SESSION.JCXXB) AND (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'');
COMMIT;

----------------------------------检测级联关系-----------------------------------------

SET OUT_JG='检测级联关系';
insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     SFMC,
     CSMC,
     LBMC,
     DJMC,
     JZBH
  from SZPX.T_SZPX_YL_JCXXB_DL
 WHERE JZBH=V_JZBH
)
,
yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
--检测省份城市级联关系
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'CSMC' as cname, '该城市无法与省份进行关联',JZBH from yssj 
 where (SFMC,CSMC) NOT IN (SELECT SFMC,CSMC FROM SZPX.T_DIM_SZPX_YL_CSB WHERE JSRQ>CURRENT DATE AND ZYBJ=1)
   and (CSMC IS NOT NULL and SFMC IS NOT NULL)
UNION ALL
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'SFMC' as cname, '省份名称为空，城市名称无法进行关联' ,JZBH from yssj 
 where CSMC IS NOT NULL and SFMC IS NULL
 --检测类别等级级联关系
union all
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'DJMC' as cname,'该等级无法与类别进行关联',JZBH from yssj 
 where (LBMC,DJMC) NOT IN ( 
                            select a.LBMC,b.DJMC 
                              from SZPX.T_DIM_SZPX_YL_LBB as a,
                                 SZPX.T_DIM_SZPX_YL_DJB as b
                             where a.id=b.lbid
                 and a.jsrq>current date 
                 and b.jsrq>current date 
                 and a.zybj=1
                 and b.zybj=1
               )
   and (DJMC IS NOT NULL AND LBMC IS NOT NULL) 
UNION ALL
select 'T_SZPX_YL_JCXXB_DL' AS XMMC,xh,'DJMC' as cname,'类别名称为空，等级名称无法进行关联',JZBH from yssj
 where LBMC IS NULL AND DJMC IS NOT NULL)
--查询结果插入结果临时表
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


--检测新增字段中的数字类型的字段
SET OUT_JG='动态检测新增字段-原料基础信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID','SFJC','SYBH','XMID','YLNF','SYNF','SYYF','SYRQ','SFID','CSID','XZ',
                          'YYLBID','YYXTID','YYBWMC','YYDDJMC','YYDJID','SEWM','YYPZMC','YYYSMC','PC','CJSJ','SJYTID');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID','SFJC','SYBH','XMID','YLNF','SYNF','SYYF','SYRQ','SFID','CSID','XZ',
                          'YYLBID','YYXTID','YYBWMC','YYDDJMC','YYDJID','SEWM','YYPZMC','YYYSMC','PC','CJSJ','SJYTID'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-原料指标表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_ZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','YTLX','YLJD','YLWD','YLHB','YLGY','YLGYCS','YLGYCL','YLYPCL','YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_ZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','YTLX','YLJD','YLWD','YLHB','YLGY','YLGYCS','YLGYCL','YLYPCL','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;
--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-原料检测指标表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','JCLXID','GPID','JCR','JCJXID','JCSJ','BZ','YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','JCLXID','GPID','JCR','JCJXID','JCSJ','BZ','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-原料主成份表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','GPID','GPMXID','JM_YC','YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','GPID','GPMXID','JM_YC','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-原料评吸指标表(老版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH', 'JZXH', 'SYBH','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','ZF','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_YL_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','NXX','PY','XF','ZF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,                 --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH', 'JZXH', 'SYBH','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','ZF','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_YL_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','NXX','PY','XF','ZF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-原料评吸指标表(新版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD from (
SELECT NAME,TBNAME,TBCREATOR
  FROM sysibm.syscolumns 
 where TBNAME='T_SZPX_YL_PXZBB_XBDL' 
   AND TBCREATOR='SZPX'
   AND NAME NOT IN ('JZBH','JZXH','SYBH','ID','PXLX','LBXQZ','LBXQL','LBZQ','LBJT','LBCJX','LBYW','LBZF','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
SELECT NAME,'T_SZPX_YL_PXZBB_XBDL' as TBNAME,TBCREATOR
  FROM sysibm.syscolumns 
 where TBNAME='T_SZPX_YL_PXZBB' 
   AND TBCREATOR='SZPX'
   AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
   AND NAME NOT IN ('JZBH','ID','SYBH','PXLX','LBXQZ','LBXQL','LBZQ','LBJT','LBCJX','LBYW','LBZF','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ'));


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,                 --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('JZBH','JZXH','SYBH','ID','PXLX','LBXQZ','LBXQL','LBZQ','LBJT','LBCJX','LBYW','LBZF','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_YL_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('JZBH','ID','SYBH','PXLX','LBXQZ','LBXQL','LBZQ','LBJT','LBCJX','LBYW','LBZF','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-原料光谱信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','GPWJM','GPLXID','JCSJ','JCJXID','JCR','SFCZGPWJ','BZ','XH','YPID','CJSJ');

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_YL_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','SYBH','GPWJM','GPLXID','JCSJ','JCJXID','JCR','SFCZGPWJ','BZ','XH','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

ELSE 
COMMIT;
END IF;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--检测产品
IF V_XMLX='PRODUCT' OR V_XMLX='product' THEN 

DELETE FROM SESSION.JCXXB;
DELETE FROM SESSION.GPXXB;
DELETE FROM SESSION.ZDYSYBH;
--基础信息
INSERT INTO SESSION.JCXXB (SYBH,XMMC,XMID,XMLX)
SELECT DISTINCT SYBH,XMMC,XMID,XMLX FROM (
SELECT A.SYBH, B.XMMC, B.ID as XMID,B.XMLX
  from SZPX.T_SZPX_CP_JCXXB AS A,
       SZPX.T_DIM_SZPX_XMB AS B
 WHERE A.XMID=B.ID);
COMMIT;

--自定样品编号

INSERT INTO SESSION.ZDYSYBH(ZDYSYBH,sybh)
SELECT A.ZDYSYBH,a.sybh
  from SZPX.T_SZPX_CP_JCXXB_DL AS A
  where a.JZBH=V_JZBH
;
COMMIT;

--光谱信息
INSERT INTO SESSION.GPXXB (ID,SYBH,GPWJM)
select A.YPID,A.SYBH,B.GPWJM
  from SZPX.T_SZPX_CP_JCXXB AS A,
       SZPX.T_SZPX_CP_GPXXB AS B
 WHERE A.SYBH=B.SYBH
   AND B.GPWJM IS NOT NULL
   AND B.GPWJM <>''
       UNION ALL
 select CAST (NULL AS INTEGER) AS ID,B.SYBH,B.ZDYGPWJM
  from SZPX.T_SZPX_CP_GPXXB_DL AS B
 WHERE JZBH=V_JZBH
   and B.ZDYGPWJM IS NOT NULL
   AND B.ZDYGPWJM <>'';
COMMIT;

--------------------------------------开始检测产品基础信息表---------------------------------------------------------------
SET OUT_JG='开始检测产品基础信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj(XH,
     XMMC,
     SYBH,
     ZDYSYBH,
     XHTXBS,
     SYNF,
     SYYF,
     SYRQ,
     CPNF,
     PPMC,
     CPGG,
     SCCJ,
     JLMC,
     YHMC,
     HBJY,
     HBCO,
     HBYJ,
     JZBH) as (
SELECT JZXH AS XH,
     XMMC,
     SYBH,
     ZDYSYBH,
     XHTXBS,
     SYNF,
     SYYF,
     SYRQ,
     CPNF,
     PPMC,
     CPGG,
     SCCJ,
     JLMC,
     YHMC,
     HBJY,
     HBCO,
     HBYJ,
     JZBH
  from SZPX.T_SZPX_CP_JCXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_rq as (
SELECT  XH,
     SYNF,
     XMMC,
       SYYF,
     SYRQ,
     JZBH
  from yssj
 WHERE  TRANSLATE(SYNF,'','0123456789') ='' And TRANSLATE(SYRQ,'','0123456789') =''  and TRANSLATE(SYYF,'','0123456789') ='')
 ,
 yssj_jy as (
SELECT  XH,
     XHTXBS, 
     CAST(HBJY AS DECIMAL(18,2)) AS HBJY,
     JZBH
  from yssj
   where TRANSLATE(HBJY,'','-.0123456789')=''),
yssj_co as (
SELECT  XH,
     XMMC,
     XHTXBS, 
     CAST(HBCO AS DECIMAL(18,2)) AS HBCO,
     JZBH
  from yssj
 WHERE TRANSLATE(HBCO,'','-.0123456789')=''),
 yssj_yj as (
SELECT XH,
     XMMC,
     XHTXBS, 
     CAST(HBYJ AS DECIMAL(18,2)) AS HBYJ,
     JZBH
  from yssj
 WHERE TRANSLATE(HBYJ,'','-.0123456789')='')
 ,
 yssj_sybhnull(jzbh,bj,nullc) as (
   select max(jzbh),'N',COUNT(*) nullc from yssj where sybh is not null
   union all
   select max(jzbh),'Y',count(*) nullc from yssj where sybh is null
 ),
 yssj_sybhnull2 as(select SYBH,xh,JZBH,(select nullc from yssj_sybhnull b where b.jzbh=a.jzbh and b.bj='N') as FKD,
                 (select nullc from yssj_sybhnull c where c.jzbh=a.jzbh and c.bj='Y') as KD
                  from yssj a
  ),
  yssj_zdysybh as (
    select   SYBH,xh,JZBH,zdysybh,count(zdysybh) over(partition by zdysybh) as cou from yssj     
  ),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as (

  --检测自定样品编号
 select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定样品编号重复',jzbh from yssj_zdysybh where cou>=2
union all 
--自定义样品编号与系统样品编号不能相同
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定义样品编号与系统样品编号相同',jzbh from yssj_zdysybh a
where exists(select 1 from session.jcxxb as b where a.zdysybh=b.sybh)
 union all 
 --检测产品基础信息表项目名称是否为空
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'XMMC' as cname,'项目名称为空',JZBH from yssj where XMMC IS NULL OR XMMC=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在落地表不存在',JZBH from yssj 
 where sybh not in (select sybh from SZPX.T_SZPX_CP_JCXXB) and sybh is not null
union all
--检测小盒条形码信息
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'XHTXBS' as cname,'无法找到条形码',JZBH from yssj where XHTXBS NOT IN (SELECT XHTXBS FROM SZPX.T_SZPX_CP_GJJJY) AND XHTXBS IS NOT NULL
union all
--检测试验年份
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYNF,'','0123456789') <>''
union all
--检测试验月份
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYYF,'','0123456789') <>''
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'试验年份为空',JZBH from yssj where (SYNF IS NULL OR SYNF='') AND SYYF IS NOT NULL
union all
--检测试验日期
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYRQ,'','0123456789') <>''
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'试验月份为空',JZBH from yssj where ((SYYF IS NULL OR SYYF='') AND SYRQ IS NOT NULL) AND SYRQ IS NOT NULL
union all
--检测产品年份
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'CPNF' as cname,'包含无效字符'  ,JZBH from yssj where TRANSLATE(CPNF,'','0123456789')<>''
union all
--检测产品牌号信息
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'PPMC' as cname,'无法找到品牌',JZBH from yssj where PPMC NOT IN (SELECT PPMC FROM SZPX.T_DIM_SZPX_CP_PPB WHERE JSRQ >= CURRENT DATE AND ZYBJ=1) AND PPMC IS NOT NULL
union all
--检测生产厂与小盒条形码级联关系
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SCCJ' as cname,'该生产厂无法与小盒条形码关联',JZBH from yssj where (XHTXBS,SCCJ) NOT IN (SELECT XHTXBS,SCQYMC FROM SZPX.T_SZPX_CP_GJJJY) AND SCCJ IS NOT NULL AND XHTXBS IS NOT NULL
union all
--检测产品规格与小盒条形码级联关系
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'CPGG' as cname,'该产品规格无法与小盒条形码关联',JZBH from yssj where (XHTXBS,CPGG) NOT IN (SELECT XHTXBS,GJJJYMC FROM SZPX.T_SZPX_CP_GJJJY) AND CPGG IS NOT NULL AND XHTXBS IS NOT NULL
union all
--检测产品品牌与条盒条形码级联关系
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'PPMC' as cname,'该品牌无法与条盒条形码关联',JZBH from yssj where (XHTXBS,PPMC) NOT IN (SELECT XHTXBS,PPMC FROM SZPX.T_SZPX_CP_GJJJY) AND PPMC IS NOT NULL AND XHTXBS IS NOT NULL
union all
--检测产品价类信息
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'JLMC' as cname,'无法找到该价类',JZBH from yssj where JLMC NOT IN (SELECT JLMC FROM SZPX.T_DIM_SZPX_CP_JLB WHERE JSRQ>=CURRENT DATE and ZYBJ=1) AND JLMC IS NOT NULL
union all
--检测价类与条形码级联关系
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'JLMC' as cname,'该价类无法与小盒条形码关联',JZBH from yssj where (XHTXBS,JLMC) NOT IN (SELECT XHTXBS,JLMC FROM SZPX.T_SZPX_CP_GJJJY) AND JLMC IS NOT NULL AND XHTXBS IS NOT NULL
union all
--检测产品烟号信息
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'YHMC' as cname,'无法找到烟号名称',JZBH from yssj where YHMC NOT IN (SELECT YHMC FROM SZPX.T_DIM_SZPX_CP_YHB WHERE ZYBJ=1) AND YHMC IS NOT NULL
union all
--检测盒标焦油
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBJY' as cname,'盒标焦油值输入错误'  ,JZBH from yssj where TRANSLATE(HBJY,'','0123456789-.')<>''
union all
--检测盒标CO
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBCO' as cname,'盒标CO值输入错误'  ,JZBH from yssj where TRANSLATE(HBCO,'','0123456789-.')<>''
union all
--检测盒标烟碱
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBYJ' as cname,'盒标烟碱值输入错误'  ,JZBH from yssj where TRANSLATE(HBYJ,'','0123456789-.')<>''
--检测试验日期是否有效
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
WHERE  integer(SYYF)=2 and (mod(integer(synf),4)>0 or (mod(integer(synf),400)<>0 and mod(integer(synf),100)=0)) and integer(SYRQ) not between 1 and 28 
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF)=2 and mod(integer(synf),4)=0 and mod(integer(synf),100)<>0 and integer(SYRQ) not between 1 and 29
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(4),(6),(9),(11)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 30
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(1),(3),(5),(6),(8),(10),(12)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 31
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'实验月份无效',JZBH from yssj_rq
where integer(syyf) not between 1 and 12
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'年份小于1980',JZBH
from yssj_rq where integer(synf)<1980
--检测盒标焦油与条形码级联关系
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBJY' as cname,'焦油盒标值无法与小盒条形码关联',JZBH from yssj_jy where (XHTXBS,HBJY) NOT IN (SELECT XHTXBS,JYHL FROM SZPX.T_SZPX_CP_GJJJY) AND HBJY IS NOT NULL AND XHTXBS IS NOT NULL
--检测盒标CO与条形码级联关系
union all
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBCO' as cname,'CO盒标值无法与小盒条形码关联',JZBH from yssj_co where (XHTXBS,HBCO) NOT IN (SELECT XHTXBS,YQCOL FROM SZPX.T_SZPX_CP_GJJJY) AND HBCO IS NOT NULL AND XHTXBS IS NOT NULL
union all
--检测盒标烟碱与条形码级联关系
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'HBYJ' as cname,'烟碱盒标值无法与条盒条形码关联',JZBH from yssj_yj where (XHTXBS,HBYJ) NOT IN (SELECT XHTXBS,YQYJL FROM SZPX.T_SZPX_CP_GJJJY) AND HBYJ IS NOT NULL AND XHTXBS IS NOT NULL 
union all 
 select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH  from yssj_sybhnull2 b
 where FKD>0 and kd>0 and sybh is null
 union all 
--检测样品编号在落地表重复
select 'T_SZPX_CP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号重复',JZBH 
from 
(select xh,jzbh,count(*) over(partition by sybh) as counts from yssj where sybh is not null)
 where counts>=2
 )

select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


--------------------------------------开始检测评吸指标表(老版)---------------------------------------------------------------
SET OUT_JG='开始检测评吸指标表(老版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     SYBH,
     JZBH
  from SZPX.T_SZPX_CP_PXZBB_LBDL
 WHERE JZBH=V_JZBH),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测样品编号
select 'T_SZPX_CP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where SYBH IS NULL OR SYBH=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_CP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;
--------------------------------------开始检测评吸指标表(新版)--------------------------------------------------------------
SET OUT_JG='开始检测评吸指标表(新版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     SYBH,
     JZBH
  from SZPX.T_SZPX_CP_PXZBB_XBDL
 WHERE JZBH=V_JZBH),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
 --检测样品编号
select 'T_SZPX_CP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where SYBH IS NULL OR SYBH=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_CP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 
 select TNAME,RNUM,CNAME,JCJG,JZBH from  yssj_jg;
COMMIT;

--------------------------------------开始检测产品检测指标表-------------------------------------------------------------
SET OUT_JG='开始检测产品检测指标表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM, 
     SYBH,
     JCLXMC,
     JCJXMC,
     JZBH
  from SZPX.T_SZPX_CP_JCZBB_DL
 WHERE JZBH=V_JZBH),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名是否为空
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where JCLXMC LIKE '%光谱%' and (GPWJM IS NULL OR GPWJM='')
 --检测试验编号是否为空
union all
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号为空',JZBH from yssj where JCLXMC not LIKE '%光谱%' and (sybh IS NULL OR sybh='')
union all
--检测光谱文件名是否存在
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj 
 where JCLXMC LIKE '%光谱%' and (GPWJM) NOT IN (select GPWJM from SESSION.GPXXB)
 union all
--检测试验编号是否存在
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号不存在',JZBH from yssj 
 where JCLXMC not LIKE '%光谱%' and (sybh) NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'')
union all
--检测 检测类型
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'无法找到该检测类型',JZBH from yssj where JCLXMC NOT IN (SELECT JCLXMC FROM SZPX.T_DIM_SZPX_JCLXB WHERE ZYBJ=1) AND JCLXMC IS NOT NULL
union all
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'检测类型为空',JZBH from yssj where (jclxmc is null or jclxmc ='')
union all
--检测 检测机型
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测机型',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 /*AND JXLB=2*/) AND JCJXMC IS NOT NULL
--检测检测仪器是否和检测类型关联
union all
select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'该检测仪器与该检测类型不对应',JZBH from yssj where JCJXMC 
NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A,SZPX.T_DIM_SZPX_JCLXB B WHERE A.JCLX=B.ID AND A.ZYBJ=1 
/*AND A.JXLB=2*/ AND B.ZYBJ=1 AND B.JCLXMC=YSSJ.JCLXMC) AND JCJXMC IS NOT NULL AND JCLXMC IS NOT NULL


--光谱检测时 光谱文件名必填并且正确，SYBH可以不填，如果填写必须正确
--非光谱检测时 ＳＹＢＨ必填并且正确，ＧＰＷＪＭ可以不填，如果填必须是该SYBH下的GPWJM

 union all
 select 'T_SZPX_CP_JCZBB_DL' AS XMMC,xh,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN 'GPWJM' ELSE 'SYBH' END as cname,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN '光谱文件名在该试验编号下无效' ELSE '试验编号与该光谱文件名无关联' END AS JCJG,
    JZBH 
 from yssj 
 where (SYBH IS NOT NULL OR SYBH <>'') AND (GPWJM IS NOT NULL OR GPWJM<>'') 
 and not exists (select 1 from SESSION.GPXXB as A where a.gpwjm=yssj.gpwjm and (a.sybh=yssj.sybh or exists(select 1 from session.zdysybh c where (c.zdysybh=yssj.sybh or yssj.sybh=c.sybh) and (a.sybh=c.sybh or a.sybh=c.zdysybh) and (zdysybh is not null or zdysybh<>''))))


)
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

--------------------------------------开始检测产品光谱信息表----------------------------------------------------------
SET OUT_JG='开始检测产品光谱信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
     GPWJM,
     ZDYGPWJM,
       JCJXMC,
     GPLXMC,
     JZXH as xh,
     SFCZGPWJ,
     JZBH
  from SZPX.T_SZPX_CP_GPXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as
 (
  SELECT XH,
         GPWJM,
         JZBH
  from yssj
 WHERE gpwjm is not null or gpwjm <>''
 ),
 yssj_zdygpwjm as 
 (select xh,jzbh,zdygpwjm,count(zdygpwjm) over(partition by zdygpwjm) as cou from yssj),
yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
--自定义光谱文件名不能在同文档内不允许重复
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'自定义光谱文件名重复',JZBH from  yssj_zdygpwjm where cou>=2
union all
 --检测光谱文件名
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,'系统光谱文件名不存在',JZBH from yssj a where  not exists (select 1 from SZPX.T_SZPX_CP_GPXXB b where a.gpwjm=b.gpwjm) and a.gpwjm is not null
union all
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'系统光谱文件名和自定义光谱文件名不能同时为空',JZBH from yssj where gpwjm is null and zdygpwjm is  null 
union all
 --检测样品编号
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh is null or SYBH=''
union all
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在产品基础信息表不存在',JZBH from yssj 
 where sybh NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'') and sybh is not null
union all
--检测 仪器名称
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测仪器',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 AND JXLB=2) AND JCJXMC IS NOT NULL
union all
--检测 光谱类型
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'GPLXMC' as cname,'无法找到该光谱类型',JZBH from yssj where GPLXMC NOT IN (SELECT GPLXMC FROM SZPX.T_DIM_SZPX_GPLXB WHERE ZYBJ=1) AND GPLXMC IS NOT NULL
UNION ALL
--检测光谱文件名是否重复
select 'T_SZPX_CP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,GPWJM||' :光谱文件名重复',JZBH from yssj_gp 
 where (GPWJM) IN (select GPWJM from yssj_gp group by gpwjm having count(*)>1)
)
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;

COMMIT;

--------------------------------------开始检测产品主成份表--------------------------------------------------------
SET OUT_JG='开始检测产品主成份表';


insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM,
       GPMXMC,
       JM_YC,     
     JZBH
  from SZPX.T_SZPX_CP_ZCFB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as
 (
  select xh,
       gpwjm,
       jzbh
    from yssj
    where gpwjm is not null or gpwjm<>''
 ),
 yssj_gpdg as
 (
  select xh,
       gpwjm,
       jzbh
     from yssj_gp
     where locate('/',gpwjm)>0 
 ),
 yssj_gpdgks(xh,gpwjm,jzbh) as (
 SELECT  XH,
     GPWJM,
     JZBH
  from yssj_gpdg
  union all
  select xh,substr(gpwjm,locate('/',gpwjm)+1) as gpwjm,jzbh
  from yssj_gpdgks
  where locate('/',gpwjm)>0),
 yssj_gpdgjs (xh,gpwjm,jzbh) as
 (select xh,case when locate('/',gpwjm)>0
            then substr(gpwjm,1,locate('/',gpwjm)-1)
            else gpwjm
            end as gpwjm,jzbh
  from yssj_gpdgks
  where gpwjm<>''),
  yssj_gpwj (xh,gpwjm,jzbh) as
  (
    select xh,gpwjm,jzbh from yssj_gp
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
    and locate('/',gpwjm)=0
  union all
  select xh,gpwjm,jzbh from yssj_gpdgjs
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
  ),
  yssj_cf as
  (
  select XH,
         GPWJM,
           GPMXMC,
           JM_YC,     
         JZBH,
       count(*) over(partition by GPWJM,GPMXMC,JM_YC) as counts
       from yssj
  ),
 --------------------------------------------------------------------------
 WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC,a.jzxh as xh,a.jzbh
          from SZPX.T_SZPX_CP_ZCFB_DL AS A
     where a.JZBH=V_JZBH
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,gpwjm,GPMXMC,JM_YC,xh,jzbh FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE('/', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 AS A WHERE LOCATE('/', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,  CASE WHEN LOCATE('/', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE('/', GPWJM) - 1) ELSE GPWJM END AS GPWJM,
     GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 WHERE GPWJM <> '' 
),
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh from (
     SELECT A.GPJWJM,
        a.GPMXMC,
        a.JM_YC,
        trim(char(REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, C.GPWJM||'/'))),'</A>',''),'<A>',''))) as GPWJM,
        a.xh,
        a.jzbh
       FROM WJMCZ_TMP2 AS A
    LEFT JOIN SZPX.T_SZPX_CP_GPXXB_DL AS C
           on a.gpwjm=c.gpwjm
       or a.gpwjm=c.zdygpwjm
     WHERE C.GPWJM IS NOT NULL AND C.ZDYGPWJM IS NOT NULL
     group by a.gpjwjm,a.GPMXMC,a.JM_YC,a.xh,a.jzbh)
),
 ---------------------------------------------------------------------------
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名，光谱模型名称，建模或预测不允许重复
 SELECT 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH 
 from (SELECT XH,JZBH,COUNT(*) OVER(PARTITION BY GPWJM,GPMXMC,JM_YC) AS COU FROM WJMCZ1) as T
 WHERE COU>=2
union all
     --检测建模预测是否为空
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'JM_YC' as cname,'建模预测为空',JZBH from yssj where JM_YC IS NULL OR JM_YC=''
union all
   --检测光谱模型名称是否为空
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'光谱模型名称为空',JZBH from yssj where GPMXMC IS NULL OR GPMXMC=''
union all
 --检测光谱文件名是否为空
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where GPWJM IS NULL OR GPWJM=''
union all
--检测 光谱模型
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'无法找到该光谱模型',JZBH from yssj where GPMXMC NOT IN (SELECT GPMXMC FROM SZPX.T_DIM_SZPX_GPMXB WHERE ZYBJ=1) AND GPMXMC IS NOT NULL
union all
--检测光谱文件名是否存在
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gp
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB) and locate('/',gpwjm)=0
 union all
 select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gpdgjs
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB)
union all
--检测光谱文件名，光谱模型名称，建模或预测不允许重复
select 'T_SZPX_CP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH  from yssj_cf
where counts>=2
  )
  
  select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


  
--检测新增字段中的数字类型的字段

SET OUT_JG='动态检测新增字段-产品基础信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS','SYBH','XMID', 'XHTXBS', 'SYNF', 'SYYF',' SYRQ', 'SCCJ', 'CPPHID', 'CPYHID', 'CPGG',  'CPNF', 'CPJLID', 'HBYJ', 'HBCO', 'HBJY','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS','SYBH','XMID', 'XHTXBS', 'SYNF', 'SYYF',' SYRQ', 'SCCJ', 'CPPHID', 'CPYHID', 'CPGG',  'CPNF', 'CPJLID', 'HBYJ', 'HBCO', 'HBJY','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-检测指标表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','SYBH','JCLXID', 'GPID','GPWJM','GPJWJM', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','SYBH','JCLXID', 'GPID','GPWJM','GPJWJM', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-光谱信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','GPWJM', 'GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','GPWJM', 'GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-主成份表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC','GPWJM','GPJWJM','YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC','GPWJM','GPJWJM','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-产品评吸指标表(老版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','ZF','QXX','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_CP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','YPID','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','ZF','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','XMMC','YPBS','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZF','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_CP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','YPID','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZF','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-产品评吸指标表(新版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('YPBS', 'JZBH', 'JZXH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_CP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('YPBS', 'JZBH', 'JZXH', 'XMMC', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_CP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

ELSE
COMMIT;
END IF;

---------------------------------------------------------------------------掺配品---------------------------------------------------------------------------------------------
--检测项目名称
IF V_XMLX='MEDIUM' OR V_XMLX='medium' THEN 

DELETE FROM SESSION.JCXXB;
DELETE FROM SESSION.GPXXB;
DELETE FROM SESSION.ZDYSYBH;

--基础信息
INSERT INTO SESSION.JCXXB (SYBH,XMMC,XMID,XMLX)
SELECT DISTINCT SYBH,XMMC,XMID,XMLX FROM (
SELECT A.SYBH, B.XMMC, B.ID as XMID,B.XMLX
  from SZPX.T_SZPX_CPP_JCXXB AS A,
       SZPX.T_DIM_SZPX_XMB AS B
 WHERE A.XMID=B.ID);
COMMIT;

--自定样品编号

INSERT INTO SESSION.ZDYSYBH(ZDYSYBH,sybh)
SELECT A.ZDYSYBH,a.sybh
  from SZPX.T_SZPX_CPP_JCXXB_DL AS A
  where a.JZBH=V_JZBH
;
COMMIT;

--光谱信息
INSERT INTO SESSION.GPXXB (ID,SYBH,GPWJM)
select A.YPID,A.SYBH,B.GPWJM 
  from SZPX.T_SZPX_CPP_JCXXB AS A,
       SZPX.T_SZPX_CPP_GPXXB AS B
 WHERE A.SYBH=B.SYBH
   AND B.GPWJM IS NOT NULL
   AND B.GPWJM <>''
          UNION ALL
 select CAST (NULL AS INTEGER) AS ID,B.SYBH,B.ZDYGPWJM
  from SZPX.T_SZPX_CPP_GPXXB_DL AS B
 WHERE JZBH=V_JZBH
   and B.ZDYGPWJM IS NOT NULL
   AND B.ZDYGPWJM <>'';
COMMIT;

--------------------------------------------开始检测掺配品基础信息表-----------------------------------------------------
SET OUT_JG='开始检测掺配品基础信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     XMMC,
     SYBH,
     ZDYSYBH,
     SYNF,
     SYYF,
     SYRQ,
     LXMC,
     JZBH
  from SZPX.T_SZPX_CPP_JCXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_rq as
 (
  select xh,
       jzbh,
       synf,
       syyf,
       syrq
    from yssj
  where TRANSLATE(SYNF,'','0123456789') ='' And TRANSLATE(SYRQ,'','0123456789') =''  and TRANSLATE(SYYF,'','0123456789') =''
 ),yssj_sybhnull(jzbh,bj,nullc) as (
   select max(jzbh),'N',COUNT(*) nullc from yssj where sybh is not null
   union all
   select max(jzbh),'Y',count(*) nullc from yssj where sybh is null
 ),
 yssj_sybhnull2 as(select SYBH,xh,JZBH,(select nullc from yssj_sybhnull b where b.jzbh=a.jzbh and b.bj='N') as FKD,
                 (select nullc from yssj_sybhnull c where c.jzbh=a.jzbh and c.bj='Y') as KD
                  from yssj a
  ),  
yssj_zdysybh as (
    select   SYBH,xh,JZBH,zdysybh,count(zdysybh) over(partition by zdysybh) as cou from yssj     
  ),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
   --检测自定样品编号
 select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定样品编号重复',jzbh from yssj_zdysybh where cou>=2
union all 
--自定义样品编号与系统样品编号不能相同
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定义样品编号与系统样品编号相同',jzbh from yssj_zdysybh a
where exists(select 1 from session.jcxxb as b where a.zdysybh=b.sybh)
 union all 
 --检测掺配品基础信息表项目名称是否为空
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'XMMC' as cname,'项目名称为空',JZBH from yssj where XMMC IS NULL OR XMMC=''
union all
--检测样品编号在落地表存在(当样品编号不为空时)
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在落地表不存在',JZBH from yssj 
 where sybh not in (select sybh from SZPX.T_SZPX_CPP_JCXXB) and sybh is not null
 union all
 --检测试验年份
 select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYNF,'','0123456789') <>''
 union all
 --检测试验月份
 select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYYF,'','0123456789') <>''
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'试验年份为空',JZBH from yssj where (SYNF IS NULL OR SYNF='') AND SYYF IS NOT NULL
union all
--检测试验日期
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYRQ,'','0123456789') <>''
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'试验月份为空',JZBH from yssj where (SYYF IS NULL OR SYYF='') AND SYRQ IS NOT NULL
union all
--检测产品牌号信息
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'LXMC' as cname,'无法找到品牌',JZBH from yssj where LXMC NOT IN (SELECT LXMC FROM SZPX.T_DIM_SZPX_CPP_LXB WHERE ZYBJ=1) AND LXMC IS NOT NULL
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
WHERE  integer(SYYF)=2 and (mod(integer(synf),4)>0 or (mod(integer(synf),400)<>0 and mod(integer(synf),100)=0)) and integer(SYRQ) not between 1 and 28 
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF)=2 and mod(integer(synf),4)=0 and mod(integer(synf),100)<>0 and integer(SYRQ) not between 1 and 29
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(4),(6),(9),(11)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 30
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(1),(3),(5),(6),(8),(10),(12)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 31
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'实验月份无效',JZBH from yssj_rq
where integer(syyf) not between 1 and 12
union all
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'年份小于1980',JZBH
from yssj_rq where integer(synf)<1980
 union all 
 select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH  from yssj_sybhnull2 b
 where FKD>0 and kd>0 and sybh is null
  union all 
--检测样品编号在落地表重复
select 'T_SZPX_CPP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号重复',JZBH 
from 
(select xh,jzbh,count(*) over(partition by sybh) as counts from yssj where sybh is not null)
 where counts>=2
 )
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;

COMMIT;

--------------------------------------------开始检测评吸指标表(老版)-----------------------------------------------------
SET OUT_JG='开始检测评吸指标表(老版)';

--检测样品编号
insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     sybh,
     JZBH
  from SZPX.T_SZPX_CPP_PXZBB_LBDL
 WHERE JZBH=V_JZBH)
 ,
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
 --检测样品编号
select 'T_SZPX_CPP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh IS NULL OR sybh=''
union all
--检测样品编号是否存在
select 'T_SZPX_CPP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

--------------------------------------------开始检测评吸指标表(新版)-----------------------------------------------------
SET OUT_JG='开始检测评吸指标表(新版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     sybh,
     JZBH
  from SZPX.T_SZPX_CPP_PXZBB_XBDL
 WHERE JZBH=V_JZBH)
 ,yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测样品编号
select 'T_SZPX_CPP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh IS NULL OR sybh=''
union all
--检测样品编号
select 'T_SZPX_CPP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

--------------------------------------------开始检测掺配品检测指标表-----------------------------------------------------
SET OUT_JG='开始检测掺配品检测指标表';
insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT gpwjm,
       sybh,
       JCLXMC, 
     JCJXMC,
     JZXH as xh,
     JZBH 
  from SZPX.T_SZPX_CPP_JCZBB_DL 
 WHERE JZBH=V_JZBH)
 --检测光谱文件名
select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名无法找到',JZBH from yssj 
 where JCLXMC LIKE '%光谱%' and  gpwjm NOT IN (select gpwjm from SESSION.GPXXB)
 union all
--检测试验编号是否存在
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号无法找到',JZBH from yssj 
 where JCLXMC not LIKE '%光谱%' and sybh NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'')
 union all
 --检测光谱文件名是否为空
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where (GPWJM IS NULL OR GPWJM='') AND JCLXMC LIKE '%光谱%'
 union all
  --检测试验编号是否为空
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号为空',JZBH from yssj where (SYBH IS NULL OR SYBH='') AND JCLXMC NOT LIKE '%光谱%'
 union all
 --检测 检测类型
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'无法找到该检测类型',JZBH from yssj where JCLXMC NOT IN (SELECT JCLXMC FROM SZPX.T_DIM_SZPX_JCLXB WHERE ZYBJ=1) AND JCLXMC IS NOT NULL
 union all
select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'检测类型为空',JZBH from yssj where (jclxmc is null or jclxmc ='')
 union all
 --检测 检测机型
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测机型',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 /*AND JXLB=2*/) AND JCJXMC IS NOT NULL
 --检测检测仪器是否和检测类型关联
union all
select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'该检测仪器与该检测类型不对应',JZBH from yssj where JCJXMC 
NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A,SZPX.T_DIM_SZPX_JCLXB B WHERE A.JCLX=B.ID AND A.ZYBJ=1 
/*AND A.JXLB=2*/ AND B.ZYBJ=1 AND B.JCLXMC=YSSJ.JCLXMC) AND JCJXMC IS NOT NULL AND JCLXMC IS NOT NULL
 
 --光谱检测时 光谱文件名必填并且正确，SYBH可以不填，如果填写必须正确
--非光谱检测时 ＳＹＢＨ必填并且正确，ＧＰＷＪＭ可以不填，如果填必须是该SYBH下的GPWJM

 union all
 select 'T_SZPX_CPP_JCZBB_DL' AS XMMC,xh,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN 'GPWJM' ELSE 'SYBH' END as cname,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN '光谱文件名在该试验编号下无效' ELSE '试验编号与该光谱文件名无关联' END AS JCJG,
    JZBH 
 from yssj 
 where (SYBH IS NOT NULL OR SYBH <>'') AND (GPWJM IS NOT NULL OR GPWJM<>'') 
 and not exists (select 1 from SESSION.GPXXB as A where a.gpwjm=yssj.gpwjm and (a.sybh=yssj.sybh or exists(select 1 from session.zdysybh c where (c.zdysybh=yssj.sybh or yssj.sybh=c.sybh) and (a.sybh=c.sybh or a.sybh=c.zdysybh) and (zdysybh is not null or zdysybh<>''))))
 
 ;
COMMIT;

--------------------------------------------开始检测掺配品光谱信息表-----------------------------------------------------
SET OUT_JG='开始检测掺配品光谱信息表';


insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
       GPLXMC,
     JCJXMC,
     gpwjm,
     zdygpwjm,
     SFCZGPWJ,
     JZXH as xh,
     JZBH
  from SZPX.T_SZPX_CPP_GPXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as 
 (
  select xh,
       gpwjm,
       jzbh
    from yssj
    where gpwjm is not null or gpwjm <>''
 ),
 yssj_zdygpwjm as 
 (select xh,jzbh,zdygpwjm,count(zdygpwjm) over(partition by zdygpwjm) as cou from yssj),
yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
--自定义光谱文件名不能在同文档内不允许重复
select 'T_SZPX_YL_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'自定义光谱文件名重复',JZBH from  yssj_zdygpwjm where cou>=2
union all
 --检测光谱文件名
select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,'系统光谱文件名不存在',JZBH from yssj a where  not exists (select 1 from SZPX.T_SZPX_CPP_GPXXB b where a.gpwjm=b.gpwjm) and a.gpwjm is not null
union all
select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'系统光谱文件名和自定义光谱文件名不能同时为空',JZBH from yssj where gpwjm is null and zdygpwjm is  null 
union all
 --检测样品编号
select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh is null or SYBH=''
union all
select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在掺配品基础信息表不存在',JZBH from yssj 
 where sybh NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'') and sybh is not null
 union all
 --检测 光谱类型
 select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'GPLXMC' as cname,'无法找到该光谱类型',JZBH from yssj where GPLXMC NOT IN (SELECT GPLXMC FROM SZPX.T_DIM_SZPX_GPLXB WHERE ZYBJ=1) AND GPLXMC IS NOT NULL
 union all
 --检测 仪器名称
 select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测仪器',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 AND JXLB=2) AND JCJXMC IS NOT NULL
 union all
 --检测光谱文件名是否重复
 select 'T_SZPX_CPP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,GPWJM||' :光谱文件名重复',JZBH from yssj_gp
 where (GPWJM) IN (select GPWJM from yssj_gp group by gpwjm having count(*)>1)
)
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
 COMMIT;

--------------------------------------------开始检测掺配品主成份表-----------------------------------------------------
SET OUT_JG='开始检测掺配品主成份表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM, 
     GPMXMC,
     JM_YC,
     JZBH
  from SZPX.T_SZPX_CPP_ZCFB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as (
  select xh,
       gpwjm,
       jzbh
    from yssj
    where (GPWJM IS NOT NULL OR GPWJM<>'')
 ),
 yssj_gpdg as (
     select xh,
        gpwjm,
      jzbh
    from yssj_gp
    where locate('/',gpwjm)>0
 ),
 yssj_dgks(xh,gpwjm,jzbh) as (
 SELECT XH,
     GPWJM,
     JZBH
  from yssj_gpdg
  union all
  select xh,substr(gpwjm,locate('/',gpwjm)+1) as gpwjm,jzbh
  from yssj_dgks
  where locate('/',gpwjm)>0),
 yssj_dgjs (xh,gpwjm,jzbh) as
 (select xh,case when locate('/',gpwjm)>0
            then substr(gpwjm,1,locate('/',gpwjm)-1)
            else gpwjm
            end as gpwjm,jzbh
  from yssj_dgks
  where gpwjm<>''),
  yssj_gpwj(xh,gpwjm,jzbh) as
  (select xh,gpwjm,jzbh 
      from yssj_gp
      where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
      and locate('/',gpwjm)=0
      union all
     select xh,gpwjm,jzbh 
   from yssj_dgjs
     where GPWJM IN (Select GPWJM from SESSION.GPXXB)
   ),
   yssj_cf as (
      select XH,
                GPWJM, 
                GPMXMC,
                JM_YC,
                JZBH,
          count(*) over(partition by GPWJM,GPMXMC,JM_YC) as counts
          from yssj
   ),
   --------------------------------------------------------------------------
 WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC,a.jzxh as xh,a.jzbh
          from SZPX.T_SZPX_CPP_ZCFB_DL AS A
     where a.JZBH=V_JZBH
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,gpwjm,GPMXMC,JM_YC,xh,jzbh FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE('/', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 AS A WHERE LOCATE('/', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,  CASE WHEN LOCATE('/', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE('/', GPWJM) - 1) ELSE GPWJM END AS GPWJM,
     GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 WHERE GPWJM <> '' 
),
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh from (
     SELECT A.GPJWJM,
        a.GPMXMC,
        a.JM_YC,
        trim(char(REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, C.GPWJM||'/'))),'</A>',''),'<A>',''))) as GPWJM,
        a.xh,
        a.jzbh
       FROM WJMCZ_TMP2 AS A
    LEFT JOIN SZPX.T_SZPX_CPP_GPXXB_DL AS C
           on a.gpwjm=c.gpwjm
       or a.gpwjm=c.zdygpwjm
     WHERE C.GPWJM IS NOT NULL AND C.ZDYGPWJM IS NOT NULL
     group by a.gpjwjm,a.GPMXMC,a.JM_YC,a.xh,a.jzbh)
),
 ---------------------------------------------------------------------------
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名，光谱模型名称，建模或预测不允许重复
 SELECT 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH 
 from (SELECT XH,JZBH,COUNT(*) OVER(PARTITION BY GPWJM,GPMXMC,JM_YC) AS COU FROM WJMCZ1) as T
 WHERE COU>=2
 union all
 --检测建模预测是否为空
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'JM_YC' as cname,'建模预测为空',JZBH from yssj where JM_YC IS NULL OR JM_YC=''
union all
   --检测光谱模型名称是否为空
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'光谱模型名称为空',JZBH from yssj where GPMXMC IS NULL OR GPMXMC=''
union all
 --检测光谱文件名是否为空
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where GPWJM IS NULL OR GPWJM=''
union all
--检测 光谱模型
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'无法找到该光谱模型',JZBH from yssj where GPMXMC NOT IN (SELECT GPMXMC FROM SZPX.T_DIM_SZPX_GPMXB WHERE ZYBJ=1) AND GPMXMC IS NOT NULL
union all
--检测光谱文件名是否存在
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gp 
where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB) and locate('/',gpwjm)=0
union all
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_dgjs
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB)
union all
--检测光谱文件名，光谱模型名称，建模或预测不允许重复
select 'T_SZPX_CPP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH from yssj_cf
where counts>=2
        )
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;

COMMIT;

SET OUT_JG='动态检测新增字段-掺配品基础信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS', 'XMID', 'SYNF', 'SYYF', 'SYRQ', 'CPPLXID', 'CPPZLXMC', 'CPPZTMC', 'CPPBL', 'CPPGY', 'CPPGD', 'CPPCL', 'CPPCLGY', 'CPPCLCS','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS', 'XMID', 'SYNF', 'SYYF', 'SYRQ', 'CPPLXID', 'CPPZLXMC', 'CPPZTMC', 'CPPBL', 'CPPGY', 'CPPGD', 'CPPCL', 'CPPCLGY', 'CPPCLCS','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-掺配品检测指标表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'SYBH','YPID', 'JCLXID', 'GPID', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'SYBH','YPID', 'JCLXID', 'GPID', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-掺配品光谱信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'SYBH', 'GPWJM','GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'SYBH', 'GPWJM','GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-掺配品主成份表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC', 'YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC', 'YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-掺配品评吸指标表(老版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','SYBH','YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ','GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'ZF', 'XX',  'MX', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'NXX', 'ZJXX', 'PY', 'XF',  'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_CPP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ','GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX',  'MX', 'JGX', 'ZF', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'NXX', 'ZJXX', 'PY', 'XF',  'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,                 --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','SYBH','YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ','GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'ZF', 'XX',  'MX', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'NXX', 'ZJXX', 'PY', 'XF',  'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_CPP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ','GCX', 'QTX', 'ZTX', 'JTX', 'QX', 'XX',  'MX', 'ZF', 'JGX', 'GX', 'JX', 'HX', 'JXQ', 'QXX', 'NXX', 'ZJXX', 'PY', 'XF',  'CY', 'ND', 'JT', 'XQZ', 'XQL', 'TFX', 'QZQ', 'SQQ', 'KJQ', 'MZQ', 'TXQ', 'SZQ', 'HFQ', 'YCQ', 'JSQ', 'QT', 'XNRHCD', 'YRG', 'CJX', 'GZX', 'YW', 'FGTZ', 'PZTZ','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-掺配品评吸指标表(新版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('SYBH', 'JZBH', 'JZXH', 'XMMC', 'ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_CPP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('SYBH', 'JZBH', 'JZXH', 'XMMC', 'ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_CPP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_CPP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

ELSE
COMMIT;
END IF;

------------------------------------------------------------产品质量------------------------------------------------------------------------------------
--检测样品信息
IF V_XMLX='productquality' THEN 

DELETE FROM SESSION.JCXXB;
DELETE FROM SESSION.GPXXB;
DELETE FROM SESSION.ZDYSYBH;
--基础信息
INSERT INTO SESSION.JCXXB (SYBH,XMMC,XMID,XMLX)
SELECT YPBH, CAST(NULL AS VARCHAR(255)) XMMC, CAST(NULL AS INTEGER) XMID,CAST(NULL AS VARCHAR(200)) XMLX
  FROM SYPT.T_SYPT_CPZL_YPXXB ;
COMMIT;

--自定样品编号

INSERT INTO SESSION.ZDYSYBH(ZDYSYBH,sybh)
SELECT A.ZDYYPBH,A.YPBH
  from SYPT.T_SYPT_CPZL_YPXXB_DL AS A
  where A.JZBH=V_JZBH;
COMMIT;

--光谱信息
INSERT INTO SESSION.GPXXB (ID,SYBH,GPWJM)
select A.YPID,A.YPBH,B.GPMC
  from SYPT.T_SYPT_CPZL_YPXXB AS A,
       SYPT.T_SYPT_CPZL_GPB AS B
 WHERE A.YPBH=B.YPBH
   AND B.GPMC IS NOT NULL
   AND B.GPMC <>''
 UNION ALL
 SELECT CAST (NULL AS INTEGER) AS ID,B.YPBH,B.ZDYGPMC
  FROM SYPT.T_SYPT_CPZL_GPB_DL AS B
 WHERE B.JZBH=V_JZBH
   AND B.ZDYGPMC IS NOT NULL
   AND B.ZDYGPMC <>'';
COMMIT;
--------------------------------------------------开始校验样品信息------------------------------------------
SET OUT_JG='开始校验样品信息';
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ(JZBH,XH,YPBH, ZDYYPBH, PPMC,CPGGMC, YZGGMC,CJMC,ZSPHMC,JJPC,ZSPC,HBCO,HBJY,HBYJ,SCSJ,BZ,CP27WM,COU,COUY)
          AS (SELECT JZBH, 
                     JZXH AS XH, 
           YPBH, 
           ZDYYPBH, 
           PPMC, 
           CPGGMC, 
           YZGGMC, 
           CJMC, 
                     ZSPHMC, 
           JJPC, 
           ZSPC, 
           HBCO, 
           HBJY, 
           HBYJ, 
           SCSJ, 
           BZ, 
           CP27WM,
           COUNT(ZDYYPBH) OVER(PARTITION BY ZDYYPBH) COU,
           COUNT(YPBH) OVER(PARTITION BY YPBH) COUY
        FROM SYPT.T_SYPT_CPZL_YPXXB_DL
        WHERE JZBH=V_JZBH),
    YSSJ_PP AS (
    SELECT A.JZBH,A.XH,A.YPBH, A.ZDYYPBH, A.PPMC,A.CPGGMC, A.YZGGMC,A.CJMC,A.ZSPHMC,A.JJPC,A.ZSPC,B.ID AS PPID 
    FROM YSSJ A,SZPX.T_DIM_SZPX_CP_PPB B
    WHERE A.PPMC=B.PPMC AND B.ZYBJ=1 AND CURRENT DATE BETWEEN B.KSRQ AND B.JSRQ
    ),    
  YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (
  --检测自定样品编号本文档不允许重复
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'ZDYYPBH' AS CNAME,'自定样品编号重复',JZBH FROM YSSJ WHERE COU>=2
  UNION ALL
  --自定义样品编号与系统样品编号不能相同
    SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'ZDYYPBH' AS CNAME,'自定义样品编号与系统样品编号相同',JZBH FROM YSSJ A
    WHERE EXISTS(SELECT 1 FROM SESSION.JCXXB AS B WHERE A.ZDYYPBH=B.SYBH)
  UNION ALL
  --检测样品编号是否在落地表存在
    SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号在落地表不存在',JZBH FROM YSSJ 
    WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH IS NOT NULL
  UNION ALL 
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号在本文档重复',JZBH FROM YSSJ 
  WHERE COUY>=2 
  UNION ALL 
  --检测盒标焦油
    SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'HBJY' AS CNAME,'盒标焦油值输入错误'  ,JZBH FROM YSSJ WHERE TRANSLATE(HBJY,'','0123456789-.')<>''
    UNION ALL
    --检测盒标CO
    SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'HBCO' AS CNAME,'盒标CO值输入错误'  ,JZBH FROM YSSJ WHERE TRANSLATE(HBCO,'','0123456789-.')<>''
    UNION ALL
    --检测盒标烟碱
    SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'HBYJ' AS CNAME,'盒标烟碱值输入错误'  ,JZBH FROM YSSJ WHERE TRANSLATE(HBYJ,'','0123456789-.')<>''
  UNION ALL
  --检查品牌是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'PPMC' AS CNAME,'品牌名称无效',JZBH FROM YSSJ 
  WHERE PPMC NOT IN (SELECT A.PPMC FROM SZPX.T_DIM_SZPX_CP_PPB A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ ) AND PPMC IS NOT NULL
  UNION ALL
  --检查产品规格是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'CPGGMC' AS CNAME,'产品规格名称无效',JZBH FROM YSSJ 
  WHERE CPGGMC NOT IN (SELECT A.CPGGMC FROM SYPT.T_DIM_SYPT_CPZL_CPGG A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ ) AND CPGGMC IS NOT NULL
  UNION ALL
  --检查烟支规格是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'YZGGMC' AS CNAME,'烟支规格名称无效',JZBH FROM YSSJ 
  WHERE YZGGMC NOT IN (SELECT A.YZGGMC FROM SYPT.T_DIM_SYPT_CPZL_YZGG A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ ) AND YZGGMC IS NOT NULL
  /*UNION ALL
  --检查烟支规格与品牌关联是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'CPGGMC' AS CNAME,'烟支规格与该品牌无关联',JZBH FROM YSSJ_PP 
  WHERE  PPID NOT IN (SELECT A.PPID FROM SYPT.T_DIM_SYPT_CPZL_YZGG A WHERE A.YZGGMC=YSSJ_PP.YZGGMC AND A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ) 
  AND YZGGMC IS NOT NULL */
  UNION ALL
  --检查生产厂家是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'CJMC' AS CNAME,'生产厂家名称无效',JZBH FROM YSSJ 
  WHERE CJMC NOT IN (SELECT A.CJMC FROM SYPT.T_DIM_SYPT_CPZL_SCCJ A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ ) AND CJMC IS NOT NULL
  UNION ALL
  --检查制丝牌号是否有效
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'ZSPHMC' AS CNAME,'制丝牌号名称无效',JZBH FROM YSSJ 
  WHERE ZSPHMC NOT IN (SELECT A.ZSPHMC FROM SYPT.T_DIM_SYPT_CPZL_ZSPH A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ ) AND ZSPHMC IS NOT NULL
  UNION ALL
  --产品规格是否与该品牌关联
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'CPGGMC' AS CNAME,'产品规格名称与该品牌无关联',JZBH FROM YSSJ_PP 
  WHERE  PPID NOT IN (SELECT A.PPID FROM SYPT.T_DIM_SYPT_CPZL_CPGG A WHERE A.CPGGMC=YSSJ_PP.CPGGMC AND A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)
  AND  CPGGMC IS NOT NULL
  UNION ALL
  --制丝牌号是否与该品牌关联
  SELECT 'T_SYPT_CPZL_YPXXB_DL' AS XMMC,XH,'ZSPHMC' AS CNAME,'制丝牌号名称与该品牌无关联',JZBH FROM YSSJ_PP 
  WHERE  PPID NOT IN (SELECT A.PPID FROM SYPT.T_DIM_SYPT_CPZL_ZSPH A WHERE A.ZSPHMC=YSSJ_PP.ZSPHMC AND A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)
  AND  ZSPHMC IS NOT NULL
  )
  select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;  

--------------------------------------------------开始校验感官信息------------------------------------------
SET OUT_JG='开始校验感官信息';
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ AS (
     SELECT JZXH XH, JZBH, YPBH,JCBMMC
   FROM SYPT.T_SYPT_CPZL_GGB_DL
   WHERE JZBH=V_JZBH
),
YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (

    -- 校验样品编号是否有效
  SELECT 'T_SYPT_CPZL_GGB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号无效',JZBH FROM YSSJ
  WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH NOT IN (SELECT ZDYSYBH FROM SESSION.ZDYSYBH WHERE ZDYSYBH <>'')
  UNION ALL
  -- 校验检测部门是否有效
    SELECT 'T_SYPT_CPZL_GGB_DL' AS XMMC,XH,'JCBMMC' AS CNAME,'检测部门无效',JZBH FROM YSSJ
    WHERE JCBMMC NOT IN (SELECT A.JCBMMC FROM SYPT.T_DIM_SYPT_CPZL_JCBM A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)  
    AND  JCBMMC IS NOT NULL
)
SELECT TNAME,RNUM,CNAME,JCJG,JZBH FROM YSSJ_JG;
COMMIT;

--------------------------------------------------开始校验烟气信息------------------------------------------
SET OUT_JG='开始校验烟气信息';
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ AS (
     SELECT JZXH XH, JZBH, YPBH,JCBMMC,JCJXMC
   FROM SYPT.T_SYPT_CPZL_YQB_DL
   WHERE JZBH=V_JZBH
),
YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (

    -- 校验样品编号是否有效
  SELECT 'T_SYPT_CPZL_YQB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号无效',JZBH FROM YSSJ
  WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH NOT IN (SELECT ZDYSYBH FROM SESSION.ZDYSYBH WHERE ZDYSYBH <>'')
  UNION ALL
  -- 校验检测部门是否有效
    SELECT 'T_SYPT_CPZL_YQB_DL' AS XMMC,XH,'JCBMMC' AS CNAME,'检测部门无效',JZBH FROM YSSJ
    WHERE JCBMMC NOT IN (SELECT A.JCBMMC FROM SYPT.T_DIM_SYPT_CPZL_JCBM A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)   
    AND  JCBMMC IS NOT NULL  
  UNION ALL
  -- 校验检测设备是否有效
    SELECT 'T_SYPT_CPZL_YQB_DL' AS XMMC,XH,'JCJXMC' AS CNAME,'吸烟机无效',JZBH FROM YSSJ
    WHERE JCJXMC NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A WHERE A.ZYBJ=1 AND JXLB=1)  
    AND  JCJXMC IS NOT NULL  
)
SELECT TNAME,RNUM,CNAME,JCJG,JZBH FROM YSSJ_JG;
COMMIT;

--------------------------------------------------开始校验化学信息------------------------------------------
SET OUT_JG='开始校验化学信息';
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ AS (
     SELECT JZXH XH, JZBH, YPBH,JCBMMC, JCJXMC
   FROM SYPT.T_SYPT_CPZL_HXB_DL
   WHERE JZBH=V_JZBH
),
YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (

    -- 校验样品编号是否有效
  SELECT 'T_SYPT_CPZL_HXB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号无效',JZBH FROM YSSJ
  WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH NOT IN (SELECT ZDYSYBH FROM SESSION.ZDYSYBH WHERE ZDYSYBH <>'')
  UNION ALL
  -- 校验检测部门是否有效
    SELECT 'T_SYPT_CPZL_HXB_DL' AS XMMC,XH,'JCBMMC' AS CNAME,'检测部门无效',JZBH FROM YSSJ
    WHERE JCBMMC NOT IN (SELECT A.JCBMMC FROM SYPT.T_DIM_SYPT_CPZL_JCBM A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ) 
  AND  JCBMMC IS NOT NULL
    UNION ALL
  -- 校验检测设备是否有效
    SELECT 'T_SYPT_CPZL_HXB_DL' AS XMMC,XH,'JCJXMC' AS CNAME,'检测设备无效',JZBH FROM YSSJ
    WHERE JCJXMC NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A WHERE A.ZYBJ=1 AND JXLB=2)  
    AND  JCJXMC IS NOT NULL  
)
SELECT TNAME,RNUM,CNAME,JCJG,JZBH FROM YSSJ_JG;
COMMIT;

--------------------------------------------------开始校验物理信息------------------------------------------
SET OUT_JG='开始校验物理信息';
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ AS (
     SELECT JZXH XH, JZBH, YPBH,JCBMMC
   FROM SYPT.T_SYPT_CPZL_WLB_DL
   WHERE JZBH=V_JZBH
),
YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (

    -- 校验样品编号是否有效
  SELECT 'T_SYPT_CPZL_WLB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号无效',JZBH FROM YSSJ
  WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH NOT IN (SELECT ZDYSYBH FROM SESSION.ZDYSYBH WHERE ZDYSYBH <>'')
  UNION ALL
  -- 校验检测部门是否有效
    SELECT 'T_SYPT_CPZL_WLB_DL' AS XMMC,XH,'JCBMMC' AS CNAME,'检测部门无效',JZBH FROM YSSJ
    WHERE JCBMMC NOT IN (SELECT A.JCBMMC FROM SYPT.T_DIM_SYPT_CPZL_JCBM A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)  
  AND  JCBMMC IS NOT NULL
)
SELECT TNAME,RNUM,CNAME,JCJG,JZBH FROM YSSJ_JG;
COMMIT;

--------------------------------------------------开始校验光谱信息------------------------------------------
INSERT INTO SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
WITH YSSJ(XH, JZBH, YPBH, GPMC, ZDYGPMC, GPWJ, GPLXMC, JCJXMC, JCBMMC, COUG,COUZ) AS (
      SELECT JZXH XH, JZBH, YPBH, GPMC, ZDYGPMC, GPWJ, GPLXMC, JCJXMC
            , JCBMMC,COUNT(GPMC) OVER(PARTITION BY GPMC) COUG,
      COUNT(ZDYGPMC) OVER(PARTITION BY ZDYGPMC) COUZ
    FROM SYPT.T_SYPT_CPZL_GPB_DL
    WHERE JZBH=V_JZBH
),
YSSJ_JG(TNAME,RNUM,CNAME,JCJG,JZBH) AS (
    -- 校验样品编号是否有效
  SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'YPBH' AS CNAME,'样品编号无效',JZBH FROM YSSJ
  WHERE YPBH NOT IN (SELECT SYBH FROM SESSION.JCXXB) AND YPBH NOT IN (SELECT ZDYSYBH FROM SESSION.ZDYSYBH WHERE ZDYSYBH <>'') AND YPBH IS NOT NULL
  UNION ALL
  --校验自定义光谱名称是否重复
  SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'ZDYGPMC' AS CNAME,'自定义光谱名称重复',JZBH FROM YSSJ
  WHERE COUZ>=2
  UNION ALL
  --校验光谱名称是否重复
  SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'GPMC' AS CNAME,'光谱名称重复',JZBH FROM YSSJ
  WHERE COUG>=2
  UNION ALL
  --校验光谱名称是否有效
  SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'GPMC' AS CNAME,'光谱名称无效',JZBH FROM YSSJ
    WHERE  NOT EXISTS (SELECT 1 FROM SYPT.T_SYPT_CPZL_GPB A WHERE A.GPMC=YSSJ.GPMC ) AND GPMC IS NOT NULL
  UNION ALL
  -- 校验检测部门是否有效
    SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'JCBMMC' AS CNAME,'检测部门无效',JZBH FROM YSSJ
    WHERE JCBMMC NOT IN (SELECT A.JCBMMC FROM SYPT.T_DIM_SYPT_CPZL_JCBM A WHERE A.ZYBJ=1 AND CURRENT DATE BETWEEN A.KSRQ AND A.JSRQ)
  AND JCBMMC IS NOT NULL
  UNION ALL
  -- 校验检测仪器型号是否有效
    SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'JCJXMC' AS CNAME,'检测仪器型号无效',JZBH FROM YSSJ
    WHERE JCJXMC NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A WHERE A.ZYBJ=1 AND JXLB=2)
  AND JCJXMC IS NOT NULL
    UNION ALL  
  --检测 光谱类型
    SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'GPLXMC' AS CNAME,'无法找到该光谱类型',JZBH FROM YSSJ 
  WHERE GPLXMC NOT IN (SELECT GPLXMC FROM SZPX.T_DIM_SZPX_GPLXB WHERE ZYBJ=1) AND GPLXMC IS NOT NULL
  UNION ALL
  SELECT 'T_SYPT_CPZL_GPB_DL' AS XMMC,XH,'ZDYGPMC' AS CNAME,'系统光谱名和自定义光谱名不能同时为空',JZBH FROM YSSJ 
  WHERE GPMC IS NULL AND ZDYGPMC IS  NULL 
)
SELECT TNAME,RNUM,CNAME,JCJG,JZBH FROM YSSJ_JG;
COMMIT;

SET OUT_JG='动态检测新增字段-样品信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_YPXXB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ('YPID','YPBH','ZDYYPBH','PPMC','CPGGMC','YZGGMC','CJMC','ZSPHMC','JJPC',
     'ZSPC','HBCO','HBJY','HBYJ','SCSJ','BZ','CP27WM','JZBH','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_YPXXB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID','YPBH','ZDYYPBH','PPMC','CPGGMC','YZGGMC','CJMC','ZSPHMC','JJPC',
     'ZSPC','HBCO','HBJY','HBYJ','SCSJ','BZ','CP27WM','JZBH','CJR','CJSJ','ZHGXR','ZHGXSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-感官表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_GGB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','JCBM','PXSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_GGB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','JCBM','PXSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-烟气表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_YQB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','XYJX','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_YQB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','XYJX','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'))
     SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-化学表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_HXB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','JCBM','JCSB','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_HXB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','JCBM','JCSB','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'))
     SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-物理表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_WLB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ( 'ID','JZBH','YPBH','YPID','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_WLB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ( 'ID','JZBH','YPBH','YPID','JCBM','JCSJ','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'))
     SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-光谱表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_GPB' 
     AND TBCREATOR='SYPT'
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','GPMC','ZDYGPMC','GPWJ','GPLX','JCYQXH','JCBM','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ(XH, TBNAME_CT, TBNAME_T, COLNAME) AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
    from sysibm.syscolumns 
   where TBNAME='T_SYPT_CPZL_GPB' 
     AND TBCREATOR='SYPT'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','JZBH','YPBH','YPID','GPMC','ZDYGPMC','GPWJ','GPLX','JCYQXH','JCBM','BZ','CJR','CJSJ','ZHGXR','ZHGXSJ'))
     SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;





ELSE 
COMMIT;
END IF;


--------------------------------------------------中间品-----------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--检测中间品
IF V_XMLX='middle' OR V_XMLX='MIDDLE' THEN 

DELETE FROM SESSION.JCXXB;
DELETE FROM SESSION.GPXXB;
DELETE FROM SESSION.ZDYSYBH;
--基础信息
INSERT INTO SESSION.JCXXB (SYBH,XMMC,XMID,XMLX)
SELECT DISTINCT SYBH,XMMC,XMID,XMLX FROM (
SELECT A.SYBH, B.XMMC, B.ID as XMID,B.XMLX
  from SZPX.T_SZPX_ZJP_JCXXB AS A,
       SZPX.T_DIM_SZPX_XMB AS B
 WHERE A.XMID=B.ID);
COMMIT;

--自定样品编号

INSERT INTO SESSION.ZDYSYBH(ZDYSYBH,sybh)
SELECT A.ZDYSYBH,a.sybh
  from SZPX.T_SZPX_ZJP_JCXXB_DL AS A
  where a.JZBH=V_JZBH
;
COMMIT;

--光谱信息
INSERT INTO SESSION.GPXXB (ID,SYBH,GPWJM)
select A.YPID,A.SYBH,B.GPWJM
  from SZPX.T_SZPX_ZJP_JCXXB AS A,
       SZPX.T_SZPX_ZJP_GPXXB AS B
 WHERE A.SYBH=B.SYBH
   AND B.GPWJM IS NOT NULL
   AND B.GPWJM <>''
       UNION ALL
 select CAST (NULL AS INTEGER) AS ID,B.SYBH,B.ZDYGPWJM
  from SZPX.T_SZPX_ZJP_GPXXB_DL AS B
 WHERE JZBH=V_JZBH
   and B.ZDYGPWJM IS NOT NULL
   AND B.ZDYGPWJM <>'';
COMMIT;

--------------------------------------开始检测中间品基础信息表---------------------------------------------------------------
SET OUT_JG='开始检测中间品基础信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj(XH,
     XMMC,
     SYBH,
     ZDYSYBH,
     SYNF,
     SYYF,
     SYRQ,
     PPMC,
     SCCJ,
     YHMC,
     JZBH,
     LXMC) as (
SELECT JZXH AS XH,
     XMMC,
     SYBH,
     ZDYSYBH,
     SYNF,
     SYYF,
     SYRQ,
     PPMC,
     SCCJ,
     YHMC,
     JZBH,
     LXMC
  from SZPX.T_SZPX_ZJP_JCXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_rq as (
SELECT  XH,
     SYNF,
     XMMC,
       SYYF,
     SYRQ,
     JZBH
  from yssj
 WHERE  TRANSLATE(SYNF,'','0123456789') ='' And TRANSLATE(SYRQ,'','0123456789') =''  and TRANSLATE(SYYF,'','0123456789') ='')
 ,
 yssj_sybhnull(jzbh,bj,nullc) as (
   select max(jzbh),'N',COUNT(*) nullc from yssj where sybh is not null
   union all
   select max(jzbh),'Y',count(*) nullc from yssj where sybh is null
 ),
 yssj_sybhnull2 as(select SYBH,xh,JZBH,(select nullc from yssj_sybhnull b where b.jzbh=a.jzbh and b.bj='N') as FKD,
                 (select nullc from yssj_sybhnull c where c.jzbh=a.jzbh and c.bj='Y') as KD
                  from yssj a
  ),
  yssj_zdysybh as (
    select   SYBH,xh,JZBH,zdysybh,count(zdysybh) over(partition by zdysybh) as cou from yssj     
  ),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as (

  --检测自定样品编号
 select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定样品编号重复',jzbh from yssj_zdysybh where cou>=2
union all 
--自定义样品编号与系统样品编号不能相同
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'ZDYSYBH' as cname,'自定义样品编号与系统样品编号相同',jzbh from yssj_zdysybh a
where exists(select 1 from session.jcxxb as b where a.zdysybh=b.sybh)
union all 
--检测类型名称是否有效
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'LXMC' as cname,'中间品类型名称无效',jzbh from yssj a
where A.LXMC NOT IN (SELECT B.LXMC FROM SZPX.T_SZPX_DIM_ZJP_LXB B WHERE B.ZYBJ=1 AND CURRENT DATE BETWEEN B.KSRQ AND B.JSRQ)
 union all 
 --检测中间品基础信息表项目名称是否为空
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'XMMC' as cname,'项目名称为空',JZBH from yssj where XMMC IS NULL OR XMMC=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在落地表不存在',JZBH from yssj 
 where sybh not in (select sybh from SZPX.T_SZPX_ZJP_JCXXB) and sybh is not null
union all
--检测试验年份
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYNF,'','0123456789') <>''
union all
--检测试验月份
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYYF,'','0123456789') <>''
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'试验年份为空',JZBH from yssj where (SYNF IS NULL OR SYNF='') AND SYYF IS NOT NULL
union all
--检测试验日期
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'包含无效字符',JZBH from yssj where TRANSLATE(SYRQ,'','0123456789') <>''
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'试验月份为空',JZBH from yssj where ((SYYF IS NULL OR SYYF='') AND SYRQ IS NOT NULL) AND SYRQ IS NOT NULL
union all
--检测中间品牌号信息
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'PPMC' as cname,'无法找到品牌',JZBH from yssj where PPMC NOT IN (SELECT PPMC FROM SZPX.T_DIM_SZPX_CP_PPB WHERE JSRQ >= CURRENT DATE AND ZYBJ=1) AND PPMC IS NOT NULL
union all
--检测中间品烟号信息
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'YHMC' as cname,'无法找到烟号名称',JZBH from yssj where YHMC NOT IN (SELECT YHMC FROM SZPX.T_DIM_SZPX_CP_YHB WHERE ZYBJ=1) AND YHMC IS NOT NULL
--检测试验日期是否有效
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
WHERE  integer(SYYF)=2 and (mod(integer(synf),4)>0 or (mod(integer(synf),400)<>0 and mod(integer(synf),100)=0)) and integer(SYRQ) not between 1 and 28 
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF)=2 and mod(integer(synf),4)=0 and mod(integer(synf),100)<>0 and integer(SYRQ) not between 1 and 29
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(4),(6),(9),(11)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 30
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYRQ' as cname,'日期在该年的月份内无效',JZBH from yssj_rq
where integer(SYYF) IN (SELECT SYYF FROM (VALUES(1),(3),(5),(6),(8),(10),(12)) AS SYYFT(SYYF)) AND integer(SYRQ) not between 1 and 31
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYYF' as cname,'实验月份无效',JZBH from yssj_rq
where integer(syyf) not between 1 and 12
union all
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYNF' as cname,'年份小于1980',JZBH
from yssj_rq where integer(synf)<1980
union all 
 select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH  from yssj_sybhnull2 b
 where FKD>0 and kd>0 and sybh is null
 union all 
--检测样品编号在落地表重复
select 'T_SZPX_ZJP_JCXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号重复',JZBH 
from 
(select xh,jzbh,count(*) over(partition by sybh) as counts from yssj where sybh is not null)
 where counts>=2
 )

select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


--------------------------------------开始检测评吸指标表(老版)---------------------------------------------------------------
SET OUT_JG='开始检测评吸指标表(老版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     SYBH,
     JZBH
  from SZPX.T_SZPX_ZJP_PXZBB_LBDL
 WHERE JZBH=V_JZBH),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测样品编号
select 'T_SZPX_ZJP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where SYBH IS NULL OR SYBH=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_ZJP_PXZBB_LBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 
 select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;
--------------------------------------开始检测评吸指标表(新版)--------------------------------------------------------------
SET OUT_JG='开始检测评吸指标表(新版)';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     SYBH,
     JZBH
  from SZPX.T_SZPX_ZJP_PXZBB_XBDL
 WHERE JZBH=V_JZBH),
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as (
 --检测样品编号
select 'T_SZPX_ZJP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where SYBH IS NULL OR SYBH=''
union all
--检测样品编号是否在落地表存在
select 'T_SZPX_ZJP_PXZBB_XBDL' AS XMMC,xh,'SYBH' as cname,'样品编号不存在',JZBH from yssj 
 where (SYBH) NOT IN (select SYBH from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>''))
 
 select TNAME,RNUM,CNAME,JCJG,JZBH from  yssj_jg;
COMMIT;

--------------------------------------开始检测中间品检测指标表-------------------------------------------------------------
SET OUT_JG='开始检测中间品检测指标表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM, 
     SYBH,
     JCLXMC,
     JCJXMC,
     JZBH
  from SZPX.T_SZPX_ZJP_JCZBB_DL
 WHERE JZBH=V_JZBH),
 yssj_jg(TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名是否为空
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where JCLXMC LIKE '%光谱%' and (GPWJM IS NULL OR GPWJM='')
 --检测试验编号是否为空
union all
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号为空',JZBH from yssj where JCLXMC not LIKE '%光谱%' and (sybh IS NULL OR sybh='')
union all
--检测光谱文件名是否存在
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj 
 where JCLXMC LIKE '%光谱%' and (GPWJM) NOT IN (select GPWJM from SESSION.GPXXB)
 union all
--检测试验编号是否存在
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'SYBH' as cname,'试验编号不存在',JZBH from yssj 
 where JCLXMC not LIKE '%光谱%' and (sybh) NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'')
union all
--检测 检测类型
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'无法找到该检测类型',JZBH from yssj where JCLXMC NOT IN (SELECT JCLXMC FROM SZPX.T_DIM_SZPX_JCLXB WHERE ZYBJ=1) AND JCLXMC IS NOT NULL
union all
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'JCLXMC' as cname,'检测类型为空',JZBH from yssj where (jclxmc is null or jclxmc ='')
union all
--检测 检测机型
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测机型',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 /*AND JXLB=2*/) AND JCJXMC IS NOT NULL
 --检测检测仪器是否和检测类型关联
union all
select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,'JCJXMC' as cname,'该检测仪器与该检测类型不对应',JZBH from yssj where JCJXMC 
NOT IN (SELECT A.JCJXMC FROM SZPX.T_DIM_SZPX_JCJX A,SZPX.T_DIM_SZPX_JCLXB B WHERE A.JCLX=B.ID AND A.ZYBJ=1 
/*AND A.JXLB=2*/ AND B.ZYBJ=1 AND B.JCLXMC=YSSJ.JCLXMC) AND JCJXMC IS NOT NULL AND JCLXMC IS NOT NULL
 


--光谱检测时 光谱文件名必填并且正确，SYBH可以不填，如果填写必须正确
--非光谱检测时 ＳＹＢＨ必填并且正确，ＧＰＷＪＭ可以不填，如果填必须是该SYBH下的GPWJM

 union all
 select 'T_SZPX_ZJP_JCZBB_DL' AS XMMC,xh,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN 'GPWJM' ELSE 'SYBH' END as cname,
        CASE WHEN JCLXMC not LIKE '%光谱%' THEN '光谱文件名在该试验编号下无效' ELSE '试验编号与该光谱文件名无关联' END AS JCJG,
    JZBH 
 from yssj 
 where (SYBH IS NOT NULL OR SYBH <>'') AND (GPWJM IS NOT NULL OR GPWJM<>'') 
 and not exists (select 1 from SESSION.GPXXB as A where a.gpwjm=yssj.gpwjm and (a.sybh=yssj.sybh or exists(select 1 from session.zdysybh c where (c.zdysybh=yssj.sybh or yssj.sybh=c.sybh) and (a.sybh=c.sybh or a.sybh=c.zdysybh) and (zdysybh is not null or zdysybh<>''))))


)
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;

--------------------------------------开始检测中间品光谱信息表----------------------------------------------------------
SET OUT_JG='开始检测中间品光谱信息表';

insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT SYBH,
     GPWJM,
     ZDYGPWJM,
       JCJXMC,
     GPLXMC,
     JZXH as xh,
     SFCZGPWJ,
     JZBH
  from SZPX.T_SZPX_ZJP_GPXXB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as
 (
  SELECT XH,
         GPWJM,
         JZBH
  from yssj
 WHERE gpwjm is not null or gpwjm <>''
 ),
 yssj_zdygpwjm as 
 (select xh,jzbh,zdygpwjm,count(zdygpwjm) over(partition by zdygpwjm) as cou from yssj),
yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
--自定义光谱文件名不能在同文档内不允许重复
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'自定义光谱文件名重复',JZBH from  yssj_zdygpwjm where cou>=2
union all
 --检测光谱文件名
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,'系统光谱文件名不存在',JZBH from yssj a where  not exists (select 1 from SZPX.T_SZPX_ZJP_GPXXB b where a.gpwjm=b.gpwjm) and a.gpwjm is not null
union all
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'ZDYGPWJM' as cname,'系统光谱文件名和自定义光谱文件名不能同时为空',JZBH from yssj where gpwjm is null and zdygpwjm is  null 
union all
 --检测样品编号
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号为空',JZBH from yssj where sybh is null or SYBH=''
union all
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'SYBH' as cname,'样品编号在中间品基础信息表不存在',JZBH from yssj 
 where sybh NOT IN (select sybh from SESSION.JCXXB) and (SYBH) NOT IN (select ZDYSYBH from SESSION.ZDYSYBH where zdysybh is not null or zdysybh<>'') and sybh is not null
union all
--检测 仪器名称
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'JCJXMC' as cname,'无法找到该检测仪器',JZBH from yssj where JCJXMC NOT IN (SELECT JCJXMC FROM SZPX.T_DIM_SZPX_JCJX WHERE ZYBJ=1 AND JXLB=2) AND JCJXMC IS NOT NULL
union all
--检测 光谱类型
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'GPLXMC' as cname,'无法找到该光谱类型',JZBH from yssj where GPLXMC NOT IN (SELECT GPLXMC FROM SZPX.T_DIM_SZPX_GPLXB WHERE ZYBJ=1) AND GPLXMC IS NOT NULL
UNION ALL
--检测光谱文件名是否重复
select 'T_SZPX_ZJP_GPXXB_DL' AS XMMC,xh,'GPWJM' as cname,GPWJM||' :光谱文件名重复',JZBH from yssj_gp 
 where (GPWJM) IN (select GPWJM from yssj_gp group by gpwjm having count(*)>1)
)
select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;

COMMIT;

--------------------------------------开始检测中间品主成份表--------------------------------------------------------
SET OUT_JG='开始检测中间品主成份表';


insert into SESSION.JGB (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
SELECT JZXH AS XH,
     GPWJM,
       GPMXMC,
       JM_YC,     
     JZBH
  from SZPX.T_SZPX_ZJP_ZCFB_DL
 WHERE JZBH=V_JZBH),
 yssj_gp as
 (
  select xh,
       gpwjm,
       jzbh
    from yssj
    where gpwjm is not null or gpwjm<>''
 ),
 yssj_gpdg as
 (
  select xh,
       gpwjm,
       jzbh
     from yssj_gp
     where locate('/',gpwjm)>0 
 ),
 yssj_gpdgks(xh,gpwjm,jzbh) as (
 SELECT  XH,
     GPWJM,
     JZBH
  from yssj_gpdg
  union all
  select xh,substr(gpwjm,locate('/',gpwjm)+1) as gpwjm,jzbh
  from yssj_gpdgks
  where locate('/',gpwjm)>0),
 yssj_gpdgjs (xh,gpwjm,jzbh) as
 (select xh,case when locate('/',gpwjm)>0
            then substr(gpwjm,1,locate('/',gpwjm)-1)
            else gpwjm
            end as gpwjm,jzbh
  from yssj_gpdgks
  where gpwjm<>''),
  yssj_gpwj (xh,gpwjm,jzbh) as
  (
    select xh,gpwjm,jzbh from yssj_gp
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
    and locate('/',gpwjm)=0
  union all
  select xh,gpwjm,jzbh from yssj_gpdgjs
  where GPWJM  IN (Select GPWJM from SESSION.GPXXB)
  ),
  yssj_cf as
  (
  select XH,
         GPWJM,
           GPMXMC,
           JM_YC,     
         JZBH,
       count(*) over(partition by GPWJM,GPMXMC,JM_YC) as counts
       from yssj
  ),
 --------------------------------------------------------------------------
 WJMCZ_TMP(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         select  A.GPWJM as GPJWJM,a.gpwjm,a.GPMXMC,a.JM_YC,a.jzxh as xh,a.jzbh
          from SZPX.T_SZPX_ZJP_ZCFB_DL AS A
     where a.JZBH=V_JZBH
), 
WJMCZ_TMP1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,gpwjm,GPMXMC,JM_YC,xh,jzbh FROM WJMCZ_TMP
  UNION ALL
         SELECT GPJWJM, SUBSTR( GPWJM, LOCATE('/', GPWJM) + 1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 AS A WHERE LOCATE('/', GPWJM) <> 0 ), 
WJMCZ_TMP2(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
         SELECT GPJWJM,  CASE WHEN LOCATE('/', GPWJM) > 0 THEN SUBSTR(GPWJM, 1, LOCATE('/', GPWJM) - 1) ELSE GPWJM END AS GPWJM,
     GPMXMC,JM_YC,xh,jzbh 
     FROM WJMCZ_TMP1 WHERE GPWJM <> '' 
),
WJMCZ1(GPJWJM,GPWJM,GPMXMC,JM_YC,xh,jzbh) as (
    SELECT GPJWJM,LEFT(GPWJM,LENGTH(GPWJM)-1) AS GPWJM ,GPMXMC,JM_YC,xh,jzbh from (
     SELECT A.GPJWJM,
        a.GPMXMC,
        a.JM_YC,
        trim(char(REPLACE(replace(xml2clob(xmlagg(xmlelement(name a, C.GPWJM||'/'))),'</A>',''),'<A>',''))) as GPWJM,
        a.xh,
        a.jzbh
       FROM WJMCZ_TMP2 AS A
    LEFT JOIN SZPX.T_SZPX_ZJP_GPXXB_DL AS C
           on a.gpwjm=c.gpwjm
       or a.gpwjm=c.zdygpwjm
     WHERE C.GPWJM IS NOT NULL AND C.ZDYGPWJM IS NOT NULL
     group by a.gpjwjm,a.GPMXMC,a.JM_YC,a.xh,a.jzbh)
),
 ---------------------------------------------------------------------------
 yssj_jg (TNAME,RNUM,CNAME,JCJG,JZBH) as(
 --检测光谱文件名，光谱模型名称，建模或预测不允许重复
 SELECT 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH 
 from (SELECT XH,JZBH,COUNT(*) OVER(PARTITION BY GPWJM,GPMXMC,JM_YC) AS COU FROM WJMCZ1) as T
 WHERE COU>=2
union all
     --检测建模预测是否为空
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'JM_YC' as cname,'建模预测为空',JZBH from yssj where JM_YC IS NULL OR JM_YC=''
union all
   --检测光谱模型名称是否为空
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'光谱模型名称为空',JZBH from yssj where GPMXMC IS NULL OR GPMXMC=''
union all
 --检测光谱文件名是否为空
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名为空',JZBH from yssj where GPWJM IS NULL OR GPWJM=''
union all
--检测 光谱模型
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPMXMC' as cname,'无法找到该光谱模型',JZBH from yssj where GPMXMC NOT IN (SELECT GPMXMC FROM SZPX.T_DIM_SZPX_GPMXB WHERE ZYBJ=1) AND GPMXMC IS NOT NULL
union all
--检测光谱文件名是否存在
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gp
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB) and locate('/',gpwjm)=0
 union all
 select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名不存在',JZBH from yssj_gpdgjs
 where GPWJM NOT IN (Select GPWJM from SESSION.GPXXB)
union all
--检测光谱文件名，光谱模型名称，建模或预测不允许重复
select 'T_SZPX_ZJP_ZCFB_DL' AS XMMC,xh,'GPWJM' as cname,'光谱文件名，光谱模型名称，建模或预测 三项不唯一',JZBH  from yssj_cf
where counts>=2
  )
  
  select TNAME,RNUM,CNAME,JCJG,JZBH from yssj_jg;
COMMIT;


  
--检测新增字段中的数字类型的字段

SET OUT_JG='动态检测新增字段-中间品基础信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS','SYBH','LXID','ZDYSYBH','XMID', 'SYNF', 'SYYF','SYRQ', 'SCCJ', 'CPPHID', 'CPYHID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_JCXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('YPID', 'YPBS','SYBH','LXID','ZDYSYBH','XMID', 'SYNF', 'SYYF',' SYRQ', 'SCCJ', 'CPPHID', 'CPYHID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-检测指标表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','SYBH','JCLXID', 'GPID','GPWJM','GPJWJM', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_JCZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','SYBH','JCLXID', 'GPID','GPWJM','GPJWJM', 'JCR', 'JCJXID', 'JCSJ', 'BZ','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-光谱信息表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','GPWJM', 'GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_GPXXB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID','GPWJM', 'GPJWJM', 'GPLXID', 'SFCZGPWJ', 'JCSJ', 'JCJXID', 'JCR', 'BZ','XH','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-主成份表';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC','GPWJM','GPJWJM','YPID','CJSJ');


IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
  select ROW_NUMBER() over() as XH,                                            --序号,用于循环
       replace(TBCREATOR||'.'||TBNAME||'_DL',' ' ,'') AS TBNAME_CT,          --含模式名的表名
       TBNAME AS TBNAME_T,                                                   --不含模式名的表名
       NAME AS COLNAME                                                       --列名
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_ZCFB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'GPID', 'GPMXID', 'JM_YC','GPWJM','GPJWJM','YPID','CJSJ'))
SELECT XH, TBNAME_CT, TBNAME_T, COLNAME FROM YSSJ;
COMMIT;

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||'_DL'||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;

--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;


SET OUT_JG='动态检测新增字段-中间品评吸指标表(老版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','ZF','QXX','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_ZJP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','YPID','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZJXX','ZF','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB_LBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('ID','JZBH','XMMC','YPBS','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZF','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')
INTERSECT
select NAME,'T_SZPX_ZJP_PXZBB_LBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID','YPID','PXLX','GCX','QTX','ZTX','JTX','QX','XX','MX','JGX','GX','JX','HX','JXQ','QXX','ZF','ZJXX','NXX','PY','XF','CY','ND','JT','XQZ','XQL','TFX','QZQ','SQQ','KJQ','MZQ','TXQ','SZQ','HFQ','YCQ','JSQ','QT','XNRHCD','YRG','CJX','GZX','YW','FGTZ','PZTZ','PXR','PXSJ','BZ','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

SET OUT_JG='动态检测新增字段-中间品评吸指标表(新版)';

--判断是否有新增的数据型字段
select count(*) into V_SFXZZD FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('YPBS', 'JZBH', 'JZXH', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_ZJP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ'));

IF V_SFXZZD > 0 THEN
INSERT INTO SESSION.XZZD (XH,TBNAME_CT,TBNAME_T,COLNAME)
WITH YSSJ AS (
SELECT ROW_NUMBER() over() as XH,                                            --序号,用于循环
     replace(TBCREATOR||'.'||TBNAME,' ' ,'') AS TBNAME_CT,          --含模式名的表名
     TBNAME AS TBNAME_T,                                                   --不含模式名的表名
     NAME AS COLNAME 
  FROM (
select NAME,TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB_XBDL' 
     AND TBCREATOR='SZPX'
     AND NAME NOT IN ('YPBS', 'JZBH', 'JZXH', 'XMMC', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')
INTERSECT
select NAME,'T_SZPX_ZJP_PXZBB_XBDL' as TBNAME,TBCREATOR
    from sysibm.syscolumns 
   where TBNAME='T_SZPX_ZJP_PXZBB' 
     AND TBCREATOR='SZPX'
     AND COLTYPE  IN ('INTEGER','DOUBLE','BIGINT','INT')
     AND NAME NOT IN ('ID', 'YPID', 'PXLX', 'PXR', 'PXSJ', 'BZ', 'LBXQZ', 'LBXQL', 'LBZQ', 'LBJT', 'LBCJX', 'LBYW', 'LBZF','CJSJ')))
select XH,TBNAME_CT,TBNAME_T,COLNAME from yssj;
COMMIT; 

--设置开始序号和结束序号
SELECT MIN(XH),MAX(XH) INTO V_KSXH,V_JSXH 
  FROM SESSION.XZZD;

--开始循环检测
WHILE V_KSXH <= V_JSXH DO

--将表名,模式名,列名 赋予变量
SELECT TBNAME_CT,TBNAME_T,COLNAME INTO V_TBNAME_CT,V_TBNAME_T,V_COLNAME
  FROM SESSION.XZZD
 WHERE XH=V_KSXH;

--将检测结果导入结果表
SET EXE_SQL='
insert into SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH) 
SELECT '''||V_TBNAME_T||''',JZXH,'''||V_COLNAME||''',''包含无效字符'','''||V_JZBH||''' FROM '||V_TBNAME_CT||' WHERE TRANSLATE('||V_COLNAME||','''',''0123456789.-'')<>'''' AND JZBH='''||V_JZBH||''' ';

PREPARE s0 from exe_sql;
EXECUTE s0;
COMMIT;
SET V_KSXH=V_KSXH+1;
END WHILE;
END IF;
--清空临时表
DELETE FROM SESSION.XZZD;
COMMIT;

ELSE
COMMIT;
END IF;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------

--插入基本信息表检测结果
SET OUT_JG='开始插入检测结果';

SET V_RES=(select count(*) from SESSION.JGB);
SET OUT_JG=char(v_res);
INSERT INTO SZPX.T_SZPX_DLJG (TNAME,RNUM,CNAME,JCJG,JZBH)
with yssj as (
select TNAME, RNUM, CNAME, JCJG,JZBH,ROW_NUMBER()OVER(PARTITION BY TNAME,RNUM,CNAME ORDER BY JCJG DESC) AS BBXH
  from SESSION.JGB)
  select TNAME, RNUM, CNAME, JCJG,JZBH from yssj where BBXH=1;
COMMIT;

SELECT COUNT(*) INTO V_RES FROM SZPX.T_SZPX_DLJG WHERE JZBH=V_JZBH;

IF V_RES<>0 OR V_RES>0 THEN 
--如果存在异常数据,则删除临时表中的导入数据.
DELETE FROM SZPX.T_DIM_SZPX_XMB  AS A       WHERE XMLX=V_XMLX 
                         AND ID NOT IN(SELECT DISTINCT XMID FROM SZPX.T_SZPX_YL_JCXXB  WHERE XMID=A.ID
                                    UNION ALL
                                    SELECT DISTINCT XMID FROM SZPX.T_SZPX_CP_JCXXB  WHERE XMID=A.ID
                                    UNION ALL
                                    SELECT DISTINCT XMID FROM SZPX.T_SZPX_CPP_JCXXB WHERE XMID=A.ID
                                    UNION ALL
                                    SELECT DISTINCT XMID FROM SZPX.T_SZPX_ZJP_JCXXB WHERE XMID=A.ID)
                                             AND XMMC in (SELECT DISTINCT XMMC FROM SZPX.T_SZPX_YL_JCXXB_DL  WHERE JZBH=V_JZBH
                                    UNION ALL
                                    SELECT DISTINCT XMMC FROM SZPX.T_SZPX_CP_JCXXB_DL  WHERE JZBH=V_JZBH
                                    UNION ALL
                                    SELECT DISTINCT XMMC FROM SZPX.T_SZPX_CPP_JCXXB_DL WHERE JZBH=V_JZBH
                                    UNION ALL
                                    SELECT DISTINCT XMMC FROM SZPX.T_SZPX_ZJP_JCXXB_DL WHERE JZBH=V_JZBH
                                    );  
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_GPXXB_DL    WHERE JZBH=V_JZBH;     --删除光谱信息表(原料)
DELETE FROM SZPX.T_SZPX_CP_GPXXB_DL    WHERE JZBH=V_JZBH;     --删除光谱信息表(产品)
DELETE FROM SZPX.T_SZPX_CPP_GPXXB_DL   WHERE JZBH=V_JZBH;     --删除光谱信息表(中间件)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_JCXXB_DL    WHERE JZBH=V_JZBH;     --删除基础信息表(原料)
DELETE FROM SZPX.T_SZPX_CP_JCXXB_DL    WHERE JZBH=V_JZBH;     --删除基础信息表(产品)
DELETE FROM SZPX.T_SZPX_CPP_JCXXB_DL   WHERE JZBH=V_JZBH;     --删除基础信息表(中间件)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_JCZBB_DL    WHERE JZBH=V_JZBH;     --删除检测指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_JCZBB_DL    WHERE JZBH=V_JZBH;     --删除检测指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_JCZBB_DL   WHERE JZBH=V_JZBH;     --删除检测指标表(中间件)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_PXZBB_LBDL  WHERE JZBH=V_JZBH;     --删除老版评吸指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_PXZBB_LBDL  WHERE JZBH=V_JZBH;     --删除老版评吸指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_PXZBB_LBDL WHERE JZBH=V_JZBH;     --删除老版评吸指标表(中间件)
COMMIT;

DELETE FROM SZPX.T_SZPX_YL_PXZBB_XBDL  WHERE JZBH=V_JZBH;     --删除新版评吸指标表(原料)
DELETE FROM SZPX.T_SZPX_CP_PXZBB_XBDL  WHERE JZBH=V_JZBH;     --删除新版评吸指标表(产品)
DELETE FROM SZPX.T_SZPX_CPP_PXZBB_XBDL WHERE JZBH=V_JZBH;     --删除新版评吸指标表(中间件)
COMMIT;


DELETE FROM SZPX.T_SZPX_YL_ZCFB_DL     WHERE JZBH=V_JZBH;     --删除主成分表(原料)
DELETE FROM SZPX.T_SZPX_CP_ZCFB_DL     WHERE JZBH=V_JZBH;     --删除主成分表(产品)
DELETE FROM SZPX.T_SZPX_CPP_ZCFB_DL    WHERE JZBH=V_JZBH;     --删除主成分表(中间件)
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

END IF;

SET OUT_JG='1';
SET OP_V_ERR_MSG='1';

END;

GRANT EXECUTE ON PROCEDURE SZPX.P_SZPX_SJJY_ALL( VARCHAR(20), VARCHAR(40), VARCHAR(3000), VARCHAR(3000) ) TO USER DB2INST2 WITH GRANT OPTION;
