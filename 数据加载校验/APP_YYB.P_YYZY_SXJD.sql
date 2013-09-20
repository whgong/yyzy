SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_SXJD ( )
  SPECIFIC SQL100302170118900
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN 
  -- 定义系统变量   
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  -- 定义
  declare sm varchar(8000);
  DECLARE STMT VARCHAR(4000); 
  DECLARE STMT2 VARCHAR(4000);
  DECLARE STMT3 VARCHAR(4000);
  DECLARE TYPE CHAR(1);
  DECLARE COUNT,i_not_found,i_yynf INTEGER DEFAULT 0;
  DECLARE ISEXIST INTEGER DEFAULT 0;
  DECLARE i INTEGER DEFAULT 0;
  DECLARE STRCNT VARCHAR(100) DEFAULT '';
  DECLARE ERR_MSG VARCHAR(1000) DEFAULT '';
  -- DECLARE RUNSTATUS INTEGER;
  DECLARE AT_END SMALLINT DEFAULT 0;
  DECLARE RUNSTATUS INTEGER ;
  declare ddate date;
  declare v_lbdm_ky varchar(10);
  declare v_dcddm_yn varchar(10);
  declare v_ddj_sd,v_ddj_zd,v_ddj_xd varchar(10);
  declare v_qxjddm integer;
  
  -- 定义动态游标
  DECLARE c1 CURSOR /*with hold*/ WITH RETURN 
  FOR 
    select distinct yynf 
    FROM YYZY.T_YYZY_yykc_new A 
    where kcjs>0 and yynf between 2002 and year(current date)
  ;
  
  declare continue handler for not found
  begin
    set i_not_found = 1; 
  end;
  
  -- 定义异常处理
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  SET ERR_MSG = ERR_MSG||'系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';  ';
  
  /*正文*/
  Set ERR_MSG = '';
  
  select rtrim(char(yylbdm)) into v_lbdm_ky
  from dim.t_dim_yyzy_yylb
  where yylbmc='烤烟'
    and jsrq>current date
  ;
  select rtrim(char(yydcddm)) into v_dcddm_yn
  from dim.t_dim_yyzy_yydcd
  where yydcdmc='云南'
    and jsrq>current date
  ;
  
  select rtrim(char(YYDDJDM)) into v_ddj_sd
  from DIM.T_DIM_YYZY_YYDDJ
  where yyddjmc='上等烟'
    and jsrq>current date
  ;
  select rtrim(char(YYDDJDM)) into v_ddj_zd
  from DIM.T_DIM_YYZY_YYDDJ
  where yyddjmc='中等烟'
    and jsrq>current date
  ;
  select rtrim(char(YYDDJDM)) into v_ddj_xd
  from DIM.T_DIM_YYZY_YYDDJ
  where yyddjmc='下等烟'
    and jsrq>current date
  ;
  
  -- 初始化生产信息 
  delete from YYZY.T_YYZY_TMP_SCJH;
  insert into YYZY.T_YYZY_TMP_SCJH (PFPHDM, NY ,jhnf,jhyf)
  select distinct PFPHDM,char(jhnf*100+jhyf),char(jhnf),char(jhyf)
  from YYZY.V_YYZY_YJHFX_WHB
  where (jhnf*100+jhyf,bbh) in (
    select jhnf*100+jhyf,max(bbh) 
    from YYZY.V_YYZY_YJHFX_WHB 
    group by jhnf*100+jhyf
  ) and jhnf in (year(current date),year(current date)+1,year(current date)+2);
  
  -- 生成品牌角色
  delete from YYZY.T_YYZY_QXJS where ybzd in ('PPDM','YYDCDDM','YYCDDM');
  insert into YYZY.T_YYZY_QXJS (QXJSDM, QXJSMC, YBMC, YBZD, YBDM, ZYBJ)
  with tmp as (
    select distinct pfphdm, pfphmc,ppmc,a.ppdm 
    from (
      select LSBH,PZDM,PZMC,YHBS,SCCJDM,PPDM,KSRQ,JSRQ 
      from DIM.T_DIM_YYZY_PZ 
      where jsrq>current date 
    ) as a,
    (
      select LSBH,PFPHDM,PFPHBS,PFPHMC,YHBS,SCCJDM,KSRQ,JSRQ 
      from DIM.T_DIM_YYZY_PFPH 
      where jsrq>current date
    ) as b,
    (
      select * from DIM.T_DIM_YYZY_PP  
      where jsrq>current date
    ) as c 
    where a.yhbs=b.yhbs 
      and a.sccjdm=b.sccjdm 
      and a.ppdm=c.ppdm 
      and a.ppdm<>0 and 
      ppmc not like '%飞马%' 
  )  
  select ppdm+200000 QXJSDM,ppmc as QXJSMC, 'DIM.T_DIM_YYZY_PP' YBMC, 'PPDM' YBZD, ppdm YBDM, '1' ZYBJ
  from (
    select distinct ppdm,ppmc 
    from tmp
  ) a;
  
  insert into YYZY.T_YYZY_QXJS(QXJSDM,QXJSMC,YBMC,YBZD,YBDM,ZYBJ) 
  select YYDCDDM+100000 QXJSDM,YYDCDMC QXJSMC,'DIM.T_DIM_YYZY_YYDCD' ybmc,'YYDCDDM' YBZD,YYDCDDM YBDM,'1' ZYBJ
  from DIM.T_DIM_YYZY_YYDCD 
  where jsrq>current date;
  
  -- 删除节点
  /* 2011-02-14 龚玮慧修改 清空动态节点的起始位置 */
  /*
  delete from YYZY.T_YYZY_QXJD where qxjddm not between 1 and 86;
  */
  delete from YYZY.T_YYZY_QXJD where qxjddm not between 1 and 94;
  /* 以上为 2011-02-14 龚玮慧修改 清空动态节点的起始位置 */


  -- 配方分析
  insert into YYZY.T_YYZY_QXJD (QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ )
  with tmp as (
    select distinct pfphdm, pfphmc, ppmc, a.ppdm
    from (
      select LSBH, PZDM, PZMC, YHBS, SCCJDM, PPDM, KSRQ, JSRQ
      from DIM.T_DIM_YYZY_PZ
      where jsrq > current date
    ) as a,
    (
      select LSBH, PFPHDM, PFPHBS, PFPHMC, YHBS, SCCJDM, KSRQ, JSRQ
      from DIM. T_DIM_YYZY_PFPH
      where jsrq > current date
    ) b,
    (
      select * 
      from DIM. T_DIM_YYZY_PP 
      where jsrq > current date
    ) c
    where a.yhbs = b.yhbs 
      and a.sccjdm = b.sccjdm 
      and a.ppdm = c.ppdm 
      and a.ppdm <> 0
      and ppmc not like '%飞马%'
      and pfphdm in (
        select PFPHDM
        from YYZY.T_YYZY_TMP_SCJH
        where rtrim(jhnf) = substr(char(current date), 1, 4)
          and rtrim(jhyf) = rtrim(char(month(current date)))
      )
  )
  ,tmp_3 as (
    select (SELECT VALUE(MAX(QXJDDM), 0) FROM YYZY.T_YYZY_QXJD) + ROWNUMBER() OVER() QXJDDM,
        ppmc as QXJDMC,2 as QXJDLX,FJDDM,3 as QXJDJB,ppdm XSSX,
        cast(null as varchar(1)) LJSX,cast(null as varchar(1)) JBSX,
        (SELECT QXMKDM FROM YYZY.T_YYZY_QXJD WHERE QXJDMC = '配方分析') QXMKDM,
        ppdm + 200000 as QXJSDM,'1' as ZYBJ
    from (
      select distinct ppdm, ppmc, 2 fjddm
      from tmp
      union all
      select distinct ppdm, ppmc, 3 fjddm from tmp
    ) as a
  )
  ,tmp2 as (
    select distinct pfphdm, pfphmc,ppmc,a.ppdm 
    from (
      select LSBH, PZDM, PZMC, YHBS, SCCJDM, PPDM, KSRQ, JSRQ 
      from DIM.T_DIM_YYZY_PZ 
      where jsrq>current date
    ) as a,
    (
      select LSBH,PFPHDM,PFPHBS,PFPHMC,YHBS,SCCJDM,KSRQ,JSRQ 
      from DIM.T_DIM_YYZY_PFPH 
      where jsrq>current date
    ) as b,
    ( 
      select * from DIM.T_DIM_YYZY_PP  
      where jsrq>current date
    ) as c 
    where a.yhbs=b.yhbs 
      and a.sccjdm=b.sccjdm 
      and a.ppdm=c.ppdm 
      and a.ppdm<>0
      and ppmc not like '%飞马%' 
      and pfphdm in (
        select  PFPHDM 
        from YYZY.T_YYZY_TMP_SCJH 
        where rtrim(jhnf)=substr(char(current date),1,4) 
          and rtrim(jhyf)=rtrim(char(month(current date)))
      )
  )
  ,tmp_4 as (
    select (SELECT VALUE(MAX(QXJDDM), 0) FROM tmp_3) + ROWNUMBER() OVER() as QXJDDM,
        pfphmc QXJDMC,1 QXJDLX,QXJDDM  FJDDM,4 QXJDJB,pfphdm XSSX,
        case 
          when FJDDM=2 then 
            '/tsas/treeOperationAction.do'||
            '?createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=formualOutput'||
            '&formualID='||char(pfphdm)
          when FJDDM=3 then 
            '/tsas/treeOperationAction.do'||
            '?createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=formualPeriod'||
            '&formualID='||char(pfphdm)
        end LJSX,
        cast(null as varchar(1)) as JBSX,A.QXMKDM as QXMKDM,
        cast(null as integer) as QXJSDM,'1' as ZYBJ
    from tmp_3 as a,
    tmp as b 
    where a.qxjsdm=ppdm+200000
  )
  select * from tmp_3
  union all
  select * from tmp_4;
  
  set SM=SM||'配方分析';
  
  -- 计划分析  三年  分年份
  insert into YYZY.T_YYZY_QXJD( QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
  with tmp (nf)AS (
    VALUES year(current date),year(current date)+1,year(current date)+2
  )
  select (SELECT VALUE(MAX(QXJDDM),0) FROM YYZY.T_YYZY_QXJD) + ROWNUMBER() OVER() as QXJDDM,
      char(b.nf) as QXJDMC,1 as QXJDLX,a.qxjddm as FJDDM,3 as QXJDJB,b.nf as XSSX,
      (
        '/tsas/treeOperationAction.do'||
        '?createOtherView_ERUPTION_BUTTON=1'||
        '&viewType=timePlanAnalyse'||
        '&jhnf='||char(b.nf) 
      ) as LJSX,
      cast(null as varchar(1)) as JBSX,QXMKDM,
      cast(null as integer) as QXJSDM,'1' as ZYBJ
  from YYZY.T_YYZY_QXJD as a,
  tmp as b 
  where qxjddm=5; 
  
  insert into YYZY.T_YYZY_QXJD (QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
  with tmp (nf)AS (
    select distinct month(riqi) 
    from DIM.T_DIM_YYZY_DATE
    where year(riqi)*100+month(riqi) 
      between year(current date) *100+00 
      and year(current date) *100+month(current date)
  )
  select (SELECT VALUE(MAX(QXJDDM),0) FROM YYZY.T_YYZY_QXJD)+ROWNUMBER()OVER() as QXJDDM,
      rtrim(char(b.nf))||'月' as QXJDMC,1 as QXJDLX, a.qxjddm as FJDDM,4 as QXJDJB,b.nf as XSSX,
      (
        '/tsas/treeOperationAction.do'||
        '?createOtherView_ERUPTION_BUTTON=1'||
        '&viewType=timePlanAnalyse'||
        '&jhnf='||rtrim(qxjdmc)||
        '&jhyf='||char(b.nf)
      ) as LJSX,
      cast(null as varchar(1)) as JBSX,QXMKDM,
      cast(null as integer) as QXJSDM,'1' as ZYBJ
  from (
    select * 
    from YYZY.T_YYZY_QXJD 
    where fjddm=5 
      and qxjdmc = char(year(current date))
  ) as a,
  tmp as b
  ;
  -- 当年分月份
  insert into YYZY.T_YYZY_QXJD (QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
  with tmp as (
    select distinct ppmc,a.ppdm,rtrim(jhyf)||'月'jhyf
    from (
      select LSBH,PZDM,PZMC,YHBS,SCCJDM,PPDM,KSRQ,JSRQ
      from DIM.T_DIM_YYZY_PZ
      where jsrq > current date
    ) as a,
    (
      select LSBH,PFPHDM,PFPHBS,PFPHMC,YHBS,SCCJDM,KSRQ,JSRQ
      from DIM. T_DIM_YYZY_PFPH
      where jsrq > current date
    ) as b,
    (
      select * 
      from DIM. T_DIM_YYZY_PP 
      where jsrq > current date 
    ) as c,
    (
      select PFPHDM,jhyf
      from YYZY.T_YYZY_TMP_SCJH
      where int(jhnf) = year(current date) and int(jhyf) <= month(current date)
    ) as d
    where a.yhbs = b.yhbs and a.sccjdm = b.sccjdm
      and a.ppdm = c.ppdm 
      and a.ppdm <> 0
      and ppmc not like '%飞马%'
      and b.pfphdm =d.pfphdm 
  )
  ,tmp11 as(
    select distinct ppmc,ppdm,jhyf 
    from tmp
  )
  ,tmp2(gg,sx) AS (
    VALUES
      ('烟号',0),
      ('规格',1)
  )
  ,tmp3 as (
    select a.*,rtrim(qxjdmc) jhyf 
    from YYZY.T_YYZY_QXJD a 
    where fJDDM in ( 
      select QXJDDM 
      from YYZY.T_YYZY_QXJD 
      where fjddm=5 
    )
  )
  ,tmp_5 as(
    select rownumber() over() as QXJDDM,ppmc as QXJDMC,2 as qxjdlx,
        qxjddm as fjddm,5 as QXJDJB,ppdm as xssx,cast(null as varchar(1)) as LJSX,
        cast(null as varchar(1)) as JBSX,QXMKDM,ppdm+200000 as QXJSDM,
        '1' as ZYBJ,rtrim(char(a.xssx)) as jhyf
    from tmp3 as a ,
    tmp as b
    where a.jhyf=b.jhyf
  )
  ,tmp_6 as (
    select (select count(1) from tmp_5)+rownumber() over() as QXJDDM,
        gg as QXJDMC,1 as QXJDLX,QXJDDM as FJDDM,6 as QXJDJB,sx as XSSX,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=timePlanAnalyse'||
          '&jhnf='||substr(char(current date),1,4)||
          '&jhyf='||rtrim(char(jhyf))||
          '&ppdm='||rtrim(char(xssx))||
          '&yhorgg='||char(sx)
        ) as LJSX, 
        JBSX, QXMKDM,cast(null as integer) as QXJSDM,ZYBJ 
    from tmp_5 a,tmp2 b
  )
  select (SELECT VALUE(MAX(QXJDDM), 0) FROM YYZY.T_YYZY_QXJD) + QXJDDM as qxjddm,
      QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ
  from tmp_5
  union all
  select(SELECT VALUE(MAX(QXJDDM), 0) FROM YYZY.T_YYZY_QXJD) + QXJDDM as qxjddm,
      QXJDMC,QXJDLX,
      (SELECT VALUE(MAX(QXJDDM), 0) FROM YYZY.T_YYZY_QXJD) + FJDDM as FJDDM,
      QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ
  from tmp_6;
  
  -- 计划分析 按品牌
  insert into YYZY.T_YYZY_QXJD (QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
  with tmp as (
    select distinct b.pfphdm,pfphmc,ppmc,a.ppdm, rtrim(char(jhnf)) jhnf
    from (
      select LSBH,PZDM,PZMC,YHBS,SCCJDM,PPDM,KSRQ,JSRQ
      from DIM.T_DIM_YYZY_PZ
      where jsrq > current date
    ) as a,
    (
      select LSBH,PFPHDM,PFPHBS,PFPHMC,YHBS,SCCJDM,KSRQ,JSRQ
      from DIM. T_DIM_YYZY_PFPH
      where jsrq > current date
    ) as b,
    (
      select * 
      from DIM. T_DIM_YYZY_PP 
      where jsrq > current date
    ) as c,
    (
      select distinct PFPHDM,jhnf
      from (
        select TOBACCOID,JHNF,PZMC,YHBS,CJDM,JHCL,PPMC,BBH,BBRQ,
            (select min(pfphdm) from DIM.T_DIM_YYZY_PFPH where yhbs=a.yhbs and sccjdm=a.cjdm) as pfphdm 
        from YYZY.T_YYZY_SNSCJH_1 as a
      ) ff
    ) d 
    where a.yhbs = b.yhbs 
      and a.sccjdm = b.sccjdm 
      and a.ppdm = c.ppdm 
      and a.ppdm <> 0 
      and ppmc not like '%飞马%' 
      and b.pfphdm =d.pfphdm 
  )
  ,tmp_sj(nf) AS ( 
    VALUES 
      year(current date), 
      year(current date)+1, 
      year(current date)+2, 
      year(current date)-1, 
      year(current date)-2 
  )
  ,tmp11 as (
    select distinct ppmc,ppdm 
    from tmp
  )
  ,tmp2(gg,sx)AS (
    VALUES
      ('烟号',1),
      ('规格',2)
  )
  ,tmp3 as (
    select * 
    from YYZY.T_YYZY_QXJD 
    where QXJDDM=6
  )
  ,tmp_3 as (
    select rownumber() over() as QXJDDM,
        ppmc AS QXJDMC,1 AS qxjdlx,qxjddm as fjddm,
        3 as QXJDJB,ppdm as xssx,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=breedPlanAnalyse'||
          '&ppdm='||char(ppdm)
        ) as LJSX,
        cast(null as varchar(1)) as JBSX,
        QXMKDM,ppdm+200000 as QXJSDM,'1' as ZYBJ
    from tmp3 as a , 
    tmp11 as b
  )
  ,tmp_4 as (
    select (select max(QXJDDM) from tmp_3)+rownumber() over() as QXJDDM,rtrim(char(nf)) as QXJDMC,
        1 as qxjdlx,qxjddm as fjddm,4 as QXJDJB,nf as xssx,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=ppPlanAnalyse'||
          '&jhnf='||rtrim(char(nf))||
          '&ppdm='||rtrim(char(xssx))
        ) as LJSX,
        cast(null as varchar(1)) as JBSX,QXMKDM,
        cast(null as integer) as QXJSDM,'1' as ZYBJ,
        qxjdmc as sjmc,rtrim(char(xssx)) as sjppdm
    from tmp_3 as a , 
    tmp_sj as b
  )
  ,tmp4 as (
    select distinct pfphdm,pfphmc,jhnf,ppmc 
    from tmp
  )
  ,tmp_5 as (
    select (select max(QXJDDM) from tmp_4)+rownumber() over() as QXJDDM,
      pfphmc as QXJDMC,1 as qxjdlx,qxjddm as fjddm,5 as QXJDJB,pfphdm as xssx,
      (
        '/tsas/treeOperationAction.do'||
        '?createOtherView_ERUPTION_BUTTON=1'||
        '&viewType=ppPlanAnalyse'||
        '&jhnf='||qxjdmc||
        '&ppdm='||sjppdm||
        '&pfphdm='||char(pfphdm)
      ) as LJSX,
      cast(null as varchar(1)) as JBSX,QXMKDM,
      cast(null as integer) QXJSDM,'1' as ZYBJ
    from tmp_4 as a , 
    tmp4 as b 
    where a.QXJDMC=b.jhnf 
      and sjmc=ppmc
  )
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+QXJDDM as qxjddm, 
      QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_3
  union all
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+QXJDDM as qxjddm, 
      QXJDMC, QXJDLX, (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+FJDDM as fjddm, 
      QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_4
  union all
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+QXJDDM as qxjddm, 
      QXJDMC, QXJDLX, 
      (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+FJDDM as fjddm, 
      QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_5
  ;
  
  set SM=SM||'计划分析';
  
  
-- 需求分析 
-- 采购需求 
  delete from YYZY.T_YYZY_QXJD where qxmkdm=4 and qxjddm>200; 
  -- 分产地 
  insert into YYZY.T_YYZY_QXJD 
  with lb as ( 
    select distinct yylbdm,yylbmc 
    from 
      (
        select yylbdm,yylbmc
        from YYZY.T_YYZY_XQFX_CGXQCD
        union all
        select YYLBDM,yylbmc
        from YYZY.V_YYZY_TZXQ_YY
		UNION ALL             --片烟
		SELECT yylbdm,yylbmc
		FROM YYZY.V_YYZY_PYTZXQ
		UNION ALL 
		SELECT yylbdm,yylbmc
		FROM YYZY.V_YYZY_PYCGJHL
		UNION ALL 
		SELECT yylbdm,yylbmc FROM YYZY.T_YYZY_XQFX_XQYJHFX_PY

      ) as a 
    where yylbdm is not null 
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from (
      select gnwbj
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all
      select gnwbj
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT gnwbj
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT gnwbj
	  FROM YYZY.V_YYZY_PYCGJHL
	  UNION ALL 
		SELECT gnwbj FROM YYZY.T_YYZY_XQFX_XQYJHFX_PY
    ) as a  
    where gnwbj is not null 
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from (
      select gnwbj,yydcddm,yydcdmc,yylbdm,yylbmc
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all
      select gnwbj,yydcddm,yydcdmc,yylbdm,yylbmc
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT gnwbj,yydcddm,yydcdmc,yylbdm,yylbmc
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT gnwbj,yydcddm,yydcdmc,yylbdm,yylbmc
	  FROM YYZY.V_YYZY_PYCGJHL
	  UNION ALL -- 2 
	  SELECT gnwbj,yydcddm,yydcdmc,yylbdm,yylbmc
	  FROM YYZY.V_YYZY_PYCGJHL
	  UNION ALL    --  2
	  SELECT case when c.gnw='国内' then 1 else 0 end gnwbj,c.dcddm as yydcddm,c.dcdmc as yydcdmc,c.yylbdm,c.yylbmc
	   FROM YYZY.V_YYZY_CGXQL AS A
	   		JOIN YYZY.T_YYZY_SYSDYDM AS B
				 ON A.YYDM=B.YYDM
			JOIN YYZY.T_YYZY_YYZDBMX AS C
				 ON  (B.YYLBDM,B.YYKBDM,B.YYCDDM,B.YYDJDM)=(C.YYLBDM,C.YYKBDM,C.YYCDDM,C.YYDJDM)
    ) as a  
    where yydcddm is not null 
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,YYLBDM,YYLBMC 
    from (
      select yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all
      select yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc
	  FROM YYZY.V_YYZY_PYCGJHL
	  UNION ALL    --  2
	  SELECT c.dcddm as yydcddm,c.dcdmc as yydcdmc,c.yylbdm,c.yylbmc,c.yycddm,c.yycdmc 
	   FROM YYZY.V_YYZY_CGXQL AS A
	   		JOIN YYZY.T_YYZY_SYSDYDM AS B
				 ON A.YYDM=B.YYDM
			JOIN YYZY.T_YYZY_YYZDBMX AS C
				 ON  (B.YYLBDM,B.YYKBDM,B.YYCDDM,B.YYDJDM)=(C.YYLBDM,C.YYKBDM,C.YYCDDM,C.YYDJDM)
    ) as a 
    where yycddm is not null 
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX 
    , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD 
    where fjddm=9 and qxjddm=13 and zybj='1' 
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,yylbmc as qxjdmc,1 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,yylbmc as qxjdmc,2 as 

qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
    , QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ;
  -- 分品牌 
  insert into YYZY.T_YYZY_QXJD
  with pp as (
    select distinct PPDM,PPMC
    from YYZY.T_YYZY_XQFX_CGXQPP
    where YYMC is not null and yydm is not null
  ),
  pfph as(
    select distinct PPDM,PPMC,PFPHDM,PFPHMC
    from YYZY.T_YYZY_XQFX_CGXQPP 
    where yymc is not null and yydm is not null 
  ),
  jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
        ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 14 and FJDDM = 9 and ZYBJ = '1'
  ),
  jd_pp as
(select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,PPMC as QXJDMC,1 as QXJDLX,
     QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,rtrim(ljsx)||'&PPDM='||char(PPDM) as LJSX,JBSX,QXMKDM, 
QXJSDM,'1' as ZYBJ
  from jd_z inner join pp
  on PPMC='中华'
  union all
  select (select value(max(QXJDDM),0) 
  from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
       PPMC as QXJDMC,2 as QXJDLX,
       QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
	   rtrim(ljsx)||'&PPDM='||char(PPDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_z inner join pp
  on PPMC<>'中华'
  ),
  jd_pfph as
  (select(select value(max(QXJDDM),0) from jd_pp)+rownumber()over() as QXJDDM,PFPHMC as QXJDMC,2 as QXJDLX,
  QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
  rtrim(ljsx)||'&PFPHDM='||char(PFPHDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_pp inner join pfph
  on PPMC = QXJDMC
  ),
  result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as
  (
  select * from jd_pp
  union all
  select * from jd_pfph
  )
  select * from result
  ;
  /*
  -- 剩余烟叶统计
  insert into YYZY.T_YYZY_QXJD 
  with lb as ( 
    select distinct yylbdm,yylbmc 
    from YYZY.T_YYZY_XQFX_CGXQSY 
    where yylbdm is not null
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from YYZY.T_YYZY_XQFX_CGXQSY 
    where gnwbj is not null
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from YYZY.T_YYZY_XQFX_CGXQSY 
    where yydcddm is not null
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,YYLBMC,YYLBDM 
    from YYZY.T_YYZY_XQFX_CGXQSY 
    where yycddm is not null
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX 
    , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD 
    where fjddm=14 and qxjddm=17 and zybj='1' 
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,yylbmc as qxjdmc,1 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,yylbmc as qxjdmc,2 as 

qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
    , QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ;
  */
  
  -- 分等级 
  insert into YYZY.T_YYZY_QXJD 
  with lb as (
    select distinct YYLBDM,YYLBMC 
    from (
      select yylbdm,yylbmc
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all 
      select yylbdm,yylbmc
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT yylbdm,yylbmc
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT yylbdm,yylbmc
	  FROM YYZY.V_YYZY_PYCGJHL
    ) as a 
    where YYLBDM is not null
  )
  ,
  ddj as (
    select distinct YYLBDM,YYLBMC,YYDDJDM,YYDDJMC
    from (
      select yylbdm,yylbmc,yyddjdm,yyddjmc
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all 
      select yylbdm,yylbmc,yyddjdm,yyddjmc
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc
	  FROM YYZY.V_YYZY_PYCGJHL
    ) as a 
    where YYDDJDM is not null
  )
  , 
  dj as (
    select distinct YYDDJDM,YYDDJMC,YYDJDM,YYDJMC,YYLBDM,YYLBMC
    from (
      select yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
      from YYZY.T_YYZY_XQFX_CGXQCD
      union all 
      select yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
      from YYZY.V_YYZY_TZXQ_YY
	  UNION ALL             --片烟
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
	  FROM YYZY.V_YYZY_PYTZXQ
	  UNION ALL 
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
	  FROM YYZY.V_YYZY_PYCGJHL
	  UNION ALL    --  2
	  SELECT C.yylbdm,C.yylbmc,C.yyddjdm,C.yyddjmc,C.yydjdm,C.yydjmc
	   FROM YYZY.V_YYZY_CGXQL AS A
	   		JOIN YYZY.T_YYZY_SYSDYDM AS B
				 ON A.YYDM=B.YYDM
			JOIN YYZY.T_YYZY_YYZDBMX AS C
				 ON  (B.YYLBDM,B.YYKBDM,B.YYCDDM,B.YYDJDM)=(C.YYLBDM,C.YYKBDM,C.YYCDDM,C.YYDJDM)
    ) as a
    where YYDJDM is not null
  )
  ,
  jd_Z as ( 
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
      ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD
    where QXJDDM = 15 and FJDDM = 9 and ZYBJ = '1' 
  )
  ,
  jd_lb as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,YYLBMC as QXJDMC,1 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,rtrim(ljsx)||'&lbdm='||char(YYLBDM) as LJSX,JBSX,QXMKDM, 
      QXJSDM,'1' as ZYBJ
    from jd_z inner join lb
      on yylbmc='烤烟'
    union all
    select (select value(max(QXJDDM),0) 
    from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
       YYLBMC as QXJDMC,2 as QXJDLX,
       QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
	   rtrim(ljsx)||'&lbdm='||char(YYLBDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z inner join lb on yylbmc<>'烤烟'
  ),
  -- select * from jd_lb
 jd_ddj as
 (select(select value(max(QXJDDM),0) from jd_lb)+rownumber()over() as QXJDDM,YYDDJMC as QXJDMC,2 as QXJDLX,
  QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
  rtrim(ljsx)||'&DDJDM='||char(YYDDJDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_lb inner join ddj
  on QXJDMC = '烤烟'
  where QXJDMC = YYLBMC
  ),
  jd_dj as
  (select(select value(max(QXJDDM),0) from jd_ddj)+rownumber()over() as QXJDDM,YYDJMC as QXJDMC,2 as QXJDLX,
  QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
  rtrim(ljsx)||'&DJDM='||char(YYDJDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_ddj inner join dj
  on YYDDJMC = QXJDMC
  and YYLBMC = '烤烟'
  ),
  result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as
  (select * from jd_lb
  union all
  select * from jd_ddj
  union all
  select * from jd_dj
  )
  select * from result 
  ; 
  
  /* 2011-01-18 龚玮慧新增功能 */
  --特殊处理 删除进口烟下所有等级节点 进口烟展示内容同分产地中的国外节点
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=9 and qxjdmc='分等级'), -1 ) into v_qxjddm; 
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='烤烟'), -1 ) into v_qxjddm;
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='进口烟'), -1 ) into v_qxjddm;
  
  delete from YYZY.T_YYZY_QXJD 
  where fjddm = v_qxjddm
  ;
  update YYZY.T_YYZY_QXJD
  set ljsx=''||
    '/tsas/treeOperationAction.do'||
    '?createOtherView_ERUPTION_BUTTON=1'||
    '&viewType=stockanalysByarea'||
    '&lbdm=1&gnwbj=0'
  where qxjddm = v_qxjddm 
  ;
  /* 以上为2011-01-18 龚玮慧新增功能 */  

  /* 2011-02-14 龚玮慧 新增中华挑烟节点*/
  -- 采购需求 中华挑烟
  -- 分产地 
  insert into YYZY.T_YYZY_QXJD 
  with 
  sj_all as ( 
    select yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc,gnwbj 
    from YYZY.T_YYZY_XQFX_CGXQPP 
    where yydjmc like '%挑%'
	UNION ALL             --片烟
	  SELECT yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc,gnwbj 
	  FROM YYZY.V_YYZY_PYTZXQ WHERE yydjmc like '%挑%'
	  UNION ALL 
	  SELECT yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc,gnwbj 
	  FROM YYZY.V_YYZY_PYCGJHL WHERE yydjmc like '%挑%'
	    UNION ALL    --  2
	  SELECT C.dcddm as yydcddm,C.dcdmc as yydcdmc,C.yylbdm,C.yylbmc,C.yycddm,C.yycdmc,CASE WHEN C.GNW='国内' then 1 else 0 end as gnwbj
	   FROM YYZY.V_YYZY_CGXQL AS A
	   		JOIN YYZY.T_YYZY_SYSDYDM AS B
				 ON A.YYDM=B.YYDM
			JOIN YYZY.T_YYZY_YYZDBMX AS C
				 ON  (B.YYLBDM,B.YYKBDM,B.YYCDDM,B.YYDJDM)=(C.YYLBDM,C.YYKBDM,C.YYCDDM,C.YYDJDM)
		WHERE c.yydjmc like '%挑%'
--    union all
--    select yydcddm,yydcdmc,yylbdm,yylbmc,yycddm,yycdmc,gnwbj 
--    from YYZY.V_YYZY_TZXQ_YY
--    where yydjmc like '%挑%'
  )
  ,lb as ( 
    select distinct yylbdm,yylbmc 
    from sj_all as a 
    where yylbdm is not null 
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from sj_all as a  
    where gnwbj is not null 
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from sj_all as a  
    where yydcddm is not null 
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,YYLBDM,YYLBMC 
    from sj_all as a 
    where yycddm is not null 
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD 
    where fjddm=88 and qxjddm=92 and zybj='1' 
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,yylbmc as qxjdmc,1 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,yylbmc as qxjdmc,2 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
        rtrim(ljsx)||'&lbdm='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ;
  -- 分品牌 
  insert into YYZY.T_YYZY_QXJD
  with 
  sj_all as (
    select * 
    from YYZY.T_YYZY_XQFX_CGXQPP 
    where yydjmc like '%挑%'
  )
  ,pp as (
    select distinct PPDM,PPMC
    from sj_all
    where YYMC is not null and yydm is not null
  ),
  pfph as(
    select distinct PPDM,PPMC,PFPHDM,PFPHMC 
    from sj_all 
    where yymc is not null and yydm is not null 
  ),
  jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 93 and FJDDM = 88 and ZYBJ = '1'
  ),
  jd_pp as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,PPMC as QXJDMC,1 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,rtrim(ljsx)||'&PPDM='||char(PPDM) as LJSX,JBSX,QXMKDM, 
      QXJSDM,'1' as ZYBJ
    from jd_z inner join pp on PPMC='中华'
    union all
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
      PPMC as QXJDMC,2 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
      rtrim(ljsx)||'&PPDM='||char(PPDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z inner join pp on PPMC<>'中华'
  ),
  jd_pfph as (
    select(select value(max(QXJDDM),0) from jd_pp)+rownumber()over() as QXJDDM,PFPHMC as QXJDMC,2 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX, 
      rtrim(ljsx)||'&PFPHDM='||char(PFPHDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_pp inner join pfph on PPMC = QXJDMC
  ),
  result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as (
    select * from jd_pp 
    union all
    select * from jd_pfph
  )
  select * from result
  ;

  -- 分等级 
  insert into YYZY.T_YYZY_QXJD 
  with 
  sj_all as (
    select yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
    from YYZY.T_YYZY_XQFX_CGXQPP
    where yydjmc like '%挑%'
		  UNION ALL             --片烟
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
	  FROM YYZY.V_YYZY_PYTZXQ WHERE yydjmc like '%挑%'
	  UNION ALL 
	  SELECT yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
	  FROM YYZY.V_YYZY_PYCGJHL WHERE yydjmc like '%挑%'
	  UNION ALL    --  2
	  SELECT  c.yylbdm,c.yylbmc,c.yyddjdm,c.yyddjmc,c.yydjdm,c.yydjmc
	   FROM YYZY.V_YYZY_CGXQL AS A
	   		JOIN YYZY.T_YYZY_SYSDYDM AS B
				 ON A.YYDM=B.YYDM
			JOIN YYZY.T_YYZY_YYZDBMX AS C
				 ON  (B.YYLBDM,B.YYKBDM,B.YYCDDM,B.YYDJDM)=(C.YYLBDM,C.YYKBDM,C.YYCDDM,C.YYDJDM)
		WHERE c.yydjmc like '%挑%'
--    union all 
--    select yylbdm,yylbmc,yyddjdm,yyddjmc,yydjdm,yydjmc
--    from YYZY.V_YYZY_TZXQ_YY
--    where yydjmc like '%挑%'
  )
  ,lb as (
    select distinct YYLBDM,YYLBMC 
    from sj_all as a 
    where YYLBDM is not null
  )
  ,
  ddj as (
    select distinct YYLBDM,YYLBMC,YYDDJDM,YYDDJMC
    from sj_all as a 
    where YYDDJDM is not null
  )
  , 
  dj as (
    select distinct YYDDJDM,YYDDJMC,YYDJDM,YYDJMC,YYLBDM,YYLBMC
    from sj_all as a 
    where YYDJDM is not null
  )
  ,
  jd_Z as ( 
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD
    where QXJDDM = 94 and FJDDM = 88 and ZYBJ = '1' 
  )
  ,
  jd_lb as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,YYLBMC as QXJDMC,1 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,rtrim(ljsx)||'&lbdm='||char(YYLBDM) as LJSX,JBSX,QXMKDM, 
      QXJSDM,'1' as ZYBJ
    from jd_z inner join lb
      on yylbmc='烤烟'
    union all
    select (select value(max(QXJDDM),0) 
    from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
       YYLBMC as QXJDMC,2 as QXJDLX,
       QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
	   rtrim(ljsx)||'&lbdm='||char(YYLBDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z inner join lb on yylbmc<>'烤烟'
  ),
  -- select * from jd_lb
 jd_ddj as (
    select (select value(max(QXJDDM),0) from jd_lb)+rownumber()over() as QXJDDM,YYDDJMC as QXJDMC,2 as QXJDLX,
      QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
      rtrim(ljsx)||'&DDJDM='||char(YYDDJDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_lb inner join ddj on QXJDMC = '烤烟'
    where QXJDMC = YYLBMC
  ),
  jd_dj as (
    select (select value(max(QXJDDM),0) from jd_ddj)+rownumber()over() as QXJDDM,
      YYDJMC as QXJDMC,2 as QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
      rtrim(ljsx)||'&DJDM='||char(YYDJDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_ddj inner join dj on YYDDJMC = QXJDMC and YYLBMC = '烤烟' 
  ),
  result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as
  (select * from jd_lb
  union all
  select * from jd_ddj
  union all
  select * from jd_dj
  )
  select * from result 
  ; 

  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=88 and qxjdmc='分等级'), -1 ) into v_qxjddm; 
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='烤烟'), -1 ) into v_qxjddm; 
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='进口烟') ,-1 ) into v_qxjddm;
  
  delete from YYZY.T_YYZY_QXJD 
  where fjddm = v_qxjddm 
  ;
  update YYZY.T_YYZY_QXJD
  set ljsx=''||
    '/tsas/treeOperationAction.do'||
    '?createOtherView_ERUPTION_BUTTON=1'||
    '&viewType=stockanalysByarea'||
    '&sftyflg=1'||
    '&lbdm=1&gnwbj=0'
  where qxjddm = value(v_qxjddm,-1) 
  ;

  /* 以上为2011-02-14 龚玮慧 新增中华挑烟节点*/
  
  -- 烟叶需求(调整为 库存分析)
  -- 分产地 
  insert into YYZY.T_YYZY_QXJD 
  with lb as ( 
    select distinct yylbdm,yylbmc 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yylbdm is not null 
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where gnwbj is not null 
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yydcddm is not null 
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,YYLBDM,YYLBMC 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yycddm is not null 
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX 
    , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD 
    where fjddm=8 and qxjddm=10 and zybj='1' ---- 改为烟叶需求  需要修改
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,yylbmc as qxjdmc,1 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
        rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,yylbmc as qxjdmc,2 as 

qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
        rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
    , QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ;
  -- 分品牌 
  insert into YYZY.T_YYZY_QXJD
  with pp as (
    select distinct PPDM,PPMC
    from YYZY.T_YYZY_XQFX_YYXQ
    where YYMC is not null and yydm is not null
  ),
  pfph as(
    select distinct PPDM,PPMC,PFPHDM,PFPHMC
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yymc is not null and yydm is not null 
  ),
  jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, 
        LJSX, JBSX
        ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 11 and FJDDM = 8 and ZYBJ = '1'---- 已改为烟叶需求  需要修改
  ),
  jd_pp as
(select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,PPMC as QXJDMC,1 as QXJDLX,
     QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,rtrim(ljsx)||'&ppdm='||char(PPDM) as LJSX,JBSX,QXMKDM, 
QXJSDM,'1' as ZYBJ
  from jd_z inner join pp
  on PPMC='中华'
  union all
  select (select value(max(QXJDDM),0) 
  from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
       PPMC as QXJDMC,2 as QXJDLX,
       QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
	   rtrim(ljsx)||'&ppdm='||char(PPDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_z inner join pp
  on PPMC<>'中华'
  ),
  jd_pfph as
  (select(select value(max(QXJDDM),0) from jd_pp)+rownumber()over() as QXJDDM,PFPHMC as QXJDMC,2 as QXJDLX,
  QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
      '/tsas/treeOperationAction.do?'||
      'createOtherView_ERUPTION_BUTTON=1&'||
      'viewType=tobaccoDemandBreed'||
      '&pfphdm='||char(PFPHDM)||
      '&ppdm='||char(ppdm) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
  from jd_pp 
  inner join pfph on PPMC = QXJDMC
  ),
  result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as
  (
  select * from jd_pp
  union all
  select * from jd_pfph
  )
  select * from result
  ;
  -- 剩余烟叶统计
  insert into YYZY.T_YYZY_QXJD
  with lb as ( 
    select distinct yylbdm,yylbmc 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yylbdm is not null
    and PPDM is null 
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where gnwbj is not null
   and PPDM is null 
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yydcddm is not null
    and PPDM is null 
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,yylbdm,yylbmc 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yycddm is not null
    and PPDM is null 
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX 
    , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD
    where fjddm=11 and qxjddm=16 and zybj='1' --- 已改为烟叶需求  需要修改
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,yylbmc as qxjdmc,1 as qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
        rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,yylbmc as qxjdmc,2 as 

qxjdlx,
        qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
        rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX
    , QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ;
  
  -- 分等级 
  insert into YYZY.T_YYZY_QXJD 
  with lb as (
    select distinct YYLBDM,YYLBMC 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where YYLBDM is not null
  )
  , ddj as (
    select distinct YYLBDM,YYLBMC,YYDDJDM,YYDDJMC
    from YYZY.T_YYZY_XQFX_YYXQ 
    where YYDDJDM is not null
  )
  , dj as (
    select distinct YYDDJDM,YYDDJMC,YYDJDM,YYDJMC,YYLBDM,YYLBMC
    from YYZY.T_YYZY_XQFX_YYXQ
    where YYDJDM is not null
  )
  , jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 12 and FJDDM = 8 and ZYBJ = '1' --- 需要修改
  )
  , jd_lb as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,
      YYLBMC as QXJDMC,1 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      1 as XSSX,rtrim(ljsx)||'&yylb='||char(YYLBDM) as LJSX,JBSX,QXMKDM, 
      QXJSDM,'1' as ZYBJ 
    from jd_z 
      inner join lb on yylbmc='烤烟'
    union all
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
      YYLBMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
      rtrim(ljsx)||'&yylb='||char(YYLBDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z 
      inner join lb on yylbmc<>'烤烟'
  )
  ,
  -- select * from jd_lb
  jd_ddj as (
    select (select value(max(QXJDDM),0) from jd_lb)+rownumber()over() as QXJDDM,
      YYDDJMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      rownumber()over() as XSSX, rtrim(ljsx)||'&ddjdm='||char(YYDDJDM) as LJSX,
      JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_lb 
      inner join ddj on QXJDMC = '烤烟' 
    where QXJDMC = YYLBMC
  ) 
  , jd_dj as (
    select 
      (select value(max(QXJDDM),0) from jd_ddj)+rownumber()over() as QXJDDM,
      YYDJMC as QXJDMC,2 as QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      rownumber()over() as XSSX,rtrim(ljsx)||'&djdm='||char(YYDJDM) as LJSX,
      JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_ddj 
      inner join dj 
        on YYDDJMC = QXJDMC and YYLBMC = '烤烟'
  )
  , result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as (
    select * from jd_lb
    union all
    select * from jd_ddj
    union all
    select * from jd_dj
  )
  select * from result 
  ; 

  -- 分年份 
  insert into YYZY.T_YYZY_QXJD(qxjddm,qxjdmc,qxjdlx,fjddm,qxjdjb,xssx,ljsx,jbsx,qxmkdm,qxjsdm,zybj) 
  with sj_lb as (
    select distinct yylbdm,yylbmc 
    from YYZY.T_YYZY_KCFX_FNFCD
  )
  ,
  gjd as (
    select *
    from YYZY.T_YYZY_QXJD 
    where qxjddm=18
  )
  ,
  results as (
    select 
      rownumber()over()+value((select max(qxjddm) from YYZY.T_YYZY_QXJD),0) as QXJDDM, 
      yylbmc as QXJDMC, QXJDLX, qxjddm as FJDDM, QXJDJB+1 as qxjdjb, 
      rownumber()over(order by yylbdm) as XSSX, 
      LJSX||'&yylb='||rtrim(char(yylbdm)) as LJSX, 
      JBSX, QXMKDM, QXJSDM, ZYBJ
    from gjd 
      inner join sj_lb on 1=1 
    where 1=1 
  )
  select *
  from results 
  ;

  
  /* 2011-01-18 龚玮慧新增功能 */
  --特殊处理 删除进口烟下所有等级节点 进口烟展示内容同分产地中的国外节点
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=8 and qxjdmc='分等级'), -1 ) into v_qxjddm;
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='烤烟'), -1 ) into v_qxjddm;
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='进口烟'), -1 ) into v_qxjddm;

  delete from YYZY.T_YYZY_QXJD 
  where fjddm = v_qxjddm
  ;
  update YYZY.T_YYZY_QXJD
  set ljsx=''||
    '/tsas/treeOperationAction.do'||
    '?createOtherView_ERUPTION_BUTTON=1'||
    '&viewType=tobaccoDemandArea'||
    '&yylb=1&gnwbj=0'
  where qxjddm = v_qxjddm 
  ;
  /* 以上为2011-01-18 龚玮慧新增功能 */

  /* 2011-0-14 龚玮慧 新增 中华挑烟节点*/
  -- 烟叶需求 中华挑烟 
  -- 分产地 
  insert into YYZY.T_YYZY_QXJD 
  with 
  sj_all as (
    select * 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yydjmc like '%挑%'
  )
  ,lb as ( 
    select distinct yylbdm,yylbmc 
    from sj_all 
    where yylbdm is not null 
  ) 
  ,gnw as ( 
    select distinct gnwbj,(case gnwbj when 0 then '国外' else '国内' end) as gnwmc 
    from sj_all 
    where gnwbj is not null 
  ) 
  ,dcd as ( 
    select distinct (case gnwbj when 0 then '国外' else '国内' end) as gnwmc,YYDCDDM,YYDCDMC,YYLBDM,YYLBMC 
    from sj_all 
    where yydcddm is not null 
  ) 
  ,cd as ( 
    select distinct yydcddm,yydcdmc,yycddm,yycdmc,YYLBDM,YYLBMC 
    from sj_all 
    where yycddm is not null 
  ) 
  ,jd_z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX , QXMKDM, QXJSDM 
    from YYZY.T_YYZY_QXJD 
    where fjddm=87 and qxjddm=89 and zybj='1' ---- 改为烟叶需求  需要修改
  )
  ,jb_flb as ( 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1 as qxjddm,
      yylbmc as qxjdmc,1 as qxjdlx, qxjddm as fjddm,qxjdjb+1 as QXJDJB,1 as xssx, 
      rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc='烤烟' 
    union all 
    select (select value(max(qxjddm),0) from YYZY.T_YYZY_QXJD)+1+rownumber()over() as qxjddm,
      yylbmc as qxjdmc,2 as qxjdlx, qxjddm as fjddm,qxjdjb+1 as QXJDJB,1+rownumber()over() as xssx, 
      rtrim(ljsx)||'&yylb='||char(yylbdm) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jd_z inner join lb on 1=1 
    where yylbmc<>'烤烟' 
  ) 
  ,jb_gnw as (
    select (select value(max(qxjddm),0) from jb_flb)+1 as qxjddm,
      gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
      1 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国内' 
    union all 
    select (select value(max(qxjddm),0) from jb_flb)+2 as qxjddm,
        gnwmc as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as QXJDJB,
        2 as sxsx,rtrim(ljsx)||'&gnwbj='||char(gnwbj) as ljsx,jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_flb inner join gnw on qxjdmc='烤烟' 
    where gnwmc='国外' 
  ) 
  ,jb_dcd as (
    select (select value(max(qxjddm),0) from jb_gnw)+rownumber()over() as qxjddm,
      YYDCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&dcddm='||char(yydcddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_gnw inner join dcd on qxjdmc='国内' 
    where gnwmc=qxjdmc
    and YYLBMC = '烤烟'
  )
  ,jb_cd as (
    select (select value(max(qxjddm),0) from jb_dcd)+rownumber()over() as qxjddm,
      YYCDMC as qxjdmc,1 as qxjdlx,qxjddm as fjddm,qxjdjb+1 as qxjdjb,
      rownumber()over() as sxsx,rtrim(ljsx)||'&cddm='||char(yycddm) as ljsx,
      jbsx,qxmkdm,qxjsdm,'1' as zybj
    from jb_dcd inner join cd on yydcdmc=qxjdmc
    and YYLBMC = '烤烟' 
  )
  ,results(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
    select * from jb_flb
    union all 
    select * from jb_gnw
    union all 
    select * from jb_dcd
    union all 
    select * from jb_cd
  ) 
  select *
  from results
  order by 1
  ; 

  -- 分品牌 
  insert into YYZY.T_YYZY_QXJD
  with 
  sj_all as (
    select *
    from YYZY.T_YYZY_XQFX_YYXQ
    where yydjmc like '%挑%'
  )
  ,pp as (
    select distinct PPDM,PPMC
    from sj_all
    where YYMC is not null and yydm is not null
  )
  , pfph as(
    select distinct PPDM,PPMC,PFPHDM,PFPHMC
    from sj_all 
    where yymc is not null and yydm is not null 
  )
  , jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 90 and FJDDM = 87 and ZYBJ = '1'---- 已改为烟叶需求  需要修改
  )
  , jd_pp as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,
      PPMC as QXJDMC,1 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,
      rtrim(ljsx)||'&ppdm='||char(PPDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z inner join pp on PPMC='中华'
    union all 
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
      PPMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX, 
      rtrim(ljsx)||'&ppdm='||char(PPDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z inner join pp on PPMC<>'中华'
  )
  , jd_pfph as (
    select (select value(max(QXJDDM),0) from jd_pp)+rownumber()over() as QXJDDM,
      PFPHMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,rownumber()over() as XSSX,
      '/tsas/treeOperationAction.do?'||
      'createOtherView_ERUPTION_BUTTON=1'||
      '&viewType=tobaccoDemandBreed'||
      '&sftyflg=1'||
      '&pfphdm='||char(PFPHDM)||
      '&ppdm='||char(ppdm) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ 
    from jd_pp 
    inner join pfph on PPMC = QXJDMC 
  )
  , result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as ( 
    select * from jd_pp 
    union all 
    select * from jd_pfph 
  )
  select * from result
  ;
  
  -- 分等级 
  insert into YYZY.T_YYZY_QXJD 
  with 
  sj_all as ( 
    select * 
    from YYZY.T_YYZY_XQFX_YYXQ 
    where yydjmc like '%挑%' 
  )
  ,lb as (
    select distinct YYLBDM,YYLBMC 
    from sj_all 
    where YYLBDM is not null
  )
  , ddj as (
    select distinct YYLBDM,YYLBMC,YYDDJDM,YYDDJMC
    from sj_all 
    where YYDDJDM is not null
  )
  , dj as (
    select distinct YYDDJDM,YYDDJMC,YYDJDM,YYDJMC,YYLBDM,YYLBMC
    from sj_all 
    where YYDJDM is not null
  )
  , jd_Z as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX ,QXMKDM, QXJSDM
    from YYZY.T_YYZY_QXJD 
    where QXJDDM = 91 and FJDDM = 87 and ZYBJ = '1' --- 需要修改
  )
  , jd_lb as (
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+1 as QXJDDM,
      YYLBMC as QXJDMC,1 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      1 as XSSX,rtrim(ljsx)||'&yylb='||char(YYLBDM) as LJSX,JBSX,QXMKDM, 
      QXJSDM,'1' as ZYBJ 
    from jd_z 
      inner join lb on yylbmc='烤烟'
    union all
    select (select value(max(QXJDDM),0) from YYZY.T_YYZY_QXJD)+rownumber()over()+1 as QXJDDM,
      YYLBMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1+rownumber()over() as XSSX,
      rtrim(ljsx)||'&yylb='||char(YYLBDM) as LJSX,JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_z 
      inner join lb on yylbmc<>'烤烟'
  )
  ,
  -- select * from jd_lb
  jd_ddj as (
    select (select value(max(QXJDDM),0) from jd_lb)+rownumber()over() as QXJDDM,
      YYDDJMC as QXJDMC,2 as QXJDLX, QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      rownumber()over() as XSSX, rtrim(ljsx)||'&ddjdm='||char(YYDDJDM) as LJSX,
      JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_lb 
      inner join ddj on QXJDMC = '烤烟' 
    where QXJDMC = YYLBMC
  ) 
  , jd_dj as (
    select 
      (select value(max(QXJDDM),0) from jd_ddj)+rownumber()over() as QXJDDM,
      YYDJMC as QXJDMC,2 as QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
      rownumber()over() as XSSX,rtrim(ljsx)||'&djdm='||char(YYDJDM) as LJSX,
      JBSX,QXMKDM, QXJSDM,'1' as ZYBJ
    from jd_ddj inner join dj on YYDDJMC = QXJDMC and YYLBMC = '烤烟'
  )
  , result(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ) as (
    select * from jd_lb
    union all
    select * from jd_ddj
    union all
    select * from jd_dj
  )
  select * from result 
  ; 

  --特殊处理 删除进口烟下所有等级节点 进口烟展示内容同分产地中的国外节点
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=87 and qxjdmc='分等级'), -1 ) into v_qxjddm; 
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='烤烟'), -1 ) into v_qxjddm; 
  values value( (select qxjddm from YYZY.T_YYZY_QXJD where qxmkdm=4 and fjddm=v_qxjddm and qxjdmc='进口烟'), -1 ) into v_qxjddm; 
  
  delete from YYZY.T_YYZY_QXJD 
  where fjddm = v_qxjddm
  ;
  update YYZY.T_YYZY_QXJD
  set ljsx=''||
    '/tsas/treeOperationAction.do'||
    '?createOtherView_ERUPTION_BUTTON=1'||
    '&viewType=tobaccoDemandArea'||
    '&sftyflg=1'||
    '&yylb=1&gnwbj=0'
  where qxjddm = v_qxjddm 
  ;
  /* 以上为2011-0-14 龚玮慧 新增 中华挑烟节点*/
  
  set SM=SM||'需求分析';
/*
  --产能分析 时间维度
  insert into YYZY.T_YYZY_QXJD(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB, XSSX, LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
  with tmp as (
    select distinct pfphdm, pfphmc, ppmc, a.ppdm
    from (
      select LSBH, PZDM, PZMC, YHBS, SCCJDM, PPDM, KSRQ, JSRQ
      from DIM.T_DIM_YYZY_PZ
      where jsrq > current date
    ) as a,
    (
      select LSBH, PFPHDM, PFPHBS, PFPHMC, YHBS, SCCJDM, KSRQ, JSRQ
      from DIM. T_DIM_YYZY_PFPH
      where jsrq > current date
    ) as b,
    (
      select * 
      from DIM. T_DIM_YYZY_PP 
      where jsrq > current date
    ) as c
    where a.yhbs = b.yhbs 
      and a.sccjdm = b.sccjdm 
      and a.ppdm = c.ppdm 
      and a.ppdm <> 0 
      and ppmc not like '%飞马%'
      and pfphdm in ( 
        select PFPHDM
        from YYZY.T_YYZY_CNFX
      )
    )
    , tmp_4 as ( 
      select ROWNUMBER() OVER() as QXJDDM,ppmc as QXJDMC,
          1 as QXJDLX,QXJDDM as FJDDM,4 as QXJDJB,ppdm as XSSX,
          (
            '/tsas/treeOperationAction.do'||
            '?createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=produceAbilityTimeVariety'||
            '&ppdm='||rtrim(char(ppdm))
          ) as LJSX,
          cast(null as varchar(1)) JBSX,QXMKDM,
          ppdm + 200000 QXJSDM,'1' as ZYBJ
      from (select distinct ppdm, ppmc  from tmp) as a,
      (select * from YYZY.T_YYZY_QXJD where qxjddm=29) as b
    )
    ,tmp_5 as (
      select (SELECT VALUE(MAX(QXJDDM), 0) FROM tmp_4) + ROWNUMBER() OVER() as QXJDDM,
          pfphmc QXJDMC,1 QXJDLX,QXJDDM  FJDDM,4 QXJDJB,pfphdm as XSSX,
  LJSX||'&pfphdm='||rtrim(char(pfphdm))   LJSX,
  cast(null as varchar(1)) JBSX,
    a.QXMKDM  QXMKDM,cast(null as integer) QXJSDM,'1' ZYBJ
 from tmp_4 a,tmp b where a.qxjsdm=ppdm+200000
 )
select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ from tmp_4
 union all
select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM, QXJDMC, QXJDLX,(select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ from tmp_5;
 
*/ 
 
  -- 产能分析 产量维度
  insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
  with 
  tmp as 
  ( 
    select distinct pfphdm, pfphmc, ppmc, a.ppdm
    from 
      (
        select LSBH, PZDM, PZMC, YHBS, SCCJDM, PPDM, KSRQ, JSRQ
          from DIM.T_DIM_YYZY_PZ
         where jsrq > current date
      ) as a, 
      (
        select LSBH, PFPHDM, PFPHBS, PFPHMC, YHBS, SCCJDM, KSRQ, JSRQ
          from DIM. T_DIM_YYZY_PFPH
         where jsrq > current date
      ) as b, 
      (
        select * from DIM. T_DIM_YYZY_PP where jsrq > current date
      ) as c
    where a.yhbs = b.yhbs 
      and a.sccjdm = b.sccjdm 
      and a.ppdm = c.ppdm 
      and a.ppdm <> 0 
      and ppmc not like '%飞马%' 
      and pfphdm in (select PFPHDM from YYZY.T_YYZY_CNPFB)
      --and pfphdm in (select pfphdm from yyzy.t_yyzy_zxpf_whb)
    order by 2
  )
  , 
  tmp_4 as 
  (
    select  
      ROWNUMBER() OVER() as QXJDDM, 
      ppmc as QXJDMC,1 as QXJDLX,QXJDDM as FJDDM,
      4 as QXJDJB,ppdm as XSSX,
      (
        '/tsas/treeOperationAction.do'||
        '?createOtherView_ERUPTION_BUTTON=1'||
        '&viewType=produceAbilityVariety'||
        '&ppdm='||rtrim(char(ppdm))
      ) as LJSX,
      cast(null as varchar(1)) as JBSX,
      QXMKDM,ppdm + 200000 as QXJSDM,'1' as ZYBJ
    from 
      (select distinct ppdm, ppmc from tmp ) as a,
      (select * from YYZY.T_YYZY_QXJD where qxjddm=26 ) as b
    where 1=1
  )
  ,
  tmp_5 as 
  (
    select 
      (SELECT VALUE(MAX(QXJDDM), 0) FROM tmp_4) + ROWNUMBER() OVER() as  QXJDDM,
      pfphmc QXJDMC,1 QXJDLX,QXJDDM  FJDDM,5 QXJDJB,pfphdm XSSX,
      (
        '/tsas/treeOperationAction.do'||
        '?createOtherView_ERUPTION_BUTTON=1'||
        '&viewType=produceAbilityRation'||
        '&pfphdm='||rtrim(char(pfphdm))
      ) as LJSX, 
      cast(null as varchar(1)) JBSX,
      a.QXMKDM  QXMKDM,cast(null as integer) QXJSDM,'1' ZYBJ
    from tmp_4 a,tmp b 
    where a.qxjsdm=ppdm+200000
  )
  select 
    (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM as qxjddm, 
    QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_4
  union all
  select 
    (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM, 
    QXJDMC, QXJDLX,
    (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ FJDDM, 
    QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_5
  ;
  
  set SM=SM||'产能分析';
  
  
  --库存分析 A
  insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
  with 
  tmp_fl as (
    select  b.yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yycddm,dcddm 
    from yyzy.t_yyzy_yyzdbmx a,
    yyzy.t_yyzy_yykc_new b 
    where a.yydm=b.yydm 
      and yycdmc<>'进口' 
      and yylbmc='烤烟' 
  )
  ,tmp1 as (
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
    from YYZY.T_YYZY_QXJD 
    where qxjddm=33
  )
  , tmp_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM YYZY.T_YYZY_QXJD) + ROWNUMBER() OVER() as qxjddm, 
        '产地' as QXJDMC, QXJDLX, QXJDDM FJDDM, QXJDJB+1 QXJDJB,1 as XSSX,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=roastTobaccoByArea'||
          '&yylb='||v_lbdm_ky
        ) as LJSX,
        JBSX, QXMKDM, QXJSDM, ZYBJ from tmp1
  )
  ,tmp_gn (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+1 as qxjddm, '国内' as QXJDMC, QXJDLX,QXJDDM FJDDM,QXJDJB+1 as QXJDJB,
        1 as XSSX,LJSX||'&dcdflg=1' as LJSX,JBSX, QXMKDM, QXJSDM  , ZYBJ 
    from tmp_cd
  )
  , tmp_gw ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+2 as qxjddm, '国外' as QXJDMC, QXJDLX,QXJDDM FJDDM,
        QXJDJB+1 as QXJDJB,1 as XSSX,LJSX||'&dcdflg=0' as LJSX,
        JBSX, QXMKDM, QXJSDM  , ZYBJ 
    from tmp_cd
  )
  , tmp_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+2 as qxjddm, '云南' as QXJDMC, QXJDLX,QXJDDM FJDDM,
        QXJDJB+1 as QXJDJB,1 as XSSX,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=roastTobaccoByArea'||
          '&yylb='||v_lbdm_ky||
          '&dcddm='||v_dcddm_yn||
          '&cdflg=1'
        ) as LJSX,
        JBSX, QXMKDM,100053 as QXJSDM, ZYBJ 
    from tmp_gn
  )
  , tmp_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT QXJDDM FROM tmp_yn) + ROWNUMBER() OVER() +1 as qxjddm, 
        '等级' as QXJDMC, QXJDLX,QXJDDM FJDDM,QXJDJB+1 as QXJDJB,2 as XSSX,
        (
          '/tsas/treeOperationAction.do?'||
          'createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=roastTobaccoByGrade'||
          '&yylb='||v_lbdm_ky
        ) as LJSX,
        JBSX, QXMKDM, QXJSDM, ZYBJ 
    from tmp1
  )
  , tmp_sdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+1 as qxjddm, '上等烟' as QXJDMC, QXJDLX,QXJDDM as FJDDM,
        QXJDJB+1 as QXJDJB,1 as XSSX,
        LJSX||'&ddjdm='||v_ddj_sd as LJSX,
        JBSX, QXMKDM, QXJSDM  , ZYBJ 
    from tmp_dj
  )
  , tmp_zdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+2 as qxjddm, '中等烟' as QXJDMC, QXJDLX,QXJDDM as FJDDM,
        QXJDJB+1 as QXJDJB,2 as XSSX,LJSX||'&ddjdm='||v_ddj_zd as LJSX,
        JBSX, QXMKDM, QXJSDM  , ZYBJ 
    from tmp_dj
  )
  , tmp_xdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select QXJDDM+3 as qxjddm, '下等烟' as QXJDMC, QXJDLX,QXJDDM as FJDDM,
        QXJDJB+1 as QXJDJB,3 as XSSX,LJSX||'&ddjdm='||v_ddj_xd as LJSX,
        JBSX, QXMKDM, QXJSDM  , ZYBJ 
    from tmp_dj
  )
  , tmp_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT QXJDDM FROM tmp_xdj) + ROWNUMBER() OVER() as QXJDDM, 
        yycdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,
        QXJDJB+1 as QXJDJB,yycddm as sxsx,
        LJSX||'&cddm='||rtrim(char(yycddm)) as LJSX,
        jbsx,qxmkdm,cast(null as integer) as QXJSDM,'1' ZYBJ
    from tmp_yn a,
    (
      select distinct yycdmc,yycddm 
      from tmp_fl 
      where dcdmc='云南' 
      and yycdmc<>'云南' 
    ) as b
  )
  , tmp_yn_xcd_1 as (
    select * from tmp_cd 
    union all 
    select * from tmp_gn 
    union all 
    select * from tmp_gw 
    union all 
    select * from tmp_yn  
    union all 
    select * from tmp_dj  
    union all 
    select * from tmp_sdj  
    union all 
    select * from tmp_zdj  
    union all 
    select * from tmp_xdj 
    union all 
    select * from tmp_yn_xcd
  )
  , tmp_gn_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_yn_xcd_1) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        replace(LJSX,'&dcdflg=1','')||'&cdflg=0&dcddm='||rtrim(char(dcddm)) as LJSX,
        jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
    from tmp_gn a,
    (
      select distinct dcdmc,dcddm 
      from tmp_fl 
      where dcdmc<>'云南' 
      and gnw='国内'
    ) as b
  )
  , tmp_gn_cd_1 as (
    select * from tmp_gn_cd 
    union all 
    select * from tmp_yn_xcd_1
  )
  , tmp_gw_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_gn_cd_1) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,
        jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
    from tmp_gw a,
    (
      select distinct dcdmc,dcddm 
      from tmp_fl 
      where  gnw='国外'
    ) as b
  )
  , tmp_gw_cd_1 as (
    select * 
    from tmp_gn_cd_1 
    union all 
    select * 
    from tmp_gw_cd
  )
  , tmp_sdy_dj  ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_gw_cd_1) + ROWNUMBER() OVER() as QXJDDM, 
        yydjmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
        LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
    from tmp_sdj a,
    (
      select distinct yydjmc,yydjdm 
      from tmp_fl 
      where yyddjmc='上等烟' 
    ) as b
  )
  ,tmp_sdy_dj_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_sdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
    from tmp_sdy_dj a,
    (
      select distinct yydjmc,dcdmc,dcddm 
      from tmp_fl 
      where yyddjmc='上等烟' 
    ) b
    where a.qxjdmc=b.yydjmc
  )
  , tmp_sdy_dj_1 as (
    select * from tmp_gw_cd_1 
    union all 
    select * from tmp_sdy_dj 
    union all 
    select * from tmp_sdy_dj_cd
  )
  , tmp_zdy_dj  ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM,QXJSDM,ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_sdy_dj_1) + ROWNUMBER() OVER() as QXJDDM, 
      yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
      LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,
      jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
    from tmp_zdj a,
    (
      select distinct yydjmc,yydjdm 
      from tmp_fl 
      where yyddjmc='中等烟' 
    ) b
  )
  ,tmp_zdy_dj_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_zdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,
        jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
    from tmp_zdy_dj a,
    (
      select distinct yydjmc,dcdmc,dcddm 
      from tmp_fl 
      where yyddjmc='中等烟' 
    ) as b
    where a.qxjdmc=b.yydjmc
  )
  , tmp_zdy_dj_1 as (
    select * from tmp_sdy_dj_1 
    union all 
    select * from tmp_zdy_dj 
    union all 
    select * from tmp_zdy_dj_cd
  ) 
  , tmp_xdy_dj  ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_zdy_dj_1) + ROWNUMBER() OVER() as QXJDDM, 
        yydjmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
        LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
    from tmp_xdj a,
    (
      select distinct yydjmc,yydjdm 
      from tmp_fl 
      where yyddjmc='下等烟' 
    ) as b
  ) 
  ,tmp_xdy_dj_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
    select (SELECT max(QXJDDM) FROM tmp_xdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,
        jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
    from tmp_xdy_dj a,
    (
      select distinct yydjmc,dcdmc,dcddm 
      from tmp_fl 
      where yyddjmc='下等烟' 
    ) as b
    where a.qxjdmc=b.yydjmc
  )
  , tmp_xdy_dj_1 as (
    select * from tmp_zdy_dj_1 
    union all 
    select * from tmp_xdy_dj 
    union all 
    select * from tmp_xdy_dj_cd
  )
  select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ   
  from tmp_xdy_dj_1;
  
  
  OPEN c1; --循环遍历库存表中的烟叶年份
  loop_ml:loop 
    set i_not_found=0;
    FETCH c1 INTO i_yynf; 
    if i_not_found=1 then 
      leave loop_ml; 
    end if; 
    
    insert into YYZY.T_YYZY_QXJD(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
    select (SELECT max(QXJDDM) FROM YYZY.T_YYZY_QXJD) +1 as QXJDDM,
        rtrim(char(i_yynf)) as QXJDMC, QXJDLX,qxjddm as FJDDM,
        QXJDJB+1 as QXJDJB,i_yynf as XSSX,cast(null as varchar(1)) as ljsx,
        JBSX, QXMKDM, QXJSDM, ZYBJ 
    from YYZY.T_YYZY_QXJD 
    where qxjddm=34;
    
    insert into YYZY.T_YYZY_QXJD(QXJDDM,QXJDMC,QXJDLX,FJDDM,QXJDJB,XSSX,LJSX,JBSX,QXMKDM,QXJSDM,ZYBJ)
    with tmp_fl as (
      select  b.yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yycddm,dcddm 
      from yyzy.t_yyzy_yyzdbmx as a,
      YYZY.T_YYZY_KCFXYYSWKC as b 
      where a.yydm=b.yydm 
        and yycdmc<>'进口'  
        and yylbmc='烤烟' 
        and b.yynf=i_yynf
    )
    ,tmp1 as (
      select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
      from YYZY.T_YYZY_QXJD 
      where qxjddm=(SELECT max(QXJDDM) FROM YYZY.T_YYZY_QXJD )
    )
    , tmp_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
      select (SELECT max(QXJDDM) FROM YYZY.T_YYZY_QXJD) + ROWNUMBER() OVER() as qxjddm , 
          '产地' as QXJDMC, QXJDLX, QXJDDM as FJDDM, QXJDJB+1 as QXJDJB,1 as XSSX,
          (
            '/tsas/treeOperationAction.do'||
            '?createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=roastTobaccoByArea'||
            '&yylb='||v_lbdm_ky||
            '&yynf='||rtrim(char(i_yynf))
          ) as LJSX,
          JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp1
    )
    , tmp_gn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
      select QXJDDM+1 as qxjddm, '国内' as QXJDMC, QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
          1 as XSSX,LJSX||'&dcdflg=1' as LJSX,JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp_cd
    )
    , tmp_gW ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select QXJDDM+2 as qxjddm, '国外' as QXJDMC, QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,
          1 as XSSX,LJSX||'&dcdflg=0' as LJSX,JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp_cd
    )
    , tmp_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select QXJDDM+2 as qxjddm, '云南' as QXJDMC, QXJDLX,
          QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,1 as XSSX,
          (
            '/tsas/treeOperationAction.do?'||
            'createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=roastTobaccoByArea'||
            '&yylb=1&dcddm='||v_dcddm_yn||
            '&cdflg=1'||
            '&yynf='||rtrim(char(i_yynf))
          ) as LJSX,
          JBSX, QXMKDM,100053 as QXJSDM, ZYBJ 
      from tmp_gn
    )
    , tmp_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT QXJDDM FROM tmp_yn) + ROWNUMBER() OVER() +1 as qxjddm, 
          '等级' as QXJDMC, QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,2 as XSSX,
          (
            '/tsas/treeOperationAction.do'||
            '?createOtherView_ERUPTION_BUTTON=1'||
            '&viewType=roastTobaccoByGrade'||
            '&yylb='||v_lbdm_ky||
            '&yynf='||rtrim(char(i_yynf)) 
          ) as LJSX,
          JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp1
    )
    , tmp_sdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select QXJDDM+1 as qxjddm, '上等烟' as QXJDMC, QXJDLX,QXJDDM as FJDDM,
          QXJDJB+1 as QXJDJB,1 XSSX,LJSX||'&ddjdm='||v_ddj_sd as LJSX,
          JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp_dj
    )
    , tmp_zdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
      select QXJDDM+2 as qxjddm, '中等烟' as QXJDMC, QXJDLX,QXJDDM as FJDDM,
          QXJDJB+1 as QXJDJB,2 as XSSX,LJSX||'&ddjdm='||v_ddj_zd as LJSX,
          JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp_dj
    ) 
    , tmp_xdj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select QXJDDM+3 as qxjddm, '下等烟' as QXJDMC, QXJDLX,
          QXJDDM FJDDM,QXJDJB+1 as QXJDJB,3 as XSSX,LJSX||'&ddjdm='||v_ddj_xd as LJSX,
          JBSX, QXMKDM, QXJSDM  , ZYBJ 
      from tmp_dj
    )
    , tmp_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
      select (SELECT QXJDDM FROM tmp_xdj) + ROWNUMBER() OVER() as QXJDDM, 
          yycdmc as QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1 as qxjdjb,
          yycddm as sxsx,LJSX||'&cddm='||rtrim(char(yycddm)) as LJSX,
          jbsx,qxmkdm,cast(null as integer) as QXJSDM,'1' as ZYBJ
      from tmp_yn a,
      (
        select distinct yycdmc,yycddm 
        from tmp_fl 
        where dcdmc='云南' 
        and yycdmc<>'云南' 
      ) b
    )
    , tmp_yn_xcd_1 as(
      select * from tmp_cd 
      union all 
      select * from tmp_gn 
      union all 
      select * from tmp_gw 
      union all 
      select * from tmp_yn  
      union all 
      select * from tmp_dj  
      union all 
      select * from tmp_sdj  
      union all 
      select * from tmp_zdj  
      union all 
      select * from tmp_xdj 
      union all 
      select * from tmp_yn_xcd
    )
    , tmp_gn_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_yn_xcd_1) + ROWNUMBER() OVER() as QXJDDM, 
          dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
          replace(LJSX,'&dcdflg=1','')||'&cdflg=0&dcddm='||rtrim(char(dcddm)) as LJSX,
          jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
      from tmp_gn a,
      (
        select distinct dcdmc,dcddm 
        from tmp_fl 
        where dcdmc<>'云南' 
        and gnw='国内' 
      ) as b 
    )
    , tmp_gn_cd_1 as (
      select * from tmp_gn_cd 
      union all 
      select * from tmp_yn_xcd_1
    )
    , tmp_gw_cd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_gn_cd_1) + ROWNUMBER() OVER() as QXJDDM, 
          dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
          LJSX||'&dcddm='||rtrim(char(dcddm)) LJSX,
          jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
      from tmp_gw as a,
      (
        select distinct dcdmc,dcddm 
        from tmp_fl 
        where  gnw='国外'  
      ) as b
    )
    , tmp_gw_cd_1 as (
      select * from tmp_gn_cd_1 
      union all 
      select * from tmp_gw_cd
    )
    , tmp_sdy_dj(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_gw_cd_1) + ROWNUMBER() OVER() as QXJDDM, 
          yydjmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
          LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,
          jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
      from tmp_sdj as a,
      (
        select distinct yydjmc,yydjdm 
        from tmp_fl 
        where yyddjmc='上等烟' 
      ) as b
    )
    ,tmp_sdy_dj_cd(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_sdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
          dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
          LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,jbsx,qxmkdm,
          dcddm+100000 as QXJSDM,'1' as ZYBJ
      from tmp_sdy_dj a,
      (
        select distinct yydjmc,dcdmc,dcddm 
        from tmp_fl 
        where yyddjmc='上等烟' 
      ) as b
      where a.qxjdmc=b.yydjmc
    )
    , tmp_sdy_dj_1 as (
      select * from tmp_gw_cd_1 
      union all 
      select * from tmp_sdy_dj 
      union all 
      select * from tmp_sdy_dj_cd
    )
    , tmp_zdy_dj(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_sdy_dj_1) + ROWNUMBER() OVER() as QXJDDM, 
          yydjmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
          LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,
          jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
      from tmp_zdj a,
      (
        select distinct yydjmc,yydjdm 
        from tmp_fl 
        where yyddjmc='中等烟' 
      ) as b
    )
    ,tmp_zdy_dj_cd (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_zdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
        dcdmc as QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1 as qxjdjb,dcddm as sxsx,
        LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,
        jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
      from tmp_zdy_dj a,
      (
        select distinct yydjmc,dcdmc,dcddm 
        from tmp_fl 
        where yyddjmc='中等烟' 
      ) as b
      where a.qxjdmc=b.yydjmc
    )
    , tmp_zdy_dj_1 as (
      select * from tmp_sdy_dj_1 
      union all 
      select * from tmp_zdy_dj 
      union all 
      select * from tmp_zdy_dj_cd
    )
    , tmp_xdy_dj(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_zdy_dj_1) + ROWNUMBER() OVER() as QXJDDM, 
          yydjmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjdjb,yydjdm as sxsx,
          LJSX||'&djdm='||rtrim(char(yydjdm)) as LJSX,
          jbsx,qxmkdm, QXJSDM,'1' as ZYBJ
      from tmp_xdj a,
      (
        select distinct yydjmc,yydjdm 
        from tmp_fl 
        where yyddjmc='下等烟' 
      ) as b
    )
    ,tmp_xdy_dj_cd(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM,ZYBJ) as (
      select (SELECT max(QXJDDM) FROM tmp_xdy_dj) + ROWNUMBER() OVER() as QXJDDM, 
          dcdmc as QXJDMC,QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as qxjddm,dcddm as sxsx,
          LJSX||'&dcddm='||rtrim(char(dcddm)) as LJSX,
          jbsx,qxmkdm,dcddm+100000 as QXJSDM,'1' as ZYBJ
      from tmp_xdy_dj a,
      (
        select distinct yydjmc,dcdmc,dcddm 
        from tmp_fl 
        where yyddjmc='下等烟' 
      ) as b
      where a.qxjdmc=b.yydjmc
    )
    , tmp_xdy_dj_1 as (
      select * from tmp_zdy_dj_1 
      union all 
      select * from tmp_xdy_dj 
      union all 
      select * from tmp_xdy_dj_cd
    )
    select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
    from tmp_xdy_dj_1
    ;
    
  end loop loop_ml;
  close c1;
  
  --剩余库存
  insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
  with tmp_fl as (
    select  b.yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yylbdm,yycddm,dcddm 
    from yyzy.t_yyzy_yyzdbmx as a,
    yyzy.t_yyzy_yykc_new as b 
    where a.yydm=b.yydm 
  )
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ rownumber() over() as QXJDDM,
      yylbmc QXJDMC, QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,yylbdm as XSSX, 
      LJSX||'&yylb='||rtrim(char(yylbdm)) as LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ  
  from YYZY.T_YYZY_QXJD a,
  (
    select distinct yylbdm,yylbmc 
    from tmp_fl
  ) as b 
  where qxjddm=40 ;
  
  --库存分析  品牌
  insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
  with tmp as (
  select distinct pfphdm, pfphmc, ppmc, a.ppdm
  from (
    select LSBH, PZDM, PZMC, YHBS, SCCJDM, PPDM, KSRQ, JSRQ
    from DIM.T_DIM_YYZY_PZ
    where jsrq > current date
  ) as a,
  (
    select LSBH, PFPHDM, PFPHBS, PFPHMC, YHBS, SCCJDM, KSRQ, JSRQ
    from DIM. T_DIM_YYZY_PFPH
    where jsrq > current date
  ) as b,
  (
    select * 
    from DIM. T_DIM_YYZY_PP 
    where jsrq > current date
  ) as c
  where a.yhbs = b.yhbs
    and a.sccjdm = b.sccjdm
    and a.ppdm = c.ppdm
    and a.ppdm <> 0
    and ppmc not like '%飞马%'
    and pfphdm in (
      select PFPHDM
      from YYZY.T_YYZY_DPSX
    )
  )
  , tmp_4 as (
    select  ROWNUMBER()OVER() as QXJDDM,
        ppmc as QXJDMC,1 as QXJDLX,QXJDDM as FJDDM,
        QXJDJB+1 as QXJDJB,ppdm as XSSX,
        LJSX||'&ppdm='||rtrim(char(ppdm)) as LJSX,
        cast(null as varchar(1)) as JBSX,
        QXMKDM,ppdm + 200000 as QXJSDM,'1' ZYBJ
    from (
      select distinct ppdm, ppmc 
      from tmp
    ) as a,
    (
      select * 
      from YYZY.T_YYZY_QXJD 
      where qxjddm=39
    ) as b
  )
  ,tmp_5 as (
    select (SELECT VALUE(MAX(QXJDDM),0) FROM tmp_4)+ROWNUMBER() OVER() as QXJDDM,
        pfphmc as QXJDMC,1 as QXJDLX,QXJDDM as FJDDM,QXJDJB+1 as QXJDJB,pfphdm as XSSX,
        (
          '/tsas/treeOperationAction.do'||
          '?createOtherView_ERUPTION_BUTTON=1'||
          '&viewType=produceAbilityRation'||
          '&pfphdm='||rtrim(char(pfphdm)) 
        ) as LJSX,
        cast(null as varchar(1)) as JBSX,a.QXMKDM as QXMKDM,
        cast(null as integer) as QXJSDM,'1' as ZYBJ
    from tmp_4 a,
    tmp b 
    where a.qxjsdm=ppdm+200000
  )
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM as qxjddm, 
      QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_4
  union all
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ QXJDDM as qxjddm, 
      QXJDMC, QXJDLX,(select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ FJDDM as fjddm, 
      QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ 
  from tmp_5
  ;
  
/*
--
  --库存分析 新烟进库 产地
insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_fl as (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1
)
,tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
 
,tmpckby as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=44)
,tmpckby_yn as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=45)
,tmpjkby as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=46)
,tmpjkby_yn as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=47)
 
, tmp_ckby_gn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM YYZY.T_YYZY_QXJD) + ROWNUMBER() OVER()  QXJDDM, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby a,(select distinct dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内' and yykbdm in (2,22,23,24)) b
)
, tmp_ckby_gn_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_gn) + ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmp_ckby_gn a,(select distinct yydjdm,yydjmc,dcdmc from tmp_fl where dcdmc<>'云南' and gnw='国内' and yykbdm in (2,22,23,24)) b
   where a.qxjdmc=b.dcdmc
)
, tmp_ckby_gn_dj_1 as (select * from tmp_ckby_gn union all select * from tmp_ckby_gn_dj)
, tmp_ckby_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_gn_dj_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,LJSX||'&cddm='||rtrim(char(yycddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_yn a,(select distinct yycdmc,yycddm from tmp_fl where dcdmc='云南' and yycdmc<>'云南'  and yykbdm in (2,22,23,24)) b
)
, tmp_ckby_yn_xcd_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_yn_xcd) + ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmp_ckby_yn_xcd a,(select distinct yydjdm,yydjmc,yycdmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南' and yykbdm in (2,22,23,24)) b
   where a.qxjdmc=b.yycdmc
)
,tmp_ckby_yn_xcd_dj_1 as (select * from tmp_ckby_gn_dj_1 union all select * from tmp_ckby_yn_xcd union all select * from tmp_ckby_yn_xcd_dj)
 
, tmp_jkby_gn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_yn_xcd_dj_1) + ROWNUMBER() OVER()  QXJDDM, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpjkby a,(select distinct dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内' and yykbdm in (3,4,5,6)) b
)
, tmp_jkby_gn_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_gn) + ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmp_jkby_gn a,(select yydjdm,yydjmc,dcdmc from tmp_fl where dcdmc<>'云南' and gnw='国内' and yykbdm in (3,4,5,6)) b
   where a.qxjdmc=b.dcdmc
)
, tmp_jkby_gn_dj_1 as (select * from tmp_ckby_yn_xcd_dj_1 union all select * from tmp_jkby_gn union all select * from tmp_jkby_gn_dj)
, tmp_jkby_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_gn_dj_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,LJSX||'&cddm='||rtrim(char(yycddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpjkby_yn a,(select distinct yycdmc,yycddm from tmp_fl where dcdmc='云南' and yycdmc<>'云南'  and yykbdm in (3,4,5,6)) b
)
, tmp_jkby_yn_xcd_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (SELECT max(QXJDDM) FROM tmp_ckby_yn_xcd) + ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmp_jkby_yn_xcd a,(select yydjdm,yydjmc,yycdmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南' and yykbdm in (3,4,5,6)) b
   where a.qxjdmc=b.yycdmc
)
,tmp_jkby_yn_xcd_dj_1 as (select * from tmp_jkby_gn_dj_1 union all select * from tmp_jkby_yn_xcd union all select * from tmp_jkby_yn_xcd_dj)
 
select * from tmp_jkby_yn_xcd_dj_1
;
 
--库存分析 新烟进库 等级
--库存分析 新烟进库 等级     52 非重点   '初烤把烟' 上等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='上等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=52)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
--库存分析 新烟进库 等级     51 重点   '初烤把烟' 上等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='上等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=51)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 

--库存分析 新烟进库 等级     54 重点   '初烤把烟' 中等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='中等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=54)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 

--库存分析 新烟进库 等级     55 非重点   '初烤把烟' 中等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='中等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=55)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
 
 
--库存分析 新烟进库 等级     57 重点   '初烤把烟' 下等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='下等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=57)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
 
 
--库存分析 新烟进库 等级     58 非重点   '初烤把烟' 下等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (2,22,23,24)  and yyddjmc='下等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=58)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
--库存分析 新烟进库 等级     62 非重点   '机烤把烟' 上等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='上等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=62)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
--库存分析 新烟进库 等级     61 重点   '机烤把烟' 上等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='上等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=61)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 

--库存分析 新烟进库 等级     64 重点   '机烤把烟' 中等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='中等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=64)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 

--库存分析 新烟进库 等级     65 非重点   '机烤把烟' 中等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='中等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=65)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
 
 
--库存分析 新烟进库 等级     67 重点   '机烤把烟' 下等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='下等烟' ) dd where sfzd=1
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=67)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
 
 
--库存分析 新烟进库 等级     68 非重点   '机烤把烟' 下等烟
 insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_zddj as (select YYDJDM from YYZY.T_YYZY_YYZDDJ where bbh= (select max(bbh) from YYZY.T_YYZY_YYZDDJ))
,
tmp_fl as (
select * from (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm,value((select 1 from tmp_zddj where yydjdm=a.yydjdm),0) sfzd from yyzy.t_yyzy_yyzdbmx a,YYZY.T_YYZY_KCFXNYYRK b where a.yydm=b.yydm
and yycdmc<>'进口'  and yylbmc='烤烟' and yynf>=year(current date) -1 and yykbdm in (3,4,5,6)  and yyddjmc='下等烟' ) dd where sfzd=0
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, REPLACE(LJSX,'newlyTobaccoByGrade','newlyTobaccoByGradeRamus') LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=68)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl where  gnw='国内' ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南' and gnw='国内'  ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南' and gnw='国内'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
 
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 

--新烟进库 白肋烟
insert into YYZY.T_YYZY_QXJD (QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ )
with
tmp_fl as (
select  yynf,a.yydm,yydjmc,yyddjmc,yylbmc,gnw,dcdmc,yycdmc ,yydjdm,yykbmc,yykbdm,
yycddm,dcddm from yyzy.t_yyzy_yyzdbmx a,YYZY.t_YYZY_KCFXNYYRK b
where a.yydm=b.yydm and yylbmc='白肋烟'
)
,tmpckby_sdy_fzddj as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM
  , ZYBJ from YYZY.T_YYZY_QXJD where qxjddm=73)
,tmpckby_sdy_fzddj_dj ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER()  QXJDDM, yydjmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yydjdm sxsx,LJSX||'&djdm='||rtrim(char(yydjdm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj a,(select distinct yydjdm,yydjmc from tmp_fl  ) b
)
,tmpckby_sdy_fzddj_dj_fyn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=0&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc<>'云南'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_fyn_1 as (select * from tmpckby_sdy_fzddj_dj union all select * from tmpckby_sdy_fzddj_dj_fyn)
,tmpckby_sdy_fzddj_dj_yn ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ,bj) as (
  select  (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (select count(1) from tmpckby_sdy_fzddj_dj_fyn_1)+ROWNUMBER() OVER()  dcdmc, dcdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,dcddm sxsx,LJSX||'&cdflg=1&dcddm='||rtrim(char(dcddm)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ,qxjdmc bj
         from tmpckby_sdy_fzddj_dj a,(select distinct yydjmc,dcdmc,dcddm from tmp_fl where dcdmc='云南'   ) b
   where a.qxjdmc=b.yydjmc
)
,tmpckby_sdy_fzddj_dj_yn_1 as (select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_fyn_1 union all select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ from tmpckby_sdy_fzddj_dj_yn)
,tmpckby_sdy_fzddj_dj_yn_xcd ( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ) as (
  select (select max(QXJDDM) from YYZY.T_YYZY_QXJD)+ (SELECT count(1) FROM tmpckby_sdy_fzddj_dj_yn_1) + ROWNUMBER() OVER()  QXJDDM, yycdmc QXJDMC,QXJDLX,QXJDDM FJDDM,QXJDJB+1,yycddm sxsx,cast(null as varchar(1)) LJSX,
         jbsx,qxmkdm,cast(null as integer) QXJSDM,'1' ZYBJ
         from tmpckby_sdy_fzddj_dj_yn a,(select distinct yycdmc,yycddm,yydjmc from tmp_fl where dcdmc='云南' and yycdmc<>'云南'   ) b
   where a.bj=b.yydjmc
)
 
,tmpckby_sdy_fzddj_dj_yn_xcd_1 as (select * from tmpckby_sdy_fzddj_dj_yn_xcd union all select * from tmpckby_sdy_fzddj_dj_yn_1)
select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM  , ZYBJ
from tmpckby_sdy_fzddj_dj_yn_xcd_1
;
 
     set SM=SM||'库存  ';

*/

--烟气控制图
insert into YYZY.T_YYZY_QXJD(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX, QXMKDM, QXJSDM, ZYBJ)
with 
pp as (
  select distinct ppdm,ppmc
  from YYZY.T_YYZY_YQKZT
  where ppdm is not null
)
,pfph as (
  select distinct ppdm,ppmc,pfphdm,pfphmc,cjdm,cjmc
  from YYZY.T_YYZY_YQKZT
  where pfphdm is not null
)
,fjd as (
  select QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, JBSX , QXMKDM, QXJSDM, ZYBJ
  from YYZY.T_YYZY_QXJD
  where qxmkdm=6 and fjddm=0 and qxjdmc='烟气控制图'
)
,jd_pp as (
  select (select max(qxjddm) from YYZY.T_YYZY_QXJD)+rownumber()over() as qxjddm,
      ppmc as qxjdmc, 2 as QXJDLX, qxjddm as fjddm, qxjdjb+1 as qxjdjb, 
      rownumber()over(order by ppdm) as xssx,ljsx,JBSX,qxmkdm,qxjsdm,zybj
  from fjd
  inner join pp on 1=1
)
,jd_pfph as (
  select (select max(qxjddm) from jd_pp)+rownumber()over() as qxjddm,pfphmc||rtrim(char(cjmc)) as qxjdmc,1 as qxjdlx,
      qxjddm as fjddm, qxjdjb+1 as qxjdjb,rownumber()over(partition by ppdm order by pfphdm,cjdm) as xssx,
      '/tsas/treeOperationAction.do?'||
      'createOtherView_ERUPTION_BUTTON=1'||
      '&viewType=smokeControl'||
      '&pfphdm='||rtrim(char(pfphdm))||
      '&cjdm='||rtrim(char(cjdm)) as ljsx, JBSX, qxmkdm, qxjsdm, zybj
  from jd_pp as a
  inner join pfph as b
    on a.qxjdmc=b.ppmc
)
,results as (
  select * from jd_pp
  union all 
  select * from jd_pfph
)
select *
from results
;

  set SM=SM||'烟气控制图';



  update YYZY.T_YYZY_QXJD  set xssx=xssx-6000 where qxjdmc='熊猫';
  update YYZY.T_YYZY_QXJD  set xssx=xssx-5000 where qxjdmc='中华';
  update YYZY.T_YYZY_QXJD  set xssx=xssx-4000 where qxjdmc='上海';
  update YYZY.T_YYZY_QXJD  set xssx=xssx-3000 where qxjdmc='红双喜';
  update YYZY.T_YYZY_QXJD  set xssx=xssx-2000 where qxjdmc='牡丹';
  update YYZY.T_YYZY_QXJD  set xssx=xssx-1000 where qxjdmc='大前门';
  update   YYZY.T_YYZY_QXJD set QXJDLX=1 where ljsx is not null;
  update   YYZY.T_YYZY_QXJD set QXJDLX=2 where ljsx is  null;
  
  
  
   
/* 原烟分析   */
IF NOT EXISTS(SELECT * FROM YYZY.T_YYZY_QXJD WHERE QXJDMC='原烟分析' AND ZYBJ='1' AND QXMKDM=7) THEN
   	 INSERT INTO YYZY.T_YYZY_QXJD(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX,QXMKDM, ZYBJ)
        VALUES(23,'原烟分析',2,0,1,1,7,'1')  ;
END IF;

    --流向
  INSERT INTO YYZY.T_YYZY_QXJD(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, QXMKDM, ZYBJ)
   WITH YYLX AS (
   SELECT DISTINCT VALUE(A.YYLXDM,10000) AS YYLXDM,VALUE(B.YYLXMC,'未知流向') AS YYLXMC
     FROM YYZY.T_YYZY_ZXCPFZB AS A
		 LEFT JOIN DIM.T_DIM_YYZY_YYLX AS B
		 	  ON A.YYLXDM=B.YYLXDM
    WHERE A.ZYBJ='1'AND A.PFNF =(SELECT MAX(PFNF) FROM YYZY.T_YYZY_ZXCPFZB WHERE ZYBJ='1')
	   	  AND (A.PYDM, A.BBH)IN(
           	  SELECT PYDM, MAX(BBH) FROM YYZY.T_YYZY_ZXCPFZB   GROUP BY PYDM)
		  AND A.ZXCPFZBDM IN(SELECT ZXCPFZBDM FROM YYZY.T_YYZY_ZXCPFCB WHERE YYHYL>0)
  )
SELECT (SELECT MAX(QXJDDM) FROM YYZY.T_YYZY_QXJD)+ROWNUMBER()OVER() AS QXJDDM,B.YYLXMC AS QXJDMC,
   	   A.QXJDLX,A.QXJDDM AS FJDDM,A.QXJDJB+1 AS QXJDJB,B.YYLXDM AS XSSX,
	   '/tsas/treeOperationAction.do?createOtherView_ERUPTION_BUTTON=1&viewType=tobaccoFlow'
	   ||'&yylxdm='||RTRIM(CHAR(B.YYLXDM)) AS LJSX,
	   A.QXMKDM,A.ZYBJ
	FROM YYZY.T_YYZY_QXJD AS A
		 LEFT JOIN YYLX AS B ON 1=1
	WHERE QXJDMC='原烟分析'  ;
-- 流向对应烟叶
   INSERT INTO YYZY.T_YYZY_QXJD( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, QXMKDM, ZYBJ)
	WITH ZXCPF AS (
  	 SELECT A.PYDM,RTRIM(B.YYCDMC)||RTRIM(C.YYDJMC) AS PYMC, VALUE(A.YYLXDM,10000) AS YYLXDM
        FROM YYZY.T_YYZY_ZXCPFZB AS A
         LEFT JOIN DIM.T_DIM_YYZY_YYZDB AS D  ON A.PYDM=D.YYDM
		 LEFT JOIN DIM.T_DIM_YYZY_YYCD AS B ON D.YYCDDM=B.YYCDDM AND B.JSRQ>CURRENT DATE
		 LEFT JOIN DIM.T_DIM_YYZY_YYDJ AS C ON D.YYLBDM=C.YYLBDM AND D.YYDJDM=C.YYDJDM AND C.JSRQ>CURRENT DATE
       WHERE A.ZYBJ='1'AND A.PFNF=(SELECT MAX(PFNF) FROM YYZY.T_YYZY_ZXCPFZB WHERE ZYBJ='1')
          AND (A.PYDM, A.PFNF, A.BBH)IN(
              SELECT PYDM, PFNF, MAX(BBH)
              FROM YYZY.T_YYZY_ZXCPFZB
              GROUP BY PYDM, PFNF)
		  AND A.ZXCPFZBDM IN(SELECT ZXCPFZBDM FROM YYZY.T_YYZY_ZXCPFCB WHERE YYHYL>0)
    )
     SELECT ( SELECT MAX(QXJDDM) FROM YYZY.T_YYZY_QXJD)+ ROWNUMBER() OVER() AS QXJDDM,A.PYMC AS QXJDMC,1 AS QXJDLX,
  		    B.QXJDDM AS FJDDM,B.QXJDJB+1 AS QXJDJB, ROWNUMBER() OVER() AS XSSX,
		    '/tsas/treeOperationAction.do?createOtherView_ERUPTION_BUTTON=1&viewType=rawtobaccoAbility'
		    ||'&pydm='||RTRIM(CHAR(A.PYDM)) AS LJSX,B.QXMKDM,'1' AS ZYBJ
  	   FROM ZXCPF AS A 
	 	  JOIN (
		  	   SELECT * FROM YYZY.T_YYZY_QXJD 
			    WHERE QXMKDM=7 AND ZYBJ='1' AND QXJDJB=2
		  ) AS B
		  	   ON A.YYLXDM=B.XSSX
	 ORDER BY A.PYMC
;

/*原烟分析--中华原料节点*/
INSERT INTO YYZY.T_YYZY_QXJD(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX,QXMKDM, ZYBJ)
select (select max(qxjddm) from YYZY.T_YYZY_QXJD)+rownumber()over() as qxjddm,'中华原料' as qxjdmc, a.qxjdlx, A.QXJDDM AS fjddm,
	   a.qxjdjb+1 as qxjdjb, (select max(xssx) from YYZY.T_YYZY_QXJD where qxmkdm=7)+1 as xssx, a.qxmkdm, a.zybj
	   from YYZY.T_YYZY_QXJD as a
	   		where QXJDMC='原烟分析';
			
--  中华原料大产地节点
INSERT INTO YYZY.T_YYZY_QXJD(QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX,QXMKDM, ZYBJ)
WITH TMP AS(
SELECT DISTINCT B.DCDDM,B.DCDMC
	   FROM YYZY.T_YYZY_YYFX_YYTXFJRBB AS A
	   LEFT JOIN YYZY.T_YYZY_YYZDBMX AS B
	   ON A.YYDM=B.YYDM)
SELECT (SELECT MAX(QXJDDM) FROM YYZY.T_YYZY_QXJD)+ROWNUMBER()OVER() AS QXJDDM,
	   DCDMC AS QXJDMC,B.QXJDLX,B.QXJDDM AS FJDDM,B.QXJDJB+1 AS QXJDJB,A.DCDDM AS XSSX,B.QXMKDM,B.ZYBJ
	   FROM TMP AS A
	   LEFT JOIN YYZY.T_YYZY_QXJD AS B
	   ON 1=1
	   WHERE B.QXMKDM=7 AND B.QXJDMC='中华原料';
	   
--  大产地对应烟叶
INSERT INTO YYZY.T_YYZY_QXJD( QXJDDM, QXJDMC, QXJDLX, FJDDM, QXJDJB, XSSX, LJSX, QXMKDM, ZYBJ)
WITH TMP AS(
SELECT DISTINCT A.YYDM,RTRIM(B.YYCDMC)||RTRIM(B.YYDJMC) AS YYMC, B.DCDDM,B.DCDMC
	   FROM YYZY.T_YYZY_YYFX_YYTXFJRBB AS A
	   LEFT JOIN YYZY.T_YYZY_YYZDBMX AS B
	   ON A.YYDM=B.YYDM)
SELECT (SELECT MAX(QXJDDM) FROM YYZY.T_YYZY_QXJD)+ROWNUMBER()OVER() AS QXJDDM,A.YYMC AS QXJDMC,1 AS QXJDLX,
	   B.QXJDDM AS FJDDM,B.QXJDJB+1 AS QXJDJB,ROWNUMBER()OVER(PARTITION BY B.QXJDDM ORDER BY A.YYDM) AS XSSX,
	   '/tsas/treeOperationAction.do?createOtherView_ERUPTION_BUTTON=1&viewType=chinaRawMaterial'
	   ||'&yydm='||RTRIM(CHAR(A.YYDM)) AS LJSX,B.QXMKDM,B.ZYBJ
	   FROM TMP AS A
	   INNER JOIN YYZY.T_YYZY_QXJD AS B
	   		 ON A.DCDDM=B.XSSX
	   INNER JOIN YYZY.T_YYZY_QXJD AS D
	   		 ON B.FJDDM=D.QXJDDM
	   WHERE D.QXMKDM=7
	   AND D.QXJDMC='中华原料';

-- 品牌权限控制
update YYZY.T_YYZY_QXJD set QXJSDM=cast(null as int) WHERE QXMKDM IN(3,4) AND ZYBJ='1';

UPDATE YYZY.T_YYZY_QXJD AS K SET QXJSDM=(
   SELECT C.QXJSDM
FROM DIM.T_DIM_YYZY_PFPH AS A
   JOIN YYZY.V_YYZY_PFPPDY AS B ON A.PFPHDM=B.PFPHDM 
   JOIN YYZY.T_YYZY_QXJS AS C ON B.PPDM=C.YBDM AND C.ZYBJ='1' AND C.YBZD='PPDM'
WHERE A.JSRQ>CURRENT DATE AND (K.QXJDMC=A.PFPHMC )
FETCH FIRST ROW ONLY
)
WHERE K.QXMKDM IN(3,4) AND K.ZYBJ='1';

	   
  
  IF ERR_MSG <>'' then
    set sm = sm||'插入节点表出错  '||err_msg;
    return -1;
  end if;

END;

COMMENT ON PROCEDURE APP_YYB.P_YYZY_SXJD(  ) IS '树形节点生成';
