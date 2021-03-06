
DROP PROCEDURE APP_YYB.P_YYZY_JHJZ_R;

SET SCHEMA ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE APP_YYB.P_YYZY_JHJZ_R
 (IN START_DATE DATE, 
  OUT MSG VARCHAR(1000)
 ) 
  SPECIFIC APP_YYB.PROC_YYZY_JHJZ_R
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  --存储过程   
begin  
  /*声明*/ 
  --定义系统变量 1
  DECLARE SQLSTATE CHAR(5);  
  DECLARE SQLCODE INTEGER; 
  --定义变量 
  declare min_yr,max_yr,yr_2,yr_1,mn_1,mn_2,v_count,i_not_found integer; 
  declare v_rq,v_ksrq,v_jsrq,v_jsrq_2,judge_time date; 
  declare v_jhnf integer;
  declare err_msg varchar(1000) default ''; 
  declare new_ksrq date; 
  declare new_jsrq date;
  
  --梗膨计算
  declare EXE_SQL           varchar(20000);   --动态SQL
  declare v_gengpeng_jm     varchar(8000);  --梗膨解密
  declare v_gengpeng_gs     varchar(8000);    --梗膨计算公式
  declare v_gengpeng_cx     varchar(8000);    --梗膨结果查询
  declare v_gengpeng_jm_p   varchar(8000);       --梗膨解密_膨
  declare v_gengpeng_gs_p   varchar(8000);    --梗膨计算公式_膨
  declare v_gengpeng_cx_p   varchar(8000);    --梗膨结果查询_膨
  
    
  --定义空游标
  DECLARE c0 CURSOR for s0;
  
  --定义静态游标 
  declare c1 cursor for 
    select rq 
    from 
      (
        select RIQI as rq
        from DIM.T_DIM_YYZY_DATE
        where riqi between START_DATE and judge_time- 1 day
        union ALL
        select ksrq as rq
        from yyzy.t_yyzy_tmp_rscjhb_whb
        group by ksrq
        union 
        select jsrq as rq
        from yyzy.t_yyzy_tmp_rscjhb_whb
        group by jsrq
      ) date
    order by rq
  ;
    
  declare c2 cursor for 
    select ksrq,min(jsrq) from yyzy.t_yyzy_tmp_rscjhb_whb 
    where ksrq>=judge_time 
    group by ksrq 
    order by ksrq
  ;
      
  declare c3 cursor for
    select distinct jhnf
    from YYZY.T_YYZY_SCJH_ZZL
    where 1=1
      and jhnf>=year(new_ksrq) 
      and jhnf<=year(new_jsrq) 
    order by jhnf
  ;
  
  declare continue handler for not found set i_not_found = 1; 
  
  --定义异常处理 
  declare exit handler for sqlexception 
  begin 
    set MSG = '系统错误：sqlcode='||rtrim(char(sqlcode))||',sqlstate='||sqlstate||';  '; 
  end; 
  
  --定义临时表
  declare global temporary table tmp2( 
    pfphdm integer,
    jhrq date,
    jhcl_avg decimal(18,2),
    jhpc_avg decimal(18,2)
  ) with replace on commit preserve rows not logged; 

  declare global temporary table tmp3 like yyzy.t_yyzy_tmp_rscjhb_whb 
  with replace on commit preserve rows not logged;
  
  /*正文*/ 
  --set new_ksrq = START_DATE - (dayofyear(START_DATE)-1) day + 3 year;
  SELECT DATE(RTRIM(CHAR(MAX(JHNF)+1))||'-01-01') INTO NEW_KSRQ 
  FROM YYZY.T_YYZY_SNSCJH_ZXB
  ;
  set new_jsrq = START_DATE - (dayofyear(START_DATE)) day + 10 year;
  
  delete from yyzy.t_yyzy_tmp_rscjhb_whb; 
  delete from yyzy.t_yyzy_rscjhb_whb; 
  delete from session.tmp2; 
  delete from session.tmp3; 
  
  --处理 YYZY.T_YYZY_SNSCJH_ZXB 中三年计划以外的数据，否则过程报错 20110630
  INSERT INTO YYZY.T_YYZY_SNSCJH_ZXB_SC(PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH, SCSJ)
  SELECT PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH,CURRENT TIMESTAMP AS SCSJ
  FROM YYZY.T_YYZY_SNSCJH_ZXB WHERE JHNF>=YEAR(CURRENT DATE)+3;
  DELETE FROM YYZY.T_YYZY_SNSCJH_ZXB WHERE JHNF>=YEAR(CURRENT DATE)+3
  ;
    
  --写入月到日的计划 
  insert into session.tmp2(pfphdm,jhrq,jhcl_avg) 
  with scjh as
  ( 
    select date(substr(a.c_date, 1, 4)||'-'||substr(a.c_date, 5, 2)||'-'||substr(a.c_date, 7, 2)) as jhrq,
      a.quantity as jhcl,a.regionid,a.tobaccoid,c.factoryid,
      substr(a.tobaccoid,17,2) as yhbs,substr(d.factoryid, 13, 1) as cjdm
      from hds_cxqj.n_cxqj_o_prodplanhist_n as a 
      left join hds_cxqj.n_cxqj_o_regioninfo b 
      on a.regionid = b.regionid 
      left join hds_cxqj.n_cxqj_o_workshopinfo c 
      on b.workshopid = c.workshopid 
      left join hds_cxqj.n_cxqj_o_factoryinfo d 
      on c.factoryid = d.factoryid
    where (d_createtime,c_date) in(
        select MAX(d_createtime),c_date 
        from hds_cxqj.n_cxqj_o_prodplanhist_n 
        where int(c_date)>=year(START_DATE)*10000+month(START_DATE)*100+day(START_DATE) 
        group by c_date 
      ) 
  ) 
  select f.pfphdm,jhrq, sum(a.jhcl) as jhcl_avg
  from scjh as a 
  left join (
    /*
      2012-06-07龚玮慧修改，配方合并逻辑
      在YYZY.T_YYZY_YS_PH27表中增加映射规则，可以合并配方
    */
      select yhbs_src, cjdm_src, YHBS, CJDM 
      from YYZY.T_YYZY_YS_PH27  
      where zybj='1'
        and yhbs_src=yhbs
        and cjdm_src<>cjdm    
        and (yhbs_src, cjdm_src,bbrq)in(
                    select yhbs_src, cjdm_src,max(bbrq)
                      from YYZY.T_YYZY_YS_PH27
                                         where yhbs_src=yhbs
                                         and cjdm_src<>cjdm
                                      group by yhbs_src, cjdm_src
                                        )
    ) as ys 
      on (ys.yhbs_src, ys.cjdm_src)=(a.yhbs,a.cjdm)
    left join (
      select pfphdm,yhbs,sccjdm 
      from dim.t_dim_yyzy_pfph 
      where jsrq>current date and ksrq <current date 
    ) as f 
      on value(ys.yhbs,a.yhbs) = f.yhbs 
      and cast(value(ys.cjdm,a.cjdm) as integer) = f.sccjdm 
      and f.sccjdm in (0,1,2) 
  group by jhrq,f.pfphdm
  ;

  --特殊处理：更新当年1月1日的计划为0 
  update session.tmp2 
  set jhcl_avg=0.00
  where jhrq=start_date - dayofyear(start_date) day + 1 day;

  --求每日计划的判定日期(最大日期的下个月的1日) 
  SELECT max(jhrq)+ 1 month- day(max(jhrq)+ 1 month) day+1 day into judge_time 
  from session.tmp2; 
  
  set yr_1=year(judge_time-1 day);     --yr_1 月到日计划最大年份 
  select min(jhnf) into yr_2         --yr_2 年计划年份 
  from YYZY.T_YYZY_SNSCJH_ZXB 
  where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB)
    AND jhnf>=(select year(max(jhrq)) from session.tmp2)
  ;
  
  set mn_1=month(judge_time-1 day);     --mn_1月到日计划最大月份 
  select value(max(jhyf),0) into mn_2         --mn_2季到月计划最大月份 
  from YYZY.T_YYZY_JSCJH 
  where (jhnf,jhyf,bbh)in( 
      select jhnf,jhyf,max(bbh) 
      from YYZY.T_YYZY_JSCJH 
      group by jhnf,jhyf 
    ) 
    and jhnf=(select year(max(jhrq)) from session.tmp2)
  ; 
  
  if yr_1=yr_2 then --年计划与月计划为同一年 
    if mn_1<12 then --月计划为当年年末,需要计算年计划剩余量 
      if mn_1<mn_2 then --月计划更新不超过季度计划，需要计算季度剩余计划 
        --季度剩余计划
        insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg) 
        with jdjh_tmp as (
          select pfphdm,date(rtrim(char(jhnf))||'-'||rtrim(char(jhyf))||'-01') as ksrq, 
              date(rtrim(char(jhnf))||'-'||rtrim(char(jhyf))||'-01')+1 month-1 day as jsrq, 
              sum(JHCL) as jhcl 
          from YYZY.T_YYZY_JSCJH as a 
          /*
            2012-06-07龚玮慧修改，配方合并逻辑
            在YYZY.T_YYZY_YS_PH27表中增加映射规则，可以合并配方
            2013-04-07修改，生产厂家合并，牌号不合并  
          */
            left join (
              select yhbs_src, cjdm_src, YHBS, CJDM 
              from YYZY.T_YYZY_YS_PH27  
              where zybj='1' and yhbs_src=yhbs
                and cjdm_src<>cjdm
                and (yhbs_src, cjdm_src,bbrq)in(
                        select yhbs_src, cjdm_src,max(bbrq)
                           from YYZY.T_YYZY_YS_PH27
                                           where yhbs_src=yhbs 
                                                 and cjdm_src<>cjdm
                                                 group by yhbs_src, cjdm_src
                                                )
            ) as py 
              on a.yhbs = py.yhbs_src and a.cjdm = int(py.cjdm_src)
            left join DIM.T_DIM_YYZY_PFPH as b 
              on value(py.yhbs,a.yhbs) = b.yhbs
              and value(int(py.cjdm),a.cjdm) = b.sccjdm 
              and b.jsrq>start_date 
          where (jhnf,jhyf,bbh)in(select jhnf,jhyf,max(bbh) from YYZY.T_YYZY_JSCJH group by jhnf,jhyf) 
              and zybj='1' 
              and jhnf=yr_1 
              and pfphdm is not null 
              and jhyf>mn_1 
            group by pfphdm,JHNF, JHYF
        )
        select pfphdm,ksrq,jsrq,jhcl/(days(jsrq)-days(ksrq)+1) as jhcl 
        from jdjh_tmp; 
      end if; --end of 月计划更新不超过季度计划，需要计算季度剩余计划 
      
      --月计划更新与季度计划同步，不需要计算季度剩余计划
      insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg)
      with pfph as ( 
        select distinct PFPHDM 
        from DIM.T_DIM_YYZY_PFPH 
        where jsrq>start_date
      ),
      njh as ( 
        select PFPHDM, JHNF, sum(JHCL) as jhcl
        from YYZY.T_YYZY_SNSCJH_ZXB 
        where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB) 
            and jhnf=(select year(max(jhrq)) from session.tmp2)
            and pfphdm is not null
          group by pfphdm,jhnf 
      ),
      jjh as (
        select  pfphdm,JHNF,sum(JHCL) as jhcl
        from YYZY.T_YYZY_JSCJH as a 
        /*
          2012-06-07龚玮慧修改，配方合并逻辑
          在YYZY.T_YYZY_YS_PH27表中增加映射规则，可以合并配方
        */
          left join (
            select yhbs_src, cjdm_src, YHBS, CJDM 
            from YYZY.T_YYZY_YS_PH27  
            where zybj='1' and yhbs_src=yhbs and cjdm_src<>cjdm
              and (yhbs_src, cjdm_src,bbrq)in(
                        select yhbs_src, cjdm_src,max(bbrq)
                        from YYZY.T_YYZY_YS_PH27
                        where yhbs_src=yhbs and cjdm_src<>cjdm
                        group by yhbs_src, cjdm_src
                      )
          ) as py 
            on a.yhbs = py.yhbs_src and a.cjdm = int(py.cjdm_src)
          left join DIM.T_DIM_YYZY_PFPH as b 
            on value(py.yhbs,a.yhbs)=b.yhbs
            and value(int(py.cjdm),a.cjdm)=b.sccjdm 
            and b.jsrq>start_date 
        where (jhnf,jhjd,jhyf,bbh)in(
              select jhnf,jhjd,jhyf,max(bbh) 
              from YYZY.T_YYZY_JSCJH 
              group by jhnf,jhjd,jhyf
          ) 
          and zybj='1' 
          and jhnf=(select YEAR(max(jhrq)) from session.tmp2)
          and pfphdm is not null 
        group by pfphdm,JHNF
      ) 
      ,jjh_add as (
        select pfphdm,jhnf,sum(jhcl) as jhcl
        from (
          select pfphdm,jhnf,jhcl
          from jjh
          /*
          2013-01-11修改，由于未加载季度生产计划，年度计划减去的实际是月到日的生产计划，因此月度生产计划需要加上历史数据，
          */ 
          union all
          select coalesce(c.pfphdm, m.pfphdm) as pfphdm,year(jhrq) as jhnf,jhcl
          from YYZY.T_YYZY_RSCJH_LSB as m 
            left join YYZY.T_YYZY_FZJG_PHB as c
              on m.pfphdm = c.fzphdm and c.bl=1
          where jhrq<current_date 
            and year(jhrq)>=year(START_DATE)
          Union all
          select pfphdm,year(jhrq) as jhnf,jhcl_avg as jhcl
          from session.tmp2
          --where month(jhrq)>6  --2013-01-11 注释 ，由于未加载季度生产计划，因此不对月到日的生产计划做限制
        ) as a
        group by pfphdm,jhnf
      )
      ,cl_dnjy as (
        select a.pfphdm,value(b.jhnf,c.jhnf) as jhnf,
            value(b.jhcl,0) as cl_njh,value(c.jhcl,0) as cl_jjh,
            value(b.jhcl,0)-value(c.jhcl,0) as cl_jy
        from pfph as a 
          left join njh as b
            on a.pfphdm=b.pfphdm 
          left join jjh_add as c 
            on a.pfphdm=c.pfphdm and b.jhnf=c.jhnf
        where b.jhnf is not null or c.jhnf is not null
      )
      , cl_ksjs as (
        select pfphdm,date(rtrim(char(jhnf))||'-'||rtrim(char(case when mn_1>mn_2 then mn_1+1 else mn_2+1 end))||'-01') as ksrq,
              date(rtrim(char(jhnf))||'-12-31') as jsrq,cl_jy as jhcl 
        from cl_dnjy
        where date(rtrim(char(jhnf))||'-'||rtrim(char(case when mn_1>mn_2 then mn_1+1 else mn_2+1 end))||'-01')<date(rtrim(char(jhnf))||'-12-31')
      )
      select pfphdm,ksrq,jsrq,jhcl*1.0000/(days(jsrq)-days(ksrq)+1) as jhcl_avg
      from cl_ksjs
      ;
      
    end if; -- end of 月计划为当年年末,需要计算年计划剩余量 
    
    --月计划到年末,不需要计算年计划剩余 
    insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg) 
    with mhnjh_tmp as ( 
      select PFPHDM,rtrim(char(jhnf))||'-01-01' as ksrq, 
          rtrim(char(jhnf))||'-12-31' as jsrq,sum(JHCL) as jhcl 
      from YYZY.T_YYZY_SNSCJH_ZXB 
      where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB) 
        AND jhnf in (yr_1+1,yr_1+2) 
        and pfphdm is not null 
      group by PFPHDM,JHNF 
    )
    select pfphdm,ksrq,jsrq,(jhcl*1.00000)/(days(jsrq)-days(ksrq)+1) as jhcl_avg
    from mhnjh_tmp; 
  end if; 
  -- end of 年计划与月计划为同一年
    
  --年计划年份<月计划年份
  if yr_2<yr_1 then 
    delete from session.tmp3 where ksrq>=date(rtrim(char(yr_1))||'-01-01');
    insert into session.tmp3(pfphdm,ksrq,jsrq,jhcl_avg)
    with mhnjh_tmp as ( 
      select PFPHDM, rtrim(char(jhnf))||'-01-01' as ksrq, 
          rtrim(char(jhnf))||'-12-31' as jsrq, sum(JHCL) as jhcl
      from YYZY.T_YYZY_SNSCJH_ZXB
      where pfphdm is not null
        and jhnf in (yr_1,yr_1+1,yr_1+2) 
      group by PFPHDM,JHNF
    ) 
    select a.pfphdm,rtrim(char(yr_1))||'-'||rtrim(char(mn_1+1))||'-01' as ksrq,
        jsrq,((a.jhcl-value(b.jhcl,0))*1.00000)/(days(jsrq)-days(rtrim(char(yr_1))||'-'||rtrim(char(mn_1+1))||'-01')+1) as jhcl_avg 
    from mhnjh_tmp as a
      left join (select pfphdm,sum(jhcl_avg) jhcl,year(jhrq) as jhnf 
         from session.tmp2 group by pfphdm,year(jhrq)
      )as b on a.pfphdm=b.pfphdm and year(ksrq)=b.jhnf
    where year(ksrq)=yr_1 
    union all 
    select pfphdm,ksrq,jsrq,(jhcl*1.00000)/(days(jsrq)-days(ksrq)+1) as jhcl_avg 
    from mhnjh_tmp 
    where year(ksrq)>yr_1; 
  end if; 
  -- end of 年计划年份<月计划年份
  
  --汇总数据，进入临时表yyzy.t_yyzy_tmp_rscjhb_whb
  insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg) 
  select pfphdm,ksrq,jsrq,jhcl_avg 
  from session.tmp3; 
  
  --外加工无法取的月到日的计划默认为每天平摊
  delete from yyzy.t_yyzy_tmp_rscjhb_whb 
  where pfphdm in ( 
    select PFPHDM 
    from DIM.T_DIM_YYZY_PFPH 
    where sccjdm in (4,5,6,7) and jsrq>=start_date 
  );
  
  insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg)
  WITH snjh_wjg as ( 
    select PFPHDM, PZMC, SCCJ, JHNF, JHCL, BBH
    from YYZY.T_YYZY_SNSCJH_ZXB
    where bbh in (select max(bbh) from YYZY.T_YYZY_SNSCJH_ZXB)
      and pfphdm in (select pfphdm from DIM.T_DIM_YYZY_PFPH where sccjdm in (4,5,6,7)) 
  ) 
  ,lsjh as (
    select PFPHDM, year(jhrq) as jhnf, sum(JHCL) as jhcl
    from YYZY.T_YYZY_RSCJH_LSB
    where pfphdm in (select pfphdm from DIM.T_DIM_YYZY_PFPH where sccjdm in (4,5,6,7) and jsrq>current_date) 
    group by pfphdm,year(jhrq)
  )
  ,snjh_ksjs as (
    select a.pfphdm,
      date(rtrim(char(a.jhnf))||'-01-01')+(case when a.jhnf=year(START_DATE) then (dayofyear(START_DATE)-1) else 0 end) day as ksrq,
      date(rtrim(char(a.jhnf))||'-12-31') as jsrq,
      case when sum(a.jhcl-value(b.jhcl,0))>0 then sum(a.jhcl-value(b.jhcl,0)) else 0 end as jhcl
    from snjh_wjg as a
      left join lsjh as b
        on a.pfphdm=b.pfphdm and a.jhnf=b.jhnf 
    group by a.pfphdm,a.jhnf
  )
  , snjh_avg as ( 
      select pfphdm, KSRQ, JSRQ, 
        jhcl*1.0000/(days(jsrq)-days(ksrq)+1) as jhcl_avg 
      from snjh_ksjs
  )
  select PFPHDM, KSRQ, JSRQ, JHCL_AVG 
  from snjh_avg
  ;
  
  --更新批次
  update yyzy.t_yyzy_tmp_rscjhb_whb e 
  set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from yyzy.t_yyzy_tmp_rscjhb_whb a 
        left join (
          select pfphdm, dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in(
              select pfphdm,max(nf*100+yf) 
              from yyzy.t_yyzy_dpclb 
              group by pfphdm
            )
        ) b
          on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.ksrq,e.jsrq)=(a.pfphdm,a.ksrq,a.jsrq)
    )
  ;
  
  --更新批次
  update session.tmp2 e 
  set jhpc_avg=(
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from session.tmp2 a 
        left join (
            select pfphdm,dpcl 
            from yyzy.t_yyzy_dpclb 
            where (pfphdm,nf*100+yf)in( 
                select pfphdm,max(nf*100+yf) 
                from yyzy.t_yyzy_dpclb 
                group by pfphdm
              )
          ) b
            on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.jhrq)=(a.pfphdm,a.jhrq)
    )
  ;
  
  --得到一个连续的系列值
  insert into yyzy.t_yyzy_tmp_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg)
  with temp as(
    select riqi as jhrq,'x' as gl 
    from dim.t_dim_yyzy_date 
    where riqi between START_DATE and judge_time-1 day 
  )
  , pfph as (
    select pfphdm 
    from session.tmp2 
    union all
    select pfphdm 
    from yyzy.t_yyzy_tmp_rscjhb_whb 
  ),
  temp1 as( 
    select distinct pfphdm,'x' as gl 
    from pfph  
    where pfphdm not in (
        select pfphdm
        from DIM.T_DIM_YYZY_PFPH
        where sccjdm in (4,5,6,7)
      )
    group by pfphdm
  )
  , gl as(
    select pfphdm,a.gl,b.jhrq 
    from temp1 a
      left join temp b on a.gl=b.gl 
  )
  , result_1 as(
    select a.pfphdm,a.jhrq,coalesce(b.jhcl_avg,0) as jhcl_avg
    from gl a 
      left join session.tmp2 b
        on a.pfphdm=b.pfphdm
        and a.jhrq=b.jhrq
  )
  , result_2 as ( 
    select pfphdm,jhrq,jhcl_avg,rownumber()over(partition by pfphdm order by jhrq) as xh
    from result_1
  )
  , result_3(pfphdm,jhrq,jhcl_avg,xh,xh_1) as(
    select pfphdm,jhrq,jhcl_avg,xh,1 as xh_1 
    from  result_2 A
    where A.XH=1
    union all
    select a.PFPHDM,A.JHRQ,a.jhcl_avg,A.XH,
      (case 
        when (a.PFPHDM,A.JHRQ,a.jhcl_avg)=(B.PFPHDM,B.JHRQ+1 day,b.jhcl_avg)
          then b.xh_1 
        else a.xh 
      end) as xh_1 
    from result_2 a,result_3 b 
    WHERE A.XH=b.XH+1 and a.pfphdm=b.pfphdm
  )
  , result_4 as (
    select a.pfphdm,min(jhrq) as ksrq,max(jhrq) as jsrq,jhcl_avg as jhcl_avg
    from result_3 a 
    group by a.pfphdm,jhcl_avg,xh_1
    union 
    select a.pfphdm,min(jhrq) as ksrq,max(jhrq) as jsrq,jhcl_avg as jhcl_avg
    from session.tmp2 a 
    where jhrq>=judge_time 
    group by a.pfphdm,jhcl_avg,year(jhrq)
  )
  select a.pfphdm,a.ksrq,a.jsrq,a.jhcl_avg,round(a.jhcl_avg/b.dpcl,2) 
  from result_4 a 
    left join (
      select pfphdm,dpcl
      from yyzy.t_yyzy_dpclb 
      where (pfphdm,nf*100+yf)in(
          select pfphdm,max(nf*100+yf)
          from yyzy.t_yyzy_dpclb 
          group by pfphdm
        ) 
    ) b 
      on a.pfphdm=b.pfphdm
  ;
    
  insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg) 
  select pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg 
  from yyzy.t_yyzy_tmp_rscjhb_whb;
    
  /*2012-04-06修改bug*/
    /* 
      中断时间段的不全算法
  */
  
  set v_ksrq = judge_time;
  select max(jsrq) into v_jsrq from yyzy.t_yyzy_tmp_rscjhb_whb;
   
  insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg) 
  with pfphlb as (
    select distinct pfphdm
    from (
        select pfphdm
        from yyzy.t_yyzy_tmp_rscjhb_whb
        union all
        select pfphdm
        from session.tmp2 
      ) as a
    where pfphdm not in (
        select pfphdm
        from dim.t_dim_yyzy_pfph
        where sccjdm in (4,5,6,7)
      )
  ) 
  , bq(pfphdm,ksrq,jsrq) as ( 
    select pfphdm, 
      (lag(jsrq + 1 day, 1, cast(null as date)) over(partition by pfphdm order by ksrq,jsrq)) as ksrq,
      ksrq - 1 day as jsrq
    from yyzy.t_yyzy_tmp_rscjhb_whb
    where jsrq>=v_ksrq and ksrq<=v_jsrq
    union all
    --s
    select pfphdm, 
      v_ksrq as ksrq,
      min(ksrq - 1 day) jsrq
    from yyzy.t_yyzy_tmp_rscjhb_whb
    where jsrq>=v_ksrq and ksrq<=v_jsrq
    group by pfphdm
    union all
    --e
    select pfphdm, 
      max(jsrq+1 day) ksrq,
      v_jsrq as jsrq
    from yyzy.t_yyzy_tmp_rscjhb_whb
    where jsrq>=v_ksrq and ksrq<=v_jsrq
    group by pfphdm
  )
  select pfphlb.pfphdm, value(ksrq,v_ksrq), value(jsrq,v_jsrq), 0, 0
  from pfphlb left join bq on pfphlb.pfphdm = bq.pfphdm
  where ksrq is not null and  jsrq is not null
    and jsrq>=ksrq
  ;
  
  /*根据前台维护每年产量的增长率，逐年计算产量*/
  open c3;
  cl_zzl:loop
    set i_not_found=0;
    fetch c3 into v_jhnf;
    if i_not_found=1 then
      leave cl_zzl;
    end if;
    
    set new_ksrq = date(rtrim(char(v_jhnf))||'-01-01');
    
    insert into yyzy.t_yyzy_rscjhb_whb(pfphdm,ksrq,jsrq,jhcl_avg,jhpc_avg)
    select 
      a.pfphdm,new_ksrq as ksrq,
      (new_ksrq+1 year -1 day) as jsrq,
      value(scjh,0)/(days(new_ksrq+1 year -1 day)-days(new_ksrq)+1) as jhcl_avg,
      0 as jhpc_avg
    from 
      yyzy.t_yyzy_rscjhb_whb as a
      left join (
        select pfphdm,scjh
        from YYZY.T_YYZY_SCJH_ZZL
        where (pfphdm,jhnf,bbh)in(
            select pfphdm,jhnf,max(bbh)
            from YYZY.T_YYZY_SCJH_ZZL
            where jhnf=v_jhnf 
            group by pfphdm,jhnf
          )
      ) as b
        on a.pfphdm = b.pfphdm
    where 1=1 
      and ((new_ksrq-1 year) between ksrq and jsrq)
      and a.pfphdm not in (select pfphdm from YYZY.T_YYZY_PFPH_CFG where gpbj in (1,2))
    ;
  end loop;
  close c3;
  /*end of 根据前台维护每年产量的增长率，逐年计算产量*/
    
  -- 2013-09-10 新增，处理外加工在上海制丝的月生产计划
  call YYZY.P_YYZY_RSCJH_WJGPHZS(START_DATE);
    
  update yyzy.t_yyzy_rscjhb_whb
  set jhcl_avg=0.00
  where jhcl_avg<0
  ;
  
  update yyzy.t_yyzy_rscjhb_whb
  set ksrq=START_DATE
  where ksrq<START_DATE
  ;
  
  update yyzy.t_yyzy_rscjhb_whb
  set bbrq=(
    select MAX(date(d_createtime)) 
    from hds_cxqj.n_cxqj_o_prodplanhist_n
  )
  ;
  
  --更新批次
  update yyzy.t_yyzy_rscjhb_whb e 
  set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from yyzy.t_yyzy_rscjhb_whb a 
      left join (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in( 
            select pfphdm,max(nf*100+yf) 
            from yyzy.t_yyzy_dpclb 
            group by pfphdm
          )
        ) b
          on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.ksrq,e.jsrq)=(a.pfphdm,a.ksrq,a.jsrq)
    )
  ;
  
  --2013年09月13日增加，处理梗膨数据
  call YYZY.P_YYZY_RSCJH_GP(START_DATE);
  
  --更新批次
  update yyzy.t_yyzy_rscjhb_whb e 
  set jhpc_avg=( 
      select round(a.jhcl_avg/b.dpcl,2) as jhpc_avg
      from yyzy.t_yyzy_rscjhb_whb a 
      left join (
          select pfphdm,dpcl 
          from yyzy.t_yyzy_dpclb 
          where (pfphdm,nf*100+yf)in( 
            select pfphdm,max(nf*100+yf) 
            from yyzy.t_yyzy_dpclb 
            group by pfphdm
          )
        ) b
          on a.pfphdm=b.pfphdm
      where (e.pfphdm,e.ksrq,e.jsrq)=(a.pfphdm,a.ksrq,a.jsrq)
    )
  ; 
  
  --梗批次不正确
  update yyzy.t_yyzy_rscjhb_whb 
  set jhpc_avg=cast(null as decimal(10,2)) 
  where pfphdm IN (select PFPHDM from YYZY.T_YYZY_PFPH_CFG WHERE GPBJ='1' AND JSBJ='0') 
  ; 
  
end;
