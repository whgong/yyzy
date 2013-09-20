SET SCHEMA = APP_YYB;

CREATE PROCEDURE APP_YYB.P_YYZY_CNFX_MX ( OUT ERR_MSG VARCHAR(1000) )
  SPECIFIC SQL100210162130000
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
/* 存储过程主体 */
BEGIN 
  --定义系统变量
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  declare v_err_msg varchar(255);
  --定义参数 
  DECLARE stmt VARCHAR(5000) DEFAULT '';
  DECLARE n_JHCL,i_PFCN,n_PFC,n_PFCN DECIMAL(18,5); 
  DECLARE n_JHRQ DATE; 
  DECLARE I_PPMC,I_PFPHMC,n_PFPHMC,n_PPMC,i_PFMC VARCHAR(50); 
  DECLARE /*ERR_MSG,*/V_MESSAGE VARCHAR(1000) DEFAULT ''; 
  DECLARE I_COUNT,SUMJHXS,HZ_LJXSZ,i_not_found INTEGER DEFAULT 0; 
  DECLARE I_PFPHDM,I_DPXS,I_PPDM,I_JSDM,I_JHXS,LJXSZ,n_cnbj INTEGER; 
  DECLARE i_LJPJZ,i_xh,LJMDSZ,i_driver,I_CNBJ,i_SUMJHXS,I_PFDM INTEGER; 
  declare n_PFPHDM,n_PPDM INTEGER; 
  
  DECLARE c1 CURSOR FOR 
    select distinct PFPHDM,DPXS 
    from YYZY.T_YYZY_DPSX 
    where (pfphdm,bbrq) in (
      select pfphdm,max(bbrq) 
      from YYZY.T_YYZY_DPSX 
      group by pfphdm
    )
    order by pfphdm
  ;
  
  DECLARE c2 CURSOR FOR 
    select distinct PFPHDM 
    from YYZY.T_YYZY_DPSX 
    where (pfphdm,bbrq) in (
      select pfphdm,max(bbrq) 
      from YYZY.T_YYZY_DPSX 
      group by pfphdm
    )
    order by pfphdm
  ;
  
  DECLARE c3 CURSOR FOR 
    select distinct PFPHDM 
    from YYZY.T_YYZY_DPSX 
    where (pfphdm,bbrq) in (
      select pfphdm,max(bbrq) 
      from YYZY.T_YYZY_DPSX 
      group by pfphdm
    )
    order by pfphdm
  ;
  
  DECLARE C4 CURSOR FOR 
    select  PFDM,PFMC,PFPHDM,PFCN
    from YYZY.T_YYZY_CNPFB
    where cnbj=2
    order by pfphdm
  ;
  
  declare continue handler for not found set i_not_found = 1;
  
  --定义异常处理
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET ERR_MSG = '系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||';'; 
  
  DECLARE GLOBAL TEMPORARY TABLE TmpA 
  (
    ppdm integer,
    ppmc varchar(50),
    pfphdm integer,
    pfphmc varchar(50),
    cncy integer,
    cnxy integer,
    cgjh integer,
    cnbj integer
  ) with replace on commit preserve rows NOT LOGGED; 
  
  DECLARE GLOBAL TEMPORARY TABLE TmpB
  (
    ppdm integer,
    ppmc varchar(50),
    pfphdm integer,
    pfphmc varchar(50),
    jsdm integer,
    yydm varchar(50),
    yymc varchar(50),
    yynf integer,
    jhxs integer,
    cnbj integer,
    xh integer
  ) with replace on commit preserve rows NOT LOGGED;
  
  DECLARE GLOBAL TEMPORARY TABLE TmpC
  (
    PPDM integer,
    PFPHDM integer,
    CNBJ integer,
    ZTJNXS DECIMAL(10,2)
  ) with replace on commit preserve rows NOT LOGGED;
  
  DECLARE GLOBAL TEMPORARY TABLE TmpD
  (
    PPDM integer,
    PFPHDM integer,
    CNBJ integer,
    Mdjnxs DECIMAL(10,2)
  ) with replace on commit preserve rows NOT LOGGED;
  
  DECLARE GLOBAL TEMPORARY TABLE TmpE 
  (
    JHRQ date,
    PFPHDM integer,
    JHCL DECIMAL(18,5),
    PFPHMC varchar(50),
    ppdm integer,
    ppmc varchar(50),
    xh integer
  ) with replace on commit preserve rows NOT LOGGED;
  
  /* 
    cnbj=1 当前产能
    cnbj=2 当年产能
    陈烟 yynf小于当前年份-2
    新烟 yynf小于当前年份-1
    采购 yynf等于当前年份
  */
  
   --cnpfb
   call YYZY.P_YYZY_CNFX_CNPFB(v_err_msg);
   call YYZY.P_YYZY_CNFX_CNPFB_QBP(v_err_msg);
   call YYZY.P_YYZY_CNFX_CNPFB_QGW(v_err_msg);
   call YYZY.P_YYZY_CNFX_CNPFB_QBPGW(v_err_msg);
 



  -- 产能分析表
   delete from YYZY.T_YYZY_CNFXB; 
   insert into YYZY.T_YYZY_CNFXB(KCZL,PFCN,KCCN,PYCN,CNBJ) 
   select *
   from YYZY.V_YYZY_CNFXB
   ;
   
   
   
  --- 醇化周期报警表
  delete from YYZY.T_YYZY_CHZQBJB;
  insert into YYZY.T_YYZY_CHZQBJB(PFMC,PFPHMC,CHZQ,SYKS,YYMC,CHZQBZ) 
  select *
  from YYZY.V_YYZY_CHZQBJB
  ;
  


 DELETE FROM YYZY.T_YYZY_TMP_CBGZ;
 delete from session.Tmpe;
 set i_not_found=0; 
   OPEN c4;
     loop_ml4: loop
            FETCH c4 INTO i_PFDM,i_PFMC,i_PFPHDM,i_PFCN;
         if i_not_found=1   then
               leave loop_ml4;
             end if;
   
  
  INSERT INTO YYZY.T_YYZY_TMP_CBGZ(JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC,XH)
      with rq(rq) as (
        select RIQI 
        from DIM.T_DIM_YYZY_DATE
      ),
      rscjh as (
        select PFPHDM, rq as jhrq, JHCL_AVG as jhcl
        from YYZY.T_YYZY_RSCJHB_WHB as a,rq as b
        where b.rq between ksrq and jsrq 
        union all 
        select pfphdm,jhrq,jhcl 
        from YYZY.T_YYZY_RSCJH_LSB
        where jhrq>current date - dayofyear(current date) day
      )
      SELECT b.JHRQ,b.PFPHDM,b.jhcl,A.PFPHMC,A.PPDM,A.PPMC,
          rownumber()over(order by jhrq) xh 
      FROM rscjh as b,YYZY.V_YYZY_PFPPDY as A 
      WHERE A.PFPHDM=b.PFPHDM
      and A.PFPHDM=i_PFPHDM; 
      
  set i_xh=1;
  set n_PFCN=i_PFCN;
  
  while (ceil(n_PFCN)>0) do 
    select JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC into n_JHRQ,n_PFPHDM,n_JHCL,n_PFPHMC,n_PPDM,n_PPMC 
    FROM YYZY.T_YYZY_TMP_CBGZ 
    where xh=i_xh; 
    
    set n_PFCN=n_PFCN-n_JHCL; 
    set i_xh=i_xh+1; 
    select count(1) into i_count from YYZY.T_YYZY_TMP_CBGZ where xh=i_xh; 
    
    if(i_count<1) then 
      set n_PFCN=0; 
    end if;
  end while;
  
  select COUNT(1) INTO I_count
  from YYZY.T_YYZY_TMP_CBGZ 
  where xh=i_xh;
  if (I_count>0) then
    insert into session.Tmpe(JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC)
    select JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC 
    from YYZY.T_YYZY_TMP_CBGZ where xh=i_xh-1;
  else 
    insert into session.Tmpe(JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC)
    select JHRQ,PFPHDM,JHCL,PFPHMC,PPDM,PPMC 
    from YYZY.T_YYZY_TMP_CBGZ where xh=(select max(xh) from YYZY.T_YYZY_TMP_CBGZ);
  end if;
  delete from YYZY.T_YYZY_TMP_CBGZ;
 
  SET i_not_found=0; 
  end loop  loop_ml4;
  close c4;
  

  -- 储备规则
  delete from YYZY.T_YYZY_CBGZBJB;
  insert into YYZY.T_YYZY_CBGZBJB(PFMC,PFPHMC,CXSCZ,CBGZCN,CCZT,ZZGZ)
  with tmp as ( 
select PZCCGZDM, PPCCGZDM, PFPHDM, FJGZXX, FJGZSX, BZ, BBH, BBRQ  
from YYZY.T_YYZY_PZCCGZB  
where (pzccgzdm,bbh)in( select pzccgzdm,max(bbh)as bbh from YYZY.T_YYZY_PZCCGZB group by pzccgzdm) 
),
tmp2 as ( 
SELECT A.PPCCGZDM, A.PPDM,B.PPMC, A.JBGZSX, A.JBGZXX, A.FZZTDM, A.BZ, A.BBH, A.BBRQ   
       FROM YYZY.T_YYZY_PPCCGZB AS A,DIM.T_DIM_YYZY_PP AS B  
    WHERE A.PPDM=B.PPDM AND (PPCCGZDM,BBH)  
    IN ( SELECT PPCCGZDM,MAX(BBH) FROM YYZY.T_YYZY_PPCCGZB GROUP BY PPCCGZDM)
),   
tmp3 as ( select LSBH, FZZTDM, FZZTMC, FZZTXS, BZ, BBH, BBRQ   
       from YYZY.T_YYZY_FZZTB  
    WHERE (FZZTDM,BBH) IN ( SELECT FZZTDM,MAX(BBH)AS BBH FROM YYZY.T_YYZY_FZZTB GROUP BY FZZTDM)
),
tmp4 as(  
select TMP2.PPMC,
    TMP.PFPHDM,
    DIM.T_DIM_YYZY_PFPH.PFPHMC,
  TMP2.JBGZSX,
  TMP2.JBGZXX,
  TMP.FJGZXX,   
    TMP.FJGZSX, 
  TMP3.FZZTMC,
  TMP3.FZZTXS,
  (JBGZXX+FJGZXX)*FZZTXS AS ZZGZXX,
  (JBGZSX+FJGZSX)*FZZTXS AS ZZGZSX   
from tmp,DIM.T_DIM_YYZY_PFPH,TMP2,TMP3   
WHERE DIM.T_DIM_YYZY_PFPH.PFPHDM=TMP.PFPHDM   
AND TMP.PPCCGZDM=TMP2.PPDM AND TMP3.FZZTDM=TMP2.FZZTDM   
),
tmp5 as(
select PFDM as ppdm,PFMC,PFPHDM,PFPHMC,sum(CNCY+CNXY+CGJH) as CBGZCN 
  from YYZY.T_YYZY_CNPFB where cnbj=2 
  group by PFDM,PFMC,PFPHDM,PFPHMC 
)
select b.PPMC,b.PFPHMC,JHRQ as CXSCZ,CBGZCN, 
   case 
       when JHRQ between current date + ZZGZXX month and current date + ZZGZSX month
       then 0
   when JHRQ<current date + ZZGZXX month
       then -1
   when JHRQ>current date + ZZGZSX month
       then 1
end CBZT,
cast((current date + ZZGZXX month) as char(10))||'--'|| cast((current date + ZZGZSX month) as char(10)) as ZZGZ        
 from tmp4 a,session.Tmpe b,tmp5 c
 where a.pfphdm=b.pfphdm and a.pfphdm=c.pfphdm;
 
 

  delete from YYZY.T_YYZY_CNSJWDB;
  insert into YYZY.T_YYZY_CNSJWDB(PFDM,PFPHDM,PFMC,PFPHMC,CXSCZ,DQJSCN,CBZT,CBSX, 
        CBXX,YYMZD, ZTJNXS, MDJNXS,CBQJYF) 
  select *
  from YYZY.V_YYZY_CNSJWDB
  ;


  -- 详细烟叶缺口表
  delete from YYZY.T_YYZY_XXYYQKB;
  insert into YYZY.T_YYZY_XXYYQKB(YYDM,CNBJ,PFDM,PFPHDM,PFPHMC,YYQKL,YYMC)
  select *
  from YYZY.V_YYZY_XXYYQKB
  ;
  
  
  delete from YYZY.T_YYZY_CNFX_JS;
  insert into YYZY.T_YYZY_CNFX_JS
  select * 
  from YYZY.V_YYZY_CNFX_JS
  ;
  

  delete from YYZY.T_YYZY_PFPHJSB; 
  insert into YYZY.T_YYZY_PFPHJSB(JSDM,JSCN,JSCNQK,FPXS,JSQKL,JHCL,SYSJ,CHZTTS,PHDM,CNBJ,
                   JHXS,PFPHMC,YYDM,YYMC,YYNF,CHZQ,YYSYL,KSSYSJ,PFPHDM)
  with tmp2 as ( 
    --当前产能 
    select PPDM, PFPHDM, JSDM, YYDM, YYNF, JHXS, KSRQ, JSRQ, FXBJ 
    from YYZY.T_YYZY_CNFX 
    --当前产能时：fxbj = 1 年度产能时：fxbj = 0
  ),tmp as ( 
    -- 当前产能 
    select PPDM, PFPHDM, JSDM, YYDM, YYNF, JHXS, KSRQ, JSRQ, FXBJ
    from YYZY.T_YYZY_CNFX 
    --当前产能时：fxbj = 1 年度产能时：fxbj = 0
    where ksrq=(select min(ksrq) 
    from tmp2 )
  )
  ,js as(
    select pfphdm,YYDM,fxbj,
      rownumber() over(partition by pfphdm,fxbj order by px) as js  
    from (
      select YYDM,min(jsdm)as px,fxbj,pfphdm 
      from tmp 
      group BY yydm,fxbj,pfphdm
    ) as a 
  )
  ,jsdm as( 
  select tmp.pfphdm,JSDM,js,js.fxbj 
  from tmp 
  left join js on tmp.yydm =js.yydm and tmp.fxbj=js.fxbj
  and tmp.pfphdm=js.pfphdm
),tmp3 as ( 
  select PPDM,jsdm.PFPHDM,tmp2.JSDM,YYDM,YYNF,JHXS,KSRQ,JSRQ,jsdm.FXBJ,js 
  from tmp2,jsdm 
  where tmp2.jsdm = jsdm.jsdm 
    and tmp2.fxbj=jsdm.fxbj 
    and tmp2.pfphdm=jsdm.pfphdm 
),tmp4 as(
  --求jhxs 
  select ppdm,pfphdm,js jsdm,zd.yydm,yymc,sum(jhxs)jhxs,fxbj,tmp3.yynf 
  from tmp3,dim.t_dim_yyzy_yyzdb zd 
  where tmp3.yydm=zd.yydm 
  group by ppdm,pfphdm,zd.yydm,yymc,js,fxbj,tmp3.yynf 
),tmp5 as (
--当前产能,dpxs
select pfphdm,JSDM as pfphjsdm, 
    (case 
      when ksrq<current date - dayofyear(current date) day+1 day 
    then current date - dayofyear(current date) day+1 day 
  else ksrq
end) as KSRQ, 
(case 
  when jsrq>=current date - dayofyear(current date) day+1 day +8 month 
    then current date - dayofyear(current date) day+1 day +8 month 
  else jsrq 
end) as JSRQ, DPXS, 1 as fxbj
from YYZY.T_YYZY_JSTZ_WHB
where zybj='1'
  and (
    (ksrq<=current date - dayofyear(current date) day+1 day and jsrq>=current date - dayofyear(current date) day+1 day)
or 
(ksrq>=current date - dayofyear(current date) day+1 day and ksrq<=current date - dayofyear(current date) day+1 day +8 month )
  )
  
  --  如果为年度产能此处条件为ksrq = 当前年Year-01-01 即：ksrq = 2008-01-01 
  -- 如果为当前产能 ksrq >= 当前年Year-01-01 and jsrq >= 当前年-09-01　
  -- 即：ksrq>='2008-01-01'and jsrq>='2008-09-01'
  
  -- 年度产能
union all
select pfphdm,JSDM as pfphjsdm, 
    (case 
      when ksrq<current date - dayofyear(current date) day+1 day 
    then current date - dayofyear(current date) day+1 day 
  else ksrq
end) as KSRQ, JSRQ, DPXS, 0 as fxbj
from YYZY.T_YYZY_JSTZ_WHB 
where zybj='1' 
  and (
    (ksrq<=current date - dayofyear(current date) day+1 day and jsrq>=current date - dayofyear(current date) day+1 day)
or 
(ksrq>=current date - dayofyear(current date) day+1 day and ksrq<=current date - dayofyear(current date) day+ 1 year) 
  ) 
),tmp6 as( 
--求DPXS
select pfphdm,pfphjsdm,ksrq,jsrq,dpxs,fxbj 
from tmp5 
where (pfphdm,pfphjsdm,ksrq,fxbj)in(
    select pfphdm,pfphjsdm,min(ksrq),fxbj from tmp 
    group by pfphdm,pfphjsdm,fxbj) 
),tmp7 as(
--求DPCL
select  PFPHDM,DPCL,NY 
from YYZY.T_YYZY_DPCLB 
where ny = (select max(ny) from YYZY.T_YYZY_DPCLB) 
),tmp8 as(
--角色产能： jscn
--jhxs / 相同角色的dpxs * 相同配方牌号的DPCL
select a.ppdm,a.pfphdm,a.jsdm,a.yydm,a.yymc,a.jhxs,a.jhxs/b.dpxs*c.dpcl as jscn,a.fxbj,a.yynf
from tmp4 a, tmp6 b,tmp7 c
where a.pfphdm=b.pfphdm and a.jsdm=b.pfphjsdm and a.fxbj=b.fxbj
and a.pfphdm=c.pfphdm),tmp9 as(
-- 求当前产能 JHCL
select pfphdm,SUM(JHCL)jhcl,1 fxbj  from YYZY.V_YYZY_YJHFX_WHB
WHERE  (JHNF,JHYF,BBH)IN (
   SELECT JHNF,JHYF,MAX(BBH) FROM YYZY.V_YYZY_YJHFX_WHB GROUP BY JHNF,JHYF) 
AND JHNF=year(current date) AND JHYF<=month(current date)
group by pfphdm ---jhnf = 当前年份,jhyf < 当前月份
union 
-- 求年度产能
select pfphdm,SUM(JHCL)jhcl,0 fxbj from YYZY.V_YYZY_NJHFX_WHB 
WHERE JHNF>=year(current date)  ---jhnf >= 当前年份
AND (JHNF,BBH)IN(SELECT JHNF,MAX(BBH)FROM YYZY.V_YYZY_NJHFX_WHB GROUP BY JHNF)
group by pfphdm
),tmp10 as(
-- 求当前产能的JSCNQK (角色缺口量)
select a.ppdm,a.pfphdm,a.jsdm,a.yydm,a.yymc,a.jscn,a.jhxs,jhcl,jscn-jhcl as JSCNQK,a.fxbj,a.yynf
from tmp8 a,tmp9 b 
where a.pfphdm=b.pfphdm and a.fxbj=b.fxbj),tmp11 as( 
-- 求当前产能的缺口量(QKL) 
select a.ppdm,a.pfphdm,a.jsdm,a.yydm,a.yymc,a.jscn,a.jhxs,a.jhcl,a.JSCNQK,a.JSCNQK*dpxs/dpcl as JSQKL,a.fxbj,a.yynf
from tmp10 a,tmp6 b,tmp7 c
where a.pfphdm=b.pfphdm and a.pfphdm=c.pfphdm and a.jsdm=b.pfphjsdm
and a.fxbj=b.fxbj
),
rscjh as (
  select PFPHDM, riqi as jhrq, JHCL_AVG as jhcl
  from YYZY.T_YYZY_RSCJHB_WHB as a,DIM.T_DIM_YYZY_DATE as b
  where b.riqi between ksrq and jsrq  
  union all
  select pfphdm,jhrq,jhcl
  from YYZY.T_YYZY_RSCJH_lsb
  where jhrq>=current date -(dayofyear(current date)-1) day
),
tmp12 as(
  -- 当前产能
  select JHRQ,PFPHDM,JHCL, 1 fxbj 
  from rscjh 
  where jhrq>=current date-(day(current date) - 1) day 
      ----如果为当前产能则jhrq >=当前年月-01 即:jhrq >= '2008-12-01' 
  --  如果为年度产能则jhrq >=当前年-01-01即：jhrq >= '2008-01-01' 
union all
-- 年度产能
  select JHRQ,PFPHDM,JHCL, 1 fxbj 
  from rscjh 
  where jhrq>=current date-(dayofyear(current date) - 1) day 
----如果为当前产能则jhrq >=当前年月-01 即:jhrq >= '2008-12-01'
--  如果为年度产能则jhrq >=当前年-01-01即：jhrq >= '2008-01-01'
),tmp13 as(
select a.ppdm,a.pfphdm,a.jsdm,a.yydm,a.yymc,a.jscn,
 cast(abs((year(jhrq)-year(current date))*12)+(month(jhrq)-month(current date - dayofyear(current date) day+1 day))as decimal(10,2)) as SYSJ,  
a.jhxs,a.jhcl,a.JSCNQK,a.JSQKL,a.fxbj,a.yynf
from tmp11 a,tmp12 b
where a.pfphdm=b.pfphdm and a.fxbj=b.fxbj and a.fxbj=1 and a.jscn-b.jhcl<0
union all
select a.ppdm,a.pfphdm,a.jsdm,a.yydm,a.yymc,a.jscn,
cast(abs((year(jhrq)-year(current date))*12)+(month(jhrq)-month(current date))as decimal(10,2)) as SYSJ,    
a.jhxs,a.jhcl,a.JSCNQK,a.JSQKL,a.fxbj,a.yynf
from tmp11 a,tmp12 b
where a.pfphdm=b.pfphdm and a.fxbj=b.fxbj and a.fxbj=0 and a.jscn-b.jhcl<0
),tmp14 as(
select pfphdm,a.yydm,zd.yymc,a.yynf,SYKSRQ, SYJSRQ, CHZQSX, CHZQXX, SYSL 
from( select zq.pfphdm,zq.yydm,zq.yynf as yynf,SYKSRQ, SYJSRQ, CHZQSX, CHZQXX, SYSL, 
         row_number() over(partition by zq.yydm,zq.pfphdm order by zq.yynf,syksrq)as num  
  from YYZY.T_YYZY_CNFX cnfx 
  join YYZY.T_YYZY_CNFXCHZQ zq on cnfx.yydm = zq.yydm and cnfx.pfphdm = zq.pfphdm 
) a 
left join DIM.T_DIM_YYZY_YYZDB zd on a.yydm = zd.yydm   
where a.num =1

),tmp15 as( 
  select a.pfphdm,a.yydm,a.yymc,
      CAST ((DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQXX-1) MONTH) AS CHAR(10))||
      '--'||CAST ((DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQSX-1) MONTH )AS CHAR(10)) as CHZQ, 
      CASE 
        WHEN SYKSRQ>DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQXX-1) MONTH 
            and DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQSX-1) MONTH -SYKSRQ>0 
          THEN '滞后'||cast(days(DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQSX-1) MONTH) - days(SYKSRQ) as char(4))||'天使用' 
        WHEN SYKSRQ>DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQXX-1) MONTH 
            and DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQSX-1) MONTH -SYKSRQ>0
    THEN  '滞后1天使用'   
    WHEN SYKSRQ<DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQXX-1) MONTH 
      THEN '提前'||cast(days(DATE('0001-01-01')+(a.YYNF-1) YEAR+(CHZQXX-1) MONTH) - days(SYKSRQ) as char(4))||'天使用' 
        when (a.yydm,a.yynf) not in(select distinct yydm,yynf from tmp4) 
          then '正常使用' 
  end as CHZTTS,SYSL,SYKSRQ,CHZQSX, CHZQXX,yynf from tmp14 a
)
select JSDM,JSCN,JSCNQK,jhxs as FPXS,JSQKL,JHCL,SYSJ, 
    CHZTTS,PPDM AS PHDM,
case 
  when fxbj=1 
    then 2 
  when fxbj=0
    then 1
end as CNBJ,JHXS,PFPHMC,a.YYDM,a.YYMC,a.YYNF,CHZQ
    ,SYSL as YYSYL,SYKSRQ as KSSYSJ, a.PFPHDM 
from tmp13 a 
left join tmp15 b
  on a.yydm=b.yydm and a.yynf=b.yynf 
left join DIM.T_DIM_YYZY_PFPH c
  on a.pfphdm=c.pfphdm
;

END;
