--drop PROCEDURE YYZY.P_YYZY_RSCJH_GP;
SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_RSCJH_GP 
( 
  IP_STARTDATE date
)
  SPECIFIC PROC_YYZY_RSCJH_GP
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
LB_MAIN:
BEGIN ATOMIC
  /* DECLARE SYSTEM VARIABLES */
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE V_SQLSTATE CHAR(5); 
  DECLARE I_SQLCODE INTEGER; 
  DECLARE SQL_CUR_AT_END INTEGER; 
  DECLARE SQL_STMT VARCHAR(2000); 
  /* DECLARE USER-DEFINED VARIABLES */ 
  -- DECLARE V_SEPARATOR VARCHAR(50) DEFAULT ','; 
  declare EXE_SQL           varchar(20000);     --动态SQL
  declare v_gengpeng_jm     varchar(8000); 	    --梗膨解密
  declare v_gengpeng_gs     varchar(8000);		--梗膨计算公式
  declare v_gengpeng_cx     varchar(8000);		--梗膨结果查询
  declare v_gengpeng_jm_p   varchar(8000); 	    --梗膨解密_膨
  declare v_gengpeng_gs_p   varchar(8000);		--梗膨计算公式_膨
  declare v_gengpeng_cx_p   varchar(8000);		--梗膨结果查询_膨
  
  /* DECLARE STATIC CURSOR */
  -- DECLARE C1 CURSOR /*WITH RETURN*/ FOR
  --   SELECT DISTINCT NAME, CREATOR, TYPE
  --   FROM SYSIBM.SYSTABLES
  --   ORDER BY TYPE,CREATOR,NAME
  -- ;
  /* DECLARE DYNAMIC CURSOR */
  DECLARE C93 CURSOR FOR S93;
  /* DECLARE EXCEPTION HANDLE */
--  DECLARE UNDO HANDLER FOR SQLEXCEPTION
--  BEGIN 
--    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
--    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
--      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
--      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
--    ; 
--  END; 
--  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  DECLARE GLOBAL TEMPORARY TABLE tb_ysjg
  (
    pfphdm INTEGER,
    gpph integer,
    gpbl decimal(18,6),
    ysbl decimal(18,6),
    gpbj integer
  ) with replace on commit preserve rows not logged
  ; 
  DECLARE GLOBAL TEMPORARY TABLE t_yyzy_rscjhb_whb like yyzy.t_yyzy_rscjhb_whb with replace on commit preserve rows not logged;
  
  /* SQL PROCEDURE BODY */
  delete from session.tb_ysjg;
  delete from session.t_yyzy_rscjhb_whb;
-----------------------------------------------------------------------------------
  insert into session.tb_ysjg(pfphdm, gpph, gpbj, gpbl, ysbl)
  with tb_ysjg as (
    select LSBH, PFPHDM, YSJGDM, YANSI, GENGSI, PENGSI1, PENGSI2, 
      BOPIAN, DXHY, DPTL, BBH, BBRQ, QYBBH, XGBZ, KSRQ, JSRQ 
    from DIM.T_DIM_YYZY_YSJGB
    where (pfphdm,BBH)in(
        select pfphdm, max(bbh)
      from DIM.T_DIM_YYZY_YSJGB
      group by PFPHDM
      )
    order by pfphdm
  )
  select pfphdm, 16 as gpph, 1 as gpbj, 
    cast(decrypt_char(gengsi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gpbl, 
    cast(decrypt_char(yansi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as ysbl
  from tb_ysjg
  union all
  select pfphdm, 23 as gpph, 2 as gpbj, 
    cast(decrypt_char(pengsi1,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gpbl, 
    cast(decrypt_char(yansi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as ysbl
  from tb_ysjg
  union all
  select pfphdm, 24 as gpph, 2 as gpbj, 
    cast(decrypt_char(pengsi2,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gpbl, 
    cast(decrypt_char(yansi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as ysbl
  from tb_ysjg
  ;
  
  lp1:
  for v1 as c1 cursor for
    SELECT a.name, b.pfphdm, b.gpbj
    FROM sysibm.syscolumns as a,YYZY.T_YYZY_PFPH_CFG as b 
    WHERE A.NAME=B.YSJGLM
      AND TBNAME='T_DIM_YYZY_YSJGB_KZB' 
      AND TBCREATOR='DIM'
      AND (NAME LIKE 'GENG%' OR NAME LIKE 'PENG%')  
      AND NAME NOT IN ('PFPHDM', 'BBH', 'ZDMC', 'ZDZ', 'ZDPFPH', 'TJRQ')
  do
    set SQL_STMT = ''||
      'insert into session.tb_ysjg(pfphdm, gpph, gpbj, gpbl, ysbl) '||
      'with tb_ysjg as ( '||
      '  select LSBH, PFPHDM, YSJGDM, YANSI, GENGSI, PENGSI1, PENGSI2,  '||
      '    BOPIAN, DXHY, DPTL, BBH, BBRQ, QYBBH, XGBZ, KSRQ, JSRQ  '||
      '  from DIM.T_DIM_YYZY_YSJGB '||
      '  where (pfphdm,BBH)in( '||
      '      select pfphdm, max(bbh) '||
      '      from DIM.T_DIM_YYZY_YSJGB '||
      '      group by PFPHDM '||
      '    ) '||
      ') '||
      ', tb_ysjgkzb as ( '||
      '  select pfphdm, '|| v1.name ||' as gpbl '||
      '  from DIM.T_DIM_YYZY_YSJGB_KZB '||
      '  where (pfphdm, bbh)in ( '||
      '      select pfphdm, max(bbh) from DIM.T_DIM_YYZY_YSJGB_KZB group by pfphdm '||
      '    ) '||
      ') '||
      'select m.PFPHDM, '||char(v1.pfphdm)||' as gpph, '||v1.gpbj||' as gpbj, '||
      '  cast(decrypt_char(m.gpbl,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as gpbl, '||
      '  cast(decrypt_char(c.yansi,yyzy.f_yyzy_myjm()) as decimal(10, 2)) as ysbl '||
      'from tb_ysjgkzb as m  '||
      '  left join tb_ysjg as c  '||
      '    on m.pfphdm = c.pfphdm  '
      ;
      prepare s93 from SQL_STMT;
      execute s93;
      
  end for lp1;
-----------------------------------------------------------------------------------
  --获得各牌号的每日所需耗用量
  delete from yyzy.t_yyzy_tmp_yyxhtjb;
  insert into yyzy.t_yyzy_tmp_yyxhtjb(pfphdm,jsdm,ksrq,jsrq,hl_d)
  select pfphdm,JSDM,
    case 
      when ad1 < bd1 then bd1 
      else ad1 
    end KSRQ,
    case 
      when ad2 > bd2 then bd2 else ad2 
    end JSRQ,xhyzl 
  from ( 
    select a.pfphdm,JSDM, a.ksrq ad1 ,a.jsrq ad2,
        b.KSRQ bd1 ,b.jsrq bd2,DPXS ,b.JHPC_AVG, 
        case 
          when a.pfphdm = 16 then jhcl_avg*DPXS/100 
          else jhpc_avg * DPXS 
        end xhyzl
    from (
      select pfphdm,JSDM,KSRQ,JSRQ,DPXS 
      from YYZY.T_YYZY_JSTZ_WHB 
      where zybj='1' 
    ) a 
    inner join YYZY.T_YYZY_RSCJHB_WHB as b 
      on a.ksrq <=b.jsrq 
      and b.ksrq<=a.jsrq 
      and a.pfphdm =b.pfphdm 
  ) as t 
  where pfphdm not in (select pfphdm from YYZY.T_YYZY_PFPH_CFG where gpbj in ('1','2')) 
  order by pfphdm,JSDM,(case when ad1<bd1 then bd1 else ad1 end)
  ; 
  
  --按时间段依次处理
  delete from session.t_yyzy_rscjhb_whb;
  lp2:
  for v2 as c2 cursor for
    select value(lag(jsrq, 1)over(order by jsrq)+1 day,IP_STARTDATE) as ksrq,jsrq
    from (
      select distinct jsrq
      from yyzy.t_yyzy_tmp_yyxhtjb
    ) as t
  do
    insert into session.t_yyzy_rscjhb_whb(pfphdm, ksrq, jsrq, jhcl_avg)
    with tb_hyl_sjd as (
      select pfphdm , 
        (case when ksrq<ksd then ksd else ksrq end) as ksrq,
        (case when jsrq>jsd then jsd else jsrq end) as jsrq , hl_d
      from yyzy.t_yyzy_tmp_yyxhtjb as m 
        inner join (values (v2.ksrq,v2.jsrq)) as t(ksd, jsd)
          on (ksrq<=jsd and ksd<=jsrq)
      where m.hl_d <> 0
    )
    , tb_hyzl as (
      select pfphdm, sum(hl_d*(days(jsrq)-days(ksrq)+1)) as hyzl
      from tb_hyl_sjd
      group by pfphdm
    )
    , tb_hyzl_sjd as(
      select j.gpph as pfphdm, 
        case 
          when gpbj = 2 then ceil(sum(hyzl * value(gpbl,0) * 1.00000 / ysbl) / 0.95)
          when gpbj = 1 then ceil(sum(hyzl * value(gpbl,0) * 1.00000 / ysbl) / 0.96*4)
        end as jhcl
      from tb_hyzl as m
        left join session.tb_ysjg as j
          on m.pfphdm = j.pfphdm
      where j.ysbl<>0
      group by gpph,gpbj
    )
    select pfphdm, v2.ksrq, v2.jsrq, jhcl*1.000000/(days(v2.jsrq)-days(v2.ksrq)+1) as jhcl_avg
    from tb_hyzl_sjd
    ;
  end for lp2;
  
  --入目标表
  delete from yyzy.t_yyzy_rscjhb_whb as e where exists (select 1 from session.t_yyzy_rscjhb_whb where pfphdm = e.pfphdm);
  insert into yyzy.t_yyzy_rscjhb_whb(pfphdm, ksrq, jsrq , jhcl_avg, jhpc_avg)
  select pfphdm, ksrq, jsrq , jhcl_avg, jhpc_avg
  from session.t_yyzy_rscjhb_whb;
  
END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJH_GP(date) IS '日生产计划 梗丝膨胀片计算';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_RSCJH_GP (date) TO USER APPUSR;
