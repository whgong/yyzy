--drop PROCEDURE YYZY.P_YYZY_SDPF_JSGX;
SET SCHEMA = ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

CREATE PROCEDURE YYZY.P_YYZY_SDPF_JSGX
( 
--  IN  IP_I_NF INTEGER,
--  OUT OP_V_ERR_MSG VARCHAR(1000) 
)
  SPECIFIC PROC_YYZY_SDPF_JSGX
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

  /* DECLARE STATIC CURSOR */
  -- DECLARE C1 CURSOR /*WITH RETURN*/ FOR
  --   SELECT DISTINCT NAME, CREATOR, TYPE
  --   FROM SYSIBM.SYSTABLES
  --   ORDER BY TYPE,CREATOR,NAME
  -- ;
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
--  DECLARE UNDO HANDLER FOR SQLEXCEPTION
--  BEGIN 
--    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
--    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
--      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
--      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
--    ; 
--  END; 
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
  DECLARE GLOBAL TEMPORARY TABLE scpcxl_n(
    pfphdm INTEGER,
    ksrq date,
    jsrq date,
    jhcl_avg decimal(18,6),
    jhpc_avg decimal(18,6),
    pcs_ks INTEGER,
    pcs_js INTEGER
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE scpcxl_o(
    pfphdm INTEGER,
    ksrq date,
    jsrq date,
    jhcl_avg decimal(18,6),
    jhpc_avg decimal(18,6),
    pcs_ks INTEGER,
    pcs_js INTEGER
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE sjfw(
    pfphdm INTEGER,
    ksrq date,
    jsrq date,
    jspc INTEGER
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE sjfw_o( 
    pfphdm INTEGER, 
    ksrq date, 
    jsrq date, 
    jspc INTEGER 
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE jsbhd(
    pfphdm integer,
    rq_o date,
    pc_o integer,
    rq_n date,
    pc_n integer
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE T_YYZY_RSCJHB_WHB like YYZY.T_YYZY_RSCJHB_WHB 
    with replace on commit preserve rows not logged
  ; 
  
  /* SQL PROCEDURE BODY */
  delete from session.scpcxl_o;
  delete from session.scpcxl_n;
  delete from session.sjfw;
  delete from session.sjfw_o;
  delete from session.jsbhd;
  delete from session.T_YYZY_RSCJHB_WHB;
-------------------------------------------------------------------------------------------
  --��üƻ�����ǰ��������
  insert into session.scpcxl_o(pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, pcs_ks, pcs_js)
  with tb_scpc as (
    select PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, (days(jsrq)-days(ksrq)+1)*jhpc_avg as pczs
    from YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI
    where jhpc_avg is not null
      and jhpc_avg<>0
    order by pfphdm, ksrq,jsrq
  )
  , tb_scpc_js as (
    select pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
      (select sum(pczs) from tb_scpc where pfphdm = m.pfphdm and jsrq<=m.jsrq) as pczs_js
    from tb_scpc as m
  )
  , tb_scpc_ks as (
    select pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
      lag(pczs_js,1,0)over(partition by pfphdm order by ksrq,jsrq)+1 as pczs_ks ,pczs_js
    from tb_scpc_js
  )
  , tb_ytlpc as (
    select m.pfphdm ,sum(PHSCPC) as ytlpc
    from YYZY.T_YYZY_SJTL_SCPC as m
      inner join (
        select pfphdm, min(ksrq) from YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI group by pfphdm
      ) as k(pfphdm, ksrq)
        on m.pfphdm = k.pfphdm
    where date(tlsj)<k.ksrq
    group by m.pfphdm
  )
  select m.pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
    pczs_ks + ytlpc as pczs_ks ,pczs_js + ytlpc as pczs_js
  from tb_scpc_ks as m
    left join tb_ytlpc as y
      on m.pfphdm = y.pfphdm
  ;
  
  --��üƻ����º���������
  insert into session.scpcxl_n(pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, pcs_ks, pcs_js)
  with tb_scpc as (
    select PFPHDM, KSRQ, JSRQ, JHCL_AVG, JHPC_AVG, (days(jsrq)-days(ksrq)+1)*jhpc_avg as pczs
    from YYZY.T_YYZY_RSCJHB_WHB
    where pfphdm <>16
      and jhpc_avg<>0
    order by pfphdm, ksrq,jsrq
  )
  , tb_scpc_js as (
    select pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
      (select sum(pczs) from tb_scpc where pfphdm = m.pfphdm and jsrq<=m.jsrq) as pczs_js
    from tb_scpc as m
  )
  , tb_scpc_ks as (
    select pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
      lag(pczs_js,1,0)over(partition by pfphdm order by ksrq,jsrq)+1 as pczs_ks ,pczs_js
    from tb_scpc_js
  )
  , tb_ytlpc as (
    select m.pfphdm ,sum(PHSCPC) as ytlpc
    from YYZY.T_YYZY_SJTL_SCPC as m
      inner join (
        select pfphdm, min(ksrq) from YYZY.T_YYZY_RSCJHB_WHB group by pfphdm
      ) as k(pfphdm, ksrq)
        on m.pfphdm = k.pfphdm
    where date(tlsj)<k.ksrq
    group by m.pfphdm
  )
  select m.pfphdm, ksrq, jsrq, jhcl_avg, jhpc_avg, 
    pczs_ks + ytlpc as pczs_ks ,pczs_js + ytlpc as pczs_js
  from tb_scpc_ks as m
    left join tb_ytlpc as y
      on m.pfphdm = y.pfphdm
  ;
----------------------------------------------------------------------------------
  --����账���ʱ�䷶Χ
  insert into session.sjfw(pfphdm, ksrq, jsrq, jspc)
--  with sdpcs as (
--    select pfphdm, max(jspcs) as jspcs
--    from YYZY.T_YYZY_PFDXXB
--    where (pfphdm, nf, yf, bbh)in(
--        select pfphdm, nf, yf, max(bbh)
--        from YYZY.T_YYZY_PFDXXB
--        group by pfphdm, nf, yf
--      )
--    group by pfphdm
--  )
--  , sdpcs_kspcrq as (
--    select m.pfphdm, m.jspcs, r.ksrq
--    from sdpcs as m
--      left join (
--        select pfphdm, min(ksrq) as ksrq
--        from session.scpcxl_n group by pfphdm
--      ) as r on m.pfphdm = r.pfphdm
--  )
--  select sc.pfphdm, sc.ksrq, 
--    ksrq + ((jspcs - pcs_ks) / jhpc_avg) day as jsrq
--  from session.scpcxl_n as sc
--    inner join sdpcs as sd 
--      on sc.pfphdm = sd.pfphdm
--      and sd.JSPCS between sc.pcs_ks and sc.pcs_js
--  ;
  with sdpcs as (
    select pfphdm, max(jspcs) as jspcs --����������ֵĽ�������
    from YYZY.T_YYZY_PFDXXB
    where (pfphdm, nf, yf, bbh)in(
        select pfphdm, nf, yf, max(bbh)
        from YYZY.T_YYZY_PFDXXB
        group by pfphdm, nf, yf
      )
    group by pfphdm
  )
  , sdpcs_kspcrq as (
    select m.pfphdm, m.jspcs, r.ksrq --�������ֵĿ�ʼ����
    from sdpcs as m
      left join (
        select pfphdm, min(ksrq) as ksrq
        from session.scpcxl_n group by pfphdm
      ) as r on m.pfphdm = r.pfphdm
  )
  select sc.pfphdm, sd.ksrq, 
    sc.ksrq + ((jspcs - pcs_ks) / jhpc_avg) day as jsrq, --�����������κ���������
    sd.jspcs as jspc
  from session.scpcxl_n as sc
    inner join sdpcs_kspcrq as sd 
      on sc.pfphdm = sd.pfphdm
      and sd.JSPCS between sc.pcs_ks and sc.pcs_js
  order by 1,2,3 
  ;
  
  --��ȡԭʼʱ�䷶Χ
  insert into session.sjfw_o(pfphdm, ksrq, jsrq, jspc)
  with sdpcs as (
    select pfphdm, max(jspcs) as jspcs
    from YYZY.T_YYZY_PFDXXB
    where (pfphdm, nf, yf, bbh)in(
        select pfphdm, nf, yf, max(bbh)
        from YYZY.T_YYZY_PFDXXB
        group by pfphdm, nf, yf
      )
    group by pfphdm
  )
  , sdpcs_kspcrq as (
    select m.pfphdm, m.jspcs, r.ksrq
    from sdpcs as m
      left join (
        select pfphdm, min(ksrq) as ksrq
        from session.scpcxl_o group by pfphdm
      ) as r on m.pfphdm = r.pfphdm
  )
  select sc.pfphdm, sd.ksrq, 
    sc.ksrq + ((jspcs - pcs_ks) / jhpc_avg) day as jsrq, sd.jspcs as jspc
  from session.scpcxl_o as sc
    inner join sdpcs_kspcrq as sd 
      on sc.pfphdm = sd.pfphdm
      and sd.JSPCS between sc.pcs_ks and sc.pcs_js
  order by 1,2,3 
  ;
--  with sdpcs as (
--    select LSBH, PFPHDM, QSPCS, JSPCS, BBH, SFXG, NF, YF, SFFS
--    from YYZY.T_YYZY_PFDXXB
--    where (pfphdm, nf*100+yf, bbh)in(
--        select pfphdm, max(nf*100+yf), max(bbh)
--        from YYZY.T_YYZY_PFDXXB
--        where (pfphdm, nf*100+yf)in(
--            select pfphdm, max(nf*100+yf)
--            from YYZY.T_YYZY_PFDXXB
--            group by pfphdm
--          )
--        group by pfphdm
--      )
--  )
--  select sc.pfphdm, sc.ksrq, 
--    ksrq + ((jspcs - pcs_ks) / jhpc_avg) day as jsrq
--  from session.scpcxl_o as sc
--    inner join sdpcs as sd 
--      on sc.pfphdm = sd.pfphdm
--      and sd.JSPCS between sc.pcs_ks and sc.pcs_js
--  ;

---------------------------------------------------------------------------------
  --��ȡ��ɫ�仯����Ϣ
  insert into session.jsbhd(pfphdm, rq_o, pc_o, rq_n, pc_n)
  with jstz_bhrq as ( --��ɫ�仯��ʼ����
    select DISTINCT pfphdm, KSRQ as bhrq --������ʼ���ڷ�Χ�ڵĽ�ɫ�仯��
    from YYZY.T_YYZY_JSTZ_WHB as m
    where exists (
          select 1
          from session.sjfw
          where pfphdm = m.pfphdm 
            and m.ksrq between ksrq and jsrq
        ) 
      and zybj = '1' 
  )
  , jstz_yspc as ( --��ɫ�仯��ʼ���� -> ��ɫԭʼ�仯���κ�
    select m.pfphdm, m.bhrq, pcs_ks + jhpc_avg*(days(m.bhrq - 1 day)-days(ksrq)+1) as bhpc
    from jstz_bhrq as m
      left join session.scpcxl_o as c
        on m.pfphdm = c.pfphdm 
        and m.bhrq between c.ksrq and c.jsrq
  )
  , jstz_gxrq as ( --ԭʼ�仯���κ� -> �ƻ��������ɫ�仯���κŶ�Ӧ����
    select m.pfphdm, m.bhrq as bhrq_o, m.bhpc, 
      ksrq + int((m.bhpc - pcs_ks)/jhpc_avg) day as bhrq_n
    from jstz_yspc as m
      left join session.scpcxl_n as c
        on m.pfphdm = c.pfphdm 
        and m.bhpc between c.pcs_ks and c.pcs_js
  )
  , jstz_gxrq1 as ( --������Χ�Ĳ���ͳһ�ƶ�����Χ�߽�
    select m.pfphdm, bhrq_o, bhpc, 
      (case when bhrq_n>=f.jsrq then f.jsrq else bhrq_n end) as bhrq_n 
    from jstz_gxrq as m 
      left join session.sjfw as f 
        on m.pfphdm = f.pfphdm 
  ) 
  -- �ƻ��������ɫ�仯���ε��Ӧ���� -> ������ɫ�����κ�
  select m.pfphdm, m.bhrq_o, m.bhpc as bhpc_o, m.bhrq_n,
    pcs_ks + jhpc_avg*(days(m.bhrq_n - 1 day)-days(ksrq)+1) as bhpc_n
  from jstz_gxrq1 as m
    left join session.scpcxl_n as c
      on m.pfphdm = c.pfphdm
      and m.bhrq_n between c.ksrq and c.jsrq
  ;
  
  delete from session.jsbhd where rq_o = rq_n and pc_o = pc_n;
  
----------------------------------------------------------------------------'
  --?���Ϊ�����汾�ķ�ʽ
  --���½�ɫ
  --����������, ����ԭ��Χ�ı仯��ͳһ����Ϊ������κŵ�ʱ��
  lp1:
  for v1 as c1 cursor for
      select m.pfphdm, m.rq_o, m.rq_n, 
        f1.ksrq as ksrqo, f1.jsrq as jsrqo,
        f2.ksrq as ksrqn, f2.jsrq as jsrqn
      from session.jsbhd as m
        inner join session.sjfw_o as f1
          on m.pfphdm = f1.pfphdm
        inner join session.sjfw as f2
          on m.pfphdm = f2.pfphdm
  do
    if v1.rq_o <= jsrqo then 
      update YYZY.T_YYZY_JSTZ_WHB as tgt 
      set ksrq = v1.rq_n 
      where pfphdm = v1.pfphdm and ksrq = rq_o 
      ;
      update YYZY.T_YYZY_JSTZ_WHB as tgt 
      set jsrq = v1.rq_n - 1 day 
      where pfphdm = v1.pfphdm and jsrq = rq_o - 1 day 
      ;
    else 
--      delete from YYZY.T_YYZY_JSTZ_WHB 
--      where pfphdm = v1.pfphdm and ksrq>=v1.jsrqo and jsrq<=v1.jsrqn 
--      ;
      update YYZY.T_YYZY_JSTZ_WHB as tgt
      set ksrq = v1.jsrqn + 1 day
      where pfphdm = v1.pfphdm and ksrq between v1.jsrqo + 1 day and v1.jsrqn 
      ;
      update YYZY.T_YYZY_JSTZ_WHB as tgt
      set jsrq = v1.jsrqn 
      where pfphdm = v1.pfphdm and jsrq between v1.jsrqo + 1 day and v1.jsrqn 
      ;
    end if;
  end for lp1;
  
  delete from YYZY.T_YYZY_JSTZ_WHB where jsrq<ksrq;
  
/*  
  update YYZY.T_YYZY_JSTZ_WHB as tgt  --��ʼ���ڸ���
  set ksrq = (select rq_n from session.jsbhd where pfphdm = tgt.pfphdm and rq_o = tgt.ksrq)
  where exists(
        select 1 from session.jsbhd as c
        where pfphdm = tgt.pfphdm and rq_o = tgt.ksrq 
          and exists(
            select 1
            from session.sjfw_o
            where pfphdm = m.pfphdm
              and c.rq_o between ksrq and jsrq
          )
      ) 
  ;
  update YYZY.T_YYZY_JSTZ_WHB as tgt  --�������ڸ���
  set jsrq = (select (rq_n - 1 day) from session.jsbhd where pfphdm = tgt.pfphdm and rq_o = tgt.jsrq + 1 day)
  where exists(
        select 1 from session.jsbhd as c
        where pfphdm = tgt.pfphdm and rq_o = tgt.jsrq + 1 day
          and exists(
            select 1
            from session.sjfw_o
            where pfphdm = m.pfphdm
              and c.rq_o between ksrq and jsrq
          )
      ) 
  ;
  */
----------------------------------------------------------------------------
  --�����쳣������, ͬ���ƺ��г����˲�ͬ���εĽ�ɫ�仯����Ϊͬһ��
  if (select count(*) from session.jsbhd group by pfphdm, rq_n having count(pc_n)>1)>0 then
    SIGNAL SQLSTATE '99999' 
      SET MESSAGE_TEXT = 'exceptional situation occured!';
  end if;
  
  --���ݽ�ɫ�仯����¼ƻ�����
  insert into session.T_YYZY_RSCJHB_WHB(pfphdm, ksrq,jsrq,jhpc_avg)
  with 
  tb_jsbhd as (
    select m.pfphdm, rq_o, pc_o, rq_n, pc_n 
    from session.jsbhd as m 
      left join session.sjfw_o as f 
        on m.pfphdm = f.pfphdm 
    where pc_o <= f.jspc + 1
--    union all
--    select distinct m.pfphdm, f.jsrq as rq_o, f.jspc as pc_o, rq_n, pc_n
--    from session.jsbhd as m
--      left join session.sjfw_o as f 
--        on m.pfphdm = f.pfphdm 
--    where rq_o > f.jsrq
  )
  , scjh as (
    select m.PFPHDM, m.ksrq, m.jsrq, JHPC_AVG as jhpc
    from YYZY.T_YYZY_RSCJHB_WHB as m
    where exists(
          select 1 from tb_jsbhd
          where pfphdm = m.pfphdm
        )
      and exists (
          select 1 from session.sjfw
          where pfphdm = m.pfphdm 
            and (ksrq<=m.jsrq and jsrq>=m.ksrq)
        )
    order by m.pfphdm,m.ksrq, m.jsrq
  )
  , jh_r as (
    select p.pfphdm, c.riqi as jhrq, value(m.jhpc,0) as jhpc
    from (select distinct pfphdm from scjh) as p
      inner join DIM.T_DIM_YYZY_DATE as c on 1=1
      inner join (select pfphdm,min(ksrq) as ksrq,max(jsrq) as jsrq from scjh group by pfphdm) as s 
        on c.riqi between s.ksrq and s.jsrq and p.pfphdm = s.pfphdm
      left join scjh as m on p.pfphdm = m.pfphdm
        and  c.riqi between m.ksrq and m.jsrq
  )
  , jh_r_xz as (
    select m.pfphdm, m.jhrq, 
      ( m.jhpc 
        - (value(c.pc_o,0) - value(c.pc_n,0)) 
        + (value(c1.pc_o,0) - value(c1.pc_n,0))
      ) as jhpc, 
      rownumber()over(partition by m.pfphdm order by m.jhrq) as xh
    from jh_r as m
      left join tb_jsbhd as c
        on m.pfphdm = c.pfphdm and c.rq_n = m.jhrq
      left join tb_jsbhd as c1
        on m.pfphdm = c1.pfphdm and c1.rq_n - 1 day = m.jhrq
  )
  , jh_r_cir(pfphdm, jhrq, jhpc, xh, xh1) as (
    select pfphdm, jhrq, jhpc, xh, 1 as xh1
    from jh_r_xz
    where xh = 1
    union all
    select a.pfphdm, a.jhrq, a.jhpc, a.xh, 
      (case when a.jhpc = b.jhpc then b.xh1 else a.xh end) as xh1
    from jh_r_xz as a,jh_r_cir as b
    where b.xh = a.xh-1
      and a.pfphdm = b.pfphdm
  )
  select pfphdm, min(jhrq) as ksrq, max(jhrq) as jsrq, jhpc
  from jh_r_cir
  group by pfphdm, jhpc, xh1
  ;

  --�������θ��²���
  update session.T_YYZY_RSCJHB_WHB as m
  set 
    jhcl_avg = (
        SELECT  DPCL FROM YYZY.T_YYZY_DPCLB as d
        WHERE (PFPHDM,NY)IN(SELECT  PFPHDM, MAX( NY) FROM YYZY.T_YYZY_DPCLB  GROUP BY PFPHDM )
          AND d.PFPHDM=m.PFPHDM
      ) * JHPC_AVG
  where pfphdm in (
      SELECT pfphdm FROM YYZY.T_YYZY_DPCLB as d1
      WHERE (PFPHDM,NY)IN(SELECT PFPHDM, MAX( NY) FROM YYZY.T_YYZY_DPCLB GROUP BY PFPHDM )
        AND d1.PFPHDM=m.PFPHDM
    )
  ;
  --���°汾����
  update session.T_YYZY_RSCJHB_WHB as m
  set bbrq = (select max(bbrq) from YYZY.T_YYZY_RSCJHB_WHB)
  ;
  
  --�������
  delete from YYZY.T_YYZY_RSCJHB_WHB as m
  where exists(
        select 1 from session.jsbhd as j
        where pfphdm = m.pfphdm
          and pc_o <= (select jspc+1 from session.sjfw where pfphdm = j.pfphdm)
      )
    and exists (
        select 1 from session.sjfw
        where pfphdm = m.pfphdm 
          and (ksrq<=m.jsrq and jsrq>=m.ksrq)
      )
  ;
  insert into YYZY.T_YYZY_RSCJHB_WHB
  select * from session.T_YYZY_RSCJHB_WHB
  ;

END LB_MAIN;

COMMENT ON PROCEDURE YYZY.P_YYZY_SDPF_JSGX() IS '�����䷽��ɫ����';

GRANT EXECUTE ON PROCEDURE YYZY.P_YYZY_SDPF_JSGX () TO USER APPUSR;
