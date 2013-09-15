
SET SCHEMA ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR;

create table ys.t_ys_pfph_pzbzdm (
  pfphdm smallint,
  jgxsmc varchar(50),
  pzbzmc varchar(100),
  ksrq date,
  jsrq date
) in ts_reg_16k
;

DROP VIEW YYZY.V_YYZY_PHCL;
CREATE VIEW YYZY.V_YYZY_PHCL
  (
    NF,
    PFPHDM,
    SCSL
  )
  AS 

with tb_sc_pz as (
select dqrq, jgxsmc, pzbzmc, sum(cl) as cl
from (
select DQRQ, PZBZ_LSBH, GJSL as cl
from JXC.N_FT_JXC_GYGJ
union all
select DQRQ, PZBZ_LSBH, JYSCSL as cl
from JXC.N_FT_JXC_GYSC
) as c
  left join DDS_DIM.N_DIM_PZBZBM as p
    on c.PZBZ_LSBH = p.lsbh
group by dqrq, jgxsmc, pzbzmc
)
, tb_sc_ph as (
  select year(dqrq) as nf,c.pfphdm, sum(m.cl) as scsl
  from tb_sc_pz as m
    left join YS.T_YS_PFPH_PZBZDM as c
    on m.pzbzmc = c.pzbzmc 
    and m.jgxsmc = c.jgxsmc
  where pfphdm>0
  group by year(dqrq),c.pfphdm
)
select nf, pfphdm, scsl
from tb_sc_ph
;
  /*
  with 
  scsl_tmp as (
    select dqrq,PZBZ_LSBH, GC_LSBH, JYSCSL as scsl
      from JXC.N_FT_JXC_GYSC
    union all
    select dqrq,PZBZ_LSBH, 77 as GC_LSBH, GJSL as scsl
      from JXC.N_FT_JXC_GYGJ
      where pzbz_lsbh= 3906 
  ) , 
  scsl_tmp2 as 
  (
    select year(dqrq) as nf,thtxbs,pzbzmc,gcmc,sum(scsl) as scsl
    from scsl_tmp as a
      left join DIM.T_DIM_GC as b
      on a.GC_LSBH=b.lsbh
      left join DIM.T_DIM_PZBZBM as c
      on a.PZBZ_LSBH = c.lsbh
    group by year(dqrq),gcmc,thtxbs,pzbzmc 
  ) 
  , 
  scsl_ph as (
  select
      case
        when gcmc like '%北京%'
          then 0
        when gcmc like '%阜阳%'
          and pzbzmc like '%红双喜%'
          then 52
        when gcmc like '%天津%'
          then
            case
              when pzbzmc like '%红双喜%百顺%'
                then 49
              when pzbzmc like '%硬盒%红双喜%'
                then 47
              when pzbzmc like '%软盒%红双喜%'
                then 47
              when pzbzmc like '%硬盒%大前门%'
                then 51
              when pzbzmc like '%软盒%大前门%'
                then 50
              else 0
            end
        when gcmc like '%高扬%'
          then
            case
              when pzbzmc like '%红双喜%8mg%'
                then 7
              when pzbzmc like '%牡丹%'
                then 19
              when pzbzmc like '%金鹿%'
                then
                  case
                    when pzbzmc like '%5mg%'
                      then 39
                    when pzbzmc like '%8mg%'
                      then 15
                    when pzbzmc like '%10mg%'
                      then 40
                    when pzbzmc like '%12mg%'
                      then 41
                    when pzbzmc like '%13mg%'
                      then 46
                    else 0
                  end
              else 0
            end
        else
          case
            when pzbzmc like '%熊猫%时代%'
              then 12
            when pzbzmc like '%熊猫%礼盒%'
              then 11
            when pzbzmc like '%中华%10mg%'
              then 6
            when pzbzmc like '%中华%5000%'
              then 32
            when pzbzmc like '%中华%全开%'
              then 25
            when pzbzmc like '%硬%中华%'
              then 3
            when pzbzmc like '%软%中华%'
              then 9
            when pzbzmc like '%红双喜%百顺%'
              then 22
            when pzbzmc like '%红双喜%10mg%'
              then 4
            when pzbzmc like '%红双喜%'
              and pzbzmc like '%特%'
              then 4
            when pzbzmc like '%红双喜%'
              and pzbzmc like '%精%'
              then 8
            when pzbzmc like '%红双喜%'
              and pzbzmc like '%晶派%'
              then 54
            when pzbzmc like '%硬%红双喜%'
              then 2
            when pzbzmc like '%软%红双喜%'
              then 2
            when pzbzmc like '%硬%大前门%'
              then 45
            when pzbzmc like '%软%大前门%'
              then 1
            when pzbzmc like '%上海%'
              then 20
            when pzbzmc like '%牡丹%'
              then 18
            when pzbzmc like '%孟菲斯%'
              then 13
            else 0
          end
      end as pfphdm, nf, pzbzmc, thtxbs, gcmc, scsl
    from scsl_tmp2 ) select nf,pfphdm,sum(scsl) as scsl
  from scsl_ph
  where pfphdm<>0
  group by nf,pfphdm
  ;
  */

GRANT CONTROL ON TABLE YYZY.V_YYZY_PHCL TO USER ETLUSR;