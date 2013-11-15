--drop view YYZY.V_YYZY_YSCJH_TQLLL; 
create view YYZY.V_YYZY_YSCJH_TQLLL 
  as 
WITH tb_yjhpc as 
(
  select pfphdm, jhny, sum(jhpc) as jhpc
  from YYZY.T_YYZY_YSCJHPC
  where jhny>='2013-10-01' 
  group by pfphdm, jhny
)
, tb_sjtl as (
  select date(to_date(char(year(TLSJ)*100+month(tlsj)),'YYYYMM')) as jhny, 
    coalesce(f.PFPHDM, m.pfphdm) as pfphdm, 
    sum(PHSCPC * coalesce(f.bl,1)) as phscpc
  from YYZY.T_YYZY_SJTL_SCPC as m
    inner join (
      select pfphdm, fzphdm, bl
      from (
          select PFPHDM, FZPHDM, BL, rownumber()over(partition by PFPHDM order by bl) as xh 
          from YYZY.T_YYZY_FZJG_PHB 
          where jsrq>=current_date
        ) as t
      where xh = 1 
    ) as f 
      on m.pfphdm = f.FZPHDM 
  where date(tlsj)>=(select date(max(CSZ)) from YYZY.T_YYZY_STCS where csmc = 'ZSPFFSQSRQ') 
  group by year(TLSJ), month(tlsj), coalesce(f.PFPHDM, m.pfphdm) 
  union all 
  select date(to_date(char(year(TLSJ)*100+month(tlsj)),'YYYYMM')) as jhny, 
    m.pfphdm,  sum(PHSCPC) as phscpc 
  from YYZY.T_YYZY_SJTL_SCPC as m 
  where date(tlsj)>=(select date(max(CSZ)) from YYZY.T_YYZY_STCS where csmc = 'ZSPFFSQSRQ') 
  group by year(TLSJ), month(tlsj), m.pfphdm 
)
, tb_sjtl_hz as ( 
  select pfphdm, jhny, sum(phscpc) as phscpc 
  from tb_sjtl 
  group by pfphdm, jhny 
) 
, tb_cb(pfphdm, jhny, jhpc) as (
  select pfphdm, jhny, -1*jhpc 
  from tb_yjhpc 
  union all 
  select pfphdm, jhny, phscpc 
  from tb_sjtl_hz 
  union all 
  select pfphdm, jhny + 1 month , tqllpc 
  from YYZY.T_YYZY_YSCJH_TQLLL 
) 
select pfphdm, jhny, sum(jhpc) as TQLLPC
from tb_cb as m
where jhny>='2013-10-01' 
  and not exists(select 1 from YYZY.T_YYZY_FZJG_PHB where fzphdm = m.pfphdm)
  and not exists(select 1 from YYZY.T_YYZY_PFPH_CFG where pfphdm = m.pfphdm and gpbj in('1','2'))
group by pfphdm, jhny
having sum(jhpc)>0
;

--delete from YYZY.T_YYZY_YSCJH_TQLLL where jhnf = 2013 and jhyf = 9;
--insert into YYZY.T_YYZY_YSCJH_TQLLL(PFPHDM, JHNF, JHYF, TQLLPC)
--values
--(1,2013,09,1),
--(2,2013,09,2),
--(3,2013,09,1),
--(7,2013,09,34),
--(9,2013,09,2),
--(18,2013,09,3)
--;

--  delete from YYZY.T_YYZY_YSCJH_TQLLL where jhny = '2013-10-01';
--  insert into YYZY.T_YYZY_YSCJH_TQLLL(PFPHDM, JHNY, TQLLPC)
--  select * 
--  from YYZY.V_YYZY_YSCJH_TQLLL 
--  where jhny = '2013-10-01' 
--  ; 
