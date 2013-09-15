drop view YYZY.V_YYZY_YYKC_KYKC;

create view YYZY.V_YYZY_YYKC_KYKC
(
  yydm, yymc, yynf, kclx, yykcjs, kcsl
)
 as 
with sykc_tmp as (
  select YYDM, YYNF, YYKCJS, KCLX
  from YYZY.V_YYZY_YYKC_WHB
  union all
  select YYDM, YYNF, -1*YYFPL as yykcjs, KCLX
  from YYZY.T_YYZY_ZXPF_SDB
  where yydm<>'0'
)
, sykc as 
(
  select yydm, yynf, kclx, sum(yykcjs) as yykcjs
  from sykc_tmp
  group by yydm, yynf, kclx
  --having sum(yykcjs)>=0
)
select m.yydm, y.yymc, m.yynf, m.kclx, m.yykcjs, 0 as kcsl
from sykc as m
left join DIM.T_DIM_YYZY_YYZDB as y
  on m.yydm = y.yydm
  and y.jsrq>=current_date
;

comment on table YYZY.V_YYZY_YYKC_KYKC is '实际可用库存(最新库存 - 配方锁定量)';

comment on YYZY.V_YYZY_YYKC_KYKC(
  yydm is '烟叶代码',
  yymc is '烟叶名称',
  yynf is '烟叶年份',
  kclx is '库存类型',
  yykcjs is '库存件数'
)
;

