drop view YYZY.V_YYZY_YYKC_KYKC7;

create view YYZY.V_YYZY_YYKC_KYKC7
(
  yydm, fkpc, yj, kcxs
)
 as 
with sykc_tmp as (
  select yydm, fkpc, yj, kcxs
  from JYHSF.T_JYHSF_KCJYJXX
  union all
  select YYDM, YYPC, cast(null as dec(18,6)) as yj, -1*YYFPL as kcxs
  from JYHSF.T_JYHSF_ZSPF_SDB
  where YYFPL>0 and yydm<>'0'
/*
  select YYDM, YYNF, YYKCJS, KCLX
  from YYZY.V_YYZY_YYKC_WHB
  union all
  select YYDM, YYNF, -1*YYFPL as yykcjs, KCLX
  from YYZY.T_YYZY_ZXPF_SDB
  where yydm<>'0'
  */
)
, sykc as 
(
  select yydm, fkpc, max(yj) as yj, sum(kcxs) as kcxs
  from sykc_tmp
  group by yydm, fkpc
  --having sum(yykcjs)>=0
)
select yydm, fkpc, yj, kcxs
from sykc
;

comment on table YYZY.V_YYZY_YYKC_KYKC7 is '实际可用库存(最新库存 - 配方锁定量) 7要素';

comment on YYZY.V_YYZY_YYKC_KYKC7(
  yydm is '烟叶代码',
  fkpc is '复烤批次',
  yj is '烟碱',
  kcxs is '库存箱数'
)
;

