-- drop table YYZY.T_YYZY_YSCJH_TQLLL; 
create table YYZY.T_YYZY_YSCJH_TQLLL( 
  pfphdm integer not null, 
  jhny date not null,
  tqllpc integer, 
  CONSTRAINT pk_YSCJH_TQLLL PRIMARY KEY(pfphdm,jhny)
) 
in ts_reg_16k; 

comment on table YYZY.T_YYZY_YSCJH_TQLLL is '月生产计划 提前领料量';

comment on YYZY.T_YYZY_YSCJH_TQLLL( 
  pfphdm is '配方牌号代码', 
  jhny is '计划年月',
  tqllpc is '提前领料批次' 
) 
; 
