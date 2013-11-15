--drop table YYZY.T_YYZY_YSCJHPC; 
create table YYZY.T_YYZY_YSCJHPC 
( 
  pfphdm integer not null, 
  jhny date not null, 
  jhpc decimal(18,6), 
  jhlx character(1) not null, 
  CONSTRAINT pk_YYZY_YSCJHPC PRIMARY KEY(pfphdm,jhny,jhlx) 
) 
in ts_reg_16k index in ts_idx_16k
;

comment on table YYZY.T_YYZY_YSCJHPC is '月生产计划批次'; 

comment on YYZY.T_YYZY_YSCJHPC( 
  pfphdm is '配方牌号代码', 
  jhny is '计划年月', 
  jhpc is '计划批次', 
  jhlx is '计划类型(1常规;2外加工本地制丝;3分组加工;4外加工)' 
) 
; 

runstats on table YYZY.T_YYZY_YSCJHPC on all columns and indexes all;
