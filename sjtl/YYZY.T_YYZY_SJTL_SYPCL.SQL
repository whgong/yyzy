drop table YYZY.T_YYZY_SJTL_SYPCL;
CREATE TABLE YYZY.T_YYZY_SJTL_SYPCL
( 
  YWQSRQ DATE not null default CURRENT_DATE,
  PFPHDM INTEGER not null ,
  ZYBJ CHARACTER not null default '1',
  SYPC DECIMAL(10,2) not null ,
  YCLPC DECIMAL(10,2) not null default 0,
  GXSJ TIMESTAMP not null default CURRENT_TIMESTAMP,
  GXJL VARCHAR(500) not null default ''
) IN ts_reg_16k
; 

ALTER TABLE YYZY.T_YYZY_SJTL_SYPCL
  ADD CONSTRAINT PK_T_YYZY_SJTL_SYPCL PRIMARY KEY(YWQSRQ,PFPHDM,ZYBJ);

COMMENT ON TABLE YYZY.T_YYZY_SJTL_SYPCL IS '烟叶资源 实际投料 剩余未使用批次量表';

COMMENT ON YYZY.T_YYZY_SJTL_SYPCL
(
  YWQSRQ is '业务起始时间',
  PFPHDM is '配方牌号代码',
  ZYBJ is '在用标记',
  SYPC is '总剩余批次',
  YCLPC is '已处理批次',
  GXSJ is '开始日期',
  GXJL is '更新记录'
) ;

runstats on table YYZY.T_YYZY_SJTL_SYPCL on all columns and indexes all;

/*测试数据
insert into YYZY.T_YYZY_SJTL_SYPCL(YWQSRQ, PFPHDM, SYPC, GXJL)
values('2013-09-11',3,50,trim(char(date(current_date)))||'add 50;'),
('2013-09-11',2,3,trim(char(date(current_date)))||'add 3;'),
('2013-09-08',6,1,trim(char(date(current_date)))||'add 1;'),
('2013-09-15',7,1,trim(char(date(current_date)))||'add 1;')
;
*/