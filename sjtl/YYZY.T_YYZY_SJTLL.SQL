alter table YYZY.T_YYZY_SJTL_YYTL drop foreign key FK_SJTL_ZC;
drop index YYZY.IDX_SJTLPC;

drop table YYZY.T_YYZY_SJTL_SCPC;
create table YYZY.T_YYZY_SJTL_SCPC
(
  SJTLZBDM BIGINT not null GENERATED ALWAYS
    AS IDENTITY (START WITH 1, INCREMENT BY 1, CACHE 20, MINVALUE 1, MAXVALUE 9999999, NO CYCLE, NO ORDER),
  TLSJ TIMESTAMP not null,
  PFPHDM INTEGER not null,
  PHSCPC DECIMAL(18,6),
  constraint PK_SJTL_SCPC primary key (SJTLZBDM)
)
in ts_reg_16k
;

comment on table YYZY.T_YYZY_SJTL_SCPC is '实际投料数据 生产批次';

comment on YYZY.T_YYZY_SJTL_SCPC(
   TLSJ is '投料时间',
   PFPHDM is '配方牌号代码',
   PHSCPC is '生产批次'
)
;

create unique index YYZY.IDX_SJTLPC on YYZY.T_YYZY_SJTL_SCPC (
  TLSJ desc, PFPHDM asc
);

--------------------------------------------------------------------
drop index YYZY.IDX_SJTLYY;
drop table YYZY.T_YYZY_SJTL_YYTL;

create table YYZY.T_YYZY_SJTL_YYTL
(
  SJTLZBDM BIGINT not null,
  TLSJ TIMESTAMP not null,
  PFPHDM INTEGER not null,
  YYDM VARCHAR(20) not null,
  YYPC VARCHAR(10) not null,
  TLSL DECIMAL(18,6)
)
in ts_reg_16k
;

comment on table YYZY.T_YYZY_SJTL_YYTL is
'实际投料 烟叶投料';

comment on YYZY.T_YYZY_SJTL_YYTL(
   TLSJ is '投料时间',
   PFPHDM is '配方牌号代码',
   YYDM is '烟叶代码',
   YYPC is '烟叶批次',
   TLSL is '投料数量'
)
;

create unique index YYZY.IDX_SJTLYY on YYZY.T_YYZY_SJTL_YYTL (
  SJTLZBDM desc, TLSJ desc, PFPHDM asc, YYDM asc, yypc asc
);

alter table YYZY.T_YYZY_SJTL_YYTL
   add constraint FK_SJTL_ZC foreign key (SJTLZBDM)
      references YYZY.T_YYZY_SJTL_SCPC (SJTLZBDM)
      on delete restrict on update restrict;

--------------------------------------------------------------------
drop table YYZY.T_YYZY_SJTL_YEAR;
create table YYZY.T_YYZY_SJTL_YEAR
(
   NF INTEGER ,
   PFPHDM INTEGER not null,
   YTLPC DECIMAL(18,6),
   constraint P_SJTL_YEAR primary key (PFPHDM)
)
in ts_reg_16k
;

comment on table YYZY.T_YYZY_SJTL_YEAR is
'本年投料批次';

comment on YYZY.T_YYZY_SJTL_YEAR(
  nf is '年份',
  PFPHDM is '配方牌号代码',
  YTLPC is '已投料数量'
)
;

runstats on table YYZY.T_YYZY_SJTL_YEAR on all columns and indexes all;
runstats on table YYZY.T_YYZY_SJTL_SCPC on all columns and indexes all;
runstats on table YYZY.T_YYZY_SJTL_YYTL on all columns and indexes all;