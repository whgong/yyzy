drop PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR; 

SET SCHEMA ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR; 

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR 
 (IN D_START_DATE DATE, 
  IN D_END_DATE DATE, 
  IN IN_BJ INTEGER 
 ) 
  SPECIFIC APP_YYB.SQL091110163410700 
  LANGUAGE SQL 
  NOT DETERMINISTIC 
  CALLED ON NULL INPUT 
  EXTERNAL ACTION 
  OLD SAVEPOINT LEVEL 
  MODIFIES SQL DATA 
  INHERIT SPECIAL REGISTERS 
  
  --开始存储过程
  BEGIN 
  
    DECLARE GLOBAL TEMPORARY TABLE t_yyzy_tmp_yyxhtjb like yyzy.t_yyzy_tmp_yyxhtjb 
      with replace on commit preserve rows not logged
    ;

    DECLARE GLOBAL TEMPORARY TABLE T_YYZY_ZXPF_WHB like YYZY.T_YYZY_ZXPF_WHB 
      with replace on commit preserve rows not logged
    ;

    --得到每天每个牌号角色的分配量
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
            when a.pfphdm =16 then jhcl_avg*DPXS/100 
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
    ) t 
    order by pfphdm,JSDM,(case when ad1<bd1 then bd1 else ad1 end); 

    --剔除配方发送锁定部分的计划
    insert into session.t_yyzy_tmp_yyxhtjb
    with lastpf as 
    (
      select m.pfphdm, m.jsdm, m.ksrq, m.jsrq, jssyl
      from YYZY.T_YYZY_ZXPF_SDB as m
      where (m.pfphdm,m.jsdm,m.tdsx)in(
          select pfphdm,jsdm,max(tdsx) as tdsx
          from YYZY.T_YYZY_ZXPF_SDB
          group by pfphdm,jsdm
        )
    )
    select m.pfphdm, m.jsdm, p.jsrq+1 day as ksrq, m.jsrq, m.hl_d
    from YYZY.T_YYZY_TMP_YYXHTJB as m
      inner join lastpf as p on m.pfphdm = p.pfphdm and m.jsdm = p.jsdm
        and p.jsrq between m.ksrq and m.jsrq
    where p.jsrq+1 day <=m.jsrq
    union all
    select m.pfphdm, m.jsdm, p.jsrq as ksrq, p.jsrq as ksrq, m.hl_d - p.jssyl as hl_d
    from YYZY.T_YYZY_TMP_YYXHTJB as m
      inner join lastpf as p on m.pfphdm = p.pfphdm and m.jsdm = p.jsdm
        and p.jsrq between m.ksrq and m.jsrq
    where p.jssyl < m.hl_d 
    union all
    select m.PFPHDM, m.JSDM, m.KSRQ, m.JSRQ, m.HL_D
    from YYZY.T_YYZY_TMP_YYXHTJB as m
      inner join lastpf as p on m.pfphdm = p.pfphdm and m.jsdm = p.jsdm
        and m.ksrq>p.jsrq
    union all
    select pfphdm, jsdm, ksrq, jsrq, hl_d
    from YYZY.T_YYZY_TMP_YYXHTJB as m
    where pfphdm not in(
      select pfphdm from lastpf
    )
    order by 1,2,3,4
    ;

    delete from YYZY.T_YYZY_TMP_YYXHTJB;
    insert into YYZY.T_YYZY_TMP_YYXHTJB
    select * from session.t_yyzy_tmp_yyxhtjb
    ;

    --取得最新库存(3种库存)
    delete from yyzy.t_yyzy_tmp_yykc;
    insert into yyzy.t_yyzy_tmp_yykc(yydm,yynf,yykcjs,kclx)
    select yydm,yynf,yykcjs,kclx 
--    from yyzy.v_yyzy_yykc_whb; 
    from YYZY.V_YYZY_YYKC_KYKC --可用库存 = 实际库存 - 已发送锁定库存
    ;

    --获得烟叶的替代规则
    --获得烟叶使用规则的全部烟叶
    delete from yyzy.t_yyzy_tmp_yyfpgzb;
    insert into yyzy.t_yyzy_tmp_yyfpgzb(pfphdm,jsdm,yydm,yynf,zlyybj,zpfbj,sdbh,sdfs,sysx,kclx)
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,value(b.sdbh,0) as sdbh,coalesce(b.sdfs,'0') as sdfs,tdsx,kclx
    from yyzy.t_yyzy_zxpf_whb a  
    left join yyzy.t_yyzy_yysdb b 
    on a.sdbh=b.sdbh 
    where a.jsrq>=D_START_DATE and tdsx>1
    ;

    --正常方式不记载采购计划烟叶 
    /*
    if in_bj=1 
      then delete from yyzy.t_yyzy_zxpf_whb where yynf=year(D_START_DATE); 
    end if; 
    */
    delete from yyzy.t_yyzy_tmp_yyfpgzb_all;
    insert into yyzy.t_yyzy_tmp_yyfpgzb_all(pfphdm,jsdm,yydm,yynf,zlyybj,zpfbj,sdbh,sdfs,sysx,kclx) 
    with fpgz as(
      select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,value(b.sdbh,0) as sdbh,
          case when coalesce(b.sdfs,'0')='4' then '0' else coalesce(b.sdfs,'0') end as sdfs,
          rownumber() over(partition by a.pfphdm,a.jsdm order by a.ksrq,a.jsrq,a.tdsx,a.zxsx) as tdsx,kclx
      from yyzy.t_yyzy_zxpf_whb a 
      left join yyzy.t_yyzy_yysdb b 
        on a.sdbh=b.sdbh 
      where a.jsrq>=D_START_DATE
        and (a.yydm,a.yynf,a.kclx) in(select yydm,yynf,kclx from YYZY.T_YYZY_TMP_YYKC)
    ),zxpf_cgjh as (
      SELECT pfphdm,jsdm,a.yydm,a.yynf,KCLX
      from (
        select a.YYDM,a.YYNF,a.KCLX ,b.yycddm,b.yylbdm,b.yykbdm,b.yydjdm 
        from YYZY.T_YYZY_TMP_YYKC as a
        left join DIM.T_DIM_YYZY_YYZDB as b 
          on a.yydm=b.yydm and a.yynf=b.yynf 
      ) as a, 
    (
     select PFPHDM,JSDM,a.YYDM,a.YYNF,b.yycddm,b.yylbdm,b.yykbdm,b.yydjdm 
       from YYZY.V_YYZY_ZXPFZLYY as a 
       left join DIM.T_DIM_YYZY_YYZDB as b 
         on a.yydm=b.yydm and a.yynf=b.yynf 
     ) as b 
    where (a.yycddm,a.yylbdm,a.yykbdm,a.yydjdm)=(b.yycddm,b.yylbdm,b.yykbdm,b.yydjdm) and a.yynf=2011 
    and in_bj=1 
    ),fpgz_2 as( 
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,a.sdbh,a.sdfs,a.tdsx,a.kclx
    from FPGZ a 
    union all 
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,'1' as zlyybj,'0' as zpfbj,0 as sdbh,'0' AS sdfs,MAX(b.TDSX)+1 AS TDSX,a.kclx 
    from zxpf_cgjh a,FPGZ b 
    where (a.pfphdm,a.jsdm)=(b.pfphdm,b.jsdm)
    GROUP BY a.PFPHDM,a.JSDM,a.yydm,a.yynf,a.kclx
    )
    select a.pfphdm,a.jsdm,a.yydm,a.yynf,a.zlyybj,a.zpfbj,a.sdbh,a.sdfs,a.tdsx,a.kclx
    from FPGZ_2 a 
    UNION ALL
    SELECT A.PFPHDM,A.JSDM,'0',0,'1','0',0,'0',MAX(TDSX)+1 AS TDSX,1 as kclx
    FROM FPGZ_2 A
    GROUP BY A.PFPHDM,A.JSDM,a.kclx
    union all
    SELECT A.PFPHDM,A.JSDM,'0',0,'1','0',0,'0',1 AS TDSX,1 as kclx
    FROM yyzy.t_yyzy_tmp_yyxhtjb A
    where (A.PFPHDM,A.JSDM) not in(
    select pfphdm,jsdm from fpgz
    group by pfphdm,jsdm
    )
    GROUP BY A.PFPHDM,A.JSDM;

    --保存分配规则
    DELETE FROM YYZY.T_YYZY_TMP_YYFPGZB_ALL_BAK WHERE LOAD_TIME<LOAD_TIME - 1 MONTH;
    INSERT INTO YYZY.T_YYZY_TMP_YYFPGZB_ALL_BAK(PFPHDM, JSDM, YYDM, YYNF, ZLYYBJ, ZPFBJ, SDBH, 
      SDL, SDFS, SYSX, KCLX, LOAD_TIME) 
    SELECT PFPHDM, JSDM, YYDM, YYNF, ZLYYBJ, ZPFBJ, SDBH, SDL, SDFS, SYSX, KCLX, CURRENT TIMESTAMP 
    FROM YYZY.T_YYZY_TMP_YYFPGZB_ALL;

    --处理锁定
    CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_SD_ALL(D_START_DATE,D_END_DATE); 
    
    delete from yyzy.t_yyzy_tmp_zxpfb_whb;
    delete from YYZY.T_YYZY_TMP_YYKC_NEW;
    
    CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_TJ_YR(D_START_DATE,D_END_DATE);
    
    --根据发送锁定配方,更新zxsx
    update YYZY.T_YYZY_ZXPF_WHB as t
    set zxsx  = value(
                  (select sum(jssyl) 
                    from YYZY.T_YYZY_ZXPF_SDB 
                    where pfphdm = t.pfphdm 
                      and jsdm = t.jsdm 
                      and jsrq=t.ksrq)
                  ,0
                )
    where (pfphdm,jsdm)in (
        select pfphdm, jsdm
        from YYZY.T_YYZY_ZXPF_SDB
      )
      and tdsx = 1
    ;
    
    
  END;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR
 (DATE, 
  DATE, 
  INTEGER
 ) 
  TO USER DB2INST1 WITH GRANT OPTION;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR
 (DATE, 
  DATE, 
  INTEGER
 ) 
  TO USER ETLUSR WITH GRANT OPTION;

