/*
drop table YYZY.T_YYZY_TMP_RSCPCB;
create table YYZY.T_YYZY_TMP_RSCPCB 
(
  pfphdm INTEGER,
  jhrq date,
  jhpc decimal(18,6),
  jhcl decimal(18,6)
)
in ts_reg_16k;
*/
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
SET SCHEMA ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

--DROP PROCEDURE YYZY.P_YYZY_RSCJHB_PCQZ_DPH;

create PROCEDURE YYZY.P_YYZY_RSCJHB_PCQZ_DPH(
  ip_pfphdm integer,
  ip_mxjjbrq date
)
  SPECIFIC YYZY.PROC_RSCJHB_PCQZDPH
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
lbmain : 
BEGIN
  /*------------------------------------declare---------------------------------------*/
  declare lc_d_bsd decimal(18,2);
  declare lc_i_flg1 integer; 
  declare lc_i_tot,lc_i_min, lc_i_ys, lc_i_ts integer; 
  declare lc_i_tmpval,lc_i_tmpys decimal(18,6);
  declare lc_i_dpcl decimal(18,6);
  
  DECLARE GLOBAL TEMPORARY TABLE scjh_r1(
    pfphdm INTEGER,
    jhrq date,
    jhpc decimal(18,6)
  ) with replace on commit preserve rows not logged; 
  
  DECLARE GLOBAL TEMPORARY TABLE scjh_yfp(
    pfphdm INTEGER, 
    nf integer, 
    yf integer, 
    ksrq date, 
    jsrq date, 
    ts integer, 
    qz decimal(18,6), 
    fppc integer 
  ) with replace on commit preserve rows not logged; 
  
  /*------------------------------------proc body---------------------------------------*/
  --清理临时表数据
  delete from session.scjh_r1;
  delete from YYZY.T_YYZY_TMP_RSCPCB;
-----------------------------------------------------------------------------------------------
  -- 跳过梗配方和无单批产量牌号的处理 
  if ip_pfphdm not in (
        SELECT PFPHDM FROM YYZY.T_YYZY_DPCLB as C
        WHERE (PFPHDM,NY) IN (SELECT PFPHDM, MAX(NY) FROM YYZY.T_YYZY_DPCLB  GROUP BY PFPHDM)
      ) 
    or ip_pfphdm IN (SELECT PFPHDM FROM YYZY.T_YYZY_PFPH_CFG WHERE JSBJ='0' AND GPBJ='1')
  then
    leave lbmain;
  end if;
-----------------------------------------------------------------------------------------------
  --计算牌号的单批产量
  set lc_i_dpcl = cast(null as integer);
  set lc_i_dpcl = YYZY.F_DPCL(ip_pfphdm);
  if lc_i_dpcl is null then
    leave lbmain;
  end if;
-----------------------------------------------------------------------------------------------
  -- 处理指定牌号的卷接包计划 
  if ip_pfphdm in (select pfphdm from DIM.T_DIM_YYZY_PFPH_PHCF where sccjdm in (1,2)) then 
    delete from YYZY.T_YYZY_TMP_RSCPCB; 
    insert into YYZY.T_YYZY_TMP_RSCPCB(pfphdm, jhrq, jhpc, jhcl) 
    select PFPHDM, riqi as jhrq, JHPC_AVG, jhcl_avg 
    from YYZY.T_YYZY_RSCJHB_WHB as m 
      inner join DIM.T_DIM_YYZY_DATE as d 
        on d.riqi between m.ksrq and m.jsrq 
    where pfphdm = ip_pfphdm 
      and jsrq < ip_mxjjbrq 
    order by JHPC_AVG desc 
    ; 
    
    lp1:    --轮询计划月份
    for v1 as c1 cursor for 
        select min(jhrq) as ksrq, max(jhrq) as jsrq 
        from YYZY.T_YYZY_TMP_RSCPCB 
        group by year(jhrq),month(jhrq)
        order by 1,2
    do 
      set lc_i_tmpval = coalesce( 
                          (select sum(jhcl) from YYZY.T_YYZY_TMP_RSCPCB where jhrq between v1.ksrq and v1.jsrq) 
                          ,0 
                        ) 
      ; 
      set lc_d_bsd = 0;
      lp2:  --轮询日计划
      for v2 as c2 cursor for 
          select jhcl 
          from YYZY.T_YYZY_TMP_RSCPCB 
          where jhrq between v1.ksrq and v1.jsrq 
          order by jhrq 
          FOR UPDATE OF jhcl 
      do
        set lc_d_bsd = MATH.F_XSQZ(v2.jhcl,lc_i_dpcl); -- - v2.jhcl; 
        if v2.jhcl = 0 then --当天计划为0
          --不处理
        elseif lc_i_tmpval<=0 then --当月产量已扣减完
          update YYZY.T_YYZY_TMP_RSCPCB 
          set jhcl = 0 
          where current of c2 
          ; 
        elseif lc_i_tmpval >= lc_d_bsd then --当月产量足够扣减
          update YYZY.T_YYZY_TMP_RSCPCB 
          set jhcl = MATH.F_XSQZ(jhcl,lc_i_dpcl) 
          where current of c2 
          ; 
          set lc_i_tmpval = lc_i_tmpval - lc_d_bsd; 
        elseif lc_i_tmpval > 0 and lc_i_tmpval < lc_d_bsd then --当月产量不够扣减
          update YYZY.T_YYZY_TMP_RSCPCB 
          set jhcl = round(jhcl * 1.000000 / lc_i_dpcl, 0) * lc_i_dpcl --四舍五入
          where current of c2 
          ; 
          set lc_i_tmpval = 0; 
        end if;
/*
        if v2.jhcl<>0 and v2.jhcl>=lc_d_bsd then    --当天的数量足够抵消前一天缺口
          update YYZY.T_YYZY_TMP_RSCPCB                   --修正当天的批次数
          set jhcl = jhcl - lc_d_bsd 
          where current of c2 
          ; 
          --向上进位
          set lc_d_bsd = round(MATH.F_XSQZ(v2.jhcl-lc_d_bsd, lc_i_dpcl) - (v2.jhcl-lc_d_bsd), 2);
          update YYZY.T_YYZY_TMP_RSCPCB 
          set jhcl = MATH.F_XSQZ(jhcl,lc_i_dpcl) 
          where current of c2 
          ; 
        elseif v2.jhcl<>0 and v2.jhcl<lc_d_bsd then   --不够抵消前一天的缺口
          update YYZY.T_YYZY_TMP_RSCPCB 
          set jhcl = 0 
          where current of c2 
          ;
          set lc_d_bsd = lc_d_bsd - v2.jhcl; 
        end if; 
*/
      end for lp2; --以上为 轮询日计划 
    end for lp1; --以上为 轮询计划月份 
    
    --更新批次数量
    update YYZY.T_YYZY_TMP_RSCPCB 
    set jhpc = round(jhcl * 1.000000 / lc_i_dpcl, 0) 
    where pfphdm = ip_pfphdm 
      and jhrq < ip_mxjjbrq 
    ; 
    
  end if; -- 以上为 处理指定牌号的卷接包计划 
---------------------------------------------------------------------------------------
  -- 奇偶标记
  set lc_i_flg1 = mod(month(ip_mxjjbrq),2); 
  
  lp3:  --轮询所有计划记录
  for v3 as c3 cursor for 
    select pfphdm, ksrq, jsrq, jhpc_avg 
    from YYZY.T_YYZY_RSCJHB_WHB 
    where pfphdm = ip_pfphdm 
      and ksrq>=(case 
                  when ip_pfphdm in (select pfphdm from DIM.T_DIM_YYZY_PFPH_PHCF where sccjdm in (1,2)) 
                    then ip_mxjjbrq 
                  else 
                    date('1980-01-01') 
                end) 
  do
    set lc_i_ts = (days(v3.jsrq) - days(v3.ksrq) + 1);
    set lc_i_tot = round(v3.jhpc_avg * lc_i_ts,0); -- + round(MATH.F_QZBS(v3.jhpc_avg * lc_i_ts),2);
    set lc_i_min = lc_i_tot / lc_i_ts;
    set lc_i_ys = mod(lc_i_tot, lc_i_ts);
    
    insert into session.scjh_r1(jhrq, jhpc)
    select riqi, 0
    from DIM.T_DIM_YYZY_DATE
    where riqi between v3.ksrq and v3.jsrq
    ;
    
    delete from session.scjh_yfp;
    insert into session.scjh_yfp(nf, yf, ksrq, jsrq, ts, qz, fppc)
    with tb_ny as (
      select year(jhrq) as nf, month(jhrq) as yf, min(jhrq) as ksrq, max(jhrq) as jsrq, count(jhrq) as ts
      from session.scjh_r1
      where jhrq between v3.ksrq and v3.jsrq
      group by year(jhrq), month(jhrq)
    )
    select nf, yf, ksrq, jsrq, ts, ts*1.000000/lc_i_ts as qz,0 as fppc
    from tb_ny
    ;
    
    set lc_i_tmpys = lc_i_ys;
    lp4: -- 计算每月所分配的余数
    for v4 as c4 cursor for 
        select nf, yf, ksrq, jsrq, ts, qz, fppc 
        from session.scjh_yfp 
        order by nf, yf 
        FOR UPDATE OF fppc 
    do 
      set lc_i_tmpval = round(v4.qz * lc_i_ys, 2); 
      set lc_i_tmpval = lc_i_tmpval + round(MATH.F_QZBS(lc_i_tmpval),2); 
      update session.scjh_yfp 
      set fppc = (case when lc_i_tmpval<=lc_i_tmpys then lc_i_tmpval else lc_i_tmpys end) 
      where current of c4 
      ; 
      set lc_i_tmpys = lc_i_tmpys - lc_i_tmpval; 
      
      if lc_i_tmpys<=0 then 
        leave lp4; 
      end if; 
    end for lp4; -- 以上为 计算每月所分配的余数
    
    lp5:  --日计划批次数据计算
    for v5 as c5 cursor for
        select nf, yf, ksrq, jsrq, ts, qz, fppc
        from session.scjh_yfp
        order by ksrq, jsrq
    do
      if (mod(v5.yf,2) = 1 and lc_i_flg1 = 1) or (mod(v5.yf,2) = 0 and lc_i_flg1 = 0) then 
        --最大卷烟包之后的月份为奇数,则奇数月份往前排列
        update session.scjh_r1 
        set jhpc = 1 
        where jhrq between v5.ksrq and v5.ksrq + (v5.fppc-1) day 
        ;
      else --反之偶数月往前排布 
        update session.scjh_r1 
        set jhpc = 1 
        where jhrq between v5.jsrq - (v5.fppc-1) day and v5.jsrq 
        ; 
      end if;
      
      --增加平均批次
      update session.scjh_r1
      set jhpc = jhpc + lc_i_min
      where jhrq between v5.ksrq and v5.jsrq
      ;
    end for lp5; --以上为 日计划批次数据计算
  end for lp3; -- 以上为 轮询所有计划记录
  
  --处理月计划弥补
  call YYZY.P_YYZY_RSCJH_RJHXZ(ip_pfphdm); 
  
  --合并相同批次数的日期
  insert into YYZY.T_YYZY_TMP_RSCJHB_WHB(pfphdm, ksrq, jsrq, jhpc_avg) 
  with tb_jhr as (
    select jhrq, jhpc, rownumber()over(order by jhrq) as xh
    from 
    (
      select pfphdm, jhrq, jhpc from YYZY.T_YYZY_TMP_RSCPCB
      union all 
      select pfphdm, jhrq, jhpc from session.scjh_r1
    ) as tb
  )
  , tb_jh_cir(jhrq, jhpc, xh, xh1) as (
    select jhrq, jhpc, xh, 1
    from tb_jhr 
    where xh = 1
    union all
    select a.jhrq, a.jhpc, a.xh, 
      (case when a.jhpc = b.jhpc then b.xh1 else a.xh end) as xh1
    from tb_jhr as a, tb_jh_cir as b
    where a.xh = b.xh+1
  )
  select ip_pfphdm, min(jhrq) as ksrq, max(jhrq) as jsrq, jhpc
  from tb_jh_cir
  group by jhpc, xh1
  ;
  
end lbmain;


-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
--DROP PROCEDURE YYZY.P_YYZY_RSCJHB_WHB_PCQZ;

CREATE PROCEDURE YYZY.P_YYZY_RSCJHB_WHB_PCQZ()
  SPECIFIC YYZY.SQL130725174626700
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  BEGIN
    /*declare*/
    declare lc_d_mxjjbrq date;
    declare lc_d_mxjjhrq date;
    declare lc_d_bbrq date;
    
    declare c1 cursor for
      select distinct pfphdm
      from YYZY.T_YYZY_RSCJHB_WHB
    ;
    
    DECLARE GLOBAL TEMPORARY TABLE SESSION.RSCJHB_TMP
    (
      PFPHDM INTEGER, 
      KSRQ DATE, 
      JSRQ DATE , 
      JHCL_AVG DECIMAL(18,2),
      JHPC_AVG DECIMAL(18,2),
      BBRQ DATE
    )ON COMMIT PRESERVE ROWS WITH REPLACE;

    DECLARE GLOBAL TEMPORARY TABLE SESSION.JSTZ_TMP(
      PFPHDM INTEGER, 
      JSDM INTEGER,
      KSRQ DATE, 
      JSRQ DATE , 
      DPXS INTEGER,
      BBH INTEGER,
      ZYBJ CHAR(1)
    )ON COMMIT PRESERVE ROWS WITH REPLACE;

  /*proc body*/
  select date(to_date(max(c_date),'YYYYMMDD')), date(max(d_createtime)) into lc_d_mxjjbrq, lc_d_bbrq
  from hds_cxqj.n_cxqj_o_prodplanhist_n
  where (c_date,n_version)in(select c_date,max(n_version) from hds_cxqj.n_cxqj_o_prodplanhist_n group by c_date)
    and c_date>=char(year(current_date)*10000+month(current_date)*100+day(current_date)) 
  ;
  set lc_d_mxjjbrq = lc_d_mxjjbrq - (day(lc_d_mxjjbrq)-1) day + 1 month; 

  delete from YYZY.T_YYZY_TMP_RSCJHB_WHB;
  lp1:  --按牌号轮询处理计划
  for v1 as c1 cursor for 
    select distinct pfphdm
    from YYZY.T_YYZY_RSCJHB_WHB
  do
    call YYZY.P_YYZY_RSCJHB_PCQZ_DPH(v1.pfphdm, lc_d_mxjjbrq);
  end for lp1;  -- 按牌号轮询处理计划
  
  --根据批次更新产量
  UPDATE  YYZY.T_YYZY_TMP_RSCJHB_WHB as A 
  SET JHCL_AVG= JHPC_AVG*(  
                            SELECT  DPCL FROM YYZY.T_YYZY_DPCLB as B
                            WHERE 
                              (PFPHDM,NY) IN(
                                  SELECT  PFPHDM, MAX( NY) 
                                  FROM YYZY.T_YYZY_DPCLB  GROUP BY PFPHDM)
                              AND A.PFPHDM=B.PFPHDM 
                        )
  WHERE PFPHDM IN (
        SELECT PFPHDM FROM YYZY.T_YYZY_DPCLB as C
        WHERE (PFPHDM,NY) IN (SELECT PFPHDM, MAX(NY) FROM YYZY.T_YYZY_DPCLB  GROUP BY PFPHDM)
          AND A.PFPHDM=C.PFPHDM
      )
    AND PFPHDM NOT IN (SELECT PFPHDM FROM YYZY.T_YYZY_PFPH_CFG WHERE JSBJ='0' AND GPBJ='1')
  ; --以上 根据批次更新产量
  
  --数据入库
  DELETE FROM YYZY.T_YYZY_RSCJHB_WHB where pfphdm in (select pfphdm from YYZY.T_YYZY_TMP_RSCJHB_WHB);
  INSERT INTO YYZY.T_YYZY_RSCJHB_WHB (PFPHDM, KSRQ, JSRQ, JHCL_AVG,JHPC_AVG,BBRQ)
  SELECT PFPHDM, KSRQ, JSRQ, JHCL_AVG,JHPC_AVG, lc_d_bbrq 
  FROM YYZY.T_YYZY_TMP_RSCJHB_WHB
  ;

END;

COMMENT ON PROCEDURE YYZY.P_YYZY_RSCJHB_WHB_PCQZ() IS '日生产计划维护表取整';