--drop PROCEDURE YYZY.P_YYZY_LSPF7PHJS_RQ; 

SET SCHEMA = ETLUSR; 
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,ETLUSR; 

CREATE PROCEDURE YYZY.P_YYZY_LSPF7PHJS_RQ 
( 
  IN IP_PFPHDM INTEGER, 
  IN IP_JSDM INTEGER, 
  IN IP_RQ DATE, 
  IN IP_PC DECIMAL(18,6) 
) 
  SPECIFIC PROC_YYZY_LSPF7PHJS_RQ 
  LANGUAGE SQL 
  NOT DETERMINISTIC 
  NO EXTERNAL ACTION 
  MODIFIES SQL DATA 
  CALLED ON NULL INPUT 
LB_MAIN: 
BEGIN ATOMIC 
  /* DECLARE SYSTEM VARIABLES */
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE V_SQLSTATE CHAR(5); 
  DECLARE I_SQLCODE INTEGER; 
  DECLARE SQL_CUR_AT_END INTEGER; 
  DECLARE SQL_STMT VARCHAR(2000); 
  /* DECLARE USER-DEFINED VARIABLES */ 
  declare lc_i_dfppc, lc_i_bhqpc decimal(18,6);
  declare lc_n_yyhyl decimal(18,6); 
  declare lc_n_dpxs integer; 
  declare lc_i_dpxs1, lc_i_dpxs2 integer;
  declare lc_i_kspc decimal(18,6);
  declare lc_i_rqks, rqjs date;
  
  declare lc_i_pfphdm, lc_i_jsdm integer; 
  declare lc_n_yhyl, lc_n_tlsl,lc_n_yyfpl, lc_n_zxsx decimal(18,6); 
  declare lc_c_yypc_k varchar(4); 
  declare lc_i_tdsx integer; 
  declare lc_c_yydm varchar(20); 
  declare lc_i_yynf, lc_i_kclx integer; 
  declare lc_d_ksrq, lc_d_jsrq date; 
  
  declare lc_c_yydm_k varchar(20); 
  declare lc_i_yynf_k, lc_i_kclx_k integer; 
  declare lc_c_ZLYYBJ, lc_c_ZPFBJ char(1); 
  
  declare lc_i_flg smallint; 

  /* DECLARE STATIC CURSOR */
--  DECLARE C1 CURSOR /*WITH RETURN*/ FOR
--    select COLUMNS from TABLES;
  
  /* DECLARE DYNAMIC CURSOR */
  -- DECLARE C2 CURSOR FOR S2;
  /* DECLARE EXCEPTION HANDLE */
--  DECLARE UNDO HANDLER FOR SQLEXCEPTION
--  BEGIN 
--    VALUES(SQLCODE,SQLSTATE) INTO I_SQLCODE,V_SQLSTATE;
--    SET OP_V_ERR_MSG = VALUE(OP_V_ERR_MSG,'')
--      ||'SYSTEM:SQLCODE='||RTRIM(CHAR(I_SQLCODE))
--      ||',SQLSTATE='||VALUE(RTRIM(V_SQLSTATE),'')||'; '
--    ; 
--  END; 
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET SQL_CUR_AT_END=1;
  /* DECLARE TEMPORARY TABLE */
--  declare GLOBAL TEMPORARY TABLE tmp_jsbh( 
--    column datatype, 
--  ) with replace on commit preserve rows not logged; 
  
  /* SQL PROCEDURE BODY */
  -------------------------------------------------------------------------------
  --判断核销日期是否在最后
  if (select count(*) from JYHSF.T_JYHSF_ZSPF_LSB_R 
      where pfphdm=IP_PFPHDM and jsdm=IP_JSDM 
        and syrq>IP_RQ 
    )>0 
  then 
    SIGNAL SQLSTATE '99999' 
      SET MESSAGE_TEXT = 'user-defined exception:unprocessed condition, cannot deducte from middle' 
    ; 
  end if;
  -------------------------------------------------------------------------------
  --计算角色耗用量
  set lc_n_dpxs = YYZY.F_DPXS(IP_PFPHDM, IP_JSDM, IP_RQ); 
  if lc_n_dpxs is null or lc_n_dpxs = 0 then --若当天没有该角色
    leave LB_MAIN; --不处理该角色
  end if; 
  set lc_n_yyhyl = lc_n_dpxs * IP_PC; 
  -------------------------------------------------------------------------------
  -- start of 耗用为0情况处理
  if lc_n_yyhyl = 0 then --若当天的耗用为0，沿用历史
    values ('-1',-1,-1,'0','0',0,-1,'-1') 
    into lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_ZLYYBJ, 
      lc_c_ZPFBJ, lc_n_yyfpl, lc_i_tdsx, lc_c_yypc_k 
    ; 
    select YYDM, YYNF, ZLYYBJ, ZPFBJ, KCLX, yypc 
        into lc_c_yydm_k, lc_i_yynf_k, lc_c_ZLYYBJ, 
          lc_c_ZPFBJ, lc_i_kclx_k, lc_c_yypc_k 
    from JYHSF.T_JYHSF_ZSPF_LSB_R 
    where pfphdm=IP_PFPHDM and jsdm=IP_JSDM 
    order by syrq desc, zxsx desc 
    fetch first 1 row only 
    ;
    set lc_n_yyfpl = 0; 
    
    if lc_c_yydm_k != '-1' and lc_c_yydm_k is not null then --若存在历史
      goto lb_data_input; --数据入库
    else --若无历史 
      goto lb_exit; --退出角色处理
    end if; 
  end if; -- end of 耗用为0情况处理
  -----------------------------------------------------------------------------------
  --start of 轮询锁定部分数据
  delete from JYHSF.T_JYHSF_ZSPF_SDB --删除异常数据,否则可能出现死循环
  where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
    and yyfpl<=0 
  ; 
  loopf1: 
  for v1 as c1 cursor for 
    select pfphdm, jsdm, yydm, yynf, kclx, yyfpl, zlyybj, zpfbj, tdsx, yypc 
    from JYHSF.T_JYHSF_ZSPF_SDB 
    where pfphdm = IP_PFPHDM and jsdm = IP_JSDM 
    order by ksrq, jsrq, tdsx, zxsx 
    for update of yyfpl 
  do --body of 轮询锁定部分数据
    ---------------------------------------------------------
    if v1.yyfpl > lc_n_yyhyl then --若获取的烟叶的锁定数量足够
      --缓存烟叶信息
      values(v1.yydm, v1.yynf, v1.ZLYYBJ, v1.zpfbj, v1.kclx, lc_n_yyhyl, v1.yypc) 
        into lc_c_yydm_k, lc_i_yynf_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, 
          lc_i_kclx_k, lc_n_yyfpl, lc_c_yypc_k 
      ; 
      --烟叶的锁定数量 - lc_n_yyhyl
      update JYHSF.T_JYHSF_ZSPF_SDB 
      set yyfpl = yyfpl - lc_n_yyhyl 
      where current of c1 
      ; 
      --待分配量减少
      set lc_n_yyhyl = 0; 
      --lc_n_yyhyl入库，退出该角色的处理
      goto lb_data_input; 
    else --若烟叶的锁定不够
      --缓存烟叶信息
      values(v1.yydm, v1.yynf, v1.ZLYYBJ, v1.zpfbj, v1.kclx, v1.yyfpl, v1.yypc) 
        into lc_c_yydm_k, lc_i_yynf_k, lc_c_ZLYYBJ, lc_c_ZPFBJ, 
          lc_i_kclx_k, lc_n_yyfpl, lc_c_yypc_k 
      ;
      --待分配量减少v1.yyfpl
      set lc_n_yyhyl = lc_n_yyhyl - v1.yyfpl; 
      --从锁定部分删除烟叶
      delete from JYHSF.T_JYHSF_ZSPF_SDB 
      where current of c1 
      ; 
      -------------------------------------------
      --设置状态，决定是否继续获取分配规则
      if lc_n_yyhyl>0 then --当日耗用量未用完
        set lc_i_flg = 1; --需要继续获取分配规则
      else --否则
        set lc_i_flg = 0; --不需要继续获取分配规则
      end if; 
      --v1.yyfpl入库
      goto lb_data_input; 
    end if;
  end for loopf1; --end of 轮询锁定部分数据
  
  --非正常退出,没有足够的锁定量,抛出异常
  SIGNAL SQLSTATE '99999' 
    SET MESSAGE_TEXT = 'user-defined exception:unprocessed condition, there is no tobacco can be deducted' 
  ; 
  
  lb_data_input:begin --历史配方数据入库
    --start of 数据入库脚本
    set lc_n_zxsx = coalesce(
        (select sum(YYSYL) from JYHSF.T_JYHSF_ZSPF_LSB_R 
          where pfphdm = IP_PFPHDM and jsdm = IP_JSDM and syrq = IP_RQ 
        ),0
      )
    ;
    merge into JYHSF.T_JYHSF_ZSPF_LSB_R as t 
    using ( 
        select m.YYDM, m.YYNF, m.KCLX, m.YYPC, 
          m.zlyybj, m.zpfbj, m.yyfpl, coalesce(l.zxsx,-1) as zxsx 
        from 
          (values (lc_c_yydm_k, lc_i_yynf_k, lc_i_kclx_k, lc_c_yypc_k, 
              lc_c_ZLYYBJ, lc_c_ZPFBJ, lc_n_yyfpl)) 
            as m(YYDM, YYNF, KCLX, YYPC, zlyybj, zpfbj, yyfpl) 
          left join (
            select YYDM, YYNF, KCLX, YYPC, zxsx 
            from JYHSF.T_JYHSF_ZSPF_LSB_R 
            where pfphdm = IP_PFPHDM and jsdm = IP_JSDM and syrq = IP_RQ 
            order by SYRQ desc,ZXSX desc
            fetch first 1 row only 
          ) as l 
            on m.yydm = l.yydm and m.YYNF = l.YYNF 
            and m.KCLX = l.KCLX and m.YYPC = l.YYPC
      ) as s on t.syrq = IP_RQ 
        and t.yydm = s.yydm and t.yynf=s.yynf and t.yypc = s.yypc 
        and t.kclx = s.kclx and t.zxsx = s.zxsx 
    when matched then 
      update set t.YYSYL = t.YYSYL + s.yyfpl 
    when not matched then 
      insert (PFPHDM, JSDM, YYDM, YYNF, SYRQ, YYSYL, 
              ZXSX, ZLYYBJ, ZPFBJ, KCLX, YYPC
              )
      values (IP_PFPHDM, IP_JSDM, s.yydm, s.yynf, IP_RQ, s.yyfpl, 
                lc_n_zxsx, s.zlyybj, s.ZPFBJ, s.KCLX, s.YYPC 
              )
    ; --end of 数据入库脚本 
    --------------------------------------------------------------
    if lc_i_flg = 1 then --判断状态，处理后续
      set lc_i_flg = 0; 
      goto loopf1; 
    end if; 
  end lb_data_input; 
  lb_exit: 
    return; 
  
END LB_MAIN; 

COMMENT ON PROCEDURE YYZY.P_YYZY_LSPF7PHJS_RQ( 
    integer, integer, date, decimal(18,6) 
  ) IS '7要素历史配方 牌号角色 指定日期'
; 
