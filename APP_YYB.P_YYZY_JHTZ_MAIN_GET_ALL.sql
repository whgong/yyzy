drop PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL;
SET SCHEMA ETLUSR;
SET CURRENT PATH = SYSIBM,SYSFUN,SYSPROC,SYSIBMADM,ETLUSR;

CREATE PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL ( ) 
  SPECIFIC APP_YYB.SQL130711172312200
  LANGUAGE SQL
  NOT DETERMINISTIC
  CALLED ON NULL INPUT
  EXTERNAL ACTION
  OLD SAVEPOINT LEVEL
  MODIFIES SQL DATA
  INHERIT SPECIAL REGISTERS
  --�洢����  
BEGIN 
  --����ϵͳ����  
  DECLARE SQLSTATE CHAR(5); 
  DECLARE SQLCODE INTEGER; 
  DECLARE MSG VARCHAR(1000); 
  
  --����
  DECLARE MIN_KSRQ, LOOP_DATE, TODAY_DATE ,START_DATE, END_DATE DATE ;   
  
  /* ÿ��1��1��,�Զ�����ִ���䷽ά����������,���������ƻ�,������ */
  SET TODAY_DATE=CURRENT DATE;
  
  IF TODAY_DATE=TODAY_DATE - DAYOFYEAR(TODAY_DATE) DAY + 1 DAY THEN 
    --1.�޸�ִ���䷽ά����������
    UPDATE YYZY.T_YYZY_ZXPF_WHB SET KCLX=1 WHERE YYNF=YEAR(CURRENT DATE)-1 AND KCLX=3;
    
    --2.�޸����������ƻ��������ӣ�
    
    --3.�޸������ʣ������ӣ�
    
  END IF;
  
  --���ݽ�ɫ������
  INSERT INTO YYZY.T_YYZY_JSTZ_WHB_BAK (PFPHDM, JSDM, KSRQ, JSRQ, DPXS, ZYBJ, BBH)
  SELECT PFPHDM, JSDM, KSRQ, JSRQ, DPXS, ZYBJ, BBH
    FROM YYZY.T_YYZY_JSTZ_WHB
   WHERE (PFPHDM,JSDM,BBH) IN (
        SELECT PFPHDM,JSDM,MAX(BBH) 
        FROM YYZY.T_YYZY_JSTZ_WHB 
        WHERE ZYBJ='1' AND CURRENT DATE BETWEEN KSRQ AND JSRQ 
        GROUP BY PFPHDM,JSDM
      ) 
     AND ZYBJ='1'
     AND CURRENT DATE BETWEEN KSRQ AND JSRQ
  ;
  
  --����1���µĽ�ɫ������������
  DELETE FROM YYZY.T_YYZY_JSTZ_WHB_BAK WHERE LOAD_TIME<CURRENT TIMESTAMP - 1 MONTH;
  
  INSERT INTO YYZY.T_YYZY_JZ_RZ(BZ, MBB, JZSJ, SFCG)
  VALUES('��ʼ����','',CURRENT TIMESTAMP,1);
  COMMIT;

--  --���ε��ø����ƻ��Ĵ洢����
--  --���������ƻ���ȫ������
--  --CALL APP_YYB.P_YYZY_JHJZ_3YEAR();
--  /* 2013-02-04 �޸� ,3��ƻ��еĵ�����ƻ�����, ����ǰ̨ҳ�� �з�->��Ҷ��Դ���Ϸ���ϵͳ->�ۺϲ�ѯ->�����ƻ�(�ֳ�) �������ݼ��������ָ�����תҳ�����ݻ���ʾ�������� */ 
  INSERT INTO YYZY.T_YYZY_SNSCJH_1 (TOBACCOID, JHNF, PZMC, YHBS, CJDM, JHCL, PPMC, BBH, BBRQ)
  WITH YSSJ AS (
    select A.TOBACCOID,
      INT(A.CYEAR) AS JHNF, 
      B.STANDARDNM AS PZMC,
      D.YHBS, 
      D.SCCJDM AS CJDM,
      SUM(A.QUANTITY) AS JHCL,
      C.brandnm as PPMC,
      E.BBH+1 AS BBH,
      DATE(SUBSTR(A.C_CREATETIME,1,4)||'-'||SUBSTR(A.C_CREATETIME,5,2)||'-'||SUBSTR(A.C_CREATETIME,7,2)) AS BBRQ
    from HDS_CXQJ.N_CXQJ_O_REGION_YEARPLAN AS A
      LEFT JOIN HDS_TF01.N_TF01_TCM21 AS B
        ON A.TOBACCOID=B.STANDARDID
      LEFT JOIN HDS_TF01.N_TF01_TCM08 AS C
        ON C.brandid=substr(A.TOBACCOID,14,3)
      LEFT JOIN dim.t_dim_yyzy_pfph AS D
        ON SUBSTR(A.TOBACCOID,17,2)=D.YHBS
      LEFT JOIN (SELECT MAX(BBH) AS BBH FROM YYZY.T_YYZY_SNSCJH_1) AS E
        ON 1=1
    WHERE INT(A.CYEAR)=YEAR(CURRENT DATE)
      AND INT(C_CREATETIME)=(SELECT MAX(INT(C_CREATETIME)) FROM HDS_CXQJ.N_CXQJ_O_REGION_YEARPLAN)
    GROUP BY A.TOBACCOID, B.STANDARDNM, C.brandnm, A.CYEAR, A.C_CREATETIME, E.BBH, D.YHBS, D.SCCJDM
  ),SNSC_BBRQ AS (  --ԭ����ƻ����汾����
    SELECT MAX(BBRQ) AS BBRQ FROM YYZY.T_YYZY_SNSCJH_1
  ),NSC_BBRQ AS (   --��������ƻ����汾����
    SELECT MAX(BBRQ) AS BBRQ FROM YSSJ 
  ),SCJHBBRQ AS (   --�����ƻ����汾���ڴ���
    SELECT A.BBRQ AS A,B.BBRQ AS B FROM SNSC_BBRQ AS A,NSC_BBRQ AS B WHERE A.BBRQ=B.BBRQ
  )
  /* �������������µ���ƻ�ʱ�Ż���и��� */
  SELECT TOBACCOID, JHNF, PZMC, YHBS, CJDM, JHCL, PPMC, BBH, BBRQ 
  FROM YSSJ WHERE NOT EXISTS (select 1 from SCJHBBRQ)
  ;
  
  --�������ƻ���ȫ������ 
  CALL APP_YYB.P_YYZY_JHJZ_YEAR(); 
  --�������ƻ������ظ��µ������� 
  --CALL APP_YYB.P_YYZY_JHJZ_QUARTER();
  
  --�������ƻ�,2012��04��28��ע��!!
  --CALL APP_YYB.P_YYZY_JHJZ_J();
  
  --�������ƻ���ȫ������
  CALL APP_YYB.P_YYZY_JHJZ_MONTH();
  
  
  --ȡ��ѭ���Ŀ�ʼ����
  SELECT MAX(JHRQ) INTO MIN_KSRQ 
  FROM YYZY.T_YYZY_RSCJH_LSB
    WHERE PFPHDM NOT IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)     --2011-11-17
  ;
  
  SELECT MAX(DQRQ)+1 DAY INTO START_DATE
  FROM YYZY.T_YYZY_YYKC_NEW
  ; 
  
  SET LOOP_DATE=MIN_KSRQ;
  WHILE MIN_KSRQ<START_DATE-1 DAY DO 
    --�����������ƻ���ʷ
    INSERT INTO YYZY.T_YYZY_RSCJH_LSB(PFPHDM,JHRQ,JHCL,JHPC,BBRQ)
    SELECT PFPHDM,KSRQ,JHCL_AVG,JHPC_AVG,BBRQ 
    FROM YYZY.T_YYZY_RSCJHB_WHB 
    WHERE KSRQ=MIN_KSRQ+1 DAY; 
    
    --������ʷ����(ֻ����һ����)
    DELETE FROM YYZY.T_YYZY_RSCJHB_WHB_BAK
    WHERE BBRQ<=CURRENT DATE-DAY(CURRENT DATE) DAY - 1 MONTH;
    
    DELETE FROM YYZY.T_YYZY_ZXPF_WHB_BAK
    WHERE BBRQ<=CURRENT DATE-DAY(CURRENT DATE) DAY - 1 MONTH;
    
    INSERT INTO YYZY.T_YYZY_RSCJHB_WHB_BAK
    SELECT * 
    FROM YYZY.T_YYZY_RSCJHB_WHB ;
/*  
    --����YYZY.T_YYZY_RSCJH_LSB���� 81#��˿��91��˿
    UPDATE YYZY.T_YYZY_RSCJH_LSB SET JHCL=0,JHPC=0 WHERE PFPHDM IN (72,73);
*/
    INSERT INTO YYZY.T_YYZY_ZXPF_WHB_BAK
    (
      PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,SDBH,ZXSX,TDSX,KSSYL,JSSYL,
      ZLYYBJ,ZPFBJ,FJCHSX,FJCHXX,KCLX,BBRQ,LOAD_TIME
    ) 
    SELECT 
      PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,SDBH,ZXSX,TDSX,
      KSSYL,JSSYL,ZLYYBJ,ZPFBJ,FJCHSX,FJCHXX,KCLX,BBRQ,(SELECT MAX(LOAD_TIME) FROM YYZY.T_YYZY_ZXPF_WHB) AS LOAD_TIME
    FROM YYZY.T_YYZY_ZXPF_WHB
    ;
    
    --������һ����������ƻ���, ������ɫ��ʱʹ�ã�2013��8��17������.
    delete from YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI; 
    insert into YYZY.T_YYZY_TMP_RSCJHB_WHB_PRI 
    select * from YYZY.T_YYZY_RSCJHB_WHB; 
    
    --�������ƻ��������� 
    DELETE FROM YYZY.T_YYZY_RSCJHB_WHB; 
    CALL APP_YYB.P_YYZY_JHJZ_R(MIN_KSRQ+2 DAY,MSG); 
    
    --��ӹ����� 
    DELETE FROM YYZY.T_YYZY_RSCJH_LSB WHERE JHRQ >= CURRENT DATE; --2013-04-10�޸ģ���֤��ӹ�ά�����ݵļ��� 
    CALL YYZY.P_YYZY_WJG_PHSD(1,MSG);   -- 2011-11-17 
    CALL YYZY.P_YYZY_WJG_PHSD(2,MSG);   -- 2011-11-17 

/* --��DS�е���
    --ʵ��Ͷ������(������ʷ�ƻ���Ϊʵ��Ͷ��),2013��8��15������.
    delete from YYZY.T_YYZY_SJTL_SCPC where date(tlsj)>=current_date - (day(current_date) - 1) day;
    insert into YYZY.T_YYZY_SJTL_SCPC(TLSJ, PFPHDM, PHSCPC)
    select timestamp(trim(char(JHRQ))||' 00:00:00.000000') as tlsj,PFPHDM, sum(value(JHPC,0)) as phscpc
    from YYZY.T_YYZY_RSCJH_LSB
    where pfphdm NOT IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)
      and jhrq >= current_date - (day(current_date) - 1) day
      and jhrq < MIN_KSRQ+2 DAY
    group by timestamp(trim(char(JHRQ))||' 00:00:00.000000'), pfphdm
    order by tlsj desc, pfphdm
    ;
    --����������,2013��8��15������.
    delete from YYZY.T_YYZY_SJTL_YEAR where nf>=year(current_date)-1;
    insert into YYZY.T_YYZY_SJTL_YEAR(nf, PFPHDM, YTLPC)
    select year(tlsj) as nf,pfphdm, sum(PHSCPC) as ytlpc
    from YYZY.T_YYZY_SJTL_SCPC
    where year(tlsj)>=year(current_date)-1
      and date(tlsj)>=(
          select date(CSZ)
          from YYZY.T_YYZY_STCS
          where csmc = 'ZSPFFSQSRQ'
          fetch first 1 row only
        )
      and date(tlsj) < MIN_KSRQ+2 DAY
    group by year(tlsj),pfphdm
    ;
    */
    --�������ƻ�ȡ�� 2013��8��15���޸�, ����˿������������ 
    CALL YYZY.P_YYZY_RSCJHB_WHB_PCQZ(); 
    
    --�����ƻ�����,2013��8��15������.
--    call YYZY.P_YYZY_RSCJH_JHXZ(year(MIN_KSRQ+2 DAY),month(MIN_KSRQ+2 DAY));
    
    --��ɫ����(�����µļƻ�����)
    --���������ƻ�
    call YYZY.P_YYZY_SDPF_JSGX();
    
    --ɾ��������Ϊ -1 �����
    DELETE FROM YYZY.T_YYZY_ZXPF_WHB WHERE YYFPL=-1;
    
    --�����䷽��ʷ�и�
    call YYZY.P_YYZY_LSPF6(MIN_KSRQ+1 DAY); --6Ҫ��
    call YYZY.P_YYZY_LSPF7(MIN_KSRQ+1 DAY); --7Ҫ��
    
    --�����䷽����
    call YYZY.P_YYZY_SDPFGX6(); --6Ҫ��
    call YYZY.P_YYZY_SDPFGX7(); --7Ҫ��
    
    --����ZXPF_LSB����
    MERGE INTO YYZY.T_YYZY_ZXPF_LSB AS E  
    USING
      (
        SELECT 
          PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,ZXSX,TDSX, 
          KSSYL,JSSYL,ZLYYBJ,ZPFBJ,BBRQ,FJCHSX,FJCHXX,KCLX 
        FROM YYZY.T_YYZY_ZXPF_WHB 
        WHERE KSRQ=MIN_KSRQ+1 DAY 
          -- 2013��8��20���޸�
          and pfphdm not in (select pfphdm from YYZY.T_YYZY_ZXPF_SDB) 
          --AND YYFPL>0  
          --2013-01-01 �޸ģ�����ש����ܻ����������ƻ���ԭ���·�����Ϊ0��������������������˵���Щ���ݣ���ִ���䷽��ʷ���ݻ��ж�
      ) AS M 
        ON (E.PFPHDM,E.JSDM,E.YYDM,E.YYNF,E.KCLX,E.JSRQ)=
            (M.PFPHDM,M.JSDM,M.YYDM,M.YYNF,M.KCLX,M.KSRQ-1 DAY)
      WHEN MATCHED THEN 
        UPDATE SET (E.YYFPL,E.JSRQ,E.ZXSX,E.JSSYL,E.ZLYYBJ,E.ZPFBJ)= 
            (E.YYFPL+M.KSSYL,M.KSRQ,M.ZXSX,M.KSSYL,M.ZLYYBJ,M.ZPFBJ)
      WHEN NOT MATCHED THEN
        INSERT(PFPHDM,JSDM,YYDM,YYNF,KSRQ,JSRQ,YYFPL,ZXSX,KSSYL,JSSYL,ZLYYBJ,ZPFBJ,KCLX)
        VALUES(M.PFPHDM,M.JSDM,M.YYDM,M.YYNF,M.KSRQ,M.KSRQ,
            M.KSSYL,M.ZXSX,M.KSSYL,M.KSSYL,M.ZLYYBJ,M.ZPFBJ,M.KCLX)
    ;
    
    --ִ���䷽��Ҷ����
    SELECT MAX(JSRQ) INTO END_DATE 
    FROM YYZY.T_YYZY_RSCJHB_WHB
    ; 
    --CALL APP_YYB.P_YYZY_JHTZ_MAIN_GET_YR(MIN_KSRQ+2 DAY,END_DATE,0); 
    
    SET MIN_KSRQ=MIN_KSRQ+1 DAY;
  
  END WHILE; 
  
  -- ��������ģ�����ݼ���
  -- �䷽���� 
--  CALL APP_YYB.P_YYZY_PFFX_WHB(CURRENT DATE + 1 MONTH - DAY(CURRENT DATE) DAY + 1 DAY,MSG); 
  -- ���ܷ���
--  CALL APP_YYB.P_YYZY_CNFX_MAIN(0);
--  CALL APP_YYB.P_YYZY_CNFX_MAIN(1);
--  CALL APP_YYB.P_YYZY_CNFX_MX(MSG);
 
  -- ����1�տ����� 
  CALL YYZY.P_YYZY_SJJZ_XYKC(START_DATE,MSG);
  --����
  CALL YYZY.P_YYZY_SJJZ_DD(MSG);
  
  INSERT INTO YYZY.T_YYZY_JZ_RZ(BZ, MBB, JZSJ, SFCG)
  VALUES('�������','',CURRENT TIMESTAMP,1);
  
  -- �����Ѿ�ת��ʷ�� ��ӹ��ƺ� ���� Ϊ 0 ����ֹשǽͼ����
  INSERT INTO YYZY.T_YYZY_RSCJHB_WHB
  SELECT PFPHDM,'2013-12-31' AS KSRQ,'2013-12-31' AS JSRQ,0 AS JHCL_AVG,0 AS JHPC_AVG,'2011-12-06' AS BBH
  FROM YYZY.T_YYZY_WJG_PHSD 
  WHERE PFPHDM NOT IN(
      SELECT PFPHDM
      FROM YYZY.T_YYZY_RSCJHB_WHB 
      WHERE PFPHDM IN(SELECT PFPHDM FROM YYZY.T_YYZY_WJG_PHSD)
    )
  ;

  /* 2013-01-24 �����䷽ͼ���У�� */ 
  CALL YYZY.P_YYZY_ZXPF_JC();

  /* 2013-02-22 �޸ģ����ƺźϲ����趨���뵽�������ƻ�,�������䷽���޷���ʾ�ƺźϲ������ȷ���� */ 
  --��������
  /* 2013-04-07�޸ģ��¶Ȳ����ƺŲ����в��ϲ���
  insert into YYZY.T_YYZY_YSCJH_BAK (YSCJHDM, JHNF, JHYF, YHBS, CJDM, BRIEFNM, WKSPDM, QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, KSRQ, JSRQ,PZMC, PZDM, PPDM)
  SELECT YSCJHDM, JHNF, JHYF, YHBS, CJDM, BRIEFNM, WKSPDM,QYDM, JHCL, ZYBJ, QYBBH, XGBZ, BBH, BBRQ, KSRQ, JSRQ, PZMC, PZDM, PPDM FROM YYZY.T_YYZY_YSCJH WHERE JHNF=YEAR(CURRENT DATE);
  DELETE FROM YYZY.T_YYZY_YSCJH_BAK WHERE LOAD_TIME<CURRENT TIMESTAMP - 1 MONTH;

  --�޸�����
  UPDATE YYZY.T_YYZY_YSCJH AS A SET (A.YHBS,A.CJDM)=(SELECT B.YHBS,int(B.CJDM) FROM YYZY.T_YYZY_YS_PH27 AS B WHERE A.YHBS=B.YHBS_SRC AND B.ZYBJ='1' AND B.YHBS_SRC=B.YHBS)
  WHERE A.YHBS IN (SELECT YHBS_SRC FROM YYZY.T_YYZY_YS_PH27 WHERE YHBS_SRC=YHBS AND YHBS NOT IN ('26','17'))
    AND (jhnf,jhyf,bbh) in (select jhnf,jhyf,max(bbh) from YYZY.T_YYZY_YSCJH WHERE ZYBJ='1' group by jhnf,jhyf)
    AND ZYBJ='1' 
  ;
*/ 

  --3��14�ո���,���������ÿ����������������,�Ա�ִ���䷽ͼ���ñ��ܹ���ѯ���µĿ������ 
  DELETE FROM YYZY.T_YYZY_YYKC_SDB;
  INSERT INTO YYZY.T_YYZY_YYKC_SDB(YYDM, YYMC, YYNF, YYKCJS)
  SELECT YYDM, YYMC, YYNF, YYKCJS 
  FROM YYZY.V_YYZY_YYKC_WHB
  ;
  
  --���Ȼ��㷨���ݼ���
  CALL JYHSF.P_JYHSF_SJJZ();

END;

GRANT EXECUTE ON PROCEDURE APP_YYB.P_YYZY_JHTZ_MAIN_GET_ALL ( ) 
  TO USER DB2INST1 WITH GRANT OPTION;

