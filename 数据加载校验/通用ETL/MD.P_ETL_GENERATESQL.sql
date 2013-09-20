SET SCHEMA = MD;

CREATE PROCEDURE MD.P_ETL_GENERATESQL (
    IN SRCTABLE    VARCHAR(128),
    IN TGTTABLE    VARCHAR(128),
    IN LOADTYPE    CHARACTER(1),
    IN LOADFIELD    VARCHAR(255),
    INOUT LOADVALUE    VARCHAR(255),
    OUT RUNSTATUS    INTEGER,
    INOUT MESSAGE    VARCHAR(4000) )
  SPECIFIC SQL120113204052200
  LANGUAGE SQL
  NOT DETERMINISTIC
  EXTERNAL ACTION
  MODIFIES SQL DATA
  CALLED ON NULL INPUT
  INHERIT SPECIAL REGISTERS
BEGIN

        --定义错误代码，错误状态
        DECLARE SQLCODE INTEGER DEFAULT 0;
           DECLARE SQLSTATE CHAR(5) DEFAULT '00000';
           DECLARE ERR_MSG VARCHAR(100) DEFAULT ''; 
        DECLARE at_end INTEGER DEFAULT 0;
    
        DECLARE not_found CONDITION FOR SQLSTATE '02000';
           
           --定义临时变量
           DECLARE id_name VARCHAR(50) DEFAULT ''; 
           DECLARE min_id VARCHAR(50) DEFAULT ''; 
           DECLARE max_id VARCHAR(50) DEFAULT ''; 
           DECLARE min_date VARCHAR(10) DEFAULT ''; 
           DECLARE max_date VARCHAR(10) DEFAULT ''; 
           DECLARE temp_sql VARCHAR(3000) DEFAULT ''; 
           DECLARE min_year INTEGER DEFAULT 0; 
           DECLARE max_year INTEGER DEFAULT 0; 
           
           --定义列字段变量
           DECLARE column_name VARCHAR(50) DEFAULT '';
           DECLARE col_name VARCHAR(50) DEFAULT '';          
           DECLARE column_year VARCHAR(10) DEFAULT ''; 
           DECLARE column_month VARCHAR(10) DEFAULT ''; 
           DECLARE column_day VARCHAR(10) DEFAULT ''; 
           DECLARE column_group VARCHAR(1000) DEFAULT '';
           DECLARE column_group_A VARCHAR(1000) DEFAULT '';
           DECLARE column_group_V VARCHAR(1000) DEFAULT '';
           DECLARE temp_group1 VARCHAR(1000) DEFAULT '';
           DECLARE temp_group2 VARCHAR(1000) DEFAULT '';
           DECLARE temp_group3 VARCHAR(1000) DEFAULT '';
           --定义目标表类型和长度
           DECLARE column_type VARCHAR(20) DEFAULT ''; 
           DECLARE column_length INTEGER; 
           --定义源表类型和长度
           DECLARE column_type1 VARCHAR(20) DEFAULT ''; 
           DECLARE column_length1 INTEGER; 
            
           
           --定义源表,目标表模式名和表名
           DECLARE srcschema VARCHAR(128) DEFAULT ''; 
           DECLARE tgtschema VARCHAR(128) DEFAULT ''; 
           DECLARE srctabname VARCHAR(128) DEFAULT ''; 
           DECLARE tgttabname VARCHAR(128) DEFAULT ''; 
              
        --定义全局临时表名称
        --DECLARE temp_etl_sql VARCHAR(50) DEFAULT ''; 
        
         --取得加载表的列字段    
         DECLARE  c1 CURSOR FOR s0;
           
         --自定义错误
           DECLARE CONTINUE HANDLER FOR not_found 
           SET at_end = 1;
           
           --自定义异常处理
           DECLARE CONTINUE HANDLER FOR SQLEXCEPTION 
           SET ERR_MSG = RTRIM(CHAR(CURRENT TIMESTAMP))||' (P_GENERATESQL)系统错误：SQLCODE='||RTRIM(CHAR(SQLCODE))||',SQLSTATE='||SQLSTATE||'';  
    
           --初始化返回状态，0--正常状态，1--异常状态
           SET RUNSTATUS=0;
           SET column_group='';
           IF loadtype='S' THEN
           ELSE
               SET srcschema=LEFT(srctable,LOCATE('.',srctable)-1);
            SET srctabname=RIGHT(srctable,LENGTH(srctable)-LOCATE('.',srctable));
        END IF;
        SET tgtschema=LEFT(tgttable,LOCATE('.',tgttable)-1);
        SET tgttabname=RIGHT(tgttable,LENGTH(tgttable)-LOCATE('.',tgttable));
    
           --随机产生临时表名
        --SET temp_etl_sql=RTRIM('temp' || CAST(BIGINT(RAND()*10000000000) AS CHAR(10)));
        --定义全局临时表
        --SET temp_sql='DECLARE GLOBAL TEMPORARY TABLE ' || temp_etl_sql || '(TGTSCHEMA  VARCHAR(128),TGTTABLE   VARCHAR(128),XH  INTEGER,TYPE  CHARACTER(1),SQL VARCHAR(1000)) NOT LOGGED WITH REPLACE' ;
        --PREPARE s0 FROM temp_sql;
        --EXECUTE s0;
        DECLARE GLOBAL TEMPORARY TABLE ETL_TEMPSQL (TGTSCHEMA  VARCHAR(128),TGTTABLE   VARCHAR(128),XH  INTEGER,TYPE  CHARACTER(1),SQL VARCHAR(3000)) NOT LOGGED WITH REPLACE ;
        
        SET temp_sql='SELECT colname FROM  SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname||''' and colname<>''LSBH'' ORDER BY  colno';     
        PREPARE s0 FROM temp_sql;
        
        IF LOCATE(',',LOADFIELD)<>0 THEN 
          SET col_name=LEFT(LOADFIELD,LOCATE(',',LOADFIELD)-1);
        END IF;
        OPEN  c1;
        fetch_loop:
        LOOP
            FETCH  c1 INTO  column_name;
            IF at_end=1 THEN
                SET at_end=0;
                LEAVE fetch_loop;
            END IF ;
              SET  column_group = column_group || column_name || ',';
              IF column_name<>col_name and column_name<>'KSRQ' AND column_name<>'JSRQ' THEN
                SET  column_group_A = column_group_A || column_name || ',';
              END IF;
              IF column_name<>'KSRQ' AND column_name<>'JSRQ' THEN
                SET  column_group_V = column_group_V || column_name || ',';
              END IF;
        END LOOP fetch_loop;
        CLOSE  c1;
        --除去最后一个逗号
        SET  column_group = LEFT(column_group,LENGTH(column_group)-1);
        SET  column_group_A = LEFT(column_group_A,LENGTH(column_group_A)-1);
        SET  column_group_V = LEFT(column_group_V,LENGTH(column_group_V)-1);
        --过滤流水编号字段
        --SET  column_group = REPLACE(column_group,'LSBH,','');
        
        --当loadvalue为null时设置为''
        IF loadvalue is null THEN
            SET loadvalue='';
        END IF;
        
        --增量加载    
        IF loadtype='I' THEN
            --字段参数为逗号分开的多流水号字段时
            IF LOCATE(',',loadfield)>0 THEN
                SET temp_group3='';
                SET  temp_group1 = loadfield;
                SET  temp_group2 = loadvalue;
                WHILE LOCATE(',',temp_group1)>0 DO
                    SET id_name=LEFT(temp_group1,LOCATE(',',temp_group1)-1);
                    SET max_id=LEFT(temp_group2,LOCATE(',',temp_group2)-1);
                    
                    --取得源表中流水号字段的类型和长度
                    SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||id_name||'''';
                    PREPARE s0 FROM temp_sql;
                    OPEN  c1;
                    FETCH  c1 INTO column_type1,column_length1;
                    CLOSE  c1;
                    
                    --源表流水号类型为VARCHAR类型时
                    IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                        SET temp_group3=temp_group3 ||id_name||'>'''||max_id||''' or ';
                    --源表流水号类型为INTEGER类型时
                    ELSE
                        SET temp_group3=temp_group3 ||id_name||'>'||max_id||' or ';
                    END IF ;
                    
                    SET temp_group1=RIGHT(temp_group1,LENGTH(temp_group1)-LOCATE(',',temp_group1));
                    SET temp_group2=RIGHT(temp_group2,LENGTH(temp_group2)-LOCATE(',',temp_group2));
                END WHILE; 
                
                --取得源表中流水号字段的类型和长度
                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||temp_group1||'''';
                PREPARE s0 FROM temp_sql;
                OPEN  c1;
                FETCH  c1 INTO column_type1,column_length1;
                CLOSE  c1;
                
                --源表流水号类型为VARCHAR类型时
                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                    SET temp_group3=temp_group3 ||temp_group1||'>'''||temp_group2||'''';
                --源表流水号类型为INTEGER类型时
                ELSE
                    SET temp_group3=temp_group3 ||temp_group1||'>'||temp_group2;
                END IF ;
                
                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||temp_group3 ;
             
                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
            --字段参数为单个流水号字段时
            ELSE
                IF loadfield='' THEN
                    --取得目标表中的流水号列
                    SET temp_sql='select colname from SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname ||''' and colno=0';
                    PREPARE s0 FROM temp_sql;
                    OPEN  c1;
                    FETCH  c1 INTO id_name;
                    CLOSE  c1;
                ELSE
                    SET id_name=loadfield;
                END IF ;
                
                --取得源表中流水号字段的类型和长度
                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||id_name||'''';
                PREPARE s0 FROM temp_sql;
                OPEN  c1;
                FETCH  c1 INTO column_type1,column_length1;
                CLOSE  c1;
                
                IF LOCATE(',',loadvalue)>0 THEN
                       SET min_id=LEFT(loadvalue,LOCATE(',',loadvalue)-1);
                       SET max_id=RIGHT(loadvalue,LENGTH(loadvalue)-LOCATE(',',loadvalue));
                       
                       --源表流水号类型为VARCHAR类型时
                    IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||id_name||'>'''||min_id||''' and '||id_name||'<'''||max_id||'''' ;
                    --源表流水号类型为INTEGER类型时
                    ELSE
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||id_name||'>'||min_id||' and '||id_name||'<'||max_id ;
                    END IF ;
                       
                    insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                   
                   ELSE
                       IF LENGTH(loadvalue)>0 THEN
                           SET max_id=loadvalue;
                       ELSE
                           --取得目标表中的最大流水号
                        SET temp_sql='select LTRIM(RTRIM(CHAR(max('||id_name||')))) as maxid from '||tgttable;
                        PREPARE s0 FROM temp_sql;
                        OPEN  c1;
                        FETCH  c1 INTO max_id;
                        CLOSE  c1;
                        
                        IF max_id IS NULL or max_id='' THEN
                               SET max_id='0';
                           END IF;
                           
                           SET loadvalue=max_id;
                           
                       END IF;
                       
                       --源表流水号类型为VARCHAR类型时
                    IF column_type1='VARCHAR' OR column_type1='CHARACTER' OR column_type1='DATE'  THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||id_name||'>'''||max_id||'''';
                    --源表流水号类型为INTEGER类型时
                    ELSE
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||id_name||'>'||max_id;
                    END IF ;
                    
                     insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                     
                END IF;
            END IF;
            
            --写入加载语句
            --SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '''||id_name||'''>='''||max_id||'''';
            --SET temp_sql=temp_sql||';INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '''||id_name||'''>'''||max_id||'''';
            --SET exec_sql='INSERT INTO SESSION.'||temp_etl_sql||' VALUES('''||tgtschema||''','''||tgttabname||''',1,''I'','''||temp_sql||''')'; 
            --PREPARE s0 FROM exec_sql;
            --EXECUTE s0;
            
        --日期加载
        ELSEIF loadtype='P' THEN
            --字段参数为逗号分开的多日期字段时
            IF LOCATE(',',loadfield)>0 THEN
                SET temp_sql=loadfield;
                SET column_year=LEFT(temp_sql,LOCATE(',',temp_sql)-1);
                SET temp_sql=RIGHT(temp_sql,LENGTH(temp_sql)-LOCATE(',',temp_sql));
                --字段参数为逗号分开的年月日三个日期字段时
                IF LOCATE(',',temp_sql)>0 THEN
                    SET column_month=LEFT(temp_sql,LOCATE(',',temp_sql)-1);
                    SET column_day=RIGHT(temp_sql,LENGTH(temp_sql)-LOCATE(',',temp_sql));
                    
                    --取得目标表中日期字段的类型和长度
                    SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname ||''' and COLNAME='''||column_year||'''';
                    PREPARE s0 FROM temp_sql;
                    OPEN  c1;
                    FETCH  c1 INTO column_type,column_length;
                    CLOSE  c1;
                    
                    --参数loadvalue不带值传入时
                    IF loadvalue='' THEN
                        --目标表日期类型为VARCHAR类型时
                        --IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                            --暂时没有这种情况    
                    
                        --目标表日期类型为INTEGER类型时
                        --ELSE
                            --取得目标表中的最大日期
                            SET temp_sql='select max(char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||'))))) as maxdate from '||tgttable;
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO max_date;
                            CLOSE  c1;
                            
                            IF max_date IS NULL or max_date='' THEN
                                   SET max_date='1900-01-01';
                               END IF;
                           
                            SET loadvalue=max_date;
                            
                            --取得最小加载日期
                            IF max_date>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                            END IF;
                            
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||max_date||'''';
                    
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                       
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||'||''-''||'||column_day||' >='''||max_date||'''';
                            --源表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||max_date||'''';
                            END IF ;
                            
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                        --END IF;        
                    --参数loadvalue带值传入时
                    ELSE
                        --日期参数是范围参数时,例如(2005-01-01,2005-12-01)
                        IF LOCATE(',',loadvalue)>0 THEN
                               SET min_date=LEFT(loadvalue,LOCATE(',',loadvalue)-1);
                               SET max_date=RIGHT(loadvalue,LENGTH(loadvalue)-LOCATE(',',loadvalue));
                           
                               --目标日期类型为VARCHAR类型时
                            IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                                --暂时没有这种情况    
                    
                               --目标表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||min_date||''' and char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) <='''||max_date||'''';
                        
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                                --取得源表中日期字段的类型和长度
                                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                                PREPARE s0 FROM temp_sql;
                                OPEN  c1;
                                FETCH  c1 INTO column_type1,column_length1;
                                CLOSE  c1;
                                       
                                --源表日期类型为VARCHAR类型时
                                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||'||''-''||'||column_day||' >='''||min_date||''' and '||column_year||'||''-''||'||column_month||'||''-''||'||column_day||' <='''||max_date||'''';
                                --源表日期类型为INTEGER类型时
                                ELSE
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||min_date||''' and char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) <='''||max_date||'''';
                                END IF ;
                            
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                            END IF;
                                
                        --日期参数是单个参数时,例如(2005-01-01)                  
                           ELSE
                               --取得最小加载日期
                            IF loadvalue>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                            ELSE
                                SET max_date=loadvalue;
                            END IF;        
                                
                            --目标日期类型为VARCHAR类型时
                            IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                                --暂时没有这种情况    
                    
                               --目标表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||max_date||'''';
                    
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                                --取得源表中日期字段的类型和长度
                                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                                PREPARE s0 FROM temp_sql;
                                OPEN  c1;
                                FETCH  c1 INTO column_type1,column_length1;
                                CLOSE  c1;
                                       
                                --源表日期类型为VARCHAR类型时
                                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||'||''-''||'||column_day||' >='''||max_date||'''';
                                --源表日期类型为INTEGER类型时
                                ELSE
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-''||rtrim(char('||column_day||')))) >='''||max_date||'''';
                                END IF ;
                            
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                               END IF;    
                        
                           END IF;
                           
                    END IF;
                    
                --字段参数为逗号分开的年月两个日期字段时
                ELSE
                    SET column_month=temp_sql;
                    
                    --取得目标表中日期字段的类型和长度
                    SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname ||''' and COLNAME='''||column_year||'''';
                    PREPARE s0 FROM temp_sql;
                    OPEN  c1;
                    FETCH  c1 INTO column_type,column_length;
                    CLOSE  c1;
                    
                    --参数loadvalue不带值传入时
                    IF loadvalue='' THEN
                        --目标表日期类型为VARCHAR类型时
                        IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                            --暂时没有这种情况    
                    
                        --目标表日期类型为INTEGER类型时
                        ELSE
                            --取得目标表中的最大日期
                            SET temp_sql='select max(char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-01''))) as maxdate from '||tgttable;
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO max_date;
                            CLOSE  c1;
                            
                            IF max_date IS NULL or max_date='' THEN
                                   SET max_date='1900-01-01';
                               END IF;
                           
                            SET loadvalue=max_date;
                        
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-01'')) >='''||max_date||'''';
                    
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                       
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||'-01'||' >='''||max_date||'''';
                            --源表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-01'')) >='''||max_date||'''';
                            END IF ;
                            
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                        END IF;        
                    --参数loadvalue带值传入时
                    ELSE
                        --日期参数是范围参数时,例如(2005-01,2005-12)
                        IF LOCATE(',',loadvalue)>0 THEN
                               SET min_date=LEFT(loadvalue,LOCATE(',',loadvalue)-1);
                               SET max_date=RIGHT(loadvalue,LENGTH(loadvalue)-LOCATE(',',loadvalue));
                           
                               --目标日期类型为VARCHAR类型时
                            IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                                --暂时没有这种情况    
                    
                               --目标表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||')))) >='''||min_date||''' and char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||')))) <='''||max_date||'''';
                        
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                                --取得源表中日期字段的类型和长度
                                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                                PREPARE s0 FROM temp_sql;
                                OPEN  c1;
                                FETCH  c1 INTO column_type1,column_length1;
                                CLOSE  c1;
                                       
                                --源表日期类型为VARCHAR类型时
                                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||' >='''||min_date||''' and '||column_year||'||''-''||'||column_month||'-01'||' <='''||max_date||'''';
                                --源表日期类型为INTEGER类型时
                                ELSE
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||')))) >='''||min_date||''' and char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||'))||''-01'')) <='''||max_date||'''';
                                END IF ;
                            
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                            END IF;
                                
                        --日期参数是单个参数时,例如(2005-01-01)                  
                           ELSE
                               SET max_date=loadvalue;
                                
                            --目标日期类型为VARCHAR类型时
                            IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                                --暂时没有这种情况    
                    
                               --目标表日期类型为INTEGER类型时
                            ELSE
                                SET temp_sql='DELETE FROM '|| tgttable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||')))) >='''||max_date||'''';
                    
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                                --取得源表中日期字段的类型和长度
                                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||column_year||'''';
                                PREPARE s0 FROM temp_sql;
                                OPEN  c1;
                                FETCH  c1 INTO column_type1,column_length1;
                                CLOSE  c1;
                                       
                                --源表日期类型为VARCHAR类型时
                                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||column_year||'||''-''||'||column_month||' >='''||max_date||'''';
                                --源表日期类型为INTEGER类型时
                                ELSE
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE char(date(rtrim(char('||column_year||'))||''-''||rtrim(char('||column_month||')))) >='''||max_date||'''';
                                END IF ;
                            
                                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       
                               END IF;    
                        
                           END IF;
                           
                    END IF;
                    
                END IF;
            
            --字段参数为单个日期字段时
            ELSE
                --取得目标表中日期字段的类型和长度
                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname ||''' and COLNAME='''||loadfield||'''';
                PREPARE s0 FROM temp_sql;
                OPEN  c1;
                FETCH  c1 INTO column_type,column_length;
                CLOSE  c1;
            
                --参数loadvalue不带值传入时
                IF loadvalue='' THEN
                    --目标表日期类型为VARCHAR类型时
                    IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                        --取得目标表中的最大日期
                        SET temp_sql='select max('||loadfield||') as maxdate from '||tgttable;
                        PREPARE s0 FROM temp_sql;
                        OPEN  c1;
                        FETCH  c1 INTO max_date;
                        CLOSE  c1;
                        
                        IF max_date IS NULL or max_date='' THEN
                               SET max_date='1900-01-01';
                           END IF;
                           
                        --取得最小加载日期
                        IF column_length=8 THEN
                            IF max_date>REPLACE(CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)),'-','') THEN
                                SET max_date=REPLACE(CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)),'-','');
                            END IF;
                        ELSEIF column_length=10 THEN
                            IF max_date>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                            END IF;
                        END IF;

                        SET loadvalue=max_date;
                        
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'>='''||max_date||'''';
                
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                        --取得源表中日期字段的类型和长度
                        SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                        PREPARE s0 FROM temp_sql;
                        OPEN  c1;
                        FETCH  c1 INTO column_type1,column_length1;
                        CLOSE  c1;
                                   
                        --源表日期类型为VARCHAR类型时
                        IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||max_date||'''';
                        --源表日期类型为DATE类型时
                        ELSE
                            IF column_length=8 THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE REPLACE(CAST('||loadfield ||' AS VARCHAR(10)),''-'','')>='''||max_date||'''';
                            ELSEIF column_length=10 THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||max_date||'''';
                            END IF;
                        END IF ;
                        
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                    --日期类型为INTEGER类型时    
                    ELSEIF column_type='INTEGER' AND column_length=4 THEN 
                        IF column_length=4 THEN
                            SET max_date=cast (year(current date -1 day) as char(4));
                        END IF;                        
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='||max_date||'';                
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);                        
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='||max_date||'';
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                       --目标表日期类型为DATE类型时
                    ELSE
                        --取得目标表中的最大日期
                        SET temp_sql='select CAST(max('||loadfield||') as VARCHAR(10)) as maxdate from '||tgttable;
                        PREPARE s0 FROM temp_sql;
                        OPEN  c1;
                        FETCH  c1 INTO max_date;
                        CLOSE  c1;
                        
                        IF max_date IS NULL or max_date='' THEN
                               SET max_date='1900-01-01';
                           END IF;
                           
                        SET loadvalue=max_date;
                        
                        --取得最小加载日期
                        IF max_date>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                            SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                        END IF;
                        
                           SET loadvalue=max_date;
                        
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||max_date||'''';
                
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                                        
                        --取得源表中日期字段的类型和长度
                        SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                        PREPARE s0 FROM temp_sql;
                        OPEN  c1;
                        FETCH  c1 INTO column_type1,column_length1;
                        CLOSE  c1;
                                   
                        --源表日期类型为VARCHAR类型时
                        IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                            IF column_length1=8 THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||REPLACE(max_date,'-','')||'''';
                            ELSEIF column_length1=10 THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||max_date||'''';
                            END IF;
                        --源表日期类型为DATE类型时
                        ELSE
                            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||max_date||'''';
                        END IF ;
                        
                        insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                    
                    END IF;
                 --参数loadvalue带值传入时
                ELSE
                    --日期参数是范围参数时,例如(2005-01-01,2005-12-01)
                    IF LOCATE(',',loadvalue)>0 THEN
                           SET min_date=LEFT(loadvalue,LOCATE(',',loadvalue)-1);
                           SET max_date=RIGHT(loadvalue,LENGTH(loadvalue)-LOCATE(',',loadvalue));
                       
                           --目标日期类型为VARCHAR类型时
                        IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                            IF column_length=8 THEN
                                SET min_date=replace(min_date,'-','');
                                SET max_date=replace(max_date,'-','');
                            ELSEIF column_length=10 THEN
                                
                            END IF;
                        
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'>='''||min_date||''' and '||loadfield||'<='''||max_date||'''';
                
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                   
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||min_date||''' and '||loadfield||'<='''||max_date||'''';
                            --源表日期类型为DATE类型时
                            ELSE
                                IF column_length=8 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE REPLACE(CAST('||loadfield ||' AS VARCHAR(10)),''-'','')>='''||min_date||''' and REPLACE(CAST('||loadfield ||' AS VARCHAR(10)),''-'','')<='''||max_date||'''';
                                ELSEIF column_length=10 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||min_date||''' and CAST('||loadfield||' AS VARCHAR(10))<='''||max_date||'''';
                                END IF;
                            END IF ;
                        
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                        
                        --日期类型为DATE类型时
                        ELSE
                            
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE CAST('||loadfield ||' AS VARCHAR(10))>='''||min_date||''' and CAST('||loadfield ||' AS VARCHAR(10))<='''||max_date||'''';
                
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                   
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                IF column_length1=8 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||REPLACE(min_date,'-','')||''' and '||loadfield||'<='''||REPLACE(max_date,'-','')||'''';
                                ELSEIF column_length1=10 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||min_date||''' and '||loadfield||'<='''||max_date||'''';
                                END IF;
                            --源表日期类型为DATE类型时
                            ELSE
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||min_date||''' and CAST('||loadfield||' AS VARCHAR(10))<='''||max_date||'''';
                            END IF ;
                        
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                    
                        END IF;
                       --日期参数是单个参数时,例如(2005-01-01)                  
                       ELSE
                           --目标日期类型为VARCHAR类型时
                        IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                            IF column_length=8 THEN
                                --取得最小加载日期
                                IF loadvalue>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                    SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                                ELSE
                                    SET max_date=loadvalue;
                                END IF;    
                            
                                SET max_date=replace(max_date,'-','');
                            ELSEIF column_length=10 THEN
                                --取得最小加载日期
                                IF loadvalue>CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                    SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                                ELSE
                                    SET max_date=loadvalue;
                                END IF;        
                            END IF;
                        
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'>='''||max_date||'''';
                
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                   
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||max_date||'''';
                            --源表日期类型为DATE类型时
                            ELSE
                                IF column_length=8 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE REPLACE(CAST('||loadfield ||' AS VARCHAR(10)),''-'','')>='''||max_date||'''';
                                ELSEIF column_length=10 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||max_date||'''';
                                END IF;
                            END IF ;
                        
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                        --日期类型为INTEGER类型时    
                        ELSEIF column_type='INTEGER' AND column_length=4 THEN 
                            IF column_length=4 THEN
                                SET max_date=loadvalue;
                            END IF;                        
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='||max_date||'';                
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);                        
                            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='||max_date||'';
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                        --日期类型为DATE类型时
                        ELSE
                            --取得最小加载日期
                            IF loadvalue >CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)) THEN
                                SET max_date=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                            ELSE
                                SET max_date=loadvalue;
                            END IF;
                        
                            SET temp_sql='DELETE FROM '|| tgttable ||' WHERE CAST('||loadfield ||' AS VARCHAR(10))>='''||max_date||'''';
                
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                                        
                            --取得源表中日期字段的类型和长度
                            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                            PREPARE s0 FROM temp_sql;
                            OPEN  c1;
                            FETCH  c1 INTO column_type1,column_length1;
                            CLOSE  c1;
                                   
                            --源表日期类型为VARCHAR类型时
                            IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                                IF column_length1=8 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||REPLACE(max_date,'-','')||'''';
                                ELSEIF column_length1=10 THEN
                                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'>='''||max_date||'''';
                                END IF;
                            --源表日期类型为DATE类型时
                            ELSE
                                SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))>='''||max_date||'''';
                            END IF ;
                        
                            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                    
                        END IF;
                       
                       END IF;
               
                END IF;
                
            END IF;
    
        --全部加载
        ELSEIF loadtype='C' THEN
            SET temp_sql='DELETE FROM '|| tgttable ;
            
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                
            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ;
            
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                   
    
        --日期加载（只更新昨日数据）
        ELSEIF loadtype='D' THEN
            --取得目标表中日期字段的类型和长度
            SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||tgtschema ||''' and TABNAME ='''||tgttabname ||''' and COLNAME='''||loadfield||'''';
            PREPARE s0 FROM temp_sql;
            OPEN  c1;
            FETCH  c1 INTO column_type,column_length;
            CLOSE  c1;
            
            --参数loadvalue不带值传入时
            IF loadvalue='' THEN
                SET loadvalue=CAST(CURRENT DATE -1 DAYS AS VARCHAR(10));
                
                --目标表日期类型为VARCHAR类型时
                IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                    IF column_length=8 THEN
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='''||REPLACE(CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)),'-','')||'''';
                    ELSEIF column_length=10 THEN
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='''||CAST(CURRENT DATE -1 DAYS AS VARCHAR(10))||'''';
                    END IF;         
                --日期类型为DATE类型时
                ELSE
                    SET temp_sql='DELETE FROM '|| tgttable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))='''||CAST(CURRENT DATE -1 DAYS AS VARCHAR(10))||'''';
                END IF ;
            
                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                
                --取得源表中日期字段的类型和长度
                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                PREPARE s0 FROM temp_sql;
                OPEN  c1;
                FETCH  c1 INTO column_type1,column_length1;
                CLOSE  c1;
                               
                --源表日期类型为VARCHAR类型时
                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                    IF column_length1=8 THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='''||REPLACE(CAST(CURRENT DATE -1 DAYS AS VARCHAR(10)),'-','')||'''';
                    ELSEIF column_length1=10 THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='''||CAST(CURRENT DATE -1 DAYS AS VARCHAR(10))||'''';
                    END IF;
                --源表日期类型为DATE类型时
                ELSE
                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))='''||CAST(CURRENT DATE -1 DAYS AS VARCHAR(10))||'''';
                END IF ;
                    
                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
            --参数loadvalue带值传入时
            ELSE
                --目标表日期类型为VARCHAR类型时
                IF column_type='VARCHAR' OR column_type='CHARACTER' THEN
                    IF column_length=8 THEN
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='''||REPLACE(loadvalue,'-','')||'''';
                    ELSEIF column_length=10 THEN
                        SET temp_sql='DELETE FROM '|| tgttable ||' WHERE '||loadfield||'='''||loadvalue||'''';
                    END IF;         
                --日期类型为DATE类型时
                ELSE
                    SET temp_sql='DELETE FROM '|| tgttable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))='''||loadvalue||'''';
                END IF ;
            
                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'D',temp_sql);
                
                --取得源表中日期字段的类型和长度
                SET temp_sql='select typename,length from SYSCAT.COLUMNS WHERE TABSCHEMA='''||srcschema ||''' and TABNAME ='''||srctabname ||''' and COLNAME='''||loadfield||'''';
                PREPARE s0 FROM temp_sql;
                OPEN  c1;
                FETCH  c1 INTO column_type1,column_length1;
                CLOSE  c1;
                               
                --源表日期类型为VARCHAR类型时
                IF column_type1='VARCHAR' OR column_type1='CHARACTER' THEN
                    IF column_length1=8 THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='''||REPLACE(loadvalue,'-','')||'''';
                    ELSEIF column_length1=10 THEN
                        SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE '||loadfield||'='''||loadvalue||'''';
                    END IF;
                --源表日期类型为DATE类型时
                ELSE
                    SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||column_group||' FROM '||srctable ||' WHERE CAST('||loadfield||' AS VARCHAR(10))='''||loadvalue||'''';
                END IF ;
            
                insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
                END IF;
        --脚本加载
        ELSEIF loadtype='S' THEN
            --存在加载脚本时
            /*
            IF EXISTS (SELECT 1 FROM MD.T_ETL_SQL WHERE TGTSCHEMA=tgtschema  and TGTTABLE =tgttabname) THEN
                IF loadvalue='' THEN
                    SET temp_sql='insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA='''||tgtschema||''' and TGTTABLE = '''||tgttabname||'''';
                
                    --insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA=tgtschema and TGTTABLE = tgttabname;    
                ELSE
                    SET temp_sql='insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,REPLACE(SQL,loadfield,loadvalue) as SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA='''||tgtschema||''' and TGTTABLE = '''||tgttabname||'''';
                    --insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,REPLACE(SQL,loadfield,loadvalue) as SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA=tgtschema and TGTTABLE = tgttabname;    
                END IF;
            ELSE
                --不存在加载脚本时
                SET RUNSTATUS=2;
            SET MESSAGE=MESSAGE||RTRIM(CHAR(CURRENT TIMESTAMP))||' (P_GENERATESQL)错误：MD.T_ETL_SQL表中不存在要加载的目标表'||tgtschema||'.'||tgttabname||';  ';
            END IF;
            */    
            IF loadvalue='' THEN
                SET temp_sql='insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA='''||tgtschema||''' and TGTTABLE = '''||tgttabname||'''';
                
                    --insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA=tgtschema and TGTTABLE = tgttabname;    
            ELSE
                   SET temp_sql='insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,REPLACE(SQL,'''||loadfield||''','''||loadvalue||''') as SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA='''||tgtschema||''' and TGTTABLE = '''||tgttabname||'''';
                    --insert into SESSION.ETL_TEMPSQL(TGTSCHEMA,TGTTABLE,XH,TYPE,SQL) SELECT TGTSCHEMA,TGTTABLE,XH,TYPE,REPLACE(SQL,loadfield,loadvalue) as SQL FROM MD.T_ETL_SQL WHERE TGTSCHEMA=tgtschema and TGTTABLE = tgttabname;    
            END IF;
            
            PREPARE s0 FROM temp_sql;
            EXECUTE s0;
                        
        --代码表加载
        ELSEIF loadtype='V' THEN
            --过滤流水编号字段
            --SET  column_group = REPLACE(column_group,'LSBH,','');
            --过滤开始日期字段
            --SET  column_group = REPLACE(column_group,',KSRQ','');
            --过滤结束日期字段
            --SET  column_group = REPLACE(column_group,',JSRQ','');
            
            SET column_group=column_group_V;
            
            SET temp_sql='UPDATE '||tgttable||' SET JSRQ = CURRENT DATE WHERE ('||column_group||') IN (SELECT '||column_group||' FROM (SELECT '||column_group||' FROM '||tgttable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ EXCEPT SELECT '||column_group||' FROM '||srctable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ ) AS TA) AND CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ';
            
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'U',temp_sql);
            
            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||',KSRQ,JSRQ) SELECT '||column_group||',CURRENT DATE, ''3000-12-31'' FROM (SELECT '||column_group||' FROM '||srctable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ EXCEPT SELECT '||column_group||' FROM '||tgttable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ ) AS TA';
        
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
        
        --维度表加载
        ELSEIF loadtype='W' THEN
            --过滤流水编号字段
            --SET  column_group = REPLACE(column_group,'LSBH,','');
            SET  temp_group1 ='A.'|| REPLACE(column_group,',',',A.');
            
            SET temp_sql='UPDATE '||tgttable||' AS A SET JSRQ = (SELECT B.JSRQ FROM '||srctable||' AS B WHERE A.'||loadfield||'=B.'||loadfield||' AND (A.KSRQ<B.JSRQ AND A.JSRQ>B.KSRQ) AND (A.KSRQ>=B.KSRQ AND A.JSRQ>B.JSRQ)) WHERE ('||column_group||') IN (SELECT '||temp_group1||' FROM '||srctable||' AS B WHERE A.'||loadfield||'=B.'||loadfield||' AND (A.KSRQ<B.JSRQ AND A.JSRQ>B.KSRQ) AND (A.KSRQ>=B.KSRQ AND A.JSRQ>B.JSRQ))'; 
                        
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'U',temp_sql);
            
            SET  temp_group1 ='B.'|| REPLACE(column_group,',',',B.');
            SET temp_sql='INSERT INTO '||tgttable||'('||column_group||') SELECT '||temp_group1||' FROM '||srctable||' AS B WHERE ('||column_group||') NOT IN (SELECT '||temp_group1||' FROM '||tgttable||' AS A WHERE A.'||loadfield||'=B.'||loadfield||' AND (A.KSRQ<B.JSRQ AND A.JSRQ>B.KSRQ))';
        
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
        --特殊代码表加载
        ELSEIF loadtype='A' THEN
            SET  temp_group1 = loadfield;
            SET  column_name='';
            WHILE LOCATE(',',temp_group1)>0 DO
                IF  column_name='' THEN
                    SET column_name=LEFT(temp_group1,LOCATE(',',temp_group1)-1);
                    SET temp_group2=RIGHT(temp_group1,LENGTH(temp_group1)-LOCATE(',',temp_group1));
                END IF;
        
                SET temp_group1=RIGHT(temp_group1,LENGTH(temp_group1)-LOCATE(',',temp_group1));
        
                IF LOCATE(',',temp_group1)>0 THEN
                    SET temp_group3=temp_group3 ||'TA.'||LEFT(temp_group1,LOCATE(',',temp_group1)-1)||'=TB.'||LEFT(temp_group1,LOCATE(',',temp_group1)-1)||' AND ';
                ELSE
                    SET temp_group3=temp_group3 ||'TA.'||temp_group1||'=TB.'||temp_group1||' ';
                END IF;
            END WHILE; 
            
        
            --过滤流水编号字段
            --SET  column_group = REPLACE(column_group,'LSBH,','');
            --过滤代码标识字段
            SET  column_group = REPLACE(column_group,column_name||',','');
            --过滤开始日期字段
            --SET  column_group = REPLACE(column_group,',KSRQ','');
            --过滤结束日期字段
            --SET  column_group = REPLACE(column_group,',JSRQ','');
            
            SET column_group=column_group_A;
                        
            SET temp_sql='UPDATE '||tgttable||' SET JSRQ = CURRENT DATE WHERE ('||column_group||') IN (SELECT '||column_group||' FROM (SELECT '||column_group||' FROM '||tgttable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ EXCEPT SELECT '||column_group||' FROM '||srctable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ ) AS TA) AND CURRENT DATE>=KSRQ AND CURRENT DATE<JSRQ';
            
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,1,'U',temp_sql);
            
            SET temp_sql='INSERT INTO '||tgttable||'('||column_name||','||column_group||',KSRQ,JSRQ) SELECT (SELECT COALESCE(MAX('||column_name||'),0) FROM '||tgttable||') + DENSE_RANK() OVER(ORDER BY '||temp_group2||') AS '||column_name||','||column_group||',CURRENT DATE, ''3000-12-31'' FROM (SELECT '||column_group||' FROM '||srctable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ EXCEPT SELECT '||column_group||' FROM '||tgttable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ ) AS TA WHERE ('||temp_group2||') NOT IN (SELECT DISTINCT '||temp_group2||' FROM '||tgttable||')';
        
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,2,'I',temp_sql);
            
            SET temp_sql='INSERT INTO '||tgttable||'('||column_name||','||column_group||',KSRQ,JSRQ) SELECT (SELECT '||column_name||' FROM '||tgttable||' AS TB WHERE '||temp_group3||' FETCH FIRST 1 ROWS ONLY) AS '||column_name||','||column_group||',CURRENT DATE, ''3000-12-31'' FROM (SELECT '||column_group||' FROM '||srctable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ EXCEPT SELECT '||column_group||' FROM '||tgttable||' WHERE CURRENT DATE >= KSRQ AND CURRENT DATE < JSRQ ) AS TA WHERE ('||temp_group2||') IN (SELECT DISTINCT '||temp_group2||' FROM '||tgttable||')';
            
            insert into SESSION.ETL_TEMPSQL VALUES(tgtschema,tgttabname,3,'I',temp_sql);
        END IF;
  
      --执行过程中发生异常错误写入日志
      IF ERR_MSG<>'' THEN
          SET RUNSTATUS=1;
          SET MESSAGE=MESSAGE||ERR_MSG;
    END IF;
   END;
