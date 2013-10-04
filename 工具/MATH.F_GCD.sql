drop FUNCTION MATH.F_GCD;

CREATE FUNCTION MATH.F_GCD( 
  VAL1 INTEGER, 
  VAL2 INTEGER 
) 
  RETURNS INTEGER
  SPECIFIC math.F_GCD 
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  CONTAINS SQL
lbmain:
begin atomic 
  declare lc_i_gval, lc_i_lval, lc_i_tmp INTEGER; 
  
  --控制判断
  if VAL1 is null or VAL2 is null then 
    SIGNAL SQLSTATE '99999' 
      SET MESSAGE_TEXT = 'error:invalid params,the param cann''t be a null value'; 
  end if; 
  --小于等于0判断
  if VAL1<=0 or VAL2<=0 then 
    SIGNAL SQLSTATE '99999' 
      SET MESSAGE_TEXT = 'error:invalid params,the param cann''t be a minus or zero'; 
  end if; 
  
  set lc_i_gval = GREATEST(VAL1,VAL2); 
  set lc_i_lval = LEAST(VAL1,VAL2); 
  
  while mod(lc_i_gval,lc_i_lval)!=0 
  do 
    set lc_i_tmp = lc_i_lval;
    set lc_i_lval = mod(lc_i_gval,lc_i_lval); 
    set lc_i_gval = lc_i_tmp; 
  end while; 
  
  return lc_i_lval; 
  
end; 
