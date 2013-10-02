drop FUNCTION MATH.F_MOD_DEC;

CREATE FUNCTION  MATH.F_MOD_DEC( 
  val DOUBLE, 
  mod DOUBLE 
) 
  RETURNS DOUBLE
  SPECIFIC MATH.F_MOD_DEC 
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  CONTAINS SQL
lbmain: 
begin atomic 
  declare lc_n_tmp double; 
  if val<0 or mod<=0 then 
    SIGNAL SQLSTATE '99999' 
      SET MESSAGE_TEXT = 'error:invalid params,the second param cann''t be a minus or zero'; 
  end if; 
  
  set lc_n_tmp = val;
  while lc_n_tmp>mod 
  do 
    set lc_n_tmp = lc_n_tmp - mod; 
  end while; 
  
  RETURN lc_n_tmp; 
end lbmain;