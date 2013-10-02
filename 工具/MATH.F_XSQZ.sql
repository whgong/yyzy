drop FUNCTION MATH.F_XSQZ;

CREATE FUNCTION  MATH.F_XSQZ( 
  val DOUBLE, 
  mod DOUBLE 
) 
  RETURNS DOUBLE
  SPECIFIC math.F_XSQZ 
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  CONTAINS SQL
RETURN 
  (case 
    when val is null 
      then cast(null as double) 
    when 0<>MATH.F_MOD_DEC(val, mod) 
      then int(val/mod)*mod + mod
    else val 
  end)
;