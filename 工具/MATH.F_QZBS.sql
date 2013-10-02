drop FUNCTION MATH.F_QZBS;

CREATE FUNCTION  MATH.F_QZBS ( val DOUBLE )
  RETURNS DOUBLE
  LANGUAGE SQL
  NOT DETERMINISTIC
  NO EXTERNAL ACTION
  CONTAINS SQL
RETURN (case when val is null then cast(null as double) when val<>int(val) then int(val+1) - val else 0 end);