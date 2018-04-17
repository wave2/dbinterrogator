/**
* Retrieve the maximum value from a column
*
* @param  column  Column Name
* @param  schema  Schema Name
* @param  table  Table Name
*
* @return  Max value stored in supplied column
*/
SELECT MAX(%s) AS MAX_VALUE
FROM %s.%s