/**
* Retrieve list of columns in table
*
* @param  schema  Schema Name
* @param  table  Table Name
*
* @return  list of columns in table
*/
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = ?
AND TABLE_NAME = ?
AND COLUMN_KEY = 'PRI'
ORDER BY ORDINAL_POSITION