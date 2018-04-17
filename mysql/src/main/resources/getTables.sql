/**
* Retrieve list of tables in schema
*
* @param  schema  Schema Name
*/
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?