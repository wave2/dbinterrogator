/**
* Retrieve routine definition
* @param  schema    Schema Name
* @param  routine   Routine Name
* @returns String Routine Definition
*/
SELECT ROUTINE_DEFINITION FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = ? AND ROUTINE_NAME=?