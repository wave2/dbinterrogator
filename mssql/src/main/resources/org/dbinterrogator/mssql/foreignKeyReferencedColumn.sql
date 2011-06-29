SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
cstr.name AS [ForeignKey_Name],
fk.constraint_column_id AS [ID],
cfk.name AS [Name],
crk.name AS [ReferencedColumn]
FROM
sys.tables AS tbl
INNER JOIN sys.foreign_keys AS cstr ON cstr.parent_object_id=tbl.object_id
INNER JOIN sys.foreign_key_columns AS fk ON fk.constraint_object_id=cstr.object_id
INNER JOIN sys.columns AS cfk ON fk.parent_column_id = cfk.column_id and fk.parent_object_id = cfk.object_id
INNER JOIN sys.columns AS crk ON fk.referenced_column_id = crk.column_id and fk.referenced_object_id = crk.object_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[ForeignKey_Name] ASC,[ID] ASC