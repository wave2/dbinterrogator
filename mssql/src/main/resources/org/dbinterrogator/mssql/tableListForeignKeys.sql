SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
cstr.name AS [Name],
CAST(cstr.is_system_named AS bit) AS [IsSystemNamed],
cstr.delete_referential_action AS [DeleteAction],
cstr.update_referential_action AS [UpdateAction],
rtbl.name AS [ReferencedTable],
schema_name(rtbl.schema_id) AS [ReferencedTableSchema],
~cstr.is_not_trusted AS [IsChecked],
~cstr.is_disabled AS [IsEnabled],
cstr.is_not_for_replication AS [NotForReplication]
FROM
sys.tables AS tbl
INNER JOIN sys.foreign_keys AS cstr ON cstr.parent_object_id=tbl.object_id
INNER JOIN sys.tables rtbl ON rtbl.object_id = cstr.referenced_object_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[Name] ASC