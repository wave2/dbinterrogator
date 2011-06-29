SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
i.name AS [Index_Name],
(case ic.is_included_column when 1 then ic.index_column_id else ic.key_ordinal end) AS [ID],
clmns.name AS [Name],
ic.is_included_column AS [IsIncluded],
ic.is_descending_key AS [Descending],
CAST(COLUMNPROPERTY(ic.object_id, clmns.name, N''IsComputed'') AS bit) AS [IsComputed]
FROM
sys.tables AS tbl
INNER JOIN sys.indexes AS i ON (i.index_id > 0 and i.is_hypothetical = 0) AND (i.object_id=tbl.object_id)
INNER JOIN sys.index_columns AS ic ON (ic.column_id > 0 and (ic.key_ordinal > 0 or ic.partition_ordinal = 0 or ic.is_included_column != 0)) AND (ic.index_id=CAST(i.index_id AS int) AND ic.object_id=i.object_id)
INNER JOIN sys.columns AS clmns ON clmns.object_id = ic.object_id and clmns.column_id = ic.column_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC