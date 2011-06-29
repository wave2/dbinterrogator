SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
st.name AS [Name],
st.no_recompute AS [NoAutomaticRecomputation],
ISNULL(st.filter_definition ,N'') AS [FilterDefinition],
st.auto_created AS [IsAutoCreated],
CAST(1 - INDEXPROPERTY(st.object_id,st.name,N'IsStatistics') AS bit) AS [IsFromIndexCreation]
FROM
sys.tables AS tbl
INNER JOIN sys.stats st ON st.object_id=tbl.object_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[Name] ASC