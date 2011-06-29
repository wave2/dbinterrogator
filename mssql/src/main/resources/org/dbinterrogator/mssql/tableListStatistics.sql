SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
st.name AS [Statistic_Name],
sic.stats_column_id AS [ID],
COL_NAME(sic.object_id, sic.column_id) AS [Name]
FROM
sys.tables AS tbl
INNER JOIN sys.stats st ON st.object_id=tbl.object_id
INNER JOIN sys.stats_columns sic ON sic.stats_id=st.stats_id AND sic.object_id=st.object_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[Statistic_Name] ASC,[ID] ASC