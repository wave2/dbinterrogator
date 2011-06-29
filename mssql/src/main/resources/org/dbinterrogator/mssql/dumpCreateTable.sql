/**
* Retrieve table information
* @param  tbl.name  Table Name
* @param  SCHEMA()  Schema Name
* @returns 
*/
SELECT
SCHEMA_NAME(tbl.schema_id) AS [Schema],
tbl.name AS [Name],
tbl.uses_ansi_nulls AS [AnsiNullsStatus],
ISNULL(dstext.name,N'') AS [TextFileGroup],
CASE WHEN 'FG'=dsidx.type THEN dsidx.name ELSE N'' END AS [FileGroup],
CASE WHEN 'FD'=dstbl.type THEN dstbl.name ELSE N'' END AS [FileStreamFileGroup],
CASE WHEN 'PS'=dstbl.type THEN dstbl.name ELSE N'' END AS [FileStreamPartitionScheme],
CASE WHEN 'PS'=dsidx.type THEN dsidx.name ELSE N'' END AS [PartitionScheme],
CAST(
 case
    when tbl.is_ms_shipped = 1 then 1
    when (
        select
            major_id
        from
            sys.extended_properties
        where
            major_id = tbl.object_id and
            minor_id = 0 and
            class = 1 and
            name = N'microsoft_database_tools_support')
        is not null then 1
    else 0
end
             AS bit) AS [IsSystemObject],
CAST(CASE WHEN 'PS'=dsidx.type THEN 1 ELSE 0 END AS bit) AS [IsPartitioned],
CAST(OBJECTPROPERTY(tbl.object_id,N'IsQuotedIdentOn') AS bit) AS [QuotedIdentifierStatus],
tbl.object_id AS [ID],
CAST(case when ctt.object_id is null then 0 else 1  end AS bit) AS [ChangeTrackingEnabled],
CAST(ISNULL(ctt.is_track_columns_updated_on,0) AS bit) AS [TrackColumnsUpdatedEnabled],
tbl.lock_escalation AS [LockEscalation],
ISNULL(stbl.name, N'') AS [Owner],
CAST(case when tbl.principal_id is null then 1 else 0 end AS bit) AS [IsSchemaOwned],
CAST(
			case
    				when databasepropertyex(DB_NAME(), 'version') < 612 then 0
    				else objectproperty(tbl.object_id, 'TableHasVarDecimalStorageFormat')
			end
            		 AS bit) AS [IsVarDecimalStorageFormatEnabled]
FROM
sys.tables AS tbl
LEFT OUTER JOIN sys.data_spaces AS dstext  ON tbl.lob_data_space_id = dstext.data_space_id
INNER JOIN sys.indexes AS idx ON idx.object_id = tbl.object_id and idx.index_id < 2
LEFT OUTER JOIN sys.data_spaces AS dsidx ON dsidx.data_space_id = idx.data_space_id
LEFT OUTER JOIN sys.tables AS t ON t.object_id = idx.object_id
LEFT OUTER JOIN sys.data_spaces AS dstbl ON dstbl.data_space_id = t.Filestream_data_space_id and idx.index_id < 2
LEFT OUTER JOIN sys.change_tracking_tables AS ctt ON ctt.object_id = tbl.object_id
LEFT OUTER JOIN sys.database_principals AS stbl ON stbl.principal_id = ISNULL(tbl.principal_id, (OBJECTPROPERTY(tbl.object_id, 'OwnerId')))
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Schema] ASC,[Name] ASC