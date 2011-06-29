DECLARE @is_policy_automation_enabled bit;
SET @is_policy_automation_enabled  = (SELECT CONVERT(bit, current_value)
                                              FROM msdb.dbo.syspolicy_configuration
                                              WHERE name = 'Enabled');
      
SELECT
tbl.name AS [Name],
SCHEMA_NAME(tbl.schema_id) AS [Schema],
'Server[@Name=' + quotename(CAST(serverproperty(N'Servername') AS sysname),'''''''') + ']' + '/Database[@Name=' + quotename(db_name(),'''''''') + ']' + '/Table[@Name=' + quotename(tbl.name,'''''''') + ' and @Schema=' + quotename(SCHEMA_NAME(tbl.schema_id),'''''''') + ']' AS [Urn],
case when 1=@is_policy_automation_enabled and exists (select * from msdb.dbo.syspolicy_system_health_state where target_query_expression_with_id like 'Server' + '/Database\[@ID=' + convert(nvarchar(20),dtb.database_id) + '\]'+ '/Table\[@ID=' + convert(nvarchar(20),tbl.object_id) + '\]%' ESCAPE '\') then 1 else 0 end AS [PolicyHealthState],
tbl.create_date AS [CreateDate],
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
             AS bit) AS [IsSystemObject]
FROM
master.sys.databases AS dtb,
sys.tables AS tbl
WHERE
(CAST(
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
             AS bit)='0')and((db_name()=?)and(dtb.name=db_name()))
ORDER BY
[Schema] ASC,[Name] ASC;