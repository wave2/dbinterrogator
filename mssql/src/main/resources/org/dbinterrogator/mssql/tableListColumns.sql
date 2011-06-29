SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
clmns.column_id AS [ID],
clmns.name AS [Name],
clmns.is_ansi_padded AS [AnsiPaddingStatus],
clmns.is_computed AS [Computed],
ISNULL(cc.definition,N'') AS [ComputedText],
usrt.name AS [DataType],
ISNULL(baset.name, N'') AS [SystemType],
sclmns.name AS [DataTypeSchema],
CAST(clmns.is_rowguidcol AS bit) AS [RowGuidCol],
CAST(CASE WHEN baset.name IN (N'nchar', N'nvarchar') AND clmns.max_length <> -1 THEN clmns.max_length/2 ELSE clmns.max_length END AS int) AS [Length],
CAST(clmns.precision AS int) AS [NumericPrecision],
clmns.is_identity AS [Identity],
CAST(ISNULL(ic.seed_value,0) AS bigint) AS [IdentitySeed],
CAST(ISNULL(ic.increment_value,0) AS bigint) AS [IdentityIncrement],
ISNULL(clmns.collation_name, N'''') AS [Collation],
CAST(clmns.scale AS int) AS [NumericScale],
clmns.is_nullable AS [Nullable],
CAST(clmns.is_filestream AS bit) AS [IsFileStream],
ISNULL(ic.is_not_for_replication, 0) AS [NotForReplication],
(case when clmns.default_object_id = 0 then N'' when d.parent_object_id > 0 then N'' else d.name end) AS [Default],
(case when clmns.default_object_id = 0 then N'' when d.parent_object_id > 0 then N'' else schema_name(d.schema_id) end) AS [DefaultSchema],
(case when clmns.rule_object_id = 0 then N'' else r.name end) AS [Rule],
(case when clmns.rule_object_id = 0 then N'' else schema_name(r.schema_id) end) AS [RuleSchema],
ISNULL(xscclmns.name, N'') AS [XmlSchemaNamespace],
ISNULL(s2clmns.name, N'') AS [XmlSchemaNamespaceSchema],
ISNULL( (case clmns.is_xml_document when 1 then 2 else 1 end), 0) AS [XmlDocumentConstraint],
CAST(ISNULL(cc.is_persisted, 0) AS bit) AS [IsPersisted],
CAST(ISNULL((select TOP 1 1 from sys.foreign_key_columns AS colfk where colfk.parent_column_id = clmns.column_id and colfk.parent_object_id = clmns.object_id), 0) AS bit) AS [IsForeignKey],
CAST(clmns.is_sparse AS bit) AS [IsSparse],
CAST(clmns.is_column_set AS bit) AS [IsColumnSet]
FROM
sys.tables AS tbl
INNER JOIN sys.all_columns AS clmns ON clmns.object_id=tbl.object_id
LEFT OUTER JOIN sys.computed_columns AS cc ON cc.object_id = clmns.object_id and cc.column_id = clmns.column_id
LEFT OUTER JOIN sys.types AS usrt ON usrt.user_type_id = clmns.user_type_id
LEFT OUTER JOIN sys.types AS baset ON (baset.user_type_id = clmns.system_type_id and baset.user_type_id = baset.system_type_id) or ((baset.system_type_id = clmns.system_type_id) and (baset.user_type_id = clmns.user_type_id) and (baset.is_user_defined = 0) and (baset.is_assembly_type = 1))
LEFT OUTER JOIN sys.schemas AS sclmns ON sclmns.schema_id = usrt.schema_id
LEFT OUTER JOIN sys.identity_columns AS ic ON ic.object_id = clmns.object_id and ic.column_id = clmns.column_id
LEFT OUTER JOIN sys.objects AS d ON d.object_id = clmns.default_object_id
LEFT OUTER JOIN sys.objects AS r ON r.object_id = clmns.rule_object_id
LEFT OUTER JOIN sys.xml_schema_collections AS xscclmns ON xscclmns.xml_collection_id = clmns.xml_collection_id
LEFT OUTER JOIN sys.schemas AS s2clmns ON s2clmns.schema_id = xscclmns.schema_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[ID] ASC