SELECT
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
i.name AS [Name],
CAST(ISNULL(k.is_system_named, 0) AS bit) AS [IsSystemNamed],
i.is_primary_key + 2*i.is_unique_constraint AS [IndexKeyType],
i.is_unique AS [IsUnique],
CAST(CASE i.index_id WHEN 1 THEN 1 ELSE 0 END AS bit) AS [IsClustered],
i.is_disabled AS [IsDisabled],
CASE WHEN 'PS'=dsi.type THEN dsi.name ELSE N'' END AS [PartitionScheme],
CAST(case when i.type=3 then 1 else 0 end AS bit) AS [IsXmlIndex],
case UPPER(ISNULL(xi.secondary_type,'')) when 'P' then 1 when 'V' then 2 when 'R' then 3 else 0 end AS [SecondaryXmlIndexType],
CASE WHEN 'FG'=dsi.type THEN dsi.name ELSE N'' END AS [FileGroup],
CASE WHEN 'FD'=dstbl.type THEN dstbl.name ELSE N'' END AS [FileStreamFileGroup],
CASE WHEN 'PS'=dstbl.type THEN dstbl.name ELSE N'' END AS [FileStreamPartitionScheme],
i.ignore_dup_key AS [IgnoreDuplicateKeys],
i.fill_factor AS [FillFactor],
CAST(INDEXPROPERTY(i.object_id, i.name, N'IsPadIndex') AS bit) AS [PadIndex],
~i.allow_row_locks AS [DisallowRowLocks],
~i.allow_page_locks AS [DisallowPageLocks],
s.no_recompute AS [NoAutomaticRecomputation],
CAST(case when i.type=4 then 1 else 0 end AS bit) AS [IsSpatialIndex],
CAST(ISNULL(spi.spatial_index_type,0) AS tinyint) AS [SpatialIndexType],
CAST(ISNULL(si.bounding_box_xmin,0) AS float(53)) AS [BoundingBoxXMin],
CAST(ISNULL(si.bounding_box_ymin,0) AS float(53)) AS [BoundingBoxYMin],
CAST(ISNULL(si.bounding_box_xmax,0) AS float(53)) AS [BoundingBoxXMax],
CAST(ISNULL(si.bounding_box_ymax,0) AS float(53)) AS [BoundingBoxYMax],
CAST(ISNULL(si.level_1_grid,0) AS smallint) AS [Level1Grid],
CAST(ISNULL(si.level_2_grid,0) AS smallint) AS [Level2Grid],
CAST(ISNULL(si.level_3_grid,0) AS smallint) AS [Level3Grid],
CAST(ISNULL(si.level_4_grid,0) AS smallint) AS [Level4Grid],
CAST(ISNULL(si.cells_per_object,0) AS int) AS [CellsPerObject],
ISNULL(i.filter_definition, N'') AS [FilterDefinition]
FROM
sys.tables AS tbl
INNER JOIN sys.indexes AS i ON (i.index_id > 0 and i.is_hypothetical = 0) AND (i.object_id=tbl.object_id)
LEFT OUTER JOIN sys.key_constraints AS k ON k.parent_object_id = i.object_id AND k.unique_index_id = i.index_id
LEFT OUTER JOIN sys.data_spaces AS dsi ON dsi.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.xml_indexes AS xi ON xi.object_id = i.object_id AND xi.index_id = i.index_id
LEFT OUTER JOIN sys.tables AS t ON t.object_id = i.object_id
LEFT OUTER JOIN sys.data_spaces AS dstbl ON dstbl.data_space_id = t.Filestream_data_space_id and i.index_id < 2
LEFT OUTER JOIN sys.stats AS s ON s.stats_id = i.index_id AND s.object_id = i.object_id
LEFT OUTER JOIN sys.spatial_indexes AS spi ON i.object_id = spi.object_id and i.index_id = spi.index_id
LEFT OUTER JOIN sys.spatial_index_tessellations as si ON i.object_id = si.object_id and i.index_id = si.index_id
WHERE
(tbl.name=? and SCHEMA_NAME(tbl.schema_id)=?)
ORDER BY
[Table_Schema] ASC,[Table_Name] ASC,[Name] ASC