-- 
-- EJEMPLO DE USO:  >>>> EXEC [dbo].[Remover_Duplicados] @NOMBRE_TABLA = "Unificado"
--
CREATE PROCEDURE [dbo].[Remover_Duplicados]
    @NOMBRE_TABLA SYSNAME
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX) = 
       'WITH cte AS
        (
            SELECT *,
            rn = ROW_NUMBER() OVER(PARTITION BY <COLUMNAS> ORDER BY (SELECT 1))
            FROM <TABLA>
        )
        DELETE FROM cte
        WHERE rn <> 1;';

    DECLARE @COLUMNAS NVARCHAR(MAX) = 
        STUFF((SELECT ',' +  column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = @NOMBRE_TABLA
            AND TABLE_SCHEMA = 'dbo'
        FOR XML PATH('')), 1, 1, '');

    SET @sql = REPLACE(@sql, '<TABLA>', QUOTENAME(@NOMBRE_TABLA));
    SET @sql = REPLACE(@sql, '<COLUMNAS>', @COLUMNAS);

    EXEC dbo.sp_executesql @sql;

    RETURN @@ROWCOUNT
END
GO