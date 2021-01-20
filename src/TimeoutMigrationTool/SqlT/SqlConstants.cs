namespace Particular.TimeoutMigrationTool.SqlT
{
    static class SqlConstants
    {
        public static string DelayedTableName(string endpointName, string suffix = "Delayed")
        {
            return $"{endpointName}.{suffix}";
        }

        public const string TimeoutMigrationStagingTable = "timeoutmigrationtoolstagingtable";

        public static readonly string DelayedMessageStoreExistsText = @"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
        ";

        public static readonly string MoveFromStagingToDelayedTableText = @"
DELETE [{4}].[{1}].[{0}]
OUTPUT DELETED.Headers,
    DELETED.Body,
    DELETED.Due
INTO [{4}].[{3}].[{2}];
SELECT @@ROWCOUNT;
";

        public static readonly string TruncateTableText = @"
TRUNCATE TABLE [{2}].[{1}].[{0}];
";

        public static readonly string CreateDelayedMessageStoreText = @"
IF EXISTS (
    SELECT *
    FROM [{2}].sys.objects
    WHERE object_id = OBJECT_ID(N'[{1}].[{0}]')
        AND type in (N'U')
)
RETURN

EXEC sp_getapplock @Resource = '{0}_lock', @LockMode = 'Exclusive'

IF EXISTS (
    SELECT *
    FROM [{2}].sys.objects
    WHERE object_id = OBJECT_ID(N'[{1}].[{0}]')
        AND type in (N'U')
)
BEGIN
    EXEC sp_releaseapplock @Resource = '{0}_lock'
    RETURN
END

CREATE TABLE [{2}].[{1}].[{0}] (
    Headers nvarchar(max) NOT NULL,
    Body varbinary(max),
    Due datetime NOT NULL,
    RowVersion bigint IDENTITY(1,1) NOT NULL
);

CREATE NONCLUSTERED INDEX [Index_Due] ON {0}
(
    [Due]
)

EXEC sp_releaseapplock @Resource = '{0}_lock'";

    }
}
