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
   SELECT TABLE_NAME
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

        public static readonly string SelectAnyFromMigrationTable = @"
 SELECT COUNT(*) FROM [{2}].[{1}].[{0}];
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

EXEC sp_releaseapplock @Resource = '{0}_lock'";

        public static readonly string DeleteDelayedMessageStoreText = @"
IF NOT EXISTS (
    SELECT *
    FROM [{2}].sys.objects
    WHERE object_id = OBJECT_ID(N'[{1}].[{0}]')
        AND type in (N'U')
)
RETURN

EXEC sp_getapplock @Resource = '{0}_lock', @LockMode = 'Exclusive'

DROP TABLE [{2}].[{1}].[{0}]

EXEC sp_releaseapplock @Resource = '{0}_lock'";
    }

}
