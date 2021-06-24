namespace Particular.TimeoutMigrationTool.MsmqSql
{
    static class SqlConstants
    {
        public static string DelayedTableName(string endpointName, string suffix = "Timeouts")
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
OUTPUT DELETED.Id,
    DELETED.Destination,
    DELETED.State,
    DELETED.Time,
    DELETED.Headers
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
    Id nvarchar(250) not null primary key,
    Destination nvarchar(200),
    State varbinary(max),
    Time datetime,
    Headers varbinary(max) not null,
    RetryCount INT NOT NULL default(0)
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
