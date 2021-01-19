namespace Particular.TimeoutMigrationTool.SqlT
{
    static class SqlConstants
    {
        public static readonly string StoreDelayedMessageText =
@"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON'
SET NOCOUNT ON;

DECLARE @DueAfter DATETIME = GETUTCDATE();
SET @DueAfter = DATEADD(ms, @DueAfterMilliseconds, @DueAfter);
SET @DueAfter = DATEADD(s, @DueAfterSeconds, @DueAfter);
SET @DueAfter = DATEADD(n, @DueAfterMinutes, @DueAfter);
SET @DueAfter = DATEADD(hh, @DueAfterHours, @DueAfter);
SET @DueAfter = DATEADD(d, @DueAfterDays, @DueAfter);

INSERT INTO {0} (
    Headers,
    Body,
    Due)
VALUES (
    @Headers,
    @Body,
    @DueAfter);

IF(@NOCOUNT = 'ON') SET NOCOUNT ON;
IF(@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

 public static readonly string MoveDueDelayedMessageText = @"
DECLARE @NOCOUNT VARCHAR(3) = 'OFF';
IF ( (512 & @@OPTIONS) = 512 ) SET @NOCOUNT = 'ON';
SET NOCOUNT ON;

WITH message AS (
    SELECT TOP(@BatchSize) *
    FROM {0} WITH (UPDLOCK, READPAST, ROWLOCK)
    WHERE Due < GETUTCDATE())
DELETE FROM message
OUTPUT
    NEWID(),
    NULL,
    NULL,
    1,
    NULL,
    deleted.Headers,
    deleted.Body
INTO {1};

IF (@NOCOUNT = 'ON') SET NOCOUNT ON;
IF (@NOCOUNT = 'OFF') SET NOCOUNT OFF;";

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
