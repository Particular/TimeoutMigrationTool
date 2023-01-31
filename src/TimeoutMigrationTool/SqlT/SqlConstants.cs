namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;

    static class SqlConstants
    {

        public const string DelayedTableNameSuffix = ".Delayed";
        public static string DelayedTableName(string endpointName)
        {
            return $"{endpointName}{DelayedTableNameSuffix}";
        }

        public static readonly string MarkMigrationAsCompleted = @"
                                                                        UPDATE
                                                                            TimeoutsMigration_State
                                                                        SET
                                                                            Status = 2,
                                                                            CompletedAt = @CompletedAt
                                                                        WHERE
                                                                            MigrationRunId = @MigrationRunId;
                                                                    ";

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

        public static readonly string ListEndPoints = @"SELECT name FROM [sys].[tables] WHERE name LIKE '%.delayed';";

        public static readonly string ListEndPointDetails = @"SELECT
	                                                                '{0}',
	                                                                COUNT(1),
	                                                                min(Due),
	                                                                max(Due),
	                                                                '{1}'
                                                                FROM 
	                                                                [{0}] 
                                                                HAVING COUNT(1) > 0;";

        public static string GetScriptToLoadBatch(string migrationRunId)
        {
            return $@"SELECT Id,
                                                        Destination,
                                                        State,
                                                        Time,
                                                        Headers
                                                    FROM
                                                        [{GetMigrationTableName(migrationRunId)}]
                                                    WHERE
                                                        BatchNumber = @BatchNumber;
                                                    ";
        }

        public static string GetScriptToAbort(string migrationRunId, string endpointName)
        {
            var migrationTableName = GetMigrationTableName(migrationRunId);
            return $@"
BEGIN TRANSACTION
    DELETE [{migrationTableName}]
        OUTPUT 
            DELETED.Headers,
            DELETED.State,
            DELETED.Time
    INTO [{endpointName}.Delayed]
    WHERE [{migrationTableName}].Status <> 2;


    UPDATE TimeoutsMigration_State
    SET
        Status = 3,
        CompletedAt = @CompletedAt
    WHERE
        MigrationRunId = '{migrationRunId}';

    DROP TABLE [{migrationTableName}];
COMMIT;";
        }

        internal static string MarkBatchAsStaged = @"UPDATE
    [TimeoutData_migration_{0}]
SET
    Status = 1
WHERE
    BatchNumber = @BatchNumber";

        internal static string GetScriptToPrepareTimeouts(string migrationRunId, string endpointName, int batchSize)
        {
            var migrationTableName = GetMigrationTableName(migrationRunId);
            return $@"
                BEGIN TRANSACTION

                    CREATE TABLE [{migrationTableName}] (
                        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
                        BatchNumber INT,
                        Status INT NOT NULL, /* 0 = Pending, 1 = staged, 2 = migrated */
                        Destination NVARCHAR(200),
                        State VARBINARY(MAX),
                        Time DATETIME,
                        Headers NVARCHAR(MAX) NOT NULL
                    );

                    CREATE NONCLUSTERED INDEX INDEX_Status_BatchNumber
                    ON [dbo].[{migrationTableName}] ([Status])
                    INCLUDE ([BatchNumber]);

                    DELETE [{endpointName}.Delayed]
                    OUTPUT NEWID(),
                        -1,
                        0,
                        '{endpointName}',
                        DELETED.[Body],
                        DELETED.Due,
                        DELETED.Headers
                    INTO [{migrationTableName}]
                    WHERE [{endpointName}.Delayed].Due >= @CutOffTime;

                    UPDATE BatchMigration
                    SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber + 1
                    FROM (
                        SELECT BatchNumber, (ROW_NUMBER() OVER (ORDER BY (select 0)) - 1) / {batchSize} AS CalculatedBatchNumber
                        FROM [{migrationTableName}]
                    ) BatchMigration;

                    INSERT INTO TimeoutsMigration_State (MigrationRunId, EndpointName, Status, RunParameters, NumberOfBatches, CutOffTime, StartedAt)
                    VALUES ('{migrationRunId}', '{endpointName}', 1, @RunParameters,(SELECT COUNT(DISTINCT BatchNumber) from [{migrationTableName}]), @CutOffTime, @StartedAt);
                COMMIT;";
        }
        internal static string GetScriptToCompleteBatch(string migrationRunId)
        {
            return $@"UPDATE
    [{GetMigrationTableName(migrationRunId)}]
SET
    Status = 2
WHERE
    BatchNumber = @BatchNumber";
        }
        internal static string GetScriptToLoadPendingMigrations()
        {

            return $@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TimeoutsMigration_State')
                BEGIN
                    CREATE TABLE TimeoutsMigration_State (
                        MigrationRunId NVARCHAR(500) NOT NULL PRIMARY KEY,
                        EndpointName NVARCHAR(500) NOT NULL,
                        Status INT NOT NULL,
                        RunParameters NVARCHAR(MAX) NOT NULL,
                        NumberOfBatches INT NOT NULL,
                        CutOffTime DATETIME NOT NULL,
                        StartedAt DATETIME NOT NULL,
                        CompletedAt DATETIME NULL
                    )
                END;
            SELECT
                MigrationRunId,
                EndpointName,
                Status,
                RunParameters,
                NumberOfBatches
            FROM
                TimeoutsMigration_State
            WHERE
                Status = 1;";
        }

        public static string GetNextBatch(string migrationRunId)
        {
            return $@"
SELECT top 1 BatchNumber,
    Status,
    COUNT(1) as NumberOfTimeouts
FROM [{GetMigrationTableName(migrationRunId)}] AS batch
GROUP BY BatchNumber, Status
HAVING Status < 2
ORDER BY BatchNumber";
        }

        static string GetMigrationTableName(string migrationRunId)
        {
            return $"TimeoutData_migration_{migrationRunId}";
        }
    }

}
