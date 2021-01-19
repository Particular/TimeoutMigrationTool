namespace Particular.TimeoutMigrationTool.SqlP
{
    using Microsoft.Data.SqlClient;
    using System.Data.Common;

    public class MsSqlServer : SqlDialect
    {
        public static string Name => nameof(MsSqlServer);

        public override DbConnection Connect(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();

            return connection;
        }

        public override string GetScriptToLoadBatch(string migrationRunId)
        {
            return $@"SELECT Id,
    Destination,
    SagaId,
    State,
    Time,
    Headers,
    PersistenceVersion
FROM
    [{GetMigrationTableName(migrationRunId)}]
WHERE
    BatchNumber = @BatchNumber;
";
        }

        public override string GetScriptToTryGetNextBatch(string migrationRunId)
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

        public override string GetScriptLoadPendingMigrations()
        {
            return $@"
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TimeoutsMigration_State')
    BEGIN
        CREATE TABLE TimeoutsMigration_State (
            MigrationRunId NVARCHAR(500) NOT NULL PRIMARY KEY,
            EndpointName NVARCHAR(500) NOT NULL,
            Status VARCHAR(15) NOT NULL,
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
    RunParameters,
    NumberOfBatches
FROM
    TimeoutsMigration_State
WHERE
    Status = 1;";
        }

        public override string GetScriptToPrepareTimeouts(string migrationRunId, string endpointName, int batchSize)
        {
            var migrationTableName = GetMigrationTableName(migrationRunId);
            return $@"
BEGIN TRANSACTION

    CREATE TABLE [{migrationTableName}] (
        Id UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        BatchNumber INT,
        Status INT NOT NULL, /* 0 = Pending, 1 = staged, 2 = migrated */
        Destination NVARCHAR(200),
        SagaId UNIQUEIDENTIFIER,
        State VARBINARY(MAX),
        Time DATETIME,
        Headers NVARCHAR(MAX) NOT NULL,
        PersistenceVersion VARCHAR(23) NOT NULL
    );

    CREATE NONCLUSTERED INDEX INDEX_Status_BatchNumber
    ON [dbo].[{migrationTableName}] ([Status])
    INCLUDE ([BatchNumber]);

    DELETE [{endpointName}_TimeoutData]
    OUTPUT DELETED.Id,
        -1,
        0,
        DELETED.Destination,
        DELETED.SagaId,
        DELETED.State,
        DELETED.Time,
        DELETED.Headers,
        DELETED.PersistenceVersion
    INTO [{migrationTableName}]
    WHERE [{endpointName}_TimeoutData].Time >= @CutOffTime;

    UPDATE BatchMigration
    SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber + 1
    FROM (
        SELECT BatchNumber, ROW_NUMBER() OVER (ORDER BY (select 0)) / {batchSize} AS CalculatedBatchNumber
        FROM [{migrationTableName}]
    ) BatchMigration;

    INSERT INTO TimeoutsMigration_State (MigrationRunId, EndpointName, Status, RunParameters, NumberOfBatches, CutOffTime, StartedAt)
    VALUES ('{migrationRunId}', '{endpointName}', 1, @RunParameters,(SELECT COUNT(DISTINCT BatchNumber) from [{migrationTableName}]), @CutOffTime, @StartedAt);
COMMIT;";
        }

        public override string GetScriptToAbortMigration(string migrationRunId, string endpointName)
        {
            var migrationTableName = GetMigrationTableName(migrationRunId);
            return $@"
BEGIN TRANSACTION
    DELETE [{migrationTableName}]
        OUTPUT DELETED.Id,
            DELETED.Destination,
            DELETED.SagaId,
            DELETED.State,
            DELETED.Time,
            DELETED.Headers,
            DELETED.PersistenceVersion
    INTO [{endpointName}_TimeoutData]
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

        public override string GetScriptToCompleteBatch(string migrationRunId)
        {
            return $@"UPDATE
    ['{GetMigrationTableName(migrationRunId)}']
SET
    Status = 2
WHERE
    BatchNumber = @BatchNumber";
        }

        public override string GetScriptToListEndpoints()
        {
            return @"DECLARE @SqlQuery NVARCHAR(MAX) = '';

SELECT
	@SqlQuery = @SqlQuery + 'SELECT
	''' + SUBSTRING(name, 0, LEN(name) - LEN('_TimeoutData') + 1) + ''' EndpointName,
	COUNT(*) NrOfTimeouts,
	MAX(Time) LongestTimeout,
	MIN(Time) ShortestTimeout,
    (SELECT DISTINCT(Destination) + '', ''
    FROM
        ' + name + '
    FOR XML PATH('''')) Destinations
FROM
	' + name + '
WHERE
	Time >= @CutOffTime
HAVING
    COUNT(*) > 0 UNION '
FROM
	sys.tables
WHERE
	name LIKE '%_TimeoutData';

IF LEN(@SqlQuery) > 0 BEGIN
SET @SqlQuery = SUBSTRING(@SqlQuery, 0, LEN(@SqlQuery) - LEN('UNION'));

EXEC sp_executesql @SqlQuery, N'@CutOffTime DATETIME', @CutOffTime

END;";
        }

        public override string GetScriptToMarkBatchAsStaged(string migrationRunId)
        {
            return $@"UPDATE
    ['{GetMigrationTableName(migrationRunId)}']
SET
    Status = 1
WHERE
    BatchNumber = @BatchNumber";
        }

        public override string GetScriptToMarkMigrationAsCompleted()
        {
            return @"
    UPDATE
        TimeoutsMigration_State
    SET
        Status = 2,
        CompletedAt = @CompletedAt
    WHERE
        MigrationRunId = @MigrationRunId;
";
        }

        string GetMigrationTableName(string migrationRunId)
        {
            return $"TimeoutData_migration_{migrationRunId}";
        }
    }
}