using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;

namespace Particular.TimeoutMigrationTool
{
    public abstract class SqlDialect
    {
        public abstract DbConnection Connect(string connectionString);

        public static SqlDialect Parse(string dialectString)
        {
            return new MsSqlServer();
        }

        public abstract string GetScriptToPrepareTimeouts(string endpointName, int batchSize);
        public abstract string GetScriptToLoadBatchInfo();
        public abstract string GetScriptToLoadToolState();
        public abstract string GetScriptToStoreToolState();
        public abstract string GetScriptToLoadBatch();
        public abstract string GetScriptToAbortBatch(string endpointName);
        public abstract string GetScriptToCompleteBatch();
        public abstract string GetScriptToListEndpoints();
        public abstract string GetScriptToMarkBatchAsStaged();
        public abstract string GetScriptToMarkMigrationAsCompleted();
    }

    public class MsSqlServer : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();

            return connection;
        }

        public override string GetScriptToLoadBatch()
        {
            return $@"SELECT Id,
    Destination,
    SagaId,
    State,
    Time,
    Headers,
    PersistenceVersion
FROM
    [TimeoutData_migration]
WHERE
    BatchNumber = @BatchNumber;
";
        }

        public override string GetScriptToLoadBatchInfo()
        {
            return $@"SELECT
    Id,
    BatchNumber,
    Status
FROM
    [TimeoutData_migration];";
        }

        public override string GetScriptToLoadToolState()
        {
            return $@"
    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TimeoutsMigration_State')
    BEGIN
        CREATE TABLE TimeoutsMigration_State (
            MigrationRunId NVARCHAR(500) NOT NULL PRIMARY KEY,
            EndpointName NVARCHAR(500) NOT NULL,
            Status VARCHAR(15) NOT NULL,
            Batches INT NOT NULL,
            RunParameters NVARCHAR(MAX)
        )
    END;

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TimeoutData_migration')
    BEGIN
       CREATE TABLE [TimeoutData_migration] (
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
    END;
SELECT
    EndpointName,
    Status,
    RunParameters
FROM
    TimeoutsMigration_State
WHERE
    MigrationRunId = 'TOOLSTATE';";
        }

        public override string GetScriptToPrepareTimeouts(string endpointName, int batchSize)
        {
            return $@"
BEGIN TRANSACTION

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
    INTO [TimeoutData_migration]
    WHERE [{endpointName}_TimeoutData].Time >= @migrateTimeoutsWithDeliveryDateLaterThan;

    UPDATE BatchMigration
    SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber
    FROM (
        SELECT BatchNumber, ROW_NUMBER() OVER (ORDER BY (select 0)) / {batchSize} AS CalculatedBatchNumber
        FROM [TimeoutData_migration]
    ) BatchMigration;

    UPDATE
        TimeoutsMigration_State
    SET
        Batches = (SELECT COUNT(DISTINCT BatchNumber) from [TimeoutData_migration])
    WHERE
        MigrationRunId = 'TOOLSTATE';

     SELECT
        Id,
        BatchNumber,
        Status
    FROM
        [TimeoutData_migration];

COMMIT;";
        }

        public override string GetScriptToAbortBatch(string endpointName)
        {
            return $@"BEGIN TRANSACTION

DELETE [TimeoutData_migration]
    OUTPUT DELETED.Id,
        DELETED.Destination,
        DELETED.SagaId,
        DELETED.State,
        DELETED.Time,
        DELETED.Headers,
        DELETED.PersistenceVersion
    INTO [{endpointName}_TimeoutData];

    DELETE
        TimeoutsMigration_State
    WHERE
        MigrationRunId = 'TOOLSTATE';

    DROP TABLE [TimeoutData_migration];

COMMIT;";
        }

        public override string GetScriptToStoreToolState()
        {
            return @"
BEGIN TRANSACTION
    IF NOT EXISTS (SELECT * FROM TimeoutsMigration_State WHERE MigrationRunId = 'TOOLSTATE')
        INSERT INTO TimeoutsMigration_State (MigrationRunId, EndpointName, Status, Batches, RunParameters)
        VALUES ('TOOLSTATE', @EndpointName, @Status, @Batches, @RunParameters);
    ELSE
        UPDATE
            TimeoutsMigration_State
        SET
            EndpointName =  @EndpointName,
            Status = @Status,
            RunParameters = @RunParameters,
            Batches = @Batches
        WHERE
            MigrationRunId = 'TOOLSTATE';

COMMIT;";
        }

        public override string GetScriptToCompleteBatch()
        {
            return $@"UPDATE
    [TimeoutData_migration]
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
    (SELECT
        Destination + '', ''
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

SET @SqlQuery = SUBSTRING(@SqlQuery, 0, LEN(@SqlQuery) - LEN('UNION'));

EXEC sp_executesql @SqlQuery, N'@CutOffTime DATETIME', @CutOffTime";
        }

        public override string GetScriptToMarkBatchAsStaged()
        {
            return $@"UPDATE
    [TimeoutData_migration]
SET
    Status = 1
WHERE
    BatchNumber = @BatchNumber";
        }

        public override string GetScriptToMarkMigrationAsCompleted()
        {
            return @"
BEGIN TRANSACTION
    UPDATE
        TimeoutsMigration_State
    SET
        Status = 2
    WHERE
        MigrationRunId = 'TOOLSTATE';

COMMIT;";
        }
    }
}