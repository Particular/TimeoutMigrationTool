using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;
using System.Threading.Tasks;

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
        public abstract string GetScriptToLoadBatchInfo(string endpointName);
        public abstract string GetScriptToLoadToolState(string endpointName);
        public abstract string GetScriptToStoreToolState(string endpointName);
        public abstract string GetScriptToLoadBatch(string endpointName);
        public abstract string GetScriptToAbortBatch(string endpointName);
        public abstract string GetScriptToCompleteBatch(string endpointName);
        public abstract string GetScriptToListEndpoints();
    }

    public class MsSqlServer : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();

            return connection;
        }

        public override string GetScriptToLoadBatch(string endpointName)
        {
            return $@"SELECT Id,
    Destination,
    SagaId,
    State,
    Time,
    Headers,
    PersistenceVersion
FROM
    [{endpointName}_TimeoutData_migration]
WHERE
    BatchNumber = @BatchNumber;
";
        }

        public override string GetScriptToLoadBatchInfo(string endpointName)
        {
            return $@"SELECT
    Id,
    BatchNumber,
    Status
FROM
    [{endpointName}_TimeoutData_migration];";
        }

        public override string GetScriptToLoadToolState(string endpointName)
        {
            return $@"SELECT
    EndpointName,
    Status,
    RunParameters
FROM
    TimeoutsMigration_State
WHERE
    EndpointName = '{endpointName}';";
        }

        public override string GetScriptToPrepareTimeouts(string endpointName, int batchSize)
        {
            return $@"
BEGIN TRANSACTION

    IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TimeoutsMigration_State')
    BEGIN
        CREATE TABLE TimeoutsMigration_State (
            EndpointName NVARCHAR(500) NOT NULL PRIMARY KEY,
            Status VARCHAR(15) NOT NULL,
            Batches INT NOT NULL,
            RunParameters NVARCHAR(MAX)
        )
    END;

    CREATE TABLE [{endpointName}_TimeoutData_migration] (
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
    INTO [{endpointName}_TimeoutData_migration]
    WHERE [{endpointName}_TimeoutData].Time >= @migrateTimeoutsWithDeliveryDateLaterThan;

    UPDATE BatchMigration
    SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber
    FROM (
        SELECT BatchNumber, ROW_NUMBER() OVER (ORDER BY (select 0)) / {batchSize} AS CalculatedBatchNumber
        FROM [{endpointName}_TimeoutData_migration]
    ) BatchMigration;

    INSERT INTO TimeoutsMigration_State VALUES ('{endpointName}', 'StoragePrepared', (SELECT COUNT(DISTINCT BatchNumber) from [{endpointName}_TimeoutData_migration]), @RunParameters);

     SELECT
        Id,
        BatchNumber,
        Status
    FROM
        [{endpointName}_TimeoutData_migration];

COMMIT;";
        }

        public override string GetScriptToAbortBatch(string endpointName)
        {
            return $@"BEGIN TRANSACTION

DELETE [{endpointName}_TimeoutData_migration]
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
        EndpointName = '{endpointName}';

    DROP TABLE [{endpointName}_TimeoutData_migration];

COMMIT;";
        }

        public override string GetScriptToStoreToolState(string endpointName)
        {
            return $@"UPDATE
    TimeoutsMigration_State
SET
    Status = @Status
WHERE
    EndpointName = '{endpointName}';";
        }

        public override string GetScriptToCompleteBatch(string endpointName)
        {
            return $@"UPDATE
    [{endpointName}_TimeoutData_migration]
SET
    Status = 2
WHERE
    BatchNumber = @BatchNumber";
        }

        public override string GetScriptToListEndpoints()
        {
            return $@"DECLARE @SqlQuery NVARCHAR(MAX) = '';

SELECT
	@SqlQuery = @SqlQuery + 'SELECT
	''' + SUBSTRING(name, 0, LEN(name) - LEN('_TimeoutData')) + ''' EndpointName,
	COUNT(*) NrOfTimeouts,
	MAX(Time) LongestTimeout,
	MIN(Time) ShortestTimeout
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
    }

    public class Oracle : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToAbortBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToCompleteBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToListEndpoints()
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatch(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatchInfo(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadToolState(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToStoreToolState(string endpointName)
        {
            throw new NotImplementedException();
        }
    }

    public class MySql : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToAbortBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToCompleteBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToListEndpoints()
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatch(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatchInfo(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadToolState(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToStoreToolState(string endpointName)
        {
            throw new NotImplementedException();
        }
    }

    public class PostgreSql : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToAbortBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToCompleteBatch(string timeoutTableName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToListEndpoints()
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatch(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadBatchInfo(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToLoadToolState(string endpointName)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToStoreToolState(string endpointName)
        {
            throw new NotImplementedException();
        }
    }
}