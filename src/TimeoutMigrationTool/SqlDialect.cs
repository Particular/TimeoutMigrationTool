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

        public abstract string GetScriptToPrepareTimeouts(string originalTableName, int batchSize);
    }

    public class MsSqlServer : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();

            return connection;
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
        Status INT, /* NULL = prepared, 1 = staged, 2 = migrated */
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
        NULL,
        DELETED.Destination,
        DELETED.SagaId,
        DELETED.State,
        DELETED.Time,
        DELETED.Headers,
        DELETED.PersistenceVersion
    INTO [{endpointName}_TimeoutData_migration]
    WHERE [{endpointName}_TimeoutData].Time <= @maxCutOff;

    UPDATE BatchMigration
    SET BatchMigration.BatchNumber = BatchMigration.CalculatedBatchNumber
    FROM (
        SELECT BatchNumber, ROW_NUMBER() OVER (ORDER BY (select 0)) / {batchSize} AS CalculatedBatchNumber
        FROM [{endpointName}_TimeoutData_migration]
    ) BatchMigration;

    INSERT INTO TimeoutsMigration_State VALUES ('{endpointName}', 'Prepared', (SELECT COUNT(DISTINCT BatchNumber) from [{endpointName}_TimeoutData_migration]), @RunParameters);
        
COMMIT;";
        }
    }

    public class Oracle : SqlDialect
    {
        public override DbConnection Connect(string connectionString)
        {
            throw new NotImplementedException();
        }

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
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

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
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

        public override string GetScriptToPrepareTimeouts(string originalTableName, int batchSize)
        {
            throw new NotImplementedException();
        }
    }
}