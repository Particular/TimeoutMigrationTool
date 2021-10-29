namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    public class SqlTTimeoutsTarget : ITimeoutsTarget
    {
        public SqlTTimeoutsTarget(ILogger logger, string connectionString, string schema)
        {
            this.schema = schema;
            this.logger = logger;
            connection = new SqlConnection(connectionString);
        }

        public async ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName)
        {
            await EnsureConnectionOpen();
            await EnsureMigrationTableExists();

            var endpointDelayedTableName = SqlConstants.DelayedTableName(endpointName);

            var actualEndpointDelayedTableName = await SqlTQueueCreator
                .DoesDelayedDeliveryTableExist(connection, endpointDelayedTableName, schema, connection.Database)
                .ConfigureAwait(false);

            return new SqlTEndpointTarget(logger, connection, actualEndpointDelayedTableName, schema);
        }

        public async ValueTask Abort(string endpointName)
        {
            await EnsureConnectionOpen();
            await RemoveMigrationTable();

            await connection.CloseAsync();
        }

        public async ValueTask Complete(string endpointName)
        {
            await EnsureConnectionOpen();
            await EnsureMigrationTableIsEmpty();
            await RemoveMigrationTable();

            await connection.CloseAsync();
        }

        async Task EnsureMigrationTableIsEmpty()
        {
            var databaseName = connection.Database;
            var sql = string.Format(SqlConstants.SelectAnyFromMigrationTable, SqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
            await using var command = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };
            var result = await command.ExecuteScalarAsync().ConfigureAwait(false) as int?;
            if (result > 0)
            {
                throw new Exception($"Unable to complete migration as there are still records available in the staging table. Found {result} records");
            }
        }

        Task RemoveMigrationTable()
        {
            return SqlTQueueCreator.DeleteStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, connection.Database);
        }

        async ValueTask EnsureMigrationTableExists()
        {
            try
            {
                await SqlTQueueCreator.CreateStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);
            }
            catch (Exception e)
            {
                logger.LogError($"Unable to create the staging queue '{SqlConstants.TimeoutMigrationStagingTable}'. The following exception occured: {e.Message}");
                throw;
            }
        }

        async ValueTask EnsureConnectionOpen()
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync().ConfigureAwait(false);
            }
        }

        public async ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            var migrationCheckResult = new MigrationCheckResult();

            try
            {
                await EnsureConnectionOpen();
            }
            catch (Exception e)
            {
                migrationCheckResult.Problems.Add($"Unable to connect to the server or database using the provided connection string. Verify the connection string. The following exception occured: {e.Message}");
                return migrationCheckResult;
            }

            var databaseName = connection.Database;

            try
            {
                await SqlTQueueCreator.CreateStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, databaseName, preview: true);
            }
            catch (Exception e)
            {
                migrationCheckResult.Problems.Add($"Attempt to verify whether the timeout migration staging table '{SqlConstants.TimeoutMigrationStagingTable}' could be created during migration mode failed. The following exception occured: {e.Message}");
            }

            var endpointDelayedTableName = SqlConstants.DelayedTableName(endpoint.EndpointName);

            if (await SqlTQueueCreator
                .DoesDelayedDeliveryTableExist(connection, endpointDelayedTableName, schema, databaseName)
                .ConfigureAwait(false) == null)
            {
                migrationCheckResult.Problems.Add($"Could not find delayed queue table with name '{endpointDelayedTableName}' for the endpoint '{endpoint.EndpointName}'");
            }

            return migrationCheckResult;
        }

        readonly SqlConnection connection;
        readonly ILogger logger;
        readonly string schema;
    }
}
