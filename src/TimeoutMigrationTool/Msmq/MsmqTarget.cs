namespace Particular.TimeoutMigrationTool.Msmq
{
    using System;
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    class MsmqTarget : ITimeoutsTarget
    {
        ILogger logger;
        SqlConnection connection;
        string schema;
        string endpointDelayedTableName;

        public MsmqTarget(ILogger logger, SqlConnection connection, string endpointName, string schema)
        {
            this.logger = logger;
            this.connection = connection;
            this.schema = schema;
            endpointDelayedTableName = MsmqSqlConstants.DelayedTableName(endpointName);
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
                await MsmqQueueCreator.CreateStagingQueue(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, databaseName, preview: true);
            }
            catch (Exception e)
            {
                migrationCheckResult.Problems.Add($"Attempt to verify whether the timeout migration staging table '{MsmqSqlConstants.TimeoutMigrationStagingTable}' could be created during migration mode failed. The following exception occured: {e.Message}");
            }

            if (!await MsmqQueueCreator
                .DoesDelayedDeliveryTableExist(connection, endpointDelayedTableName, schema, databaseName)
                .ConfigureAwait(false))
            {
                migrationCheckResult.Problems.Add($"Could not find delayed queue table with name '{endpointDelayedTableName}' for the endpoint '{endpoint.EndpointName}'");
            }

            return migrationCheckResult;
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

        public async ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName)
        {
            await EnsureConnectionOpen();
            await EnsureMigrationTableExists();

            return new MsmqEndpointTargetBatchMigrator(logger, connection, endpointName, schema);
        }

        async ValueTask EnsureMigrationTableExists()
        {
            try
            {
                await MsmqQueueCreator.CreateStagingQueue(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);
            }
            catch (Exception e)
            {
                logger.LogError($"Unable to create the staging queue '{MsmqSqlConstants.TimeoutMigrationStagingTable}'. The following exception occured: {e.Message}");
                throw;
            }
        }

        async Task EnsureMigrationTableIsEmpty()
        {
            var databaseName = connection.Database;
            var sql = string.Format(MsmqSqlConstants.SelectAnyFromMigrationTable, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
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
            return MsmqQueueCreator.DeleteStagingQueue(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, connection.Database);
        }

        async ValueTask EnsureConnectionOpen()
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync().ConfigureAwait(false);
            }
        }
    }
}
