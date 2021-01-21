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
            this.connectionString = connectionString;
            connection = new SqlConnection(connectionString);
        }

        public async ValueTask<ITimeoutsTarget.IEndpointTarget> Migrate(string endpointName)
        {
            await EnsureConnectionOpen();
            await EnsureMigrationTableExists();

            return new SqlTEndpointTarget(logger, connection, endpointName, schema);
        }

        public async ValueTask Abort(string endpointName)
        {
            await EnsureConnectionOpen();
            await RemoveMigrationTable();

            await connection.CloseAsync();
        }

        private Task RemoveMigrationTable()
        {
            return SqlTQueueCreator.DeleteStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, connection.Database);
        }

        private async ValueTask EnsureMigrationTableExists()
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

        private async ValueTask EnsureConnectionOpen()
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
                migrationCheckResult.Problems.Add($"Unable to connect to the server or database under connection string '{connectionString}'. The following exception occured: {e.Message}");
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

            if (!await SqlTQueueCreator
                .DoesDelayedDeliveryTableExist(connection, endpointDelayedTableName, schema, databaseName)
                .ConfigureAwait(false))
            {
                migrationCheckResult.Problems.Add($"Could not find delayed queue table with name '{endpointDelayedTableName}' for the endpoint '{endpoint.EndpointName}'");
            }

            return migrationCheckResult;
        }

        private readonly SqlConnection connection;
        private readonly string connectionString;
        private readonly ILogger logger;
        private readonly string schema;

    }
}