namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    public class SqlTTimeoutCreator : ICreateTransportTimeouts
    {
        public SqlTTimeoutCreator(ILogger logger, string connectionString, string schema)
        {
            this.schema = schema;
            this.logger = logger;
            this.connectionString = connectionString;
            connection = new SqlConnection(connectionString);
        }

        public Task<int> StageBatch(List<TimeoutData> timeouts)
        {
            throw new System.NotImplementedException();
        }

        public Task<int> CompleteBatch(int number)
        {
            throw new System.NotImplementedException();
        }

        public async Task<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            var migrationCheckResult = new MigrationCheckResult();

            if (connection.State != ConnectionState.Open)
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    migrationCheckResult.Problems.Add($"Unable to connect to the server or database under connection string '{connectionString}'. The following exception occured: {e.Message}");
                    return migrationCheckResult;
                }
            }

            databaseName = connection.Database;

            try
            {
                await SqlTQueueCreator.CreateStagingQueue(connection, timeoutmigrationStagingTable, databaseName);
            }
            catch (Exception e)
            {
                migrationCheckResult.Problems.Add($"Unable to create the staging queue '{timeoutmigrationStagingTable}'. The following exception occured: {e.Message}");
            }

            var suffix = "Delayed";
            var endpointDelayedTableName = $"{endpoint.EndpointName}.{suffix}";

            if (!await SqlTQueueCreator
                .DoesDelayedDeliveryTableExist(connection, endpointDelayedTableName, schema, databaseName)
                .ConfigureAwait(false))
            {
                migrationCheckResult.Problems.Add($"Could not find delayed queue table with name '{endpointDelayedTableName}' for the endpoint '{endpoint.EndpointName}'");
            }

            return migrationCheckResult;
        }

        private readonly SqlConnection connection;
        private string connectionString;
        private readonly ILogger logger;
        private readonly string timeoutmigrationStagingTable = "timeoutmigrationtoolstagingtable";
        private string schema;
        private string databaseName;
    }
}