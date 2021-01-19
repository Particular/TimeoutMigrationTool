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

        public async Task<int> StageBatch(IReadOnlyList<TimeoutData> timeouts)
        {
            if (connection.State != ConnectionState.Open)
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    logger.LogError(e, "Improve");
                    return 0;
                }
            }

            var dt = new DataTable();
            dt.Columns.Add("Headers");
            dt.Columns.Add("Body", typeof(byte[]));
            dt.Columns.Add("Due", typeof(DateTime));

            foreach (var timeout in timeouts)
            {
                dt.Rows.Add(DictionarySerializer.Serialize(timeout.Headers), timeout.State, timeout.Time);
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
            // TODO: Verify options
            using var sqlBulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = TimeoutMigrationStagingTable,
            };
            try
            {
                await sqlBulk.WriteToServerAsync(dt);
                await transaction.CommitAsync();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Improve");
                return 0;
            }

            return timeouts.Count;
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
                await SqlTQueueCreator.CreateStagingQueue(connection, TimeoutMigrationStagingTable, databaseName);
            }
            catch (Exception e)
            {
                migrationCheckResult.Problems.Add($"Unable to create the staging queue '{TimeoutMigrationStagingTable}'. The following exception occured: {e.Message}");
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
        private string schema;
        private string databaseName;
        const string TimeoutMigrationStagingTable = "timeoutmigrationtoolstagingtable";
    }
}