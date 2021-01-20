namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    class SqlTEndpointTarget : ITimeoutsTarget.IEndpointTarget
    {
        private readonly SqlConnection connection;
        private readonly string databaseName;
        private readonly string endpointName;
        private readonly string schema;
        private readonly ILogger logger;
        private readonly DataTable stagingDataTable;
        private string endpointDelayedTableName;

        public SqlTEndpointTarget(ILogger logger, SqlConnection connection, string endpointName, string schema)
        {
            this.logger = logger;
            this.schema = schema;
            this.endpointName = endpointName;
            this.connection = connection;
            databaseName = connection.Database;
            endpointDelayedTableName = SqlConstants.DelayedTableName(endpointName);

            stagingDataTable = new DataTable();
            stagingDataTable.Columns.Add("Headers");
            stagingDataTable.Columns.Add("Body", typeof(byte[]));
            stagingDataTable.Columns.Add("Due", typeof(DateTime));
        }

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            await SqlTQueueCreator.TruncateTable(connection, SqlConstants.TimeoutMigrationStagingTable, schema, databaseName);

            foreach (var timeout in timeouts)
            {
                stagingDataTable.Rows.Add(DictionarySerializer.Serialize(timeout.Headers), timeout.State, timeout.Time);
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
            // TODO: Verify options
            using var sqlBulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = SqlConstants.TimeoutMigrationStagingTable,
            };
            try
            {
                await sqlBulk.WriteToServerAsync(stagingDataTable);
                await transaction.CommitAsync();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Improve");
                return 0;
            }
            finally
            {
                stagingDataTable.Clear();
            }

            return timeouts.Count;
        }

        public async ValueTask<int> CompleteBatch(int batchNumber)
        {
            return await SqlTQueueCreator.MoveFromTo(connection, SqlConstants.TimeoutMigrationStagingTable, schema, endpointDelayedTableName, schema, databaseName);
        }

        public async ValueTask DisposeAsync()
        {
            await connection.CloseAsync();
        }
    }
}