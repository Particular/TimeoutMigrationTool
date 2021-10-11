namespace Particular.TimeoutMigrationTool.Msmq
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Xml.Serialization;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    class MsmqEndpointTargetBatchMigrator : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        readonly SqlConnection connection;
        readonly string databaseName;
        readonly string schema;
        readonly ILogger logger;
        readonly DataTable stagingDataTable;
        string endpointDelayedTableName;

        public MsmqEndpointTargetBatchMigrator(ILogger logger, SqlConnection connection, string endpointName, string schema)
        {
            this.logger = logger;
            this.schema = schema;
            this.connection = connection;
            databaseName = connection.Database;
            endpointDelayedTableName = MsmqSqlConstants.DelayedTableName(endpointName);

            stagingDataTable = new DataTable();
            stagingDataTable.Columns.Add("Id");
            stagingDataTable.Columns.Add("Destination");
            stagingDataTable.Columns.Add("State", typeof(byte[]));
            stagingDataTable.Columns.Add("Time", typeof(DateTime));
            stagingDataTable.Columns.Add("Headers", typeof(byte[]));
        }

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            await MsmqQueueCreator.TruncateTable(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, databaseName);

            foreach (var timeout in timeouts)
            {
                stagingDataTable.Rows.Add(SerializeHeaders(timeout.Headers), timeout.State, timeout.Time);
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
            using var sqlBulk = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = MsmqSqlConstants.TimeoutMigrationStagingTable,
            };
            try
            {
                await sqlBulk.WriteToServerAsync(stagingDataTable);
                await transaction.CommitAsync();
            }
            catch (Exception e)
            {
                logger.LogError(e, $"Unable to bulk copy batch '{batchNumber}' to the staging table. Exception occured: {e.Message}");
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
            return await MsmqQueueCreator.MoveFromTo(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, endpointDelayedTableName, schema, databaseName);
        }

        public async ValueTask DisposeAsync()
        {
            await connection.CloseAsync();
        }

        byte[] SerializeHeaders(Dictionary<string, string> headers)
        {
            using (var stream = new MemoryStream())
            {
                var wrappedHeaders = headers.Select(pair => new HeaderInfo
                {
                    Key = pair.Key,
                    Value = pair.Value
                }).ToList();

                headerSerializer.Serialize(stream, wrappedHeaders);
                return stream.ToArray();
            }
        }

        static XmlSerializer headerSerializer = new XmlSerializer(typeof(List<HeaderInfo>));
    }
}