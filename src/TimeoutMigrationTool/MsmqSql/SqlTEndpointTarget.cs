namespace Particular.TimeoutMigrationTool.MsmqSql
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;

    class SqlTEndpointTarget : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        readonly SqlConnection connection;
        readonly string databaseName;
        readonly string schema;
        readonly ILogger logger;
        readonly DataTable stagingDataTable;
        string endpointDelayedTableName;
        static System.Xml.Serialization.XmlSerializer headerSerializer = new System.Xml.Serialization.XmlSerializer(typeof(List<HeaderInfo>));

        public SqlTEndpointTarget(ILogger logger, SqlConnection connection, string endpointName, string schema)
        {
            this.logger = logger;
            this.schema = schema;
            this.connection = connection;
            databaseName = connection.Database;
            endpointDelayedTableName = SqlConstants.DelayedTableName(endpointName);

            stagingDataTable = new DataTable();
            stagingDataTable.Columns.Add("Id");
            stagingDataTable.Columns.Add("Destination");
            stagingDataTable.Columns.Add("State", typeof(byte[]));
            stagingDataTable.Columns.Add("Time", typeof(DateTime));
            stagingDataTable.Columns.Add("Headers", typeof(byte[]));
        }

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            await DelayedDeliveryTableCreator.TruncateTable(connection, SqlConstants.TimeoutMigrationStagingTable, schema, databaseName);

            foreach (var timeout in timeouts)
            {
                stagingDataTable.Rows.Add(
                    timeout.Id,
                    timeout.Destination,
                    timeout.State,
                    timeout.Time,
                    SerializeHeaders(timeout));
            }

            await using var transaction = (SqlTransaction)await connection.BeginTransactionAsync();
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
                logger.LogError(e, $"Unable to bulk copy batch '{batchNumber}' to the staging table. Exception occured: {e.Message}");
                return 0;
            }
            finally
            {
                stagingDataTable.Clear();
            }

            return timeouts.Count;
        }

        static byte[] SerializeHeaders(TimeoutData timeout)
        {
            byte[] headerArray;

            using (var stream = new MemoryStream())
            {
                var headers = timeout.Headers.Select(pair => new HeaderInfo
                {
                    Key = pair.Key,
                    Value = pair.Value
                }).ToList();

                headerSerializer.Serialize(stream, headers);
                headerArray = stream.ToArray();
            }

            return headerArray;
        }

        public async ValueTask<int> CompleteBatch(int batchNumber)
        {
            return await DelayedDeliveryTableCreator.MoveFromTo(connection, SqlConstants.TimeoutMigrationStagingTable, schema, endpointDelayedTableName, schema, databaseName);
        }

        public async ValueTask DisposeAsync()
        {
            await connection.CloseAsync();
        }
    }
}