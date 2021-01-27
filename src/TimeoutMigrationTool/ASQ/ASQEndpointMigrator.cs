namespace Particular.TimeoutMigrationTool.ASQ
{
    using System;
    using Microsoft.Azure.Cosmos.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos.Table.Queryable;

    public class ASQEndpointMigrator : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        public ASQEndpointMigrator(CloudTableClient client, string destinationTimeoutTableName, string stagingTableName)
        {
            this.client = client;
            this.destinationTimeoutTableName = destinationTimeoutTableName;
            this.stagingTableName = stagingTableName;
        }

        public async ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            await PurgeStagingTable();

            var timeoutsToBeStaged = timeouts.Select(timeout => StagedDelayedMessageEntity.FromTimeoutData(timeout, batchNumber)).GroupBy(x => x.PartitionKey);
            var table = client.GetTableReference(stagingTableName);

            var timeoutsStaged = await BatchProcessRecords(table, timeoutsToBeStaged, (operation, entity) => operation.InsertOrReplace(entity));
            return timeoutsStaged;
        }

        public async ValueTask<int> CompleteBatch(int batchNumber)
        {
            var stagingTable = client.GetTableReference(stagingTableName);
            var query = stagingTable.CreateQuery<StagedDelayedMessageEntity>().Where(x => x.PartitionKey == batchNumber.ToString()).AsTableQuery();
            var delayedMessages = await stagingTable.ExecuteQueryAsync(query, CancellationToken.None);

            var destinationTable = client.GetTableReference(destinationTimeoutTableName);
            var timeouts = delayedMessages.Select(DelayedMessageEntity.FromStagedTimeout);

            var groupedTimeouts = timeouts.GroupBy(timeout => timeout.PartitionKey);
            var timeoutsMigrated = await BatchProcessRecords(destinationTable, groupedTimeouts, (operation, entity) => operation.InsertOrReplace(entity));

            await ClearStagedTimeoutsForBatch(batchNumber);
            return timeoutsMigrated;
        }

        public ValueTask DisposeAsync()
        {
            return new ValueTask();
        }

        async Task ClearStagedTimeoutsForBatch(int batchNumber)
        {
            var stagingTable = client.GetTableReference(stagingTableName);
            var query = stagingTable.CreateQuery<StagedDelayedMessageEntity>().Where(x => x.PartitionKey == batchNumber.ToString()).AsTableQuery();
            var delayedMessages = await stagingTable.ExecuteQueryAsync(query, CancellationToken.None);

            var groupedRecords = delayedMessages.GroupBy(x => x.PartitionKey);
            await BatchProcessRecords(stagingTable, groupedRecords, (operation, entity) => operation.Delete(entity));
        }

        async Task PurgeStagingTable()
        {
            var table = client.GetTableReference(stagingTableName);
            var query = new TableQuery<DynamicTableEntity>().Select(new[] { "PartitionKey", "RowKey" });
            var continuationToken = new TableContinuationToken();
            var batchOperation = new TableBatchOperation();
            var batchTasks = new List<Task>();

            static async Task ExecuteBatch(CloudTable table, TableBatchOperation batchToBeCopied)
            {
                var copiedBatch = new TableBatchOperation();
                foreach (var operation in batchToBeCopied)
                {
                    copiedBatch.Add(operation);
                }
                await table.ExecuteBatchAsync(copiedBatch).ConfigureAwait(false);
            }

            do
            {
                var page = table.ExecuteQuerySegmented(query, continuationToken);
                var partitions = page.GroupBy(x => x.PartitionKey);

                foreach (var partition in partitions)
                {
                    foreach (var entity in partition)
                    {
                        if (batchOperation.Count < MaxOperationsPerBatchOperation)
                        {
                            batchOperation.Delete(entity);
                        }
                        else
                        {
                            batchTasks.Add(ExecuteBatch(table, batchOperation));
                            batchOperation.Clear();
                        }
                    }

                    if (batchOperation.Any())
                    {
                        batchTasks.Add(ExecuteBatch(table, batchOperation));
                        batchOperation.Clear();
                    }
                }

                continuationToken = page.ContinuationToken;

            } while (continuationToken != null);

            await Task.WhenAll(batchTasks).ConfigureAwait(false);
        }

        async Task<int> BatchProcessRecords<T>(CloudTable table,
            IEnumerable<IGrouping<string, T>> groupedRecordsByPartition, Action<TableBatchOperation, T> batchOperationAction) where T : TableEntity, ICanCalculateMySize
        {
            var batchOperation = new TableBatchOperation();
            var batchTasks = new List<Task<int>>();

            static async Task<int> ExecuteBatch(CloudTable table, TableBatchOperation batchToBeCopied)
            {
                var copiedBatch = new TableBatchOperation();
                foreach (var operation in batchToBeCopied)
                {
                    copiedBatch.Add(operation);
                }
                var result = await table.ExecuteBatchAsync(copiedBatch).ConfigureAwait(false);
                return result.Count;
            }

            foreach (var grouping in groupedRecordsByPartition)
            {
                var currentBatchOperationSize = 0L;
                var recordsInGroup = grouping.ToList();
                foreach (var record in recordsInGroup)
                {
                    var recordSize = record.CalculateSize();
                    if (currentBatchOperationSize + recordSize > MaxPayloadPerBatchOperation || batchOperation.Count == MaxOperationsPerBatchOperation)
                    {
                        batchTasks.Add(ExecuteBatch(table, batchOperation));
                        batchOperation.Clear();
                        currentBatchOperationSize = 0;
                    }

                    batchOperationAction(batchOperation, record);
                    currentBatchOperationSize += recordSize;
                }

                if (batchOperation.Any())
                {
                    batchTasks.Add(ExecuteBatch(table, batchOperation));
                    batchOperation.Clear();
                }
            }

            await Task.WhenAll(batchTasks).ConfigureAwait(false);
            return batchTasks.Sum(x => x.Result);
        }

        CloudTableClient client;
        string destinationTimeoutTableName;
        string stagingTableName;
        const long MaxPayloadPerBatchOperation = 4096 * 1024;
        const long MaxOperationsPerBatchOperation = 100;
    }
}