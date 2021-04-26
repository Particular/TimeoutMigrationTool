namespace Particular.TimeoutMigrationTool.Asp
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Net;
    using System.Reflection;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos.Table;
    using Newtonsoft.Json;
    using static Microsoft.Azure.Cosmos.Table.TableQuery;

    public class AspTimeoutsSource : ITimeoutsSource
    {
        readonly string connectionString;
        readonly int batchSize;
        readonly string tablePrefix;
        readonly string containerName;
        readonly string partitionKeyScope;
        string endpointNameToBelisted;
        string timeoutTableName;

        const long MaxPayloadPerBatchOperation = 4092 * 1024;
        const int MaxOperationsPerBatchOperation = 100;

        public AspTimeoutsSource(string connectionString, int batchSize, string containerName,
            string endpointNameToBelisted, string timeoutTableName, string tablePrefix = null,
            string partitionKeyScope = AspConstants.PartitionKeyScope)
        {
            this.timeoutTableName = timeoutTableName;
            this.endpointNameToBelisted = endpointNameToBelisted;
            this.partitionKeyScope = partitionKeyScope;
            this.containerName = containerName;
            this.tablePrefix = tablePrefix;
            this.batchSize = batchSize;
            this.connectionString = connectionString;
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            var tableClient = CreateCloudTableClient();

            var toolStateEntity = await TryLoadNotCompletedAndNotAbortedToolStateEntity(tableClient);

            return toolStateEntity == null
                ? null
                : new AspToolState(GetNextBatch, toolStateEntity.RunParameters, toolStateEntity.EndpointName,
                    toolStateEntity.NumberOfBatches, toolStateEntity.Status);
        }

        async Task<BatchInfo> GetNextBatch()
        {
            var tableClient = CreateCloudTableClient();

            var toolStateEntity = await TryLoadNotCompletedAndNotAbortedToolStateEntity(tableClient);

            if (toolStateEntity.CurrentBatchNumber > toolStateEntity.BatchNumberAndSizes.Count)
            {
                return null;
            }

            return new BatchInfo(toolStateEntity.CurrentBatchNumber, toolStateEntity.CurrentBatchState,
                toolStateEntity.BatchNumberAndSizes[toolStateEntity.CurrentBatchNumber - 1].batchSize);
        }

        async Task<CloudTable> GetAndCreateToolStateTableIfNotExists(CloudTableClient tableClient)
        {
            var toolStateTable = tableClient.GetTableReference($"{tablePrefix}{AspConstants.ToolStateTableName}");
            await toolStateTable.CreateIfNotExistsAsync();
            return toolStateTable;
        }

        CloudTableClient CreateCloudTableClient()
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var tableClient = account.CreateCloudTableClient();
            return tableClient;
        }

        public async Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName,
            IDictionary<string, string> runParameters)
        {
            var tableClient = CreateCloudTableClient();

            var toolStateTable = await GetAndCreateToolStateTableIfNotExists(tableClient);

            var migrationTable = GetMigrationTable(tableClient);
            await PurgeMigrationTable(migrationTable);

            var endpointTimeoutTable = await GetEndpointTimeoutTable(endpointName, tableClient);

            var toolStateEntity = new ToolStateEntity
            {
                EndpointName = endpointName,
                MigrationRunId = Guid.NewGuid(),
                RunParameters = runParameters,
                Status = MigrationStatus.Preparing,
                UniqueHiddenEndpointName = string.Format(AspConstants.MigrationHiddenEndpointNameFormat, Path.GetFileNameWithoutExtension(Path.GetTempFileName()), endpointName)
            };

            await toolStateTable.ExecuteAsync(TableOperation.Insert(toolStateEntity));

            var cutOffTimeAsPartitionKeyScope = maxCutoffTime.ToString(partitionKeyScope);
            var upperLimitForCutOffToFilterGuidEntriesOut = maxCutoffTime.AddYears(100).ToString(partitionKeyScope);

            // we introduce an upper limit in the query to make sure we never get entries that use a guid as a partition key
            // unfortunately if we select only the lower limit due to lexicographical query analysis guid entries might still match
            // this does not entirely eliminate the possibility of getting guid entries though but at least slims down the changes
            // that we stream data to the client we are never interested in
            var query = new TableQuery<TimeoutDataEntity>()
                .Where(CombineFilters(
                    CombineFilters(
                        GenerateFilterCondition(nameof(TimeoutDataEntity.PartitionKey), QueryComparisons.GreaterThanOrEqual,
                            cutOffTimeAsPartitionKeyScope),
                        TableOperators.And,
                        GenerateFilterCondition(nameof(TimeoutDataEntity.PartitionKey), QueryComparisons.LessThanOrEqual,
                            upperLimitForCutOffToFilterGuidEntriesOut)
                    ),
                    TableOperators.And,
                    GenerateFilterCondition(nameof(TimeoutDataEntity.OwningTimeoutManager), QueryComparisons.Equal, toolStateEntity.EndpointName)));

            TableContinuationToken token = null;
            CancellationToken queryCancellationToken = CancellationToken.None;
            var totalNumberOfItems = 0;
            int previousBatch = 1, currentBatch = 1, numberOfItemsInBatch = 0;
            var batchNumberAndSizes = new List<(int batchNumber, int batchSize)>();

            do
            {
                // will issue a table scan for now
                var seg = await endpointTimeoutTable.ExecuteQuerySegmentedAsync(
                        query: query,
                        token: token,
                        requestOptions: null,
                        operationContext: null,
                        cancellationToken: queryCancellationToken)
                    .ConfigureAwait(false);
                token = seg.ContinuationToken;

                // need to copy since to make sure there are no side effects
                var timeoutEntitiesToHide = seg.Results.Select(r => r.Clone()).Cast<TimeoutDataEntity>();
                var hideTimeoutsTask = HideTimeoutsOfCurrentSegment(endpointTimeoutTable, timeoutEntitiesToHide, toolStateEntity.UniqueHiddenEndpointName);

                // no need to copy since we don't care about the entities anymore after this stage
                var timeoutEntitiesToMigrate = seg.Results;
                var migrateTimeoutTask = MigrateTimeoutsOfCurrentSegment(
                    migrationTable,
                    timeoutEntitiesToMigrate,
                    batchNumberAndSizes,
                    previousBatch,
                    currentBatch,
                    totalNumberOfItems,
                    numberOfItemsInBatch,
                    batchSize);

                await Task.WhenAll(hideTimeoutsTask, migrateTimeoutTask);

                (previousBatch, currentBatch, totalNumberOfItems, numberOfItemsInBatch) = await migrateTimeoutTask;

            } while (token != null && !queryCancellationToken.IsCancellationRequested);

            if (numberOfItemsInBatch > 0)
            {
                batchNumberAndSizes.Add((currentBatch, numberOfItemsInBatch));
            }

            toolStateEntity.Status = MigrationStatus.StoragePrepared;
            toolStateEntity.BatchNumberAndSizes = batchNumberAndSizes;
            toolStateEntity.CurrentBatchState = BatchState.Pending;
            toolStateEntity.CurrentBatchNumber = 1;

            await toolStateTable.ExecuteAsync(TableOperation.Merge(toolStateEntity));

            return new AspToolState(GetNextBatch, toolStateEntity.RunParameters, toolStateEntity.EndpointName,
                toolStateEntity.NumberOfBatches, toolStateEntity.Status);
        }

        static async Task PurgeMigrationTable(CloudTable migrationTable)
        {
            await migrationTable.DeleteIfExistsAsync();

            var created = false;
            do
            {
                try
                {
                    await migrationTable.CreateIfNotExistsAsync();
                    created = true;
                }
                catch (StorageException ex) when (ex.RequestInformation.HttpStatusCode == (int)HttpStatusCode.Conflict)
                {
                    await Task.Delay(1000);
                }
            } while (!created);
        }

        static async Task HideTimeoutsOfCurrentSegment(CloudTable endpointTimeoutTable,
            IEnumerable<TimeoutDataEntity> timeoutsOfSegment, string uniqueHiddenEndpointName)
        {
            // with batching we can at least efficiently insert
            long hideByPartitionKeyBatchSize = 0;
            var hideByPartitionBatch = new TableBatchOperation();
            var hideTasks = new List<Task>();
            foreach (var entitiesInTheSamePartition in timeoutsOfSegment.GroupBy(x => x.PartitionKey))
            {
                foreach (var timeoutDataEntity in entitiesInTheSamePartition)
                {
                    // entries with Guid as partition key should never be modified. We are only interested in the query entities
                    if (Guid.TryParse(timeoutDataEntity.PartitionKey.AsSpan(), out _))
                    {
                        continue;
                    }

                    // we don't want to preserve the etag and we always want to win
                    timeoutDataEntity.ETag = "*";
                    // Fix the hiding part
                    timeoutDataEntity.OwningTimeoutManager = uniqueHiddenEndpointName;

                    var entitySize = timeoutDataEntity.CalculateSize();

                    // the batch can have max 100 items and max 4 MB of data
                    // the partition key for all operations in the batch has to be the same
                    if (hideByPartitionKeyBatchSize + entitySize > MaxPayloadPerBatchOperation ||
                        hideByPartitionBatch.Count == MaxOperationsPerBatchOperation)
                    {
                        hideTasks.Add(endpointTimeoutTable.ExecuteBatchAsync(hideByPartitionBatch.Clone()));
                        hideByPartitionKeyBatchSize = 0;
                        hideByPartitionBatch.Clear();
                    }

                    hideByPartitionKeyBatchSize += entitySize;
                    var tableOperation = TableOperation.Merge(timeoutDataEntity);
                    SetEchoContentTo(tableOperation, false);
                    hideByPartitionBatch.Add(tableOperation);
                }

                if (hideByPartitionBatch.Count > 0)
                {
                    hideTasks.Add(endpointTimeoutTable.ExecuteBatchAsync(hideByPartitionBatch.Clone()));
                }

                hideByPartitionBatch.Clear();
                hideByPartitionKeyBatchSize = 0;
            }

            if (hideTasks.Count > 0)
            {
                await Task.WhenAll(hideTasks);
            }
        }

        static async Task<(int previousBatch, int currentBatch, int totalNumberOfItems, int numberOfItemsInBatch)>
            MigrateTimeoutsOfCurrentSegment(
                CloudTable migrationTable,
                IEnumerable<TimeoutDataEntity> timeoutsOfSegment,
                IList<(int batchNumber, int batchSize)> batchNumberAndSizes,
                int previousBatch,
                int currentBatch,
                int totalNumberOfItems,
                int numberOfItemsInBatch,
                int batchSize)
        {
            // with batching we can at least efficiently insert
            long migrationBatchSize = 0;
            var migrationBatch = new TableBatchOperation();
            var batchTasks = new List<Task>();
            foreach (var timeoutDataEntity in timeoutsOfSegment)
            {
                // entries with Guid as partition key should never be modified. We are only interested in the query entities
                if (Guid.TryParse(timeoutDataEntity.PartitionKey.AsSpan(), out _))
                {
                    continue;
                }

                previousBatch = currentBatch;
                currentBatch = (totalNumberOfItems / batchSize) + 1;

                // there is no need to download the state from blob storage yet, we can do that when migrating the batches
                var migratedTimeoutDataEntity = new MigratedTimeoutDataEntity(timeoutDataEntity, currentBatch);

                var entitySize = migratedTimeoutDataEntity.CalculateSize();

                if (currentBatch != previousBatch)
                {
                    batchNumberAndSizes.Add((previousBatch, numberOfItemsInBatch));
                    numberOfItemsInBatch = 0;
                }

                // the batch can have max 100 items and max 4 MB of data
                // the partition key for all operations in the batch has to be the same
                if (currentBatch != previousBatch || migrationBatchSize + entitySize > MaxPayloadPerBatchOperation ||
                    migrationBatch.Count == MaxOperationsPerBatchOperation)
                {
                    batchTasks.Add(migrationBatch.Count > 0 ? migrationTable.ExecuteBatchAsync(migrationBatch.Clone()) : Task.CompletedTask);
                    migrationBatchSize = 0;
                    migrationBatch.Clear();
                }

                migrationBatch.Add(TableOperation.Insert(migratedTimeoutDataEntity, false));

                migrationBatchSize += entitySize;
                numberOfItemsInBatch++;
                totalNumberOfItems++;
            }

            if (migrationBatch.Count > 0)
            {
                batchTasks.Add(migrationTable.ExecuteBatchAsync(migrationBatch.Clone()));
            }

            if (batchTasks.Count > 0)
            {
                await Task.WhenAll(batchTasks);
            }

            return (previousBatch, currentBatch, totalNumberOfItems, numberOfItemsInBatch);
        }

        CloudTable GetMigrationTable(CloudTableClient tableClient)
        {
            var migrationTable = tableClient.GetTableReference($"{tablePrefix}{AspConstants.MigrationTableName}");
            return migrationTable;
        }

        async Task<CloudTable> GetEndpointTimeoutTable(string endpointName, CloudTableClient tableClient)
        {
            var endpointTimeoutTableName = $"{tablePrefix}{timeoutTableName}";
            var endpointTimeoutTable = tableClient.GetTableReference(endpointTimeoutTableName);

            if (!await endpointTimeoutTable.ExistsAsync())
            {
                throw new Exception(
                    $"The timeout table '{endpointTimeoutTableName}' of the endpoint '{endpointName}' was not found.");
            }

            return endpointTimeoutTable;
        }

        public async Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            var tableClient = CreateCloudTableClient();
            var blobServiceClient = new BlobServiceClient(connectionString);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(containerName);

            var migrationTable = GetMigrationTable(tableClient);

            var query = new TableQuery<MigratedTimeoutDataEntity>()
                .Where(CombineFilters(
                    GenerateFilterCondition(nameof(MigratedTimeoutDataEntity.PartitionKey), QueryComparisons.Equal,
                        batchNumber.ToString(CultureInfo.InvariantCulture)),
                    TableOperators.And,
                    GenerateFilterConditionForInt(nameof(MigratedTimeoutDataEntity.BatchState), QueryComparisons.LessThan, (int)BatchState.Completed)));

            TableContinuationToken token = null;
            var timeoutData = new List<TimeoutData>(batchSize);
            CancellationToken queryCancellationToken = CancellationToken.None;
            do
            {
                // will issue a table scan for now
                var seg = await migrationTable.ExecuteQuerySegmentedAsync(
                        query: query,
                        token: token,
                        requestOptions: null,
                        operationContext: null,
                        cancellationToken: queryCancellationToken)
                    .ConfigureAwait(false);
                token = seg.ContinuationToken;

                await foreach (var data in MaterializeTimeoutData(blobContainerClient, seg.Results,
                    queryCancellationToken))
                {
                    timeoutData.Add(data);
                }

            } while (token != null && !queryCancellationToken.IsCancellationRequested);

            return timeoutData;
        }

        async IAsyncEnumerable<TimeoutData> MaterializeTimeoutData(
            BlobContainerClient blobContainerClient,
            IEnumerable<MigratedTimeoutDataEntity> timeoutDataEntities,
            [EnumeratorCancellation] CancellationToken cancellationToken,
            int maxConcurrentBlobDownloads = 200) // a single blob can handle up to 500 req/s
        {
            using var throttler = new SemaphoreSlim(maxConcurrentBlobDownloads);

            var tasks = new List<Task<TimeoutData>>();
            foreach (var timeoutDataEntity in timeoutDataEntities)
            {
                tasks.Add(MaterializeTimeout(blobContainerClient, timeoutDataEntity, throttler, cancellationToken));
            }

            while (tasks.Count > 0 && !cancellationToken.IsCancellationRequested)
            {
                var done = await Task.WhenAny(tasks).ConfigureAwait(false);
                tasks.Remove(done);

                yield return await done
                    .ConfigureAwait(false);
            }
        }

        static async Task<TimeoutData> MaterializeTimeout(BlobContainerClient blobContainerClient,
            MigratedTimeoutDataEntity entity, SemaphoreSlim throttler, CancellationToken cancellationToken)
        {
            try
            {
                await throttler.WaitAsync(cancellationToken);

                var blob = blobContainerClient.GetBlobClient(entity.StateAddress);
                await using var stream = new MemoryStream();
                await blob.DownloadToAsync(stream, cancellationToken).ConfigureAwait(false);

                return new TimeoutData
                {
                    Destination = entity.Destination,
                    Headers = JsonConvert.DeserializeObject<Dictionary<string, string>>(entity.Headers),
                    Id = entity.RowKey,
                    OwningTimeoutManager = entity.OwningTimeoutManager,
                    SagaId = entity.SagaId,
                    State = stream.ToArray(),
                    Time = entity.Time
                };
            }
            finally
            {
                throttler.Release();
            }
        }

        public Task MarkBatchAsCompleted(int batchNumber) => ChangeBatchState(batchNumber, batchNumber + 1, BatchState.Completed);

        public Task MarkBatchAsStaged(int batchNumber) => ChangeBatchState(batchNumber, batchNumber, BatchState.Staged);

        async Task ChangeBatchState(int batchNumber, int nextBatch, BatchState batchState)
        {
            var tableClient = CreateCloudTableClient();

            var migrationTable = GetMigrationTable(tableClient);

            var query = new TableQuery<PartialMigratedTimeoutDataEntityWithBatchState>()
                .Where(GenerateFilterCondition(nameof(PartialMigratedTimeoutDataEntityWithBatchState.PartitionKey), QueryComparisons.Equal,
                    batchNumber.ToString(CultureInfo.InvariantCulture)));

            TableContinuationToken token = null;
            CancellationToken queryCancellationToken = CancellationToken.None;

            do
            {
                // will issue a table scan for now
                var seg = await migrationTable.ExecuteQuerySegmentedAsync(
                        query: query,
                        token: token,
                        requestOptions: null,
                        operationContext: null,
                        cancellationToken: CancellationToken.None)
                    .ConfigureAwait(false);
                token = seg.ContinuationToken;

                var changeTasks = new List<Task>(seg.Results.Count / MaxOperationsPerBatchOperation);
                // with batching we can at least efficiently update
                var batch = new TableBatchOperation();
                foreach (var t in seg.Results)
                {
                    t.BatchState = batchState;

                    // we don't want to preserve the etag and we always want to win
                    t.ETag = "*";

                    // the batch can have max 100 items and max 4 MB of data
                    // we don't need to check the size since we are only getting partition and row key as well as batch state
                    // the partition key for all operations in the batch has to be the same
                    if (batch.Count == MaxOperationsPerBatchOperation)
                    {
                        changeTasks.Add(migrationTable.ExecuteBatchAsync(batch.Clone()));
                        batch.Clear();
                    }

                    var tableOperation = TableOperation.Merge(t);
                    SetEchoContentTo(tableOperation, false);
                    batch.Add(tableOperation);
                }

                if (batch.Count > 0)
                {
                    changeTasks.Add(migrationTable.ExecuteBatchAsync(batch.Clone()));
                }

                if (changeTasks.Count > 0)
                {
                    await Task.WhenAll(changeTasks);
                }
            } while (token != null && !queryCancellationToken.IsCancellationRequested);

            var toolState = await TryLoadNotCompletedAndNotAbortedToolStateEntity(tableClient);
            toolState.CurrentBatchNumber = nextBatch;

            var toolStateTable = await GetAndCreateToolStateTableIfNotExists(tableClient);
            await toolStateTable.ExecuteAsync(TableOperation.Merge(toolState));
        }

        public async Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            var tableClient = CreateCloudTableClient();

            var endpointTimeoutTable = await GetEndpointTimeoutTable(endpointNameToBelisted, tableClient);

            var cutOffTimeAsPartitionKeyScope = cutOffTime.ToString(partitionKeyScope);
            var upperLimitForCutOffToFilterGuidEntriesOut = cutOffTime.AddYears(100).ToString(partitionKeyScope);

            // we introduce an upper limit in the query to make sure we never get entries that use a guid as a partition key
            // unfortunately if we select only the lower limit due to lexicographical query analysis guid entries might still match
            // this does not entirely eliminate the possibility of getting guid entries though but at least slims down the changes
            // that we stream data to the client we are never interested in
            var query = new TableQuery<DynamicTableEntity>()
                .Where(CombineFilters(
                    CombineFilters(
                        GenerateFilterCondition(nameof(DynamicTableEntity.PartitionKey), QueryComparisons.GreaterThanOrEqual,
                            cutOffTimeAsPartitionKeyScope),
                        TableOperators.And,
                        GenerateFilterCondition(nameof(DynamicTableEntity.PartitionKey), QueryComparisons.LessThanOrEqual,
                            upperLimitForCutOffToFilterGuidEntriesOut)
                    ),
                    TableOperators.And,
                    GenerateFilterCondition(nameof(TimeoutDataEntity.OwningTimeoutManager), QueryComparisons.Equal, endpointNameToBelisted)));

            query.SelectColumns = new List<string> { nameof(DynamicTableEntity.PartitionKey), nameof(TimeoutDataEntity.Destination) };

            TableContinuationToken token = null;
            var numberOfTimeouts = 0;
            CancellationToken queryCancellationToken = CancellationToken.None;
            DateTime minDateTime = DateTime.MaxValue;
            DateTime maxDateTime = DateTime.MinValue;
            var destinations = new HashSet<string>(StringComparer.Ordinal);
            do
            {
                var seg = await endpointTimeoutTable.ExecuteQuerySegmentedAsync(
                        query: query,
                        token: token,
                        requestOptions: null,
                        operationContext: null,
                        cancellationToken: queryCancellationToken)
                    .ConfigureAwait(false);
                token = seg.ContinuationToken;

                foreach (var timeoutEntry in seg.Results)
                {
                    if (!DateTime.TryParseExact(timeoutEntry.PartitionKey, partitionKeyScope,
                        CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal,
                        out var parsedPartitionKeyAsDateTime) || parsedPartitionKeyAsDateTime <= cutOffTime)
                    {
                        continue;
                    }

                    numberOfTimeouts++;

                    if (parsedPartitionKeyAsDateTime > maxDateTime)
                    {
                        maxDateTime = parsedPartitionKeyAsDateTime;
                    }

                    if (parsedPartitionKeyAsDateTime < minDateTime)
                    {
                        minDateTime = parsedPartitionKeyAsDateTime;
                    }

                    destinations.Add(timeoutEntry["Destination"].StringValue);
                }
            } while (token != null && !queryCancellationToken.IsCancellationRequested);

            return new List<EndpointInfo>
            {
                new EndpointInfo
                {
                    Destinations = destinations.ToList(),
                    EndpointName = endpointNameToBelisted,
                    LongestTimeout = maxDateTime,
                    ShortestTimeout = minDateTime,
                    NrOfTimeouts = numberOfTimeouts
                }
            };
        }

        public async Task Abort()
        {
            var tableClient = CreateCloudTableClient();

            ToolStateEntity toolState = await TryLoadNotCompletedAndNotAbortedToolStateEntity(tableClient);

            var toolStateTable = await GetAndCreateToolStateTableIfNotExists(tableClient);

            var endpointTimeoutTable = await GetEndpointTimeoutTable(toolState.EndpointName, tableClient);

            var lowerLimitForCutOffToFilterGuidEntriesOut = DateTime.UtcNow.AddYears(-1).ToString(partitionKeyScope);
            var upperLimitForCutOffToFilterGuidEntriesOut = DateTime.UtcNow.AddYears(100).ToString(partitionKeyScope);

            // we introduce an upper limit in the query to make sure we never get entries that use a guid as a partition key
            // unfortunately if we select only the lower limit due to lexicographical query analysis guid entries might still match
            // this does not entirely eliminate the possibility of getting guid entries though but at least slims down the changes
            // that we stream data to the client we are never interested in
            var query = new TableQuery<PartialTimeoutDataEntityWithOwningTimeoutManager>()
                .Where(CombineFilters(
                    CombineFilters(
                        GenerateFilterCondition(nameof(PartialTimeoutDataEntityWithOwningTimeoutManager.PartitionKey), QueryComparisons.GreaterThanOrEqual,
                            lowerLimitForCutOffToFilterGuidEntriesOut),
                        TableOperators.And,
                        GenerateFilterCondition(nameof(PartialTimeoutDataEntityWithOwningTimeoutManager.PartitionKey), QueryComparisons.LessThanOrEqual,
                            upperLimitForCutOffToFilterGuidEntriesOut)
                    ),
                    TableOperators.And,
                    GenerateFilterCondition(nameof(PartialTimeoutDataEntityWithOwningTimeoutManager.OwningTimeoutManager), QueryComparisons.Equal,
                        toolState.UniqueHiddenEndpointName)));

            query.SelectColumns = new List<string> { nameof(PartialTimeoutDataEntityWithOwningTimeoutManager.OwningTimeoutManager) };
            TableContinuationToken token = null;
            CancellationToken queryCancellationToken = CancellationToken.None;

            do
            {
                // will issue a table scan for now
                var seg = await endpointTimeoutTable.ExecuteQuerySegmentedAsync(
                        query: query,
                        token: token,
                        requestOptions: null,
                        operationContext: null,
                        cancellationToken: queryCancellationToken)
                    .ConfigureAwait(false);
                token = seg.ContinuationToken;

                foreach (var t in seg.Results.GroupBy(x => x.PartitionKey))
                {
                    // entries with Guid as partition key should never be modified. We are only interested in the query entities
                    if (Guid.TryParse(t.Key.AsSpan(), out _))
                    {
                        continue;
                    }

                    // with batching we can at least efficiently insert
                    var batch = new TableBatchOperation();
                    foreach (var partialTimeoutDataEntity in t)
                    {
                        var currentOwningTimeoutManager = partialTimeoutDataEntity.OwningTimeoutManager;

                        partialTimeoutDataEntity.ETag = "*";
                        partialTimeoutDataEntity.OwningTimeoutManager = toolState.EndpointName;

                        // the batch can have max 100 items and max 4 MB of data
                        // we don't need to check the size since we are only getting OwningTimeoutManager which is size restricted
                        // the partition key for all operations in the batch has to be the same
                        if (batch.Count == 100)
                        {
                            await endpointTimeoutTable.ExecuteBatchAsync(batch);
                            batch.Clear();
                        }

                        var tableOperation = TableOperation.Merge(partialTimeoutDataEntity);
                        SetEchoContentTo(tableOperation, false);
                        batch.Add(tableOperation);
                    }

                    if (batch.Count > 0)
                    {
                        await endpointTimeoutTable.ExecuteBatchAsync(batch);
                    }
                }
            } while (token != null && !queryCancellationToken.IsCancellationRequested);

            toolState.Status = MigrationStatus.Aborted;

            // todo error checking
            await toolStateTable.ExecuteAsync(TableOperation.Replace(toolState));
        }

        public async Task Complete()
        {
            var tableClient = CreateCloudTableClient();

            var toolStateTable = await GetAndCreateToolStateTableIfNotExists(tableClient);

            var query = new TableQuery<ToolStateEntity>()
                .Where(
                    CombineFilters(
                        GenerateFilterCondition(nameof(ToolStateEntity.PartitionKey), QueryComparisons.Equal,
                            ToolStateEntity.FixedPartitionKey),
                        TableOperators.And,
                        GenerateFilterConditionForInt(nameof(ToolStateEntity.Status), QueryComparisons.Equal, (int)MigrationStatus.StoragePrepared)))
                .Take(1);

            var toolStateEntity =
                (await toolStateTable.ExecuteQuerySegmentedAsync(query, null)).Results.SingleOrDefault();

            toolStateEntity.Status = MigrationStatus.Completed;
            toolStateEntity.CompletedAt = DateTime.UtcNow;

            await toolStateTable.ExecuteAsync(TableOperation.Replace(toolStateEntity));
        }

        public async Task<bool> CheckIfAMigrationIsInProgress()
        {
            var tableClient = CreateCloudTableClient();

            return await TryLoadNotCompletedAndNotAbortedToolStateEntity(tableClient) != null;
        }

        async Task<ToolStateEntity> TryLoadNotCompletedAndNotAbortedToolStateEntity(CloudTableClient tableClient)
        {
            var toolStateTable = await GetAndCreateToolStateTableIfNotExists(tableClient);

            var query = new TableQuery<ToolStateEntity>()
                .Where(
                    CombineFilters(
                        GenerateFilterCondition(nameof(ToolStateEntity.PartitionKey), QueryComparisons.Equal,
                            ToolStateEntity.FixedPartitionKey),
                        TableOperators.And,
                        GenerateFilterConditionForInt(nameof(ToolStateEntity.Status), QueryComparisons.LessThan, (int)MigrationStatus.Completed)))
                .Take(1);

            var toolStateEntities = await toolStateTable.ExecuteQuerySegmentedAsync(query, null);
            return toolStateEntities.Results.SingleOrDefault();
        }

        // unfortunately EchoContent is by default set to true which means the server will stream back the data we have written and thus creating additional
        // overhead. The public API only allows to set echoContent to false on insert operations. So we are using this backdoor here to make it efficient
        static Action<TableOperation, bool> CreateEchoContentSetter()
        {
            ParameterExpression instance = Expression.Parameter(typeof(TableOperation), "instance");
            ParameterExpression echoContentParameter = Expression.Parameter(typeof(bool), "param");

            var echoContentProperty =
                typeof(TableOperation).GetProperty("EchoContent", BindingFlags.Instance | BindingFlags.NonPublic);

            var body = Expression.Call(instance, echoContentProperty.SetMethod, echoContentParameter);
            var parameters = new[] { instance, echoContentParameter };

            return Expression.Lambda<Action<TableOperation, bool>>(body, parameters).Compile();
        }

        static readonly Action<TableOperation, bool> SetEchoContentTo = CreateEchoContentSetter();
    }
}
