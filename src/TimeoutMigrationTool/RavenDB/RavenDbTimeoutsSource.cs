namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    public class RavenDbTimeoutsSource : ITimeoutsSource
    {
        public RavenDbTimeoutsSource(
            ILogger logger,
            string serverUrl,
            string databaseName,
            string timeoutDocumentPrefix,
            RavenDbVersion ravenVersion,
            bool useIndex)
        {
            this.logger = logger;
            this.timeoutDocumentPrefix = timeoutDocumentPrefix;
            this.ravenVersion = ravenVersion;
            this.useIndex = useIndex;
            ravenAdapter = RavenDataReaderFactory.Resolve(serverUrl, databaseName, ravenVersion);
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });
            if (ravenToolState == null)
            {
                return null;
            }

            var batches = await ravenAdapter.GetDocuments<RavenBatch>(ravenToolState.Batches, (doc, id) => { });
            return ravenToolState.ToToolState(batches);
        }

        public async Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTimeOffset cutoffTime)
        {
            var nrOfTimeoutsFound = await GuardAgainstTooManyTimeoutsWithoutIndexUsage();

            bool Filter(TimeoutData td)
            {
                return td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(
                    RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            if (useIndex)
            {
                return await ListEndpointsUsingIndex(Filter);
            }

            return await ListEndpointsWithoutIndex(Filter, nrOfTimeoutsFound);
        }

        async Task<List<EndpointInfo>> ListEndpointsWithoutIndex(Func<TimeoutData, bool> filter, int nrOfTimeoutsFound)
        {
            var findMoreTimeouts = true;
            var endpoints = new Dictionary<string, EndpointInfo>();
            var nrOfTimeoutsRetrieved = 0;
            var nrOfPages = 3;

            var tcs = new CancellationTokenSource();
            var printTask = Task.Run(
                async () =>
            {
                while (!tcs.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), tcs.Token);
                    logger.LogInformation($"{nrOfTimeoutsRetrieved} of {nrOfTimeoutsFound} have been scanned.");
                }
            }, CancellationToken.None);

            do
            {
                var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, nrOfTimeoutsRetrieved, (doc, id) => doc.Id = id, nrOfPages);

                nrOfTimeoutsRetrieved += timeouts.Count;
                ProcessTimeoutsIntoEndpointsFound(timeouts, endpoints, filter);

                if (timeouts.Count == 0)
                {
                    findMoreTimeouts = false;
                }
            }
            while (findMoreTimeouts);

            tcs.Cancel();

            try
            {
                await printTask;
            }
            catch (OperationCanceledException)
            {
            }

            return endpoints.Select(x => x.Value).ToList();
        }

        async Task<IReadOnlyList<EndpointInfo>> ListEndpointsUsingIndex(Func<TimeoutData, bool> filter)
        {
            var findMoreTimeouts = true;
            var endpoints = new Dictionary<string, EndpointInfo>();
            var nrOfTimeoutsRetrieved = 0;
            var initialIndexEtag = string.Empty;
            var nrOfTimeouts = 0;
            var tcs = new CancellationTokenSource();
            var printTask = Task.Run(
                async () =>
            {
                while (!tcs.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), tcs.Token);
                    logger.LogInformation($"{nrOfTimeoutsRetrieved} of {nrOfTimeouts} have been scanned.");
                }
            }, CancellationToken.None);

            do
            {
                var result = await ravenAdapter.GetDocumentsByIndex<TimeoutData>(nrOfTimeoutsRetrieved, TimeSpan.FromSeconds(5), (doc, id) => doc.Id = id);
                if (string.IsNullOrEmpty(initialIndexEtag))
                {
                    initialIndexEtag = result.IndexETag;
                    nrOfTimeouts = result.NrOfDocuments;
                }

                if (result.IsStale || result.IndexETag != initialIndexEtag)
                {
                    throw new Exception("We ran into a stale index while trying to list the endpoints. This means that there are still endpoints running that are using the legacy Timeout Manager connected to this storage. Please shut them down and try again.");
                }

                nrOfTimeoutsRetrieved += result.Documents.Count;
                ProcessTimeoutsIntoEndpointsFound(result.Documents, endpoints, filter);

                if (result.Documents.Count == 0)
                {
                    findMoreTimeouts = false;
                }
            }
            while (findMoreTimeouts);

            tcs.Cancel();

            try
            {
                await printTask;
            }
            catch (OperationCanceledException)
            {
            }

            return endpoints.Select(x => x.Value).ToList();
        }

        static void ProcessTimeoutsIntoEndpointsFound(IReadOnlyList<TimeoutData> timeouts, Dictionary<string, EndpointInfo> endpoints, Func<TimeoutData, bool> filter)
        {
            var eligibleTimeouts = timeouts.Where(filter).ToList();

            var endpointsFetched = eligibleTimeouts.GroupBy(
                key => key.OwningTimeoutManager.Replace(RavenConstants.MigrationDonePrefix, "").Replace(RavenConstants.MigrationOngoingPrefix, ""),
                elements => elements,
                (endpointName, destinationTimeouts) =>
                {
                    var endpointTimeouts = eligibleTimeouts.Where(x => x.OwningTimeoutManager.Replace(RavenConstants.MigrationDonePrefix, "").Replace(RavenConstants.MigrationOngoingPrefix, "") == endpointName).ToList();
                    return new EndpointInfo
                    {
                        EndpointName = endpointName,
                        NrOfTimeouts = endpointTimeouts.Count(),
                        ShortestTimeout = endpointTimeouts.Min(x => x.Time),
                        LongestTimeout = endpointTimeouts.Max(x => x.Time),
                        Destinations = eligibleTimeouts.GroupBy(x => x.Destination).Select(g => g.Key).ToList()
                    };
                }).ToList();

            endpointsFetched.ForEach(ep =>
            {
                if (endpoints.ContainsKey(ep.EndpointName))
                {
                    var endpoint = endpoints[ep.EndpointName];
                    endpoint.NrOfTimeouts += ep.NrOfTimeouts;
                    var destinations = endpoint.Destinations.ToList();
                    destinations.AddRange(ep.Destinations);
                    endpoint.Destinations = destinations.Distinct().ToArray();
                    endpoint.ShortestTimeout = ep.ShortestTimeout < endpoint.ShortestTimeout ? ep.ShortestTimeout : endpoint.ShortestTimeout;
                    endpoint.LongestTimeout = ep.LongestTimeout > endpoint.LongestTimeout ? ep.LongestTimeout : endpoint.LongestTimeout;
                }
                else
                {
                    endpoints.Add(ep.EndpointName, ep);
                }
            });
        }

        async Task<int> GuardAgainstTooManyTimeoutsWithoutIndexUsage()
        {
            var nrOfTimeoutsResult = await ravenAdapter.GetDocumentsByIndex<TimeoutData>(0, TimeSpan.FromSeconds(5), (doc, id) => doc.Id = id);
            if (nrOfTimeoutsResult.NrOfDocuments > RavenConstants.GetMaxNrOfTimeoutsWithoutIndexByRavenVersion(ravenVersion) && !useIndex)
            {
                throw new Exception($"We've encountered around {nrOfTimeoutsResult.NrOfDocuments} timeouts to process. Given the amount of timeouts to migrate, please shut down your endpoints before migrating and use the --{ApplicationOptions.ForceUseIndex} option.");
            }

            return nrOfTimeoutsResult.NrOfDocuments;
        }

        public async Task<IToolState> Prepare(DateTimeOffset maxCutoffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            var toolStateDTO = new RavenToolStateDto { RunParameters = runParameters, Endpoint = endpointName, Status = MigrationStatus.Preparing };

            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, toolStateDTO);

            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime, endpointName);

            toolStateDTO.Batches = RavenToolStateDto.ToBatches(batches);
            toolStateDTO.Status = MigrationStatus.StoragePrepared;
            toolStateDTO.StartedAt = DateTimeOffset.UtcNow;
            toolStateDTO.NumberOfBatches = batches.Count();
            toolStateDTO.NumberOfTimeouts = batches.Sum(b => b.NumberOfTimeouts);

            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, toolStateDTO);

            return toolStateDTO.ToToolState(batches);
        }

        public async Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            var batch = await ravenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{batchNumber}", (doc, id) => { });
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(batch.TimeoutIds, (doc, id) => { doc.Id = id; });
            return timeouts;
        }

        public async Task MarkBatchAsCompleted(int batchNumber)
        {
            var batch = await ravenAdapter.GetDocument<RavenBatch>($"{RavenConstants.BatchPrefix}/{batchNumber}", (doc, id) => { });
            batch.State = BatchState.Completed;

            await ravenAdapter.CompleteBatchAndUpdateTimeouts(batch);
        }

        public async Task MarkBatchAsStaged(int batchNumber)
        {
            var batchId = $"{RavenConstants.BatchPrefix}/{batchNumber}";
            var batch = await ravenAdapter.GetDocument<RavenBatch>(batchId, (doc, id) => { });
            batch.State = BatchState.Staged;

            await ravenAdapter.UpdateDocument(batchId, batch);
        }

        public async Task Abort()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });
            var batches = await ravenAdapter.GetDocuments<RavenBatch>(ravenToolState.Batches, (doc, id) => { });

            // Only restoring the timeouts in pending batches to their original state
            var incompleteBatches = batches.Where(bi => bi.State != BatchState.Completed).ToList();
            await CleanupExistingBatchesAndResetTimeouts(batches, incompleteBatches);

            ravenToolState.CompletedAt = DateTimeOffset.UtcNow;
            ravenToolState.Status = MigrationStatus.Aborted;

            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(ravenToolState.Endpoint), ravenToolState);
        }

        internal async Task<List<RavenBatch>> PrepareBatchesAndTimeouts(DateTimeOffset cutoffTime, string endpointName)
        {
            return useIndex
                ? await PrepareBatchesWithIndexUsage(cutoffTime, endpointName)
                : await PrepareBatchesWithoutIndexUsage(cutoffTime, endpointName);
        }

        public async Task Complete()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });

            ravenToolState.Status = MigrationStatus.Completed;
            ravenToolState.CompletedAt = DateTimeOffset.UtcNow;

            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(ravenToolState.Endpoint), ravenToolState);
        }

        public async Task<bool> CheckIfAMigrationIsInProgress()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });
            return ravenToolState != null;
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(IReadOnlyList<RavenBatch> batchesToRemove, IReadOnlyList<RavenBatch> batchesForWhichToResetTimeouts)
        {
            foreach (var batch in batchesToRemove)
            {
                if (batchesForWhichToResetTimeouts.Any(b => b.Number == batch.Number))
                {
                    await ravenAdapter.DeleteBatchAndUpdateTimeouts(batch);
                }
                else
                {
                    await ravenAdapter.DeleteDocument($"{RavenConstants.BatchPrefix}/{batch.Number}");
                }
            }
        }

        async Task<List<RavenBatch>> PrepareBatchesWithoutIndexUsage(DateTimeOffset cutoffTime, string endpointName)
        {
            var batches = new List<RavenBatch>();
            var batchesExisted = false;

            bool ElegibleFilter(TimeoutData td)
            {
                return td.OwningTimeoutManager.ToLower() == endpointName.ToLower() && td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(
                    RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase);
            }
            bool DoesNotExistInBatchesFilter(TimeoutData td)
            {
                return batches.SelectMany(x => x.TimeoutIds).All(t => t != td.Id);
            }

            var findMoreTimeouts = true;
            var iteration = 0;
            var nrOfPages = 1;

            var existingBatches = await ravenAdapter.GetDocuments<RavenBatch>(batch => true, RavenConstants.BatchPrefix, (batch, idSetter) => { });
            if (existingBatches.Any())
            {
                var batch = existingBatches.First();
                if (batch.CutoffDate != cutoffTime || batch.EndpointName.ToLower() != endpointName.ToLower())
                {
                    throw new Exception($"Found remaining batches from previous run, using a different cutoff date or endpoint than current run. Please abort the previous migration with parameters Cutoffdate = {batch.CutoffDate} and EndpointName = {batch.EndpointName}");
                }

                logger.LogInformation($"Found existing batches, resuming prepare for endpoint {endpointName}");
                batches.AddRange(existingBatches);
                batchesExisted = true;
            }

            while (findMoreTimeouts)
            {
                var startFrom = iteration * nrOfPages * RavenConstants.DefaultPagingSize;
                var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, startFrom, (doc, id) => doc.Id = id, nrOfPages);

                var elegibleTimeouts = timeouts.Where(ElegibleFilter).ToList();
                if (batchesExisted)
                {
                    elegibleTimeouts = elegibleTimeouts.Where(DoesNotExistInBatchesFilter).ToList();
                }

                if (elegibleTimeouts.Any())
                {
                    var batch = new RavenBatch(batches.Count + 1, BatchState.Pending, elegibleTimeouts.Count())
                    {
                        TimeoutIds = elegibleTimeouts.Select(t => t.Id).ToArray(),
                        CutoffDate = cutoffTime,
                        EndpointName = endpointName
                    };
                    await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
                    logger.LogInformation($"Batch {batch.Number} was created to handle {elegibleTimeouts.Count} timeouts");
                    batches.Add(batch);
                }

                if (timeouts.Count == 0)
                {
                    findMoreTimeouts = false;
                }

                iteration++;
            }

            return batches;
        }

        async Task<List<RavenBatch>> PrepareBatchesWithIndexUsage(DateTimeOffset cutoffTime, string endpointName)
        {
            var batches = new List<RavenBatch>();
            var batchesExisted = false;
            var timeoutIds = new Dictionary<string, string>();
            var nrOfTimeoutsRetrieved = 0;
            var initialIndexEtag = string.Empty;
            var nrOfTimeouts = 0;

            var tcs = new CancellationTokenSource();
            var printTask = Task.Run(
                async () =>
            {
                while (!tcs.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(5), tcs.Token);
                    logger.LogInformation($"{nrOfTimeoutsRetrieved} of {nrOfTimeouts} have been scanned.");
                }
            }, CancellationToken.None);

            bool DoesNotExistInBatchesFilter(TimeoutData td)
            {
                return batches.SelectMany(x => x.TimeoutIds).All(t => t != td.Id);
            }
            bool ElegibleFilter(TimeoutData td)
            {
                return td.OwningTimeoutManager.ToLower() == endpointName.ToLower() && td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(
                    RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase) && !timeoutIds.ContainsKey(td.Id);
            }

            var findMoreTimeouts = true;

            var existingBatches = await ravenAdapter.GetDocuments<RavenBatch>(batch => true, RavenConstants.BatchPrefix);
            if (existingBatches.Any())
            {
                var batch = existingBatches.First();
                if (batch.CutoffDate != cutoffTime || batch.EndpointName.ToLower() != endpointName.ToLower())
                {
                    throw new Exception($"Found remaining batches from previous run, using a different cutoff date or endpoint than current run. Please abort the previous migration with parameters Cutoffdate = {batch.CutoffDate} and EndpointName = {batch.EndpointName}");
                }

                logger.LogInformation($"Found existing batches, resuming prepare for endpoint {endpointName}");
                batches.AddRange(existingBatches);
                batchesExisted = true;
            }

            while (findMoreTimeouts)
            {
                var startFrom = nrOfTimeoutsRetrieved;
                var timeoutsResult = await ravenAdapter.GetDocumentsByIndex<TimeoutData>(startFrom, TimeSpan.FromSeconds(5), (doc, id) => doc.Id = id);

                if (string.IsNullOrEmpty(initialIndexEtag))
                {
                    initialIndexEtag = timeoutsResult.IndexETag;
                    nrOfTimeouts = timeoutsResult.NrOfDocuments;
                }
                if (timeoutsResult.IsStale || timeoutsResult.IndexETag != initialIndexEtag)
                {
                    throw new Exception("We ran into a stale index while trying to prepare the batches to migrate. This means that there are still endpoints running that are using the legacy Timeout Manager connected to this storage. Please shut them down and try again.");
                }

                nrOfTimeoutsRetrieved += timeoutsResult.Documents.Count;

                var eligibleTimeouts = timeoutsResult.Documents.Where(ElegibleFilter).ToList();
                if (batchesExisted)
                {
                    eligibleTimeouts = eligibleTimeouts.Where(DoesNotExistInBatchesFilter).ToList();
                }

                var timeouts = eligibleTimeouts.Select(x => x.Id);
                foreach (var timeoutId in timeouts)
                {
                    timeoutIds.Add(timeoutId, null);
                }

                if (timeoutsResult.Documents.Count == 0)
                {
                    findMoreTimeouts = false;
                }
            }

            tcs.Cancel();

            try
            {
                await printTask;
            }
            catch (OperationCanceledException)
            {
            }

            var timeoutsToProcess = timeoutIds.Select(x => x.Key).ToList();
            var nrOfBatches = Math.Ceiling(timeoutsToProcess.Count / (decimal)RavenConstants.DefaultPagingSize);
            for (var i = 0; i < nrOfBatches; i++)
            {
                var timeoutsInThisBatch = timeoutsToProcess.Skip(i * RavenConstants.DefaultPagingSize).Take(RavenConstants.DefaultPagingSize).ToList();
                var batch = new RavenBatch(batches.Count + 1, BatchState.Pending, timeoutsInThisBatch.Count())
                {
                    TimeoutIds = timeoutsInThisBatch.ToArray(),
                    CutoffDate = cutoffTime,
                    EndpointName = endpointName
                };
                await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
                logger.LogInformation($"Batch {batch.Number} was created to handle {timeoutsInThisBatch.Count} timeouts");
                batches.Add(batch);
            }

            return batches;
        }

        string GetArchivedToolStateId(string endpointName)
        {
            return $"{RavenConstants.ArchivedToolStateIdPrefix}{endpointName}-{DateTimeOffset.UtcNow:yyyy-MM-dd hh-mm-ss}";
        }

        readonly ILogger logger;
        readonly string timeoutDocumentPrefix;
        readonly ICanTalkToRavenVersion ravenAdapter;
        readonly bool useIndex;
        readonly RavenDbVersion ravenVersion;
    }
}