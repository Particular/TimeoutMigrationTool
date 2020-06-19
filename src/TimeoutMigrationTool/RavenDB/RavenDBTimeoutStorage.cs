﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    using Microsoft.Extensions.Logging;

    public class RavenDBTimeoutStorage : ITimeoutStorage
    {
        readonly ILogger logger;
        private readonly string timeoutDocumentPrefix;
        private readonly ICanTalkToRavenVersion ravenAdapter;

        public RavenDBTimeoutStorage(ILogger logger, string serverUrl, string databaseName, string timeoutDocumentPrefix,
            RavenDbVersion ravenVersion)
        {
            this.logger = logger;
            this.timeoutDocumentPrefix = timeoutDocumentPrefix;
            ravenAdapter = RavenDataReaderFactory.Resolve(serverUrl, databaseName, ravenVersion);
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });
            if (ravenToolState == null) return null;

            //TODO: replace with an Include
            var batches = await ravenAdapter.GetDocuments<RavenBatch>(ravenToolState.Batches, (doc, id) => { });
            return ravenToolState.ToToolState(batches);
        }

        public async Task<List<EndpointInfo>> ListEndpoints(DateTime cutoffTime)
        {
            bool filter(TimeoutData td)
            {
                return td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            var findMoreTimeouts = true;
            var endpoints = new List<EndpointInfo>();
            var nrOfTimeoutsRetrieved = 0;
            var nrOfPages = 3;

            while (findMoreTimeouts)
            {
                var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, (doc, id) => doc.Id = id, nrOfTimeoutsRetrieved, nrOfPages);
                var eligibleTimeouts = timeouts.Where(filter).ToList();
                nrOfTimeoutsRetrieved += timeouts.Count;

                var endpointsFetched = eligibleTimeouts.GroupBy(
                    key => key.OwningTimeoutManager.Replace(RavenConstants.MigrationDonePrefix, "").Replace(RavenConstants.MigrationOngoingPrefix, ""),
                    elements => elements,
                    (owningTimeoutManager, destinationTimeouts) => new EndpointInfo
                    {
                        EndpointName = owningTimeoutManager,
                        NrOfTimeouts = eligibleTimeouts.Count(),
                        ShortestTimeout = eligibleTimeouts.Min(x => x.Time),
                        LongestTimeout = eligibleTimeouts.Max(x => x.Time),
                        Destinations = eligibleTimeouts.GroupBy(x => x.Destination).Select(g => g.Key).ToList()
                    }).ToList();

                endpointsFetched.ForEach(ep =>
                {
                    if (endpoints.Any(x => x.EndpointName == ep.EndpointName))
                    {
                        var endpoint = endpoints.Single(x => x.EndpointName == ep.EndpointName);
                        endpoint.NrOfTimeouts += ep.NrOfTimeouts;
                        var destinations = endpoint.Destinations.ToList();
                        destinations.AddRange(ep.Destinations);
                        endpoint.Destinations = destinations.Distinct().ToArray();
                        endpoint.ShortestTimeout = ep.ShortestTimeout < endpoint.ShortestTimeout ? ep.ShortestTimeout : endpoint.ShortestTimeout;
                        endpoint.LongestTimeout = ep.LongestTimeout > endpoint.LongestTimeout ? ep.LongestTimeout : endpoint.LongestTimeout;
                    }
                    else
                    {
                        endpoints.Add(ep);
                    }
                });

                if (timeouts.Count == 0)
                    findMoreTimeouts = false;
            }

            return endpoints;
        }

        public async Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime, endpointName);
            var toolState = new RavenToolState(runParameters, endpointName, batches);
            await StoreToolState(toolState);
            return toolState;
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
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
            if (ravenToolState == null)
            {
                throw new ArgumentNullException(nameof(ravenToolState), "Can't abort without a tool state");
            }

            var batches = await ravenAdapter.GetDocuments<RavenBatch>(ravenToolState.Batches, (doc, id) => { });
            var toolState = ravenToolState.ToToolState(batches);

            // Only restoring the timeouts in pending batches to their original state
            var incompleteBatches = batches.Where(bi => bi.State != BatchState.Completed).ToList();
            await CleanupExistingBatchesAndResetTimeouts(batches, incompleteBatches);
            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(ravenToolState.Endpoint), toolState);
        }

        internal async Task<List<RavenBatch>> PrepareBatchesAndTimeouts(DateTime cutoffTime, string endpointName)
        {
            var now = DateTime.UtcNow;
            while (DateTime.UtcNow.Subtract(now) < TimeSpan.FromSeconds(15))
            {
                var succeeded = await ravenAdapter.HideTimeouts(cutoffTime);
                if (succeeded)
                    break;
            }
            //
            // var batches = new List<RavenBatch>();
            // var batchesExisted = false;
            //
            // bool elegibleFilter(TimeoutData td)
            // {
            //     return td.OwningTimeoutManager.ToLower() == endpointName.ToLower() && td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix,
            //         StringComparison.OrdinalIgnoreCase);
            // }
            // bool doesNotExistInBatchesFilter(TimeoutData td)
            // {
            //     return batches.SelectMany(x => x.TimeoutIds).All(t => t != td.Id);
            // }
            //
            // var findMoreTimeouts = true;
            // var iteration = 0;
            // var nrOfPages = 1;
            //
            // var existingBatches = await ravenAdapter.GetDocuments<RavenBatch>(batch => { return true; }, RavenConstants.BatchPrefix, (batch, idSetter) => { });
            // if (existingBatches.Any())
            // {
            //     var batch = existingBatches.First();
            //     if (batch.CutoffDate != cutoffTime || batch.EndpointName.ToLower() != endpointName.ToLower())
            //     {
            //         throw new Exception($"Found remaining batches from previous run, using a different cutoff date or endpoint than current run. Please abort the previous migration with parameters Cutoffdate = {batch.CutoffDate} and EndpointName = {batch.EndpointName}");
            //     }
            //
            //     logger.LogInformation($"Found existing batches, resuming prepare for endpoint {endpointName}");
            //     batches.AddRange(existingBatches);
            //     batchesExisted = true;
            // }
            //
            // while (findMoreTimeouts)
            // {
            //     var startFrom = iteration * (nrOfPages * RavenConstants.DefaultPagingSize);
            //     var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, (doc, id) => doc.Id = id, startFrom, nrOfPages);
            //
            //     var elegibleTimeouts = timeouts.Where(elegibleFilter).ToList();
            //     if (batchesExisted)
            //     {
            //         elegibleTimeouts = elegibleTimeouts.Where(doesNotExistInBatchesFilter).ToList();
            //     }
            //
            //     if (elegibleTimeouts.Any())
            //     {
            //         var batch = new RavenBatch(batches.Count + 1, BatchState.Pending, elegibleTimeouts.Count())
            //         {
            //             TimeoutIds = elegibleTimeouts.Select(t => t.Id).ToArray(),
            //             CutoffDate = cutoffTime,
            //             EndpointName = endpointName
            //         };
            //         await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
            //         logger.LogInformation($"Batch {batch.Number} was created to handle {elegibleTimeouts.Count} timeouts");
            //         batches.Add(batch);
            //     }
            //
            //     if (timeouts.Count == 0)
            //         findMoreTimeouts = false;
            //     iteration++;
            // }
            //
            // return batches;
            return new List<RavenBatch>();
        }

        public async Task Complete()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolStateDto>(RavenConstants.ToolStateId, (doc, id) => { });
            var batches = await ravenAdapter.GetDocuments<RavenBatch>(ravenToolState.Batches, (doc, id) => { });
            ravenToolState.Status = MigrationStatus.Completed;

            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(ravenToolState.Endpoint), ravenToolState.ToToolState(batches));
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(List<RavenBatch> batchesToRemove, List<RavenBatch> batchesForWhichToResetTimeouts)
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

        async Task StoreToolState(IToolState toolState)
        {
            var ravenToolState = RavenToolStateDto.FromToolState((RavenToolState)toolState);
            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, ravenToolState);
        }

        string GetArchivedToolStateId(string endpointName)
        {
            return $"{RavenConstants.ArchivedToolStateIdPrefix}{endpointName}-{DateTime.Now:yyyy-MM-dd hh-mm-ss}";
        }
    }
}