using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenDBTimeoutStorage : ITimeoutStorage
    {
        private readonly string timeoutDocumentPrefix;
        private readonly ICanTalkToRavenVersion ravenAdapter;

        public RavenDBTimeoutStorage(string serverUrl, string databaseName, string timeoutDocumentPrefix,
            RavenDbVersion ravenVersion)
        {
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

            while (findMoreTimeouts)
            {
                var startFrom = nrOfTimeoutsRetrieved + 1;
                var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, (doc, id) => doc.Id = id, startFrom, 3);
                var elegibleTimeouts = timeouts.Where(filter).ToList();
                nrOfTimeoutsRetrieved += timeouts.Count;

                var endpointsFetched = elegibleTimeouts.GroupBy(
                    key => key.OwningTimeoutManager.Replace(RavenConstants.MigrationDonePrefix, "").Replace(RavenConstants.MigrationOngoingPrefix, ""),
                    elements => elements,
                    (owningTimeoutManager, destinationTimeouts) => new EndpointInfo
                    {
                        EndpointName = owningTimeoutManager,
                        NrOfTimeouts = elegibleTimeouts.Count(),
                        ShortestTimeout = elegibleTimeouts.Min(x => x.Time),
                        LongestTimeout = elegibleTimeouts.Max(x => x.Time),
                        Destinations = elegibleTimeouts.GroupBy(x => x.Destination).Select(g => g.Key).ToList()
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
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(t => batch.TimeoutIds.Contains(t.Id), timeoutDocumentPrefix,
                (doc, id) => doc.Id = id);
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

        internal async Task<List<RavenBatch>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime, string endpointName)
        {
            bool filter(TimeoutData td)
            {
                return td.OwningTimeoutManager == endpointName && td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            var findMoreTimeouts = true;
            var iteration = 0;
            var maxNrOfTimeouts = RavenConstants.MaxNrOfDocumentsToRetrieve;
            var batches = new List<RavenBatch>();

            while (findMoreTimeouts)
            {
                var startFrom = iteration * maxNrOfTimeouts;
                var timeouts = await ravenAdapter.GetPagedDocuments<TimeoutData>(timeoutDocumentPrefix, (doc, id) => doc.Id = id, startFrom, maxNrOfTimeouts);
                var elegibleTimeouts = timeouts.Where(filter).ToList();

                var batch = new RavenBatch(iteration + 1, BatchState.Pending, elegibleTimeouts.Count())
                {
                    TimeoutIds = elegibleTimeouts.Select(t => t.Id).ToArray()
                };
                await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
                batches.Add(batch);

                if (timeouts.Count == 0)
                    findMoreTimeouts = false;
                iteration++;
            }

            return batches;
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