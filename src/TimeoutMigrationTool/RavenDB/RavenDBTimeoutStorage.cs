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
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolState>(RavenConstants.ToolStateId, (doc, id) => { });
            if (ravenToolState == null) return null;

            //TODO: replace with an Include
            var batches = await ravenAdapter.GetDocuments<BatchInfo>(ravenToolState.Batches, (doc, id) => { });
            return ravenToolState.ToToolState(batches);
        }

        public async Task<List<EndpointInfo>> ListEndpoints(DateTime cutoffTime)
        {
            bool filter(TimeoutData td)
            {
                return td.Time >= cutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationDonePrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(filter,
                timeoutDocumentPrefix,
                (doc, id) => doc.Id = id);

            var endpoints = timeouts.GroupBy(
                key => key.OwningTimeoutManager.Replace(RavenConstants.MigrationOngoingPrefix, ""),
                elements => elements,
                (owningTimeoutManager, destinationTimeouts) => new EndpointInfo
                {
                    EndpointName = owningTimeoutManager,
                    NrOfTimeouts = timeouts.Count(),
                    ShortestTimeout = timeouts.Min(x => x.Time),
                    LongestTimeout = timeouts.Max(x => x.Time),
                    Destinations = timeouts.GroupBy(x => x.Destination).Select(g => g.Key).ToList()
                }).ToList();

            return endpoints;
        }

        public async Task<IToolState> Prepare(DateTime maxCutoffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime, endpointName);
            var toolState = new ToolState(runParameters, endpointName, batches);
            await StoreToolState(toolState);
            return toolState;
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            var batch = await ravenAdapter.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchNumber}", (doc, id) => { });
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(t => batch.TimeoutIds.Contains(t.Id), timeoutDocumentPrefix,
                (doc, id) => doc.Id = id);
            return timeouts;
        }

        public async Task MarkBatchAsCompleted(int batchNumber)
        {
            var batch = await ravenAdapter.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchNumber}", (doc, id) => { });
            batch.State = BatchState.Completed;

            await ravenAdapter.CompleteBatchAndUpdateTimeouts(batch);
        }

        public async Task MarkBatchAsStaged(int batchNumber)
        {
            var batchId = $"{RavenConstants.BatchPrefix}/{batchNumber}";
            var batch = await ravenAdapter.GetDocument<BatchInfo>(batchId, (doc, id) => { });
            batch.State = BatchState.Staged;

            await ravenAdapter.UpdateDocument(batchId, batch);
        }

        public async Task Abort()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolState>(RavenConstants.ToolStateId, (doc, id) => { });

            if (ravenToolState == null)
            {
                throw new ArgumentNullException(nameof(ravenToolState), "Can't abort without a tool state");
            }

            var batches = await ravenAdapter.GetDocuments<BatchInfo>(ravenToolState.Batches, (doc, id) => { });

            // Only restoring the timeouts in pending batches to their original state
            var incompleteBatches = batches.Where(bi => bi.State != BatchState.Completed).ToList();
            await CleanupExistingBatchesAndResetTimeouts(batches, incompleteBatches);
            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(ravenToolState.Endpoint), ravenToolState);
        }
        internal async Task<List<BatchInfo>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime, string endpointName)
        {
            bool filter(TimeoutData td)
            {
                return td.OwningTimeoutManager == endpointName && td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            var timeouts =
                await ravenAdapter.GetDocuments<TimeoutData>(filter, timeoutDocumentPrefix, (doc, id) => doc.Id = id);

            var nrOfBatches = Math.Ceiling(timeouts.Count / (decimal)RavenConstants.DefaultPagingSize);
            var batches = new List<BatchInfo>();
            for (var i = 0; i < nrOfBatches; i++)
            {
                batches.Add(new BatchInfo
                {
                    Number = i + 1,
                    State = BatchState.Pending,
                    TimeoutIds = timeouts.Skip(i * RavenConstants.DefaultPagingSize)
                        .Take(RavenConstants.DefaultPagingSize)
                        .Select(t => t.Id).ToArray()
                });
            }

            foreach (var batch in batches)
            {
                await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
            }

            return batches;
        }

        public async Task Complete()
        {
            var toolState = await TryLoadOngoingMigration();

            var ravenToolState = RavenToolState.FromToolState((ToolState)toolState);

            ravenToolState.Status = MigrationStatus.Completed;
            await ravenAdapter.ArchiveDocument(GetArchivedToolStateId(toolState.EndpointName), ravenToolState);
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(List<BatchInfo> batchesToRemove, List<BatchInfo> batchesForWhichToResetTimeouts)
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
            var ravenToolState = RavenToolState.FromToolState((ToolState)toolState);
            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, ravenToolState);
        }

        string GetArchivedToolStateId(string endpointName)
        {
            return $"{RavenConstants.ArchivedToolStateIdPrefix}{endpointName}-{DateTime.Now:yyyy-MM-dd hh-mm-ss}";
        }
    }
}