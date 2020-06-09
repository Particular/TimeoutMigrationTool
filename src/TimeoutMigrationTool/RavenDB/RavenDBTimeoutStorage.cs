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

        public async Task<ToolState> GetToolState()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolState>(RavenConstants.ToolStateId, (doc, id) => { });
            if (ravenToolState == null) return null;

            //TODO: replace with an Include
            var batches = await ravenAdapter.GetDocuments<BatchInfo>(ravenToolState.Batches, (doc, id) => { });
            return ravenToolState.ToToolState(batches);
        }

        public async Task<bool> CanPrepareStorage()
        {
            var existingBatches =
                await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, (doc, id) => { });
            if (existingBatches.Any()) return false;

            return true;
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

        public async Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint)
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint), "EndpointInfo is required.");

            var batchesInStorage =
                await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, (doc, id) => { });

            if (batchesInStorage.Any())
                await ravenAdapter.BatchDelete(batchesInStorage.Select(b => $"{RavenConstants.BatchPrefix}/{b.Number}")
                    .ToArray());

            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime, endpoint);
            return batches;
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            var toolState = await GetToolState();
            var prefix = RavenConstants.DefaultTimeoutPrefix;
            if (toolState.RunParameters.ContainsKey(ApplicationOptions.RavenTimeoutPrefix))
                prefix = toolState.RunParameters[ApplicationOptions.RavenTimeoutPrefix];

            var batch = await ravenAdapter.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchNumber}", (doc, id) => { });
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(t => batch.TimeoutIds.Contains(t.Id), prefix,
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

        public async Task StoreToolState(ToolState toolState)
        {
            var ravenToolState = RavenToolState.FromToolState(toolState);
            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, ravenToolState);
        }

        public async Task Abort()
        {
            var toolState = await GetToolState();

            if (toolState.Batches.Any())
            {
                var incompleteBatches = toolState.Batches.Where(bi => bi.State != BatchState.Completed).ToList();
                await CleanupExistingBatchesAndResetTimeouts(toolState.Batches.ToList(), incompleteBatches);
            }
            else
            {
                var batches = await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, (doc, id) => { });
                await CleanupExistingBatchesAndResetTimeouts(batches, batches);
            }

            await RemoveToolState();
        }

        internal async Task<List<BatchInfo>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime, EndpointInfo endpoint)
        {
            if (endpoint == null) throw new ArgumentNullException(nameof(endpoint), "EndpointInfo is required.");

            bool filter(TimeoutData td)
            {
                return td.OwningTimeoutManager == endpoint.EndpointName && td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix,
                    StringComparison.OrdinalIgnoreCase);
            }

            var timeouts =
                await ravenAdapter.GetDocuments<TimeoutData>(filter, timeoutDocumentPrefix, (doc, id) => doc.Id = id);

            var nrOfBatches = Math.Ceiling(timeouts.Count / (decimal)RavenConstants.DefaultPagingSize);
            var batches = new List<BatchInfo>();
            for (var i = 0; i < nrOfBatches; i++)
                batches.Add(new BatchInfo
                {
                    Number = i + 1,
                    State = BatchState.Pending,
                    TimeoutIds = timeouts.Skip(i * RavenConstants.DefaultPagingSize)
                        .Take(RavenConstants.DefaultPagingSize)
                        .Select(t => t.Id).ToArray()
                });

            foreach (var batch in batches) await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);

            return batches;
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(List<BatchInfo> batchesToRemove,
            List<BatchInfo> batchesForWhichToResetTimeouts)
        {
            foreach (var batch in batchesToRemove)
                if (batchesForWhichToResetTimeouts.Any(b => b.Number == batch.Number))
                    await ravenAdapter.DeleteBatchAndUpdateTimeouts(batch);
                else
                    await ravenAdapter.DeleteRecord($"{RavenConstants.BatchPrefix}/{batch.Number}");
        }

        internal async Task RemoveToolState()
        {
            await ravenAdapter.DeleteRecord(RavenConstants.ToolStateId);
        }

        public async Task Complete()
        {
            var toolState = await GetToolState();

            toolState.Status = MigrationStatus.Completed;

            var ravenToolState = RavenToolState.FromToolState(toolState);
            await ravenAdapter.UpdateDocument(RavenConstants.ToolStateId, ravenToolState);
        }
    }
}