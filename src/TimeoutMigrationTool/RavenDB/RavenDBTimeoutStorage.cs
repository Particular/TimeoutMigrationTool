using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenDBTimeoutStorage : ITimeoutStorage
    {
        readonly string timeoutDocumentPrefix;
        ICanTalkToRavenVersion ravenAdapter;

        public RavenDBTimeoutStorage(string serverUrl, string databaseName, string timeoutDocumentPrefix, RavenDbVersion ravenVersion)
        {
            this.timeoutDocumentPrefix = timeoutDocumentPrefix;
            ravenAdapter = RavenDataReaderFactory.Resolve(serverUrl, databaseName, ravenVersion);
        }

        public async Task<ToolState> GetToolState()
        {
            var ravenToolState = await ravenAdapter.GetDocument<RavenToolState>(RavenConstants.ToolStateId);
            if (ravenToolState == null)
            {
                return null;
            }

            //TODO: replace with an Include
            var batches = await ravenAdapter.GetDocuments<BatchInfo>(ravenToolState.Batches);
            return ravenToolState.ToToolState(batches);
        }

        public async Task<bool> CanPrepareStorage()
        {
            var existingBatches = await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, CancellationToken.None);
            if (existingBatches.Any())
            {
                return false;
            }

            return true;
        }

        public Task<List<EndpointInfo>> ListEndpoints(DateTime cutoffTime)
        {
            throw new NotImplementedException();
        }

        public async Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint)
        {
            var batchesInStorage = await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, CancellationToken.None);
            if (batchesInStorage.Any())
            {
                await ravenAdapter.BatchDelete(batchesInStorage.Select(b => $"{RavenConstants.BatchPrefix}/{b.Number}").ToArray());
            }

            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime);
            return batches;
        }

        internal async Task<List<BatchInfo>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime)
        {
            bool filter(TimeoutData td) => td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationPrefix, StringComparison.OrdinalIgnoreCase);
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(filter, timeoutDocumentPrefix, CancellationToken.None);

            var nrOfBatches = Math.Ceiling(timeouts.Count / (decimal) RavenConstants.DefaultPagingSize);
            var batches = new List<BatchInfo>();
            for (int i = 0; i < nrOfBatches; i++)
            {
                batches.Add(new BatchInfo
                {
                    Number = i + 1,
                    State = BatchState.Pending,
                    TimeoutIds = timeouts.Skip(i * RavenConstants.DefaultPagingSize).Take(RavenConstants.DefaultPagingSize)
                        .Select(t => t.Id).ToArray()
                });
            }

            foreach (var batch in batches)
            {
                await ravenAdapter.CreateBatchAndUpdateTimeouts(batch);
            }

            return batches;
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
                    await ravenAdapter.DeleteRecord($"{RavenConstants.BatchPrefix}/{batch.Number}");
                }
            }
        }

        internal async Task RemoveToolState()
        {
            await ravenAdapter.DeleteRecord(RavenConstants.ToolStateId);
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            var toolState = await GetToolState();
            string prefix = RavenConstants.DefaultTimeoutPrefix;
            if (toolState.RunParameters.ContainsKey(ApplicationOptions.RavenTimeoutPrefix))
            {
                prefix = toolState.RunParameters[ApplicationOptions.RavenTimeoutPrefix];
            }

            var batch = await ravenAdapter.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchNumber}");
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(t => batch.TimeoutIds.Contains(t.Id), prefix, CancellationToken.None);
            return timeouts;
        }

        public async Task CompleteBatch(int batchNumber)
        {
            var batch = await ravenAdapter.GetDocument<BatchInfo>($"{RavenConstants.BatchPrefix}/{batchNumber}");
            batch.State = BatchState.Completed;

            await ravenAdapter.UpdateRecord($"{RavenConstants.BatchPrefix}/{batchNumber}", batch);
        }

        public async Task StoreToolState(ToolState toolState)
        {
            var ravenToolState = RavenToolState.FromToolState(toolState);
            await ravenAdapter.UpdateRecord(RavenConstants.ToolStateId, ravenToolState);
        }

        public async Task Abort(ToolState toolState)
        {
            if (toolState == null)
            {
                throw new ArgumentNullException(nameof(toolState), "Can't abort without a tool state");
            }

            if (toolState.Batches.Any())
            {
                var incompleteBatches = toolState.Batches.Where(bi => bi.State != BatchState.Completed).ToList();
                await CleanupExistingBatchesAndResetTimeouts(toolState.Batches.ToList(), incompleteBatches);
            }
            else
            {
                var batches = await ravenAdapter.GetDocuments<BatchInfo>(x => true, RavenConstants.BatchPrefix, CancellationToken.None);
                await CleanupExistingBatchesAndResetTimeouts(batches, batches);
            }

            await RemoveToolState();
        }
    }
}