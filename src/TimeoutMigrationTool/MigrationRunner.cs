using System.Collections.Generic;

namespace Particular.TimeoutMigrationTool
{
    using System.Threading.Tasks;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportTimeoutsCreator = transportTimeoutsCreator;
        }

        public async Task Run()
        {
            var toolState = await timeoutStorage.GetOrCreateToolState().ConfigureAwait(false);

            if (!toolState.IsStoragePrepared)
            {
                IEnumerable<BatchInfo> batches = await timeoutStorage.Prepare().ConfigureAwait(false);
                toolState.InitBatches(batches);
                await MarkStorageAsPrepared(toolState).ConfigureAwait(false);
            }

            while (toolState.HasMoreBatches)
            {
                var batch = toolState.GetCurrentBatch();
                if (batch.State == BatchState.Pending)
                {
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number).ConfigureAwait(false);

                    await transportTimeoutsCreator.StageBatch(timeouts).ConfigureAwait(false);
                    await MarkCurrentBatchAsStaged(toolState).ConfigureAwait(false);
                }

                await transportTimeoutsCreator.CompleteBatch(batch.Number).ConfigureAwait(false);
                await CompleteCurrentBatch(toolState).ConfigureAwait(false);
            }
        }

        async Task MarkStorageAsPrepared(ToolState toolState)
        {
            toolState.IsStoragePrepared = true;
            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        async Task MarkCurrentBatchAsStaged(ToolState toolState)
        {
            toolState.GetCurrentBatch().State = BatchState.Staged;
            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        async Task CompleteCurrentBatch(ToolState toolState)
        {
            toolState.GetCurrentBatch().State = BatchState.Completed;
            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}