namespace Particular.TimeoutMigrationTool
{
    using System.Threading.Tasks;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportAdapter)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportAdapter = transportAdapter;
        }

        public async Task Run()
        {
            var toolState = await timeoutStorage.GetToolState().ConfigureAwait(false);

            if (!toolState.IsStoragePrepared)
            {
                var storageInfo = await timeoutStorage.Prepare().ConfigureAwait(false);

                await MarkStorageAsPrepared(toolState, storageInfo).ConfigureAwait(false);
            }

            while (toolState.HasMoreBatches)
            {
                var batch = toolState.CurrentBatch;

                if (batch.State == BatchState.Pending)
                {
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number).ConfigureAwait(false);

                    await transportAdapter.StageBatch(timeouts).ConfigureAwait(false);

                    await MarkCurrentBatchAsStaged(toolState).ConfigureAwait(false);
                }

                await transportAdapter.CompleteBatch(batch.Number).ConfigureAwait(false);

                await timeoutStorage.CompleteBatch(batch.Number).ConfigureAwait(false);

                await CompleteCurrentBatch(toolState).ConfigureAwait(false);
            }
        }

        async Task MarkStorageAsPrepared(ToolState toolState, StorageInfo storageInfo)
        {
            toolState.StorageInfo = storageInfo;

            toolState.CurrentBatch = new BatchInfo
            {
                Number = 0,
                State = BatchState.Pending
            };

            toolState.IsStoragePrepared = true;

            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        async Task MarkCurrentBatchAsStaged(ToolState toolState)
        {
            toolState.CurrentBatch.State = BatchState.Staged;

            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        async Task CompleteCurrentBatch(ToolState toolState)
        {
            var newBatch = toolState.CurrentBatch.Number++;
            toolState.CurrentBatch = new BatchInfo
            {
                Number = newBatch,
                State = BatchState.Pending
            };

            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
        }

        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportAdapter;
    }
}