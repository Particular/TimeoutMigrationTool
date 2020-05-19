namespace Particular.TimeoutMigrationTool
{
    using System.Threading.Tasks;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ITransportAdapter transportAdapter)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportAdapter = transportAdapter;
        }

        public async Task Run()
        {
            //TODO: Not happy with this, I think we just need to get some DTO instead?
            var toolState = await timeoutStorage.GetOrCreateToolState().ConfigureAwait(false);

            if (!toolState.IsStoragePrepared)
            {
                var storageInfo = await timeoutStorage.Prepare().ConfigureAwait(false);

                await toolState.MarkStorageAsPrepared(storageInfo).ConfigureAwait(false);
            }

            while (toolState.HasMoreBatches)
            {
                var batch = toolState.CurrentBatch;

                if (batch.State == BatchState.Pending)
                {
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number).ConfigureAwait(false);

                    await transportAdapter.StageBatch(timeouts).ConfigureAwait(false);

                    //TODO: do we need to tell the storage that the batch is staged?

                    await toolState.MarkCurrentBatchAsStaged().ConfigureAwait(false);
                }

                await transportAdapter.CompleteBatch().ConfigureAwait(false);

                await toolState.CompleteCurrentBatch().ConfigureAwait(false);
            }
        }

        readonly ITimeoutStorage timeoutStorage;
        readonly ITransportAdapter transportAdapter;
    }
}