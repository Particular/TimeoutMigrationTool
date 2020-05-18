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
            var toolState = await timeoutStorage.GetOrCreateToolState().ConfigureAwait(false);

            if (!toolState.IsPrepared)
            {
                var storageInfo = await timeoutStorage.Prepare().ConfigureAwait(false);

                await toolState.MarkAsPrepared(storageInfo).ConfigureAwait(false);
            }

            while (toolState.HasMoreBatches)
            {
                var batch = toolState.CurrentBatch;

                if (batch.State == BatchState.Pending)
                {
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number).ConfigureAwait(false);

                    await transportAdapter.StageBatch(timeouts).ConfigureAwait(false);

                    //Do we need to tell the storage that the batch is staged?

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