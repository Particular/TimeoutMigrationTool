namespace Particular.TimeoutMigrationTool
{
    using System.Threading.Tasks;
    using System;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportTimeoutsCreator = transportTimeoutsCreator;
        }

        public async Task Run()
        {
            await Console.Out.WriteAsync("Creating tool state").ConfigureAwait(false);
            var toolState = await timeoutStorage.GetOrCreateToolState().ConfigureAwait(false);
            await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false);

            if (!toolState.IsStoragePrepared)
            {
                await Console.Out.WriteAsync("Preparing storage").ConfigureAwait(false);
                var batches = await timeoutStorage.Prepare().ConfigureAwait(false);

                toolState.InitBatches(batches);

                await MarkStorageAsPrepared(toolState).ConfigureAwait(false);

                await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false); ;
            }

            while (toolState.HasMoreBatches)
            {
                var batch = toolState.GetCurrentBatch();

                await Console.Out.WriteAsync($"Migrating batch {batch.Number}").ConfigureAwait(false);

                if (batch.State == BatchState.Pending)
                {
                    await Console.Out.WriteAsync($" - reading").ConfigureAwait(false);
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number).ConfigureAwait(false);

                    await Console.Out.WriteAsync($" - staging").ConfigureAwait(false);

                    await transportTimeoutsCreator.StageBatch(timeouts).ConfigureAwait(false);

                    await MarkCurrentBatchAsStaged(toolState).ConfigureAwait(false);

                    await Console.Out.WriteAsync($" - staged").ConfigureAwait(false);
                }

                await Console.Out.WriteAsync($" - completing").ConfigureAwait(false);
                await transportTimeoutsCreator.CompleteBatch(batch.Number).ConfigureAwait(false);
                await CompleteCurrentBatch(toolState).ConfigureAwait(false);

                await Console.Out.WriteLineAsync($" - done").ConfigureAwait(false);
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

        async Task ExecuteWithStatusReport(string description, Func<Task> action)
        {
            await Console.Out.WriteAsync(description).ConfigureAwait(false);

            await action().ConfigureAwait(false);

            await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false); ;
        }

        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}