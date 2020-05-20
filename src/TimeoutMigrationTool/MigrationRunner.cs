using System;
using System.Collections.Generic;

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

        public async Task Run(IDictionary<string, string> runParameters)
        {
            var forceMigration = runParameters.ContainsKey(ApplicationOptions.ForceMigration);
            if (forceMigration) 
            {
                await Console.Out.WriteAsync("Migration will be forced.").ConfigureAwait(false);
                //TODO: is this approach any better?
                await timeoutStorage.ResetState().ConfigureAwait(false);
            }

            await Console.Out.WriteAsync("Checking for existing tool state").ConfigureAwait(false);
            var toolState = await timeoutStorage.GetToolState().ConfigureAwait(false);
            await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false);  

            if (toolState == null || toolState.Status == MigrationStatus.Completed || forceMigration)
            {
                toolState = new ToolState(runParameters);
                await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
            }
            else 
            {
                //TODO: check if run parameters are all the same, then we're safe to continue
            }


            if (toolState.Status == MigrationStatus.NeverRun)
            {
                DateTime cutOffTime = DateTime.Now.AddDays(-1);
                if (runParameters.TryGetValue(ApplicationOptions.CutoffTime, out var cutOffTimeValue)) 
                {
                    if (!DateTime.TryParse(cutOffTimeValue, out cutOffTime)) 
                    {
                        throw new ArgumentException($"{ApplicationOptions.CutoffTime} is not a valid System.DateTime value.");
                    }
                }

                await Console.Out.WriteAsync("Preparing storage").ConfigureAwait(false);
                var batches = await timeoutStorage.Prepare(cutOffTime).ConfigureAwait(false);
              
                toolState.InitBatches(batches);

                await MarkStorageAsPrepared(toolState).ConfigureAwait(false);
                await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false); ;
            }

            while (toolState.HasMoreBatches())
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
            toolState.Status = MigrationStatus.StoragePrepared;
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