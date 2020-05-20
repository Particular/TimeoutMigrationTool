using System;
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

        public async Task Run(IDictionary<string, string> runParameters)
        {
            var forceMigration = runParameters.ContainsKey(ApplicationOptions.ForceMigration);
            //if (forceMigration) 
            //{
            //    TODO: is this approach any better?
            //    await timeoutStorage.ResetState().ConfigureAwait(false);
            //}

            var toolState = await timeoutStorage.GetToolState().ConfigureAwait(false);
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

                IEnumerable<BatchInfo> batches = await timeoutStorage.Prepare(cutOffTime).ConfigureAwait(false);
                toolState.InitBatches(batches);
                await MarkStorageAsPrepared(toolState).ConfigureAwait(false);
            }

            while (toolState.HasMoreBatches())
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