using System.Runtime.InteropServices;

namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System;
    using System.Linq;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportTimeoutsCreator = transportTimeoutsCreator;
        }

        public async Task Run(DateTime cutOffTime, IDictionary<string, string> runParameters)
        {
            var toolState = await timeoutStorage.GetToolState().ConfigureAwait(false);

            if (ShouldWeCreateAToolState(toolState))
            {
                toolState = new ToolState(runParameters);
                await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
                await Console.Out.WriteLineAsync("Migration status created and stored.").ConfigureAwait(false);
            }

            switch (toolState.Status)
            {
                case MigrationStatus.NeverRun:
                    var canPrepStorage = await timeoutStorage.CanPrepareStorage().ConfigureAwait(false);
                    if (!canPrepStorage)
                        await Console.Error.WriteLineAsync(
                            "We found some leftovers of a previous run. Please use the abort option to clean up the state and then rerun.").ConfigureAwait(false);
                    break;
                case MigrationStatus.Completed:
                    await Console.Out.WriteAsync("Preparing storage").ConfigureAwait(false);
                    var batches = await timeoutStorage.Prepare(cutOffTime).ConfigureAwait(false);

                    if (!batches.Any())
                    {
                        await Console.Out.WriteLineAsync(
                                $"No data was found to migrate. If you think this is not possible, verify your parameters and try again.")
                            .ConfigureAwait(false);
                    }

                    toolState.InitBatches(batches);
                    await MarkStorageAsPrepared(toolState).ConfigureAwait(false);
                    await Console.Out.WriteLineAsync(" - done").ConfigureAwait(false); ;
                    break;
                case MigrationStatus.StoragePrepared when RunParametersAreDifferent(toolState.RunParameters, runParameters):
                    await Console.Out
                        .WriteLineAsync(
                            $"In progress migration parameters didn't match, either rerun with the --abort option or adjust the parameters to match to continue the current migration:")
                        .ConfigureAwait(false);
                    foreach (var setting in toolState.RunParameters)
                    {
                        await Console.Out.WriteLineAsync($"\t'{setting.Key}': '{setting.Value}'.")
                            .ConfigureAwait(false);
                    }
                    return;
                case MigrationStatus.StoragePrepared:
                    await Console.Out.WriteAsync("Resuming where we left off...").ConfigureAwait(false);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
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

            toolState.Status = MigrationStatus.Completed;

            await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"Migration completed successfully").ConfigureAwait(false);
        }

        private bool ShouldWeCreateAToolState(ToolState toolState)
        {
            if (toolState == null) return true;
            return toolState.Status == MigrationStatus.Completed;
        }

        bool RunParametersAreDifferent(IDictionary<string, string> inProgressRunParameters, IDictionary<string, string> currentRunParameters)
        {
            if (inProgressRunParameters.Count != currentRunParameters.Count)
            {
                return true;
            }

            foreach (var parameterKey in inProgressRunParameters.Keys)
            {
                if (!currentRunParameters.ContainsKey(parameterKey))
                {
                    return true;
                }

                if (!string.Equals(inProgressRunParameters[parameterKey], currentRunParameters[parameterKey], StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
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