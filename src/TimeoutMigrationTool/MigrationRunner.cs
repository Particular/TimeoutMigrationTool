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

        public async Task Run(DateTime cutOffTime, bool forceMigration, IDictionary<string, string> runParameters)
        {
            if (forceMigration)
            {
                await Console.Out.WriteAsync("Migration will be forced.").ConfigureAwait(false);
                await timeoutStorage.Reset().ConfigureAwait(false);
                await Console.Out.WriteLineAsync("Timeouts storage migration status reset completed.").ConfigureAwait(false);
            }

            await Console.Out.WriteAsync("Checking for existing tool state").ConfigureAwait(false);
            var toolState = await timeoutStorage.GetToolState().ConfigureAwait(false);

            if (toolState != null)
            {
                await Console.Out.WriteLineAsync("Found existing tool state.").ConfigureAwait(false);
                await Console.Out.WriteLineAsync("Checking migration status.").ConfigureAwait(false);

                if (toolState.Status == MigrationStatus.Completed)
                {
                    await Console.Out.WriteLineAsync("Migration already completed. To rerun a migration on this timeout storage run the tool using --forceMigration to reset the migration state.").ConfigureAwait(false);
                    return;
                }

                await Console.Out.WriteLineAsync("Migration is not complete.").ConfigureAwait(false);
                await Console.Out.WriteLineAsync("Checking run parameters.").ConfigureAwait(false);

                if (!EnsureRunParametersMatch(toolState.RunParameters, runParameters))
                {
                    await Console.Out.WriteLineAsync($"An in progress migration (State: {toolState.Status}) was found in the timeouts storage. The in progress migration settings don't match the current migration attempt settings. Please rerun the tool either by using the same settings, or using the --forceMigration option to reset the migration state.").ConfigureAwait(false);
                    await Console.Out.WriteLineAsync($"In progress migration settings:").ConfigureAwait(false);
                    foreach (var setting in toolState.RunParameters)
                    {
                        await Console.Out.WriteLineAsync($"\t'{setting.Key}': '{setting.Value}'.").ConfigureAwait(false);
                    }

                    return;
                }

                await Console.Out.WriteLineAsync("Run parameters check completed successfully.").ConfigureAwait(false);
            }
            else
            {
                toolState = new ToolState(runParameters);
                await timeoutStorage.StoreToolState(toolState).ConfigureAwait(false);
                await Console.Out.WriteLineAsync("Migration status created and stored.").ConfigureAwait(false);
            }

            if (toolState.Status == MigrationStatus.NeverRun)
            {
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

        bool EnsureRunParametersMatch(IDictionary<string, string> inProgressRunParameters, IDictionary<string, string> currentRunParameters)
        {
            if (inProgressRunParameters.Count != currentRunParameters.Count)
            {
                return false;
            }

            foreach (var parameterKey in inProgressRunParameters.Keys)
            {
                if (!currentRunParameters.ContainsKey(parameterKey))
                {
                    return false;
                }

                if (!string.Equals(inProgressRunParameters[parameterKey], currentRunParameters[parameterKey], StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }

            return true;
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