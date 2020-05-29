namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System;
    using System.Linq;
    using System.Text;

    public class MigrationRunner
    {
        public MigrationRunner(ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.timeoutStorage = timeoutStorage;
            this.transportTimeoutsCreator = transportTimeoutsCreator;
        }

        public async Task Run(DateTime cutOffTime, EndpointFilter endpointFilter, IDictionary<string, string> runParameters)
        {
            var allEndpoints = await timeoutStorage.ListEndpoints(cutOffTime);

            var endpointsToMigrate = allEndpoints.Where(e => e.NrOfTimeouts > 0 && endpointFilter.ShouldInclude(e.EndpointName))
                .ToList();

            var problematicEndpoints = new List<(EndpointInfo Endpoint, List<string> Problems)>();
            foreach (var endpoint in endpointsToMigrate)
            {
                await Console.Out.WriteLineAsync($"Verifying that {endpoint.EndpointName} has native delay infrastructure in place");
                var migrationCheckResult = await transportTimeoutsCreator.AbleToMigrate(endpoint);

                if (!migrationCheckResult.CanMigrate)
                {
                    problematicEndpoints.Add((endpoint, migrationCheckResult.Problems));
                }
            }

            if (problematicEndpoints.Any())
            {
                var listOfEndpoints = string.Join(";", problematicEndpoints.Select(e => e.Endpoint.EndpointName));
                var sb = new StringBuilder();

                sb.AppendLine("Migration aborted:");
                foreach (var problematicEndpoint in problematicEndpoints)
                {
                    sb.AppendLine($"{problematicEndpoint.Endpoint.EndpointName}:");

                    foreach (var problem in problematicEndpoint.Problems)
                    {
                        sb.AppendLine($"\t - {problem}");
                    }
                }

                throw new Exception(sb.ToString());
            }

            foreach (var enpointToMigrate in endpointsToMigrate)
            {
                await Console.Out.WriteLineAsync($"Starting migration for {enpointToMigrate.EndpointName}, {enpointToMigrate.NrOfTimeouts}");
                await Run(cutOffTime, enpointToMigrate, runParameters);
            }
        }

        async Task Run(DateTime cutOffTime, EndpointInfo endpointInfo, IDictionary<string, string> runParameters)
        {
            var toolState = await timeoutStorage.GetToolState();

            if (ShouldCreateFreshToolState(toolState))
            {
                toolState = new ToolState(runParameters, endpointInfo);
                await timeoutStorage.StoreToolState(toolState);
                await Console.Out.WriteLineAsync("Migration status created and stored.");
            }

            switch (toolState.Status)
            {
                case MigrationStatus.NeverRun:
                    var canPrepStorage = await timeoutStorage.CanPrepareStorage();
                    if (!canPrepStorage)
                    {
                        await Console.Error.WriteLineAsync("We found some leftovers of a previous run. Please use the abort option to clean up the state and then rerun.");
                    }

                    await Prepare(cutOffTime, toolState, endpointInfo);

                    break;
                case MigrationStatus.Completed:

                    await Prepare(cutOffTime, toolState, endpointInfo);

                    break;
                case MigrationStatus.StoragePrepared when RunParametersAreDifferent(endpointInfo, runParameters, toolState):
                    await Console.Out.WriteLineAsync($"In progress migration parameters didn't match, either rerun with the --abort option or adjust the parameters to match to continue the current migration:");

                    await Console.Out.WriteLineAsync($"\t'--endpoint': '{endpointInfo.EndpointName}'.");

                    foreach (var setting in toolState.RunParameters)
                    {
                        await Console.Out.WriteLineAsync($"\t'{setting.Key}': '{setting.Value}'.");
                    }

                    return;
                case MigrationStatus.StoragePrepared:
                    await Console.Out.WriteLineAsync("Resuming in progress migration");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }


            while (toolState.HasMoreBatches())
            {
                var batch = toolState.GetCurrentBatch();
                await Console.Out.WriteAsync($"Migrating batch {batch.Number}");

                if (batch.State == BatchState.Pending)
                {
                    await Console.Out.WriteAsync($" - reading");
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number);

                    await Console.Out.WriteAsync($" - staging");
                    await transportTimeoutsCreator.StageBatch(timeouts);

                    await MarkCurrentBatchAsStaged(toolState);
                    await Console.Out.WriteAsync($" - staged");
                }

                await Console.Out.WriteAsync($" - completing");
                await transportTimeoutsCreator.CompleteBatch(batch.Number);
                await CompleteCurrentBatch(toolState);

                await Console.Out.WriteLineAsync($" - done");
            }

            toolState.Status = MigrationStatus.Completed;

            await timeoutStorage.StoreToolState(toolState);
            await Console.Out.WriteLineAsync($"Migration completed successfully");
        }

        async Task Prepare(DateTime cutOffTime, ToolState toolState, EndpointInfo endpoint)
        {
            await Console.Out.WriteAsync("Preparing storage");
            var batches = await timeoutStorage.Prepare(cutOffTime, endpoint);

            if (!batches.Any())
            {
                await Console.Out.WriteLineAsync("No data was found to migrate. If you think this is not possible, verify your parameters and try again.");
            }

            toolState.InitBatches(batches);
            await MarkStorageAsPrepared(toolState);
            await Console.Out.WriteLineAsync(" - done");
        }

        bool ShouldCreateFreshToolState(ToolState toolState)
        {
            if (toolState == null) return true;
            return toolState.Status == MigrationStatus.Completed;
        }

        bool RunParametersAreDifferent(EndpointInfo endpointInfo, IDictionary<string, string> runParameters, ToolState currentRunState)
        {
            if (endpointInfo.EndpointName != currentRunState.Endpoint.EndpointName)
            {
                return true;
            }

            var currentRunParameters = currentRunState.RunParameters;

            if (runParameters.Count != currentRunParameters.Count)
            {
                return true;
            }

            foreach (var parameterKey in runParameters.Keys)
            {
                if (!currentRunParameters.ContainsKey(parameterKey))
                {
                    return true;
                }

                if (!string.Equals(runParameters[parameterKey], currentRunParameters[parameterKey], StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }

            return false;
        }

        async Task MarkStorageAsPrepared(ToolState toolState)
        {
            toolState.Status = MigrationStatus.StoragePrepared;
            await timeoutStorage.StoreToolState(toolState);
        }

        async Task MarkCurrentBatchAsStaged(ToolState toolState)
        {
            toolState.GetCurrentBatch().State = BatchState.Staged;
            await timeoutStorage.StoreToolState(toolState);
        }

        async Task CompleteCurrentBatch(ToolState toolState)
        {
            var currentBatch = toolState.GetCurrentBatch();
            currentBatch.State = BatchState.Completed;
            await timeoutStorage.CompleteBatch(currentBatch.Number);
            await timeoutStorage.StoreToolState(toolState);
        }

        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}