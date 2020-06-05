namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System;
    using System.Linq;
    using System.Text;
    using Microsoft.Extensions.Logging;

    public class MigrationRunner
    {
        public MigrationRunner(ILogger logger, ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.logger = logger;
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
                logger.LogInformation($"Verifying that timeouts set by {endpoint.EndpointName} can be migrated");
                var migrationCheckResult = await transportTimeoutsCreator.AbleToMigrate(endpoint);

                if (!migrationCheckResult.CanMigrate)
                {
                    problematicEndpoints.Add((endpoint, migrationCheckResult.Problems));
                }
            }

            if (problematicEndpoints.Any())
            {
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

            foreach (var endpointToMigrate in endpointsToMigrate)
            {
                logger.LogInformation($"Starting migration for {endpointToMigrate.EndpointName}, {endpointToMigrate.NrOfTimeouts}");
                await Run(cutOffTime, endpointToMigrate, runParameters);
            }
        }

        async Task Run(DateTime cutOffTime, EndpointInfo endpointInfo, IDictionary<string, string> runParameters)
        {
            var toolState = await timeoutStorage.GetToolState();

            if (ShouldCreateFreshToolState(toolState))
            {
                toolState = new ToolState(runParameters, endpointInfo);
                await timeoutStorage.StoreToolState(toolState);
                logger.LogInformation("Migration status created and stored.");
            }

            switch (toolState.Status)
            {
                case MigrationStatus.NeverRun:
                    var canPrepStorage = await timeoutStorage.CanPrepareStorage();
                    if (!canPrepStorage)
                    {
                        throw new Exception("We found some leftovers of a previous run. Please use the abort option to clean up the state and then rerun.");
                    }

                    await Prepare(cutOffTime, toolState, endpointInfo);

                    break;
                case MigrationStatus.Completed:

                    await Prepare(cutOffTime, toolState, endpointInfo);

                    break;
                case MigrationStatus.StoragePrepared when RunParametersAreDifferent(endpointInfo, runParameters, toolState):
                    var sb = new StringBuilder();

                    sb.AppendLine("In progress migration parameters didn't match, either rerun with the --abort option or adjust the parameters to match to continue the current migration:");

                    sb.AppendLine($"\t'--endpoint': '{endpointInfo.EndpointName}'.");

                    foreach (var setting in toolState.RunParameters)
                    {
                        sb.AppendLine($"\t'{setting.Key}': '{setting.Value}'.");
                    }

                    throw new Exception(sb.ToString());

                case MigrationStatus.StoragePrepared:
                    logger.LogInformation("Resuming in progress migration");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            while (toolState.HasMoreBatches())
            {
                var batch = toolState.GetCurrentBatch();
                logger.LogInformation($"Migrating batch {batch.Number}");

                if (batch.State == BatchState.Pending)
                {
                    logger.LogDebug("Reading batch");
                    var timeouts = await timeoutStorage.ReadBatch(batch.Number);

                    logger.LogDebug("Staging batch");
                    var stagedTimeoutCount = await transportTimeoutsCreator.StageBatch(timeouts);
                    if (batch.TimeoutIds.Length != stagedTimeoutCount)
                    {
                        throw new InvalidOperationException($"The amount of staged timeouts does not match the amount of timeouts in the batch of a number: {batch.Number}. Staged amount of timeouts: {stagedTimeoutCount}, batch contains {batch.TimeoutIds.Length}.");
                    }

                    await MarkCurrentBatchAsStaged(toolState);
                    logger.LogDebug("Batch marked as staged");
                }

                logger.LogDebug("Completing batch");
                var completedTimeoutsCount = await transportTimeoutsCreator.CompleteBatch(batch.Number);

                if (batch.TimeoutIds.Length != completedTimeoutsCount)
                {
                    throw new InvalidOperationException($"The amount of completed timeouts does not match the amount of timeouts in the batch of a number: {batch.Number}. Completed amount of timeouts: {completedTimeoutsCount}, batch contains {batch.TimeoutIds.Length}.");
                }

                await CompleteCurrentBatch(toolState);

                logger.LogDebug("Batch fully migrated");
            }

            toolState.Status = MigrationStatus.Completed;

            await timeoutStorage.StoreToolState(toolState);
            logger.LogInformation("Migration completed successfully");
        }

        async Task Prepare(DateTime cutOffTime, ToolState toolState, EndpointInfo endpoint)
        {
            logger.LogDebug("Preparing storage");
            var batches = await timeoutStorage.Prepare(cutOffTime, endpoint);

            if (!batches.Any())
            {
                logger.LogWarning("No data was found to migrate. If you think this is not possible, verify your parameters and try again.");
            }

            toolState.InitBatches(batches);
            await MarkStorageAsPrepared(toolState);
            logger.LogInformation("Storage prepared");
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
            var currentBatch = toolState.GetCurrentBatch();
            currentBatch.State = BatchState.Staged;
            await timeoutStorage.MarkBatchAsStaged(currentBatch.Number);
            await timeoutStorage.StoreToolState(toolState);
        }

        async Task CompleteCurrentBatch(ToolState toolState)
        {
            var currentBatch = toolState.GetCurrentBatch();
            currentBatch.State = BatchState.Completed;
            await timeoutStorage.MarkBatchAsCompleted(currentBatch.Number);
            await timeoutStorage.StoreToolState(toolState);
        }

        readonly ILogger logger;
        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}