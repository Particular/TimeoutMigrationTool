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
                if(endpointToMigrate.NrOfTimeouts == 0)
                {
                    logger.LogInformation($"No timeouts found for {endpointToMigrate.EndpointName} migration will be skipped");
                    continue;
                }

                logger.LogInformation($"Starting migration for {endpointToMigrate.EndpointName}, {endpointToMigrate.NrOfTimeouts}");
                await Run(cutOffTime, endpointToMigrate.EndpointName, runParameters);
            }
        }

        async Task Run(DateTime cutOffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            var toolState = await timeoutStorage.TryLoadOngoingMigration();
            GuardAgainstInvalidState(endpointName, runParameters, toolState);

            if (toolState == null)
            {
                toolState = await timeoutStorage.Prepare(cutOffTime, endpointName, runParameters);
                logger.LogInformation("Storage has been prepared for migration.");
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

            await timeoutStorage.Complete();
            logger.LogInformation("Migration completed successfully");
        }

        void GuardAgainstInvalidState(string endpointName, IDictionary<string, string> runParameters, ToolState toolState)
        {
            if (toolState == null)
            {
                return;
            }
            if (toolState.Status == MigrationStatus.Completed)
            {
                throw new Exception("We messed up, found a completed toolstate");
            }

            if (RunParametersAreDifferent(endpointName, runParameters, toolState))
            {
                var sb = new StringBuilder();

                sb.AppendLine("In progress migration parameters didn't match, either rerun with the --abort option or adjust the parameters to match to continue the current migration:");
                sb.AppendLine($"\t'--endpoint': '{endpointName}'.");

                foreach (var setting in toolState.RunParameters)
                {
                    sb.AppendLine($"\t'{setting.Key}': '{setting.Value}'.");
                }

                throw new Exception(sb.ToString());
            }

            logger.LogInformation("Resuming in progress migration");
        }


        bool RunParametersAreDifferent(string endpointName, IDictionary<string, string> runParameters, ToolState currentRunState)
        {
            if (endpointName != currentRunState.EndpointName)
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

        async Task MarkCurrentBatchAsStaged(ToolState toolState)
        {
            var currentBatch = toolState.GetCurrentBatch();
            currentBatch.State = BatchState.Staged;
            await timeoutStorage.MarkBatchAsStaged(currentBatch.Number);
        }

        async Task CompleteCurrentBatch(ToolState toolState)
        {
            var currentBatch = toolState.GetCurrentBatch();
            currentBatch.State = BatchState.Completed;
            await timeoutStorage.MarkBatchAsCompleted(currentBatch.Number);
        }

        readonly ILogger logger;
        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}