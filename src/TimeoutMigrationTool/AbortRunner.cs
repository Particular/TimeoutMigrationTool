namespace Particular.TimeoutMigrationTool
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Threading.Tasks;

    public class AbortRunner
    {
        public AbortRunner(ILogger logger, ITimeoutStorage timeoutStorage)
        {
            this.logger = logger;
            this.timeoutStorage = timeoutStorage;
        }

        public async Task Run()
        {
            var shouldAbort = await timeoutStorage.CheckIfAMigrationIsInProgress();
            if (!shouldAbort)
            {
                throw new Exception("Could not find a previous migration to abort.");
            }

            var toolState = await timeoutStorage.TryLoadOngoingMigration();
            if (toolState != null)
            {
                logger.LogInformation($"Aborting ongoing migration for {toolState.EndpointName}");
            }
            else
            {
                logger.LogInformation("Cleaning up changes made in the previous interrupted migration");
            }

            await timeoutStorage.Abort();

            logger.LogInformation("Previous migration was successfully aborted. That means that the timeouts hidden away from the TimeoutManager, have been made available again");
        }

        readonly ILogger logger;
        readonly ITimeoutStorage timeoutStorage;
    }
}