namespace Particular.TimeoutMigrationTool
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Threading.Tasks;

    public class AbortRunner
    {
        public AbortRunner(ILogger logger, ITimeoutsSource timeoutsSource)
        {
            this.logger = logger;
            this.timeoutsSource = timeoutsSource;
        }

        public async Task Run()
        {
            var shouldAbort = await timeoutsSource.CheckIfAMigrationIsInProgress();
            if (!shouldAbort)
            {
                throw new Exception("Could not find a previous migration to abort.");
            }

            var toolState = await timeoutsSource.TryLoadOngoingMigration();
            if (toolState != null)
            {
                logger.LogInformation($"Aborting ongoing migration for {toolState.EndpointName}");
            }
            else
            {
                logger.LogInformation("Cleaning up changes made in the previous interrupted migration");
            }

            await timeoutsSource.Abort();

            logger.LogInformation("Previous migration was successfully aborted. That means that the timeouts hidden away from the TimeoutManager, have been made available again");
        }

        readonly ILogger logger;
        readonly ITimeoutsSource timeoutsSource;
    }
}