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
            var toolState = await timeoutStorage.TryLoadOngoingMigration();
            if (toolState == null)
            {
                throw new Exception("Could not find a previous migration to abort.");
            }

            logger.LogInformation($"Aborting ongoing migration for {toolState.EndpointName}");

            await timeoutStorage.Abort();
        }

        readonly ILogger logger;
        readonly ITimeoutStorage timeoutStorage;
    }
}