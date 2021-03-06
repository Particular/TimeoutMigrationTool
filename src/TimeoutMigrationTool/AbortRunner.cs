﻿namespace Particular.TimeoutMigrationTool
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Threading.Tasks;

    public class AbortRunner
    {
        public AbortRunner(ILogger logger, ITimeoutsSource timeoutsSource, ITimeoutsTarget timeoutsTarget)
        {
            this.timeoutsTarget = timeoutsTarget;
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
            await timeoutsSource.Abort();
            await timeoutsTarget.Abort(toolState.EndpointName);

            logger.LogInformation("Previous migration was successfully aborted. That means that the timeouts hidden away from the TimeoutManager, have been made available again");
        }

        readonly ILogger logger;
        readonly ITimeoutsSource timeoutsSource;
        readonly ITimeoutsTarget timeoutsTarget;
    }
}