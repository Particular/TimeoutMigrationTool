﻿namespace Particular.TimeoutMigrationTool
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class PreviewRunner
    {
        public PreviewRunner(ILogger logger, ITimeoutsSource timeoutsSource, ITimeoutsTarget timeoutsTarget)
        {
            this.logger = logger;
            this.timeoutsSource = timeoutsSource;
            this.timeoutsTarget = timeoutsTarget;
        }

        public async Task Run()
        {
            var endpoints = await timeoutsSource.ListEndpoints(DateTime.Parse("2012-01-01"));

            var endpointProblems = new Dictionary<string, List<string>>();
            if (!endpoints.Any())
            {
                logger.LogInformation($"No endpoints found in storage");
                return;
            }

            logger.LogInformation($"The following endpoints were found:\n");

            foreach (var endpoint in endpoints)
            {
                logger.LogInformation($"{endpoint.EndpointName}");
                logger.LogInformation($"\t-Total number of timeouts: {endpoint.NrOfTimeouts}");
                logger.LogInformation($"\t-Shortest timeout: {endpoint.ShortestTimeout}");
                logger.LogInformation($"\t-Longest timeout: {endpoint.LongestTimeout}");
                logger.LogInformation($"\t-Timeout destinations: {string.Join(",", endpoint.Destinations)}");

                endpointProblems[endpoint.EndpointName] = [];

                var migrationCheckResult = await timeoutsTarget.AbleToMigrate(endpoint);

                if (!migrationCheckResult.CanMigrate)
                {
                    endpointProblems[endpoint.EndpointName].AddRange(migrationCheckResult.Problems);
                }

                if ((endpoint.ShortestTimeout - DateTime.UtcNow) < TimeSpan.FromHours(4))
                {
                    endpointProblems[endpoint.EndpointName].Add($"Shortest timeout is {endpoint.ShortestTimeout} is about to trigger and can trigger to late should the migration take a long time.");
                }
            }

            foreach (var endpoint in endpointProblems)
            {
                if (!endpoint.Value.Any())
                {
                    continue;
                }

                logger.LogInformation($"Potential issues detected for: {endpoint.Key}");
                foreach (var problem in endpoint.Value)
                {
                    logger.LogInformation($"\t-{problem}");
                }
            }
        }

        readonly ILogger logger;
        readonly ITimeoutsSource timeoutsSource;
        readonly ITimeoutsTarget timeoutsTarget;
    }
}