namespace Particular.TimeoutMigrationTool
{
    using Microsoft.Extensions.Logging;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class PreviewRunner
    {
        public PreviewRunner(ILogger logger, ITimeoutStorage timeoutStorage, ICreateTransportTimeouts transportTimeoutsCreator)
        {
            this.logger = logger;
            this.timeoutStorage = timeoutStorage;
            this.transportTimeoutsCreator = transportTimeoutsCreator;
        }

        public async Task Run()
        {
            var endpoints = await timeoutStorage.ListEndpoints(DateTime.Parse("2012-01-01"));

            var endpointProblems = new Dictionary<string,List<string>>();
            logger.LogInformation("Running preview");
            PrintLine();
            PrintRow("Endpoint", "Number of timeouts", "Shortest", "Longest", "Destinations");
            PrintLine();
            foreach (var endpoint in endpoints)
            {
                PrintRow(endpoint.EndpointName, endpoint.NrOfTimeouts, endpoint.ShortestTimeout, endpoint.LongestTimeout,string.Join(",", endpoint.Destinations));

                endpointProblems[endpoint.EndpointName] = new List<string>();

                var migrationCheckResult = await transportTimeoutsCreator.AbleToMigrate(endpoint);

                if (!migrationCheckResult.CanMigrate)
                {
                    endpointProblems[endpoint.EndpointName].AddRange(migrationCheckResult.Problems);
                }

                if((endpoint.ShortestTimeout - DateTime.UtcNow) < TimeSpan.FromHours(4) )
                {
                    endpointProblems[endpoint.EndpointName].Add($"Shortest timeout is {endpoint.ShortestTimeout} is about to trigger and can trigger to late should the migration take a long time.");
                }
            }
            PrintLine();

            foreach (var endpoint in endpointProblems)
            {
                if(!endpoint.Value.Any())
                {
                    continue;
                }

                logger.LogInformation($"Potential issues detected for: {endpoint.Key}");
                foreach (var problem in endpoint.Value)
                {
                    logger.LogInformation($"\t-{problem}");
                }

                PrintLine();
            }
        }

        void PrintLine()
        {
            logger.LogInformation(new string('-', tableWidth));
        }

        void PrintRow(params object[] columns)
        {
            int width = (tableWidth - columns.Length) / columns.Length;
            string row = "|";

            foreach (var column in columns)
            {
                row += AlignCentre(column.ToString(), width) + "|";
            }

            logger.LogInformation(row);
        }

        string AlignCentre(string text, int width)
        {
            text = text.Length > width ? text.Substring(0, width - 3) + "..." : text;

            if (string.IsNullOrEmpty(text))
            {
                return new string(' ', width);
            }
            else
            {
                return text.PadRight(width - (width - text.Length) / 2).PadLeft(width);
            }
        }

        static int tableWidth = 120;

        readonly ILogger logger;
        readonly ITimeoutStorage timeoutStorage;
        readonly ICreateTransportTimeouts transportTimeoutsCreator;
    }
}