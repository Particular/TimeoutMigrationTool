using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

[assembly: InternalsVisibleTo("TimeoutMigrationTool.Raven4.Tests")]
[assembly: InternalsVisibleTo("TimeoutMigrationTool.Raven3.Tests")]

namespace Particular.TimeoutMigrationTool.RavenDB
{
    internal class RavenDBTimeoutsReader
    {
        public async Task<string[]> ListDestinationEndpoints(string serverName, string databaseName, string prefix,
            RavenDbVersion version, CancellationToken cancellationToken)
        {
            var timeouts =
                await ReadTimeoutsFrom(serverName, databaseName, prefix, DateTime.Now.AddDays(-1), version,
                    cancellationToken).ConfigureAwait(false);
            return timeouts.Select(x => x.Destination).Distinct().ToArray();
        }

        public async Task<List<TimeoutData>> ReadTimeoutsFrom(string serverUrl, string databaseName, string prefix,
            DateTime maxCutoffTime, RavenDbVersion version, CancellationToken cancellationToken)
        {
            var pageSize = 100;
            var ravenDbReader = new RavenDbReader<TimeoutData>(serverUrl, databaseName, version, pageSize);
            var timeouts = await ravenDbReader.GetItems(data => data.Time >= maxCutoffTime, prefix, cancellationToken).ConfigureAwait(false);

            return timeouts;

        }
    }
}