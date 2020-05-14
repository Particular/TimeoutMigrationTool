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
namespace Particular.TimeoutMigrationTool.RavenDB
{
    class RavenDBTimeoutsReader
    {
        public async Task<string[]> ListDestinationEndpoints(string serverName, string databaseName, string prefix, CancellationToken cancellationToken)
        {
            var timeouts = await ReadTimeoutsFrom(serverName, databaseName, prefix, DateTime.Now.AddDays(-1), cancellationToken).ConfigureAwait(false);
            return timeouts.Select(x => x.Destination).Distinct().ToArray();
        }

        public async Task<List<TimeoutData>> ReadTimeoutsFrom(string serverUrl, string databaseName, string prefix,
            DateTime maxCutoffTime,
            CancellationToken cancellationToken)
        {
            var timeouts = new List<TimeoutData>();
            var pageSize = 100;
            using (var client = new HttpClient())
            {
                var url = $"{serverUrl}/databases/{databaseName}/docs?startsWith={prefix}&pageSize={pageSize}";
                var checkForMoreResults = true;
                var iteration = 0;

                while (checkForMoreResults)
                {
                    var skipFirst = $"&start={iteration * pageSize}";
                    var getUrl = iteration == 0 ? url : url + skipFirst;
                    var result = await client.GetAsync(getUrl, cancellationToken).ConfigureAwait(false);
                
                    if (result.StatusCode == HttpStatusCode.OK)
                    {
                        var contentString = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var jObject = JObject.Parse(contentString);
                        var resultSet = jObject.SelectToken("Results");
                        var pagedTimeouts = JsonConvert.DeserializeObject < List<TimeoutData>>(resultSet.ToString());
                        
                        if (pagedTimeouts.Count == 0 || pagedTimeouts.Count < pageSize) 
                            checkForMoreResults = false;

                        var elegibleTimeouts = pagedTimeouts.Where(x => x.Time >= maxCutoffTime);
                        timeouts.AddRange(elegibleTimeouts);
                        iteration++;
                    }
                }

                return timeouts;
            }
        }
    }
}
