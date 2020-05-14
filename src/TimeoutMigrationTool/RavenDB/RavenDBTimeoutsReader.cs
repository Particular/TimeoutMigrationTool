using System;
using System.Collections.Generic;
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
        public Task<string[]> ListEndpoints(string connectionString, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public async Task<List<TimeoutData>> ReadTimeoutsFrom(string connectionString, CancellationToken cancellationToken)
        {
            //this needs to be paged so Task<List<TimeoutData>> is not enough
            var timeouts = new List<TimeoutData>();
            var pageSize = 100;
            using (var client = new HttpClient())
            {
                var url = $"{connectionString}/docs?startsWith=TimeoutDatas&pageSize={pageSize}";
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
                        
                        timeouts.AddRange(pagedTimeouts);
                        iteration++;
                    }
                }

                return timeouts;
            }
        }
    }
}
