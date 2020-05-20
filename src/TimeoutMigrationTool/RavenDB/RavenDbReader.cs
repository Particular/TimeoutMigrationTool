using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenDbReader
    {
        private readonly string serverUrl;
        private readonly string databaseName;
        private readonly RavenDbVersion version;

        public RavenDbReader(string serverUrl, string databaseName, RavenDbVersion version)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
            this.version = version;
        }

        public async Task<List<T>> GetItems<T>(Func<T, bool> filterPredicate, string prefix, CancellationToken cancellationToken, int pageSize = RavenConstants.DefaultPagingSize) where T: class
        {
            var items = new List<T>();
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
                        var pagedTimeouts =
                            await GetDocumentsFromResponse<T>(result.Content).ConfigureAwait(false);
                        if (pagedTimeouts.Count == 0 || pagedTimeouts.Count < pageSize)
                            checkForMoreResults = false;

                        var elegibleItems = pagedTimeouts.Where(filterPredicate);
                        items.AddRange(elegibleItems);
                        iteration++;
                    }
                }

                return items;
            }
        }

        private async Task<List<T>> GetDocumentsFromResponse<T>(HttpContent resultContent) where T : class
        {
            var contentString = await resultContent.ReadAsStringAsync().ConfigureAwait(false);
            if (version == RavenDbVersion.Four)
            {
                var jObject = JObject.Parse(contentString);
                var resultSet = jObject.SelectToken("Results");
                contentString = resultSet.ToString();
            }

            return JsonConvert.DeserializeObject<List<T>>(contentString);
        }
    }
}