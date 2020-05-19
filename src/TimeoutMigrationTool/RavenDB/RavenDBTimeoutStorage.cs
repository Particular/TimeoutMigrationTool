using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public class RavenDBTimeoutStorage : ITimeoutStorage
    {
        private readonly string databaseName;
        private readonly string prefix;
        private readonly RavenDbVersion ravenVersion;

        private readonly string serverUrl;

        public RavenDBTimeoutStorage(string serverUrl, string databaseName, string prefix, RavenDbVersion ravenVersion)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
            this.prefix = prefix;
            this.ravenVersion = ravenVersion;
        }

        public async Task<ToolState> GetOrCreateToolState()
        {
            ToolState toolState;
            using (var httpClient = new HttpClient())
            {
                var getStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.GetAsync(getStateUrl).ConfigureAwait(false);
                if (result.StatusCode == HttpStatusCode.NotFound)
                {
                    var insertStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                    // Insert the tool state data
                    toolState = new ToolState
                    {
                        IsStoragePrepared = false
                    };

                    var serializeObject = JsonConvert.SerializeObject(toolState);
                    var httpContent = new StringContent(serializeObject);

                    var insertResult = await httpClient.PutAsync(insertStateUrl, httpContent).ConfigureAwait(false);
                    insertResult.EnsureSuccessStatusCode();
                    return toolState;
                }

                var content = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
                var jObject = JObject.Parse(content);
                var resultSet = jObject.SelectToken("Results");
                content = resultSet.ToString();

                toolState = JsonConvert.DeserializeObject<List<ToolState>>(content).Single();
                return toolState;
            }
        }

        public Task<List<BatchInfo>> Prepare()
        {
            throw new NotImplementedException();
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            throw new NotImplementedException();
        }

        public Task CompleteBatch(int number)
        {
            throw new NotImplementedException();
        }

        public Task StoreToolState(ToolState toolState)
        {
            throw new NotImplementedException();
        }
    }
}