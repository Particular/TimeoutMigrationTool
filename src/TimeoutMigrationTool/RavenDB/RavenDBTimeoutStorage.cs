using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

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

        public async Task<List<BatchInfo>> Prepare()
        {
            var ravenDbReader = new RavenDBTimeoutsReader();
            var timeouts = await ravenDbReader.ReadTimeoutsFrom(serverUrl, databaseName, prefix, DateTime.Now, ravenVersion,
                CancellationToken.None).ConfigureAwait(false);

            var nrOfBatches = Math.Ceiling(timeouts.Count / (decimal)RavenConstants.DefaultPagingSize);
            var batches = new List<BatchInfo>();
            for (int i = 0; i < nrOfBatches; i++)
            {
                batches.Add(new BatchInfo
                {
                    Number = i+1,
                    State = BatchState.Pending,
                    TimeoutIds = timeouts.Skip(i*RavenConstants.DefaultPagingSize).Take(RavenConstants.DefaultPagingSize).Select(t => t.Id).ToArray()
                });
            }

            using (var httpClient = new HttpClient())
            {
                var bulkInsertUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                foreach (var batch in batches)
                {
                    var bulkCreateBatchAndUpdateTimeoutsCommand = GetBatchInsertCommand(batch, ravenVersion);
                    var serializedCommands = JsonConvert.SerializeObject(bulkCreateBatchAndUpdateTimeoutsCommand);
                    var result = await httpClient.PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json")).ConfigureAwait(false);
                    result.EnsureSuccessStatusCode();
                }
            }

            return batches;
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

        private static object GetBatchInsertCommand(BatchInfo batch, RavenDbVersion version)
        {
            if (version == RavenDbVersion.Four)
            {
                var insertCommand = new PutCommand
                {
                    Id = $"batch/{batch.Number}",
                    Type = "PUT",
                    ChangeVector = (object)null,
                    Document = batch
                };

                var timeoutUpdateCommands = batch.TimeoutIds.Select(timeoutId => new PatchCommand
                {
                    Id = timeoutId,
                    Type = "PATCH",
                    ChangeVector = null,
                    Patch = new Patch()
                    {
                        Script = "this.OwningTimeoutManager = 'Archived_' + this.OwningTimeoutManager;",
                        Values = new { }
                    }
                }).ToList();

                List<object> commands = new List<object>();
                commands.Add(insertCommand);
                commands.AddRange(timeoutUpdateCommands);

                return new
                {
                    Commands = commands.ToArray()
                };
            }

            return null;
        }
    }
}