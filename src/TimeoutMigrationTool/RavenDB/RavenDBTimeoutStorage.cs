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
        private readonly RavenDbReader reader;

        public RavenDBTimeoutStorage(string serverUrl, string databaseName, string prefix, RavenDbVersion ravenVersion)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
            this.prefix = prefix;
            this.ravenVersion = ravenVersion;
            this.reader = new RavenDbReader(serverUrl, databaseName, ravenVersion);
        }

        public async Task<ToolState> GetToolState()
        {
            ToolState toolState;
            using (var httpClient = new HttpClient())
            {
                var getStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.GetAsync(getStateUrl).ConfigureAwait(false);
                if (result.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }

                var content = await result.Content.ReadAsStringAsync().ConfigureAwait(false);
                var jObject = JObject.Parse(content);
                var resultSet = jObject.SelectToken("Results");
                content = resultSet.ToString();

                toolState = JsonConvert.DeserializeObject<List<ToolState>>(content).Single();
                return toolState;
            }
        }

        public async Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime)
        {
            await CleanupOnlyExistingBatches().ConfigureAwait(false);

            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime).ConfigureAwait(false);
            return batches;
        }

        internal async Task<List<BatchInfo>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime)
        {
            bool filter(TimeoutData td) => td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationPrefix, StringComparison.OrdinalIgnoreCase);
            var timeouts = await reader.GetItems<TimeoutData>(filter, prefix, CancellationToken.None).ConfigureAwait(false);

            var nrOfBatches = Math.Ceiling(timeouts.Count / (decimal) RavenConstants.DefaultPagingSize);
            var batches = new List<BatchInfo>();
            for (int i = 0; i < nrOfBatches; i++)
            {
                batches.Add(new BatchInfo
                {
                    Number = i + 1,
                    State = BatchState.Pending,
                    TimeoutIds = timeouts.Skip(i * RavenConstants.DefaultPagingSize).Take(RavenConstants.DefaultPagingSize)
                        .Select(t => t.Id).ToArray()
                });
            }

            using (var httpClient = new HttpClient())
            {
                var bulkInsertUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                foreach (var batch in batches)
                {
                    var bulkCreateBatchAndUpdateTimeoutsCommand = GetBatchInsertCommand(batch, ravenVersion);
                    var serializedCommands = JsonConvert.SerializeObject(bulkCreateBatchAndUpdateTimeoutsCommand);
                    var result = await httpClient
                        .PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"))
                        .ConfigureAwait(false);
                    result.EnsureSuccessStatusCode();
                }
            }

            return batches;
        }

        internal Task CleanupOnlyExistingBatches()
        {
            throw new NotImplementedException();
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(IEnumerable<BatchInfo> batchesForWhichToResetTimeouts)
        {
            //TODO: implement (and test) new behavior:
            /*
             * Cleanup all the batches (and not only the ones in the batchesToReset)
             * Reset only timeouts referenced by the list of batches to reset, provided as an argument here
             */
            var existingBatches = await reader.GetItems<BatchInfo>(x => true, "batch", CancellationToken.None).ConfigureAwait(false);
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                foreach (var batch in existingBatches)
                {
                    object commandsToExecuteForBatch;
                    if (batchesForWhichToResetTimeouts.Any(b => b.Number == batch.Number))
                        commandsToExecuteForBatch = GetBatchDeleteAndUpdateTimeoutCommand(batch, ravenVersion);
                    else
                        commandsToExecuteForBatch = GetBatchDeleteCommand(batch, ravenVersion);

                    var serializedCommands = JsonConvert.SerializeObject(commandsToExecuteForBatch);
                    var result = await httpClient.PostAsync(bulkUpdateUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json")).ConfigureAwait(false);
                    result.EnsureSuccessStatusCode();
                }
            }
        }

        internal async Task RemoveToolState()
        {
            using (var httpClient = new HttpClient())
            {
                var deleteStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.DeleteAsync(deleteStateUrl).ConfigureAwait(false);
                result.EnsureSuccessStatusCode();
            }
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            throw new NotImplementedException();
        }

        public Task CompleteBatch(int number)
        {
            throw new NotImplementedException();
        }

        public async Task StoreToolState(ToolState toolState)
        {
            using (var httpClient = new HttpClient())
            {
                var insertStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                var serializeObject = JsonConvert.SerializeObject(toolState);
                var httpContent = new StringContent(serializeObject);

                var insertResult = await httpClient.PutAsync(insertStateUrl, httpContent).ConfigureAwait(false);
                insertResult.EnsureSuccessStatusCode();
            }
        }

        public async Task Reset()
        {
            //TODO: implement (and test) new behavior:
            /*
             * Cleanup all the batches (and not only the ones in the batchesToReset)
             * Remove tool state
             * Reset only timeouts referenced by the list of batches to reset, provided as an argument here
             */
            var toolState = await GetToolState().ConfigureAwait(false);
            var incompleteBatches = toolState.Batches.Where(bi => bi.State != BatchState.Completed);
            await CleanupExistingBatchesAndResetTimeouts(incompleteBatches).ConfigureAwait(false);
            await RemoveToolState().ConfigureAwait(false);
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
                        Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationPrefix}' + this.OwningTimeoutManager;",
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

        private static object GetDeleteCommand(BatchInfo batch, RavenDbVersion version)
        {
            if (version == RavenDbVersion.Four)
            {
                return new
                {
                    Id = $"batch/{batch.Number}",
                    ChangeVector = (object) null,
                    Type = "DELETE"
                };
            }

            return null;
        }

        private static object GetBatchDeleteCommand(BatchInfo batch, RavenDbVersion version)
        {
            List<object> commands = new List<object>();
            commands.Add(GetDeleteCommand(batch, version));

            return new
            {
                Commands = commands.ToArray()
            };
        }

        private static object GetBatchDeleteAndUpdateTimeoutCommand(BatchInfo batch, RavenDbVersion version)
        {
            if (version == RavenDbVersion.Four)
            {
                var deleteCommand = GetDeleteCommand(batch, version);
                var timeoutUpdateCommands = batch.TimeoutIds.Select(timeoutId => new PatchCommand
                {
                    Id = timeoutId,
                    Type = "PATCH",
                    ChangeVector = null,
                    Patch = new Patch()
                    {
                        Script = $"this.OwningTimeoutManager = this.OwningTimeoutManager.substr({RavenConstants.MigrationPrefix.Length});",
                        Values = new { }
                    }
                }).ToList();

                List<object> commands = new List<object>();
                commands.Add(deleteCommand);
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