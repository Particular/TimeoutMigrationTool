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
                var result = await httpClient.GetAsync(getStateUrl);
                if (result.StatusCode == HttpStatusCode.NotFound)
                {
                    return null;
                }

                var content = await result.Content.ReadAsStringAsync();
                var jObject = JObject.Parse(content);
                var resultSet = jObject.SelectToken("Results");
                content = resultSet.ToString();

                toolState = JsonConvert.DeserializeObject<List<ToolState>>(content).Single();
                return toolState;
            }
        }

        public async Task<bool> CanPrepareStorage()
        {
            var existingBatches = await reader.GetItems<BatchInfo>(x => true, "batch", CancellationToken.None)
                ;
            if (existingBatches.Any())
                return false;
            return true;
        }

        public async Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime)
        {
            var batchesInStorage = await reader.GetItems<BatchInfo>(x => true, "batch", CancellationToken.None);
            var batchDeleteCommand = GetBatchDeleteCommand(batchesInStorage.ToArray(), ravenVersion);
            using (var httpClient = new HttpClient())
            {
                var bulkDeleteUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                var serializedCommands = JsonConvert.SerializeObject(batchDeleteCommand);
                var result = await httpClient.PostAsync(bulkDeleteUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }

            var batches = await PrepareBatchesAndTimeouts(maxCutoffTime);
            return batches;
        }

        internal async Task<List<BatchInfo>> PrepareBatchesAndTimeouts(DateTime maxCutoffTime)
        {
            bool filter(TimeoutData td) => td.Time >= maxCutoffTime && !td.OwningTimeoutManager.StartsWith(RavenConstants.MigrationPrefix, StringComparison.OrdinalIgnoreCase);
            var timeouts = await reader.GetItems<TimeoutData>(filter, prefix, CancellationToken.None);

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
                        ;
                    result.EnsureSuccessStatusCode();
                }
            }

            return batches;
        }

        internal async Task CleanupExistingBatchesAndResetTimeouts(List<BatchInfo> batchesToRemove, List<BatchInfo> batchesForWhichToResetTimeouts)
        {
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                foreach (var batch in batchesToRemove)
                {
                    object commandsToExecuteForBatch;
                    if (batchesForWhichToResetTimeouts.Any(b => b.Number == batch.Number))
                        commandsToExecuteForBatch = GetBatchDeleteAndUpdateTimeoutCommand(batch, ravenVersion);
                    else
                        commandsToExecuteForBatch = GetBatchDeleteCommand(new []{batch}, ravenVersion);

                    var serializedCommands = JsonConvert.SerializeObject(commandsToExecuteForBatch);
                    var result = await httpClient.PostAsync(bulkUpdateUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                    result.EnsureSuccessStatusCode();
                }
            }
        }

        internal async Task RemoveToolState()
        {
            using (var httpClient = new HttpClient())
            {
                var deleteStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.DeleteAsync(deleteStateUrl);
                result.EnsureSuccessStatusCode();
            }
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            var toolState = await GetToolState();
            string prefix = RavenConstants.DefaultTimeoutPrefix;
            if (toolState.RunParameters.ContainsKey(ApplicationOptions.RavenTimeoutPrefix))
                prefix = toolState.RunParameters[ApplicationOptions.RavenTimeoutPrefix];

            var batch = await reader.GetItem<BatchInfo>($"batch/{batchNumber}");
            var timeouts = await reader.GetItems<TimeoutData>(t => batch.TimeoutIds.Contains(t.Id), prefix, CancellationToken.None);
            return timeouts;
        }

        public async Task CompleteBatch(int batchNumber)
        {
            var batch = await reader.GetItem<BatchInfo>($"batch/{batchNumber}");
            batch.State = BatchState.Completed;

            using (var httpClient = new HttpClient())
            {
                var updateBatchUrl = $"{serverUrl}/databases/{databaseName}/docs?id=batch/{batchNumber}";
                var serializeObject = JsonConvert.SerializeObject(batch);
                var httpContent = new StringContent(serializeObject);

                var saveResult = await httpClient.PutAsync(updateBatchUrl, httpContent);
                saveResult.EnsureSuccessStatusCode();
            }
        }

        public async Task StoreToolState(ToolState toolState)
        {
            using (var httpClient = new HttpClient())
            {
                var insertStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                var serializeObject = JsonConvert.SerializeObject(toolState);
                var httpContent = new StringContent(serializeObject);

                var saveResult = await httpClient.PutAsync(insertStateUrl, httpContent);
                saveResult.EnsureSuccessStatusCode();
            }
        }

        public async Task Abort(ToolState toolState)
        {
            if (toolState == null)
                throw new ArgumentNullException(nameof(toolState), "Can't abort without a toolstate");

            if (toolState.Batches.Any())
            {
                var incompleteBatches = toolState.Batches.Where(bi => bi.State != BatchState.Completed).ToList();
                await CleanupExistingBatchesAndResetTimeouts(toolState.Batches.ToList(), incompleteBatches);
            }
            else
            {
                var batches = await reader.GetItems<BatchInfo>(x => true, "batch", CancellationToken.None);
                await CleanupExistingBatchesAndResetTimeouts(batches, batches);
            }

            await RemoveToolState();
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

        private static object GetBatchDeleteCommand(BatchInfo[] batches, RavenDbVersion version)
        {
            List<object> commands = new List<object>();
            foreach (var batch in batches)
            {
                commands.Add(GetDeleteCommand(batch, version));
            }

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