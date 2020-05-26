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
    internal class Raven3Adapter : ICanTalkToRavenVersion
    {
        private string serverUrl;
        private string databaseName;
        public Raven3Adapter(string serverUrl, string databaseName)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
        }
        public async Task UpdateRecord(string key, object document)
        {
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                var command = new[] {
                    new
                    {
                        Key = key,
                        Method = "PUT",
                        Document = document,
                        Metadata = new object()
                    }
                };

                var serializedCommands = JsonConvert.SerializeObject(command);
                var result = await httpClient.PostAsync(bulkUpdateUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        public Task InsertRecord(string key, object document)
        {
            throw new NotImplementedException();
        }

        public async Task DeleteRecord(string key)
        {
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                var deleteCommand = GetDeleteCommand(key);
                var command = new[]
                {
                    deleteCommand
                };
                var serializedCommands = JsonConvert.SerializeObject(command);
                var result = await httpClient.PostAsync(bulkUpdateUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        public async Task CreateBatchAndUpdateTimeouts(BatchInfo batch)
        {
            var commands = batch.TimeoutIds.Select(timeoutId =>
            {
                return new Raven3BatchCommand
                {
                    Key = timeoutId,
                    Method = "EVAL",
                    DebugMode = false,
                    Patch = new Patch()
                    {
                        Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationPrefix}' + this.OwningTimeoutManager;",
                        Values = new { }
                    }
                };
            }).Cast<object>().ToList();

            commands.Add(new
            {
                Method = "PUT",
                Key = $"batch/{batch.Number}",
                Document = batch,
                Metadata = new object()
            });

            await PostToBulkDocs(commands);
        }

        public async Task DeleteBatchAndUpdateTimeouts(BatchInfo batch)
        {
            var deleteCommand = GetDeleteCommand($"batch/{batch.Number}");
            var commands = batch.TimeoutIds.Select(timeoutId =>
            {
                return new Raven3BatchCommand
                {
                    Key = timeoutId,
                    Method = "EVAL",
                    DebugMode = false,
                    Patch = new Patch()
                    {
                        Script = $"this.OwningTimeoutManager = this.OwningTimeoutManager.substr({RavenConstants.MigrationPrefix.Length});",
                        Values = new { }
                    }
                };
            }).ToList();
            commands.Add(deleteCommand);

            await PostToBulkDocs(commands);
        }

        public async Task BatchDelete(string[] keys)
        {
            var commands = keys.Select(GetDeleteCommand);
            await PostToBulkDocs(commands);
        }

        public async Task<List<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string prefix,
            CancellationToken cancellationToken, int pageSize = RavenConstants.DefaultPagingSize) where T : class
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
                    var result = await client.GetAsync(getUrl, cancellationToken);

                    if (result.StatusCode == HttpStatusCode.OK)
                    {
                        var pagedTimeouts = await GetDocumentsFromResponse<T>(result.Content);
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

        public async Task<T> GetDocument<T>(string key) where T : class
        {
            var url = $"{serverUrl}/databases/{databaseName}/docs?id={key}";
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync(url);

                if (response.StatusCode == HttpStatusCode.NotFound)
                    return default;

                var contentString = await response.Content.ReadAsStringAsync();
                return JsonConvert.DeserializeObject<T>(contentString);
            }
        }

        private async Task<List<T>> GetDocumentsFromResponse<T>(HttpContent resultContent) where T : class
        {
            var contentString = await resultContent.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<T>>(contentString);
        }

        private async Task PostToBulkDocs(IEnumerable<object> commands)
        {
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                var serializedCommands = JsonConvert.SerializeObject(commands);
                var result = await httpClient.PostAsync(bulkUpdateUrl,
                    new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        private Raven3BatchCommand GetDeleteCommand(string key)
        {
            return new Raven3BatchCommand
            {
                Key = key,
                Method = "DELETE",
                DebugMode = false
            };
        }
    }
}