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

    public class Raven4Adapter : ICanTalkToRavenVersion
    {
        readonly string serverUrl;
        readonly string databaseName;

        public Raven4Adapter(string serverUrl, string databaseName)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
        }

        public async Task UpdateRecord(string key, object document)
        {
            using (var httpClient = new HttpClient())
            {
                var updateBatchUrl = $"{serverUrl}/databases/{databaseName}/docs?id={key}";
                var serializeObject = JsonConvert.SerializeObject(document);
                var httpContent = new StringContent(serializeObject);

                var saveResult = await httpClient.PutAsync(updateBatchUrl, httpContent);
                saveResult.EnsureSuccessStatusCode();
            }
        }

        public async Task DeleteRecord(string key)
        {
            using (var httpClient = new HttpClient())
            {
                var deleteStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={key}";
                var result = await httpClient.DeleteAsync(deleteStateUrl);
                result.EnsureSuccessStatusCode();
            }
        }

        private static object GetDeleteCommand(string key)
        {
            return new
            {
                Id = key,
                ChangeVector = (object) null,
                Type = "DELETE"
            };
        }

        public async Task CreateBatchAndUpdateTimeouts(BatchInfo batch)
        {
            var insertCommand = new PutCommand
            {
                Id = $"{RavenConstants.BatchPrefix}/{batch.Number}",
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
                    Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationOngoingPrefix}' + this.OwningTimeoutManager;",
                    Values = new { }
                }
            }).ToList();

            List<object> commands = new List<object>();
            commands.Add(insertCommand);
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task DeleteBatchAndUpdateTimeouts(BatchInfo batch)
        {
            var deleteCommand = GetDeleteCommand($"{RavenConstants.BatchPrefix}/{batch.Number}");
            var timeoutUpdateCommands = batch.TimeoutIds.Select(timeoutId => new PatchCommand
            {
                Id = timeoutId,
                Type = "PATCH",
                ChangeVector = null,
                Patch = new Patch()
                {
                    Script = $"this.OwningTimeoutManager = this.OwningTimeoutManager.substr({RavenConstants.MigrationOngoingPrefix.Length});",
                    Values = new { }
                }
            }).ToList();

            List<object> commands = new List<object>();
            commands.Add(deleteCommand);
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task CompleteBatchAndUpdateTimeouts(BatchInfo batch)
        {
            var updateCommand = new PutCommand
            {
                Id = $"{RavenConstants.BatchPrefix}/{batch.Number}",
                Type = "PUT",
                ChangeVector = (object) null,
                Document = batch
            };
            var timeoutUpdateCommands = batch.TimeoutIds.Select(timeoutId => new PatchCommand
            {
                Id = timeoutId,
                Type = "PATCH",
                ChangeVector = null,
                Patch = new Patch()
                {
                    Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationDonePrefix}' + this.OwningTimeoutManager.substr({RavenConstants.MigrationOngoingPrefix.Length});",
                    Values = new { }
                }
            }).ToList();

            List<object> commands = new List<object>();
            commands.Add(updateCommand);
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task BatchDelete(string[] keys)
        {
            var commands = keys.Select(GetDeleteCommand);
            await PostToBulkDocs(commands);
        }

        public async Task<List<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string prefix, Action<T, string> idSetter, int pageSize = RavenConstants.DefaultPagingSize) where T : class
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
                    var result = await client.GetAsync(getUrl);

                    if (result.StatusCode == HttpStatusCode.OK)
                    {
                        var pagedTimeouts = await GetDocumentsFromResponse<T>(result.Content, idSetter);
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

        public async Task<T> GetDocument<T>(string id, Action<T, string> idSetter) where T : class
        {
            var documents = await GetDocuments<T>(new[] { id }, idSetter);
            return documents.SingleOrDefault();
        }

        public async Task<List<T>> GetDocuments<T>(IEnumerable<string> ids, Action<T, string> idSetter) where T : class
        {
            if (!ids.Any())
            {
                return new List<T>();
            }

            var qs = ids.Aggregate("", (queryString, id) => queryString + $"id={id}&", queryString=>queryString.TrimEnd('&'));
            var url = $"{serverUrl}/databases/{databaseName}/docs?{qs}";
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.GetAsync(url);

                if (response.StatusCode == HttpStatusCode.NotFound)
                    return new List<T>();

                var results = await GetDocumentsFromResponse<T>(response.Content, idSetter);

                return results;
            }
        }

        private async Task<List<T>> GetDocumentsFromResponse<T>(HttpContent resultContent, Action<T, string> idSetter) where T : class
        {
            var results = new List<T>();

            var contentString = await resultContent.ReadAsStringAsync();
            var jObject = JObject.Parse(contentString);
            var resultSet = jObject.SelectToken("Results");

            foreach (var item in resultSet)
            {
                var document = JsonConvert.DeserializeObject<T>(item.ToString());
                var id = (string)((dynamic)item)["@metadata"]["@id"];
                idSetter(document, id);
                results.Add(document);
            }

            return results;
        }

        private async Task PostToBulkDocs(IEnumerable<object> commands)
        {
            var bulkCommand = new
            {
                Commands = commands.ToArray()
            };
            using (var httpClient = new HttpClient())
            {
                var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
                var serializedCommands = JsonConvert.SerializeObject(bulkCommand);
                var result = await httpClient.PostAsync(bulkUpdateUrl,
                    new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }
    }
}