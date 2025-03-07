namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using HttpCommands;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    public class Raven4Adapter : ICanTalkToRavenVersion
    {
        readonly string serverUrl;
        readonly string databaseName;
        static readonly HttpClient httpClient = new HttpClient();

        public Raven4Adapter(string serverUrl, string databaseName)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
        }

        public async Task UpdateDocument(string key, object document)
        {
            var updateBatchUrl = $"{serverUrl}/databases/{databaseName}/docs?id={Uri.EscapeDataString(key)}";
            var serializeObject = JsonConvert.SerializeObject(document);
            using var httpContent = new StringContent(serializeObject);
            using var saveResult = await httpClient.PutAsync(updateBatchUrl, httpContent);
            saveResult.EnsureSuccessStatusCode();
        }

        public async Task DeleteDocument(string key)
        {
            var deleteStateUrl = $"{serverUrl}/databases/{databaseName}/docs?id={Uri.EscapeDataString(key)}";
            using var result = await httpClient.DeleteAsync(deleteStateUrl);
            result.EnsureSuccessStatusCode();
        }

        static object GetDeleteCommand(string key)
        {
            return new
            {
                Id = key,
                ChangeVector = (object)null,
                Type = "DELETE"
            };
        }

        public async Task CreateBatchAndUpdateTimeouts(RavenBatch batch)
        {
            var insertCommand = new PutCommand
            {
                Id = $"{RavenConstants.BatchPrefix}/{batch.Number}",
                Type = "PUT",
                ChangeVector = null,
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

            var commands = new List<object>
            {
                insertCommand
            };
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task DeleteBatchAndUpdateTimeouts(RavenBatch batch)
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

            var commands = new List<object>
            {
                deleteCommand
            };
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task CompleteBatchAndUpdateTimeouts(RavenBatch batch)
        {
            var insertCommand = new PutCommand
            {
                Id = $"{RavenConstants.BatchPrefix}/{batch.Number}",
                Type = "PUT",
                ChangeVector = null,
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

            var commands = new List<object>
            {
                insertCommand
            };
            commands.AddRange(timeoutUpdateCommands);

            await PostToBulkDocs(commands);
        }

        public async Task ArchiveDocument(string archivedToolStateId, RavenToolStateDto ravenToolState)
        {
            var insertCommand = new
            {
                Id = archivedToolStateId,
                Type = "PUT",
                Document = ravenToolState,
                ChangeVector = (object)null
            };

            var deleteCommand = GetDeleteCommand(RavenConstants.ToolStateId);
            var commands = ravenToolState.Batches.Select(b => GetDeleteCommand(b)).ToList();
            commands.Add(insertCommand);
            commands.Add(deleteCommand);

            await PostToBulkDocs(commands);
        }

        public async Task<GetByIndexResult<T>> GetDocumentsByIndex<T>(int startFrom, TimeSpan timeToWaitForNonStaleResults, Action<T, string> idSetter = null) where T : class
        {
            var indexExists = await DoesTimeoutIndexExist();
            if (!indexExists)
            {
                throw new Exception($"Could not find the TimeoutIndex named '{RavenConstants.TimeoutIndexName}' on the database, unable to continue an index-based migration");
            }

            var url = $"{serverUrl}/databases/{databaseName}/queries?query=from%20index%20%27{RavenConstants.TimeoutIndexName}%27&parameters=%7B%7D&start={startFrom}&pageSize={RavenConstants.DefaultPagingSize}&metadataOnly=false";

            JObject jObject;
            bool isStale;
            var stopWatch = Stopwatch.StartNew();
            try
            {
                do
                {
                    using var result = await httpClient.GetAsync(url);
                    using var content = result.Content;

                    var contentString = await result.Content.ReadAsStringAsync();
                    jObject = JObject.Parse(contentString);
                    isStale = Convert.ToBoolean(jObject.SelectToken("IsStale"));

                    if (isStale)
                    {
                        await Task.Delay(500);
                    }
                } while (isStale && stopWatch.Elapsed < timeToWaitForNonStaleResults);
            }
            finally
            {
                stopWatch.Stop();
            }

            var results = new List<T>();
            var resultSet = jObject.SelectToken("Results");
            var totalNrOfDocuments = Convert.ToInt32(jObject.SelectToken("TotalResults"));
            var indexETag = Convert.ToString(jObject.SelectToken("ResultEtag"));

            foreach (var item in resultSet)
            {
                if (string.IsNullOrEmpty(item.ToString()))
                {
                    throw new Exception("No document found for one of the specified id's");
                }

                var document = JsonConvert.DeserializeObject<T>(item.ToString());
                var id = (string)((dynamic)item)["@metadata"]["@id"];
                idSetter?.Invoke(document, id);
                results.Add(document);
            }

            return new GetByIndexResult<T>
            {
                Documents = results,
                IsStale = isStale,
                NrOfDocuments = totalNrOfDocuments,
                IndexETag = indexETag
            };
        }

        public async Task BatchDelete(string[] keys)
        {
            var commands = keys.Select(GetDeleteCommand);
            await PostToBulkDocs(commands);
        }

        public async Task<IReadOnlyList<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string prefix, Action<T, string> idSetter = null, int pageSize = RavenConstants.DefaultPagingSize) where T : class
        {
            var items = new List<T>();
            var url = $"{serverUrl}/databases/{databaseName}/docs?startsWith={Uri.EscapeDataString(prefix)}&pageSize={pageSize}";
            var checkForMoreResults = true;
            var iteration = 0;

            while (checkForMoreResults)
            {
                var skipFirst = $"&start={iteration * pageSize}";
                var getUrl = iteration == 0 ? url : url + skipFirst;
                using var result = await httpClient.GetAsync(getUrl);

                if (result.StatusCode == HttpStatusCode.OK)
                {
                    var pagedTimeouts = await GetDocumentsFromResponse(result.Content, idSetter);
                    if (pagedTimeouts.Count == 0 || pagedTimeouts.Count < pageSize)
                    {
                        checkForMoreResults = false;
                    }

                    var elegibleItems = pagedTimeouts.Where(filterPredicate);
                    items.AddRange(elegibleItems);
                    iteration++;
                }
            }

            return items;
        }

        public async Task<IReadOnlyList<T>> GetPagedDocuments<T>(string documentPrefix, int startFrom, Action<T, string> idSetter = null, int nrOfPages = 0) where T : class
        {
            var items = new List<T>();
            var url = $"{serverUrl}/databases/{databaseName}/docs?startsWith={Uri.EscapeDataString(documentPrefix)}&pageSize={RavenConstants.DefaultPagingSize}";

            var checkForMoreResults = true;
            var fetchStartFrom = startFrom;
            var iteration = 0;

            while (checkForMoreResults)
            {
                var skipFirst = $"&start={fetchStartFrom}";
                var getUrl = fetchStartFrom == 0 ? url : url + skipFirst;
                using var result = await httpClient.GetAsync(getUrl);

                if (result.StatusCode == HttpStatusCode.OK)
                {
                    var pagedTimeouts = await GetDocumentsFromResponse(result.Content, idSetter);

                    items.AddRange(pagedTimeouts);
                    fetchStartFrom += pagedTimeouts.Count;
                    iteration++;

                    if (iteration == nrOfPages)
                    {
                        checkForMoreResults = false;
                    }
#if NETCOREAPP3_1
                    else if (pagedTimeouts.Count == 0 || pagedTimeouts.Count < RavenConstants.DefaultPagingSize)
#else
                    else if (pagedTimeouts.Count is 0 or < RavenConstants.DefaultPagingSize)
#endif
                    {
                        checkForMoreResults = false;
                    }
                }
            }

            return items;
        }

        public async Task<T> GetDocument<T>(string id, Action<T, string> idSetter = null) where T : class
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new InvalidOperationException("Cannot retrieve a document with empty id");
            }

            var documents = await GetDocuments(new[] { id }, idSetter);
            var document = documents.SingleOrDefault();
            idSetter?.Invoke(document, id);
            return document;
        }

        public async Task<IReadOnlyList<T>> GetDocuments<T>(IEnumerable<string> ids, Action<T, string> idSetter = null) where T : class
        {
            if (!ids.Any())
            {
                return [];
            }

            if (ids.Any(id => string.IsNullOrEmpty(id)))
            {
                throw new InvalidOperationException("Cannot retrieve a document with empty id");
            }

            var url = $"{serverUrl}/databases/{databaseName}/docs?";
            var queryStringIds = ids.Select(id => $"id={Uri.EscapeDataString(id)}").ToList();
            var uris = new List<string>();
            var uriBuilder = new StringBuilder(RavenConstants.MaxUriLength, RavenConstants.MaxUriLength);
            uriBuilder.Append(url);

            while (queryStringIds.Any())
            {
                var idQry = queryStringIds.First();
                var token = $"{idQry}&";

                if (uriBuilder.Length > uriBuilder.MaxCapacity - token.Length)
                {
                    var uri = uriBuilder.ToString().TrimEnd('&');
                    uris.Add(uri);
                    uriBuilder = new StringBuilder(RavenConstants.MaxUriLength, RavenConstants.MaxUriLength);
                    uriBuilder.Append(url);
                }

                uriBuilder.Append(token);
                queryStringIds.Remove(idQry);
            }

            uris.Add(uriBuilder.ToString().TrimEnd('&'));

            var results = new List<T>();
            foreach (var uri in uris)
            {
                using var response = await httpClient.GetAsync(uri);
                if (response.StatusCode == HttpStatusCode.NotFound)
                {
                    continue;
                }

                var resultsFromUri = await GetDocumentsFromResponse(response.Content, idSetter);
                results.AddRange(resultsFromUri);
            }

            return results;
        }

        static async Task<IReadOnlyList<T>> GetDocumentsFromResponse<T>(HttpContent resultContent, Action<T, string> idSetter) where T : class
        {
            var results = new List<T>();

            var contentString = await resultContent.ReadAsStringAsync();
            var jObject = JObject.Parse(contentString);
            var resultSet = jObject.SelectToken("Results");

            foreach (var item in resultSet)
            {
                if (string.IsNullOrEmpty(item.ToString()))
                {
                    throw new Exception("No document found for one of the specified id's");
                }

                var document = JsonConvert.DeserializeObject<T>(item.ToString());
                var id = (string)((dynamic)item)["@metadata"]["@id"];
                idSetter?.Invoke(document, id);
                results.Add(document);
            }

            return results;
        }

        async Task PostToBulkDocs(IEnumerable<object> commands)
        {
            var bulkCommand = new
            {
                Commands = commands.ToArray()
            };

            var bulkUpdateUrl = $"{serverUrl}/databases/{databaseName}/bulk_docs";
            var serializedCommands = JsonConvert.SerializeObject(bulkCommand);

            using var httpContent = new StringContent(serializedCommands, Encoding.UTF8, "application/json");
            using var request = new HttpRequestMessage(HttpMethod.Post, bulkUpdateUrl)
            {
                Version = HttpVersion.Version10,
                Content = httpContent
            };

            using var result = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            result.EnsureSuccessStatusCode();
        }

        async Task<bool> DoesTimeoutIndexExist()
        {
            var indexUrl = $"{serverUrl}/databases/{databaseName}/indexes?namesOnly";
            using var indexResults = await httpClient.GetAsync(indexUrl);
            indexResults.EnsureSuccessStatusCode();

            var indexContentString = await indexResults.Content.ReadAsStringAsync();
            var jObject = JObject.Parse(indexContentString);
            var resultSet = jObject.SelectToken("Results");

            foreach (var item in resultSet)
            {
                var indexName = (string)((dynamic)item)["Name"];
                if (indexName == RavenConstants.TimeoutIndexName)
                {
                    return true;
                }
            }

            return false;
        }
    }
}