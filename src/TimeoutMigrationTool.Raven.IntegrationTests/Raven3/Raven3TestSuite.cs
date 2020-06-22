namespace TimeoutMigrationTool.Raven.IntegrationTests.Raven3
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class Raven3TestSuite : IRavenTestSuite
    {
        public ICanTalkToRavenVersion RavenAdapter => new Raven3Adapter(ServerName, DatabaseName);
        public ILogger Logger => new ConsoleLogger(false);

        public string ServerName
        {
            get { return serverName; }
        }

        public async Task SetupDatabase()
        {
            var testId = Guid.NewGuid().ToString("N");
            DatabaseName = $"ravendb-{testId}";
            EndpointName = "A";

            var createDbUrl = $"{serverName}/admin/databases/{DatabaseName}";

            // Create the db
            var db = new DatabaseRecordForRaven3(DatabaseName);

            var stringContent = new StringContent(JsonConvert.SerializeObject(db));
            var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
            Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        public async Task InitTimeouts(int nrOfTimeouts, bool alternateEndpoints = false, string prefixIdsWith = null)
        {
            var timeoutsPrefix = "TimeoutDatas";
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                var idPrefix = string.IsNullOrEmpty(prefixIdsWith) ? "" : prefixIdsWith;
                var insertTimeoutUrl = $"{serverName}/databases/{DatabaseName}/docs/{timeoutsPrefix}/{idPrefix +i}";

                // Insert the timeout data
                var timeoutData = new TimeoutData
                {
                    Id = $"{timeoutsPrefix}/{i}",
                    Destination = "WeDontCare.ThisShouldBeIgnored.BecauseItsJustForRouting",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = "A",
                    Time = i < nrOfTimeouts / 2 ? DateTime.Now.AddDays(7) : DateTime.Now.AddDays(14),
                    Headers = new Dictionary<string, string>(),
                    State = Encoding.ASCII.GetBytes("This is my state")
                };
                if (alternateEndpoints)
                {
                    timeoutData.OwningTimeoutManager = i < (nrOfTimeouts / 3) ? "A" : i < (nrOfTimeouts / 3) * 2 ? "B" : "C";
                }

                var serializeObject = JsonConvert.SerializeObject(timeoutData);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(insertTimeoutUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }

        public async Task<List<RavenBatch>> SetupExistingBatchInfoInDatabase()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(Logger, serverName, DatabaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive, false);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now, EndpointName);
            return batches;
        }

        public RavenToolState SetupToolState(DateTime cutoffTime)
        {
            var runParameters = new Dictionary<string, string>
            {
                {ApplicationOptions.CutoffTime, cutoffTime.ToString()},
                {ApplicationOptions.RavenServerUrl, serverName},
                {ApplicationOptions.RavenDatabaseName, DatabaseName},
                {ApplicationOptions.RavenVersion, RavenDbVersion.ThreeDotFive.ToString()},
                {ApplicationOptions.RavenTimeoutPrefix, RavenConstants.DefaultTimeoutPrefix}
            };

            var batches = new List<RavenBatch>
            {
                new RavenBatch(1, BatchState.Pending, 2)
                {
                   TimeoutIds = new[] {"TimeoutDatas/1", "TimeoutDatas/2"}
                },
                new RavenBatch(2, BatchState.Pending, 2)
                {
                   TimeoutIds = new[] {"TimeoutDatas/3", "TimeoutDatas/4"}
                }
            };

            return new RavenToolState(runParameters, EndpointName, batches);
        }

        public async Task SaveToolState(RavenToolState toolState)
        {
            var bulkInsertUrl = $"{serverName}/databases/{DatabaseName}/bulk_docs";

            var batchInsertCommands = toolState.Batches.Select(b => new
            {
                Method = "PUT",
                Key = $"{RavenConstants.BatchPrefix}/{b.Number}",
                Document = b,
                Metadata = new object()
            }).Cast<object>().ToList();

            batchInsertCommands.Add(
                new
                {
                    Method = "PUT",
                    Key = RavenConstants.ToolStateId,
                    Document = RavenToolStateDto.FromToolState(toolState),
                    Metadata = new object()
                });

            var serializedCommands = JsonConvert.SerializeObject(batchInsertCommands);
            var result = await httpClient
                .PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        public async Task<RavenToolState> GetToolState()
        {
            var url = $"{serverName}/databases/{DatabaseName}/docs?id={RavenConstants.ToolStateId}";

            var response = await httpClient.GetAsync(url);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }

            var contentString = await response.Content.ReadAsStringAsync();
            var ravenToolState = JsonConvert.DeserializeObject<RavenToolStateDto>(contentString);
            var batches = await GetBatches(ravenToolState.Batches.ToArray());

            return ravenToolState.ToToolState(batches);
        }

        public async Task<List<RavenBatch>> GetBatches(string[] ids)
        {
            var batches = new List<RavenBatch>();

            foreach (var id in ids)
            {
                var url = $"{serverName}/databases/{DatabaseName}/docs?id={id}";
                var response = await httpClient.GetAsync(url);
                var contentString = await response.Content.ReadAsStringAsync();

                var batch = JsonConvert.DeserializeObject<RavenBatch>(contentString);
                batches.Add(batch);
            }

            return batches;
        }

        public async Task TeardownDatabase()
        {
            var i = 0;

            while (i < 10)
            {
                try
                {
                    var resp = await DeleteDatabase();
                    resp.EnsureSuccessStatusCode();
                    return;
                }
                catch
                {
                    i++;
                }

                await Task.Delay(TimeSpan.FromSeconds(1));
            }
        }

        public string DatabaseName { get; private set; }

        public RavenDbVersion RavenVersion
        {
            get => RavenDbVersion.ThreeDotFive;
        }

        public string EndpointName { get; set; }

        Task<HttpResponseMessage> DeleteDatabase()
        {
            var killDb = $"{serverName}/admin/databases/{DatabaseName}";
            return httpClient.DeleteAsync(killDb);
        }

        public string serverName = (Environment.GetEnvironmentVariable("Raven35Url") ?? "http://localhost:8383").TrimEnd('/');
        protected static readonly HttpClient httpClient = new HttpClient();

        public async Task CreateLegacyTimeoutManagerIndex()
        {
            var map = "from doc in docs select new {  doc.Time, doc.SagaId }";
            var index = new
            {
                Analyzers = (object)null,
                Fields = new List<object>(),
                Indexes = (object)null,
                InternalFieldsMapping = (object)null,
                IsTestIndex = false,
                IsSideBySideIndex = false,
                IsCompiled = false,
                IsMapReduce = false,
                LockMode = "Unlock",
                Map = map,
                Maps = new List<string> {map},
                Name = RavenConstants.TimeoutIndexName,
                Reduce = (object)null,
                SortOptions = (object)null,
                SpatialIndexes = (object)null,
                Stores = (object)null,
                SuggestionsOptions = new List<object>(),
                TermVectors = (object)null,
                Type = "Map",
                MaxIndexOutputsPerDocument = (object)null
            };

            var createIndexUrl = $"{serverName}/databases/{DatabaseName}/indexes/{RavenConstants.TimeoutIndexName}?definition=yes";
            var content = JsonConvert.SerializeObject(index);
            var result = await httpClient
                .PutAsync(createIndexUrl, new StringContent(content, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }
    }
}