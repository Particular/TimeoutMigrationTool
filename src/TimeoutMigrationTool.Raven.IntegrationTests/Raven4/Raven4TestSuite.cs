namespace TimeoutMigrationTool.Raven.IntegrationTests.Raven4
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
    using Newtonsoft.Json.Linq;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

    class Raven4TestSuite : IRavenTestSuite
    {
        public ICanTalkToRavenVersion RavenAdapter => new Raven4Adapter(ServerName, DatabaseName);
        public ILogger Logger => new ConsoleLogger(false);

        public string ServerName
        {
            get
            {
                var ravenUrls = Environment.GetEnvironmentVariable("CommaSeparatedRavenClusterUrls");

                if (string.IsNullOrEmpty(ravenUrls))
                {
                    return "http://localhost:8080";
                }

                return ravenUrls.Split(",").First().TrimEnd('/');
            }
        }

        public async Task SetupDatabase()
        {
            var testId = Guid.NewGuid().ToString("N");
            DatabaseName = $"ravendb-{testId}";
            EndpointName = "A";

            var createDbUrl = $"{ServerName}/admin/databases?name={DatabaseName}";

            // Create the db
            var db = new DatabaseRecordForRaven4
            {
                Disabled = false,
                DatabaseName = DatabaseName
            };

            var stringContent = new StringContent(JsonConvert.SerializeObject(db));
            var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
            Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.Created));
        }

        public async Task InitTimeouts(int nrOfTimeouts, bool alternateEndpoints = false, string prefixIdsWith = null)
        {
            var timeoutsPrefix = "TimeoutDatas";
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                var idPrefix = string.IsNullOrEmpty(prefixIdsWith) ? "" : prefixIdsWith;
                var insertTimeoutUrl = $"{ServerName}/databases/{DatabaseName}/docs?id={timeoutsPrefix}/{idPrefix + i}";

                // Insert the timeout data
                var timeoutData = new TimeoutData
                {
                    Destination = "WeDontCare.ThisShouldBeIgnored.BecauseItsJustForRouting",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = "A",
                    Time = i < nrOfTimeouts / 2 ? DateTime.Now.AddDays(7) : DateTime.Now.AddDays(14),
                    Headers = new Dictionary<string, string>(),
                    State = Encoding.ASCII.GetBytes("This is my state")
                };
                if (alternateEndpoints)
                {
                    timeoutData.OwningTimeoutManager = i < (nrOfTimeouts / 2) ? "A" : "B";
                }

                var serializeObject = JsonConvert.SerializeObject(timeoutData);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(insertTimeoutUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }

        public RavenToolState SetupToolState(DateTime cutoffTime)
        {
            var runParameters = new Dictionary<string, string>
            {
                {ApplicationOptions.CutoffTime, cutoffTime.ToString()},
                {ApplicationOptions.RavenServerUrl, ServerName},
                {ApplicationOptions.RavenDatabaseName, DatabaseName},
                {ApplicationOptions.RavenVersion, RavenDbVersion.Four.ToString()},
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

        public async Task<List<RavenBatch>> SetupExistingBatchInfoInDatabase()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(Logger, ServerName, DatabaseName, "TimeoutDatas", RavenDbVersion.Four, false);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now, EndpointName);
            return batches;
        }

        public async Task SaveToolState(RavenToolState toolState)
        {
            var bulkInsertUrl = $"{ServerName}/databases/{DatabaseName}/bulk_docs";

            var inserts = toolState.Batches.Select(batch => new PutCommand
            {
                Id = $"{RavenConstants.BatchPrefix}/{batch.Number}",
                Type = "PUT",
                ChangeVector = null,
                Document = batch
            }).ToList();

            inserts.Add(new PutCommand
            {
                Id = $"{RavenConstants.ToolStateId}",
                Type = "PUT",
                ChangeVector = null,
                Document = RavenToolStateDto.FromToolState(toolState)
            });

            var request = new
            {
                Commands = inserts.ToArray()
            };

            var serializeObject = JsonConvert.SerializeObject(request);
            var result = await httpClient.PostAsync(bulkInsertUrl, new StringContent(serializeObject, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        public async Task<RavenToolState> GetToolState()
        {
            var url = $"{ServerName}/databases/{DatabaseName}/docs?id={RavenConstants.ToolStateId}";
            var response = await httpClient.GetAsync(url);

            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }

            var contentString = await response.Content.ReadAsStringAsync();

            var jObject = JObject.Parse(contentString);
            var resultSet = jObject.SelectToken("Results");

            var ravenToolState = JsonConvert.DeserializeObject<RavenToolStateDto[]>(resultSet.ToString()).Single();
            var batches = await GetBatches(ravenToolState.Batches.ToArray());

            return ravenToolState.ToToolState(batches);
        }

        public async Task<List<RavenBatch>> GetBatches(string[] ids)
        {
            var batches = new List<RavenBatch>();

            foreach (var id in ids)
            {
                var url = $"{ServerName}/databases/{DatabaseName}/docs?id={id}";
                var response = await httpClient.GetAsync(url);
                var contentString = await response.Content.ReadAsStringAsync();

                var jObject = JObject.Parse(contentString);
                var resultSet = jObject.SelectToken("Results");

                var timeout = JsonConvert.DeserializeObject<RavenBatch[]>(resultSet.ToString()).SingleOrDefault();
                batches.Add(timeout);
            }

            return batches;
        }

        public async Task TeardownDatabase()
        {
            var killDb = $"{ServerName}/admin/databases";
            var deleteDb = new DeleteDbParamsForRaven4
            {
                DatabaseNames = new[] {DatabaseName},
                HardDelete = true
            };
            var httpRequest = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new StringContent(JsonConvert.SerializeObject(deleteDb)),
                RequestUri = new Uri(killDb)
            };

            var killDbResult = await httpClient.SendAsync(httpRequest);
            Assert.That(killDbResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        public async Task CreateLegacyTimeoutManagerIndex()
        {
            var map = "from doc in docs select new {  doc.Time, doc.SagaId }";
            var index = new
            {
                Name = RavenConstants.TimeoutIndexName,
                Maps = new List<string> {map},
                Type = "Map",
                LockMode = "Unlock",
                Priority = "Normal",
                Configuration = new object(),
                Fields = new object(),
                OutputReduceToCollection = new object(),
                PatternForOutputReduceToCollectionReferences = new object(),
                PatternReferencesCollectionNam = new object(),
                AdditionalSources = new object()
            };

            var indexes = new
            {
                Indexes = new List<object> {index}
            };

            var createIndexUrl = $"{ServerName}/databases/{DatabaseName}/admin/indexes";
            var content = JsonConvert.SerializeObject(indexes);
            var result = await httpClient
                .PutAsync(createIndexUrl, new StringContent(content, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();

            await EnsureIndexIsNotStale();
        }

        public string DatabaseName { get; private set; }

        public RavenDbVersion RavenVersion
        {
            get => RavenDbVersion.Four;
        }

        public string EndpointName { get; set; }
        protected static readonly HttpClient httpClient = new HttpClient();

        static async Task EnsureIndexIsNotStale()
        {
            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }
}