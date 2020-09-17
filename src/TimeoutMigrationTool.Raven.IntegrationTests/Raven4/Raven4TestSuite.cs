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
        static Random random = new Random();

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
            var dbCreationResult = await HttpClient.PutAsync(createDbUrl, stringContent);
            Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.Created));
        }

        public async Task InitTimeouts(int nrOfTimeouts)
        {
            var timeoutsPrefix = "TimeoutDatas";
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                var insertTimeoutUrl = $"{ServerName}/databases/{DatabaseName}/docs?id={timeoutsPrefix}/{i}";

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
                
                var serializeObject = JsonConvert.SerializeObject(timeoutData);
                var httpContent = new StringContent(serializeObject);

                var result = await HttpClient.PutAsync(insertTimeoutUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }
        
        public async Task<InitiTimeoutsResult> InitTimeouts(int nrOfTimeouts, string endpointName, int startFromId)
        {
            var timeoutsPrefix = "TimeoutDatas";
            var shortestTimeout = DateTime.MaxValue;
            var longestTimeout = DateTime.MinValue;
            var daysToTrigger = random.Next(2, 60); // randomize the Time property
            
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                var insertTimeoutUrl = $"{ServerName}/databases/{DatabaseName}/docs?id={timeoutsPrefix}/{startFromId + i}";

                // Insert the timeout data
                var timeoutData = new TimeoutData
                {
                    Destination = "WeDontCare.ThisShouldBeIgnored.BecauseItsJustForRouting",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = endpointName,
                    Time = DateTime.Now.AddDays(daysToTrigger),
                    Headers = new Dictionary<string, string>(),
                    State = Encoding.ASCII.GetBytes("This is my state")
                };
                
                var serializeObject = JsonConvert.SerializeObject(timeoutData);
                var httpContent = new StringContent(serializeObject);

                var result = await HttpClient.PutAsync(insertTimeoutUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
                if (shortestTimeout > timeoutData.Time) shortestTimeout = timeoutData.Time;
                if (longestTimeout < timeoutData.Time) longestTimeout = timeoutData.Time;
            }
            
            return new InitiTimeoutsResult               
            {
                ShortestTimeout = shortestTimeout,
                LongestTimeout = longestTimeout
            };
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
            var result = await HttpClient.PostAsync(bulkInsertUrl, new StringContent(serializeObject, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        public async Task<RavenToolState> GetToolState()
        {
            var url = $"{ServerName}/databases/{DatabaseName}/docs?id={RavenConstants.ToolStateId}";
            var response = await HttpClient.GetAsync(url);

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
                var response = await HttpClient.GetAsync(url);
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

            var killDbResult = await HttpClient.SendAsync(httpRequest);
            Assert.That(killDbResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        public async Task CreateLegacyTimeoutManagerIndex(bool waitForIndexToBeUpToDate)
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
            var result = await HttpClient
                .PutAsync(createIndexUrl, new StringContent(content, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();

            if (waitForIndexToBeUpToDate)
            {
                await EnsureIndexIsNotStale();
            }
        }

        public string DatabaseName { get; private set; }

        public RavenDbVersion RavenVersion
        {
            get => RavenDbVersion.Four;
        }

        public string EndpointName { get; set; }
        protected static readonly HttpClient HttpClient = new HttpClient();

        public async Task EnsureIndexIsNotStale()
        {
            var isIndexStale = true;
            while (isIndexStale)
            {
                var url = $"{ServerName}/databases/{DatabaseName}/queries?query=from%20index%20%27{RavenConstants.TimeoutIndexName}%27&parameters=%7B%7D&start={0}&pageSize={1}&metadataOnly=true";
                using var result = await HttpClient
                    .GetAsync(url);
                var contentString = await result.Content.ReadAsStringAsync();
                var jObject = JObject.Parse(contentString);
                isIndexStale = Convert.ToBoolean(jObject.SelectToken("IsStale"));
                if (isIndexStale) await Task.Delay(TimeSpan.FromMilliseconds(500));
            }
        }
    }
}