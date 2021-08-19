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
    using Newtonsoft.Json.Linq;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

    public class Raven3TestSuite : IRavenTestSuite
    {
        public ICanTalkToRavenVersion RavenAdapter => new Raven3Adapter(ServerName, DatabaseName);
        public ILogger Logger => new ConsoleLogger(false);

        public string ServerName => serverName;

        static Random random = new Random();

        public async Task SetupDatabase()
        {
            var testId = Guid.NewGuid().ToString("N");
            DatabaseName = $"ravendb-{testId}";
            EndpointName = "A";

            var createDbUrl = $"{serverName}/admin/databases/{DatabaseName}";

            // Create the db
            var db = new DatabaseRecordForRaven3(DatabaseName);

            var stringContent = new StringContent(JsonConvert.SerializeObject(db));
            var dbCreationResult = await HttpClient.PutAsync(createDbUrl, stringContent);
            Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        public async Task InitTimeouts(int nrOfTimeouts)
        {
            var commands = new List<object>();
            var bulkInsertUrl = $"{ServerName}/databases/{DatabaseName}/bulk_docs";
            var timeoutsPrefix = "TimeoutDatas";
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                // Insert the timeout data
                var timeoutData = new TimeoutData
                {
                    Destination = "WeDontCare.ThisShouldBeIgnored.BecauseItsJustForRouting",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = "A",
                    Time = i < nrOfTimeouts / 2 ? DateTimeOffset.UtcNow.AddDays(7) : DateTimeOffset.UtcNow.AddDays(14),
                    Headers = new Dictionary<string, string>(),
                    State = Encoding.ASCII.GetBytes("This is my state")
                };

                commands.Add(new
                {
                    Document = timeoutData,
                    Method = "PUT",
                    Key = $"{timeoutsPrefix}/{i}",
                    Metadata = new object()
                });
            }

            var serializeObject = JsonConvert.SerializeObject(commands);
            using var stringContent = new StringContent(serializeObject, Encoding.UTF8, "application/json");
            using var result = await HttpClient.PostAsync(bulkInsertUrl, stringContent);

            Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        public async Task<InitiTimeoutsResult> InitTimeouts(int nrOfTimeouts, string endpointName, int startFromId)
        {
            var timeoutsPrefix = "TimeoutDatas";
            var shortestTimeout = DateTimeOffset.MaxValue;
            var longestTimeout = DateTimeOffset.MinValue;
            var daysToTrigger = random.Next(2, 60); // randomize the Time property

            var commands = new List<object>();
            var bulkInsertUrl = $"{ServerName}/databases/{DatabaseName}/bulk_docs";

            for (var i = 0; i < nrOfTimeouts; i++)
            {
                // Insert the timeout data
                var timeoutData = new TimeoutData
                {
                    Destination = "WeDontCare.ThisShouldBeIgnored.BecauseItsJustForRouting",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = endpointName,
                    Time = DateTimeOffset.UtcNow.AddDays(daysToTrigger),
                    Headers = new Dictionary<string, string>(),
                    State = Encoding.ASCII.GetBytes("This is my state")
                };

                commands.Add(new
                {
                    Document = timeoutData,
                    Metadata = new object(),
                    Method = "PUT",
                    Key = $"{timeoutsPrefix}/{startFromId + i}"
                });

                if (shortestTimeout > timeoutData.Time)
                {
                    shortestTimeout = timeoutData.Time;
                }

                if (longestTimeout < timeoutData.Time)
                {
                    longestTimeout = timeoutData.Time;
                }
            }

            var serializeObject = JsonConvert.SerializeObject(commands);
            using var stringContent = new StringContent(serializeObject, Encoding.UTF8, "application/json");
            using var result = await HttpClient.PostAsync(bulkInsertUrl, stringContent);

            Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.OK));

            return new InitiTimeoutsResult
            {
                ShortestTimeout = shortestTimeout,
                LongestTimeout = longestTimeout
            };
        }

        public async Task<List<RavenBatch>> SetupExistingBatchInfoInDatabase()
        {
            var timeoutStorage = new RavenDbTimeoutsSource(Logger, serverName, DatabaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive, false);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTimeOffset.UtcNow, EndpointName);
            return batches;
        }

        public RavenToolState SetupToolState(DateTimeOffset cutoffTime)
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

            return new RavenToolState(runParameters, EndpointName, batches, MigrationStatus.StoragePrepared);
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
                    Document = this.FromToolState(toolState),
                    Metadata = new object()
                });

            var serializedCommands = JsonConvert.SerializeObject(batchInsertCommands);
            var result = await HttpClient
                .PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        public async Task<RavenToolState> GetToolState()
        {
            var url = $"{serverName}/databases/{DatabaseName}/docs?id={RavenConstants.ToolStateId}";

            var response = await HttpClient.GetAsync(url);
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
                var response = await HttpClient.GetAsync(url);
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

        public RavenDbVersion RavenVersion => RavenDbVersion.ThreeDotFive;

        public string EndpointName { get; set; }

        Task<HttpResponseMessage> DeleteDatabase()
        {
            var killDb = $"{serverName}/admin/databases/{DatabaseName}";
            return HttpClient.DeleteAsync(killDb);
        }

        public string serverName = (Environment.GetEnvironmentVariable("Raven35Url") ?? "http://localhost:8383").TrimEnd('/');
        protected static readonly HttpClient HttpClient = new HttpClient();

        public async Task CreateLegacyTimeoutManagerIndex(bool waitForIndexToBeUpToDate)
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
                Maps = new List<string> { map },
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
            using var result = await HttpClient
                .PutAsync(createIndexUrl, new StringContent(content, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();

            if (waitForIndexToBeUpToDate)
            {
                await EnsureIndexIsNotStale();
            }
        }

        public async Task EnsureIndexIsNotStale()
        {
            var isIndexStale = true;
            while (isIndexStale)
            {
                var url = $"{serverName}/databases/{DatabaseName}/indexes/{RavenConstants.TimeoutIndexName}?start={0}&pageSize={1}";
                using var result = await HttpClient
                    .GetAsync(url);
                var contentString = await result.Content.ReadAsStringAsync();
                var jObject = JObject.Parse(contentString);
                isIndexStale = Convert.ToBoolean(jObject.SelectToken("IsStale"));
                if (isIndexStale)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(500));
                }
            }
        }
    }
}