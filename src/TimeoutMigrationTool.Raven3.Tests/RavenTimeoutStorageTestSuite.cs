namespace TimeoutMigrationTool.Raven3.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public abstract class RavenTimeoutStorageTestSuite
    {
        [SetUp]
        public async Task SetupDatabase()
        {
            var testId = Guid.NewGuid().ToString("N");
            databaseName = $"ravendb-{testId}";
            endpoint = new EndpointInfo
            {
                EndpointName = "A"
            };

            var createDbUrl = $"{ServerName}/admin/databases/{databaseName}";

            // Create the db
            var db = new DatabaseRecord(databaseName);

            var stringContent = new StringContent(JsonConvert.SerializeObject(db));
            var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
            Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
        }

        protected async Task InitTimeouts(int nrOfTimeouts, bool alternateEndpoints = false)
        {
            var timeoutsPrefix = "TimeoutDatas";
            for (var i = 0; i < nrOfTimeouts; i++)
            {
                var insertTimeoutUrl = $"{ServerName}/databases/{databaseName}/docs/{timeoutsPrefix}/{i}";

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

        protected async Task<List<BatchInfo>> SetupExistingBatchInfoInDatabase()
        {
            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await timeoutStorage.PrepareBatchesAndTimeouts(DateTime.Now, endpoint);
            return batches;
        }

        protected ToolState SetupToolState(DateTime cutoffTime, MigrationStatus status = MigrationStatus.NeverRun)
        {
            var runParameters = new Dictionary<string, string>
            {
                {ApplicationOptions.CutoffTime, cutoffTime.ToString()},
                {ApplicationOptions.RavenServerUrl, ServerName},
                {ApplicationOptions.RavenDatabaseName, databaseName},
                {ApplicationOptions.RavenVersion, RavenDbVersion.ThreeDotFive.ToString()},
                {ApplicationOptions.RavenTimeoutPrefix, RavenConstants.DefaultTimeoutPrefix}
            };

            var toolState = new ToolState(runParameters, endpoint)
            {
                Status = status
            };

            return toolState;
        }

        protected async Task SaveToolState(ToolState toolState)
        {
            var bulkInsertUrl = $"{ServerName}/databases/{databaseName}/bulk_docs";
            var bulkCreateBatchAndUpdateTimeoutsCommand = new[]
            {
                new
                {
                    Method = "PUT",
                    Key = RavenConstants.ToolStateId,
                    Document = RavenToolState.FromToolState(toolState),
                    Metadata = new object()
                }
            };

            var serializedCommands = JsonConvert.SerializeObject(bulkCreateBatchAndUpdateTimeoutsCommand);
            var result = await httpClient
                .PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        protected async Task<ToolState> GetToolState()
        {
            var url = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

            var response = await httpClient.GetAsync(url);
            if (response.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }

            var contentString = await response.Content.ReadAsStringAsync();
            var ravenToolState = JsonConvert.DeserializeObject<RavenToolState>(contentString);
            var batches = await GetBatches(ravenToolState.Batches.ToArray());

            return ravenToolState.ToToolState(batches);
        }

        protected async Task<List<BatchInfo>> GetBatches(string[] ids)
        {
            var batches = new List<BatchInfo>();

            foreach (var id in ids)
            {
                var url = $"{ServerName}/databases/{databaseName}/docs?id={id}";
                var response = await httpClient.GetAsync(url);
                var contentString = await response.Content.ReadAsStringAsync();

                var batch = JsonConvert.DeserializeObject<BatchInfo>(contentString);
                batches.Add(batch);
            }

            return batches;
        }

        [TearDown]
        public async Task Teardown()
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

        private Task<HttpResponseMessage> DeleteDatabase()
        {
            var killDb = $"{ServerName}/admin/databases/{databaseName}";
            return httpClient.DeleteAsync(killDb);
        }

        protected string ServerName = (Environment.GetEnvironmentVariable("Raven35Url") ?? "http://localhost:8383").TrimEnd('/');
        protected string databaseName;
        protected EndpointInfo endpoint = new EndpointInfo();
        protected static readonly HttpClient httpClient = new HttpClient();
    }
}