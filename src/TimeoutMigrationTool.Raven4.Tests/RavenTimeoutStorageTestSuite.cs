namespace TimeoutMigrationTool.Raven4.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using TimeoutMigrationTool.Tests;

    public abstract class RavenTimeoutStorageTestSuite
    {
        protected const string ServerName = "http://localhost:8080";
        protected string databaseName;
        protected int nrOfTimeoutsInStore = 1500;
        protected List<string> destinations = new List<string>()
        {
            "A", "B", "C"
        };

        [SetUp]
        public async Task Setup()
        {
            var testId = Guid.NewGuid().ToString("N");
            databaseName = $"ravendb-{testId}";

            var createDbUrl = $"{ServerName}/admin/databases?name={databaseName}";


            using (var httpClient = new HttpClient())
            {
                // Create the db
                var db = new DatabaseRecord
                {
                    Disabled = false,
                    DatabaseName = databaseName
                };

                var stringContent = new StringContent(JsonConvert.SerializeObject(db));
                var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
                Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.Created));

                Random rnd = new Random();
                var timeoutsPrefix = "TimeoutDatas";


                for (var i = 0; i < nrOfTimeoutsInStore; i++)
                {
                    var insertTimeoutUrl = $"{ServerName}/databases/{databaseName}/docs?id={timeoutsPrefix}/{i}";

                    // Insert the timeout data
                    var timeoutData = new TimeoutData
                    {
                        Destination = i <100 ? "A" : i== 100 ? "B" : "C",
                        SagaId = Guid.NewGuid(),
                        OwningTimeoutManager = "FakeOwningTimeoutManager",
                        Time = i < 125 ? DateTime.Now.AddDays(7) : DateTime.Now.AddDays(14),
                        Headers = new Dictionary<string, string>(),
                        State = Encoding.ASCII.GetBytes("This is my state")
                    };

                    var serializeObject = JsonConvert.SerializeObject(timeoutData);
                    var httpContent = new StringContent(serializeObject);

                    var result = await httpClient.PutAsync(insertTimeoutUrl, httpContent);
                    Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
                }
            }
        }

        [TearDown]
        public async Task Teardown()
        {
            var killDb = $"{ServerName}/admin/databases";
            var deleteDb = new DeleteDbParams
            {
                DatabaseNames = new[] {databaseName},
                HardDelete = true
            };
            var httpRequest = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new StringContent(JsonConvert.SerializeObject(deleteDb)),
                RequestUri = new Uri(killDb)
            };

            using (var httpClient = new HttpClient())
            {
                var killDbResult = await httpClient.SendAsync(httpRequest);
                Assert.That(killDbResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            }
        }

        protected async Task<TimeoutData> GetTimeout(string timeoutId)
        {
            var list = await GetTimeouts(new[] { timeoutId });
            return list.SingleOrDefault();
        }

        protected async Task<List<TimeoutData>> GetTimeouts(string[] timeoutIds)
        {
            var timeouts = new List<TimeoutData>();

            foreach (var timeoutId in timeoutIds)
            {
                var url = $"{ServerName}/databases/{databaseName}/docs?id={timeoutId}";
                using (var httpClient = new HttpClient())
                {
                    var response = await httpClient.GetAsync(url);
                    var contentString = await response.Content.ReadAsStringAsync();

                    var jObject = JObject.Parse(contentString);
                    var resultSet = jObject.SelectToken("Results");

                    var timeout = JsonConvert.DeserializeObject<TimeoutData[]>(resultSet.ToString()).SingleOrDefault();
                    timeouts.Add(timeout);
                }
            }

            return timeouts;
        }
    }
}