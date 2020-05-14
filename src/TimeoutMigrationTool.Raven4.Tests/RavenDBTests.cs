using System.Threading;

namespace TimeoutMigrationTool.Raven4.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Net.Http;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using NUnit.Framework;
    using TimeoutMigrationTool.Tests;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenDBTests //: RavenTestDriver
    {
        private const string ServerName = "http://localhost:8080";
        string databaseName;
        int nrOfTimeoutsInStore = 250;

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
                        Destination = "fakeEndpoint",
                        SagaId = Guid.NewGuid(),
                        OwningTimeoutManager = "FakeOwningTimeoutManager",
                        Time = DateTime.Now.AddDays(rnd.Next(1, 30)),
                        Headers = new Dictionary<string, string>(),
                        State = new byte[0]
                    };

                    var serializeObject = JsonConvert.SerializeObject(timeoutData);
                    var httpContent = new StringContent(serializeObject);

                    var result = await httpClient.PutAsync(insertTimeoutUrl, httpContent);
                    Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
                }
            }
        }

        [Test]
        public async Task Foo()
        {
            
            RavenDBTimeoutsReader reader = new RavenDBTimeoutsReader();

            var timeouts = await reader.ReadTimeoutsFrom($"{ServerName}/databases/{databaseName}", CancellationToken.None);
            
            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeoutsInStore));

            await Task.CompletedTask;
        }

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
                Assert.That(killDbResult.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }
        }
    }
}