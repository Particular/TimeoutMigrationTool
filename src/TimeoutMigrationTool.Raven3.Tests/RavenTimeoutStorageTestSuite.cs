using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Raven3.Tests
{
    public abstract class RavenTimeoutStorageTestSuite
    {
        protected const string ServerName = "http://localhost:8383";
        protected string databaseName;

        [SetUp]
        public async Task SetupDatabase()
        {
            var testId = Guid.NewGuid().ToString("N");
            databaseName = $"ravendb-{testId}";

            var createDbUrl = $"{ServerName}/admin/databases/{databaseName}";

            using (var httpClient = new HttpClient())
            {
                // Create the db
                var db = new DatabaseRecord(databaseName);

                var stringContent = new StringContent(JsonConvert.SerializeObject(db));
                var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
                Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.OK));
            }
        }

        protected async Task InitTimeouts(int nrOfTimeouts)
        {
           using (var httpClient = new HttpClient())
            {
                var timeoutsPrefix = "TimeoutDatas";
                for (var i = 0; i < nrOfTimeouts; i++)
                {
                    var insertTimeoutUrl = $"{ServerName}/databases/{databaseName}/docs/{timeoutsPrefix}/{i}";

                    // Insert the timeout data
                    var timeoutData = new TimeoutData
                    {
                        Id = $"{timeoutsPrefix}/{i}",
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
            int i = 0;

            while (i<10)
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
            
            var httpRequest = new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                //Content = new StringContent(JsonConvert.SerializeObject(deleteDb)),
                RequestUri = new Uri(killDb)
            };

            using (var httpClient = new HttpClient())
            {
                //var killDbResult = await httpClient.SendAsync(httpRequest);
                return httpClient.DeleteAsync(killDb);
            }
        }
    }
}