using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using TimeoutMigrationTool.Raven4.Tests;

namespace TimeoutMigrationTool.Tests
{
    public class RavenDBTests //: RavenTestDriver
    {
        [Test]
        public async Task Foo() 
        {
            var testId = Guid.NewGuid().ToString("N");

            var servername = "http://localhost:8080";
            var databaseName = $"ravendb-{testId}";
            string timeoutsPrefix = "TimeoutDatas";
            int timeoutId = 1;
            var url =
                $"{servername}/databases/{databaseName}/docs?id={timeoutsPrefix}/{timeoutId}";

            var createDbUrl = $"{servername}/admin/databases?name={databaseName}";
            var killDb = $"{servername}/admin/databases";

            using (HttpClient httpClient = new HttpClient())
            {
                var db = new DatabaseRecord
                {
                    Disabled = false,
                    DatabaseName = databaseName
                };

                var stringContent = new StringContent(JsonConvert.SerializeObject(db));
                var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
                Assert.That(dbCreationResult.StatusCode, Is.EqualTo(HttpStatusCode.Created));
                var timeoutData = new TimeoutData()
                {
                    Destination = "fakeEndpoint",
                    SagaId = Guid.NewGuid(),
                    OwningTimeoutManager = "FakeOwningTimeoutManager",
                    Time = DateTime.Now.AddDays(30),
                    Headers = new Dictionary<string, string>(),
                    State = new byte[0]
                };

                var serializeObject = JsonConvert.SerializeObject(timeoutData);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(url, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));

                var deleteDb = new DeleteDbParams()
                {
                    DatabaseNames = new[] {databaseName},
                    HardDelete = true
                };
                var httpRequest = new HttpRequestMessage()
                {
                    Method = HttpMethod.Delete,
                    Content = new StringContent(JsonConvert.SerializeObject(deleteDb)),
                    RequestUri = new Uri( killDb)
                };
                var killDbResult = await httpClient.SendAsync(httpRequest);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }


            //set up embedded Raven 4
            // ConfigureServer(new TestServerOptions
            // {
            //     DataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"test-data-{testId}")
            // });
            //
            // using (var store = GetDocumentStore())
            // {
            //     using (var session = store.OpenAsyncSession())
            //     {
            //         //load timeout data into RavenDB
            //     }
            //
            //     //extracts them using HTTP and paging
            //     //"http://{server-name-and-port}/databases/{database-name}/docs?startsWith={timeouts-prefix}&start={integer}&pageSize={integer}"
            //     //  - timeouts-prefix is tricky as users can customize conventions in Raven and define their own document/collection name
            //     
            //     
            // }

            await Task.CompletedTask;
        }
    }

    public class DatabaseRecord
    {
        public string DatabaseName { get; set; }
        public bool Disabled { get; set; }
        
    }

    public class DeleteDbParams
    {
        public String[]  DatabaseNames { get; set; }
        public bool HardDelete { get; set; }
        public TimeSpan TimeToWaitForConfirmation { get; set; } = TimeSpan.FromSeconds(10);
    }
}
