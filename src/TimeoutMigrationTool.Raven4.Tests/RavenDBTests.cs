using NUnit.Framework;
using Raven.Embedded;
using Raven.TestDriver;
using System;
using System.IO;
using System.Threading.Tasks;

namespace TimeoutMigrationTool.Tests
{
    public class RavenDBTests : RavenTestDriver
    {
        [Test]
        public async Task Foo() 
        {
            var testId = Guid.NewGuid().ToString("N");
            //set up embedded Raven 4
            ConfigureServer(new TestServerOptions
            {
                DataDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, $"test-data-{testId}")
            });

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    //load timeout data into RavenDB
                }

                //extracts them using HTTP and paging
                //"http://{server-name-and-port}/databases/{database-name}/docs?startsWith={timeouts-prefix}&start={integer}&pageSize={integer}"
                //  - timeouts-prefix is tricky as users can customize conventions in Raven and define their own document/collection name
            }

            await Task.CompletedTask;
        }
    }
}
