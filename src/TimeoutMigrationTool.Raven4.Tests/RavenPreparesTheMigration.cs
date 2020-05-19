using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven4.Tests
{
    public class RavenPreparesTheMigration : RavenTimeoutStorageTestSuite
    {

        [Test]
        public async Task WhenGettingTimeoutStateAndNoneIsFoundWeCreateOne()
        {
            using (var httpClient = new HttpClient())
            {
                var getStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";
                var result = await httpClient.GetAsync(getStateUrl);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
            }
            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var toolState = await timeoutStorage.GetOrCreateToolState();

            Assert.That(toolState.IsStoragePrepared, Is.False);
            Assert.IsEmpty(toolState.Batches);
        }

        [Test]
        public async Task WhenGettingTimeoutStateAndOneIsFoundWeReturnIt()
        {
            using (var httpClient = new HttpClient())
            {
                var insertStateUrl = $"{ServerName}/databases/{databaseName}/docs?id={RavenConstants.ToolStateId}";

                // Insert the tool state data
                var toolState = new ToolState()
                {
                    IsStoragePrepared = false
                };

                var serializeObject = JsonConvert.SerializeObject(toolState);
                var httpContent = new StringContent(serializeObject);

                var result = await httpClient.PutAsync(insertStateUrl, httpContent);
                Assert.That(result.StatusCode, Is.EqualTo(HttpStatusCode.Created));
            }

            var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var retrievedToolState = await timeoutStorage.GetOrCreateToolState();

            Assert.That(retrievedToolState.IsStoragePrepared, Is.False);
            Assert.IsEmpty(retrievedToolState.Batches);
        }

        // [Test]
        // public async Task WhenTheStorageHasNotBeenPreparedWeWantToInitBatches()
        // {
        //     var timeoutStorage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
        //     var batches = await timeoutStorage.Prepare();
        // }
    }
}