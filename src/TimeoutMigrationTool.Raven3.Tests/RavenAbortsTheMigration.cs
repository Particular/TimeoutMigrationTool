using System;
using System.Linq;
using System.Net.Http;
using System.Reflection.Metadata;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven3.Tests
{
    public class RavenAbortsTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public void WhenThereIsNoStateAbortShouldNotFail()
        {
            Assert.ThrowsAsync<ArgumentNullException>(async () =>
            {
                var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
                await sut.Abort(null);
            });
        }

        [Test]
        public async Task WhenThereIsStateAndNoTimeoutsAbortShouldDeleteState()
        {
            var toolState = SetupToolState(DateTime.Now);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.Abort(toolState);

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenAbortingOnPreparedStorageStateShouldBeCleanedUp()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var toolState = SetupToolState(cutOffTime);
            await SaveToolState(toolState);
            await InitTimeouts(nrOfTimeouts);

            var storage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var batches = await storage.Prepare(cutOffTime);
            toolState.InitBatches(batches);
            await SaveToolState(toolState);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.Abort(toolState);

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        [Ignore("testing http put")]
        public async Task WhenPuttingDataToRaven3()
        {
            using (var httpClient = new HttpClient())
            {
                var bulkInsertUrl = $"{ServerName}/databases/{databaseName}/bulk_docs";
                var bulkCreateBatchAndUpdateTimeoutsCommand = new[]
                {
                    new {
                        Method = "PUT",
                        Key = $"{RavenConstants.BatchPrefix}/1",
                        Document = new BatchInfo
                        {
                            Number = 1,
                            State = BatchState.Pending,
                            TimeoutIds = new []{"TimeoutDatas/1"}
                        },
                        Metadata = new object()
                    }
                };

                var serializedCommands = JsonConvert.SerializeObject(bulkCreateBatchAndUpdateTimeoutsCommand);
                var result = await httpClient
                    .PostAsync(bulkInsertUrl, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInIncompleteBatchesAreReset()
        {
            await InitTimeouts(nrOfTimeouts);
            SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var incompleteBatch = incompleteBatches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new RavenDbReader(ServerName, databaseName, RavenDbVersion.ThreeDotFive);
            var incompleteBatchFromStorage = await ravenDbReader.GetItem<BatchInfo>($"{RavenConstants.BatchPrefix}/{incompleteBatch.Number}");
            var resetTimeouts = await ravenDbReader.GetItems<TimeoutData>(x => incompleteBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", CancellationToken.None);

            Assert.That(incompleteBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => !x.StartsWith(RavenConstants.MigrationPrefix)));
        }

        [Test]
        public async Task WhenCleaningUpBatchesThenTimeoutsInCompleteBatchesAreNotReset()
        {
            await InitTimeouts(nrOfTimeouts);
            SetupToolState(DateTime.Now.AddDays(-1));
            var preparedBatches = await SetupExistingBatchInfoInDatabase();
            var incompleteBatches = preparedBatches.Skip(1).Take(1).ToList();
            var completeBatch = preparedBatches.First();

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            await sut.CleanupExistingBatchesAndResetTimeouts(preparedBatches, incompleteBatches);

            var ravenDbReader = new RavenDbReader(ServerName, databaseName, RavenDbVersion.ThreeDotFive);
            var completeBatchFromStorage = await ravenDbReader.GetItem<BatchInfo>($"{RavenConstants.BatchPrefix}/{completeBatch.Number}");
            var resetTimeouts = await ravenDbReader.GetItems<TimeoutData>(x => completeBatch.TimeoutIds.Contains(x.Id), "TimeoutDatas", CancellationToken.None);

            Assert.That(completeBatchFromStorage, Is.Null);
            Assert.That(resetTimeouts.Select(t => t.OwningTimeoutManager), Is.All.Matches<string>(x => x.StartsWith(RavenConstants.MigrationPrefix)));
        }
    }
}