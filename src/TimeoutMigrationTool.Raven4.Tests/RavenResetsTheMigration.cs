using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven4.Tests
{
    public class RavenResetsTheMigration : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public async Task WhenThereIsNoStateResetShouldNotFail()
        {
            await InitTimeouts(nrOfTimeouts);

            Assert.DoesNotThrowAsync(async () =>
            {
                var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
                await sut.Reset();
            });
        }

        [Test]
        public void WhenThereIsNoStateAndNoTimeoutsResetShouldNotFail()
        {
            Assert.DoesNotThrowAsync(async () =>
            {
                var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
                await sut.Reset();
            });
        }

        [Test]
        public async Task WhenThereIsStateAndNoTimeoutsResetShouldDeleteState()
        {
            await SaveToolState(SetupToolState(DateTime.Now)).ConfigureAwait(false);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.Reset();

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }

        [Test]
        public async Task WhenResetOnPreparedStorageStateShouldBeReset()
        {
            var cutOffTime = DateTime.Now.AddDays(-1);
            var toolState = SetupToolState(cutOffTime);
            await SaveToolState(toolState).ConfigureAwait(false);
            await InitTimeouts(nrOfTimeouts);

            var storage = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var batches = await storage.Prepare(cutOffTime);
            toolState.InitBatches(batches);
            await SaveToolState(toolState);
            var batchesToReset = batches.Skip(1);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            await sut.Reset();

            var storedSate = await GetToolState();
            Assert.That(storedSate, Is.Null);
        }
    }
}