using System;
using System.Threading;
using System.Threading.Tasks;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven3.Tests
{
    public class RavenDBTests : RavenTimeoutStorageTestSuite
    {
        private int nrOfTimeouts = 250;

        [SetUp]
        public async Task Setup()
        {
            await InitTimeouts(nrOfTimeouts);
        }

        [Test]
        public async Task WhenReadingTimeouts()
        {
            var reader = new RavenDbReader(ServerName, databaseName, RavenDbVersion.ThreeDotFive);
            var timeouts = await reader.GetItems<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(-1), "TimeoutDatas", CancellationToken.None);

            Assert.That(timeouts.Count, Is.EqualTo(250));
        }

        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var reader = new RavenDbReader(ServerName, databaseName, RavenDbVersion.ThreeDotFive);
            var timeouts = await reader.GetItems<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(10), "TimeoutDatas", CancellationToken.None);

            Assert.That(timeouts.Count, Is.EqualTo(125));
        }

        [Test]
        public async Task WhenArchivingTimeouts()
        {
            var writer = new RavenDBTimeoutsArchiver();
            await writer.ArchiveTimeouts(ServerName, databaseName, new[] { "TimeoutDatas/5" }, RavenDbVersion.ThreeDotFive, CancellationToken.None);
        }
    }
}