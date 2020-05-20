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

        //[Test]
        //public async Task WhenListingEndpoints()
        //{

        //    var reader = new RavenDBTimeoutsReader();
        //    var endpoints = await reader.ListDestinationEndpoints(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive, CancellationToken.None);
        //    Assert.That(endpoints.Length, Is.EqualTo(3));
        //    Assert.That(endpoints.Contains("A"), Is.EqualTo(true));
        //    Assert.That(endpoints.Contains("B"), Is.EqualTo(true));
        //    Assert.That(endpoints.Contains("C"), Is.EqualTo(true));
        //}

        [Test]
        public async Task WhenArchivingTimeouts()
        {
            var writer = new RavenDBTimeoutsArchiver();
            await writer.ArchiveTimeouts(ServerName, databaseName, new[] { "TimeoutDatas/5" }, RavenDbVersion.ThreeDotFive, CancellationToken.None);
        }

        // ravendb-d2c02b94530943c587b3108113797a5e
    }
}