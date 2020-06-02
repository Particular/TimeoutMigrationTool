namespace TimeoutMigrationTool.Raven4.Tests
{
    using System;
    using System.Linq;
    using System.Reflection.Metadata;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

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
            var reader = new Raven4Adapter(ServerName, databaseName);
            var timeouts = await reader.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(-1), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeouts));
        }

        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var reader = new Raven4Adapter(ServerName, databaseName);
            var timeouts = await reader.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(10), "TimeoutDatas", (doc, id) => doc.Id = id);

            foreach (var timeout in timeouts)
            {
                Assert.That(timeout.Id, Is.Not.Null);
            }
            Assert.That(timeouts.Count, Is.EqualTo(125));
        }

        [Test]
        public async Task WhenArchivingOneTimeout()
        {
            var timeoutId = "TimeoutDatas/5";
            var original = await GetTimeout(timeoutId);

            var writer = new RavenDBTimeoutsArchiver();
            await writer.ArchiveTimeouts(ServerName, databaseName, new[] { timeoutId }, RavenDbVersion.Four, CancellationToken.None);

            var archived = await GetTimeout(timeoutId);

            Assert.That(original.OwningTimeoutManager, Is.Not.EqualTo(archived.OwningTimeoutManager));
            Assert.IsTrue(archived.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix, StringComparison.OrdinalIgnoreCase));
        }

        [Test]
        public async Task WhenArchivingMultipleTimeouts()
        {
            var timeoutIds = new[] { "TimeoutDatas/5", "TimeoutDatas/125", "TimeoutDatas/197" };

            var writer = new RavenDBTimeoutsArchiver();
            await writer.ArchiveTimeouts(ServerName, databaseName, timeoutIds, RavenDbVersion.Four, CancellationToken.None);

            var archived = await GetTimeouts(timeoutIds);

            Assert.That(archived.Count, Is.EqualTo(3));
            Assert.That(archived.All(a => a.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix, StringComparison.OrdinalIgnoreCase)));
            Assert.That(archived.All(a => a.OwningTimeoutManager.StartsWith(RavenConstants.MigrationOngoingPrefix, StringComparison.OrdinalIgnoreCase)));
        }
    }
}