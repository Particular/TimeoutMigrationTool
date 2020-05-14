using System;
using System.Linq;

namespace TimeoutMigrationTool.Raven4.Tests
{
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.RavenDB;
    
    public class RavenDBTests : RavenTimeoutStorageTestSuite
    {
        [Test]
        public async Task WhenReadingTimeouts()
        {
            var reader = new RavenDBTimeoutsReader();

            var timeouts =
                await reader.ReadTimeoutsFrom(ServerName, databaseName, "TimeoutDatas", DateTime.Now.AddDays(-1), CancellationToken.None);

            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeoutsInStore));
        }
        
        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var reader = new RavenDBTimeoutsReader();
        
            var timeouts =
                await reader.ReadTimeoutsFrom(ServerName, databaseName, "TimeoutDatas", DateTime.Now.AddDays(10), CancellationToken.None);
        
            Assert.That(timeouts.Count, Is.EqualTo(125));
        }

        [Test]
        public async Task WhenListingEndpoints()
        {
            
            var reader = new RavenDBTimeoutsReader();
            var endpoints = await reader.ListDestinationEndpoints(ServerName, databaseName, "TimeoutDatas", CancellationToken.None);
            Assert.That(endpoints.Length, Is.EqualTo(3));
            Assert.That(endpoints.Contains("A"), Is.EqualTo(true));
            Assert.That(endpoints.Contains("B"), Is.EqualTo(true));
            Assert.That(endpoints.Contains("C"), Is.EqualTo(true));
        }

        [Test]
        public async Task WhenArchivingTimeouts()
        {
            var writer = new RavenDBTimeoutsArchiver();
            await writer.ArchiveTimeout(ServerName, databaseName, "TimeoutDatas/5", CancellationToken.None);
        }
    }
}