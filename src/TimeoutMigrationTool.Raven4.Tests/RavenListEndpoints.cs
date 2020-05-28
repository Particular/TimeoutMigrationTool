using System;
using System.Threading.Tasks;
using NUnit.Framework;
using Particular.TimeoutMigrationTool.RavenDB;

namespace TimeoutMigrationTool.Raven4.Tests
{
    public class RavenListEndpoints : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public async Task WhenThereAreNoTimeoutsListEndpointsReturnsAnEmptyList()
        {
            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.IsEmpty(endpoints);
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsReturnsEndpointsList()
        {
            await InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(3));
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsRespectsTheCutoffDate()
        {
            await InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.Four);
            var endpoints = await sut.ListEndpoints(DateTime.Now.AddDays(8));

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }
    }
}