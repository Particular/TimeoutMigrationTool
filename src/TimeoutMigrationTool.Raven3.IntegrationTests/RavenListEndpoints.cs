namespace TimeoutMigrationTool.Raven3.IntegrationTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public class RavenListEndpoints : RavenTimeoutStorageTestSuite
    {
        private readonly int nrOfTimeouts = 1500;

        [Test]
        public async Task WhenThereAreNoTimeoutsListEndpointsReturnsAnEmptyList()
        {
            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.IsEmpty(endpoints);
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsReturnsEndpointsList()
        {
            await InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(3));
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsRespectsTheCutoffDate()
        {
            await InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var endpoints = await sut.ListEndpoints(DateTime.Now.AddDays(8));

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task WhenThereAreCompletedTimeoutsTheseAreIgnoredWhenListingEndpoints()
        {
            await InitTimeouts(50, false);

            var adapter = new Raven3Adapter(ServerName, databaseName);
            var timeout = await adapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationDonePrefix}{timeout.OwningTimeoutManager}";
            await adapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(49));
        }

        [Test]
        public async Task WhenThereAreInProgressTimeoutsTheseAreIncludedWhenListingEndpoints()
        {
            await InitTimeouts(50, false);

            var adapter = new Raven3Adapter(ServerName, databaseName);
            var timeout = await adapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationOngoingPrefix}{timeout.OwningTimeoutManager}";
            await adapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(ServerName, databaseName, "TimeoutDatas", RavenDbVersion.ThreeDotFive);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(50));
        }
    }
}