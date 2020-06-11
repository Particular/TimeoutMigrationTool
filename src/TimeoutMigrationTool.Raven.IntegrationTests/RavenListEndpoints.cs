namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven3;
    using Raven4;

    public abstract class RavenListEndpoints
    {
        private readonly int nrOfTimeouts = 1500;
        IRavenTestSuite testSuite;

        [SetUp]
        public async Task Setup()
        {
            testSuite = CreateTestSuite();
            await testSuite.SetupDatabase();
        }

        [TearDown]
        public async Task TearDown()
        {
            await testSuite.TeardownDatabase();
        }

        protected abstract IRavenTestSuite CreateTestSuite();

        [Test]
        public async Task WhenThereAreNoTimeoutsListEndpointsReturnsAnEmptyList()
        {
            var sut = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.IsEmpty(endpoints);
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsReturnsEndpointsList()
        {
            await testSuite.InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(3));
        }

        [Test]
        public async Task WhenThereTimeoutsListEndpointsRespectsTheCutoffDate()
        {
            await testSuite.InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var endpoints = await sut.ListEndpoints(DateTime.Now.AddDays(8));

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }

        [Test]
        public async Task WhenThereAreCompletedTimeoutsTheseAreIgnoredWhenListingEndpoints()
        {
            await testSuite.InitTimeouts(50, false);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationDonePrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(49));
        }

        [Test]
        public async Task WhenThereAreInProgressTimeoutsTheseAreIncludedWhenListingEndpoints()
        {
            await testSuite.InitTimeouts(50, false);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationOngoingPrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(50));
            Assert.That(endpoints.First().EndpointName, Is.EqualTo(testSuite.Endpoint.EndpointName));
        }
    }

    [TestFixture]
    public class Raven4ListsEndpoints : RavenListEndpoints
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }

    [TestFixture]
    public class Raven3ListsEndpoints : RavenListEndpoints
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }
}