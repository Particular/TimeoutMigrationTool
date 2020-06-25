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
            await testSuite.CreateLegacyTimeoutManagerIndex(true);
        }

        [TearDown]
        public async Task TearDown()
        {
            await testSuite.TeardownDatabase();
        }

        protected abstract IRavenTestSuite CreateTestSuite();

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreNoTimeoutsListEndpointsReturnsAnEmptyList(bool useIndex)
        {
            var sut = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.IsEmpty(endpoints);
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereTimeoutsListEndpointsReturnsEndpointsList(bool useIndex)
        {
            await testSuite.InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreTimeoutsListEndpointsRespectsTheCutoffDate(bool useIndex)
        {
            await testSuite.InitTimeouts(nrOfTimeouts, true);

            var sut = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreCompletedTimeoutsTheseAreIgnoredWhenListingEndpoints(bool useIndex)
        {
            await testSuite.InitTimeouts(50, false);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationDonePrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(49));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreInProgressTimeoutsTheseAreIncludedWhenListingEndpoints(bool useIndex)
        {
            await testSuite.InitTimeouts(50, false);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationOngoingPrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDBTimeoutStorage(testSuite.Logger,testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(50));
            Assert.That(endpoints.First().EndpointName, Is.EqualTo(testSuite.EndpointName));
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