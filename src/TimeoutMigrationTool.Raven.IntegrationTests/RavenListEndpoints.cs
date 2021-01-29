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
        readonly int nrOfTimeouts = 1500;
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
            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.IsEmpty(endpoints);
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereTimeoutsListEndpointsReturnsEndpointsList(bool useIndex)
        {
            var endpointATimes = await testSuite.InitTimeouts(nrOfTimeouts, "EndpointA", 0);
            var endpointBTimes = await testSuite.InitTimeouts(500, "EndpointB", nrOfTimeouts);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));

            var endpointA = endpoints.FirstOrDefault(x => x.EndpointName == "EndpointA");
            Assert.IsNotNull(endpointA);
            Assert.That(endpointA.NrOfTimeouts, Is.EqualTo(nrOfTimeouts));
            Assert.That(endpointA.ShortestTimeout, Is.EqualTo(endpointATimes.ShortestTimeout));
            Assert.That(endpointA.LongestTimeout, Is.EqualTo(endpointATimes.LongestTimeout));

            var endpointB = endpoints.FirstOrDefault(x => x.EndpointName == "EndpointB");
            Assert.IsNotNull(endpointB);
            Assert.That(endpointB.NrOfTimeouts, Is.EqualTo(500));
            Assert.That(endpointB.ShortestTimeout, Is.EqualTo(endpointBTimes.ShortestTimeout));
            Assert.That(endpointB.LongestTimeout, Is.EqualTo(endpointBTimes.LongestTimeout));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreTimeoutsListEndpointsRespectsTheCutoffDate(bool useIndex)
        {
            await testSuite.InitTimeouts(nrOfTimeouts, "EndpointA", 0);
            await testSuite.InitTimeouts(50, "EndpointB", nrOfTimeouts);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(2));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreCompletedTimeoutsTheseAreIgnoredWhenListingEndpoints(bool useIndex)
        {
            await testSuite.InitTimeouts(50);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationDonePrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
            var endpoints = await sut.ListEndpoints(DateTime.Now);

            Assert.IsNotNull(endpoints);
            Assert.That(endpoints.Count, Is.EqualTo(1));
            Assert.That(endpoints.First().NrOfTimeouts, Is.EqualTo(49));
        }

        [TestCase(true)]
        [TestCase(false)]
        public async Task WhenThereAreInProgressTimeoutsTheseAreIncludedWhenListingEndpoints(bool useIndex)
        {
            await testSuite.InitTimeouts(50);

            var timeout = await testSuite.RavenAdapter.GetDocument<TimeoutData>("TimeoutDatas/0", (data, id) => data.Id = id);
            timeout.OwningTimeoutManager = $"{RavenConstants.MigrationOngoingPrefix}{timeout.OwningTimeoutManager}";
            await testSuite.RavenAdapter.UpdateDocument(timeout.Id, timeout);

            var sut = new RavenDbTimeoutsSource(testSuite.Logger, testSuite.ServerName, testSuite.DatabaseName, "TimeoutDatas", testSuite.RavenVersion, useIndex);
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