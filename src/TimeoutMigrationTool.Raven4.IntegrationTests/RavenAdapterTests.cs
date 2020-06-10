namespace TimeoutMigrationTool.Raven4.IntegrationTests
{
    using System;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Raven3;
    using Raven4;

    public abstract class RavenAdapterTests
    {
        private int nrOfTimeouts = 250;
        IRavenTestSuite testSuite;

        [SetUp]
        public async Task Setup()
        {
            testSuite = CreateTestSuite();
            await testSuite.SetupDatabase();
            await testSuite.InitTimeouts(nrOfTimeouts);
        }

        [TearDown]
        public async Task TearDown()
        {
            await testSuite.TeardownDatabase();
        }

        protected abstract IRavenTestSuite CreateTestSuite();

        [Test]
        public async Task WhenReadingTimeouts()
        {
            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(-1), "TimeoutDatas", (doc, id) => doc.Id = id);
            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeouts));
        }

        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(10), "TimeoutDatas", (doc, id) => doc.Id = id);

            foreach (var timeout in timeouts)
            {
                Assert.That(timeout.Id, Is.Not.Null);
            }
            Assert.That(timeouts.Count, Is.EqualTo(125));
        }
    }

    [TestFixture]
    public class Raven3AdapterTests : RavenAdapterTests
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }
    }

    [TestFixture]
    public class Raven4AdapterTests : RavenAdapterTests
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven4TestSuite();
        }
    }
}