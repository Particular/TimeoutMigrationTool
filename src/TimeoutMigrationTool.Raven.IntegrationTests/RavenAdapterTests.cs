namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven3;
    using Raven4;

    public abstract class RavenAdapterTests
    {
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
        public async Task CanReadDocumentsBySpecifiedIds()
        {
            var nrOfTimeouts = 5;
            await testSuite.InitTimeouts(nrOfTimeouts);
            var timeoutIds = new[] {"TimeoutDatas/1", "TimeoutDatas/2", "TimeoutDatas/3", "TimeoutDatas/4", "TimeoutDatas/0"};

            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(timeoutIds, (doc, id) => doc.Id = id);

            Assert.That(timeouts.Count, Is.EqualTo(5));
            Assert.That(string.IsNullOrEmpty(timeouts.First().Id), Is.False);
        }

        [Test]
        public async Task CanReadDocumentsBySpecifiedIdsWhenNumberOfIdsWouldResultInTooLongUri()
        {
            var nrOfTimeouts = 500;
            await testSuite.InitTimeouts(nrOfTimeouts);
            var timeoutIds = new List<string>();

            for (var i = 0; i < 500; i++)
            {
                timeoutIds.Add($"TimeoutDatas/{i}");
            }

            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(timeoutIds, (doc, id) => doc.Id = id);

            Assert.That(timeouts.Count, Is.EqualTo(500));
        }

        [Test]
        public async Task WhenReadingTimeouts()
        {
            var nrOfTimeouts = 250;
            await testSuite.InitTimeouts(nrOfTimeouts);
            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(-1), "TimeoutDatas", (doc, id) => doc.Id = id);
            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeouts));
        }

        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var nrOfTimeouts = 250;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var timeouts = await testSuite.RavenAdapter.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(10), "TimeoutDatas", (doc, id) => doc.Id = id);

            foreach (var timeout in timeouts)
            {
                Assert.That(timeout.Id, Is.Not.Null);
            }

            Assert.That(timeouts.Count, Is.EqualTo(125));
        }

        [Test]
        public async Task WhenPagingThroughTimeoutsStartingFromTheBeginningWeGetAllDocuments()
        {
            var nrOfTimeouts = 1250;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var timeouts = await testSuite.RavenAdapter.GetPagedDocuments<TimeoutData>("TimeoutDatas", (doc, id) => doc.Id = id, 0, 5);
            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeouts));
        }

        [Test]
        public async Task WhenPagingThroughTimeoutsForNrOfPagesThatAreLessThanActualAmountOfDocuments()
        {
            var nrOfTimeouts = RavenConstants.DefaultPagingSize * 4;
            var nrOfPages = 3;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var timeouts = await testSuite.RavenAdapter.GetPagedDocuments<TimeoutData>("TimeoutDatas", (doc, id) => doc.Id = id, 0, nrOfPages);
            Assert.That(timeouts.Count, Is.EqualTo(RavenConstants.DefaultPagingSize * nrOfPages));
        }

        [Test]
        public async Task WhenPagingThroughTimeoutsStartingFromASpecificNumberWeGetAllDocuments()
        {
            var nrOfTimeouts = RavenConstants.DefaultPagingSize * 4;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var startFrom = 125;
            var timeouts = await testSuite.RavenAdapter.GetPagedDocuments<TimeoutData>("TimeoutDatas", (doc, id) => doc.Id = id, startFrom);
            Assert.That(timeouts.Count, Is.EqualTo(nrOfTimeouts - startFrom));
        }

        [Test]
        public async Task WhenPagingThroughTimeoutsStartingFromTheBeginningWithOnePageWeGetDefaultBatchSizeAmountOfDocuments()
        {
            var nrOfTimeouts = RavenConstants.DefaultPagingSize * 4;
            await testSuite.InitTimeouts(nrOfTimeouts);

            var timeouts = await testSuite.RavenAdapter.GetPagedDocuments<TimeoutData>("TimeoutDatas", (doc, id) => doc.Id = id, 0, 1);
            Assert.That(timeouts.Count, Is.EqualTo(RavenConstants.DefaultPagingSize));
        }

        [Test]
        public async Task CanReadDocumentsByIndexWhenItDoesntExist()
        {
            var nrOfTimeouts = 500;
            await testSuite.InitTimeouts(nrOfTimeouts);

            Assert.ThrowsAsync<Exception>(() => testSuite.RavenAdapter.GetDocumentsByIndex<TimeoutData>((doc, id) => doc.Id = id, 0, TimeSpan.Zero));
        }

        [Test]
        public async Task CanReadDocumentsByIndexWhenIndexExist()
        {
            var nrOfTimeouts = 500;
            await testSuite.InitTimeouts(nrOfTimeouts);
            await testSuite.CreateLegacyTimeoutManagerIndex(true);

            var result = await testSuite.RavenAdapter.GetDocumentsByIndex<TimeoutData>((doc, id) => doc.Id = id, 0, TimeSpan.Zero);

            Assert.That(result.Documents.Count, Is.EqualTo(500));
        }
    }

    [TestFixture]
    public class Raven3AdapterTests : RavenAdapterTests
    {
        protected override IRavenTestSuite CreateTestSuite()
        {
            return new Raven3TestSuite();
        }

        [Test]
        [Ignore("This test just proves that we're unable to use indexes to do this")]
        public async Task CanPageDocumentsAndGetUniqueResultsWhenQueryingByIndex()
        {
            var nrOfTimeouts = 1500;

            var suite = new Raven3TestSuite();
            await suite.SetupDatabase();
            await suite.InitTimeouts(nrOfTimeouts);

            var ravenAdapter = (Raven3Adapter)suite.RavenAdapter;
            await Task.Delay(TimeSpan.FromSeconds(2));
            var resultsPage1 = await ravenAdapter.GetDocumentsByIndex<TimeoutData>((doc, id) => doc.Id = id, 0, TimeSpan.Zero);
            var timeoutData = resultsPage1.Documents.First();
            timeoutData.OwningTimeoutManager = "bla";
            await ravenAdapter.UpdateDocument(timeoutData.Id, timeoutData);
            await Task.Delay(TimeSpan.FromSeconds(2));

            var resultsPage2 = await ravenAdapter.GetDocumentsByIndex<TimeoutData>((doc, id) => doc.Id = id, RavenConstants.DefaultPagingSize, TimeSpan.Zero);

            Assert.That(resultsPage1.Documents.Count, Is.EqualTo(1024));
            Assert.That(resultsPage2.Documents.Count, Is.EqualTo(nrOfTimeouts - RavenConstants.DefaultPagingSize));
            var timeoutIds = resultsPage1.Documents.Select(x => x.Id).ToList();
            timeoutIds.AddRange(resultsPage2.Documents.Select(x => x.Id));
            var uniqueTimeoutIds = timeoutIds.Distinct();
            Assert.That(uniqueTimeoutIds.Count(), Is.EqualTo(nrOfTimeouts));

            await suite.TeardownDatabase();
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