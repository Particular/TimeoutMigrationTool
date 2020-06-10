﻿namespace TimeoutMigrationTool.Raven3.IntegrationTests
{
    using System;
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
            var reader = new Raven3Adapter(ServerName, databaseName);
            var timeouts = await reader.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(-1), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(timeouts.Count, Is.EqualTo(250));
        }

        [Test]
        public async Task WhenReadingTimeoutsWithCutoffDateNextWeek()
        {
            var reader = new Raven3Adapter(ServerName, databaseName);
            var timeouts = await reader.GetDocuments<TimeoutData>(x => x.Time >= DateTime.Now.AddDays(10), "TimeoutDatas", (doc, id) => doc.Id = id);

            Assert.That(timeouts.Count, Is.EqualTo(125));
        }

    }
}