namespace TimeoutMigrationTool.RabbitMq.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using RabbitMQ.Client;

    [TestFixture]
    public class RabbitMqTimeoutCreatorTest
    {
        string rabbitUrl;
        ConnectionFactory factory;
        string ExistingEndpointNameUsingConventional = "ExistingEndpointName";
        string ExistingEndpointNameUsingDirect = "ExistingEndpointNameDirect";
        string NonExistingEndpointName = "NonExistingEndpointName";

        [OneTimeSetUp]
        public void TestSuitSetup()
        {
            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";
            factory = new ConnectionFactory(){Uri = new Uri(rabbitUrl)};
        }

        [SetUp]
        public void Setup()
        {
            using (var connection = factory.CreateConnection())
            {
                using (var model = connection.CreateModel())
                {
                    model.QueueDeclare(ExistingEndpointNameUsingConventional, true, false, false, null);
                    model.ExchangeDeclare(ExistingEndpointNameUsingConventional, "fanout", true, false, null);
                    model.QueueDeclare(ExistingEndpointNameUsingDirect, true, false, false, null);
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            using (var connection = factory.CreateConnection())
            {
                using (var model = connection.CreateModel())
                {
                    model.QueueDelete(ExistingEndpointNameUsingConventional);
                    model.QueueDelete(ExistingEndpointNameUsingDirect);
                    model.ExchangeDelete(ExistingEndpointNameUsingConventional);
                }
            }
        }

        [Test]
        public void AbleToMigrate_ExistingDestination_ReturnsNoProblems()
        {
            var sut = new RabbitMqTimeoutCreator(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo();
            info.EndpointName = ExistingEndpointNameUsingConventional;
            info.ShortestTimeout = DateTime.UtcNow.AddDays(3);
            info.LongestTimeout = DateTime.UtcNow.AddDays(5);
            info.Destinations = new List<string>{ExistingEndpointNameUsingConventional, ExistingEndpointNameUsingDirect};
            var result = sut.AbleToMigrate(info);

            Assert.IsTrue(result.Result.CanMigrate);
        }

        [Test]
        public void AbleToMigrate_NonExistingDestination_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutCreator(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo();
            info.EndpointName = ExistingEndpointNameUsingConventional;
            info.ShortestTimeout = DateTime.UtcNow.AddDays(3);
            info.LongestTimeout = DateTime.UtcNow.AddDays(5);
            info.Destinations = new List<string>{ExistingEndpointNameUsingConventional, NonExistingEndpointName};
            var result = sut.AbleToMigrate(info);

            Assert.IsFalse(result.Result.CanMigrate);
        }

        [Test]
        public void AbleToMigrate_TimeoutHigherThan9Years_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutCreator(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo();
            info.EndpointName = ExistingEndpointNameUsingConventional;
            info.ShortestTimeout = DateTime.UtcNow.AddDays(3);
            info.LongestTimeout = DateTime.UtcNow.AddYears(9);
            info.Destinations = new List<string>{ExistingEndpointNameUsingConventional};
            var result = sut.AbleToMigrate(info);

            Assert.IsFalse(result.Result.CanMigrate);
        }
    }
}