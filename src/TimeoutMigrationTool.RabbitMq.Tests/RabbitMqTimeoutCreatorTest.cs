namespace TimeoutMigrationTool.RabbitMq.Tests
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
        string ExistingEndpointName = "ExistingEndpointName";
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
                    model.QueueDeclare(ExistingEndpointName, true, false, false, null);
                    model.ExchangeDeclare(ExistingEndpointName, "fanout", true, false, null);
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
                    model.QueueDelete(ExistingEndpointName);
                    model.ExchangeDelete(ExistingEndpointName);
                }
            }
        }

        [Test]
        public void AbleToMigrate_ExistingDestination_ReturnsNoProblems()
        {
            var sut = new RabbitMqTimeoutCreator(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo();
            info.EndpointName = ExistingEndpointName;
            info.ShortestTimeout = DateTime.UtcNow.AddDays(3);
            info.LongestTimeout = DateTime.UtcNow.AddDays(5);
            info.Destinations = new List<string>{ExistingEndpointName};
            var result = sut.AbleToMigrate(info);

            Assert.IsTrue(result.Result.CanMigrate);
        }

        [Test]
        public void AbleToMigrate_NonExistingDestination_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutCreator(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo();
            info.EndpointName = ExistingEndpointName;
            info.ShortestTimeout = DateTime.UtcNow.AddDays(3);
            info.LongestTimeout = DateTime.UtcNow.AddDays(5);
            info.Destinations = new List<string>{ExistingEndpointName, NonExistingEndpointName};
            var result = sut.AbleToMigrate(info);

            Assert.IsFalse(result.Result.CanMigrate);
        }
    }
}