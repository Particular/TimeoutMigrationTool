namespace TimeoutMigrationTool.RabbitMq.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using RabbitMQ.Client;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.RabbitMqHost)]
    public class RabbitMqTimeoutsTargetTests
    {
        string rabbitUrl;
        ConnectionFactory factory;
        string ExistingEndpointNameUsingConventional = "ExistingEndpointName";
        string ExistingEndpointNameUsingDirect = "ExistingEndpointNameDirect";
        string NonExistingEndpointName = "NonExistingEndpointName";
        string EndpointWithShortTimeout = "EndpointWithShortTimeout";
        string DelayDeliveryExchange = "nsb.delay-delivery";

        [OneTimeSetUp]
        public void TestSuitSetup()
        {
            rabbitUrl = $"amqp://guest:guest@{Environment.GetEnvironmentVariable(EnvironmentVariables.RabbitMqHost)}:5672";
            factory = new ConnectionFactory() { Uri = new Uri(rabbitUrl) };
        }

        [SetUp]
        public void Setup()
        {
            using var connection = factory.CreateConnection();
            using var model = connection.CreateModel();

            model.QueueDeclare(ExistingEndpointNameUsingConventional, true, false, false, null);
            model.ExchangeDeclare(ExistingEndpointNameUsingConventional, "fanout", true, false, null);
            model.QueueBind(ExistingEndpointNameUsingConventional, ExistingEndpointNameUsingConventional, "");

            model.QueueDeclare(ExistingEndpointNameUsingDirect, true, false, false, null);

            model.QueueDeclare(EndpointWithShortTimeout, true, false, false, null);

            model.ExchangeDeclare(DelayDeliveryExchange, "topic", true, false, null);
            model.ExchangeDeclare("nsb.v2.delay-level-00", "topic", true, false, null);
            model.ExchangeBind(DelayDeliveryExchange, "nsb.v2.delay-level-00", "*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.0.#");
        }

        [TearDown]
        public void TearDown()
        {
            using var connection = factory.CreateConnection();
            using var model = connection.CreateModel();

            model.QueueDelete(ExistingEndpointNameUsingConventional);
            model.QueueDelete(ExistingEndpointNameUsingDirect);
            model.ExchangeDelete(ExistingEndpointNameUsingConventional);
            model.QueueDelete(EndpointWithShortTimeout);
        }

        [Test]
        public async Task AbleToMigrate_ExistingDestination_ReturnsNoProblems()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointNameUsingConventional,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string>
                {
                    ExistingEndpointNameUsingConventional, ExistingEndpointNameUsingDirect
                }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsTrue(result.CanMigrate);
        }

        [Test]
        public async Task AbleToMigrate_DelayedDeliveryDoesNotExist_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);
            DeleteDelayDelivery();

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointNameUsingConventional,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string>
                {
                    ExistingEndpointNameUsingConventional, ExistingEndpointNameUsingDirect
                }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsFalse(result.CanMigrate);
        }

        void DeleteDelayDelivery()
        {
            using var connection = factory.CreateConnection();
            using var model = connection.CreateModel();
            model.ExchangeDelete(DelayDeliveryExchange);
        }

        [Test]
        public async Task AbleToMigrate_NonExistingDestination_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointNameUsingConventional,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string> { ExistingEndpointNameUsingConventional, NonExistingEndpointName }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsFalse(result.CanMigrate);
        }

        [Test]
        public async Task AbleToMigrate_TimeoutHigherThan9Years_ReturnsProblems()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointNameUsingConventional,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddYears(9),
                Destinations = new List<string> { ExistingEndpointNameUsingConventional }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsFalse(result.CanMigrate);
        }

        [Test]
        public async Task Should_handle_negative_delays()
        {
            const int BatchNumber = 33;

            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointNameUsingConventional,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string> { ExistingEndpointNameUsingConventional }
            };

            var migrateResult = await sut.AbleToMigrate(info);
            Assert.IsTrue(migrateResult.CanMigrate);

            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(ExistingEndpointNameUsingConventional);

            await endpointTarget.StageBatch(new List<TimeoutData>
            {
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>(),
                    Destination = ExistingEndpointNameUsingConventional,
                    State = new byte[2],
                    Time = DateTime.Now - TimeSpan.FromDays(1)
                }
            }, BatchNumber);

            var numPumped = await sut.CompleteBatch(BatchNumber);

            Assert.AreEqual(1, numPumped);

            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            var result = channel.BasicGet(ExistingEndpointNameUsingConventional, true);

            Assert.NotNull(result);
            Assert.AreEqual(0, result.MessageCount);
        }

        [Test]
        public async Task Should_delete_staging_queue_when_aborting()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);
            var endpointName = "FakeEndpoint";
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Abort(endpointName);

            using var connection = factory.CreateConnection(rabbitUrl);
            using var model = connection.CreateModel();

            Assert.Throws<RabbitMQ.Client.Exceptions.OperationInterruptedException>(() => model.QueueDeclarePassive(QueueCreator.StagingQueueName));
        }

        [Test]
        public async Task Should_delete_staging_queue_when_completing()
        {
            var sut = new RabbitMqTimeoutTarget(new TestLoggingAdapter(), rabbitUrl);
            var endpointName = "FakeEndpoint";
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Complete(endpointName);

            using var connection = factory.CreateConnection(rabbitUrl);
            using var model = connection.CreateModel();

            Assert.Throws<RabbitMQ.Client.Exceptions.OperationInterruptedException>(() => model.QueueDeclarePassive(QueueCreator.StagingQueueName));
        }
    }
}