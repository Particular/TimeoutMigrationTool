namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using Particular.TimeoutMigrationTool.SqlP;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestFixture]
    class SqlPToRabbitMqEndToEnd : SqlPAcceptanceTest
    {
        [SetUp]
        public void Setup()
        {
            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";
        }

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacySqlPEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint));

            await Scenario.Define<SourceTestContext>()
                 .WithEndpoint<LegacySqlPEndpoint>(b => b.CustomConfig(ec =>
                 {
                     var persistence = ec.UsePersistence<SqlPersistence>();

                     persistence.SqlDialect<NServiceBus.SqlDialect.MsSqlServer>();
                     persistence.ConnectionBuilder(
                         connectionBuilder: () =>
                         {
                             return new SqlConnection(connectionString);
                         });
                 })
                 .When(async (session, c) =>
                 {
                     var delayedMessage = new DelayedMessage();

                     var options = new SendOptions();

                     options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                     options.SetDestination(targetEndpoint);

                     await session.Send(delayedMessage, options);

                     await WaitUntilTheTimeoutIsSavedInSql(sourceEndpoint);

                     c.TimeoutSet = true;
                 }))
                 .Done(c => c.TimeoutSet)
                 .Run(TimeSpan.FromSeconds(30));

            var context = await Scenario.Define<TargetTestContext>()
                .WithEndpoint<NewRabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = new SqlTimeoutsSource(connectionString, new MsSqlServer(), 1024);
                    var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }

        async Task WaitUntilTheTimeoutIsSavedInSql(string endpoint)
        {
            while (true)
            {
                var numberOfTimeouts = await QueryScalarAsync<int>($"SELECT COUNT(*) FROM {endpoint}_TimeoutData");

                if (numberOfTimeouts > 0)
                {
                    return;
                }
            }
        }

        private string rabbitUrl;

        public class SourceTestContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetTestContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class LegacySqlPEndpoint : EndpointConfigurationBuilder
        {
            public LegacySqlPEndpoint()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class NewRabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public NewRabbitMqEndpoint()
            {
                EndpointSetup<RabbitMqEndpoint>();
            }

            class DelayedMessageHandler : IHandleMessages<DelayedMessage>
            {
                public DelayedMessageHandler(TargetTestContext testContext)
                {
                    this.testContext = testContext;
                }

                public Task Handle(DelayedMessage message, IMessageHandlerContext context)
                {
                    testContext.GotTheDelayedMessage = true;
                    return Task.CompletedTask;
                }

                readonly TargetTestContext testContext;
            }
        }

        public class DelayedMessage : IMessage
        {
        }
    }
}