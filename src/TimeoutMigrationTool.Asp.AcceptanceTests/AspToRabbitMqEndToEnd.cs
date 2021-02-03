namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using NServiceBus.Features;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;


    [TestFixture]
    class AspToRabbitMqEndToEnd : AspAcceptanceTest
    {
        [SetUp]
        public void Setup()
        {
            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";
        }

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacyAspEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqEndpoint));

            await Scenario.Define<SourceTestContext>()
                 .WithEndpoint<LegacyAspEndpoint>(b => b.CustomConfig(ec =>
                 {
                     SetupPersistence(ec);
                 })
                 .When(async (session, c) =>
                 {
                     var delayedMessage = new DelayedMessage();

                     var options = new SendOptions();

                     options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                     options.SetDestination(targetEndpoint);

                     await session.Send(delayedMessage, options);

                     await WaitUntilTheTimeoutsAreSavedInAsp(sourceEndpoint, 2);

                     c.TimeoutSet = true;
                 }))
                 .Done(c => c.TimeoutSet)
                 .Run(TimeSpan.FromSeconds(30));

            var context = await Scenario.Define<TargetTestContext>()
                .WithEndpoint<RabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = CreateTimeoutStorage(sourceEndpoint);
                    var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }

        string rabbitUrl;

        public class SourceTestContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetTestContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class LegacyAspEndpoint : EndpointConfigurationBuilder
        {
            public LegacyAspEndpoint()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class RabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public RabbitMqEndpoint()
            {
                EndpointSetup<DefaultServer>(ec =>
                {
                    ec.DisableFeature<Sagas>();
                });
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