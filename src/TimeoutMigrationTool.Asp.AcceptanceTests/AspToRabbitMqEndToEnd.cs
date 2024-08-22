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
    [EnvironmentSpecificTest(EnvironmentVariables.AzureStorageConnectionString, EnvironmentVariables.RabbitMqHost)]
    class AspToRabbitMqEndToEnd : AspAcceptanceTest
    {
        [SetUp]
        public void Setup()
        {
            rabbitUrl = $"amqp://guest:guest@{Environment.GetEnvironmentVariable(EnvironmentVariables.RabbitMqHost)}:5672";
        }

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AspSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqTarget));

            await Scenario.Define<SourceContext>()
                 .WithEndpoint<AspSource>(b => b.CustomConfig(ec =>
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

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<RabbitMqTarget>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = CreateTimeoutStorage(sourceEndpoint);
                    var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl, false);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.That(context.GotTheDelayedMessage, Is.True);
        }

        string rabbitUrl;

        public class SourceContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class AspSource : EndpointConfigurationBuilder
        {
            public AspSource()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>(ec =>
                {
                    ec.DisableFeature<Sagas>();
                });
            }
        }

        public class RabbitMqTarget : EndpointConfigurationBuilder
        {
            public RabbitMqTarget()
            {
                EndpointSetup<DefaultServer>(ec =>
                {
                    ec.DisableFeature<Sagas>();
                });
            }

            class DelayedMessageHandler : IHandleMessages<DelayedMessage>
            {
                public DelayedMessageHandler(TargetContext context)
                {
                    this.context = context;
                }

                public Task Handle(DelayedMessage message, IMessageHandlerContext context)
                {
                    this.context.GotTheDelayedMessage = true;
                    return Task.CompletedTask;
                }

                readonly TargetContext context;
            }
        }

        public class DelayedMessage : IMessage
        {
        }
    }
}