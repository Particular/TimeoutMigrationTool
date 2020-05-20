namespace TimeoutMigrationTool.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using Particular.TimeoutMigrationTool.RavenDB;
    using System;
    using System.Threading.Tasks;

    [TestFixture]
    class RavenDBToRabbitMqEndToEnd : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var context = await Scenario.Define<Context>()
            .WithEndpoint<LegacyRavenDBEndpoint>(b => b
                .When(async (session, c) =>
                {
                    var startSagaMessage = new DelayedMessage();

                    var options = new SendOptions();

                    options.DelayDeliveryWith(TimeSpan.FromSeconds(5));
                    options.SetDestination(NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint)));

                    await session.Send(startSagaMessage, options);

                    c.TimeoutSet = true;
                }))
            .Done(c => c.TimeoutSet)
            .Run();

            var timeoutStorage = new RavenDBTimeoutStorage("http://localhost:8080", "TimeoutMigrationTests", "TimeoutDatas", RavenDbVersion.Four);
            var transportAdapter = new RabbitMqTimeoutCreator("blah");
            var migrationRunner = new MigrationRunner(timeoutStorage, transportAdapter);

            await migrationRunner.Run();

            context = await Scenario.Define<Context>()
             .WithEndpoint<NewRabbitMqEndpoint>()
             .Done(c => c.GotTheDelayedMessage)
             .Run();

            Assert.True(context.GotTheDelayedMessage);
        }

        public class Context : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
            public bool GotTheDelayedMessage { get; set; }
        }

        public class LegacyRavenDBEndpoint : EndpointConfigurationBuilder
        {
            public LegacyRavenDBEndpoint()
            {
                EndpointSetup<RavenDbEndpoint>();
            }
        }

        public class NewRabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public NewRabbitMqEndpoint()
            {
                EndpointSetup<RavenDbEndpoint>();
            }

            class DelayedMessageHandler : IHandleMessages<DelayedMessage>
            {
                public DelayedMessageHandler(Context testContext)
                {
                    this.testContext = testContext;
                }

                public Task Handle(DelayedMessage message, IMessageHandlerContext context)
                {
                    testContext.GotTheDelayedMessage = true;
                    return Task.CompletedTask;
                }

                readonly Context testContext;
            }
        }

        public class DelayedMessage : IMessage
        {
        }
    }
}