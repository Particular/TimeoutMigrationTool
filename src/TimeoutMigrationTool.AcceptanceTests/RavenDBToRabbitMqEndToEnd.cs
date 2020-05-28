using System.Threading;

namespace TimeoutMigrationTool.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using Particular.TimeoutMigrationTool.RavenDB;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestFixture]
    class RavenDBToRabbitMqEndToEnd : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var serverUrl = "http://localhost:8081";
            var databaseName = "TimeoutMigrationTests";
            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.Four;
            var ravenAdapter = new Raven4Adapter(serverUrl, databaseName);

            var context = await Scenario.Define<Context>()
                .WithEndpoint<LegacyRavenDBEndpoint>(b => b
                .When(async (session, c) =>
                {
                    var startSagaMessage = new DelayedMessage();

                    var options = new SendOptions();

                    options.DelayDeliveryWith(TimeSpan.FromSeconds(5));
                    options.SetDestination(NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint)));

                    await session.Send(startSagaMessage, options);

                    await WaitUntilTheTimeoutIsSavedInRaven(ravenAdapter, ravenTimeoutPrefix);

                    c.TimeoutSet = true;
                }))
                .Done(c => c.TimeoutSet)
                .Run();


            var targetConnectionString = "amqp://guest:guest@localhost:5672";

            var timeoutStorage = new RavenDBTimeoutStorage(serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion);
            var transportAdapter = new RabbitMqTimeoutCreator(targetConnectionString);
            var migrationRunner = new MigrationRunner(timeoutStorage, transportAdapter);

            context = await Scenario.Define<Context>()
             .WithEndpoint<NewRabbitMqEndpoint>(async c => await migrationRunner.Run(DateTime.Now.AddDays(-1), new Dictionary<string, string>()))
             .Done(c => c.GotTheDelayedMessage)
             .Run();

            Assert.True(context.GotTheDelayedMessage);
        }

        private static async Task WaitUntilTheTimeoutIsSavedInRaven(Raven4Adapter ravenAdapter, string ravenTimeoutPrefix)
        {
            List<TimeoutData> timeouts;
            do
            {
                timeouts = await ravenAdapter.GetDocuments<TimeoutData>(x =>
                    x.OwningTimeoutManager.Equals("Ravendbtorabbitmqendtoend.LegacyRavenDBEndpoint",
                        StringComparison.OrdinalIgnoreCase), ravenTimeoutPrefix, CancellationToken.None);

                Thread.Sleep(300);
            } while (timeouts.Count < 1);
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
                EndpointSetup<RabbitMqEndpoint>();
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