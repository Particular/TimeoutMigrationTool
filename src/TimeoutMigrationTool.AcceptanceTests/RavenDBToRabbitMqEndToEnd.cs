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
    class RavenDBToRabbitMqEndToEnd : RavenDBAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacyRavenDBEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.Four;

            var ravenAdapter = new Raven4Adapter(serverUrl, databaseName);

            var context = await Scenario.Define<Context>()
                .WithEndpoint<LegacyRavenDBEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UsePersistence<RavenDBPersistence>()
                        .SetDefaultDocumentStore(GetDocumentStore(serverUrl, databaseName));

                })
                .When(async (session, c) =>
                {
                    var delayedMessage = new DelayedMessage();

                    var options = new SendOptions();

                    options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                    options.SetDestination(targetEndpoint);

                    await session.Send(delayedMessage, options);

                    c.TimeoutSet = true;
                }))
                .WithEndpoint<NewRabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);

                })
                .When(async c =>
                {
                    if (!c.TimeoutSet)
                    {
                        return false;
                    }
                    return await WaitUntilTheTimeoutIsSavedInRaven(ravenAdapter, sourceEndpoint);

                }, async (_, c) =>
                {
                    var logger = new TestLoggingAdapter();
                    var timeoutStorage = new RavenDBTimeoutStorage(serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion);
                    var transportAdapter = new RabbitMqTimeoutCreator(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);
                    await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(targetEndpoint), new Dictionary<string, string>());

                    c.MigrationComplete = true;

                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }


        static async Task<bool> WaitUntilTheTimeoutIsSavedInRaven(Raven4Adapter ravenAdapter, string endpoint)
        {
            var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(x =>
                x.OwningTimeoutManager.Equals(endpoint,
                    StringComparison.OrdinalIgnoreCase), "TimeoutDatas", (doc, id) => doc.Id = id);

            return timeouts.Count > 0;
        }

        public class Context : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
            public bool MigrationComplete { get; set; }
            public bool GotTheDelayedMessage { get; set; }
        }

        public class LegacyRavenDBEndpoint : EndpointConfigurationBuilder
        {
            public LegacyRavenDBEndpoint()
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