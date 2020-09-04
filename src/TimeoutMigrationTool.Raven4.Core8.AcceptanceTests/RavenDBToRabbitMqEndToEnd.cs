namespace TimeoutMigrationTool.Raven4.Core8.AcceptanceTests
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

            await Scenario.Define<SourceTestContext>()
                 .WithEndpoint<LegacyRavenDBEndpoint>(b => b.CustomConfig(ec =>
                 {
                     ec.UsePersistence<RavenDBPersistence>()
                         .SetDefaultDocumentStore(GetDocumentStore(serverUrl, databaseName));

                 })
                 .When(async (session, c) =>
                 {
                     var delayedMessage = new DelayedMessage();

                     var options = new SendOptions();

                     options.DelayDeliveryWith(TimeSpan.FromSeconds(20));
                     options.SetDestination(targetEndpoint);

                     await session.Send(delayedMessage, options);

                     await WaitUntilTheTimeoutIsSavedInRaven(ravenAdapter, sourceEndpoint);

                     c.TimeoutSet = true;
                 }))
                 .Done(c => c.TimeoutSet)
                 .Run(TimeSpan.FromSeconds(15));

            var context = await Scenario.Define<TargetTestContext>()
                .WithEndpoint<NewRabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = new RavenDBTimeoutStorage(logger, serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion, false);
                    var transportAdapter = new RabbitMqTimeoutCreator(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);
                    await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }

        static async Task WaitUntilTheTimeoutIsSavedInRaven(Raven4Adapter ravenAdapter, string endpoint)
        {
            while(true)
            {
                var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(x =>
                    x.OwningTimeoutManager.Equals(endpoint,
                        StringComparison.OrdinalIgnoreCase), "TimeoutDatas", (doc, id) => doc.Id = id);

                if(timeouts.Count > 0)
                {
                    return;
                }
            }
        }

        public class SourceTestContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetTestContext : ScenarioContext
        {
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