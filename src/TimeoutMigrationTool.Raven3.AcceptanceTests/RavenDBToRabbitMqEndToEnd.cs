namespace TimeoutMigrationTool.Raven3.AcceptanceTests
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
    [EnvironmentSpecificTest(EnvironmentVariables.RabbitMqHost, EnvironmentVariables.Raven3Url)]
    class RavenDBToRabbitMqEndToEnd : RavenDBAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RavenDBSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqTarget));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.ThreeDotFive;

            var ravenAdapter = new Raven3Adapter(serverUrl, databaseName);

            await Scenario.Define<SourceContext>()
                .WithEndpoint<RavenDBSource>(b => b.CustomConfig(ec =>
                    {
                        ec.UsePersistence<RavenDBPersistence>()
                            .DoNotSetupDatabasePermissions()
                            .DisableSubscriptionVersioning()
                            .SetDefaultDocumentStore(GetDocumentStore(serverUrl, databaseName));
                    })
                    .When(async (session, c) =>
                    {
                        var delayedMessage = new DelayedMessage();

                        var options = new SendOptions();

                        options.DelayDeliveryWith(TimeSpan.FromSeconds(30));
                        options.SetDestination(targetEndpoint);

                        await session.Send(delayedMessage, options);

                        await WaitUntilTheTimeoutIsSavedInRaven(ravenAdapter, sourceEndpoint);

                        c.TimeoutSet = true;
                    }))
                .Done(c => c.TimeoutSet)
                .Run(TimeSpan.FromSeconds(15));

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<RabbitMqTarget>(b => b.CustomConfig(ec =>
                    {
                        ec.UseTransport<RabbitMQTransport>()
                            .ConnectionString(rabbitUrl);
                    })
                    .When(async (_, c) =>
                    {
                        var logger = new TestLoggingAdapter();
                        var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion, false);
                        var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl);
                        var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);
                        await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                    }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(60));

            Assert.True(context.GotTheDelayedMessage);
        }

        public class SourceContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class RavenDBSource : EndpointConfigurationBuilder
        {
            public RavenDBSource()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class RabbitMqTarget : EndpointConfigurationBuilder
        {
            public RabbitMqTarget()
            {
                EndpointSetup<RabbitMqEndpoint>();
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