namespace TimeoutMigrationTool.Raven4.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using NServiceBus.Features;
    using Particular.TimeoutMigrationTool.ASQ;

    [TestFixture]
    class RavenDBToAsqEndToEnd : RavenDBAcceptanceTest
    {
        string asqConnectionString = Environment.GetEnvironmentVariable("AzureStorageQueue_ConnectionString") ?? "UseDevelopmentStorage=true";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RavenDBSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AsqTarget));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.Four;

            var ravenAdapter = new Raven4Adapter(serverUrl, databaseName);

            await Scenario.Define<SourceContext>()
                .WithEndpoint<RavenDBSource>(b => b.CustomConfig(ec =>
                    {
                        ec.UsePersistence<RavenDBPersistence>()
                            .SetDefaultDocumentStore(GetDocumentStore(serverUrl, databaseName));
                        ec.UseSerialization<NewtonsoftSerializer>();
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

            var context = await Scenario.Define<TargetContext>()
                // Create the legacy endpoint to forward the delayed message to the native delayed delivery endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until delivery is needed
                .WithEndpoint<RavenDBSource>(b => b.CustomConfig(ec =>
                {
                    var transport = ec.UseTransport<AzureStorageQueueTransport>().ConnectionString(asqConnectionString);
                    transport.DisablePublishing();

                    transport.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                }))
                .WithEndpoint<AsqTarget>(b => b.CustomConfig(ec =>
                    {
                        var transport = ec.UseTransport<AzureStorageQueueTransport>().ConnectionString(asqConnectionString);
                        transport.DisablePublishing();

                        transport.DelayedDelivery().DisableTimeoutManager();

                        ec.UseSerialization<NewtonsoftSerializer>();
                    })
                    .When(async (_, c) =>
                    {
                        var logger = new TestLoggingAdapter(c);
                        var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion, false);
                        var timeoutsTarget = new ASQTarget(asqConnectionString, new DelayedDeliveryTableNameProvider());
                        var migrationRunner = new MigrationRunner(logger, timeoutsSource, timeoutsTarget);

                        await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                    }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

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

        public class AsqTarget : EndpointConfigurationBuilder
        {
            public AsqTarget()
            {
                EndpointSetup<DefaultServer>();
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