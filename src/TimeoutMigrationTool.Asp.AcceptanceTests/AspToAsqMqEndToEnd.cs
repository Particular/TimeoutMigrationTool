namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using NServiceBus.Features;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool.ASQ;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.AzureStorage_ConnectionString)]

    class AspToAsqMqEndToEnd : AspAcceptanceTest
    {
        string asqConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorage_ConnectionString) ?? "UseDevelopmentStorage=true";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AspSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AsqTarget));

            await Scenario.Define<SourceContext>()
                 .WithEndpoint<AspSource>(b => b.CustomConfig(ec =>
                 {
                     SetupPersistence(ec);

                     ec.UseSerialization<NewtonsoftSerializer>();
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
                // Create the legacy endpoint to forward the delayed message to the reporting endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until
                // delivery is needed
                .WithEndpoint<AspSource>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                }))
                // Start the reporting endpoint to receive and process the delayed message
                .WithEndpoint<AsqTarget>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = CreateTimeoutStorage(sourceEndpoint);
                    var timeoutsTarget = new ASQTarget(asqConnectionString, new DelayedDeliveryTableNameProvider());
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, timeoutsTarget);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
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

        public class AsqTarget : EndpointConfigurationBuilder
        {
            public AsqTarget()
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