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
    class AspToASQMqEndToEnd : AspAcceptanceTest
    {
        string asqConnectionString = Environment.GetEnvironmentVariable("AzureStorageQueue_ConnectionString") ?? "UseDevelopmentStorage=true";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacyAspEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(ASQEndpoint));

            await Scenario.Define<SourceTestContext>()
                 .WithEndpoint<LegacyAspEndpoint>(b => b.CustomConfig(ec =>
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

            var context = await Scenario.Define<TargetTestContext>()
                // Create the sales endpoint to forward the delayed message to the reporting endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until
                // delivery is needed
                .WithEndpoint<LegacyAspEndpoint>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                }))
                // Start the reporting endpoint to receieve and process the delayed message
                .WithEndpoint<ASQEndpoint>(b => b.CustomConfig(ec =>
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
                EndpointSetup<LegacyTimeoutManagerEndpoint>(ec =>
                {
                    ec.DisableFeature<Sagas>();
                });
            }
        }

        public class ASQEndpoint : EndpointConfigurationBuilder
        {
            public ASQEndpoint()
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