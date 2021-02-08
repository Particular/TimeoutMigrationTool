namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.ASQ;
    using Particular.TimeoutMigrationTool.SqlP;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestFixture]
    class SqlPToASQEndToEnd : SqlPAcceptanceTest
    {
        string asqConnectionString = Environment.GetEnvironmentVariable("AzureStorageQueue_ConnectionString") ?? "UseDevelopmentStorage=true";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var salesEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SalesSqlPEndpoint));
            var reportingEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(ReportingEndpoint));

            // Make sure delayed delivery queue is created so that the migration can run.
            await Scenario.Define<SourceTestContext>()
                .WithEndpoint<SalesSqlPEndpoint>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                })).Run(TimeSpan.FromSeconds(10));

            // Sending a delayed delivery message using TimeoutManager
            await Scenario.Define<SourceTestContext>()
                .WithEndpoint<SalesSqlPEndpoint>(b => b.CustomConfig(ec =>
                {
                    var persistence = ec.UsePersistence<SqlPersistence>();
                    persistence.SubscriptionSettings().DisableCache();

                    persistence.SqlDialect<NServiceBus.SqlDialect.MsSqlServer>();
                    persistence.ConnectionBuilder(
                        connectionBuilder: () => new SqlConnection(connectionString));

                    ec.UseSerialization<NewtonsoftSerializer>();
                })
                .When(async (session, c) =>
                {
                    var delayedMessage = new DelayedMessage();

                    var options = new SendOptions();

                    options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                    options.SetDestination(reportingEndpoint);

                    await session.Send(delayedMessage, options);

                    await WaitUntilTheTimeoutIsSavedInSql(salesEndpoint);

                    c.TimeoutSet = true;
                }))
                .Done(c => c.TimeoutSet)
                .Run(TimeSpan.FromSeconds(30));

            var context = await Scenario.Define<TargetTestContext>()
                // Create the sales endpoint to forward the delayed message to the reporting endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until
                // delivery is needed
                .WithEndpoint<SalesSqlPEndpoint>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                }))
                // Start the reporting endpoint to receieve and process the delayed message
                .WithEndpoint<ReportingEndpoint>(b => b.CustomConfig(ec =>
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
                    var timeoutStorage = new SqlTimeoutsSource(connectionString, new MsSqlServer(), 1024);
                    var timeoutsTarget = new ASQTarget(logger, asqConnectionString, new DelayedDeliveryTableNameProvider());
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, timeoutsTarget);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(salesEndpoint), new Dictionary<string, string>());
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

        public class SalesSqlPEndpoint : EndpointConfigurationBuilder
        {
            public SalesSqlPEndpoint()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class ReportingEndpoint : EndpointConfigurationBuilder
        {
            public ReportingEndpoint()
            {
                EndpointSetup<DefaultServer>();
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