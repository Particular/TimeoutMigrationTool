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
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString, EnvironmentVariables.AzureStorageConnectionString)]
    class SqlPToAsqEndToEnd : SqlPAcceptanceTest
    {
        string asqConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorageConnectionString);

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var salesEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlPSource));
            var reportingEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AsqTarget));

            // Make sure delayed delivery queue is created so that the migration can run.
            await Scenario.Define<SourceContext>()
                .WithEndpoint<SqlPSource>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftJsonSerializer>();
                })).Run(TimeSpan.FromSeconds(10));

            // Sending a delayed delivery message using TimeoutManager
            await Scenario.Define<SourceContext>()
                .WithEndpoint<SqlPSource>(b => b.CustomConfig(ec =>
                {
                    var persistence = ec.UsePersistence<SqlPersistence>();
                    persistence.SubscriptionSettings().DisableCache();

                    persistence.SqlDialect<NServiceBus.SqlDialect.MsSqlServer>();
                    persistence.ConnectionBuilder(
                        connectionBuilder: () => new SqlConnection(connectionString));

                    ec.UseSerialization<NewtonsoftJsonSerializer>();
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

            var context = await Scenario.Define<TargetContext>()
                // Create the sales endpoint to forward the delayed message to the reporting endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until
                // delivery is needed
                .WithEndpoint<SqlPSource>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftJsonSerializer>();
                }))
                // Start the reporting endpoint to receieve and process the delayed message
                .WithEndpoint<AsqTarget>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftJsonSerializer>();
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = new SqlTimeoutsSource(connectionString, new MsSqlServer(), 1024);
                    var timeoutsTarget = new ASQTarget(asqConnectionString, new DelayedDeliveryTableNameProvider());
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, timeoutsTarget);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(salesEndpoint), new Dictionary<string, string>());
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

        public class SqlPSource : EndpointConfigurationBuilder
        {
            public SqlPSource()
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