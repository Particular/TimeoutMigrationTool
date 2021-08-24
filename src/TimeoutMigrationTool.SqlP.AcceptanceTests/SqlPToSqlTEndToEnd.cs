namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.SqlT;
    using Particular.TimeoutMigrationTool.SqlP;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestFixture]
    class SqlPToSqlTEndToEnd : SqlPAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlPSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlTTarget));

            await Scenario.Define<SourceContext>()
                 .WithEndpoint<SqlPSource>(b => b.CustomConfig(ec =>
                 {
                     var persistence = ec.UsePersistence<SqlPersistence>();

                     persistence.SqlDialect<NServiceBus.SqlDialect.MsSqlServer>();
                     persistence.ConnectionBuilder(
                         connectionBuilder: () =>
                         {
                             return new SqlConnection(connectionString);
                         });
                 })
                 .When(async (session, c) =>
                 {
                     var delayedMessage = new DelayedMessage();
                     var options = new SendOptions();

                     options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                     options.SetDestination(targetEndpoint);

                     await session.Send(delayedMessage, options);

                     await WaitUntilTheTimeoutIsSavedInSql(sourceEndpoint);

                     c.TimeoutSet = true;
                 }))
                 .Done(c => c.TimeoutSet)
                 .Run(TimeSpan.FromSeconds(30));

            var setupContext = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlTTarget>(b => b.CustomConfig(ec =>
                {
                    ec.OverrideLocalAddress(sourceEndpoint);

                    ec.UseTransport<SqlServerTransport>()
                    .ConnectionString(connectionString);
                }))
                .Done(c => c.EndpointsStarted)
                .Run(TimeSpan.FromSeconds(30));

            var logger = new TestLoggingAdapter(setupContext);
            var timeoutSource = new SqlTimeoutsSource(connectionString, new MsSqlServer(), 1024);
            var timeoutTarget = new SqlTTimeoutsTarget(logger, connectionString, "dbo");
            var migrationRunner = new MigrationRunner(logger, timeoutSource, timeoutTarget);

            await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlTTarget>(b => b.CustomConfig(ec =>
                {
                    ec.OverrideLocalAddress(sourceEndpoint);

                    ec.UseTransport<SqlServerTransport>()
                    .ConnectionString(connectionString);
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(90));

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

        public class SqlTTarget : EndpointConfigurationBuilder
        {
            public SqlTTarget()
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