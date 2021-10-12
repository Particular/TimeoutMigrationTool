namespace TimeoutMigrationTool.Msmq.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.MsmqSql;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    [TestFixture]
    class MsmqSqlToSqlPEndToEnd : MsmqAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(MsmqSqlSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlPTarget));

            await Scenario.Define<SourceContext>()
                 .WithEndpoint<MsmqSqlSource>(b => b.CustomConfig(ec =>
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

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlPTarget>(b => b.CustomConfig(ec => { })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var source = default(ITimeoutsSource); //TODO: Implement
                    var target = new SqlTTimeoutsTarget(logger, connectionString, "");
                    var migrationRunner = new MigrationRunner(logger, source, target);

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

        public class MsmqSqlSource : EndpointConfigurationBuilder
        {
            public MsmqSqlSource()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class SqlPTarget : EndpointConfigurationBuilder
        {
            public SqlPTarget()
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