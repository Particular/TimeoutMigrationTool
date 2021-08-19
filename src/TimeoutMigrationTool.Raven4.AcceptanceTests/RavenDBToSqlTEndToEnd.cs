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
    using Particular.TimeoutMigrationTool.SqlT;
    using SqlP.AcceptanceTests;

    [TestFixture]
    class RavenDBToSqlTEndToEnd : RavenDBAcceptanceTest
    {
        string sqlConnectionString;

        [SetUp]
        public async Task Setup()
        {
            databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-", "")}";

            sqlConnectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";

            await MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(sqlConnectionString);
        }

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RavenDBSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlTTarget));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.Four;

            var ravenAdapter = new Raven4Adapter(serverUrl, databaseName);

            await Scenario.Define<SourceContext>()
                .WithEndpoint<RavenDBSource>(b => b.CustomConfig(ec =>
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

            var setupContext = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlTTarget>(b => b.CustomConfig(ec =>
                    {
                        ec.OverrideLocalAddress(sourceEndpoint);

                        ec.UseTransport<SqlServerTransport>()
                            .ConnectionString(sqlConnectionString);
                    }))
                .Done(c => c.EndpointsStarted)
                .Run(TimeSpan.FromSeconds(30));

            var logger = new TestLoggingAdapter(setupContext);
            var timeoutsSource = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion, false);
            var timeoutsTarget = new SqlTTimeoutsTarget(logger, sqlConnectionString, "dbo");
            var migrationRunner = new MigrationRunner(logger, timeoutsSource, timeoutsTarget);

            await migrationRunner.Run(DateTimeOffset.UtcNow.AddDays(-1), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlTTarget>(b => b.CustomConfig(ec =>
                {
                    ec.OverrideLocalAddress(sourceEndpoint);

                    ec.UseTransport<SqlServerTransport>()
                        .ConnectionString(sqlConnectionString);
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