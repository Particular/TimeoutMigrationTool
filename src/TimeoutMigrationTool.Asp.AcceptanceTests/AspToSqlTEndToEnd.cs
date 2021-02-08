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
    using Particular.TimeoutMigrationTool.SqlT;
    using TimeoutMigrationTool.SqlP.AcceptanceTests;


    [TestFixture]
    class AspToSqlTEndToEnd : AspAcceptanceTest
    {
        string sqlConnectionString;
        string databaseName;

        [SetUp]
        public async Task Setup()
        {
            databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-", "")}";

            sqlConnectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";

            await MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(sqlConnectionString);
        }

        [TearDown]
        public async Task Teardown()
        {
            await MsSqlMicrosoftDataClientHelper.RemoveDbIfExists(sqlConnectionString);
        }

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacyAspEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlTEndpoint));

            await Scenario.Define<SourceTestContext>()
                 .WithEndpoint<LegacyAspEndpoint>(b => b.CustomConfig(ec =>
                 {
                     SetupPersistence(ec);
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
                .WithEndpoint<SqlTEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.OverrideLocalAddress(sourceEndpoint);

                    ec.UseTransport<SqlServerTransport>()
                        .ConnectionString(sqlConnectionString);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = CreateTimeoutStorage(sourceEndpoint);
                    var timeoutTarget = new SqlTTimeoutsTarget(logger, sqlConnectionString, "dbo");
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, timeoutTarget);

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

        public class SqlTEndpoint : EndpointConfigurationBuilder
        {
            public SqlTEndpoint()
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