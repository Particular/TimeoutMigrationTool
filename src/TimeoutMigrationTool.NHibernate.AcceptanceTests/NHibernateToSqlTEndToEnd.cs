namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.NHibernate;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Text;
    using NServiceBus.Features;
    using Particular.TimeoutMigrationTool.SqlT;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.SQLServerConnectionString)]
    class NHibernateToSqlTEndToEnd : NHibernateAcceptanceTests
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NHibernateSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlTTarget));

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    await testSession.SaveAsync(new TimeoutEntity
                    {
                        Endpoint = sourceEndpoint,
                        Destination = targetEndpoint,
                        SagaId = Guid.NewGuid(),
                        Headers = "{\"NServiceBus.EnclosedMessageTypes\": \"TimeoutMigrationTool.NHibernate.AcceptanceTests.NHibernateToSqlTEndToEnd+DelayedMessage\"}",
                        State = Encoding.UTF8.GetBytes("{}"),
                        Time = DateTime.UtcNow.AddSeconds(15)
                    });

                    await testTx.CommitAsync();
                }
            }

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<SqlTTarget>(b => b.CustomConfig(ec =>
                    {
                        ec.OverrideLocalAddress(sourceEndpoint);
                        var transportConfig = ec.UseTransport<SqlServerTransport>();

                        transportConfig.ConnectionString(connectionString);

                        ec.UseSerialization<NewtonsoftSerializer>();
                    })
                    .When(async (_, c) =>
                    {
                        var logger = new TestLoggingAdapter(c);
                        var timeoutsSource = new NHibernateTimeoutsSource(connectionString, 1024, DatabaseDialect);
                        var timeoutTarget = new SqlTTimeoutsTarget(logger, connectionString, "dbo");

                        var migrationRunner = new MigrationRunner(logger, timeoutsSource, timeoutTarget);

                        await migrationRunner.Run(DateTime.Now.AddDays(-10),
                            EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                    }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(90));

            Assert.True(context.GotTheDelayedMessage);
        }

        public class TargetContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class NHibernateSource : EndpointConfigurationBuilder
        {
            public NHibernateSource()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>(ec =>
                {
                    ec.DisableFeature<Sagas>();
                });
            }
        }

        public class SqlTTarget : EndpointConfigurationBuilder
        {
            public SqlTTarget()
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

        [Serializable]
        public class DelayedMessage : IMessage
        {
        }
    }
}