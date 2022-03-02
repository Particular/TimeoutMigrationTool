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
    using Particular.TimeoutMigrationTool.ASQ;

    [TestFixture]
#if SQLSERVER
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString, EnvironmentVariables.AzureStorageConnectionString)]
#endif
#if ORACLE
    [EnvironmentSpecificTest(EnvironmentVariables.OracleConnectionString, EnvironmentVariables.AzureStorageConnectionString)]
#endif
    class NHibernateToAsqEndToEnd : NHibernateAcceptanceTests
    {
        string asqConnectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.AzureStorageConnectionString);

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NHibernateSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(AsqTarget));

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    await testSession.SaveAsync(new TimeoutEntity
                    {
                        Endpoint = sourceEndpoint,
                        Destination = targetEndpoint,
                        SagaId = Guid.NewGuid(),
                        Headers = "{\"NServiceBus.EnclosedMessageTypes\": \"TimeoutMigrationTool.NHibernate.AcceptanceTests.NHibernateToAsqEndToEnd+DelayedMessage\"}",
                        State = Encoding.UTF8.GetBytes("{}"),
                        Time = DateTime.UtcNow.AddSeconds(15)
                    });

                    await testTx.CommitAsync();
                }
            }

            var context = await Scenario.Define<TargetContext>()
                // Create the legacy endpoint to forward the delayed message to the reporting endpoint
                // This is needed as ASQ stores the delayed messages at the sending endpoint until
                // delivery is needed
                .WithEndpoint<NHibernateSource>(b => b.CustomConfig(ec =>
                {
                    var transportConfig = ec.UseTransport<AzureStorageQueueTransport>();
                    transportConfig.ConnectionString(asqConnectionString);
                    transportConfig.DisablePublishing();

                    transportConfig.DelayedDelivery().DisableTimeoutManager();

                    ec.UseSerialization<NewtonsoftSerializer>();
                }))
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
                    var timeoutsSource = new NHibernateTimeoutsSource(connectionString, 512, DatabaseDialect);
                    var timeoutsTarget = new ASQTarget(asqConnectionString, new DelayedDeliveryTableNameProvider());
                    var migrationRunner = new MigrationRunner(logger, timeoutsSource, timeoutsTarget);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
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

        [Serializable]
        public class DelayedMessage : IMessage
        {
        }
    }
}