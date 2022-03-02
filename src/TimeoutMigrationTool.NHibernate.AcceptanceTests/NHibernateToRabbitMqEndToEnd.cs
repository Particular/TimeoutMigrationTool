namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.NHibernate;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Text;
    using NServiceBus.Features;

    [TestFixture]
#if SQLSERVER
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString, EnvironmentVariables.RabbitMqHost)]
#endif
#if ORACLE
    [EnvironmentSpecificTest(EnvironmentVariables.OracleConnectionString, EnvironmentVariables.RabbitMqHost)]
#endif
    class NHibernateToRabbitMqEndToEnd : NHibernateAcceptanceTests
    {
        string rabbitUrl = $"amqp://guest:guest@{Environment.GetEnvironmentVariable(EnvironmentVariables.RabbitMqHost)}:5672";

        [Test]
        [Explicit("In CI this test exceeds the hardcoded 60 second timeout in the acceptance test framework for 'Executing given and whens'.")]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NHibernateSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqTarget));

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    await testSession.SaveAsync(new TimeoutEntity
                    {
                        Endpoint = sourceEndpoint,
                        Destination = targetEndpoint,
                        SagaId = Guid.NewGuid(),
                        Headers = "{\"NServiceBus.EnclosedMessageTypes\": \"TimeoutMigrationTool.NHibernate.AcceptanceTests.NHibernateToRabbitMqEndToEnd+DelayedMessage\"}",
                        State = Encoding.UTF8.GetBytes("{}"),
                        Time = DateTime.UtcNow.AddSeconds(15)
                    });

                    await testTx.CommitAsync();
                }
            }

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<RabbitMqTarget>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);

                    ec.UseSerialization<NewtonsoftSerializer>();
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutsSource = new NHibernateTimeoutsSource(connectionString, 512, DatabaseDialect);
                    var timeoutsTarget = new RabbitMqTimeoutTarget(logger, rabbitUrl);
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

        public class RabbitMqTarget : EndpointConfigurationBuilder
        {
            public RabbitMqTarget()
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