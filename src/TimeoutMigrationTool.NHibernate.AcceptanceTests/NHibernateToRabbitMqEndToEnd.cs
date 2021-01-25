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

    [TestFixture]
    class NHibernateToRabbitMqEndToEnd : NHibernateAcceptanceTests
    {
        string rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = "SomeRandomEndpointName";
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqEndpoint));

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
                        State = Encoding.UTF8.GetBytes("<DelayedMessage></DelayedMessage>"),
                        Time = DateTime.UtcNow.AddSeconds(15)
                    });

                    await testTx.CommitAsync();
                }
            }

            var context = await Scenario.Define<TargetTestContext>()
                .WithEndpoint<RabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutsSource = new NHibernateTimeoutsSource(connectionString, 1024, DatabaseDialect);
                    var timeoutsTarget = new RabbitMqTimeoutTarget(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutsSource, timeoutsTarget);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }

        public class TargetTestContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class RabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public RabbitMqEndpoint()
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

        [Serializable]
        public class DelayedMessage : IMessage
        {
        }
    }
}