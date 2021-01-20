namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using Microsoft.Data.SqlClient;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.Nhb;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.IO;
    using System.Text;

    [TestFixture]
    class NHibernateToRabbitMqEndToEnd : NHibernateAcceptanceTests
    {
        string rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

        [Test]
        public async Task Can_migrate_timeouts()
        {
            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    await testSession.SaveAsync(new TimeoutEntity
                    {
                        Endpoint = "NewRabbitMqEndpoint",
                        Destination = "NewRabbitMqEndpoint",
                        SagaId = Guid.NewGuid(),
                        Headers = "{\"NServiceBus.EnclosedMessageTypes\": \"TimeoutMigrationTool.NHibernate.AcceptanceTests.NHibernateToRabbitMqEndToEnd+DelayedMessage\"}",
                        State = Encoding.UTF8.GetBytes("<DelayedMessage></DelayedMessage>"),
                        Time = DateTime.UtcNow.AddSeconds(15)
                    });

                    await testTx.CommitAsync();
                }
            }

            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint));

            var context = await Scenario.Define<TargetTestContext>()
                .WithEndpoint<NewRabbitMqEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UseTransport<RabbitMQTransport>()
                    .ConnectionString(rabbitUrl);
                })
                .When(async (_, c) =>
                {
                    var logger = new TestLoggingAdapter(c);
                    var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1024, DatabaseDialect);
                    var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl);
                    var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);

                    await migrationRunner.Run(DateTime.Now.AddDays(-10), EndpointFilter.SpecificEndpoint("NewRabbitMqEndpoint"), new Dictionary<string, string>());
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(30));

            Assert.True(context.GotTheDelayedMessage);
        }

        public class TargetTestContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class NewRabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public NewRabbitMqEndpoint()
            {
                EndpointSetup<RabbitMqEndpoint>();
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

        byte[] ObjectToByteArray(object obj)
        {
            if (obj == null)
                return null;
            BinaryFormatter bf = new BinaryFormatter();
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, obj);
                return ms.ToArray();
            }
        }
    }
}