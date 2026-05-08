namespace TimeoutMigrationTool.Raven3.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven.Abstractions.Data;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading.Tasks;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.RabbitMqHost, EnvironmentVariables.Raven3Url)]
    class RavenDBToRabbitMqEndToEnd : RavenDBAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RavenDBSource));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(RabbitMqTarget));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.ThreeDotFive;

            await StoreLegacyTimeout(sourceEndpoint, targetEndpoint, ravenTimeoutPrefix);

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<RabbitMqTarget>(b => b.CustomConfig(ec =>
                    {
                        ec.UseTransport<RabbitMQTransport>()
                            .ConnectionString(rabbitUrl);
                    })
                    .When(async (_, c) =>
                    {
                        var logger = new TestLoggingAdapter();
                        var timeoutStorage = new RavenDbTimeoutsSource(logger, serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion, false);
                        var transportAdapter = new RabbitMqTimeoutTarget(logger, rabbitUrl, false);
                        var migrationRunner = new MigrationRunner(logger, timeoutStorage, transportAdapter);
                        await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(sourceEndpoint), new Dictionary<string, string>());
                    }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(60)).Token);

            Assert.That(context.GotTheDelayedMessage, Is.True);
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

        public class RabbitMqTarget : EndpointConfigurationBuilder
        {
            public RabbitMqTarget()
            {
                EndpointSetup<RabbitMqEndpoint>();
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

        async Task StoreLegacyTimeout(string sourceEndpoint, string targetEndpoint, string ravenTimeoutPrefix)
        {
            using var documentStore = GetDocumentStore(serverUrl, databaseName);
            await documentStore.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(new DatabaseDocument
            {
                Id = databaseName
            });

            using var session = documentStore.OpenAsyncSession();
            await session.StoreAsync(new TimeoutData
            {
                Destination = targetEndpoint,
                SagaId = Guid.NewGuid(),
                OwningTimeoutManager = sourceEndpoint,
                Time = DateTime.UtcNow.AddSeconds(5),
                Headers = new Dictionary<string, string>
                {
                    { "NServiceBus.ContentType", "application/json" },
                    { "NServiceBus.EnclosedMessageTypes", typeof(DelayedMessage).AssemblyQualifiedName },
                    { "NServiceBus.MessageId", Guid.NewGuid().ToString("N") }
                },
                State = Encoding.UTF8.GetBytes("{}")
            }, $"{ravenTimeoutPrefix}/1");

            await session.SaveChangesAsync();
        }
    }
}
