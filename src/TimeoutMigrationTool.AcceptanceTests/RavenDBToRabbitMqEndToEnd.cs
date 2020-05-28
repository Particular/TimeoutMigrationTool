namespace TimeoutMigrationTool.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RabbitMq;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven.Client.Documents;
    using Raven.Client.ServerWide;
    using Raven.Client.ServerWide.Operations;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    [TestFixture]
    class RavenDBToRabbitMqEndToEnd : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            var sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(LegacyRavenDBEndpoint));
            var targetEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(NewRabbitMqEndpoint));

            var ravenTimeoutPrefix = "TimeoutDatas";
            var ravenVersion = RavenDbVersion.Four;

            var ravenAdapter = new Raven4Adapter(serverUrl, databaseName);

            var context = await Scenario.Define<Context>()
                .WithEndpoint<LegacyRavenDBEndpoint>(b => b.CustomConfig(ec =>
                {
                    ec.UsePersistence<RavenDBPersistence>()
                        .SetDefaultDocumentStore(GetDocumentStore(serverUrl, databaseName));

                })
                .When(async (session, c) =>
                {
                    var delayedMessage = new DelayedMessage();

                    var options = new SendOptions();

                    options.DelayDeliveryWith(TimeSpan.FromSeconds(5));
                    options.SetDestination(targetEndpoint);

                    await session.Send(delayedMessage, options);

                    await WaitUntilTheTimeoutIsSavedInRaven(ravenAdapter, sourceEndpoint);

                    c.TimeoutSet = true;
                }))
                .Done(c => c.TimeoutSet)
                .Run();

            var timeoutStorage = new RavenDBTimeoutStorage(serverUrl, databaseName, ravenTimeoutPrefix, ravenVersion);
            var transportAdapter = new RabbitMqTimeoutCreator(rabbitUrl);
            var migrationRunner = new MigrationRunner(timeoutStorage, transportAdapter);

            context = await Scenario.Define<Context>()
             .WithEndpoint<NewRabbitMqEndpoint>(b => b.CustomConfig(ec =>
             {
                 ec.UseTransport<RabbitMQTransport>()
                 .ConnectionString(rabbitUrl);

             })
             .When(async (session, c) =>
                {
                    await migrationRunner.Run(DateTime.Now.AddDays(-1), EndpointFilter.SpecificEndpoint(targetEndpoint), new Dictionary<string, string>());
                })
             )
             .Done(c => c.GotTheDelayedMessage)
             .Run();

            Assert.True(context.GotTheDelayedMessage);
        }


        static async Task WaitUntilTheTimeoutIsSavedInRaven(Raven4Adapter ravenAdapter, string endpoint)
        {
            do
            {
                var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(x =>
                    x.OwningTimeoutManager.Equals(endpoint,
                        StringComparison.OrdinalIgnoreCase), "TimeoutDatas", (doc, id) => doc.Id = id);

                if (timeouts.Count > 0)
                {
                    return;
                }

                await Task.Delay(300);
            }
            while (true);
        }

        public class Context : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
            public bool GotTheDelayedMessage { get; set; }
        }

        public class LegacyRavenDBEndpoint : EndpointConfigurationBuilder
        {
            public LegacyRavenDBEndpoint()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }
        }

        public class NewRabbitMqEndpoint : EndpointConfigurationBuilder
        {
            public NewRabbitMqEndpoint()
            {
                EndpointSetup<RabbitMqEndpoint>();
            }

            class DelayedMessageHandler : IHandleMessages<DelayedMessage>
            {
                public DelayedMessageHandler(Context testContext)
                {
                    this.testContext = testContext;
                }

                public Task Handle(DelayedMessage message, IMessageHandlerContext context)
                {
                    testContext.GotTheDelayedMessage = true;
                    return Task.CompletedTask;
                }

                readonly Context testContext;
            }
        }

        public class DelayedMessage : IMessage
        {
        }

        public override async Task SetUp()
        {
            await base.SetUp();

            serverUrl = Environment.GetEnvironmentVariable("CommaSeparatedRavenClusterUrls") ?? "http://localhost:8080";

            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

            databaseName = TestContext.CurrentContext.Test.ID;
        }

        public override async Task TearDown()
        {
            await base.SetUp();

            await DeleteDatabase(serverUrl, databaseName);
        }

        static DocumentStore GetDocumentStore(string urls, string dbName)
        {
            var documentStore = GetInitializedDocumentStore(urls, dbName);

            CreateDatabase(documentStore, dbName);

            return documentStore;
        }

        static DocumentStore GetInitializedDocumentStore(string urls, string defaultDatabase)
        {
            var documentStore = new DocumentStore
            {
                Urls = urls.Split(','),
                Database = defaultDatabase
            };

            documentStore.Initialize();

            return documentStore;
        }

        static void CreateDatabase(IDocumentStore defaultStore, string dbName)
        {
            var dbRecord = new DatabaseRecord(dbName);
            defaultStore.Maintenance.Server.Send(new CreateDatabaseOperation(dbRecord));
        }

        static async Task DeleteDatabase(string urls, string dbName)
        {
            // Periodically the delete will throw an exception because Raven has the database locked
            // To solve this we have a retry loop with a delay
            var triesLeft = 3;

            while (triesLeft-- > 0)
            {
                try
                {
                    // We are using a new store because the global one is disposed of before cleanup
                    using (var storeForDeletion = GetInitializedDocumentStore(urls, dbName))
                    {
                        storeForDeletion.Maintenance.Server.Send(new DeleteDatabasesOperation(storeForDeletion.Database, hardDelete: true));
                        break;
                    }
                }
                catch
                {
                    if (triesLeft == 0)
                    {
                        throw;
                    }

                    await Task.Delay(250);
                }
            }

            Console.WriteLine("Deleted '{0}' database", dbName);
        }

        string databaseName = "";
        string serverUrl = "";
        string rabbitUrl = "";
    }
}