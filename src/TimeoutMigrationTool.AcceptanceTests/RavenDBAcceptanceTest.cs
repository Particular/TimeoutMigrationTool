namespace TimeoutMigrationTool.AcceptanceTests
{
    using NUnit.Framework;
    using Raven.Client.Documents;
    using Raven.Client.ServerWide;
    using Raven.Client.ServerWide.Operations;
    using System;
    using System.Threading.Tasks;

    public abstract class RavenDBAcceptanceTest : NServiceBusAcceptanceTest
    {
        public override async Task SetUp()
        {
            await base.SetUp();

            serverUrl = Environment.GetEnvironmentVariable("CommaSeparatedRavenClusterUrls") ?? "http://localhost:8080";

            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

            databaseName = TestContext.CurrentContext.Test.ID;
        }

        public override async Task TearDown()
        {
            await base.TearDown();

            await DeleteDatabase(serverUrl, databaseName);
        }

        protected DocumentStore GetDocumentStore(string urls, string dbName)
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

        protected string databaseName = "";
        protected string serverUrl = "";
        protected string rabbitUrl = "";
    }
}