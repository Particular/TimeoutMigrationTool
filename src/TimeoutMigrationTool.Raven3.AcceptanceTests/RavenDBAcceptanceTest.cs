﻿namespace TimeoutMigrationTool.Raven3.AcceptanceTests
{
    using NUnit.Framework;
    using Raven.Client;
    using Raven.Client.Document;
    using System;
    using System.Threading.Tasks;

    public abstract class RavenDBAcceptanceTest : NServiceBusAcceptanceTest
    {
        public override async Task SetUp()
        {
            await base.SetUp();

            serverUrl = Environment.GetEnvironmentVariable("Raven35Url") ?? "http://localhost:8383";

            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

            databaseName = TestContext.CurrentContext.Test.ID;
        }

        public override async Task TearDown()
        {
            await base.SetUp();

            await DeleteDatabase(serverUrl, databaseName);
        }

        protected DocumentStore GetDocumentStore(string url, string dbName)
        {
            return GetInitializedDocumentStore(url, dbName);
        }

        static DocumentStore GetInitializedDocumentStore(string url, string defaultDatabase)
        {
            var apiKey = Environment.GetEnvironmentVariable("RavenDbApiKey");
            var documentStore = new DocumentStore
            {
                Url = url,
                DefaultDatabase = defaultDatabase,
                ApiKey = apiKey
            };

            documentStore.Initialize();

            return documentStore;
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
                        await storeForDeletion.AsyncDatabaseCommands.GlobalAdmin.DeleteDatabaseAsync(dbName, true);
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