﻿namespace TimeoutMigrationTool.Raven4.AcceptanceTests
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;
    using Raven.Client.Documents;
    using Raven.Client.ServerWide;
    using Raven.Client.ServerWide.Operations;

    public abstract class RavenDBAcceptanceTest
    {
        [SetUp]
        public Task SetUp()
        {
            NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention = t =>
            {
                var classAndEndpoint = t.FullName.Split('.').Last();

                var testName = classAndEndpoint.Split('+').First();

                testName = testName.Replace("When_", "");

                var endpointBuilder = classAndEndpoint.Split('+').Last();


                testName = Thread.CurrentThread.CurrentCulture.TextInfo.ToTitleCase(testName);

                testName = testName.Replace("_", "");

                // A staging table from one framework run that is deleted will cause another attempt to create it to result in Conflict exception
                // for several seconds after the delete occurs, causing a problem for other target framework test runs unless the endpoint
                // names are different
                return $"{testName}-{endpointBuilder}-net{Environment.Version.Major}";
            };

            serverUrl = Environment.GetEnvironmentVariable(EnvironmentVariables.Raven4Url);

            rabbitUrl = $"amqp://guest:guest@{Environment.GetEnvironmentVariable(EnvironmentVariables.RabbitMqHost)}:5672";

            databaseName = TestContext.CurrentContext.Test.ID;

            return Task.CompletedTask;
        }

        [TearDown]
        public Task TearDown()
        {
            return DeleteDatabase(serverUrl, databaseName);
        }

        protected DocumentStore GetDocumentStore(string urls, string dbName)
        {
            var documentStore = GetInitializedDocumentStore(urls, dbName);

            CreateDatabase(documentStore, dbName);

            return documentStore;
        }

        protected static async Task WaitUntilTheTimeoutIsSavedInRaven(ICanTalkToRavenVersion ravenAdapter, string endpoint)
        {
            while (true)
            {
                var timeouts = await ravenAdapter.GetDocuments<TimeoutData>(
                    x =>
                        x.OwningTimeoutManager.Equals(
                            endpoint,
                            StringComparison.OrdinalIgnoreCase),
                    "TimeoutDatas",
                    (doc, id) => doc.Id = id);

                if (timeouts.Count > 0)
                {
                    return;
                }
            }
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

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            if (Directory.Exists(StorageRootDir))
            {
                Directory.Delete(StorageRootDir, true);
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            if (Directory.Exists(StorageRootDir))
            {
                Directory.Delete(StorageRootDir, true);
            }
        }

        public static string StorageRootDir
        {
            get
            {
                string tempDir;

                if (Environment.OSVersion.Platform == PlatformID.Win32NT)
                {
                    //can't use bin dir since that will be too long on the build agents
                    tempDir = @"c:\temp";
                }
                else
                {
                    tempDir = Path.GetTempPath();
                }

                return Path.Combine(tempDir, "timeoutmigrationtool-accpt-tests");
            }
        }
    }
}