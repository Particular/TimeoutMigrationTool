namespace TimeoutMigrationTool.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting.Customization;
    using NServiceBus.AcceptanceTesting.Support;
    using NServiceBus.Features;
    using NUnit.Framework;
    using Raven.Client.Documents;
    using Raven.Client.Documents.Operations;
    using Raven.Client.Exceptions;
    using Raven.Client.Exceptions.Database;
    using Raven.Client.ServerWide;
    using Raven.Client.ServerWide.Operations;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    public class RavenDbEndpoint : IEndpointSetupTemplate
    {
        public async virtual Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor, EndpointCustomizationConfiguration endpointCustomizationConfiguration, Action<EndpointConfiguration> configurationBuilderCustomization)
        {
            var endpointConfiguration = new EndpointConfiguration(endpointCustomizationConfiguration.EndpointName);

            endpointConfiguration.TypesToIncludeInScan(endpointCustomizationConfiguration.GetTypesScopedByTestClass());

            endpointConfiguration.Recoverability()
                .Delayed(delayed => delayed.NumberOfRetries(0))
                .Immediate(immediate => immediate.NumberOfRetries(0));

            var storageDir = Path.Combine(NServiceBusAcceptanceTest.StorageRootDir, TestContext.CurrentContext.Test.ID);

            endpointConfiguration.EnableInstallers();

            var transport = endpointConfiguration.UseTransport<AcceptanceTestingTransport>();
            transport.StorageDirectory(storageDir);
            transport.UseNativeDelayedDelivery(false);

            var persistence = endpointConfiguration.UsePersistence<RavenDBPersistence>();

            var databaseName = "TimeoutMigrationTests";
            var documentStore = new DocumentStore
            {
                Urls = new[] { "http://localhost:8080" },
                Database = databaseName,
            };

            documentStore.Initialize();

            await EnsureDatabaseExistsAsync(documentStore, databaseName);

            persistence.SetDefaultDocumentStore(documentStore);

            endpointConfiguration.EnableFeature<TimeoutManager>();

            endpointConfiguration.RegisterComponentsAndInheritanceHierarchy(runDescriptor);

            configurationBuilderCustomization(endpointConfiguration);

            return endpointConfiguration;
        }

        async Task EnsureDatabaseExistsAsync(IDocumentStore store, string database)
        {
            database = database ?? store.Database;

            if (string.IsNullOrWhiteSpace(database))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(database));

            try
            {
                await store.Maintenance.ForDatabase(database).SendAsync(new GetStatisticsOperation());
            }
            catch (DatabaseDoesNotExistException)
            {
                try
                {
                    await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(database)));
                }
                catch (ConcurrencyException)
                {
                }
            }
        }
    }
}