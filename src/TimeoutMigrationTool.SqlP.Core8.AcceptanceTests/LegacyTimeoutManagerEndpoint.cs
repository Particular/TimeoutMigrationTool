namespace TimeoutMigrationTool.SqlP.Core8.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting.Customization;
    using NServiceBus.AcceptanceTesting.Support;
    using NServiceBus.Features;
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    public class LegacyTimeoutManagerEndpoint : IEndpointSetupTemplate
    {
        public virtual Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor, EndpointCustomizationConfiguration endpointCustomizationConfiguration, Action<EndpointConfiguration> configurationBuilderCustomization)
        {
            var endpointConfiguration = new EndpointConfiguration(endpointCustomizationConfiguration.EndpointName);

            endpointConfiguration.TypesToIncludeInScan(endpointCustomizationConfiguration.GetTypesScopedByTestClass());

            endpointConfiguration.Recoverability()
                .Delayed(delayed => delayed.NumberOfRetries(0))
                .Immediate(immediate => immediate.NumberOfRetries(0));

            var storageDir = Path.Combine(SqlPToRabbitMqEndToEnd.StorageRootDir, TestContext.CurrentContext.Test.ID);

            endpointConfiguration.EnableInstallers();

            var transport = endpointConfiguration.UseTransport<AcceptanceTestingTransport>();
            transport.StorageDirectory(storageDir);
            transport.UseNativeDelayedDelivery(false);

            endpointConfiguration.EnableFeature<TimeoutManager>();

            endpointConfiguration.RegisterComponentsAndInheritanceHierarchy(runDescriptor);

            configurationBuilderCustomization(endpointConfiguration);

            return Task.FromResult(endpointConfiguration);
        }
    }
}