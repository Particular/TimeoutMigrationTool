namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting.Customization;
    using NServiceBus.AcceptanceTesting.Support;
    using NUnit.Framework;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    public class LegacyTimeoutManagerEndpoint : IEndpointSetupTemplate
    {
        public virtual async Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor, EndpointCustomizationConfiguration endpointCustomizationConfiguration, Func<EndpointConfiguration, Task> configurationBuilderCustomization)
        {
            var endpointConfiguration = new EndpointConfiguration(endpointCustomizationConfiguration.EndpointName);

            endpointConfiguration.TypesToIncludeInScan(endpointCustomizationConfiguration.GetTypesScopedByTestClass());

            endpointConfiguration.Recoverability()
                .Delayed(delayed => delayed.NumberOfRetries(0))
                .Immediate(immediate => immediate.NumberOfRetries(0));

            endpointConfiguration.EnableInstallers();

            var storageDir = Path.Combine(AspAcceptanceTest.StorageRootDir, TestContext.CurrentContext.Test.ID);
            endpointConfiguration.UseTransport(new AcceptanceTestingTransport(enableNativeDelayedDelivery: false)
            {
                StorageLocation = storageDir
            });

            endpointConfiguration.RegisterComponentsAndInheritanceHierarchy(runDescriptor);

            await configurationBuilderCustomization(endpointConfiguration);

            return endpointConfiguration;
        }
    }
}
