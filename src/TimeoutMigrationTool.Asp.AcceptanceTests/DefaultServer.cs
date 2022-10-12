namespace TimeoutMigrationTool.Asp.AcceptanceTests
{
    using System;
    using System.Threading.Tasks;
    using NServiceBus;
    using NServiceBus.AcceptanceTesting.Customization;
    using NServiceBus.AcceptanceTesting.Support;

    public class DefaultServer : IEndpointSetupTemplate
    {
        public virtual Task<EndpointConfiguration> GetConfiguration(RunDescriptor runDescriptor, EndpointCustomizationConfiguration endpointCustomizationConfiguration, Action<EndpointConfiguration> configurationBuilderCustomization)
        {
            var endpointConfiguration = new EndpointConfiguration(endpointCustomizationConfiguration.EndpointName);

            endpointConfiguration.TypesToIncludeInScan(endpointCustomizationConfiguration.GetTypesScopedByTestClass());

            endpointConfiguration.Recoverability()
                .Delayed(delayed => delayed.NumberOfRetries(0))
                .Immediate(immediate => immediate.NumberOfRetries(0));

            endpointConfiguration.EnableInstallers();

            var transport = endpointConfiguration.UseTransport<RabbitMQTransport>();

            transport.UseConventionalRoutingTopology(QueueType.Quorum);

            endpointConfiguration.RegisterComponentsAndInheritanceHierarchy(runDescriptor);

            configurationBuilderCustomization(endpointConfiguration);

            return Task.FromResult(endpointConfiguration);
        }
    }
}