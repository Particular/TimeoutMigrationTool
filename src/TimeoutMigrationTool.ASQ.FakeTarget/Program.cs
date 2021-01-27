namespace TimeoutMigrationTool.ASQ.FakeTarget
{
    using System.Threading.Tasks;
    using NServiceBus;
    using NServiceBus.Features;

    class Program
    {
        static async Task Main()
        {
            var endpointConfigA = new EndpointConfiguration("EndpointA");
            SetupEndpointConfig(endpointConfigA);

            var endpointConfigB = new EndpointConfiguration("EndpointB");
            SetupEndpointConfig(endpointConfigB);

            var endpointConfigC = new EndpointConfiguration("EndpointC");
            SetupEndpointConfig(endpointConfigC);

            var destinationEndpointConfig = new EndpointConfiguration("DestinationEndpoint");
            SetupEndpointConfig(destinationEndpointConfig);

            var endpointA = await Endpoint.Start(endpointConfigA).ConfigureAwait(false);
            var endpointB = await Endpoint.Start(endpointConfigB).ConfigureAwait(false);
            var endpointC = await Endpoint.Start(endpointConfigC).ConfigureAwait(false);
            var destinationEndpoint = await Endpoint.Start(destinationEndpointConfig).ConfigureAwait(false);

            await endpointA.Stop().ConfigureAwait(false);
            await endpointB.Stop().ConfigureAwait(false);
            await endpointC.Stop().ConfigureAwait(false);
            await destinationEndpoint.Stop().ConfigureAwait(false);
        }

        static void SetupEndpointConfig(EndpointConfiguration endpointConfig)
        {
            endpointConfig.SendFailedMessagesTo("error");
            endpointConfig.AuditProcessedMessagesTo("audit");
            endpointConfig.UseTransport<AzureStorageQueueTransport>().ConnectionString("UseDevelopmentStorage=true;");
            endpointConfig.EnableInstallers();
            endpointConfig.UseSerialization<NewtonsoftSerializer>();
            endpointConfig.DisableFeature<TimeoutManager>();
            endpointConfig.DisableFeature<MessageDrivenSubscriptions>();
        }
    }
}