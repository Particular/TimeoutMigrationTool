namespace TimeoutMigrationTool.RabbitMq.FakeTarget
{
    using System.Threading.Tasks;
    using NServiceBus;

    class Program
    {
        static async Task Main(string[] args)
        {
            var endpointConfigA = new EndpointConfiguration("EndpointA");
            SetupEndpointConfig(endpointConfigA);

            var endpointConfigB = new EndpointConfiguration("EndpointB");
            SetupEndpointConfig(endpointConfigB);

            var endpointConfigC = new EndpointConfiguration("EndpointC");
            SetupEndpointConfig(endpointConfigC);

            var destinationEndpointConfig = new EndpointConfiguration("DestinationEndpoint");
            SetupEndpointConfig(destinationEndpointConfig);

            var endpointA = await Endpoint.Start(endpointConfigA);
            var endpointB = await Endpoint.Start(endpointConfigB);
            var endpointC = await Endpoint.Start(endpointConfigC);
            var destinationEndpoint = await Endpoint.Start(destinationEndpointConfig);

            await endpointA.Stop();
            await endpointB.Stop();
            await endpointC.Stop();
            await destinationEndpoint.Stop();
        }

        private static void SetupEndpointConfig(EndpointConfiguration endpointConfig)
        {
            endpointConfig.SendFailedMessagesTo("error");
            endpointConfig.AuditProcessedMessagesTo("audit");
            endpointConfig.UseTransport<RabbitMQTransport>().UseConventionalRoutingTopology()
                .ConnectionString("host=localhost;username=guest;password=guest");
            endpointConfig.EnableInstallers();
        }
    }
}