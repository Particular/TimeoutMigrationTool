using System.Threading.Tasks;
using NServiceBus;

namespace TimeoutMigrationTool.RabbitMq.FakeTarget
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var endpointConfigA = new EndpointConfiguration("A");
            SetupEndpointConfig(endpointConfigA);

            var endpointConfigB = new EndpointConfiguration("B");
            SetupEndpointConfig(endpointConfigB);

            var endpointConfigC = new EndpointConfiguration("C");
            SetupEndpointConfig(endpointConfigC);

            await Endpoint.Start(endpointConfigA);
            await Endpoint.Start(endpointConfigB);
            await Endpoint.Start(endpointConfigC);
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