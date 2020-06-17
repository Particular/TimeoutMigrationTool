namespace TimeoutMigrationTool.SqlP.FakeData
{
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using NServiceBus;

    class Program
    {
        static async Task Main(string[] args)
        {
            var endpointConfiguration = new EndpointConfiguration("SqlP.FakeTimeouts");

            var noOfTimeouts = 20000;

            var persistence = endpointConfiguration.UsePersistence<SqlPersistence>();
            var connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=TimeoutTest;Integrated Security=True;";
            persistence.SqlDialect<SqlDialect.MsSqlServer>();
            persistence.ConnectionBuilder(
                connectionBuilder: () =>
                {
                    return new SqlConnection(connectionString);
                });
            var subscriptions = persistence.SubscriptionSettings();
            subscriptions.CacheFor(TimeSpan.FromMinutes(1));

            endpointConfiguration.UseTransport<MsmqTransport>();
            endpointConfiguration.SendFailedMessagesTo("error");
            endpointConfiguration.EnableInstallers();

            var endpointInstance = await Endpoint.Start(endpointConfiguration)
                .ConfigureAwait(false);

            var message = new FakeMessage { Topic="bla" };
            Random random = new Random();
            for (int i = 0; i < noOfTimeouts; i++)
            {
                var daysToTrigger = random.Next(2, 60); // randomize the Time property

                var options = new SendOptions();
                options.DelayDeliveryWith(TimeSpan.FromDays(daysToTrigger));
                options.SetDestination( i % 10 == 0 ? "DestinationEndpoint" : "EndpointB");

                await endpointInstance.Send(message, options).ConfigureAwait(false);
            }

            await endpointInstance.Stop()
                .ConfigureAwait(false);
        }

    }

    class FakeMessage : IMessage
    {
        public string Topic { get; set; }
    }
}