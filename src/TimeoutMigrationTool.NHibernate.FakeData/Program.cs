namespace TimeoutMigrationTool.NHibernate.FakeData
{
    using System;
    using System.Threading.Tasks;
    using global::NHibernate.Cfg;
    using NServiceBus;
    using NServiceBus.Persistence;
    using NServiceBus.Persistence.NHibernate;

    class Program
    {
        static async Task Main(string[] args)
        {
            var endpointConfiguration = new EndpointConfiguration("NHibernateEndpoint.FakeTimeouts");
            var connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog=NHibernateTests;Integrated Security=True;";
            var nhConfiguration = new Configuration
            {
                Properties =
                {
                    ["dialect"] = "NHibernate.Dialect.MsSql2008Dialect",
                    ["connection.connection_string"] = connectionString
                }
            };
            var noOfTimeouts = 20000;

            var persistence = endpointConfiguration.UsePersistence<NHibernatePersistence>();

            persistence.ConnectionString(connectionString);
            persistence.UseTimeoutStorageConfiguration(nhConfiguration);

            endpointConfiguration.UseTransport<MsmqTransport>();
            endpointConfiguration.SendFailedMessagesTo("error");
            endpointConfiguration.EnableInstallers();

            var endpointInstance = await Endpoint.Start(endpointConfiguration);

            var message = new FakeMessage { Topic = "bla" };
            var random = new Random();
            for (var i = 0; i < noOfTimeouts; i++)
            {
                var daysToTrigger = random.Next(2, 60); // randomize the Time property

                var options = new SendOptions();
                options.DelayDeliveryWith(TimeSpan.FromDays(daysToTrigger));
                options.SetDestination(i % 10 == 0 ? "DestinationEndpoint" : "EndpointB");

                await endpointInstance.Send(message, options).ConfigureAwait(false);
            }

            await endpointInstance.Stop();
        }
    }

    class FakeMessage : IMessage
    {
        public string Topic { get; set; }
    }
}