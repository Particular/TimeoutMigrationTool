namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NUnit.Framework;
    using System;
    using System.Data.SqlClient;
    using System.Threading.Tasks;
    using Msmq.AcceptanceTests;
    using Conventions = NServiceBus.AcceptanceTesting.Customization.Conventions;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.SQLServerConnectionString)]
    class SqlPToMsmqEndToEnd : SqlPAcceptanceTest
    {
        [Test]
        public async Task Can_migrate_timeouts()
        {
            await Scenario.Define<SourceContext>()
                 .WithEndpoint<EndpointBeforeMigration>(b => b.CustomConfig(ec =>
                 {
                     Console.WriteLine("EndpointBeforeMigration connectionString: " + connectionString);
                     var persistence = ec.UsePersistence<SqlPersistence>();
                     persistence.SubscriptionSettings().DisableCache();
                     persistence.SqlDialect<SqlDialect.MsSqlServer>();
                     persistence.ConnectionBuilder(
                         connectionBuilder: () =>
                         {
                             return new SqlConnection(connectionString);
                         });
                 })
                 .When(async (session, c) =>
                 {
                     var delayedMessage = new DelayedMessage();
                     var options = new SendOptions();

                     options.DelayDeliveryWith(TimeSpan.FromSeconds(30));
                     options.RouteToThisEndpoint();

                     await session.Send(delayedMessage, options);

                     await WaitUntilTheTimeoutIsSavedInSql(Conventions.EndpointNamingConvention(typeof(Endpoint)));

                     c.TimeoutSet = true;
                 }))
                 .Done(c => c.TimeoutSet)
                 .Run(TimeSpan.FromSeconds(60));

            var setupContext = await Scenario.Define<TargetContext>()
                .WithEndpoint<Endpoint>(b => b.CustomConfig(ec =>
                {
                    Console.WriteLine("MsmqEndpoint connectionString: " + connectionString);
                    var transport = ec.UseTransport<MsmqTransport>();
                    transport.NativeDelayedDelivery(new SqlServerDelayedMessageStore(connectionString));

                    //To ensure we don't pick up the timeout via the timeout manager
                    var persistence = ec.UsePersistence<InMemoryPersistence>();
                }))
                .Done(c => c.EndpointsStarted)
                .Run(TimeSpan.FromSeconds(90));


            MigrationRunner.Run(connectionString);

            var context = await Scenario.Define<TargetContext>()
                .WithEndpoint<Endpoint>(b => b.CustomConfig(ec =>
                {
                    Console.WriteLine("MsmqEndpoint after migration connectionString: " + connectionString);
                    var transport = ec.UseTransport<MsmqTransport>();
                    transport.NativeDelayedDelivery(new SqlServerDelayedMessageStore(connectionString));

                    //To ensure we don't pick up the timeout via the timeout manager
                    var persistence = ec.UsePersistence<InMemoryPersistence>();
                }))
                .Done(c => c.GotTheDelayedMessage)
                .Run(TimeSpan.FromSeconds(120));

            Assert.True(context.GotTheDelayedMessage);
        }

        public class SourceContext : ScenarioContext
        {
            public bool TimeoutSet { get; set; }
        }

        public class TargetContext : ScenarioContext
        {
            public bool GotTheDelayedMessage { get; set; }
        }

        public class EndpointBeforeMigration : EndpointConfigurationBuilder
        {
            public EndpointBeforeMigration()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>().CustomEndpointName(Conventions.EndpointNamingConvention(typeof(Endpoint)));
            }
        }

        public class Endpoint : EndpointConfigurationBuilder
        {
            public Endpoint()
            {
                EndpointSetup<DefaultServer>();
            }

            class DelayedMessageHandler : IHandleMessages<DelayedMessage>
            {
                public DelayedMessageHandler(TargetContext context)
                {
                    this.context = context;
                }

                public Task Handle(DelayedMessage message, IMessageHandlerContext context)
                {
                    this.context.GotTheDelayedMessage = true;
                    return Task.CompletedTask;
                }

                readonly TargetContext context;
            }
        }

        public class DelayedMessage : IMessage
        {
        }
    }
}