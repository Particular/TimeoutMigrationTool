using NServiceBus;
using NServiceBus.AcceptanceTesting;
using NServiceBus.Persistence.Sql;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.SqlP;
using System;
using System.Threading.Tasks;

namespace TimeoutMigrationTool.AcceptanceTests
{
    [TestFixture]
    class SqlTimeoutsReaderTests : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Can_load_the_timeouts_scheduled_by_an_endpoint()
        {
            await Scenario.Define<Context>()
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.TimeoutsSet)
                .Run();

            var reader = new SqlTimeoutsReader();
            var timeouts = await reader.ReadTimeoutsFrom(MsSqlMicrosoftDataClientHelper.GetConnectionString(), "Sqltimeoutsreadertests_SqlP_WithTimeouts_Endpoint_TimeoutData", new MsSqlServer(), new System.Threading.CancellationToken());

            Assert.AreEqual(1, timeouts?.Count);
        }

        public class Context : ScenarioContext
        {
            public bool TimeoutsSet { get; set; }
        }

        public class SqlP_WithTimeouts_Endpoint : EndpointConfigurationBuilder
        {
            public SqlP_WithTimeouts_Endpoint()
            {
                EndpointSetup<SqlPEndpoint>(config =>
                {
                });
            }

            [SqlSaga(correlationProperty: nameof(TestSaga.Id))]
            public class TimeoutSaga : Saga<TestSaga>, IAmStartedByMessages<StartSagaMessage>, IHandleTimeouts<Timeout>
            {
                // ReSharper disable once MemberCanBePrivate.Global
                public Context TestContext { get; set; }

                public async Task Handle(StartSagaMessage message, IMessageHandlerContext context)
                {
                    await RequestTimeout(context, DateTime.Now.AddSeconds(0.1), new Timeout { Id = message.Id }); // Wait for the timeout messages to be sent to the timeout manager
                    await RequestTimeout(context, DateTime.Now.AddDays(7), new Timeout { Id = message.Id });
                }

                public Task Timeout(Timeout state, IMessageHandlerContext context)
                {
                    TestContext.TimeoutsSet = true;

                    return Task.CompletedTask;
                }

                protected override void ConfigureHowToFindSaga(SagaPropertyMapper<TestSaga> mapper)
                {
                    mapper.ConfigureMapping<StartSagaMessage>(a => a.Id).ToSaga(s => s.Id);
                    mapper.ConfigureMapping<Timeout>(a => a.Id).ToSaga(s => s.Id);
                }
            }
        }

        public class TestSaga : ContainSagaData
        {
            public override Guid Id { get; set; }
        }

        public class Timeout
        {
            public Guid Id { get; set; }
        }

        public class StartSagaMessage : ICommand
        {
            public Guid Id { get; set; }
        }
    }
}