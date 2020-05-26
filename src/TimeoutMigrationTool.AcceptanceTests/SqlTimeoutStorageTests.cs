using NServiceBus;
using NServiceBus.AcceptanceTesting;
using NServiceBus.Persistence.Sql;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.SqlP;
using System;
using System.Security.Cryptography.Xml;
using System.Threading.Tasks;

namespace TimeoutMigrationTool.AcceptanceTests
{
    [TestFixture]
    class SqlTimeoutStorageTests : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Creates_TimeoutsMigration_State_Table()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Creates_TimeoutsMigration_State_Table";

            var context = await Scenario.Define<Context>()
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
                .When(session =>
                {
                    var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                    return session.SendLocal(startSagaMessage);
                }))
            .Done(c => c.TimeoutsSet)
            .Run();

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1024, "");
            await timeoutStorage.Prepare(DateTime.Now.AddYears(9).AddMonths(6));

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT TOP 1 Batches FROM TimeoutsMigration_State WHERE EndpointName = '{SqlP_WithTimeouts_Endpoint.EndpointName}'");

            Assert.AreEqual(1, numberOfBatches);
        }

        [Test]
        public async Task Splits_timeouts_into_correct_number_of_batches()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Splits_timeouts_into_correct_number_of_batches";

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
                .When(session =>
                {
                    var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                    return session.SendLocal(startSagaMessage);
                }))
            .Done(c => c.TimeoutsSet)
            .Run();

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            var batchInfo = await timeoutStorage.Prepare(DateTime.Now.AddYears(9).AddMonths(6));

            Assert.AreEqual(4, batchInfo.Count);
        }

        [Test]
        public async Task Removes_Timeouts_From_Original_TimeoutData_Table()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Removes_Timeouts_From_Original_TimeoutData_Table";

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
                .When(session =>
                {
                    var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                    return session.SendLocal(startSagaMessage);
                }))
            .Done(c => c.TimeoutsSet)
            .Run();

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            await timeoutStorage.Prepare(DateTime.Now.AddYears(9).AddMonths(6));

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

            Assert.AreEqual(0, numberOfBatches);
        }

        [Test]
        public async Task Copies_Timeouts_From_Original_TimeoutData_Table_To_New_Table()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Copies_Timeouts_From_Original_TimeoutData_Table_To_New_Table";

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 5)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
                .When(session =>
                {
                    var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                    return session.SendLocal(startSagaMessage);
                }))
            .Done(c => c.TimeoutsSet)
            .Run();

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            await timeoutStorage.Prepare(DateTime.Now.AddYears(9).AddMonths(6));

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData_migration");

            Assert.AreEqual(5, numberOfBatches);
        }

        public class Context : ScenarioContext
        {
            public bool TimeoutsSet { get; set; }
            public int NumberOfTimeouts { get; set; } = 1;
        }

        public class SqlP_WithTimeouts_Endpoint : EndpointConfigurationBuilder
        {
            public static string EndpointName { get; set; }

            public SqlP_WithTimeouts_Endpoint()
            {
                if (!string.IsNullOrWhiteSpace(EndpointName))
                {
                    CustomEndpointName(EndpointName);
                }

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
                    for (var x = 0; x < TestContext.NumberOfTimeouts; x++)
                    {
                        await RequestTimeout(context, DateTime.Now.AddDays(7 + x), new Timeout { Id = message.Id });
                    }
                    await RequestTimeout(context, DateTime.Now.AddSeconds(0.1), new Timeout { Id = message.Id }); // Wait for the timeout messages to be sent to the timeout manager
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