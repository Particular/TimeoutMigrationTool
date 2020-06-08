using Microsoft.Data.SqlClient;
using NServiceBus;
using NServiceBus.AcceptanceTesting;
using NServiceBus.Persistence.Sql;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TimeoutMigrationTool.AcceptanceTests
{
    [TestFixture]
    class SqlTimeoutStorageTests : SqlPAcceptanceTest
    {
        static EndpointInfo sourceEndpoint = new EndpointInfo { EndpointName = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlP_WithTimeouts_Endpoint)).Replace(".", "_") };

        [Test]
        public async Task Creates_TimeoutsMigration_State_Table()
        {
            var timeoutStorage = GetTimeoutStorage();
            await timeoutStorage.StoreToolState(new ToolState(new Dictionary<string, string>(), sourceEndpoint));

            var storedToolState = await timeoutStorage.GetToolState();

            Assert.AreEqual(MigrationStatus.NeverRun, storedToolState.Status);
            Assert.AreEqual(sourceEndpoint.EndpointName, storedToolState.Endpoint.EndpointName);
            CollectionAssert.IsEmpty(storedToolState.Batches);
        }


        [Test]
        public async Task Loads_ToolState_For_Existing_Migration()
        {
            await Scenario.Define<Context>()
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint.EndpointName))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), sourceEndpoint);
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint);

            var loadedToolState = await timeoutStorage.GetToolState();

            Assert.AreEqual(1, loadedToolState.Batches.Count());
        }

        [Test]
        public async Task Saves_ToolState_Status_When_Changed()
        {
            await Scenario.Define<Context>()
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                 .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint.EndpointName))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), sourceEndpoint);
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint);

            var loadedToolState = await timeoutStorage.GetToolState();

            loadedToolState.Status = MigrationStatus.Completed;
            await timeoutStorage.StoreToolState(loadedToolState);

            var secondLoadedToolState = await timeoutStorage.GetToolState();

            Assert.AreEqual(MigrationStatus.Completed, secondLoadedToolState.Status);
        }

        [Test]
        public async Task Splits_timeouts_into_correct_number_of_batches()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint.EndpointName))
                .Run();

            var timeoutStorage = GetTimeoutStorage(3);
            var batchInfo = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint);

            Assert.AreEqual(4, batchInfo.Count);
        }

        [Test]
        public async Task Only_Moves_timeouts_After_migrateTimeoutsWithDeliveryDateLaterThan_date()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint.EndpointName))
                .Run();

            var timeoutStorage = GetTimeoutStorage(1);
            var toolState = new ToolState(new Dictionary<string, string>(), sourceEndpoint);
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now.AddDays(10), sourceEndpoint);

            var numberOfBatches = await QueryScalarAsync<int>($"SELECT MAX(Batches) FROM TimeoutsMigration_State");

            Assert.AreEqual(6, numberOfBatches);
        }

        [Test]
        public async Task Loads_Endpoints_With_Valid_Timeouts()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Loads_Endpoints_With_Valid_Timeouts";

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage();

            var endpoints = await timeoutStorage.ListEndpoints(DateTime.Now.AddYears(-10));

            // this will blow if the names don't match
            var loadedEndpoint = endpoints.Single(a => string.Equals(a.EndpointName, SqlP_WithTimeouts_Endpoint.EndpointName, StringComparison.OrdinalIgnoreCase));

            Assert.IsTrue(endpoints.Count > 0);
        }

        [Test]
        public async Task Loads_Destinations_For_Endpoints_With_Valid_Timeouts()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Loads_Endpoints_With_Valid_Timeouts";

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(async session =>
                    {
                        var delayedMessage = new StartSagaMessage();

                        var options = new SendOptions();

                        options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                        options.SetDestination("FirstDestination");

                        await session.Send(delayedMessage, options);

                        delayedMessage = new StartSagaMessage();

                        options = new SendOptions();

                        options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                        options.SetDestination("SecondDestination");

                        await session.Send(delayedMessage, options);

                        delayedMessage = new StartSagaMessage();

                        options = new SendOptions();

                        options.DelayDeliveryWith(TimeSpan.FromSeconds(15));
                        options.SetDestination("ThirdDestination");

                        await session.Send(delayedMessage, options);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == 3;
                })
                .Run();

            var endpoints = await GetTimeoutStorage().ListEndpoints(DateTime.Now.AddYears(-10));

            var expectedDestinations = new List<string> { "FirstDestination", "SecondDestination", "ThirdDestination" };

            CollectionAssert.AreEquivalent(expectedDestinations, endpoints.First().Destinations);
        }

        [Test]
        public async Task Doesnt_Load_Endpoints_With_Timeouts_outside_the_cutoff_date()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Doesnt_Load_Endpoints_With_Invalid_Timeouts";

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var endpoints = await GetTimeoutStorage().ListEndpoints(DateTime.Now.AddYears(10));

            Assert.AreEqual(0, endpoints.Count);
        }

        [Test]
        public async Task Batches_Completed_Can_Be_Completed()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Batches_Completed_Can_Be_Completed";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), new EndpointInfo { EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName });
            await timeoutStorage.StoreToolState(toolState);
            var batches = await timeoutStorage.Prepare(DateTime.Now, endpoint);

            foreach (var batch in batches)
            {
                await timeoutStorage.CompleteBatch(endpoint, batch.Number);
            }

            var loadedState = await timeoutStorage.GetToolState();

            Assert.IsTrue(loadedState.Batches.All(b => b.State == BatchState.Completed));
        }

        [Test]
        public async Task Timeouts_Split_Can_Be_Read_By_Batch()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Timeouts_Split_Can_Be_Read_By_Batch";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage(3);
            var toolState = new ToolState(new Dictionary<string, string>(), new EndpointInfo { EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName });
            await timeoutStorage.StoreToolState(toolState);
            var batches = await timeoutStorage.Prepare(DateTime.Now, endpoint);

            foreach (var batch in batches)
            {
                var timeoutIdsCreatedDuringSplit = batch.TimeoutIds;
                var timeoutIdsFromDatabase = (await timeoutStorage.ReadBatch(endpoint, batch.Number)).Select(timeout => timeout.Id).ToList();

                CollectionAssert.AreEquivalent(timeoutIdsCreatedDuringSplit, timeoutIdsFromDatabase);
            }
        }

        [Test]
        public async Task Removes_Timeouts_From_Original_TimeoutData_Table()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Removes_Timeouts_From_Original_TimeoutData_Table";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), new EndpointInfo { EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName });
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

            Assert.AreEqual(0, numberOfBatches);
        }

        [Test]
        public async Task Restores_Timeouts_To_Original_TimeoutData_Table_When_Aborted()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Restores_Timeouts_To_Original_TimeoutData_Table_When_Aborted";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), new EndpointInfo { EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName });
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now.AddDays(-10), endpoint);

            var loadedToolState = await timeoutStorage.GetToolState();

            await timeoutStorage.Abort(loadedToolState);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

            Assert.AreEqual(10, numberOfBatches);
        }

        [Test]
        public async Task Copies_Timeouts_From_Original_TimeoutData_Table_To_New_Table()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Copies_Timeouts_From_Original_TimeoutData_Table_To_New_Table";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 5)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b.CustomConfig(ec => SetupPersitence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c =>
                {
                    var numberOfTimeouts = MsSqlMicrosoftDataClientHelper.QueryScalar<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData");

                    return numberOfTimeouts == c.NumberOfTimeouts;
                })
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = new ToolState(new Dictionary<string, string>(), new EndpointInfo { EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName });
            await timeoutStorage.StoreToolState(toolState);
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM TimeoutData_migration");

            Assert.AreEqual(5, numberOfBatches);
        }

        public class Context : ScenarioContext
        {
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

                EndpointSetup<LegacyTimeoutManagerEndpoint>();
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
                }

                public Task Timeout(Timeout state, IMessageHandlerContext context)
                {
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