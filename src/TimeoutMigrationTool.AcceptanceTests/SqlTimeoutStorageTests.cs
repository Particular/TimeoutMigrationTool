using NServiceBus;
using NServiceBus.AcceptanceTesting;
using NServiceBus.Persistence.Sql;
using NUnit.Framework;
using Particular.TimeoutMigrationTool;
using Particular.TimeoutMigrationTool.SqlP;
using System;
using System.Linq;
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
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>()
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1024, "");
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT TOP 1 Batches FROM TimeoutsMigration_State WHERE EndpointName = '{SqlP_WithTimeouts_Endpoint.EndpointName}'");

            Assert.AreEqual(1, numberOfBatches);
        }

        [Test]
        public async Task Loads_ToolState_For_Existing_Migration()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Loads_ToolState_For_Existing_Migration";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>()
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1024, "");
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var toolState = await timeoutStorage.GetToolState();

            Assert.AreEqual(1, toolState.Batches.Count());
        }

        [Test]
        public async Task Saves_ToolState_Status_When_Changed()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Saves_ToolState_Status_When_Changed";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>()
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1024, "");
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var toolState = await timeoutStorage.GetToolState();

            toolState.Status = MigrationStatus.Completed;
            await timeoutStorage.StoreToolState(toolState);

            var loadedToolState = await timeoutStorage.GetToolState();

            Assert.AreEqual(MigrationStatus.Completed, loadedToolState.Status);
        }

        [Test]
        public async Task Splits_timeouts_into_correct_number_of_batches()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Splits_timeouts_into_correct_number_of_batches";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            var batchInfo = await timeoutStorage.Prepare(DateTime.Now, endpoint);

            Assert.AreEqual(4, batchInfo.Count);
        }

        [Test]
        public async Task Only_Moves_timeouts_After_migrateTimeoutsWithDeliveryDateLaterThan_date()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Only_Moves_timeouts_After_migrateTimeoutsWithDeliveryDateLaterThan_date";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1, "");
            await timeoutStorage.Prepare(DateTime.Now.AddDays(10), endpoint);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT MAX(Batches) FROM TimeoutsMigration_State WHERE EndpointName = '{SqlP_WithTimeouts_Endpoint.EndpointName}'");

            Assert.AreEqual(6, numberOfBatches);
        }

        [Test]
        public async Task Loads_Endpoints_With_Valid_Timeouts()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Loads_Endpoints_With_Valid_Timeouts";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1, "");

            var endpoints = await timeoutStorage.ListEndpoints(DateTime.Now.AddYears(-10));

            Assert.IsTrue(endpoints.Count > 0);
        }

        [Test]
        public async Task Doesnt_Load_Endpoints_With_Invalid_Timeouts()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Doesnt_Load_Endpoints_With_Invalid_Timeouts";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 1, "");

            var endpoints = await timeoutStorage.ListEndpoints(DateTime.Now.AddYears(10));

            Assert.IsNull(endpoints);
        }

        [Test]
        public async Task Batches_Completed_Can_Be_Completed()
        {
            SqlP_WithTimeouts_Endpoint.EndpointName = "Batches_Completed_Can_Be_Completed";
            var endpoint = new EndpointInfo
            {
                EndpointName = SqlP_WithTimeouts_Endpoint.EndpointName
            };

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            var batches = await timeoutStorage.Prepare(DateTime.Now, endpoint);

            foreach (var batch in batches)
            {
                await timeoutStorage.CompleteBatch(batch.Number);
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

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            var batches = await timeoutStorage.Prepare(DateTime.Now, endpoint);

            foreach (var batch in batches)
            {
                var timeoutIdsCreatedDuringSplit = batch.TimeoutIds;
                var timeoutIdsFromDatabase = (await timeoutStorage.ReadBatch(batch.Number)).Select(timeout => timeout.Id).ToList();

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

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
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

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            await timeoutStorage.Prepare(DateTime.Now.AddDays(-10), endpoint);

            var toolState = await timeoutStorage.GetToolState();

            await timeoutStorage.Abort(toolState);

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

            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 5)
            .WithEndpoint<SqlP_WithTimeouts_Endpoint>(b => b
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

            var timeoutStorage = new SqlTimeoutStorage(MsSqlMicrosoftDataClientHelper.GetConnectionString(), Particular.TimeoutMigrationTool.SqlDialect.Parse("MsSql"), SqlP_WithTimeouts_Endpoint.EndpointName, 3, "");
            await timeoutStorage.Prepare(DateTime.Now, endpoint);

            var numberOfBatches = await MsSqlMicrosoftDataClientHelper.QueryScalarAsync<int>($"SELECT COUNT(*) FROM {SqlP_WithTimeouts_Endpoint.EndpointName}_TimeoutData_migration");

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