namespace TimeoutMigrationTool.SqlP.AcceptanceTests
{
    using NServiceBus;
    using NServiceBus.AcceptanceTesting;
    using NServiceBus.Persistence.Sql;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString)]
    class SqlTimeoutSourceTests : SqlPAcceptanceTest
    {
        static string sourceEndpoint = NServiceBus.AcceptanceTesting.Customization.Conventions.EndpointNamingConvention(typeof(SqlPEndpoint));

        [Test]
        public async Task Loads_ToolState_For_Existing_Migration()
        {
            await Scenario.Define<Context>()
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            Assert.AreEqual(1, toolState.NumberOfBatches);
        }

        [Test]
        public async Task Can_prepare_storage_for_migration()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage(3);
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            Assert.AreEqual(4, toolState.NumberOfBatches);
        }

        [Test]
        public async Task Only_Moves_timeouts_After_migrateTimeoutsWithDeliveryDateLaterThan_date()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage(1);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(10), sourceEndpoint, new Dictionary<string, string>());

            Assert.AreEqual(6, toolState.NumberOfBatches);
        }

        [Test]
        public async Task Loads_Endpoints_With_Valid_Timeouts()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };
                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var endpoints = await timeoutStorage.ListEndpoints(DateTime.Now.AddYears(-10));

            CollectionAssert.Contains(endpoints.Select(e => e.EndpointName), sourceEndpoint);
            Assert.AreEqual(1, endpoints.Single(e => e.EndpointName == sourceEndpoint).Destinations.Count());
        }

        [Test]
        public async Task Loads_Destinations_For_Endpoints_With_Valid_Timeouts()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 1)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
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
                .Done(c => NumberOfTimeouts(sourceEndpoint) == 3)
                .Run();

            var endpoints = await GetTimeoutStorage().ListEndpoints(DateTime.Now.AddYears(-10));

            CollectionAssert.AreEquivalent(new List<string> { "FirstDestination", "SecondDestination", "ThirdDestination" }, endpoints.First().Destinations);
        }

        [Test]
        public async Task Doesnt_Load_Endpoints_With_Timeouts_outside_the_cutoff_date()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var endpoints = await GetTimeoutStorage().ListEndpoints(DateTime.Now.AddYears(10));

            CollectionAssert.IsEmpty(endpoints);
        }

        [Test]
        public async Task Batches_Completed_Can_Be_Completed()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            BatchInfo batch;

            while ((batch = await toolState.TryGetNextBatch()) != null)
            {
                await timeoutStorage.MarkBatchAsCompleted(batch.Number);
                batch.State = BatchState.Completed;
            }

            var loadedState = await timeoutStorage.TryLoadOngoingMigration();

            Assert.IsNull(await loadedState.TryGetNextBatch());
        }

        [Test]
        public async Task Can_mark_batch_as_staged()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            var batch = await toolState.TryGetNextBatch();

            await timeoutStorage.MarkBatchAsStaged(batch.Number);

            var loadedState = await timeoutStorage.TryLoadOngoingMigration();

            var stagedBatch = await loadedState.TryGetNextBatch();

            Assert.AreEqual(BatchState.Staged, stagedBatch.State);
        }

        [Test]
        public async Task Timeouts_Split_Can_Be_Read_By_Batch()
        {
            var context = await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage(3);
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            var timeoutsStored = 0;

            BatchInfo batch;

            while ((batch = await toolState.TryGetNextBatch()) != null)
            {
                var timeouts = await timeoutStorage.ReadBatch(batch.Number);

                Assert.AreEqual(batch.NumberOfTimeouts, timeouts.Count());
                timeoutsStored += batch.NumberOfTimeouts;

                await timeoutStorage.MarkBatchAsCompleted(batch.Number);
            }

            Assert.AreEqual(10, timeoutsStored);
        }

        [Test]
        public async Task Removes_Timeouts_From_Original_TimeoutData_Table()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            var numberOfTimeouts = await QueryScalarAsync<int>($"SELECT COUNT(*) FROM [{sourceEndpoint}_TimeoutData]");

            Assert.AreEqual(0, numberOfTimeouts);
        }

        [Test]
        public async Task Restores_Timeouts_To_Original_TimeoutData_Table_When_Aborted()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 10)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage(3);
            var toolState = await timeoutStorage.Prepare(DateTime.Now.AddDays(-10), sourceEndpoint, new Dictionary<string, string>());

            var batch1 = await toolState.TryGetNextBatch();

            await timeoutStorage.MarkBatchAsCompleted(batch1.Number);

            await timeoutStorage.Abort();

            var numberOfTimeouts = await QueryScalarAsync<int>($"SELECT COUNT(*) FROM [{sourceEndpoint}_TimeoutData]");

            Assert.AreEqual(10 - batch1.NumberOfTimeouts, numberOfTimeouts);

            Assert.AreEqual(1, await QueryScalarAsync<int>($"SELECT COUNT(*) FROM TimeoutsMigration_State WHERE Status = 3"), "Status should be set to aborted");
        }

        [Test]
        public async Task Copies_Timeouts_From_Original_TimeoutData_Table_To_New_Table()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 5)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage(1);
            var toolState = await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            Assert.AreEqual(5, toolState.NumberOfBatches);
        }

        [Test]
        public async Task WhenCompletingMigrationStatusIsSetToCompleted()
        {
            await Scenario.Define<Context>(c => c.NumberOfTimeouts = 5)
                .WithEndpoint<SqlPEndpoint>(b => b.CustomConfig(ec => SetupPersistence(ec))
                    .When(session =>
                    {
                        var startSagaMessage = new StartSagaMessage { Id = Guid.NewGuid() };

                        return session.SendLocal(startSagaMessage);
                    }))
                .Done(c => c.NumberOfTimeouts == NumberOfTimeouts(sourceEndpoint))
                .Run();

            var timeoutStorage = GetTimeoutStorage();
            await timeoutStorage.Prepare(DateTime.Now, sourceEndpoint, new Dictionary<string, string>());

            Assert.NotNull(await timeoutStorage.TryLoadOngoingMigration());
            await timeoutStorage.Complete();

            Assert.Null(await timeoutStorage.TryLoadOngoingMigration());
        }

        public class Context : ScenarioContext
        {
            public int NumberOfTimeouts { get; set; } = 1;
        }

        public class SqlPEndpoint : EndpointConfigurationBuilder
        {
            public SqlPEndpoint()
            {
                EndpointSetup<LegacyTimeoutManagerEndpoint>();
            }

            [SqlSaga(correlationProperty: nameof(TestSaga.CorrelationProperty))]
            public class TimeoutSaga : Saga<TestSaga>, IAmStartedByMessages<StartSagaMessage>, IHandleTimeouts<Timeout>
            {
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
                    mapper.MapSaga(s => s.CorrelationProperty).ToMessage<StartSagaMessage>(m => m.Id);
                }
            }
        }

        public class TestSaga : ContainSagaData
        {
            public override Guid Id { get; set; }
            public Guid CorrelationProperty { get; set; }
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