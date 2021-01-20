namespace TimeoutMigrationTool.NHibernate.AcceptanceTests
{
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.Nhb;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    [TestFixture]
    class NHibernateTimeoutStorageTests : NHibernateAcceptanceTests
    {
        [Test]
        public async Task TryLoadOngoingMigration_Should_Be_Null_When_No_Migration_Running()
        {
            // Arrange
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 10, DatabaseDialect);

            // Act
            var currentMigration = await timeoutStorage.TryLoadOngoingMigration();

            // Assert
            Assert.IsNull(currentMigration);
        }

        [Test]
        public async Task Preparing_Creates_A_MigrationsEntity_And_Returns_It()
        {
            // Arrange
            var endpointName = "Preparing_Creates_A_MigrationsEntity_And_Returns_It";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 10, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            // Act
            var currentMigration = await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(0, currentMigration.NumberOfBatches);
        }

        [Test]
        public async Task Preparing_Sets_The_Number_Of_Batches_Correctly()
        {
            // Arrange
            var endpointName = "Preparing_Sets_The_Number_Of_Batches_Correctly";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 2; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName,
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            // Act
            var currentMigration = await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Assert
            Assert.IsNotNull(currentMigration);

            Assert.AreEqual(endpointName, currentMigration.EndpointName);
            Assert.AreEqual(runParameters, currentMigration.RunParameters);
            Assert.AreEqual(2, currentMigration.NumberOfBatches);
        }

        [Test]
        public async Task Can_Read_Batch_By_Batch_Number()
        {
            // Arrange
            var endpointName = "Can_Read_Batch_By_Batch_Number";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;
            var expectedDestinations = new List<string>();

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 3; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName + x.ToString(),
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });

                        expectedDestinations.Add(endpointName + x.ToString());
                    }

                    await testTx.CommitAsync();
                }
            }

            await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            for (var x = 0; x < 3; x++)
            {
                var batch = await timeoutStorage.ReadBatch(x + 1);
                expectedDestinations.Remove(batch.First().Destination);
            }

            // Assert
            // If all the batches were loaded correctly, the destinations would have been removed from the list.
            Assert.IsEmpty(expectedDestinations);
        }

        [Test]
        [TestCase(BatchState.Completed)]
        [TestCase(BatchState.Staged)]
        public async Task Marking_A_Batch_As_Complete_Updates_The_Status_Correctly(BatchState batchState)
        {
            // Arrange
            var endpointName = "Marking_A_Batch_As_Complete_Updates_The_Status_Correctly";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var sessionFactory = CreateSessionFactory();

            using (var testSession = sessionFactory.OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 2; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName,
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            switch (batchState)
            {
                case BatchState.Staged:
                    await timeoutStorage.MarkBatchAsStaged(1);
                    break;
                case BatchState.Completed:
                    await timeoutStorage.MarkBatchAsCompleted(1);
                    break;

                default:
                    return;
            }

            // Assert
            using var dbQuerySession = sessionFactory.OpenSession();
            var query = dbQuerySession.CreateSQLQuery(GetSqlQueryToLoadBatchState(1));
            Assert.AreEqual((int)batchState, query.List<object>().Select(o => Convert.ToInt32(o)).First());
        }

        [Test]
        public async Task ListEndpoints_Loads_All_Endpoints_With_Timeouts()
        {
            // Arrange
            var endpointName = "ListEndpoints_Loads_All_Endpoints_With_Timeouts";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var sessionFactory = CreateSessionFactory();

            using (var testSession = sessionFactory.OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 2; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName + x.ToString(),
                            Destination = endpointName + "_Destination" + x.ToString(),
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            var endpoints = await timeoutStorage.ListEndpoints(DateTime.UtcNow.AddDays(-100));

            // Assert
            Assert.AreEqual(2, endpoints.Count);
            Assert.AreEqual(new[] { $"{endpointName}0", $"{endpointName}1" }, endpoints.Select(endpoint => endpoint.EndpointName).ToArray());
        }

        [Test]
        public async Task Complete_Sets_The_MigrationStatus_Correctly()
        {
            var endpointName = "Complete_Sets_The_MigrationStatus_Correctly";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 2; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName,
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            var currentMigration = await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            for (var x = 0; x < currentMigration.NumberOfBatches; x++)
            {
                await timeoutStorage.MarkBatchAsCompleted(x + 1);
            }

            // Act
            await timeoutStorage.Complete();

            // Assert
            var loadedMigrationAfterCompletion = await timeoutStorage.TryLoadOngoingMigration();
            Assert.IsNull(loadedMigrationAfterCompletion);
        }

        [Test]
        public async Task Aborting_Returns_StagedTimeouts_Back_To_TimeoutEntity_Table()
        {
            // Arrange
            var endpointName = "Aborting_Returns_StagedTimeouts_Back_To_TimeoutEntity_Table";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;

            var sessionFactory = CreateSessionFactory();

            using (var testSession = sessionFactory.OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 2; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName,
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            await timeoutStorage.Abort();

            // Assert
            using var validateSession = sessionFactory.OpenSession();
            var timeouts = await validateSession.QueryOver<TimeoutEntity>().ListAsync();
            Assert.AreEqual(2, timeouts.Count);

            var currentAfterAborting = await timeoutStorage.TryLoadOngoingMigration();
            Assert.IsNull(currentAfterAborting);
        }

        [Test]
        public async Task GetNextBatch_Returns_The_Next_Batch_Not_Migrated()
        {
            // Arrange
            var endpointName = "GetNextBatch_Returns_The_Next_Batch_Not_Migrated";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;
            var expectedDestinations = new List<string>();

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 3; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName + x.ToString(),
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }
                    await testTx.CommitAsync();
                }
            }

            var toolState = await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);

            // Act
            var firstBatch = await toolState.TryGetNextBatch();

            // Assert
            Assert.IsNotNull(firstBatch);
            Assert.AreEqual(BatchState.Pending, firstBatch.State);
            Assert.AreEqual(1, firstBatch.NumberOfTimeouts);
        }

        [Test]
        public async Task GetNextBatch_When_Batches_Are_Migrated_Returns_Null()
        {
            // Arrange
            var endpointName = "GetNextBatch_Returns_The_Next_Batch_Not_Migrated";
            var timeoutStorage = new NHibernateTimeoutSource(connectionString, 1, DatabaseDialect);
            var runParameters = new Dictionary<string, string> { { "Test", "TestValue" } };
            var cutOffDate = DateTime.UtcNow;
            var expectedDestinations = new List<string>();

            using (var testSession = CreateSessionFactory().OpenSession())
            { // Explicit using scope to ensure dispose before SUT connects
                using (var testTx = testSession.BeginTransaction())
                {
                    for (var x = 0; x < 3; x++)
                    {
                        await testSession.SaveAsync(new TimeoutEntity
                        {
                            Endpoint = endpointName,
                            Destination = endpointName + x.ToString(),
                            SagaId = Guid.NewGuid(),
                            State = null,
                            Time = cutOffDate.AddDays(1)
                        });
                    }

                    await testTx.CommitAsync();
                }
            }

            var toolState = await timeoutStorage.Prepare(cutOffDate, endpointName, runParameters);
            for (var x = 0; x < 3; x++)
            {
                await timeoutStorage.MarkBatchAsCompleted(x+1);
            }

            // Act
            var firstBatch = await toolState.TryGetNextBatch();

            // Assert
            Assert.IsNull(firstBatch);
        }
    }
}