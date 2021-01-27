namespace TimeoutMigrationTool.SqlT.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using NUnit.Framework;
    using Particular.Approvals;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.SqlT;
    using SqlP.AcceptanceTests;

    [TestFixture]
    public class SqlTTimeoutsTargetTests
    {
        string databaseName;
        string connectionString;

        string ExistingEndpointName = "ExistingEndpointName";
        string ExistingDestination = "ExistingEndpointNameDirect";
        string schema = "dbo";

        [SetUp]
        public async Task SetUp()
        {
            databaseName = $"IntegrationTests{TestContext.CurrentContext.Test.ID.Replace("-", "")}";

            connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";

            await MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(connectionString);
        }

        [TearDown]
        public async Task TearDown()
        {
            await MsSqlMicrosoftDataClientHelper.RemoveDbIfExists(connectionString);
        }

        [Test]
        public async Task AbleToMigrate_delayed_delivery_does_exist_should_indicate_no_problems()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
CREATE TABLE [{1}].[{0}] (
    RowVersion bigint IDENTITY(1,1) NOT NULL
);
", $"{ExistingEndpointName}.Delayed", schema);
            await command.ExecuteNonQueryAsync();

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointName,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string>
                {
                    ExistingDestination
                }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsTrue(result.CanMigrate);
        }

        [Test]
        public async Task AbleToMigrate_delayed_delivery_does_not_exist_should_indicate_problems()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
IF OBJECT_ID('{0}.{1}', 'u') IS NOT NULL
  DROP TABLE {0}.{1};
", $"{ExistingEndpointName}.Delayed", schema);
            await command.ExecuteNonQueryAsync();

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointName,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new List<string>
                {
                    ExistingDestination
                }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.IsFalse(result.CanMigrate);
        }

        [Test]
        public async Task Should_delete_staging_queue_when_aborting()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);
            var endpointName = "FakeEndpoint";
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Abort(endpointName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
", SqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
            var result = await command.ExecuteScalarAsync() as int?;

            Assert.That(Convert.ToBoolean(result), Is.False);
        }

        [Test]
        public async Task Should_delete_staging_queue_when_completing()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);
            var endpointName = "FakeEndpoint";
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Complete(endpointName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
", SqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
            var result = await command.ExecuteScalarAsync() as int?;

            Assert.That(Convert.ToBoolean(result), Is.False);
        }

        [Test]
        public async Task Should_migrate_into_delayed_table()
        {
            var endpointDelayedTableName = $"{ExistingEndpointName}.Delayed";

            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();

            command.CommandText = string.Format(@"
CREATE TABLE [{1}].[{0}] (
    Headers nvarchar(max) NOT NULL,
    Body varbinary(max),
    Due datetime NOT NULL,
    RowVersion bigint IDENTITY(1,1) NOT NULL
);
", endpointDelayedTableName, schema);
            await command.ExecuteNonQueryAsync();

            const int BatchNumber = 2;
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(ExistingEndpointName);
            await endpointTarget.StageBatch(new List<TimeoutData>
            {
                new TimeoutData
                {
                    Id = "SomeID",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeMessageId" }
                    },
                    Destination = "SomeDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 12, 12, DateTimeKind.Utc)
                },
                new TimeoutData
                {
                    Id = "SomeOtherId",
                    Headers = new Dictionary<string, string>
                    {
                        { "NServiceBus.MessageId", "SomeOtherMessageId" }
                    },
                    Destination = "SomeOtherDestination",
                    State = new byte[2],
                    Time = new DateTime(2021, 12, 12, 12, 13, 13, DateTimeKind.Utc)
                },
            }, BatchNumber);

            await endpointTarget.CompleteBatch(BatchNumber);

            var endpointDelayedTableDataTable = new DataTable();
            using var endpointDelayedTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", endpointDelayedTableName, schema), connection);
            endpointDelayedTableDataAdapter.Fill(endpointDelayedTableDataTable);

            var migrationTableDataTable = new DataTable();
            using var migrationTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", SqlConstants.TimeoutMigrationStagingTable, schema), connection);
            migrationTableDataAdapter.Fill(migrationTableDataTable);

            Approver.Verify(endpointDelayedTableDataTable.Rows.OfType<DataRow>().SelectMany(r => r.ItemArray.Take(3)));
            Assert.IsEmpty(migrationTableDataTable.Rows);
        }

        [Test]
        public async Task Should_delete_staging_table_when_empty()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await SqlTQueueCreator.CreateStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);

            await sut.Complete(ExistingEndpointName);

            var sqlStatement = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{SqlConstants.TimeoutMigrationStagingTable}' AND TABLE_CATALOG = '{databaseName}'";
            await using var command = new SqlCommand(sqlStatement, connection)
            {
                CommandType = CommandType.Text
            };
            var result = await command.ExecuteScalarAsync().ConfigureAwait(false) as int?;

            Assert.That(result, Is.EqualTo(0));
        }

        [Test]
        public async Task Should_throw_when_staging_table_not_empty()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await SqlTQueueCreator.CreateStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);

            var sql =
                $"INSERT INTO {schema}.{SqlConstants.TimeoutMigrationStagingTable} VALUES('headers', NULL, DATEADD(DAY, 1, GETDATE()))";
            await using var command = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync();

            Assert.ThrowsAsync<Exception>(async () =>
            {
                await sut.Complete(ExistingEndpointName);
            });
        }
    }
}