namespace TimeoutMigrationTool.Msmq.IntegrationTests
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
    using Particular.TimeoutMigrationTool.Msmq;
    using SqlP.AcceptanceTests;

    [TestFixture]
    [EnvironmentSpecificTest(EnvironmentVariables.SqlServerConnectionString)]
    public class MsmqTimeoutsTargetTests
    {
        string databaseName;
        string connectionString;

        string ExistingEndpointName = "ExistingEndpointName";
        string ExistingDestination = "ExistingEndpointNameDirect";
        string schema = "dbo";

        [SetUp]
        public async Task SetUp()
        {
            connectionString = Environment.GetEnvironmentVariable(EnvironmentVariables.SqlServerConnectionString);
            databaseName = new SqlConnectionStringBuilder(connectionString).InitialCatalog;
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
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
CREATE TABLE [{1}].[{0}] (
    RowVersion bigint IDENTITY(1,1) NOT NULL
);
", $"{MsmqSqlConstants.DelayedTableName(ExistingEndpointName)}", schema);
            await command.ExecuteNonQueryAsync();

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointName,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new[] { ExistingDestination }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.That(result.CanMigrate, Is.True, string.Join("\r", result.Problems));
        }

        [Test]
        public async Task AbleToMigrate_delayed_delivery_does_not_exist_should_indicate_problems()
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
IF OBJECT_ID('{0}.{1}', 'u') IS NOT NULL
  DROP TABLE {0}.{1};
", $"{MsmqSqlConstants.DelayedTableName(ExistingEndpointName)}", schema);
            await command.ExecuteNonQueryAsync();

            var info = new EndpointInfo
            {
                EndpointName = ExistingEndpointName,
                ShortestTimeout = DateTime.UtcNow.AddDays(3),
                LongestTimeout = DateTime.UtcNow.AddDays(5),
                Destinations = new[] { ExistingDestination }
            };
            var result = await sut.AbleToMigrate(info);

            Assert.That(result.CanMigrate, Is.False, string.Join("\r", result.Problems));
        }

        [Test]
        public async Task Should_delete_staging_queue_when_aborting()
        {
            var endpointName = "FakeEndpoint";

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Abort(endpointName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
", MsmqSqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
            var result = await command.ExecuteScalarAsync() as int?;

            Assert.That(Convert.ToBoolean(result), Is.False);
        }

        [Test]
        public async Task Should_delete_staging_queue_when_completing()
        {
            var endpointName = "FakeEndpoint";
            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(endpointName);
            await sut.Complete(endpointName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
", MsmqSqlConstants.TimeoutMigrationStagingTable, schema, databaseName);
            var result = await command.ExecuteScalarAsync() as int?;

            Assert.That(Convert.ToBoolean(result), Is.False);
        }

        [Test]
        public async Task Should_migrate_into_delayed_table()
        {
            var endpointDelayedTableName = MsmqSqlConstants.DelayedTableName(ExistingEndpointName);

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();

            command.CommandText = string.Format(@"
CREATE TABLE [{1}].[{0}] (
    Id nvarchar(250) not null primary key,
    Destination nvarchar(200),
    State varbinary(max),
    Time datetime,
    Headers varbinary(max) not null,
    RetryCount INT NOT NULL default(0)
);
", endpointDelayedTableName, schema);
            await command.ExecuteNonQueryAsync();

            const int batchNumber = 2;
            await using var endpointTarget = await sut.PrepareTargetEndpointBatchMigrator(ExistingEndpointName);
            await endpointTarget.StageBatch(
            [
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
            ], batchNumber);

            await endpointTarget.CompleteBatch(batchNumber);

            var endpointDelayedTableDataTable = new DataTable();
            using var endpointDelayedTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", endpointDelayedTableName, schema), connection);
            endpointDelayedTableDataAdapter.Fill(endpointDelayedTableDataTable);

            var migrationTableDataTable = new DataTable();
            using var migrationTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", MsmqSqlConstants.TimeoutMigrationStagingTable, schema), connection);
            migrationTableDataAdapter.Fill(migrationTableDataTable);

            Approver.Verify(endpointDelayedTableDataTable.Rows.OfType<DataRow>().SelectMany(r => r.ItemArray.Take(3)));
            Assert.That(migrationTableDataTable.Rows, Is.Empty);
        }

        [Test]
        public async Task Should_delete_staging_table_when_empty()
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await MsmqQueueCreator.CreateStagingQueue(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);

            await sut.Complete(ExistingEndpointName);

            var sqlStatement = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{MsmqSqlConstants.TimeoutMigrationStagingTable}' AND TABLE_CATALOG = '{databaseName}'";
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
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var sut = new MsmqTarget(new TestLoggingAdapter(), connectionString, schema);

            await MsmqQueueCreator.CreateStagingQueue(connection, MsmqSqlConstants.TimeoutMigrationStagingTable, schema, connection.Database, preview: false);

            var sql =
                $"INSERT INTO {schema}.{MsmqSqlConstants.TimeoutMigrationStagingTable} VALUES('id', 'destination', NULL, DATEADD(DAY, 1, GETDATE()), CAST('headers' AS VARBINARY(MAX)), 0)";
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