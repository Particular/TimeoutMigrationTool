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
        private string databaseName;
        private string connectionString;

        string ExistingEndpointName = "ExistingEndpointName";
        string ExistingDestination = "ExistingEndpointNameDirect";

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
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, "dbo");

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
CREATE TABLE [{1}].[{0}] (
    RowVersion bigint IDENTITY(1,1) NOT NULL
);
", $"{ExistingEndpointName}.Delayed", "dbo");
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
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, "dbo");

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
IF OBJECT_ID('{0}.{1}', 'u') IS NOT NULL
  DROP TABLE {0}.{1};
", $"{ExistingEndpointName}.Delayed", "dbo");
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
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, "dbo");
            var endpointName = "FakeEndpoint";
            await using var endpointTarget = await sut.Migrate(endpointName);
            await sut.Abort(endpointName);

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await using var command = connection.CreateCommand();
            command.CommandText = string.Format(@"
   SELECT COUNT(*)
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{0}' AND TABLE_CATALOG = '{2}'
", "timeoutmigrationtoolstagingtable", "dbo", databaseName);
            var result = await command.ExecuteScalarAsync() as int?;

            Assert.That(Convert.ToBoolean(result), Is.False);
        }

        [Test]
        public async Task Should_migrate_into_delayed_table()
        {
            var endpointDelayedTableName = $"{ExistingEndpointName}.Delayed";

            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, "dbo");

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
", endpointDelayedTableName, "dbo");
            await command.ExecuteNonQueryAsync();

            const int BatchNumber = 2;
            await using var endpointTarget = await sut.Migrate(ExistingEndpointName);
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
            using var endpointDelayedTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", endpointDelayedTableName, "dbo"), connection);
            endpointDelayedTableDataAdapter.Fill(endpointDelayedTableDataTable);

            var migrationTableDataTable = new DataTable();
            using var migrationTableDataAdapter = new SqlDataAdapter(string.Format("SELECT * FROM [{1}].[{0}]", "timeoutmigrationtoolstagingtable", "dbo"), connection);
            migrationTableDataAdapter.Fill(migrationTableDataTable);

            Approver.Verify(endpointDelayedTableDataTable.Rows.OfType<DataRow>().SelectMany(r => r.ItemArray.Take(3)));
            Assert.IsEmpty(migrationTableDataTable.Rows);
        }
    }
}