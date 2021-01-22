namespace TimeoutMigrationTool.SqlT.IntegrationTests
{
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using NUnit.Framework;
    using Particular.TimeoutMigrationTool.SqlT;
    using SqlP.AcceptanceTests;

    [TestFixture]
    public class When_completing_a_migration
    {
        [Test]
        public async Task Should_delete_staging_table_when_empty()
        {
            var sut = new SqlTTimeoutsTarget(new TestLoggingAdapter(), connectionString, "dbo");

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            await SqlTQueueCreator.CreateStagingQueue(connection, SqlConstants.TimeoutMigrationStagingTable, "dbo", connection.Database, preview: false);

            await sut.Complete(ExistingEndpointName);

            Assert.That(connection.State, Is.EqualTo(ConnectionState.Closed));
            await connection.OpenAsync();

            var sqlStatement = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = '{SqlConstants.TimeoutMigrationStagingTable}' AND TABLE_CATALOG = '{databaseName}'";
            await using var command = new SqlCommand(sqlStatement, connection)
            {
                CommandType = CommandType.Text
            };
            var result = await command.ExecuteScalarAsync().ConfigureAwait(false) as int?;
            Assert.That(result, Is.EqualTo(0));
        }

        [Test]
        public void Should_throw_when_staging_table_not_empty()
        {

        }

        private string databaseName;
        private string connectionString;

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

        string ExistingEndpointName = "ExistingEndpointName";
    }
}