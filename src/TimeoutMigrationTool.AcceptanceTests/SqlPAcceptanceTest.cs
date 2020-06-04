namespace TimeoutMigrationTool.AcceptanceTests
{
    using NUnit.Framework;
    using System;
    using System.Threading.Tasks;

    public abstract class SqlPAcceptanceTest : NServiceBusAcceptanceTest
    {
        public override async Task SetUp()
        {
            await base.SetUp();
            databaseName = $"Att{TestContext.CurrentContext.Test.ID.Replace("-","")}";

            connectionString =  $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={databaseName};Integrated Security=True;";
            rabbitUrl = Environment.GetEnvironmentVariable("RabbitMQ_uri") ?? "amqp://guest:guest@localhost:5672";

            MsSqlMicrosoftDataClientHelper.RecreateDbIfNotExists(connectionString);
        }

        public override async Task TearDown()
        {
            await base.SetUp();

            //TODO
            //MsSqlMicrosoftDataClientHelper.DropDatabase(connectionString);
        }

        protected async Task<T> QueryScalar<T>(string sqlStatement)
        {
            using (var connection = MsSqlMicrosoftDataClientHelper.Build(connectionString))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = sqlStatement;

                    return (T) await command.ExecuteScalarAsync();
                }
            }
        }

        protected string databaseName;
        protected string connectionString;
        protected string rabbitUrl = "";
    }
}