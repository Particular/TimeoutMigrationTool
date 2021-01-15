namespace Particular.TimeoutMigrationTool.SqlT
{
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public static class SqlTQueueCreator
    {
        public static async Task CreateStagingQueue(SqlConnection connection, string tableName, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(SqlConstants.CreateDelayedMessageStoreText, tableName, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }
    }
}