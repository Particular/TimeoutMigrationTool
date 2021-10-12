namespace Particular.TimeoutMigrationTool.Msmq
{
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public class MsmqQueueCreator
    {
        public static async Task CreateStagingQueue(SqlConnection connection, string tableName, string schema, string databaseName, bool preview = false)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(MsmqSqlConstants.CreateDelayedMessageStoreText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (preview)
            {
                await transaction.RollbackAsync().ConfigureAwait(false);
            }
            else
            {
                await transaction.CommitAsync().ConfigureAwait(false);
            }
        }

        public static async Task<bool> DoesDelayedDeliveryTableExist(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            var sql = string.Format(MsmqSqlConstants.DelayedMessageStoreExistsText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };
            var result = await command.ExecuteScalarAsync().ConfigureAwait(false) as int?;
            return result == 1;
        }

        public static async Task DeleteStagingQueue(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(MsmqSqlConstants.DeleteDelayedMessageStoreText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
        }

        public static async Task TruncateTable(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(MsmqSqlConstants.TruncateTableText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
        }

        public static async Task<int> MoveFromTo(SqlConnection connection, string fromTable, string fromSchema, string toTable, string toSchema, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(MsmqSqlConstants.MoveFromStagingToDelayedTableText, fromTable, fromSchema, toTable, toSchema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            var rowCount = (int)await command.ExecuteScalarAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
            return rowCount;
        }
    }
}