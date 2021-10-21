namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;

    public static class SqlTQueueCreator
    {
        public static async Task CreateStagingQueue(SqlConnection connection, string tableName, string schema, string databaseName, bool preview = false)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(SqlConstants.CreateDelayedMessageStoreText, tableName, schema, databaseName);
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

        public static async Task DeleteStagingQueue(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(SqlConstants.DeleteDelayedMessageStoreText, tableName, schema, databaseName);
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
            var sql = string.Format(SqlConstants.MoveFromStagingToDelayedTableText, fromTable, fromSchema, toTable, toSchema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            var rowCount = (int)await command.ExecuteScalarAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
            return rowCount;
        }

        public static async Task TruncateTable(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            await using var transaction = connection.BeginTransaction();
            var sql = string.Format(SqlConstants.TruncateTableText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection, transaction)
            {
                CommandType = CommandType.Text
            };
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await transaction.CommitAsync().ConfigureAwait(false);
        }

        public static async Task<string> DoesDelayedDeliveryTableExist(SqlConnection connection, string tableName, string schema, string databaseName)
        {
            var underscorePositions = tableName.AllIndexesOf("_").ToArray();

            var sql = string.Format(SqlConstants.DelayedMessageStoreExistsText, tableName, schema, databaseName);
            await using var command = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };
            var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            while (await reader.ReadAsync().ConfigureAwait(false))
            {
                var candidate = reader.GetString(0);

                //Return only table names that differ by having dots (.) in place of underscores in the table name returned from the source.
                if (underscorePositions.All(i => candidate[i] == '_' || candidate[i] == '.'))
                {
                    return candidate;
                }
            }

            return null;
        }

        static IEnumerable<int> AllIndexesOf(this string str, string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new ArgumentException("the string to find may not be empty", nameof(value));
            }

            for (int index = 0; ; index += value.Length)
            {
                index = str.IndexOf(value, index);
                if (index == -1)
                {
                    break;
                }
                yield return index;
            }
        }
    }
}