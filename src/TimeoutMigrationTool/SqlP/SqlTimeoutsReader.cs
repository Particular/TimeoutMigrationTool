using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.SqlP
{
    public class SqlTimeoutsReader
    {
        public async Task<List<TimeoutData>> ReadTimeoutsFrom(string connectionString, string tableName, SqlDialect dialect, CancellationToken cancellationToken)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = $@"SELECT Destination,
    SagaId,
    State,
    Time,
    Headers
from {tableName}";

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (reader.HasRows)
                        {
                            return ReadRows(reader).ToList();
                        }
                    }
                }
            }

            return null;
        }

        IEnumerable<TimeoutData> ReadRows(DbDataReader reader)
        {
            while (reader.Read())
            {
                yield return new TimeoutData
                {
                    Id = reader.GetString(0),
                    SagaId = reader.GetGuid(1),
                    State = GetBytes(reader, 2),
                    Time = reader.GetFieldValue<DateTime>(3),
                    Headers = GetHeaders(reader)
                };
            }
        }

        byte[] GetBytes(DbDataReader reader, int ordinal)
        {
            byte[] result = null;

            if (!reader.IsDBNull(ordinal))
            {
                long size = reader.GetBytes(ordinal, 0, null, 0, 0);
                result = new byte[size];
                int bufferSize = 1024;
                long bytesRead = 0;
                int curPos = 0;

                while (bytesRead < size)
                {
                    bytesRead += reader.GetBytes(ordinal, curPos, result, curPos, bufferSize);
                    curPos += bufferSize;
                }
            }

            return result;
        }

        private Dictionary<string, string> GetHeaders(DbDataReader reader)
        {
            using (var stream = reader.GetTextReader(4))
            {
                using (var jsonReader = new JsonTextReader(stream))
                {
                    return serializer.Deserialize<Dictionary<string, string>>(jsonReader);
                }
            }
        }

        static JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            DefaultValueHandling = DefaultValueHandling.Ignore
        });
    }
}