using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.SqlP
{
    class SqlTimeoutsReader
    {
        public async Task<List<TimeoutData>> ReadTimeoutsFrom(string connectionString, SqlDialect dialect, CancellationToken cancellationToken)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    var tableName = "TimeoutData"; // [{schema}].[{prefix}TimeoutData]
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

            throw new NotImplementedException();
        }

        IEnumerable<TimeoutData> ReadRows(DbDataReader reader)
        {
            while (reader.Read())
            {
                yield return new TimeoutData
                {
                    Id = reader.GetGuid(0).ToString(),
                    SagaId = reader.GetGuid(1),
                    //State = reader.GetBytes(2, 0, ..)
                    Time = reader.GetFieldValue<DateTime>(3),
                    Headers = GetHeaders(reader)
                };
            }
        }

        private Dictionary<string, string> GetHeaders(DbDataReader reader)
        {
            using (var stream = reader.GetTextReader(4))
            {
                using (var jsonReader = new JsonTextReader(stream))
                {
                    var serializer = JsonSerializer.Create(new JsonSerializerSettings
                    {
                        Formatting = Formatting.Indented,
                        DefaultValueHandling = DefaultValueHandling.Ignore
                    });

                    return serializer.Deserialize<Dictionary<string, string>>(jsonReader);
                }
            }
        }
    }
}