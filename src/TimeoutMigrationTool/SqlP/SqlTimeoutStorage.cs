namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Linq;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    public class SqlTimeoutStorage : ITimeoutStorage
    {
        public SqlTimeoutStorage(string sourceConnectionString, SqlDialect dialect, int batchSize)
        {
            connectionString = sourceConnectionString;
            this.dialect = dialect;
            this.batchSize = batchSize;
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptLoadPendingMigrations();

            using var reader = await command.ExecuteReaderAsync();

            if (!reader.Read())
            {
                return null;
            }

            migrationRunId = reader.GetString(0);
            var endpoint = reader.GetString(1);
            var runParameters = JsonConvert.DeserializeObject<Dictionary<string, string>>(reader.GetString(2));
            var numberOfBatches = reader.GetInt32(3);

            if (reader.Read())
            {
                throw new Exception("Multiple uncompleted migrations found");
            }

            return new SqlPToolState(connectionString, dialect, migrationRunId, runParameters, endpoint, numberOfBatches);
        }

        public async Task<IToolState> Prepare(DateTime cutOffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            migrationRunId = Guid.NewGuid().ToString().Replace("-", "");

            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueuerTimeout;
            command.CommandText = dialect.GetScriptToPrepareTimeouts(migrationRunId, endpointName, batchSize);

            var runParametersParameter = command.CreateParameter();
            runParametersParameter.ParameterName = "RunParameters";
            runParametersParameter.Value = JsonConvert.SerializeObject(runParameters);
            command.Parameters.Add(runParametersParameter);

            var cutOffTimeParameter = command.CreateParameter();
            cutOffTimeParameter.ParameterName = "CutOffTime";
            cutOffTimeParameter.Value = cutOffTime;
            command.Parameters.Add(cutOffTimeParameter);

            var startedAtParameter = command.CreateParameter();
            startedAtParameter.ParameterName = "StartedAt";
            startedAtParameter.Value = DateTime.UtcNow;
            command.Parameters.Add(startedAtParameter);

            await command.ExecuteNonQueryAsync();

            return await TryLoadOngoingMigration();
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToLoadBatch(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = batchNumber;

            command.Parameters.Add(parameter);

            using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            if (reader.HasRows)
            {
                return ReadTimeoutDataRows(reader).ToList();
            }

            return null;
        }

        public async Task MarkBatchAsCompleted(int number)
        {
            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToCompleteBatch(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;

            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task MarkBatchAsStaged(int number)
        {
            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToMarkBatchAsStaged(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;

            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task Complete()
        {
            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            var migrationRunIdParameter = command.CreateParameter();
            migrationRunIdParameter.ParameterName = "MigrationRunId";
            migrationRunIdParameter.Value = migrationRunId;
            command.Parameters.Add(migrationRunIdParameter);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTime.UtcNow;
            command.Parameters.Add(completedAtParameter);

            command.CommandText = dialect.GetScriptToMarkMigrationAsCompleted();

            await command.ExecuteNonQueryAsync();
        }

        public async Task<bool> CheckIfAMigrationIsInProgress()
        {
            var toolState = await TryLoadOngoingMigration();
            return toolState != null;
        }

        public async Task Abort()
        {
            var toolState = await TryLoadOngoingMigration();

            using var connection = dialect.Connect(connectionString);
            using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueuerTimeout;
            command.CommandText = dialect.GetScriptToAbortMigration(migrationRunId, toolState.EndpointName);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTime.UtcNow;
            command.Parameters.Add(completedAtParameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task<List<EndpointInfo>> ListEndpoints(DateTime migrateTimeoutsWithDeliveryDateLaterThan)
        {
            using var connection = dialect.Connect(connectionString);

            using var command = connection.CreateCommand();
            command.CommandText = dialect.GetScriptToListEndpoints();

            var parameter = command.CreateParameter();
            parameter.ParameterName = "CutOffTime";
            parameter.Value = migrateTimeoutsWithDeliveryDateLaterThan;

            command.Parameters.Add(parameter);

            using var reader = await command.ExecuteReaderAsync();

            if (reader.HasRows)
            {
                var results = new List<EndpointInfo>();
                while (reader.Read())
                {
                    results.Add(new EndpointInfo
                    {
                        EndpointName = reader.GetString(0),
                        NrOfTimeouts = reader.GetInt32(1),
                        LongestTimeout = reader.GetDateTime(2),
                        ShortestTimeout = reader.GetDateTime(3),
                        Destinations = reader.GetString(4).Split(", ", StringSplitOptions.RemoveEmptyEntries)
                    });
                }

                return results;
            }


            return new List<EndpointInfo>();
        }

        IEnumerable<TimeoutData> ReadTimeoutDataRows(DbDataReader reader)
        {
            while (reader.Read())
            {
                yield return new TimeoutData
                {
                    Id = reader.GetGuid(0).ToString(),
                    Destination = reader.GetString(1),
                    SagaId = reader.GetGuid(2),
                    State = GetBytes(reader, 3),
                    Time = reader.GetFieldValue<DateTime>(4),
                    Headers = GetHeaders(reader)
                };
            }
        }

        byte[] GetBytes(DbDataReader reader, int ordinal)
        {
            byte[] result = null;

            if (!reader.IsDBNull(ordinal))
            {
                var size = reader.GetBytes(ordinal, 0, null, 0, 0);
                result = new byte[size];
                const int bufferSize = 1024;
                long bytesRead = 0;
                var curPos = 0;

                while (bytesRead < size)
                {
                    bytesRead += reader.GetBytes(ordinal, curPos, result, curPos, bufferSize);
                    curPos += bufferSize;
                }
            }

            return result;
        }

        Dictionary<string, string> GetHeaders(DbDataReader reader)
        {
            using var stream = reader.GetTextReader(5);
            using var jsonReader = new JsonTextReader(stream);
            return serializer.Deserialize<Dictionary<string, string>>(jsonReader);
        }

        static JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            DefaultValueHandling = DefaultValueHandling.Ignore
        });

        string migrationRunId;
        int longRunningQueuerTimeout = 1200;

        readonly SqlDialect dialect;
        readonly string connectionString;
        readonly int batchSize;
    }
}