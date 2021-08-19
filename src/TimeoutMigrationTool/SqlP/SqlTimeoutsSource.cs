namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    public class SqlTimeoutsSource : ITimeoutsSource
    {
        public SqlTimeoutsSource(string sourceConnectionString, SqlDialect dialect, int batchSize)
        {
            connectionString = sourceConnectionString;
            this.dialect = dialect;
            this.batchSize = batchSize;
        }

        public async Task<IToolState> TryLoadOngoingMigration()
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptLoadPendingMigrations();

            await using var reader = await command.ExecuteReaderAsync();

            if (!await reader.ReadAsync())
            {
                return null;
            }

            migrationRunId = reader.GetString(0);
            var endpoint = reader.GetString(1);
            var status = (MigrationStatus)reader.GetInt32(2);
            var runParameters = JsonConvert.DeserializeObject<Dictionary<string, string>>(reader.GetString(3));
            var numberOfBatches = reader.GetInt32(4);

            if (await reader.ReadAsync())
            {
                throw new Exception("Multiple uncompleted migrations found");
            }

            return new SqlPToolState(connectionString, dialect, migrationRunId, runParameters, endpoint, numberOfBatches, status);
        }

        public async Task<IToolState> Prepare(DateTimeOffset cutOffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            migrationRunId = Guid.NewGuid().ToString().Replace("-", "");

            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueryTimeout;
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
            startedAtParameter.Value = DateTimeOffset.UtcNow;
            command.Parameters.Add(startedAtParameter);

            await command.ExecuteNonQueryAsync();

            return await TryLoadOngoingMigration();
        }

        public async Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToLoadBatch(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = batchNumber;

            command.Parameters.Add(parameter);

            await using var reader = await command.ExecuteReaderAsync();
            List<TimeoutData> results = null;
            if (reader.HasRows)
            {
                results = new List<TimeoutData>();
                await foreach (var timeoutDataRow in ReadTimeoutDataRows(reader))
                {
                    results.Add(timeoutDataRow);
                }
            }

            return results;
        }

        public async Task MarkBatchAsCompleted(int number)
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToCompleteBatch(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;

            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task MarkBatchAsStaged(int number)
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandText = dialect.GetScriptToMarkBatchAsStaged(migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;

            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task Complete()
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            var migrationRunIdParameter = command.CreateParameter();
            migrationRunIdParameter.ParameterName = "MigrationRunId";
            migrationRunIdParameter.Value = migrationRunId;
            command.Parameters.Add(migrationRunIdParameter);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTimeOffset.UtcNow;
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

            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueryTimeout;
            command.CommandText = dialect.GetScriptToAbortMigration(migrationRunId, toolState.EndpointName);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTimeOffset.UtcNow;
            command.Parameters.Add(completedAtParameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTimeOffset migrateTimeoutsWithDeliveryDateLaterThan)
        {
            await using var connection = dialect.Connect(connectionString);

            await using var command = connection.CreateCommand();
            command.CommandText = dialect.GetScriptToListEndpoints();

            var parameter = command.CreateParameter();
            parameter.ParameterName = "CutOffTime";
            parameter.Value = migrateTimeoutsWithDeliveryDateLaterThan;

            command.Parameters.Add(parameter);

            await using var reader = await command.ExecuteReaderAsync();

            var results = new List<EndpointInfo>();
            if (reader.HasRows)
            {

                while (await reader.ReadAsync())
                {
                    results.Add(new EndpointInfo
                    {
                        EndpointName = reader.GetString(0),
                        NrOfTimeouts = reader.GetInt32(1),
                        LongestTimeout = reader.GetFieldValue<DateTimeOffset>(2),
                        ShortestTimeout = reader.GetFieldValue<DateTimeOffset>(3),
                        Destinations = reader.GetString(4).Split(", ", StringSplitOptions.RemoveEmptyEntries)
                    });
                }
            }
            return results;
        }

        async IAsyncEnumerable<TimeoutData> ReadTimeoutDataRows(DbDataReader reader)
        {
            while (await reader.ReadAsync())
            {
                yield return new TimeoutData
                {
                    Id = reader.GetGuid(0).ToString(),
                    Destination = reader.GetString(1),
                    SagaId = reader.GetGuid(2),
                    State = GetBytes(reader, 3),
                    Time = reader.GetFieldValue<DateTimeOffset>(4),
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
        int longRunningQueryTimeout = 1200;

        readonly SqlDialect dialect;
        readonly string connectionString;
        readonly int batchSize;
    }
}