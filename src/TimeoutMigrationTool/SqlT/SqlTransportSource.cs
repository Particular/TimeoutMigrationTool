namespace Particular.TimeoutMigrationTool.SqlT
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Particular.TimeoutMigrationTool.SqlP;

    class SqlTransportSource : ITimeoutsSource
    {
        const string tableSuffix = ".Delayed";
        const int longRunningQueryTimeout = 1200;
        readonly ILogger logger;
        readonly SqlConnection connection;
        readonly int batchSize;
        string migrationRunId;

        public SqlTransportSource(ILogger logger, string connectionString, int batchSize)
        {
            this.logger = logger;
            this.batchSize = batchSize;
            connection = new SqlConnection(connectionString);
        }

        public async Task Abort()
        {
            var toolState = await TryLoadOngoingMigration();

            await using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueryTimeout;
            command.CommandText = SqlConstants.GetScriptToAbort(migrationRunId, toolState.EndpointName);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTime.UtcNow;
            command.Parameters.Add(completedAtParameter);

            await command.ExecuteNonQueryAsync();
        }
        public async Task<bool> CheckIfAMigrationIsInProgress()
        {
            var toolState = await TryLoadOngoingMigration();
            return toolState != null;
        }

        public async Task Complete()
        {
            await EnsureConnectionOpen();
            await using var command = connection.CreateCommand();

            var migrationRunIdParameter = command.CreateParameter();
            migrationRunIdParameter.ParameterName = "MigrationRunId";
            migrationRunIdParameter.Value = migrationRunId;
            command.Parameters.Add(migrationRunIdParameter);

            var completedAtParameter = command.CreateParameter();
            completedAtParameter.ParameterName = "CompletedAt";
            completedAtParameter.Value = DateTime.UtcNow;
            command.Parameters.Add(completedAtParameter);

            command.CommandText = SqlConstants.MarkMigrationAsCompleted;

            await command.ExecuteNonQueryAsync();
            logger.LogInformation("Migration Completed");
        }
        public async Task<IReadOnlyList<EndpointInfo>> ListEndpoints(DateTime cutOffTime)
        {
            await EnsureConnectionOpen();

            var sql = string.Format(SqlConstants.ListEndPoints);
            await using var listEndpointsCommand = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };
            var results = new List<EndpointInfo>();
            var endpoints = new List<string>();
            using (var reader = await listEndpointsCommand.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (reader.HasRows)
                {
                    while (await reader.ReadAsync())
                    {
                        var tableName = reader.GetString(0);
                        endpoints.Add(tableName);
                    }
                }
            }

            foreach (var endpoint in endpoints)
            {
                var endpointName = endpoint.Replace(tableSuffix, string.Empty, StringComparison.InvariantCultureIgnoreCase);
                await using var listEndPointDetailCommand = new SqlCommand(string.Format(SqlConstants.ListEndPointDetails, endpoint, endpointName), connection)
                {
                    CommandType = CommandType.Text
                };
                using (var endpointDetailsReader = await listEndPointDetailCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (endpointDetailsReader.HasRows)
                    {
                        while (await endpointDetailsReader.ReadAsync())
                        {
                            results.Add(new EndpointInfo
                            {
                                EndpointName = endpointDetailsReader.GetString(0).Replace(tableSuffix, string.Empty, StringComparison.InvariantCultureIgnoreCase),
                                NrOfTimeouts = endpointDetailsReader.GetInt32(1),
                                ShortestTimeout = endpointDetailsReader.IsDBNull(2) ? DateTime.MaxValue : endpointDetailsReader.GetDateTime(2),
                                LongestTimeout = endpointDetailsReader.IsDBNull(3) ? DateTime.MaxValue : endpointDetailsReader.GetDateTime(3),
                                Destinations = endpointDetailsReader.GetString(4).Split(", ", StringSplitOptions.RemoveEmptyEntries)
                            });
                        }
                    }
                }
            }
            return results;
        }

        public async Task MarkBatchAsCompleted(int number)
        {
            await EnsureConnectionOpen();
            await using var command = connection.CreateCommand();

            command.CommandText = SqlConstants.GetScriptToCompleteBatch(migrationRunId);
            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;

            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }
        public async Task MarkBatchAsStaged(int number)
        {
            await EnsureConnectionOpen();
            await using var command = connection.CreateCommand();

            command.CommandText = string.Format(SqlConstants.MarkBatchAsStaged, migrationRunId);

            var parameter = command.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = number;
            command.Parameters.Add(parameter);

            await command.ExecuteNonQueryAsync();
        }

        public async Task<IToolState> Prepare(DateTime cutOffTime, string endpointName, IDictionary<string, string> runParameters)
        {
            migrationRunId = Guid.NewGuid().ToString().Replace("-", "");

            await EnsureConnectionOpen();
            await using var command = connection.CreateCommand();

            command.CommandTimeout = longRunningQueryTimeout;
            command.CommandText = SqlConstants.GetScriptToPrepareTimeouts(migrationRunId, endpointName, batchSize);

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

        public async Task<IReadOnlyList<TimeoutData>> ReadBatch(int batchNumber)
        {
            await EnsureConnectionOpen();

            var sql = SqlConstants.GetScriptToLoadBatch(migrationRunId);
            await using var loadBatchCommand = new SqlCommand(sql, connection)
            {
                CommandType = CommandType.Text
            };

            var parameter = loadBatchCommand.CreateParameter();
            parameter.ParameterName = "BatchNumber";
            parameter.Value = batchNumber;

            loadBatchCommand.Parameters.Add(parameter);

            await using var reader = await loadBatchCommand.ExecuteReaderAsync();
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
        public async Task<IToolState> TryLoadOngoingMigration()
        {
            await EnsureConnectionOpen();
            await using var command = connection.CreateCommand();

            command.CommandText = SqlConstants.GetScriptToLoadPendingMigrations();

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

            return new SqlTToolState(connection, migrationRunId, runParameters, endpoint, numberOfBatches, status);
        }

        async ValueTask EnsureConnectionOpen()
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync().ConfigureAwait(false);
            }
        }
        async IAsyncEnumerable<TimeoutData> ReadTimeoutDataRows(DbDataReader reader)
        {
            while (await reader.ReadAsync())
            {
                yield return new TimeoutData
                {
                    Id = reader.GetGuid(0).ToString(),
                    Destination = reader.GetString(1),
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
            using var stream = reader.GetTextReader(4);
            using var jsonReader = new JsonTextReader(stream);
            return serializer.Deserialize<Dictionary<string, string>>(jsonReader);
        }

        static JsonSerializer serializer = JsonSerializer.Create(new JsonSerializerSettings
        {
            Formatting = Formatting.Indented,
            DefaultValueHandling = DefaultValueHandling.Ignore
        });
    }
}
