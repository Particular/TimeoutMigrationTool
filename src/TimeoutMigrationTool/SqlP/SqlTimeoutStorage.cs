namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    public class SqlTimeoutStorage : ITimeoutStorage
    {
        public SqlTimeoutStorage(string sourceConnectionString, SqlDialect dialect, int batchSize, string runParameters)
        {
            connectionString = sourceConnectionString;
            this.dialect = dialect;
            this.runParameters = runParameters;
            this.batchSize = batchSize;
        }

        public async Task<ToolState> GetToolState()
        {
            using (var connection = dialect.Connect(connectionString))
            {
                ToolState state = null;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadToolState();

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (reader.HasRows && reader.Read())
                        {
                            state = new ToolState(null, null); // Deserialize reader.GetString(2));
                            state.Status = ParseMigrationStatus(reader.GetString(1));
                            state.Endpoint = new EndpointInfo { EndpointName = reader.GetString(0) };
                        }
                    }

                    if (state == null)
                    {
                        return null;
                    }

                    command.CommandText = dialect.GetScriptToLoadBatchInfo(ToolStateID);

                    state.InitBatches(await ReadBatchInfo(command).ConfigureAwait(false));

                    return state;
                }
            }
        }

        public async Task<List<BatchInfo>> Prepare(DateTime migrateTimeoutsWithDeliveryDateLaterThan, EndpointInfo endpoint)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                var command = connection.CreateCommand();
                command.CommandText = dialect.GetScriptToPrepareTimeouts(endpoint.EndpointName, batchSize);

                var migrateTimeoutsWithDeliveryDateLaterThanParameter = command.CreateParameter();
                migrateTimeoutsWithDeliveryDateLaterThanParameter.ParameterName = "migrateTimeoutsWithDeliveryDateLaterThan";
                migrateTimeoutsWithDeliveryDateLaterThanParameter.Value = migrateTimeoutsWithDeliveryDateLaterThan;
                command.Parameters.Add(migrateTimeoutsWithDeliveryDateLaterThanParameter);

                var runParametersParameter = command.CreateParameter();
                runParametersParameter.ParameterName = "RunParameters";
                runParametersParameter.Value = runParameters;
                command.Parameters.Add(runParametersParameter);

                return await ReadBatchInfo(command).ConfigureAwait(false);
            }
        }

        public async Task<List<TimeoutData>> ReadBatch(EndpointInfo endpoint, int batchNumber)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadBatch(endpoint.EndpointName);

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "BatchNumber";
                    parameter.Value = batchNumber;

                    command.Parameters.Add(parameter);

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (reader.HasRows)
                        {
                            return ReadTimeoutDataRows(reader).ToList();
                        }
                    }
                }
            }

            return null;
        }

        public async Task CompleteBatch(EndpointInfo endpoint, int number)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToCompleteBatch(endpoint.EndpointName);

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "BatchNumber";
                    parameter.Value = number;

                    command.Parameters.Add(parameter);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task StoreToolState(ToolState toolState)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToStoreToolState();

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "Status";
                    parameter.Value = toolState.Status;
                    command.Parameters.Add(parameter);

                    parameter = command.CreateParameter();
                    parameter.ParameterName = "EndpointName";
                    parameter.Value = toolState.Endpoint.EndpointName;
                    command.Parameters.Add(parameter);

                    parameter = command.CreateParameter();
                    parameter.ParameterName = "Batches";
                    parameter.Value = toolState.Batches.Count();
                    command.Parameters.Add(parameter);

                    parameter = command.CreateParameter();
                    parameter.ParameterName = "RunParameters";
                    parameter.Value = JsonConvert.SerializeObject(toolState.RunParameters);
                    command.Parameters.Add(parameter);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task Abort(ToolState toolState)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToAbortBatch(toolState.Endpoint.EndpointName);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public Task<bool> CanPrepareStorage()
        {
            return Task.FromResult(true);
        }

        public async Task<List<EndpointInfo>> ListEndpoints(DateTime migrateTimeoutsWithDeliveryDateLaterThan)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToListEndpoints();

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "CutOffTime";
                    parameter.Value = migrateTimeoutsWithDeliveryDateLaterThan;

                    command.Parameters.Add(parameter);

                    using (var reader = await command.ExecuteReaderAsync())
                    {
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
                    }
                }
            }

            return new List<EndpointInfo>();
        }

        async Task<List<BatchInfo>> ReadBatchInfo(DbCommand command)
        {
            var batches = new List<BatchInfo>();
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (reader.HasRows)
                {
                    var batchRows = ReadBatchRows(reader);

                    batches = batchRows.GroupBy(row => row.BatchNumber).Select(batchNumber => new BatchInfo
                    {
                        Number = batchNumber.Key,
                        State = batchNumber.First().Status,
                        TimeoutIds = batchNumber.Select(message => message.MessageId.ToString()).ToArray()
                    }).ToList();
                }
            }

            return batches;
        }

        IEnumerable<BatchRowRecord> ReadBatchRows(DbDataReader reader)
        {
            while (reader.Read())
            {
                yield return new BatchRowRecord
                {
                    MessageId = reader.GetGuid(0),
                    BatchNumber = reader.GetInt32(1),
                    Status = GetBatchStatus(reader.GetInt32(2))
                };
            }
        }

        MigrationStatus ParseMigrationStatus(string status)
        {
            return (MigrationStatus)Enum.Parse(typeof(MigrationStatus), status);
        }

        BatchState GetBatchStatus(int dbStatus)
        {
            return (BatchState)dbStatus;
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

        private Dictionary<string, string> GetHeaders(DbDataReader reader)
        {
            using (var stream = reader.GetTextReader(5))
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

        readonly SqlDialect dialect;
        readonly string connectionString;
        readonly string runParameters;
        readonly int batchSize;

        const string ToolStateID = "TOOLSTATE";
    }

    class BatchRowRecord
    {
        public Guid MessageId { get; internal set; }
        public int BatchNumber { get; internal set; }
        public BatchState Status { get; internal set; }
    }
}