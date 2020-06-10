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
        public SqlTimeoutStorage(string sourceConnectionString, SqlDialect dialect, int batchSize)
        {
            connectionString = sourceConnectionString;
            this.dialect = dialect;
            this.batchSize = batchSize;
        }

        public async Task<ToolState> TryLoadOngoingMigration()
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadToolState();

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (!reader.HasRows)
                        {
                            return null;
                        }

                        var endpoint = new EndpointInfo { EndpointName = reader.GetString(0) };
                        var status = ParseMigrationStatus(reader.GetString(1));
                        var runParameters = JsonConvert.DeserializeObject<Dictionary<string, string>>(reader.GetString(2));

                        if(reader.Read())
                        {
                            throw new Exception("Multiple uncompleted migrations found");
                        }

                        command.CommandText = dialect.GetScriptToLoadBatchInfo();

                        var batches = await ExecuteCommandThatReturnsBatches(command).ConfigureAwait(false);
                        return new ToolState(runParameters, endpoint, batches)
                        {
                            Status = status
                        };
                    }
                }
            }
        }

        public async Task<ToolState> Prepare(DateTime migrateTimeoutsWithDeliveryDateLaterThan, EndpointInfo endpoint, IDictionary<string, string> runParameters)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                var command = connection.CreateCommand();
                command.CommandText = dialect.GetScriptToPrepareTimeouts(endpoint.EndpointName, batchSize);

                var migrateTimeoutsWithDeliveryDateLaterThanParameter = command.CreateParameter();
                migrateTimeoutsWithDeliveryDateLaterThanParameter.ParameterName = "migrateTimeoutsWithDeliveryDateLaterThan";
                migrateTimeoutsWithDeliveryDateLaterThanParameter.Value = migrateTimeoutsWithDeliveryDateLaterThan;
                command.Parameters.Add(migrateTimeoutsWithDeliveryDateLaterThanParameter);

                var batches = await ExecuteCommandThatReturnsBatches(command).ConfigureAwait(false);
                var toolState = new ToolState(runParameters, endpoint, batches);
                await StoreToolState(toolState); // todo: pass in the connection
                return toolState;
            }
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadBatch();

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

        public async Task MarkBatchAsCompleted(int number)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToCompleteBatch();

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "BatchNumber";
                    parameter.Value = number;

                    command.Parameters.Add(parameter);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public async Task MarkBatchAsStaged(int number)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToMarkBatchAsStaged();

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

        public async Task Abort()
        {
            var toolState = await TryLoadOngoingMigration();

            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToAbortMigration(toolState.Endpoint.EndpointName);

                    await command.ExecuteNonQueryAsync();
                }
            }
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

        async Task<List<BatchInfo>> ExecuteCommandThatReturnsBatches(DbCommand command)
        {
            var batches = new List<BatchInfo>();
            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                if (reader.HasRows)
                {
                    var batchRows = ReadBatchRows(reader);

                    //TODO Do a group by in the DB?
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

        public async Task Complete()
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToMarkMigrationAsCompleted();

                    //var parameter = command.CreateParameter();
                    //parameter.ParameterName = "MigrationRunId";
                    //parameter.Value = $"Completed_{DateTime.Now.ToShortTimeString()}";

                    //command.Parameters.Add(parameter);

                    await command.ExecuteNonQueryAsync();
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
        readonly int batchSize;
    }

    class BatchRowRecord
    {
        public Guid MessageId { get; internal set; }
        public int BatchNumber { get; internal set; }
        public BatchState Status { get; internal set; }
    }
}