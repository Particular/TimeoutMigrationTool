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
        public SqlTimeoutStorage(string sourceConnectionString, SqlDialect dialect, string timeoutTableName, int batchSize, string runParameters)
        {
            connectionString = sourceConnectionString;
            this.dialect = dialect;
            this.timeoutTableName = timeoutTableName;
            this.runParameters = runParameters;
            this.batchSize = batchSize;
        }

        public async Task<ToolState> GetToolState()
        {
            using (var connection = dialect.Connect(connectionString))
            {
                ToolState state = null;
                string endpointName = null;

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadToolState(timeoutTableName);

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (reader.HasRows && reader.Read())
                        {
                            state = new ToolState(null); // Deserialize reader.GetString(2));
                            state.Status = ParseMigrationStatus(reader.GetString(1));
                            endpointName = reader.GetString(0);
                        }
                    }

                    if (state == null)
                    {
                        throw new ApplicationException("No migration found");
                    }

                    var batchInfoCommand = connection.CreateCommand();
                    command.CommandText = dialect.GetScriptToLoadBatchInfo(endpointName);

                    state.InitBatches(await ReadBatchInfo(command).ConfigureAwait(false));

                    return state;
                }
            }
        }

        public async Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                var command = connection.CreateCommand();
                command.CommandText = dialect.GetScriptToPrepareTimeouts(timeoutTableName, batchSize);

                var cutOffParameter = command.CreateParameter();
                cutOffParameter.ParameterName = "maxCutOff";
                cutOffParameter.Value = maxCutoffTime;
                command.Parameters.Add(cutOffParameter);

                var runParametersParameter = command.CreateParameter();
                runParametersParameter.ParameterName = "RunParameters";
                runParametersParameter.Value = runParameters;
                command.Parameters.Add(runParametersParameter);

                return await ReadBatchInfo(command).ConfigureAwait(false);
            }
        }

        public async Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToLoadBatch(timeoutTableName);

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

        public Task CompleteBatch(int number)
        {
            throw new System.NotImplementedException();
        }

        public async Task StoreToolState(ToolState toolState)
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToStoreToolState(timeoutTableName);

                    var parameter = command.CreateParameter();
                    parameter.ParameterName = "Status";
                    parameter.Value = toolState.Status;

                    command.Parameters.Add(parameter);

                    await command.ExecuteNonQueryAsync();
                }
            }
        }

        public Task Abort(ToolState toolState)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CanPrepareStorage()
        {
            return Task.FromResult(true);
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
        readonly string timeoutTableName;
        readonly int batchSize;
    }

    class BatchRowRecord
    {
        public Guid MessageId { get; internal set; }
        public int BatchNumber { get; internal set; }
        public BatchState Status { get; internal set; }
    }
}