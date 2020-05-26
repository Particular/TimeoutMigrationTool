namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using System.Data.Common;
    using System.Threading.Tasks;

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

        public Task<ToolState> GetToolState()
        {
            throw new System.NotImplementedException();
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

        BatchState GetBatchStatus(int dbStatus)
        {
            return BatchState.Pending;
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            throw new System.NotImplementedException();
        }

        public Task CompleteBatch(int number)
        {
            throw new System.NotImplementedException();
        }

        public Task StoreToolState(ToolState toolState)
        {
            throw new System.NotImplementedException();
        }

        public Task Abort(ToolState toolState)
        {
            throw new NotImplementedException();
        }

        public Task<bool> CanPrepareStorage()
        {
            throw new NotImplementedException();
        }

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