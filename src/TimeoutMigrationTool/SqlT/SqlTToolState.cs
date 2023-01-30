namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Data;
    using System.Threading.Tasks;
    using Microsoft.Data.SqlClient;
    using Particular.TimeoutMigrationTool.SqlT;

    public class SqlTToolState : IToolState
    {
        public SqlTToolState(
            SqlConnection connection,
            string migrationRunId,
            IDictionary<string, string> runParameters,
            string endpointName,
            int numberOfBatches,
            MigrationStatus migrationStatus)
        {
            this.connection = connection;
            this.migrationRunId = migrationRunId;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            Status = migrationStatus;
        }

        public IDictionary<string, string> RunParameters { get; }
        public MigrationStatus Status { get; }

        public string EndpointName { get; }

        public int NumberOfBatches { get; }

        public async Task<BatchInfo> TryGetNextBatch()
        {
            await using var command = connection.CreateCommand();
            command.CommandText = SqlConstants.GetNextBatch(migrationRunId);

            await using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            if (!await reader.ReadAsync())
            {
                return null;
            }

            return new BatchInfo(reader.GetInt32(0), GetBatchStatus(reader.GetInt32(1)), reader.GetInt32(2));
        }

        BatchState GetBatchStatus(int dbStatus)
        {
            return (BatchState)dbStatus;
        }

        readonly SqlConnection connection;
        readonly string migrationRunId;
    }
}