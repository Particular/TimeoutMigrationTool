namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class SqlPToolState : IToolState
    {
        public SqlPToolState(
            string connectionString,
            SqlDialect dialect,
            string migrationRunId,
            IDictionary<string, string> runParameters,
            string endpointName,
            int numberOfBatches)
        {
            this.connectionString = connectionString;
            this.dialect = dialect;
            this.migrationRunId = migrationRunId;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
        }

        public IDictionary<string, string> RunParameters { get; }

        public string EndpointName { get; }

        public int NumberOfBatches { get; }

        public async Task<BatchInfo> TryGetNextBatch()
        {
            await using var connection = dialect.Connect(connectionString);
            await using var command = connection.CreateCommand();
            command.CommandText = dialect.GetScriptToTryGetNextBatch(migrationRunId);

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

        readonly string connectionString;
        readonly SqlDialect dialect;
        readonly string migrationRunId;
    }
}