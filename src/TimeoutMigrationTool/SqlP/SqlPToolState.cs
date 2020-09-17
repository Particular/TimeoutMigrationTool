namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Linq;
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

        public string EndpointName { get; set; }

        public int NumberOfBatches { get; }

        public async Task<BatchInfo> TryGetNextBatch()
        {
            using (var connection = dialect.Connect(connectionString))
            {
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = dialect.GetScriptToTryGetNextBatch(migrationRunId);

                    using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    {
                        if (!reader.Read())
                        {
                            return null;
                        }

                        return new BatchInfo(reader.GetInt32(0), GetBatchStatus(reader.GetInt32(1)), reader.GetInt32(2));
                    }
                }
            }
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