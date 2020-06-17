namespace Particular.TimeoutMigrationTool.SqlP
{
    using System;
    using System.Data.Common;

    public abstract class SqlDialect
    {
        public abstract DbConnection Connect(string connectionString);

        public static SqlDialect Parse(string dialectString)
        {
            if (dialectString.Equals(MsSqlServer.Name, StringComparison.InvariantCultureIgnoreCase))
            {
                return new MsSqlServer();
            }

            throw new InvalidOperationException($"{dialectString} is not a supported dialect");
        }

        public abstract string GetScriptToPrepareTimeouts(string migrationRunId, string endpointName, int batchSize);
        public abstract string GetScriptToTryGetNextBatch(string migrationRunId);
        public abstract string GetScriptLoadPendingMigrations();
        public abstract string GetScriptToLoadBatch(string migrationRunId);
        public abstract string GetScriptToAbortMigration(string migrationRunId, string endpointName);
        public abstract string GetScriptToCompleteBatch(string migrationRunId);
        public abstract string GetScriptToListEndpoints();
        public abstract string GetScriptToMarkBatchAsStaged(string migrationRunId);
        public abstract string GetScriptToMarkMigrationAsCompleted();
    }
}