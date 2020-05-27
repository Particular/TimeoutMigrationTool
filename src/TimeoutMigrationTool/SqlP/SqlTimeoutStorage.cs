using System;

namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class SqlTimeoutStorage : ITimeoutStorage
    {
        public SqlTimeoutStorage(string sourceConnectionString, SqlDialect dialect, string timeoutTableName)
        {
            this.sourceConnectionString = sourceConnectionString;
            this.dialect = dialect;
            this.timeoutTableName = timeoutTableName;
        }

        public Task<ToolState> GetToolState()
        {
            throw new System.NotImplementedException();
        }

        public Task<List<BatchInfo>> Prepare(DateTime maxCutoffTime, EndpointInfo endpoint)
        {
            throw new System.NotImplementedException();
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

        public Task<List<EndpointInfo>> ListEndpoints()
        {
            throw new NotImplementedException();
        }

        readonly string sourceConnectionString;
        readonly SqlDialect dialect;
        readonly string timeoutTableName;
    }
}