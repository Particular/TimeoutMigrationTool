﻿namespace Particular.TimeoutMigrationTool.SqlP
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

        public Task<StorageInfo> Prepare()
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

        readonly string sourceConnectionString;
        readonly SqlDialect dialect;
        readonly string timeoutTableName;
    }
}