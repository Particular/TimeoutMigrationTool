﻿namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class RavenDBTimeoutStorage : ITimeoutStorage
    {
        public RavenDBTimeoutStorage(string serverUrl, string databaseName, string prefix, RavenDbVersion ravenVersion)
        {
            this.serverUrl = serverUrl;
            this.databaseName = databaseName;
            this.prefix = prefix;
            this.ravenVersion = ravenVersion;
        }

        public Task<ToolState> GetOrCreateToolState()
        {
            throw new System.NotImplementedException();
        }

        public Task<List<BatchInfo>> Prepare()
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

        readonly string serverUrl;
        readonly string databaseName;
        readonly string prefix;
        readonly RavenDbVersion ravenVersion;
    }
}