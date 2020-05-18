namespace Particular.TimeoutMigrationTool
{
    using Particular.TimeoutMigrationTool.RavenDB;
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

        public Task<StorageInfo> Prepare()
        {
            throw new System.NotImplementedException();
        }

        public Task<List<TimeoutData>> ReadBatch(int batchNumber)
        {
            throw new System.NotImplementedException();
        }

        readonly string serverUrl;
        readonly string databaseName;
        readonly string prefix;
        readonly RavenDbVersion ravenVersion;
    }
}