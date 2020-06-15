namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    public interface IRavenTestSuite
    {
        ICanTalkToRavenVersion RavenAdapter { get; }
        string ServerName { get; }
        string DatabaseName { get; }
        RavenDbVersion RavenVersion { get; }
        string EndpointName { get; set; }
        Task SetupDatabase();
        Task InitTimeouts(int nrOfTimeouts, bool alternateEndpoints = false);
        ToolState SetupToolState(DateTime cutoffTime);
        Task<List<RavenBatchInfo>> SetupExistingBatchInfoInDatabase();
        Task SaveToolState(ToolState toolState);
        Task<ToolState> GetToolState();
        Task<List<RavenBatchInfo>> GetBatches(string[] ids);
        Task TeardownDatabase();
    }
}