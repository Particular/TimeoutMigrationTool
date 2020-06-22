namespace TimeoutMigrationTool.Raven.IntegrationTests
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using Particular.TimeoutMigrationTool.RavenDB;

    public interface IRavenTestSuite
    {
        ICanTalkToRavenVersion RavenAdapter { get; }
        ILogger Logger { get; }
        string ServerName { get; }
        string DatabaseName { get; }
        RavenDbVersion RavenVersion { get; }
        string EndpointName { get; set; }
        Task SetupDatabase();
        Task InitTimeouts(int nrOfTimeouts, bool alternateEndpoints = false, string prefixIdsWith = null);
        RavenToolState SetupToolState(DateTime cutoffTime);
        Task<List<RavenBatch>> SetupExistingBatchInfoInDatabase();
        Task SaveToolState(RavenToolState toolState);
        Task<RavenToolState> GetToolState();
        Task<List<RavenBatch>> GetBatches(string[] ids);
        Task TeardownDatabase();
        Task CreateLegacyTimeoutManagerIndex();
    }
}