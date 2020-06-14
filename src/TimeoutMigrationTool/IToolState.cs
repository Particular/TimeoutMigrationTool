namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;

    public interface IToolState
    {
        IDictionary<string, string> RunParameters { get; }
        string EndpointName { get; }
        int NumberOfBatches { get; }

        bool HasMoreBatches();
        BatchInfo GetCurrentBatch();
    }
}