namespace Particular.TimeoutMigrationTool
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IToolState
    {
        IDictionary<string, string> RunParameters { get; }
        string EndpointName { get; }
        int NumberOfBatches { get; }

        Task<BatchInfo> TryGetNextBatch();
    }
}