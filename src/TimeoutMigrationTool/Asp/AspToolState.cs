namespace Particular.TimeoutMigrationTool.Asp
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class AspToolState : IToolState
    {
        readonly Func<Task<BatchInfo>> getNextBatch;

        public AspToolState(Func<Task<BatchInfo>> getNextBatch, IDictionary<string, string> runParameters, string endpointName, int numberOfBatches)
        {
            this.getNextBatch = getNextBatch;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
        }

        public IDictionary<string, string> RunParameters { get; }
        public string EndpointName { get; }
        public int NumberOfBatches { get; }
        public Task<BatchInfo> TryGetNextBatch()
        {
            return getNextBatch();
        }
    }
}