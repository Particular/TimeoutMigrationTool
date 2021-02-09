namespace Particular.TimeoutMigrationTool.Asp
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class AspToolState : IToolState
    {
        readonly Func<Task<BatchInfo>> getNextBatch;

        public AspToolState(Func<Task<BatchInfo>> getNextBatch, IDictionary<string, string> runParameters, string endpointName, int numberOfBatches, MigrationStatus migrationStatus)
        {
            this.getNextBatch = getNextBatch;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            Status = migrationStatus;
        }

        public IDictionary<string, string> RunParameters { get; }
        public MigrationStatus Status { get; }
        public string EndpointName { get; }
        public int NumberOfBatches { get; }
        public Task<BatchInfo> TryGetNextBatch()
        {
            return getNextBatch();
        }
    }
}