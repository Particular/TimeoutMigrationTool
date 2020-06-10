namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ToolState
    {
        public ToolState(IDictionary<string, string> runParameters, EndpointInfo endpointInfo, IEnumerable<BatchInfo> batches)
        {
            RunParameters = runParameters;
            Endpoint = endpointInfo;
            Batches = batches;
            Status = MigrationStatus.StoragePrepared;
        }

        public IEnumerable<BatchInfo> Batches { get; }
        public IDictionary<string, string> RunParameters { get; }
        public MigrationStatus Status { get; set; }
        public EndpointInfo Endpoint { get; set; }

        public bool HasMoreBatches() => Batches.Any(x => x.State != BatchState.Completed);

        public BatchInfo GetCurrentBatch()
        {
            if (Batches.All(x => x.State == BatchState.Completed))
            {
                return null;
            }

            var stagedBatch = Batches.SingleOrDefault(x => x.State == BatchState.Staged);

            if (stagedBatch != null)
            {
                return stagedBatch;
            }

            return Batches.First(x => x.State != BatchState.Completed);
        }
    }
}