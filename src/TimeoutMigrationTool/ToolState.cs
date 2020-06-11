namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ToolState
    {
        public ToolState(IDictionary<string, string> runParameters, string endpointName, IEnumerable<BatchInfo> batches)
        {
            RunParameters = runParameters;
            EndpointName = endpointName;
            Batches = batches;
            Status = MigrationStatus.StoragePrepared;
        }

        public IEnumerable<BatchInfo> Batches { get; }
        public IDictionary<string, string> RunParameters { get; }
        public MigrationStatus Status { get; set; }
        public string EndpointName { get; set; }

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