namespace Particular.TimeoutMigrationTool
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class ToolState
    {
        public ToolState(IDictionary<string, string> runParameters, EndpointInfo endpointInfo)
        {
            RunParameters = runParameters;
            Endpoint = endpointInfo;
        }

        internal ToolState(IEnumerable<BatchInfo> batches, IDictionary<string, string> runParameters, MigrationStatus migrationStatus)
        {
            Batches = batches;
            RunParameters = runParameters;
            Status = migrationStatus;
        }

        public IEnumerable<BatchInfo> Batches { get; internal set; } = new List<BatchInfo>();
        public IDictionary<string, string> RunParameters { get; private set; } = new Dictionary<string, string>();
        public MigrationStatus Status { get; set; }
        public EndpointInfo Endpoint { get;  set; }

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

        public void InitBatches(IEnumerable<BatchInfo> batches)
        {
            if (Batches.Any() && (Status != MigrationStatus.NeverRun || Status != MigrationStatus.StoragePrepared))
            {
                throw new InvalidOperationException("Batches have already been initialized");
            }

            Batches = batches;
        }
    }
}