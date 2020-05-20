using System;
using System.Collections.Generic;
using System.Linq;

namespace Particular.TimeoutMigrationTool
{
    public class ToolState
    {
        public ToolState(IDictionary<string, string> runParameters)
        {
            this.RunParameters = runParameters;
        }

        private ToolState()
        {
            //TEMP: this constructor is required by RavenDB to deserialize the stored document
        }

        public IEnumerable<BatchInfo> Batches { get; private set; } = new List<BatchInfo>();
        public IDictionary<string, string> RunParameters { get; private set; } = new Dictionary<string, string>();
        public MigrationStatus Status { get; set; }

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