using System;
using System.Collections.Generic;
using System.Linq;

namespace Particular.TimeoutMigrationTool
{
    public class ToolState
    {
        public bool IsStoragePrepared { get; set; }
        public IEnumerable<BatchInfo> Batches { get; private set; } = new List<BatchInfo>();

        public bool HasMoreBatches() => Batches.Any(x => x.State != BatchState.Completed);

        public BatchInfo GetCurrentBatch()
        {
            if (Batches.All(x => x.State == BatchState.Completed))
                return null;

            return Batches.First(x => x.State != BatchState.Completed);
        }

        public void InitBatches(IEnumerable<BatchInfo> batches)
        {
            if (Batches.Any() && IsStoragePrepared)
                throw new InvalidOperationException("Batches have already been initialized");

            Batches = batches;
        }
    }
}