﻿namespace TimeoutMigrationTool.Tests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeToolState : IToolState
    {
        public IDictionary<string, string> RunParameters { get; set; }
        public MigrationStatus Status { get; set; }

        public string EndpointName { get; set; }

        public int NumberOfBatches => Batches.Count;

        public List<BatchInfo> Batches { get; set; }

        public Task<BatchInfo> TryGetNextBatch()
        {
            if (Batches.All(x => x.State == BatchState.Completed))
            {
                return Task.FromResult<BatchInfo>(null);
            }

            var stagedBatch = Batches.SingleOrDefault(x => x.State == BatchState.Staged);

            if (stagedBatch != null)
            {
                return Task.FromResult(stagedBatch);
            }

            var pendingBatch = Batches.First(x => x.State == BatchState.Pending);

            return Task.FromResult(pendingBatch);
        }
    }
}