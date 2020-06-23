namespace TimeoutMigrationTool.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Particular.TimeoutMigrationTool;

    public class FakeToolState : IToolState
    {
        public IDictionary<string, string> RunParameters { get; set; }

        public string EndpointName { get; set; }

        public int NumberOfBatches
        {
            get
            {
                return Batches.Count;
            }
        }

        public List<FakeBatchInfo> Batches { get; set; }

        public Task<BatchInfo> TryGetNextBatch()
        {
            if (Batches.All(x => x.State == BatchState.Completed))
            {
                return Task.FromResult<BatchInfo>(null);
            }

            var stagedBatch = Batches.SingleOrDefault(x => x.State == BatchState.Staged);

            if (stagedBatch != null)
            {
                return Task.FromResult<BatchInfo>(stagedBatch);
            }

            var pendingBatch = Batches.First(x => x.State == BatchState.Pending);

            return Task.FromResult<BatchInfo>(pendingBatch);
        }
    }


    public class FakeBatchInfo : BatchInfo
    {
        public FakeBatchInfo(int number, BatchState state, int numberOfTimeouts) : base(number, state, numberOfTimeouts)
        {

        }

        public string[] TimeoutIds { get; set; }
        public DateTime CutoffDate { get; set; }
        public string EndpointName { get; set; }

        public void MarkAsCompleted()
        {
            State = BatchState.Completed;
        }

        public void MarkAsStaged()
        {
            State = BatchState.Staged;
        }
    }
}