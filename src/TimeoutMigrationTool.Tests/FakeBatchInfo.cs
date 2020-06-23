namespace TimeoutMigrationTool.Tests
{
    using System;
    using Particular.TimeoutMigrationTool;

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