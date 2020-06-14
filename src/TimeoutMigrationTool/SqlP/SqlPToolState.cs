namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Linq;

    public class SqlPToolState : IToolState
    {
        public SqlPToolState(IDictionary<string, string> runParameters, string endpointName, IEnumerable<BatchInfo> batches, int numberOfBatches)
        {
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            this.batches = batches;
        }

        IEnumerable<BatchInfo> batches;
        public IDictionary<string, string> RunParameters { get; }
        public string EndpointName { get; set; }

        public int NumberOfBatches { get; }

        public bool HasMoreBatches() => batches.Any(x => x.State != BatchState.Completed);

        public BatchInfo GetCurrentBatch()
        {
            if (batches.All(x => x.State == BatchState.Completed))
            {
                return null;
            }

            var stagedBatch = batches.SingleOrDefault(x => x.State == BatchState.Staged);

            if (stagedBatch != null)
            {
                return stagedBatch;
            }

            return batches.First(x => x.State != BatchState.Completed);
        }
    }
}