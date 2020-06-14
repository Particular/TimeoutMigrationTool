namespace Particular.TimeoutMigrationTool.SqlP
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class SqlPToolState : IToolState
    {
        public SqlPToolState(IDictionary<string, string> runParameters, string endpointName, IEnumerable<BatchInfo> batches, int numberOfBatches)
        {
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            this.batches = batches;
        }

        public IDictionary<string, string> RunParameters { get; }

        public string EndpointName { get; set; }

        public int NumberOfBatches { get; }

        public Task<BatchInfo> TryGetNextBatch()
        {
            if (batches.All(x => x.State == BatchState.Completed))
            {
                return Task.FromResult<BatchInfo>(null);
            }

            var stagedBatch = batches.SingleOrDefault(x => x.State == BatchState.Staged);

            if (stagedBatch != null)
            {
                return Task.FromResult(stagedBatch);
            }

            return Task.FromResult( batches.First(x => x.State != BatchState.Completed));
        }

        IEnumerable<BatchInfo> batches;
    }
}