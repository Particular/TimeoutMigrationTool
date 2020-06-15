namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class RavenToolState : IToolState
    {
        public RavenToolState(IDictionary<string, string> runParameters, string endpointName, IEnumerable<RavenBatch> batches)
        {
            RunParameters = runParameters;
            EndpointName = endpointName;
            Batches = batches;
        }

        public IEnumerable<RavenBatch> Batches { get; }
        public IDictionary<string, string> RunParameters { get; }
        public string EndpointName { get; set; }

        public int NumberOfBatches => Batches.Count();

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
}