using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.NHibernate
{
    public class NHibernateToolState : IToolState
    {
        public NHibernateToolState(Func<Task<BatchInfo>> getNextBatch, string migrationRunId, IDictionary<string, string> runParameters, string endpointName, int numberOfBatches)
        {
            MigrationRunId = migrationRunId;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            this.getNextBatch = getNextBatch;
        }

        public IDictionary<string, string> RunParameters { get; }

        public string EndpointName { get; }

        public int NumberOfBatches { get; }

        public string MigrationRunId { get; }

        public Task<BatchInfo> TryGetNextBatch()
        {
            return getNextBatch();
        }

        private Func<Task<BatchInfo>> getNextBatch;
    }
}
