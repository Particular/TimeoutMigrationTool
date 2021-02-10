namespace Particular.TimeoutMigrationTool.NHibernate
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class NHibernateToolState : IToolState
    {
        public NHibernateToolState(Func<Task<BatchInfo>> getNextBatch, string migrationRunId, IDictionary<string, string> runParameters, string endpointName, int numberOfBatches, MigrationStatus migrationStatus)
        {
            MigrationRunId = migrationRunId;
            RunParameters = runParameters;
            EndpointName = endpointName;
            NumberOfBatches = numberOfBatches;
            this.getNextBatch = getNextBatch;
            Status = migrationStatus;
        }

        public IDictionary<string, string> RunParameters { get; }
        public MigrationStatus Status { get; }

        public string EndpointName { get; }

        public int NumberOfBatches { get; }

        public string MigrationRunId { get; }

        public Task<BatchInfo> TryGetNextBatch()
        {
            return getNextBatch();
        }

        Func<Task<BatchInfo>> getNextBatch;
    }
}
