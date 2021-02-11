namespace Particular.TimeoutMigrationTool.SQS
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public class SQSEndpointTarget : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        readonly string stagingQueueUrl;

        public SQSEndpointTarget(string stagingQueueUrl)
        {
            this.stagingQueueUrl = stagingQueueUrl;
        }

        public ValueTask DisposeAsync() => throw new System.NotImplementedException();

        public ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber) => throw new System.NotImplementedException();

        public ValueTask<int> CompleteBatch(int batchNumber) => throw new System.NotImplementedException();
    }
}