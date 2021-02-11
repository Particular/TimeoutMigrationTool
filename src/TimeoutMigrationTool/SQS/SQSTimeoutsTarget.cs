namespace Particular.TimeoutMigrationTool.SQS
{
    using System.Threading.Tasks;
    using Amazon.SQS;
    using Amazon.SQS.Model;

    public class SQSTimeoutsTarget : ITimeoutsTarget
    {
        readonly AmazonSQSClient sqsClient;

        public SQSTimeoutsTarget()
        {
            sqsClient = new AmazonSQSClient();
        }

        public async ValueTask<MigrationCheckResult> AbleToMigrate(EndpointInfo endpoint)
        {
            var migrationCheckResult = new MigrationCheckResult();
            foreach (var destinationName in endpoint.Destinations)
            {
                try
                {
                    await sqsClient.GetQueueUrlAsync(destinationName + SQSConstants.FifoQueuePostfix);
                }
                catch (QueueDoesNotExistException)
                {
                    migrationCheckResult.Problems.Add($"There is no fifo queue for destination {destinationName}.");
                }
            }

            return migrationCheckResult;
        }

        public ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(string endpointName) => throw new System.NotImplementedException();

        public ValueTask Abort(string endpointName) => throw new System.NotImplementedException();

        public ValueTask Complete(string endpointName) => throw new System.NotImplementedException();
    }
}