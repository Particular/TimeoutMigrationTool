namespace Particular.TimeoutMigrationTool.SQS
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.SQS;
    using Amazon.SQS.Model;

    public class SQSTimeoutsTarget : ITimeoutsTarget
    {
        readonly AmazonSQSClient sqsClient;
        readonly string queueNamePrefix;

        public SQSTimeoutsTarget(string queueNamePrefix = null)
        {
            this.queueNamePrefix = queueNamePrefix;
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

        public async ValueTask<ITimeoutsTarget.IEndpointTargetBatchMigrator> PrepareTargetEndpointBatchMigrator(
            string endpointName)
        {
            var delayedDeliveryStagingQueueName = $"{endpointName}-staging{SQSConstants.FifoQueuePostfix}";
            var delayedDeliveryStagingQueuePhysicalAddress = QueueNameHelper.GetQueueName(delayedDeliveryStagingQueueName, queueNamePrefix);
            var createQueueRequest = new CreateQueueRequest
            {
                QueueName = delayedDeliveryStagingQueuePhysicalAddress,
                Attributes = new Dictionary<string, string>
                {
                    { "FifoQueue", "true" }
                }
            };

            var createQueueResponse = await sqsClient.CreateQueueAsync(createQueueRequest).ConfigureAwait(false);

            return new SQSEndpointTarget(sqsClient, createQueueResponse.QueueUrl);
        }

        public async ValueTask Abort(string endpointName)
        {
            var delayedDeliveryStagingQueueName = $"{endpointName}-staging{SQSConstants.FifoQueuePostfix}";
            var delayedDeliveryStagingQueuePhysicalAddress = QueueNameHelper.GetQueueName(delayedDeliveryStagingQueueName, queueNamePrefix);

            var response = await sqsClient.GetQueueUrlAsync(delayedDeliveryStagingQueuePhysicalAddress);

            await sqsClient.DeleteQueueAsync(response.QueueUrl);
        }

        public async ValueTask Complete(string endpointName)
        {
            // TODO cleanup the queue url fetching and physical name creation to make this better and somehow shared with delete staging queue
            var delayedDeliveryStagingQueueName = $"{endpointName}-staging{SQSConstants.FifoQueuePostfix}";
            var delayedDeliveryStagingQueuePhysicalAddress = QueueNameHelper.GetQueueName(delayedDeliveryStagingQueueName, queueNamePrefix);

            var response = await sqsClient.GetQueueUrlAsync(delayedDeliveryStagingQueuePhysicalAddress);

            var queueAttributes = await sqsClient.GetQueueAttributesAsync(response.QueueUrl, new List<string> { "*" });
            if (queueAttributes.ApproximateNumberOfMessages > 0)
            {
                throw new Exception("TODO");
            }

            await sqsClient.DeleteQueueAsync(response.QueueUrl);
        }
    }
}