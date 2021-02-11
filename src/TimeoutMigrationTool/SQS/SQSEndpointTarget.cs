namespace Particular.TimeoutMigrationTool.SQS
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.SQS;
    using Amazon.SQS.Model;
    using Newtonsoft.Json;

    public class SQSEndpointTarget : ITimeoutsTarget.IEndpointTargetBatchMigrator
    {
        readonly string stagingQueueUrl;
        readonly AmazonSQSClient sqsClient;

        public SQSEndpointTarget(AmazonSQSClient sqsClient, string stagingQueueUrl)
        {
            this.sqsClient = sqsClient;
            this.stagingQueueUrl = stagingQueueUrl;
        }

        public ValueTask DisposeAsync() => throw new System.NotImplementedException();

        public ValueTask<int> StageBatch(IReadOnlyList<TimeoutData> timeouts, int batchNumber)
        {
            // TODO: S3 handling and such
            // Really wonder how S3 would even work because we are getting the headers and the raw core payload
            // but S3 handling is actually done by the transport but with timeout storage timeouts are entirely in the
            // persister and SQS doesn't even get involved. So it seems we have to reimplement parts of the upload logic
            // because the byte load could be bigger than 256 kb?

            foreach (var timeoutsPerDestination in timeouts.GroupBy(t => t.Destination))
            {
                var sqsMessages = new List<SendMessageRequest>();

                foreach (var timeoutData in timeoutsPerDestination)
                {
                    var transportMessage = new TransportMessage(timeoutData.Headers, timeoutData.State);

                    var message = new SendMessageRequest
                    {
                        MessageDeduplicationId = timeoutData.Id,
                        MessageGroupId = timeoutData.Id,
                        MessageAttributes =
                        {
                            ["TimeoutDataTime"] = new MessageAttributeValue
                            {
                                StringValue = DateTimeHelper.ToWireFormattedString(timeoutData.Time),
                                DataType = "String"
                            }
                        },
                        MessageBody = JsonConvert.SerializeObject(transportMessage)
                    };

                }

            }
        }

        public ValueTask<int> CompleteBatch(int batchNumber) => throw new System.NotImplementedException();
    }
}