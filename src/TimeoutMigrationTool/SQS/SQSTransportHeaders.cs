namespace Particular.TimeoutMigrationTool.SQS
{
    static class SQSTransportHeaders
    {
        const string Prefix = "NServiceBus.AmazonSQS.";
        public const string TimeToBeReceived = Prefix + nameof(TimeToBeReceived);
        public const string DelaySeconds = Prefix + nameof(DelaySeconds);
        public const string S3BodyKey = "S3BodyKey";
        public const string MessageTypeFullName = "MessageTypeFullName";
    }
}