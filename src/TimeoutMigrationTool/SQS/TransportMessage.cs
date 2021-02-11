namespace Particular.TimeoutMigrationTool.SQS
{
    using System;
    using System.Collections.Generic;

    class TransportMessage
    {
        // Empty constructor required for deserialization.
        public TransportMessage()
        {
        }

        public TransportMessage(Dictionary<string, string> headers, byte[] body)
        {
            Headers = headers;

            Body = body != null ? Convert.ToBase64String(body) : "empty message";
        }

        public Dictionary<string, string> Headers { get; }

        public string Body { get; set; }

        public string S3BodyKey { get; set; }
    }
}