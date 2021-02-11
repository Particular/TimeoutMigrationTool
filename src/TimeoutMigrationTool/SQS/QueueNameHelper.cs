namespace Particular.TimeoutMigrationTool.SQS
{
    using System;
    using System.Text;

    public static class QueueNameHelper
    {
        public static string GetQueueName(string destination, string queueNamePrefix)
        {
            if (string.IsNullOrWhiteSpace(destination))
            {
                throw new ArgumentNullException(nameof(destination));
            }

            // we need to process again because of the way we handle fifo queues
            var queueName = !string.IsNullOrEmpty(queueNamePrefix) &&
                            destination.StartsWith(queueNamePrefix, StringComparison.Ordinal) ?
                destination :
                $"{queueNamePrefix}{destination}";

            if (queueName.Length > 80)
            {
                throw new Exception($"Address {destination} with configured prefix {queueNamePrefix} is longer than 80 characters and therefore cannot be used to create an SQS queue. Use a shorter queue name.");
            }

            return GetSanitizedQueueName(queueName);
        }

        static string GetSanitizedQueueName(string queueName)
        {
            var queueNameBuilder = new StringBuilder(queueName);
            var skipCharacters = queueName.EndsWith(".fifo") ? 5 : 0;
            // SQS queue names can only have alphanumeric characters, hyphens and underscores.
            // Any other characters will be replaced with a hyphen.
            for (var i = 0; i < queueNameBuilder.Length - skipCharacters; ++i)
            {
                var c = queueNameBuilder[i];
                if (!char.IsLetterOrDigit(c)
                    && c != '-'
                    && c != '_')
                {
                    queueNameBuilder[i] = '-';
                }
            }

            return queueNameBuilder.ToString();
        }
    }
}