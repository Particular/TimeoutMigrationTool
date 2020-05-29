using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    class RabbitBatchWriter
    {
        public RabbitBatchWriter(string rabbitConnectionString)
        {
            this.rabbitConnectionString = rabbitConnectionString;
        }

        public Task<bool> WriteTimeoutsToStagingQueue(List<TimeoutData> timeouts, string stageExchangeName)
        {
            //todo: check the count and purge the queue if not empty + log the situation
            using (var connection = GetConnection(this.rabbitConnectionString))
            {
                using (var model = connection.CreateModel())
                {
                    PurgueQueueIfNotEmpty(model);
                    foreach (var timeout in timeouts)
                    {
                        PublishTimeout(model, timeout, stageExchangeName);
                    }
                }
            }
            return Task<bool>.FromResult(true);
        }

        private void PurgueQueueIfNotEmpty(IModel model)
        {
            var statingQueueMessageLength = QueueCreator.GetStatingQueueMessageLength(model);
            if (statingQueueMessageLength > 0)
            {
                Console.Error.WriteLine("Purging staging queue - staging queue contains messages.");
                QueueCreator.PurgeStatingQueue(model);
            }
        }

        private void PublishTimeout(IModel model, TimeoutData timeout, string stageExchangeName)
        {
            int level;

            //TODO: guard for negative timespan
            var delay = (timeout.Time - DateTime.UtcNow);
            var delayInSeconds = Convert.ToInt32(Math.Ceiling(delay.TotalSeconds));
            var routingKey = CalculateRoutingKey(delayInSeconds, timeout.Destination, out level);

            var properties = model.CreateBasicProperties();
            Fill(properties, timeout, delay);

            properties.Headers["TimeoutMigrationTool.DelayExchange"] = LevelName(level);
            properties.Headers["TimeoutMigrationTool.RoutingKey"] = routingKey;

            //TODO: Blow up when the time > 8.5 years[max value].

            model.BasicPublish(stageExchangeName, routingKey, true, properties, timeout.State);
        }

        private IConnection GetConnection(string rabbitMqBroker)
        {
            this.factory = new ConnectionFactory();
            factory.Uri = new Uri(rabbitMqBroker);
            return factory.CreateConnection();
        }

        private void Fill(IBasicProperties properties, TimeoutData timeout, TimeSpan delay)
        {
            var messageHeaders = timeout.Headers ?? new Dictionary<string, string>();

            if (messageHeaders.TryGetValue("NServiceBus.MessageId", out var originalMessageId) && !string.IsNullOrEmpty(originalMessageId))
            {
                properties.MessageId = originalMessageId;
            }
            else
            {
                properties.MessageId = timeout.Id;
            }

            properties.Persistent = true;

            properties.Headers = messageHeaders.ToDictionary(p => p.Key, p => (object)p.Value);

            properties.Headers["NServiceBus.Transport.RabbitMQ.DelayInSeconds"] = Convert.ToInt32(Math.Ceiling(delay.TotalSeconds));

            properties.Expiration = Convert.ToInt32(delay.TotalMilliseconds).ToString(CultureInfo.InvariantCulture);

            if (messageHeaders.TryGetValue("NServiceBus.CorrelationId", out var correlationId) && correlationId != null)
            {
                properties.CorrelationId = correlationId;
            }

            if (messageHeaders.TryGetValue("NServiceBus.EnclosedMessageTypes", out var enclosedMessageTypes) && enclosedMessageTypes != null)
            {
                var index = enclosedMessageTypes.IndexOf(',');

                if (index > -1)
                {
                    properties.Type = enclosedMessageTypes.Substring(0, index);
                }
                else
                {
                    properties.Type = enclosedMessageTypes;
                }
            }

            if (messageHeaders.TryGetValue("NServiceBus.ContentType", out var contentType) && contentType != null)
            {
                properties.ContentType = contentType;
            }
            else
            {
                properties.ContentType = "application/octet-stream";
            }

            if (messageHeaders.TryGetValue("NServiceBus.ReplyToAddress", out var replyToAddress) && replyToAddress != null)
            {
                properties.ReplyTo = replyToAddress;
            }
        }


        private static string CalculateRoutingKey(int delayInSeconds, string address, out int startingDelayLevel)
        {
            if (delayInSeconds < 0)
            {
                delayInSeconds = 0;
            }

            var bitArray = new BitArray(new[] { delayInSeconds });
            var sb = new StringBuilder();
            startingDelayLevel = 0;

            for (var level = MaxLevel; level >= 0; level--)
            {
                if (startingDelayLevel == 0 && bitArray[level])
                {
                    startingDelayLevel = level;
                }

                sb.Append(bitArray[level] ? "1." : "0.");
            }

            sb.Append(address);

            return sb.ToString();
        }

        private static string LevelName(int level) => $"nsb.delay-level-{level:D2}";

        public static int MaxDelayInSeconds = (1 << MaxLevel) - 1;
        private static int MaxLevel = 28;

        private readonly string rabbitConnectionString;
        private ConnectionFactory factory;
    }
}