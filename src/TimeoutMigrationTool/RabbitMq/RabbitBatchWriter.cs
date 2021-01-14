﻿namespace Particular.TimeoutMigrationTool.RabbitMq
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;
    using RabbitMQ.Client;

    class RabbitBatchWriter
    {
        public RabbitBatchWriter(ILogger logger, string rabbitConnectionString)
        {
            this.logger = logger;
            this.rabbitConnectionString = rabbitConnectionString;
        }

        public Task<int> WriteTimeoutsToStagingQueue(List<TimeoutData> timeouts, string stageExchangeName)
        {
            int messageCount;
            using (var connection = GetConnection(this.rabbitConnectionString))
            {
                using (var model = connection.CreateModel())
                {
                    PurgueQueueIfNotEmpty(model);
                    model.ConfirmSelect();
                    foreach (var timeout in timeouts)
                    {
                        PublishTimeout(model, timeout, stageExchangeName);
                    }

                    model.WaitForConfirmsOrDie(TimeSpan.FromSeconds(30));
                    messageCount = Convert.ToInt32(QueueCreator.GetStatingQueueMessageLength(model));
                }
            }

            return Task.FromResult(messageCount);
        }

        void PurgueQueueIfNotEmpty(IModel model)
        {
            var statingQueueMessageLength = QueueCreator.GetStatingQueueMessageLength(model);
            if (statingQueueMessageLength > 0)
            {
                logger.LogWarning("Purging staging queue - staging queue contains messages.");
                QueueCreator.PurgeStagingQueue(model);
            }
        }

        void PublishTimeout(IModel model, TimeoutData timeout, string stageExchangeName)
        {
            int level;

            var delay = timeout.Time - DateTime.UtcNow;
            var delayInSeconds = Convert.ToInt32(Math.Ceiling(delay.TotalSeconds));
            if (delayInSeconds < 0)
            {//when the timeout is due we zero the delay
                delay = TimeSpan.Zero;
                delayInSeconds = 0;
            }

            var routingKey = CalculateRoutingKey(delayInSeconds, timeout.Destination, out level);

            var properties = model.CreateBasicProperties();
            Fill(properties, timeout, delay);

            properties.Headers["TimeoutMigrationTool.DelayExchange"] = LevelName(level);
            properties.Headers["TimeoutMigrationTool.RoutingKey"] = routingKey;

            model.BasicPublish(stageExchangeName, routingKey, true, properties, timeout.State);
        }

        IConnection GetConnection(string rabbitMqBroker)
        {
            this.factory = new ConnectionFactory();
            factory.Uri = new Uri(rabbitMqBroker);
            return factory.CreateConnection();
        }

        void Fill(IBasicProperties properties, TimeoutData timeout, TimeSpan delay)
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

        static string CalculateRoutingKey(int delayInSeconds, string address, out int startingDelayLevel)
        {
            if (delayInSeconds < 0)
            {
                delayInSeconds = 0;
            }

            var bitArray = new BitArray(new[] { delayInSeconds });
            var sb = new StringBuilder();
            startingDelayLevel = 0;

            for (var level = RabbitMqTimeoutCreator.MaxLevel; level >= 0; level--)
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

        ConnectionFactory factory;

        readonly ILogger logger;
        readonly string rabbitConnectionString;
        static string LevelName(int level) => $"nsb.delay-level-{level:D2}";
    }
}