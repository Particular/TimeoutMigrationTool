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
    class RabbitMqWriter
    {
        public Task<bool> WriteTimeoutsTo(string rabbitMqBroker, List<TimeoutData> timeouts, CancellationToken cancellationToken)
        {
            using (var connection = GetConnection(rabbitMqBroker))
            {
                using (var model = connection.CreateModel())
                {
                    foreach (var timeout in timeouts)
                    {
                        PublishTimeout(model, timeout);
                    }
                }
            }
        }

        private void PublishTimeout(IModel model, TimeoutData timeout)
        {
            int level;

            //TODO: guard for negative timespan
            var delay = (timeout.Time - DateTime.UtcNow); 
            var delayInSeconds = Convert.ToInt32(Math.Ceiling(delay.TotalSeconds));
            var routingKey = CalculateRoutingKey(delayInSeconds, timeout.Destination, out level);
            
            var properties = model.CreateBasicProperties();
            Fill(properties, timeout, delay);

            //TODO: Blow up when the time > 8.5 years[max value].

            model.BasicPublish(LevelName(level), routingKey, true, properties, timeout.State);
        }

        private IConnection GetConnection(string rabbitMqBroker)
        {
            var factory = new ConnectionFactory();
            factory.Uri = new Uri(rabbitMqBroker);
            return factory.CreateConnection();
        }

        public static void Fill(this IBasicProperties properties, TimeoutData timeout, TimeSpan delay)
        {
            properties.MessageId = timeout.Id; //TODO: pull from the header first 

            properties.Persistent = true;

            var messageHeaders = timeout.Headers ?? new Dictionary<string, string>();            

            properties.Headers = messageHeaders.ToDictionary(p => p.Key, p => (object)p.Value);
                        
            properties.Headers["NServiceBus.Transport.RabbitMQ.DelayInSeconds"] = Convert.ToInt32(Math.Ceiling(delay.TotalSeconds));

            properties.Expiration = delay.TotalMilliseconds.ToString(CultureInfo.InvariantCulture);
            
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
        
        public static string LevelName(int level) => $"nsb.delay-level-{level:D2}";

        private static int MaxLevel = 28;
    }
}