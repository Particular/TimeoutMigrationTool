namespace Particular.TimeoutMigrationTool.ASQ
{
    using Microsoft.Azure.Cosmos.Table;
    using System;

    /// <summary>
    /// Represents a record in the native delays storage table which can be deferred message, saga timeouts, and delayed retries.
    /// </summary>
    public class StagedDelayedMessageEntity : TableEntity, ICanCalculateMySize
    {
        public string Destination { get; set; }
        public byte[] Body { get; set; }
        public string MessageId { get; set; }
        public string Headers { get; set; }
        public DateTimeOffset Time { get; set; }

        static string Serialize<T>(T value)
        {
            return SimpleJson.SimpleJson.SerializeObject(value);
        }

        public static StagedDelayedMessageEntity FromTimeoutData(TimeoutData timeout, int batchNumber)
        {
            return new StagedDelayedMessageEntity
            {
                PartitionKey = batchNumber.ToString(),
                RowKey = $"{GetRawRowKeyPrefix(timeout.Time)}_{SanitizeId(timeout.Id)}",
                Destination = timeout.Destination,
                Body = timeout.State,
                MessageId = SanitizeId(timeout.Id),
                Headers = Serialize(timeout.Headers),
                Time = timeout.Time
            };
        }

        static string SanitizeId(string timeoutId)
        {
            return timeoutId.Replace('/', '-');
        }

        const string RowKeyScope = "yyyyMMddHHmmss";

        public static string GetRawRowKeyPrefix(DateTimeOffset dto)
        {
            return dto.ToString(RowKeyScope);
        }

        public long CalculateSize()
        {
            // As documented in https://www.wintellect.com/wp-content/uploads/2017/05/AzureStorageTables-1.pdf
            unsafe
            {
                return (2 * PartitionKey.Length) +
                       (2 * RowKey.Length) +
                       sizeof(DateTimeOffset) + // Timestamp
                       4 + (2 * nameof(Body).Length) + Body.LongLength +
                       8 + (2 * nameof(Headers).Length) + (sizeof(char) * Headers.Length) +
                       8 + (2 * nameof(MessageId).Length) + (sizeof(char) * MessageId.Length) +
                       8 + (2 * nameof(Destination).Length) + (sizeof(char) * Destination.Length) +
                       8 + (2 * nameof(Time).Length) + sizeof(DateTimeOffset);
            }
        }
    }
}
