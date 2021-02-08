namespace Particular.TimeoutMigrationTool.ASQ
{
    using Microsoft.Azure.Cosmos.Table;
    using System;

    /// <summary>
    /// Represents a record in the native delays storage table which can be deferred message, saga timeouts, and delayed retries.
    /// </summary>
    public class DelayedMessageEntity : TableEntity, ICanCalculateMySize
    {
        public string Destination { get; set; }
        public byte[] Body { get; set; }
        public string MessageId { get; set; }
        public string Headers { get; set; }

        public static DelayedMessageEntity FromStagedTimeout(StagedDelayedMessageEntity timeout)
        {
            return new DelayedMessageEntity
            {
                PartitionKey = GetPartitionKey(timeout.Time),
                RowKey = $"{GetRawRowKeyPrefix(timeout.Time)}_{timeout.MessageId}",
                Destination = timeout.Destination,
                Body = timeout.Body,
                MessageId = timeout.MessageId,
                Headers = timeout.Headers
            };
        }

        const string PartitionKeyScope = "yyyyMMddHH";
        const string RowKeyScope = "yyyyMMddHHmmss";

        public long CalculateSize()
        {
            // As documented in https://www.wintellect.com/wp-content/uploads/2017/05/AzureStorageTables-1.pdf
            unsafe
            {
                return (2 * PartitionKey.Length) +
                       (2 * RowKey.Length) +
                       sizeof(DateTimeOffset) + // Timestamp
                       8 + (2 * nameof(Body).Length) + Body.LongLength +
                       8 + (2 * nameof(Headers).Length) + (sizeof(char) * Headers.Length) +
                       8 + (2 * nameof(MessageId).Length) + (sizeof(char) * MessageId.Length) +
                       8 + (2 * nameof(Destination).Length) + (sizeof(char) * Destination.Length);
            }
        }

        public static string GetPartitionKey(DateTimeOffset dto)
        {
            return dto.ToString(PartitionKeyScope);
        }

        public static string GetRawRowKeyPrefix(DateTimeOffset dto)
        {
            return dto.ToString(RowKeyScope);
        }
    }
}
