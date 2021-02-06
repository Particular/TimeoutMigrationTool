namespace Particular.TimeoutMigrationTool.Asp
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using Microsoft.Azure.Cosmos.Table;

    public interface ICanCalculateMySize
    {
        long CalculateSize();
    }

    public class TimeoutDataEntity : TableEntity, ICanCalculateMySize, ICloneable
    {
        public TimeoutDataEntity() { }

        public TimeoutDataEntity(string partitionKey, string rowKey)
            : base(partitionKey, rowKey)
        {
        }

        public string Destination { get; set; }

        public Guid SagaId { get; set; }

        public string StateAddress { get; set; }

        public DateTime Time { get; set; }

        public string OwningTimeoutManager { get; set; }

        public string Headers { get; set; }

        public long CalculateSize()
        {
            unsafe
            {
                return (2 * PartitionKey.Length) +
                    (2 * RowKey.Length) +
                    sizeof(DateTimeOffset) + // Timestamp
                    8 + (2 * nameof(Destination).Length) + Destination.Length +
                    8 + (2 * nameof(SagaId).Length) + SagaId.ToString().Length +
                    8 + (2 * nameof(StateAddress).Length) + StateAddress.Length +
                    8 + (2 * nameof(Time).Length) + sizeof(DateTime) + // Time property
                    8 + (2 * nameof(OwningTimeoutManager).Length) + OwningTimeoutManager.Length +
                    8 + (2 * nameof(Headers).Length) + Headers.Length;
            }
        }

        // since this is a simple entity shallow copying is good enough
        public object Clone() => MemberwiseClone();
    }

    public class MigratedTimeoutDataEntity : TableEntity, ICanCalculateMySize
    {
        public MigratedTimeoutDataEntity() { }

        public MigratedTimeoutDataEntity(TimeoutDataEntity timeoutDataEntity, int batchNumber)
            : base(batchNumber.ToString(CultureInfo.InvariantCulture), timeoutDataEntity.RowKey)
        {
            Destination = timeoutDataEntity.Destination;
            SagaId = timeoutDataEntity.SagaId;
            StateAddress = timeoutDataEntity.StateAddress;
            Time = timeoutDataEntity.Time;
            OwningTimeoutManager = timeoutDataEntity.OwningTimeoutManager;
            Headers = timeoutDataEntity.Headers;
            ETag = "*";
            BatchState = BatchState.Pending;
        }

        public string Destination { get; set; }

        public Guid SagaId { get; set; }

        public string StateAddress { get; set; }

        public DateTime Time { get; set; }

        public string OwningTimeoutManager { get; set; }

        public string Headers { get; set; }

        [IgnoreProperty]
        public BatchState BatchState { get; set; }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entity = base.WriteEntity(operationContext);

            entity.Add(nameof(BatchState), EntityProperty.GeneratePropertyForInt((int)BatchState));

            return entity;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            BatchState = (BatchState)properties[nameof(BatchState)].Int32Value;
        }

        public long CalculateSize()
        {
            unsafe
            {
                return (2 * PartitionKey.Length) +
                    (2 * RowKey.Length) +
                    sizeof(DateTimeOffset) + // Timestamp
                    8 + (2 * nameof(Destination).Length) + Destination.Length +
                    8 + (2 * nameof(SagaId).Length) + SagaId.ToString().Length +
                    8 + (2 * nameof(StateAddress).Length) + StateAddress.Length +
                    8 + (2 * nameof(Time).Length) + sizeof(DateTime) + // Time property
                    8 + (2 * nameof(OwningTimeoutManager).Length) + OwningTimeoutManager.Length +
                    8 + (2 * nameof(Headers).Length) + Headers.Length +
                    8 + (2 * nameof(BatchState).Length) + sizeof(int);
            }
        }
    }

    public class PartialMigratedTimeoutDataEntityWithBatchState : TableEntity
    {
        [IgnoreProperty]
        public BatchState BatchState { get; set; }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entity = base.WriteEntity(operationContext);

            entity.Add(nameof(BatchState), EntityProperty.GeneratePropertyForInt((int)BatchState));

            return entity;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            BatchState = (BatchState)properties[nameof(BatchState)].Int32Value;
        }
    }

    public class PartialTimeoutDataEntityWithOwningTimeoutManager : TableEntity
    {
        public string OwningTimeoutManager { get; set; }
    }
}