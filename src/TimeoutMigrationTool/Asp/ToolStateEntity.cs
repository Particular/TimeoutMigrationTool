namespace Particular.TimeoutMigrationTool.Asp
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.Cosmos.Table;
    using Newtonsoft.Json;

    public class ToolStateEntity : TableEntity
    {
        public const string FixedPartitionKey = "2E81644B-3DD2-42FD-8D81-23B757AEE714";

        [IgnoreProperty]
        public IList<(int batchNumber, int batchSize)> BatchNumberAndSizes { get; set; }

        [IgnoreProperty]
        public BatchState CurrentBatchState { get; set; }
        public int CurrentBatchNumber { get; set; }

        [IgnoreProperty]
        public IDictionary<string, string> RunParameters { get; set; }

        [IgnoreProperty]
        public MigrationStatus Status { get; set; }

        [IgnoreProperty]
        public Guid MigrationRunId
        {
            get => Guid.Parse(RowKey);
            set => RowKey = value.ToString();
        }

        public string UniqueHiddenEndpointName { get; set; }

        public string EndpointName { get; set; }

        public DateTimeOffset? CompletedAt { get; set; }

        [IgnoreProperty]
        public int NumberOfBatches => BatchNumberAndSizes?.Count ?? 0;

        public ToolStateEntity()
        {
            PartitionKey = FixedPartitionKey;
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            var entityAsDictionary = base.WriteEntity(operationContext);

            entityAsDictionary.Add(nameof(RunParameters), EntityProperty.GeneratePropertyForString(JsonConvert.SerializeObject(RunParameters)));
            entityAsDictionary.Add(nameof(BatchNumberAndSizes), EntityProperty.GeneratePropertyForString(JsonConvert.SerializeObject(BatchNumberAndSizes)));
            entityAsDictionary.Add(nameof(Status), EntityProperty.GeneratePropertyForInt((int)Status));
            entityAsDictionary.Add(nameof(CurrentBatchState), EntityProperty.GeneratePropertyForInt((int)CurrentBatchState));

            return entityAsDictionary;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);

            RunParameters = JsonConvert.DeserializeObject<IDictionary<string, string>>(properties[nameof(RunParameters)].StringValue);
            BatchNumberAndSizes = JsonConvert.DeserializeObject<IList<(int batchNumber, int batchSize)>>(properties[nameof(BatchNumberAndSizes)].StringValue);
            Status = (MigrationStatus)properties[nameof(Status)].Int32Value;
            CurrentBatchState = (BatchState)properties[nameof(CurrentBatchState)].Int32Value;
        }
    }
}