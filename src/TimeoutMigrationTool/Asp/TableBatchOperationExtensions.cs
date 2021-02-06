namespace Particular.TimeoutMigrationTool.Asp
{
    using Microsoft.Azure.Cosmos.Table;

    static class TableBatchOperationExtensions
    {
        public static TableBatchOperation Clone(this TableBatchOperation batchToBeCloned)
        {
            var clone = new TableBatchOperation();
            foreach (var operation in batchToBeCloned)
            {
                clone.Add(operation);
            }
            return clone;
        }
    }
}