namespace TimeoutMigrationTool.Asp.FakeData
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;
    using System.Text;
    using System.Threading.Tasks;
    using Azure;
    using Azure.Storage.Blobs;
    using Microsoft.Azure.Cosmos.Table;

    class Program
    {
        static readonly Random random = new Random();

        static async Task Main()
        {
            var connectionString = $@"UseDevelopmentStorage=true";
            const string TimeoutDataTableName = "TimeoutData";
            const string PartitionKeyScope = "yyyy-MM-dd";
            const string ContainerName = "timeoutsdata";

            var account = CloudStorageAccount.Parse(connectionString);
            var tableClient = account.CreateCloudTableClient();

            var endpointTimeoutTable = tableClient.GetTableReference(TimeoutDataTableName);
            await endpointTimeoutTable.CreateIfNotExistsAsync().ConfigureAwait(false);

            var blobServiceClient = new BlobServiceClient(connectionString);
            BlobContainerClient blobContainerClient = null;
            try
            {
                blobContainerClient = await blobServiceClient.CreateBlobContainerAsync(ContainerName).ConfigureAwait(false);
            }
            catch (RequestFailedException)
            {
                blobContainerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
            }

            var noOfTimeouts = 20000;

            var cutOffDate = DateTime.Now;

            var allTimeouts = new List<TimeoutDataEntity>(noOfTimeouts);

            var stateAddress = Guid.NewGuid().ToString();

            var blobClient = blobContainerClient.GetBlobClient(stateAddress);
            await blobClient.UploadAsync(new MemoryStream(Encoding.UTF8.GetBytes("{ Topic: bla }"))).ConfigureAwait(false);

            for (var i = 0; i < noOfTimeouts; i++)
            {
                var dateTime = cutOffDate.AddDays(random.Next(1, 20));

                allTimeouts.Add(new TimeoutDataEntity(dateTime.ToString(PartitionKeyScope), Guid.NewGuid().ToString())
                {
                    OwningTimeoutManager = "Asp.FakeTimeouts",
                    Destination = i % 10 == 0 ? "EndpointA" : "EndpointB",
                    SagaId = Guid.NewGuid(),
                    StateAddress = stateAddress,
                    Time = dateTime,
                    Headers = "{}",
                });
            }

            var tasks = new List<Task>(noOfTimeouts / 100);
            foreach (IGrouping<string, TimeoutDataEntity> byPartition in allTimeouts.GroupBy(x => x.PartitionKey))
            {
                static async Task ExecuteBatch(CloudTable table, TableBatchOperation batch)
                {
                    await table.ExecuteBatchAsync(batch).ConfigureAwait(false);
                    await Console.Error.WriteAsync(".").ConfigureAwait(false);
                }

                var batch = new TableBatchOperation();
                foreach (TimeoutDataEntity entity in byPartition)
                {
                    if (batch.Count == 100)
                    {
                        tasks.Add(ExecuteBatch(endpointTimeoutTable, batch.Clone()));
                        batch.Clear();
                    }

                    var insertOrReplace = TableOperation.InsertOrReplace(entity);
                    SetEchoContentTo(insertOrReplace, false);
                    batch.Add(insertOrReplace);
                }

                if (batch.Count > 0)
                {
                    tasks.Add(ExecuteBatch(endpointTimeoutTable, batch.Clone()));
                }
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        static Action<TableOperation, bool> CreateEchoContentSetter()
        {
            ParameterExpression instance = Expression.Parameter(typeof(TableOperation), "instance");
            ParameterExpression echoContentParameter = Expression.Parameter(typeof(bool), "param");

            var echoContentProperty =
                typeof(TableOperation).GetProperty("EchoContent", BindingFlags.Instance | BindingFlags.NonPublic);

            var body = Expression.Call(instance, echoContentProperty.SetMethod, echoContentParameter);
            var parameters = new[] { instance, echoContentParameter };

            return Expression.Lambda<Action<TableOperation, bool>>(body, parameters).Compile();
        }

        static readonly Action<TableOperation, bool> SetEchoContentTo = CreateEchoContentSetter();
    }

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

    public class TimeoutDataEntity : TableEntity
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
    }
}