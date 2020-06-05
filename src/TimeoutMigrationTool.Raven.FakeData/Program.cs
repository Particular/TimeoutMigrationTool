namespace TimeoutMigrationTool.Raven.FakeData
{
    using System;
    using System.Collections.Generic;
    using System.Net.Http;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;
    using Particular.TimeoutMigrationTool;
    using Particular.TimeoutMigrationTool.RavenDB;

    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 2) throw new InvalidOperationException("At least 3 arguments are needed in order to run: Servername and DatabaseName. If you want to skip db-creation, add a third argument set to true");
            var serverName = args[0];
            var databaseName = args[1];
            var skipDbCreation = args.Length > 2 && Convert.ToBoolean(args[2]);
            var nrOfTimeoutsToInsert = (args.Length < 4 || string.IsNullOrEmpty(args[3])) ? 1000000 : Convert.ToInt32(args[3]);

            if (!skipDbCreation)
            {
                var createDbUrl = $"{serverName}/admin/databases?name={databaseName}";
                // Create the db
                var db = new DatabaseRecord
                {
                    Disabled = false,
                    DatabaseName = databaseName
                };

                var stringContent = new StringContent(JsonConvert.SerializeObject(db));
                var dbCreationResult = await httpClient.PutAsync(createDbUrl, stringContent);
                if (!dbCreationResult.IsSuccessStatusCode)
                    throw new Exception(
                        $"Something went wrong while creating the database. Error code {dbCreationResult.StatusCode}");
            }

            var timeoutsPrefix = "TimeoutDatas";
            var ratio = Math.Floor((decimal)(nrOfTimeoutsToInsert / 3));

            var nrOfBatches = Math.Ceiling(nrOfTimeoutsToInsert / (decimal)RavenConstants.DefaultPagingSize);
            var timeoutIdCounter = 1;
            for (var i = 1; i <= nrOfBatches; i++)
            {
                var commands = new List<PutCommand>();
                var startIndex = i * RavenConstants.DefaultPagingSize;
                var bulkInsertUrl = $"{serverName}/databases/{databaseName}/bulk-docs";

                for (var j = 0; j < startIndex; j++)
                {
                    // Insert the timeout data
                    var timeoutData = new TimeoutData
                    {
                        Id = $"{timeoutsPrefix}/{i + 1}",
                        Destination = "DestinationEndpoint",
                        SagaId = Guid.NewGuid(),
                        OwningTimeoutManager = i < ratio ? "EndpointA" : i < ratio*2 ? "EndpointB" : "EndpointC",
                        Time = i < 125 ? DateTime.Now.AddDays(7) : DateTime.Now.AddDays(14),
                        Headers = new Dictionary<string, string>(),
                        State = Encoding.ASCII.GetBytes("This is my state")
                    };

                    var insertCommand = new PutCommand()
                    {
                        Id = $"{timeoutsPrefix}/{timeoutIdCounter}",
                        Type = "PUT",
                        ChangeVector = null,
                        Document = timeoutData
                    };
                    commands.Add(insertCommand);

                    timeoutIdCounter++;
                }

                var serializeObject = JsonConvert.SerializeObject(commands);
                var result = await httpClient.PostAsync(bulkInsertUrl,
                    new StringContent(serializeObject, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        static readonly HttpClient httpClient = new HttpClient();
    }

    public class DatabaseRecord
    {
        public string DatabaseName { get; set; }
        public bool Disabled { get; set; }
    }

    class PutCommand
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public object ChangeVector { get; set; }
        public TimeoutData Document { get; set; }
    }
}