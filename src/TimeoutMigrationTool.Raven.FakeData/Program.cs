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

            var nrOfBatches = Math.Ceiling(nrOfTimeoutsToInsert / (decimal)RavenConstants.DefaultPagingSize);
            var timeoutIdCounter = 0;

            for (var i = 1; i <= nrOfBatches; i++) // batch inserts per paging size
            {
                var commands = new List<PutCommand>();
                var bulkInsertUrl = $"{serverName}/databases/{databaseName}/bulk_docs";

                for (var j = 0; j < RavenConstants.DefaultPagingSize; j++)
                {
                    timeoutIdCounter++;
                    var endpoint = j < (RavenConstants.DefaultPagingSize / 3) ? "EndpointA" : j < (RavenConstants.DefaultPagingSize / 3 / 3) * 2 ? "EndpointB" : "EndpointC";

                    var insertCommand = CreateTimeoutInsertCommand(timeoutsPrefix, timeoutIdCounter, endpoint);
                    commands.Add(insertCommand);
                }

                var request = new
                {
                    Commands = commands.ToArray()
                };

                var serializeObject = JsonConvert.SerializeObject(request);
                var result = await httpClient.PostAsync(bulkInsertUrl,
                    new StringContent(serializeObject, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        static PutCommand CreateTimeoutInsertCommand(string timeoutsPrefix, int timeoutIdCounter, string endpoint)
        {
            var daysToTrigger = random.Next(0, 60); // randomize the Time property

            // Create the timeout
            var timeoutData = new TimeoutData
            {
                Id = $"{timeoutsPrefix}/{timeoutIdCounter}",
                Destination = "DestinationEndpoint",
                SagaId = Guid.NewGuid(),
                OwningTimeoutManager = endpoint,
                Time = DateTime.Now.AddDays(daysToTrigger),
                Headers = new Dictionary<string, string>(),
                State = Encoding.ASCII.GetBytes("This is my state")
            };

            // Create insert command for timeout
            var insertCommand = new PutCommand()
            {
                Id = $"{timeoutsPrefix}/{timeoutIdCounter}",
                Type = "PUT",
                ChangeVector = null,
                Document = timeoutData
            };
            return insertCommand;
        }

        static readonly HttpClient httpClient = new HttpClient();
        static Random random = new Random();
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