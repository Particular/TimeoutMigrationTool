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
            if (args.Length < 3) throw new InvalidOperationException("At least 3 arguments are needed in order to run: Servername and DatabaseName. If you want to skip db-creation, add a third argument set to true");
            var serverName = args[0];
            var databaseName = args[1];
            var ravenVersion = args[2] == "4" ? RavenDbVersion.Four : RavenDbVersion.ThreeDotFive;
            var nrOfTimeoutsToInsert = (args.Length == 3 || string.IsNullOrEmpty(args[3])) ? 100050 : Convert.ToInt32(args[3]);

            var createDbUrl = ravenVersion == RavenDbVersion.Four ? $"{serverName}/admin/databases?name={databaseName}" : $"{serverName}/admin/databases/{databaseName}";
            var httpContent = BuildHttpContentForDbCreation(ravenVersion, databaseName);

            var dbCreationResult = await httpClient.PutAsync(createDbUrl, httpContent);
            if (!dbCreationResult.IsSuccessStatusCode)
            {
                throw new Exception($"Something went wrong while creating the database. Error code {dbCreationResult.StatusCode}");
            }

            var timeoutsPrefix = "TimeoutDatas";
            var nrOfBatches = Math.Ceiling(nrOfTimeoutsToInsert / (decimal)RavenConstants.DefaultPagingSize);
            var timeoutIdCounter = await InitTimeouts(nrOfBatches, serverName, databaseName, nrOfTimeoutsToInsert, timeoutsPrefix, ravenVersion);

            Console.WriteLine($"{timeoutIdCounter} timeouts were created");
            Console.WriteLine("Creating the index....");
            await CreateIndex(serverName, databaseName);
            Console.WriteLine("Index created.");
        }

        static async Task<int> InitTimeouts(decimal nrOfBatches, string serverName, string databaseName, int nrOfTimeoutsToInsert, string timeoutsPrefix, RavenDbVersion ravenVersion)
        {
            var timeoutIdCounter = 0;


            for (var i = 1; i <= nrOfBatches; i++) // batch inserts per paging size
            {
                var commands = new List<object>();
                var bulkInsertUrl = $"{serverName}/databases/{databaseName}/bulk_docs";

                for (var j = 0; j < RavenConstants.DefaultPagingSize && timeoutIdCounter < nrOfTimeoutsToInsert; j++)
                {
                    timeoutIdCounter++;
                    var timeout = CreateTimeoutData(timeoutsPrefix, timeoutIdCounter);

                    var insertCommand = ravenVersion == RavenDbVersion.Four ? CreateRaven4TimeoutInsertCommand(timeout) : CreateRaven3TimeoutInsertCommand(timeout);
                    commands.Add(insertCommand);
                }

                object request;
                if (ravenVersion == RavenDbVersion.Four)
                {
                    request = new
                    {
                        Commands = commands.ToArray()
                    };
                }
                else
                {
                    request = commands.ToArray();
                }

                var serializeObject = JsonConvert.SerializeObject(request);
                var result = await httpClient.PostAsync(bulkInsertUrl, new StringContent(serializeObject, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }

            return timeoutIdCounter;
        }

        static object CreateRaven3TimeoutInsertCommand(TimeoutData timeout)
        {
            // Create insert command for timeout
            var insertCommand = new Raven3InsertCommand
            {
                Key = timeout.Id,
                Method = "PUT",
                Metadata = new object(),
                Document = timeout
            };
            return insertCommand;
        }

        static object CreateRaven4TimeoutInsertCommand(TimeoutData timeout)
        {
            // Create insert command for timeout
            var insertCommand = new Raven4InsertCommand
            {
                Id = timeout.Id,
                Type = "PUT",
                ChangeVector = null,
                Document = timeout
            };
            return insertCommand;
        }

        static TimeoutData CreateTimeoutData(string timeoutsPrefix, int timeoutIdCounter)
        {
            var daysToTrigger = random.Next(2, 60); // randomize the Time property
            // Create the timeout
            var timeoutData = new TimeoutData
            {
                Id = $"{timeoutsPrefix}/{timeoutIdCounter}",
                Destination = "DestinationEndpoint",
                SagaId = Guid.NewGuid(),
                OwningTimeoutManager = "EndpointA",
                Time = DateTime.UtcNow.AddDays(daysToTrigger),
                Headers = new Dictionary<string, string>(),
                State = Encoding.ASCII.GetBytes("This is my state")
            };
            return timeoutData;
        }

        static StringContent BuildHttpContentForDbCreation(RavenDbVersion ravenVersion, string databaseName)
        {
            StringContent stringContent;
            if (ravenVersion == RavenDbVersion.Four)
            {
                // Create the db
                var dbRaven4 = new DatabaseRecordRaven4
                {
                    Disabled = false,
                    DatabaseName = databaseName
                };
                stringContent = new StringContent(JsonConvert.SerializeObject(dbRaven4));
                return stringContent;
            }

            var dbRaven3 = new DatabaseRecordRaven3(databaseName);
            stringContent = new StringContent(JsonConvert.SerializeObject(dbRaven3));
            return stringContent;
        }

        static async Task CreateIndex(string serverName, string databaseName)
        {
            var map = "from doc in docs select new {  doc.Time, doc.SagaId, doc.OwningTimeoutManager }";
            var index = new
            {
                Analyzers = (object)null,
                Fields = new List<object>(),
                Indexes = (object)null,
                InternalFieldsMapping = (object)null,
                IsTestIndex = false,
                IsSideBySideIndex = false,
                IsCompiled = false,
                IsMapReduce = false,
                LockMode = "Unlock",
                Map = map,
                Maps = new List<string> {map},
                Name = RavenConstants.TimeoutIndexName,
                Reduce = (object)null,
                SortOptions = (object)null,
                SpatialIndexes = (object)null,
                Stores = (object)null,
                SuggestionsOptions = new List<object>(),
                TermVectors = (object)null,
                Type = "Map",
                MaxIndexOutputsPerDocument = (object)null
            };

            var createIndexUrl = $"{serverName}/databases/{databaseName}/indexes/{RavenConstants.TimeoutIndexName}?definition=yes";
            var content = JsonConvert.SerializeObject(index);
            var result = await httpClient
                .PutAsync(createIndexUrl, new StringContent(content, Encoding.UTF8, "application/json"));
            result.EnsureSuccessStatusCode();
        }

        static readonly HttpClient httpClient = new HttpClient();
        static Random random = new Random();
    }
}