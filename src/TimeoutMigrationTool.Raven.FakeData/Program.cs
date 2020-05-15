using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Particular.TimeoutMigrationTool;

namespace TimeoutMigrationTool.Raven.FakeData
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length < 2) throw new InvalidOperationException("At least 3 arguments are needed in order to run: Servername and DatabaseName. If you want to skip db-creation, add a third argument set to true");
            var serverName = args[0];
            var databaseName = args[1];
            bool skipDbCreation = args.Length > 2 && Convert.ToBoolean(args[2]);
            int nrOfTimeoutsToInsert = (args.Length < 4 || string.IsNullOrEmpty(args[3])) ? 250 : Convert.ToInt32(args[3]);

            using (var httpClient = new HttpClient())
            {
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
                for (var i = 0; i < nrOfTimeoutsToInsert; i++)
                {
                    var insertTimeoutUrl = $"{serverName}/databases/{databaseName}/docs?id={timeoutsPrefix}/{i}";

                    // Insert the timeout data
                    var timeoutData = new TimeoutData
                    {
                        Destination = i <100 ? "A" : i== 100 ? "B" : "C",
                        SagaId = Guid.NewGuid(),
                        OwningTimeoutManager = "FakeOwningTimeoutManager",
                        Time = i < 125 ? DateTime.Now.AddDays(7) : DateTime.Now.AddDays(14),
                        Headers = new Dictionary<string, string>(),
                        State = Encoding.ASCII.GetBytes("This is my state")
                    };

                    var serializeObject = JsonConvert.SerializeObject(timeoutData);
                    var httpContent = new StringContent(serializeObject);

                    await httpClient.PutAsync(insertTimeoutUrl, httpContent);
                }
            }
        }
    }

    public class DatabaseRecord
    {
        public string DatabaseName { get; set; }
        public bool Disabled { get; set; }
    }
}