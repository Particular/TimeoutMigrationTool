using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    class RavenDBTimeoutsArchiver
    {
        public async Task ArchiveTimeouts(string serverName, string databaseName, string[] timeoutIds, CancellationToken cancellationToken)
        {
            string url = $"{serverName}/databases/{databaseName}/bulk_docs";

            var command = new
            {
                Commands = timeoutIds.Select(timeoutId =>
                {
                    return new PatchCommand
                    {
                        Id = timeoutId,
                        Type = "PATCH",
                        ChangeVector = null,
                        Patch = new Patch()
                        {
                            Script = "this.OwningTimeoutManager = 'Archived_' + this.OwningTimeoutManager;",
                            Values = new { }
                        }
                    };
                }).ToArray()
            };
            
            using (var httpClient = new HttpClient())
            {
                var serializedCommands = JsonConvert.SerializeObject(command);

                var result = await httpClient.PostAsync(url, new StringContent(serializedCommands, Encoding.UTF8, "application/json")).ConfigureAwait(false);
                result.EnsureSuccessStatusCode();
            }
        }
    }

    class PatchCommand
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public string ChangeVector { get; set; }
        public Patch Patch { get; set; }
    }

    class Patch
    {
        public string Script { get; set; }
        public object Values { get; set; }
    }
}