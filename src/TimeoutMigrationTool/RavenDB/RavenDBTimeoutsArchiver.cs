using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Particular.TimeoutMigrationTool.RavenDB.HttpCommands;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    class RavenDBTimeoutsArchiver
    {
        public async Task ArchiveTimeouts(string serverName, string databaseName, string[] timeoutIds, RavenDbVersion version, CancellationToken cancellationToken)
        {
            var url = $"{serverName}/databases/{databaseName}/bulk_docs";

            var command = GetPatchCommand(timeoutIds, version);

            using (var httpClient = new HttpClient())
            {
                var serializedCommands = JsonConvert.SerializeObject(command);

                var result = await httpClient.PostAsync(url, new StringContent(serializedCommands, Encoding.UTF8, "application/json"));
                result.EnsureSuccessStatusCode();
            }
        }

        private static object GetPatchCommand(string[] timeoutIds, RavenDbVersion version)
        {
            if (version == RavenDbVersion.Four)
            {
                return new
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
                                Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationOngoingPrefix}' + this.OwningTimeoutManager;",
                                Values = new { }
                            }
                        };
                    }).ToArray()
                };
            }

            return timeoutIds.Select(timeoutId =>
            {
                return new
                {
                    Key = timeoutId,
                    Method = "EVAL",
                    DebugMode = false,
                    Patch = new Patch()
                    {
                        Script = $"this.OwningTimeoutManager = '{RavenConstants.MigrationOngoingPrefix}' + this.OwningTimeoutManager;",
                        Values = new { }
                    }
                };
            }).ToArray();
        }
    }
}