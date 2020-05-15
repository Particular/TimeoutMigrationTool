using System;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    class RavenDBTimeoutsArchiver
    {
        public async Task ArchiveTimeout(string serverName, string databaseName, string timeoutId, CancellationToken cancellationToken)
        {
            string url = $"{serverName}/databases/{databaseName}/bulk_docs";
            var command = new
            {
                Commands = new[]
                {
                    new {
                        Id = timeoutId, 
                        Type="PATCH",
                        ChangeVector = (string)null,
                        Patch = new
                        {
                            Script = "this.OwningTimeoutManager = 'Archived_' + this.OwningTimeoutManager;",
                            Values = new {}
                        }
                    },
                    
                }
            };
            
            using (var httpClient = new HttpClient())
            {
                var serializedCommands = JsonConvert.SerializeObject(command);

                var result = await httpClient.PostAsync(url, new StringContent(serializedCommands, Encoding.UTF8, "application/json")).ConfigureAwait(false);
                result.EnsureSuccessStatusCode();
            }
        }
    }
}