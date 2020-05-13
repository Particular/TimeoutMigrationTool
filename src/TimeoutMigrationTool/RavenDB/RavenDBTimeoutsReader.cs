using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    class RavenDBTimeoutsReader
    {
        public Task<string[]> ListEndpoints(string connectionString, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public Task<List<TimeoutData>> ReadTimeoutsFrom(string connectionString, CancellationToken cancellationToken)
        {
            //this needs to be paged so Task<List<TimeoutData>> is not enough
            throw new NotImplementedException();
        }
    }
}
