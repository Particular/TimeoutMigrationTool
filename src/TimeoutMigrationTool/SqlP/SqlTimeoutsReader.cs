using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.SqlP
{
    class SqlTimeoutsReader
    {
        public Task<List<TimeoutData>> ReadTimeoutsFrom(string connectionString, SqlDialect dialect, CancellationToken cancellationToken)
        {
            using (var connection = dialect.Connect(connectionString))
            {

            }

            throw new NotImplementedException();
        }
    }
}