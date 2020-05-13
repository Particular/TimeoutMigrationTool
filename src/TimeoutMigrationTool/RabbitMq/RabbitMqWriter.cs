using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RabbitMq
{
    class RabbitMqWriter
    {
        public Task<bool> WriteTimeoutsTo(string rabbitMqBroker, List<TimeoutData> timeouts, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }
    }
}