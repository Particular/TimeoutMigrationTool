﻿using System.Collections.Generic;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool
{
    public interface ICreateTransportTimeouts
    {
        Task StageBatch(List<TimeoutData> timeouts);
        Task CompleteBatch(int number);
        Task<bool> AbleToMigrate(string endpointName);
    }
}