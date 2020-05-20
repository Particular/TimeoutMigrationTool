using System;
using System.Collections.Generic;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public static class RavenStoreParameterExtensions
    {
        public static RavenStoreParameters ToRavenParams(this IDictionary<string, string> toolStateParameters)
        {
            var parameters = new RavenStoreParameters();
            if (toolStateParameters.ContainsKey(ApplicationOptions.CutoffTime))
                parameters.MaxCutoffTime = Convert.ToDateTime(toolStateParameters[ApplicationOptions.CutoffTime]);

            return parameters;
        }
    }
}