namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System;
    using System.Collections.Generic;

    public static class RavenStoreParameterExtensions
    {
        public static RavenStoreParameters ToRavenParams(this IDictionary<string, string> toolStateParameters)
        {
            var parameters = new RavenStoreParameters();
            if (toolStateParameters.ContainsKey(ApplicationOptions.CutoffTime))
            {
                parameters.MaxCutoffTime = DateTimeOffset.Parse(toolStateParameters[ApplicationOptions.CutoffTime]);
            }

            return parameters;
        }
    }
}