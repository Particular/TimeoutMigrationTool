namespace Particular.TimeoutMigrationTool
{
    using System;

    public class EndpointFilter
    {
        public static EndpointFilter IncludeAll
        {
            get
            {
                return new EndpointFilter(true);
            }
        }

        public bool IncludeAllEndpoints { get; private set; }

        public static EndpointFilter SpecificEndpoint(string endpoint)
        {
            return new EndpointFilter(endpoint);
        }

        public bool ShouldInclude(string endpoint)
        {
            if (IncludeAllEndpoints)
            {
                return true;
            }

            return string.Equals(endpointName, endpoint, StringComparison.OrdinalIgnoreCase);
        }

        EndpointFilter(bool includeAll)
        {
            IncludeAllEndpoints = includeAll;
        }

        EndpointFilter(string endpointName)
        {
            if(string.IsNullOrEmpty(endpointName))
            {
                throw new ArgumentException("Endpoint name must be a non empty string");
            }

            this.endpointName = endpointName;
        }

        readonly string endpointName;
    }
}