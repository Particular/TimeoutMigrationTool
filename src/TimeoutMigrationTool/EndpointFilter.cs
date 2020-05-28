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

        public static EndpointFilter SpecificEndpoint(string endpoint)
        {
            return new EndpointFilter(endpoint);
        }

        public bool ShouldInclude(string endpoint)
        {
            if (includeAll)
            {
                return true;
            }

            return string.Equals(endpointName, endpoint, StringComparison.OrdinalIgnoreCase);
        }

        EndpointFilter(bool includeAll)
        {
            this.includeAll = includeAll;
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
        readonly bool includeAll;
    }
}