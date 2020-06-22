namespace Particular.TimeoutMigrationTool.RavenDB
{
    using System.Collections.Generic;

    public class GetByIndexResult<T> where T: class
    {
        public string IndexETag { get; set; }
        public bool IsStale { get; set; }
        public int NrOfDocuments { get; set; }
        public List<T> Documents { get; set;}
    }
}