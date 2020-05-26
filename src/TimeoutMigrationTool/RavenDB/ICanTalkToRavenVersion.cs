using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    internal interface ICanTalkToRavenVersion
    {
        Task UpdateRecord(string key, object document);
        Task DeleteRecord(string key);
        Task CreateBatchAndUpdateTimeouts(BatchInfo batch);
        Task DeleteBatchAndUpdateTimeouts(BatchInfo batch);
        Task BatchDelete(string[] keys);
        Task<List<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string prefix,
            CancellationToken cancellationToken, int pageSize = RavenConstants.DefaultPagingSize) where T : class;
        Task<T> GetDocument<T>(string id) where T : class;
    }
}