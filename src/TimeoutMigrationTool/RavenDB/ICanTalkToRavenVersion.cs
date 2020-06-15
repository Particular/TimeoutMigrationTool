using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public interface ICanTalkToRavenVersion
    {
        Task UpdateDocument(string key, object document);
        Task DeleteDocument(string key);
        Task CreateBatchAndUpdateTimeouts(RavenBatchInfo batch);
        Task DeleteBatchAndUpdateTimeouts(RavenBatchInfo batch);
        Task BatchDelete(string[] keys);
        Task<List<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string documentPrefix, Action<T, string> idSetter, int pageSize = RavenConstants.DefaultPagingSize) where T : class;
        Task<T> GetDocument<T>(string id, Action<T, string> idSetter) where T : class;
        Task<List<T>> GetDocuments<T>(IEnumerable<string> ids, Action<T, string> idSetter) where T : class;
        Task CompleteBatchAndUpdateTimeouts(RavenBatchInfo batch);
        Task ArchiveDocument(string archivedToolStateId, RavenToolState toolState);
    }
}