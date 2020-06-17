using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Particular.TimeoutMigrationTool.RavenDB
{
    public interface ICanTalkToRavenVersion
    {
        Task UpdateDocument(string key, object document);
        Task DeleteDocument(string key);
        Task CreateBatchAndUpdateTimeouts(RavenBatch batch);
        Task DeleteBatchAndUpdateTimeouts(RavenBatch batch);
        Task BatchDelete(string[] keys);
        Task<List<T>> GetDocuments<T>(Func<T, bool> filterPredicate, string documentPrefix, Action<T, string> idSetter, int pageSize = RavenConstants.DefaultPagingSize) where T : class;
        Task<List<T>> GetPagedDocuments<T>(string documentPrefix, Action<T, string> idSetter, int startFrom, int nrOfPages = 0) where T : class;
        Task<T> GetDocument<T>(string id, Action<T, string> idSetter) where T : class;
        Task<List<T>> GetDocuments<T>(IEnumerable<string> ids, Action<T, string> idSetter) where T : class;
        Task CompleteBatchAndUpdateTimeouts(RavenBatch batch);
        Task ArchiveDocument(string archivedToolStateId, RavenToolState toolState);
    }
}