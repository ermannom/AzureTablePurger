using Azure.Data.Tables;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;

namespace AzureTablePurger.Services
{
    /// <summary>
    /// Used to create clients for Azure Storage accounts. A single instance of a specific client is created and cached.
    /// </summary>
    public class AzureStorageClientFactory : IAzureStorageClientFactory
    {
        private static readonly ConcurrentDictionary<string, TableServiceClient> TableServiceClientCache = new ConcurrentDictionary<string, TableServiceClient>();

        private readonly ILogger<AzureStorageClientFactory> _logger;

        public AzureStorageClientFactory(ILogger<AzureStorageClientFactory> logger)
        {
            _logger = logger;
        }

        public TableServiceClient GetTableServiceClient(string connectionString)
        {
            if (TableServiceClientCache.ContainsKey(connectionString))
            {
                return TableServiceClientCache[connectionString];
            }

            _logger.LogDebug("TableServiceClient not found in cache. Creating new one and adding to cache");

            var tableServiceClient = new TableServiceClient(connectionString);

            bool resultOfAdd = TableServiceClientCache.TryAdd(connectionString, tableServiceClient);

            if (!resultOfAdd)
            {
                _logger.LogDebug("Adding TableServiceClient to cache failed. Another thread must have beat us to it. Obtaining and returning the one in cache");
                return TableServiceClientCache[connectionString];
            }

            return tableServiceClient;
        }
    }
}
