using Azure.Data.Tables;

namespace AzureTablePurger.Services
{
    public interface IAzureStorageClientFactory
    {
        TableServiceClient GetTableServiceClient(string connectionString);
    }
}