using System;

namespace AzureTablePurger.Services
{
    public interface IPartitionKeyHandler
    {
        string GetTableQuery(int purgeEntitiesOlderThanDays);

        DateTime ConvertPartitionKeyToDateTime(string partitionKey);

        string GetPartitionKeyForDate(DateTime date);

        string GetTableQuery(string lowerBoundPartitionKey, string upperBoundPartitionKey);
    }
}
