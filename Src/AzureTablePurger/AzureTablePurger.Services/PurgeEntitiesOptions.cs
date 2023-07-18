namespace AzureTablePurger.Services
{
    public class PurgeEntitiesOptions
    {
        public static string PurgeEntitiesSection = "PurgeEntities";
        public string TargetStorageAccountConnectionString { get; set; }

        public string TargetTableName { get; set; }

        public int PurgeRecordsOlderThanDays { get; set; }

        public string PartitionKeyFormat { get; set; }
    }
}