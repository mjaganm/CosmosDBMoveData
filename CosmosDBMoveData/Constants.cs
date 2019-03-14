
namespace CosmosDBMoveData
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using Microsoft.Azure.Documents.Client;

    /// <summary>
    /// Contains the constants used in the application
    /// </summary>
    public static class Constants
    {
        public static string SourceCosmosDBEndPointUrl = ConfigurationManager.AppSettings["SourceCosmosDBEndPointUrl"];

        public static string SourceCosmosDBAuthorizationKey = ConfigurationManager.AppSettings["SourceCosmosDBAuthorizationKey"];

        public static string SourceDatabaseName = ConfigurationManager.AppSettings["SourceDatabaseName"];

        public static string SourceCollectionName = ConfigurationManager.AppSettings["SourceCollectionName"];

        public static string DestCosmosDBEndPointUrl = ConfigurationManager.AppSettings["DestCosmosDBEndPointUrl"];

        public static string DestCosmosDBAuthorizationKey = ConfigurationManager.AppSettings["DestCosmosDBAuthorizationKey"];

        public static string DestDatabaseName = ConfigurationManager.AppSettings["DestDatabaseName"];

        public static string DestCollectionName = ConfigurationManager.AppSettings["DestCollectionName"];

        public static List<string> pkRangeIdList = string.IsNullOrEmpty(ConfigurationManager.AppSettings["pkRangeIdsToUse"]) ?
            null : new List<string> (ConfigurationManager.AppSettings["pkRangeIdsToUse"].Split(','));

        public static Uri SourceCollectionUri = UriFactory.CreateDocumentCollectionUri(
            SourceDatabaseName,
            SourceCollectionName);

        public static Uri DestCollectionUri = UriFactory.CreateDocumentCollectionUri(
            DestDatabaseName,
            DestCollectionName);

        public static int LogicalProcessors = Environment.ProcessorCount;
    }
}
