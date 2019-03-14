
namespace CosmosDBMoveData
{
    using Microsoft.Azure.Documents.Client;
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Entry point into the CosmosDBMoveData
    /// </summary>
    class Program
    {
        /// <summary>
        /// Provides the entry point into the application
        /// </summary>
        static void Main(string[] args)
        {
            DocumentClient sourceClient = new DocumentClient(
                new Uri(Constants.SourceCosmosDBEndPointUrl),
                Constants.SourceCosmosDBAuthorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });

            DocumentClient destClient = new DocumentClient(
                new Uri(Constants.DestCosmosDBEndPointUrl),
                Constants.DestCosmosDBAuthorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Direct,
                    ConnectionProtocol = Protocol.Tcp
                });

            ChangeFeedViewer changeFeed = new ChangeFeedViewer(sourceClient, destClient);

            Task<Dictionary<string, string>> pTask = changeFeed.GetChanges(sourceClient);

            pTask.Wait();

            Console.WriteLine("List of ChangeFeed per partition");
            foreach (KeyValuePair<string, string> entry in pTask.Result)
            {
                Console.WriteLine("Partition: {0} Value: {1}", entry.Key, entry.Value);
            }
        }
    }
}
