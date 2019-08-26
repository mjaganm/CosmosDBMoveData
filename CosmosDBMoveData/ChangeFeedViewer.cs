 

namespace CosmosDBMoveData
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// Views the ChangeFeed on the collection for every pre-defined time-span
    /// </summary>
    public class ChangeFeedViewer
    {
        /// <summary>
        /// DocumentClient used to connect to Source CosmosDB
        /// </summary>
        private DocumentClient SourceClient { get; set; }

        /// <summary>
        /// DocumentClient used to connect to Destination CosmosDB
        /// </summary>
        private DocumentClient DestClient { get; set; }

        /// <summary>
        /// Checkpoints in the partitions
        /// </summary>
        private Dictionary<string, string> Checkpoints = new Dictionary<string, string>();

        /// <summary>
        /// Destination collection for the rollup data to write to
        /// </summary>
        private Uri destCollectionUri = UriFactory.CreateDocumentCollectionUri(Constants.DestDatabaseName, Constants.DestCollectionName);

        private int NumOfPartitions = 0;

        private ManualResetEvent resetEvent = new ManualResetEvent(false);

        /// <summary>
        /// Initializes a new instance of the ChangeFeedViewer class
        /// </summary>
        /// <param name="sourceClient">Document Client</param>
        public ChangeFeedViewer(DocumentClient sourceClient, DocumentClient destClient)
        {
            SourceClient = sourceClient;
            DestClient = destClient;
        }

        /// <summary>
        /// Gets the changes for the collection and iterates for over time-period
        /// </summary>
        /// <param name="sourceClient">Source Document Client</param>
        /// <returns></returns>
        public async Task<Dictionary<string, string>> GetChanges(
            DocumentClient sourceClient)
        {
            // Set Max threads in the threadpool
            // ThreadPool.SetMaxThreads(Constants.LogicalProcessors, Constants.LogicalProcessors);

            string pkRangesResponseContinuation = null;
            List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();

            do
            {
                FeedResponse<PartitionKeyRange> pkRangesResponse = await sourceClient.ReadPartitionKeyRangeFeedAsync(
                    Constants.SourceCollectionUri,
                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });

                partitionKeyRanges.AddRange(pkRangesResponse);
                pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
            }
            while (pkRangesResponseContinuation != null);

            // Number of threads to launch
            NumOfPartitions = partitionKeyRanges.Count;
            Console.WriteLine("Number of partitions in Source Collection: {0}", NumOfPartitions);

            if (Constants.pkRangeIdList != null)
            {
                Parallel.ForEach(Constants.pkRangeIdList, new ParallelOptions { MaxDegreeOfParallelism = Constants.DegreeOfParallelism },
                    pkRange =>
                    {
                        CallbackProcessEachPartition(pkRange);
                    });
            }
            else
            {
                Parallel.ForEach(partitionKeyRanges, new ParallelOptions { MaxDegreeOfParallelism = Constants.DegreeOfParallelism },
                    pkRange =>
                    {
                        CallbackProcessEachPartition(pkRange.Id);
                    });
            }

            // Wait for all the threads to complete
            resetEvent.WaitOne();

            // Console.WriteLine("Read {0} documents from the change feed", numChangesRead);

            return Checkpoints;
        }

        /// <summary>
        /// ThreadPool callback to process each partition
        /// </summary>
        /// <param name="pkRange">current PartitionKeyRange</param>
        public void CallbackProcessEachPartition(string pkRangeId)
        {
            string continuation = null;
            Checkpoints.TryGetValue(pkRangeId, out continuation);

            IDocumentQuery<Document> query = SourceClient.CreateDocumentChangeFeedQuery(
                Constants.SourceCollectionUri,
                new ChangeFeedOptions
                {
                    PartitionKeyRangeId = pkRangeId,
                    StartFromBeginning = true,
                    RequestContinuation = continuation,
                    MaxItemCount = -1
                });

            int numOfDocsUploaded = 0;
            while (query.HasMoreResults)
            {
                FeedResponse<Document> readChangesResponse = query.ExecuteNextAsync<Document>().Result;

                List<Task<bool>> taskList = new List<Task<bool>>();

                numOfDocsUploaded = 0;
                foreach (Document changedDocument in readChangesResponse)
                {
                    Task<bool> pTask = UploadToDestCollectionAsync(changedDocument);
                    taskList.Add(pTask);

                    // Wait for fixed number of tasks before creating new tasks
                    if (taskList.Count == 100)
                    {
                        Task.WaitAll(taskList.ToArray());

                        // Console.WriteLine("ThreadId: {0} Clearing the 100 tasks", Thread.CurrentThread.ManagedThreadId);
                        Console.Write(".");

                        taskList.Clear();
                    }

                    // Console.WriteLine("\t Debug: Read document {0} from the change feed.", changedDocument.ToString());

                    numOfDocsUploaded++;
                }

                Task.WaitAll(taskList.ToArray());

                Console.WriteLine("ThreadId: {0} Number of documents uploaded: {1}",
                    Thread.CurrentThread.ManagedThreadId,
                    numOfDocsUploaded);

                Checkpoints[pkRangeId] = readChangesResponse.ResponseContinuation;
            }

            // If this is the last thread to complete, set the event so that the main thread can continue
            if (Interlocked.Decrement(ref NumOfPartitions) == 0)
            {
                resetEvent.Set();
            }
        }

        /// <summary>
        /// Uploads the document to destination collection
        /// </summary>
        /// <param name="changedDocument"></param>
        /// <returns></returns>
        public async Task<bool> UploadToDestCollectionAsync(Document changedDocument)
        {
            bool operationCompleted = false;
            int numOfAttempts = 1;

            while (operationCompleted == false && numOfAttempts <= Constants.NumOfRetries)
            {
                try
                {
                    await DestClient.UpsertDocumentAsync(Constants.DestCollectionUri, changedDocument);

                    return true;
                }
                catch (DocumentClientException dex)
                {
                    if ((int)dex.StatusCode != 429 && (int)dex.StatusCode != 400)
                    {
                        Console.WriteLine("Unhandled Exception trying to upload DocumentId: {0}. Exception: {1}", changedDocument.Id, dex);
                        throw;
                    }

                    RunExponentialBackoff(numOfAttempts);
                    numOfAttempts++;
                }
            }

            return false;
        }

        /// <summary>
        /// Runs Exponential Backoff using the current number of attempts made.
        /// </summary>
        /// <param name="numOfAttempts">number of attempts.</param>
        public void RunExponentialBackoff(int numOfAttempts)
        {
            // Ceiling for Exponential backoff
            if (numOfAttempts > Constants.MaxAttemptsForExponentialBackoff)
            {
                numOfAttempts = Constants.MaxAttemptsForExponentialBackoff;
            }

            int waitTimeInMilliSeconds = (int)Math.Pow(2, numOfAttempts) * 100;
            Console.WriteLine("Exponential backoff: Retrying after Waiting for {0} milliseconds", waitTimeInMilliSeconds);
            Thread.Sleep(waitTimeInMilliSeconds);
        }
    }
}
