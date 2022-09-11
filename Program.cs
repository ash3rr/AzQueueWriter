using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Files.Shares;
using Azure.Storage.Files.Shares.Models;
using Azure.Storage.Sas;
using Azure.Messaging.ServiceBus;
using Azure.Storage.Blobs.Models;


namespace QueueingFileSystem
{
    class Program
    {
        static string QueueConnectionString = "";
        static string queueName = "testqueue";
        static Queue<ServiceBusMessage> messages = new Queue<ServiceBusMessage>();
        static string BlobConnectionString = $"";
        static string ContainerName = "queue";
        static string fileName = "QueueInput.txt";


        static async Task Main()
        {
            // send a message to the queue
            //await SendMessageAsync();

            // send a batch of messages to the queue
            await GetBlob();

            await SendMessageBatchAsync();

            
        }



        static async Task GetBlob()
        {

            // Create BlobServiceClient from the connection string.
            BlobServiceClient blobServiceClient = new BlobServiceClient(BlobConnectionString);

            // Get and container with blob
            BlobContainerClient container = blobServiceClient.GetBlobContainerClient(ContainerName);
            //await container.CreateIfNotExistsAsync();
            
            // get file from blob
            BlobClient blob = container.GetBlobClient(fileName);

            BlobDownloadInfo download = blob.Download();
            
            var content = download.Content;
            using (var streamReader = new StreamReader(content))
            {
                while (!streamReader.EndOfStream)
                {
                    var line = await streamReader.ReadLineAsync();
                    Console.WriteLine(line);
                    messages.Enqueue(new ServiceBusMessage(line));
                
                }
            }

            //Console.Write("Downloading contents");

            //string contents = blob.DownloadAsync().ToString();

            //Console.WriteLine(contents);

            //// List all blobs in the container
            //await foreach (BlobItem blobItem in container.GetBlobsAsync())
            //{
            //    Console.WriteLine("\t" + blobItem.Name);

            //    //return contents;
            //}


        }

        static async Task SendMessageBatchAsync()
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(QueueConnectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(queueName);

                // total number of messages to be sent to the Service Bus queue
                int messageCount = messages.Count;

                // while all messages are not sent to the Service Bus queue
                while (messages.Count > 0)
                {
                    // start a new batch 
                    using ServiceBusMessageBatch messageBatch = await sender.CreateMessageBatchAsync();

                    // add the first message to the batch
                    if (messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue once the message is added to the batch
                        messages.Dequeue();
                    }
                    else
                    {
                        // if the first message can't fit, then it is too large for the batch
                        throw new Exception($"Message {messageCount - messages.Count} is too large and cannot be sent.");
                    }

                    // add as many messages as possible to the current batch
                    while (messages.Count > 0 && messageBatch.TryAddMessage(messages.Peek()))
                    {
                        // dequeue the message from the .NET queue as it has been added to the batch
                        messages.Dequeue();
                    }

                    // now, send the batch
                    await sender.SendMessagesAsync(messageBatch);

                    // if there are any remaining messages in the .NET queue, the while loop repeats 
                }

                Console.WriteLine($"Sent a batch of {messageCount} messages to the topic: {queueName}");
            }
        }




        static async Task SendMessageAsync()
        {
            // create a Service Bus client 
            await using (ServiceBusClient client = new ServiceBusClient(QueueConnectionString))
            {
                // create a sender for the queue 
                ServiceBusSender sender = client.CreateSender(queueName);

                // create a message that we can send
                ServiceBusMessage message = new ServiceBusMessage("Hello world!");

                // send the message
                await sender.SendMessageAsync(message);
                Console.WriteLine($"Sent a single message to the queue: {queueName}");
            }
        }
    }
}
