using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;

namespace peek_topic_messages
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceBusPrimaryConnectionString = ConfigurationManager.AppSettings["Assignment.ServiceBus.ConnectionString"];
            var topicName = ConfigurationManager.AppSettings["ServiceBus.Topic"];
            PeekMessages(serviceBusPrimaryConnectionString, topicName, "msaas-assignment-centralus");
            //PeekMessages(serviceBusSecondaryConnectionString, topicName, "msaas-assignment-eastus2");
            Console.WriteLine("Finished browsing messages from topic: " + topicName);
            Console.Read();
        }

        static void PeekMessages(string connectionString, string topicName, string sbNamespace)
        {
            var receiverFactory = MessagingFactory.CreateFromConnectionString(connectionString);
            receiverFactory.RetryPolicy = new RetryExponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(1), 10);

            var receiver = receiverFactory.CreateTopicClient(topicName);
            var outputPath = Path.Combine(Directory.GetCurrentDirectory(), "topic-messages");
            if (!Directory.Exists(outputPath))
            {
                Directory.CreateDirectory(outputPath);
            }

            var fileName = Path.Combine(outputPath, "messages_" + sbNamespace + ".txt");
            File.WriteAllText(fileName, "");

            Console.WriteLine("Browsing messages from topic: " + topicName + ", Service Bus Namespace: " + sbNamespace);
            while (true)
            {
                try
                {
                    var message = receiver.PeekAsync().GetAwaiter().GetResult();
                    if (message != null)
                    {
                        //var body = new StreamReader(message.GetBody<Stream>(), true).ReadToEnd();
                        var messagePropertiesJson = Newtonsoft.Json.JsonConvert.SerializeObject(message.Properties, Newtonsoft.Json.Formatting.Indented);
                        StringBuilder sb = new StringBuilder();
                        sb.AppendLine("MessageId: " + message.MessageId);
                        sb.AppendLine("EnqueuedTimeUtc: " + message.EnqueuedTimeUtc.ToString("o"));
                        sb.AppendLine("ScheduledEnqueueTimeUtc: " + message.ScheduledEnqueueTimeUtc.ToString("o"));
                        sb.AppendLine("SequenceNumber: " + message.SequenceNumber);
                        sb.AppendLine("State: " + message.State);
                        sb.AppendLine("Properties: " + messagePropertiesJson);
                        sb.AppendLine("CurrentTimeUtc: " + DateTime.UtcNow.ToString("o"));
                        sb.AppendLine("********************************************************************************************************");
                        using (StreamWriter file = new StreamWriter(fileName, true))
                        {
                            file.WriteLine(sb.ToString());
                        }
                    }
                    else
                    {
                        //no more messages in the queue
                        break;
                    }
                }
                catch (MessagingException e)
                {
                    if (!e.IsTransient)
                    {
                        Console.WriteLine(e.Message);
                        throw;
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
            }
        }
    }
}
