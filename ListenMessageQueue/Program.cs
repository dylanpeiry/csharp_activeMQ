using Apache.NMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ListenMessageQueue
{
    class Program
    {
        const string DEFAULT_PORT = "61616";
        const string DEFAULT_HOST = "localhost";
        public static string queueName = null;
        public static string hostIp = null;
        public static string hostPort = null;
        static void Main(string[] args)
        {
            do
            {
                Console.Write("HostIp : ");
                hostIp = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(hostIp))
                    hostIp = DEFAULT_HOST;
            } while (hostIp == null);

            do
            {
                Console.Write("HostPort : ");
                hostPort = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(hostPort))
                    hostPort = DEFAULT_PORT;
            } while (hostPort == null);

            do
            {
                Console.Write("QueueName : ");
                queueName = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(queueName))
                    queueName = null;
            } while (queueName == null);

            Console.WriteLine("Waiting for messages");

            // Read all messages off the queue
            while (ReadNextMessageQueue())
            {
                Console.WriteLine("Successfully read message");
            }

            Console.WriteLine("Finished");
        }

        static bool ReadNextMessageQueue()
        {

            string brokerUri = $"activemq:tcp://{hostIp}:{hostPort}";  // Default port
            NMSConnectionFactory factory = new NMSConnectionFactory(brokerUri);

            using (IConnection connection = factory.CreateConnection())
            {
                connection.Start();
                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (IDestination dest = session.GetQueue(queueName))
                using (IMessageConsumer consumer = session.CreateConsumer(dest))
                {
                    IMessage msg = consumer.Receive();
                    if (msg is ITextMessage)
                    {
                        ITextMessage txtMsg = msg as ITextMessage;
                        string body = txtMsg.Text;
                        Console.WriteLine($"Received message: {txtMsg.Text}");
                        return true;
                    }
                    else
                    {
                        Console.WriteLine("Unexpected message type: " + msg.GetType().Name);
                    }
                }
            }
            return false;
        }
    }
}
