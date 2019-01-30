using Apache.NMS;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SendMessageQueue
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

            while (true)
            {
                string text = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(text)) return;
                SendNewMessageQueue(text);
            }
        }


        private static void SendNewMessageQueue(string text)
        {

            Console.WriteLine($"Adding message to queue topic: {queueName}");

            string brokerUri = $"activemq:tcp://{hostIp}:{hostPort}";  // Default port
            NMSConnectionFactory factory = new NMSConnectionFactory(brokerUri);

            using (IConnection connection = factory.CreateConnection())
            {
                connection.Start();

                using (ISession session = connection.CreateSession(AcknowledgementMode.AutoAcknowledge))
                using (IDestination dest = session.GetQueue(queueName))
                using (IMessageProducer producer = session.CreateProducer(dest))
                {
                    producer.DeliveryMode = MsgDeliveryMode.NonPersistent;

                    producer.Send(session.CreateTextMessage(text));
                    Console.WriteLine($"Sent {text} messages");
                }
            }
        }
    }
}
