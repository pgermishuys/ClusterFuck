using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Newtonsoft.Json.Linq;

namespace ClusterFuck
{
    class Program
    {
        private static readonly List<Node> Nodes = 
            new List<Node>() { new Node(1), new Node(2) };

        private static IEventStoreConnection _connection;

        static void Main(string[] args)
        {
            Nodes.ForEach(n => n.Start());

            _connection = EventStoreConnection.Create(ConnectionSettings.Default,
                ClusterSettings.Create()
                    .DiscoverClusterViaGossipSeeds()
                    .SetGossipSeedEndPoints(new[]
                    {
                        new IPEndPoint(IPAddress.Loopback, 10004), new IPEndPoint(IPAddress.Loopback, 20004)
                    }));

            _connection.ConnectAsync().Wait();

            Console.WriteLine("Waiting for nodes to start");
            Console.WriteLine("CBA to write code for this - Go sort out projections then press enter to begin");
            Console.ReadLine();

            Node master = GetSlave();

            while (!AreProjectionsFuckedYet())
            {
                master = GetSlave();
                master.FuckOff();
                Thread.Sleep(15000);
            }

            Console.WriteLine("Projections fucked!!! (Master is {0}, previously {1})", GetSlave().Name, master.Name);
            Console.ReadLine();
            Nodes.ForEach(n => n.FuckOff());
        }

        static void WriteSomething()
        {
            
            var jsons = Enumerable.Range(0, 100).Select(i => { return "{ \"something\": " + i + " }"; });

            var events =
                jsons.Select(j => new EventData(Guid.NewGuid(), "TestEvent", true, Encoding.UTF8.GetBytes(j), new byte[0]));

            _connection.AppendToStreamAsync("TestStream", ExpectedVersion.Any, events,
                new UserCredentials("admin", "changeit")).Wait();
        }

        static Node GetSlave()
        {
            var c = new HttpClient();
            var req = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1:10004/gossip");
            req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var resp = c.SendAsync(req).Result;
            var respContent = resp.Content.ReadAsStringAsync().Result;
            var gossip = JObject.Parse(respContent);

            var master = gossip["members"].First(m => m["state"].ToString() == "Slave");
            var masterIndex = Convert.ToInt16(master["internalTcpPort"].ToString().First().ToString()) - 1;

            return Nodes[masterIndex];
        }

        static bool AreProjectionsFuckedYet()
        {
            var c = new HttpClient();
            var req = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1:10004/projections/all-non-transient");
            req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var resp = c.SendAsync(req).Result;
            var respContent = resp.Content.ReadAsStringAsync().Result;
            var projections = JObject.Parse(respContent);

            var statuses = projections["projections"].Select(p => p["status"].ToString());

            Console.WriteLine("[{0}] Checking projections: {1}", DateTime.Now.ToString("G"), string.Join(",", statuses));
                
            return statuses.Any(s => !s.Contains("Running"));
        }
    }

    class Node
    {
        private const string Executable = "ES\\EventStore.ClusterNode.exe";
        private bool _shouldRestart = true;
        private DateTime _lastStart = DateTime.MinValue;
        public int Number;

        public string Name => $"Node{Number}";
        public int IntTcp  => Number*10000 + 1;
        public int ExtTcp  => Number*10000 + 2;
        public int IntHttp => Number*10000 + 3;
        public int ExtHttp => Number*10000 + 4;

        private Process _nodeProcess;

        public Node(int nodeNum)
        {
            Number = nodeNum;
        }

        public void Start()
        {
            var dir = Directory.GetCurrentDirectory();

            var otherNodes = Number == 1
                ? new[] {2}
                : Number == 2 ? new[] {1} : new[] {1};

            var seeds = String.Join(",",otherNodes.Select(n => $"127.0.0.1:{n*10000 + 3}"));

            var args =
                $"--db ./{Name}DB --log ./{Name}Logs --int-tcp-port={IntTcp} --ext-tcp-port={ExtTcp} --int-http-port={IntHttp} --ext-http-port={ExtHttp} --run-projections=all --cluster-size=3 --discover-via-dns=false --gossip-seed={seeds} --gossip-on-ext=true";
            _nodeProcess = Process.Start(dir + "\\" + Executable, args);
            _nodeProcess.EnableRaisingEvents = true;
            _nodeProcess.Exited += NodeProcessOnExited;
            _lastStart = DateTime.Now;
        }

        private void NodeProcessOnExited(object sender, EventArgs eventArgs)
        {
            Console.WriteLine("[{1}] Restarting node {0}", Name, DateTime.Now.ToString("G"));
            Thread.Sleep(1000);
            Start();
        }

        public bool IsDead => _nodeProcess.HasExited;

        public void FuckOff()
        {
            Console.WriteLine("[{1}] Killing node {0}", Name, DateTime.Now.ToString("G"));
            _nodeProcess.Kill();   
        }
    }
}
