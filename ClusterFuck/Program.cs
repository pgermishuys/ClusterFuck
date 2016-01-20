using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;

namespace ClusterFuck
{
    class Program
    {
        private static readonly List<Node> Nodes = 
            new List<Node>() { new Node(1), new Node(2), new Node(3) };

        static void Main(string[] args)
        {
            Nodes.ForEach(n => n.Start());
            Console.WriteLine("Waiting for nodes to start");
            Console.WriteLine("CBA to write code for this - Go sort out projections then press enter to begin");
            Console.ReadLine();

            while (!AreProjectionsFuckedYet())
            {
                var master = GetMaster();
                master.FuckOff();
                master.Start();
                Thread.Sleep(15000);
            }

            Console.WriteLine("Projections fucked!!! (Master was {0})", GetMaster().Name);
            Console.ReadLine();
            Nodes.ForEach(n => n.FuckOff());
        }

        static Node GetMaster()
        {
            var c = new HttpClient();
            var req = new HttpRequestMessage(HttpMethod.Get, "http://127.0.0.1:10004/gossip");
            req.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            var resp = c.SendAsync(req).Result;
            var respContent = resp.Content.ReadAsStringAsync().Result;
            var gossip = JObject.Parse(respContent);

            var master = gossip["members"].First(m => m["state"].ToString() == "Master");
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
                ? new[] {2, 3}
                : Number == 2 ? new[] {1, 3} : new[] {1, 2};

            var seeds = String.Join(",",otherNodes.Select(n => $"127.0.0.1:{n*10000 + 3}"));

            var args =
                $"--db ./{Name}DB --log ./{Name}Logs --int-tcp-port={IntTcp} --ext-tcp-port={ExtTcp} --int-http-port={IntHttp} --ext-http-port={ExtHttp} --run-projections=all --cluster-size=3 --discover-via-dns=false --gossip-seed={seeds} --gossip-on-ext=true";
            _nodeProcess = Process.Start(dir + "\\" + Executable, args);
        }

        public void FuckOff()
        {
            _nodeProcess.Kill();   
        }
    }
}
