// Copyright 2020 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;
using Web.Operations;
using Web.Models;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using System.Threading;

namespace Web.Controllers
{
    public class HomeController : Controller 
    {
        string bootstrapServers = Helper.BootstrapServers;
       

        private string FrivolousTopic, RequestTimeTopic;
        private readonly KafkaDependentProducer<Null, string> producer;
        private readonly KafkaDependentProducer<string, string> producer1;
        private readonly KafkaDependentProducer<string, string> producer2;
        private readonly ILogger logger;
        private static volatile bool _running = true;

        private static string[] _values =
            {
                        "aaaaaaaaaaaaa",
                        "bbbbbbbbbbbbbbbbb",
                        "cccccccccccccccccccccccccccc"
                        //"dddddddddddddddddddddddddddddddd",
                        //"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                        //"ffffffffffffffffffffffffffffffffffffffffff",
                        //@"ggggggggggggggggggggggggggggggggggggggggggggggggggggg
                        //  hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh",
                        //"iiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiii",
                        //"jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj",
                        //"kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk",
                        //"lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll",
                        //"mmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm"
                    };
        private static string[] _values1 =
           {
                        "111111111111",
                        "222222222222222222222",
                        "33333333333333333333333333333",
                        "44444444444444444444444444444444444",
                        "5555555555555555555555555555555555555555",
                        "66666666666666666666666666666666666666666666666666",
                        @"7777777777777777777777777777777777777777777777777777777
                          88888888888888888888888888888888888888888888888888888888888888",
                        "999999999999999999999999999999999999999999999999999999999999999999999999",
                        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                        
                    };

        public HomeController(KafkaDependentProducer<Null, string> producer, KafkaDependentProducer<string, string> producer1, KafkaDependentProducer<string, string> producer2, IConfiguration config, ILogger<HomeController> logger)
        //public HomeController( KafkaDependentProducer<string, string> producer1,  IConfiguration config, ILogger<HomeController> logger)
        {
            // In a real-world application, you might be using Kafka as a system of record and may wish
            // to update application state in your request handlers. Or you may wish to write some
            // analytics data related to your handler logic. In this example, we aren't doing anything
            // interesting, so we just write frivolous messages to demonstrate how to set things up.
            this.FrivolousTopic = config.GetValue<string>("Kafka:FrivolousTopic");
            this.RequestTimeTopic = config.GetValue<string>("Kafka:RequestTimeTopic");
            this.producer = producer;
            this.producer1 = producer1;
            this.producer2 = producer2;
            this.logger = logger;
        }

        public ActionResult Index()
        {
             UAEProducer();
            //USAProducer();
            if (TempData["topicslist"] == null && TempData["topiclist"] ==null)
                return View();
            else if(TempData["topicslist"] != null)
            {
                //KafkaServerDetails kss = JsonConvert.DeserializeObject<KafkaServerDetails>((string)TempData["serverdetails"]);
                var gettopics = TempData["topicslist"];
                ViewBag.gettopics= TempData["topicslist"];
                return View(gettopics);
            }
            else if (TempData["topiclist"] != null)
            {
                //KafkaServerDetails kss = JsonConvert.DeserializeObject<KafkaServerDetails>((string)TempData["serverdetails"]);
                var gettopics = TempData["topiclist"];
                ViewBag.gettopic = TempData["topiclist"];
                return View(gettopics);
            }

            return View();

        }

        public async Task UAEProducer()
        {

            // await Task.Yield();
            //var random = new Random();
            //int i = 0;
            await Task.Run(() =>
            {
                foreach (string data in _values)
                {
                    Confluent.Kafka.Partition p = new Confluent.Kafka.Partition(2);
                   
                    TopicPartition tp = new TopicPartition(RequestTimeTopic, p);

                    //var data = " " + _values[random.Next(_values.Length)];
                    this.producer.Produce1(tp, new Message<Null, string> {  Value = data }, INDdeliveryReportHandler);
                    System.Threading.CancellationToken ct = new CancellationToken();
                    //this.producer1.Produce(RequestTimeTopic, new Message<string, string> { Key = "2345", Value = data }, UAEdeliveryReportHandler);
                    //this.producer.Produce(RequestTimeTopic, new Message<Null, string> {  Value = data }, INDdeliveryReportHandler);
                    this.producer.Generator_LineInputData(this.bootstrapServers, "2345",  ct);
                    //this.producer2.Produce(RequestTimeTopic, new Message<string, string> { Key = "abcd", Value = data }, USAdeliveryReportHandler);
                    //this.producer2.Produce(FrivolousTopic, new Message<string, string> { Key = "abcd", Value = data }, UAEdeliveryReportHandler);

                }
            });
            //RedirectToAction("Index", "Home");
           
        }
        public async Task USAProducer()
        {

            //await Task.Yield();
            //var random = new Random();
            //int i = 0;
            await Task.Run(() =>
            {
                foreach (string data in _values1)
                {

                    //var data = " " + _values[random.Next(_values.Length)];
                    this.producer2.Produce(RequestTimeTopic, new Message<string, string> { Key = "abcd", Value = data }, USAdeliveryReportHandler);
                }
            });
        }
        public async Task<IActionResult> Index2()
        {
            
        await Task.Yield();
            var random = new Random();
            int i = 0;
           foreach(string data in _values)
            {
                
                //var data = " " + _values[random.Next(_values.Length)];
                this.producer1.Produce(FrivolousTopic, new Message<string, string> { Key = "2345", Value = data }, UAEdeliveryReportHandler);
            }
            foreach (string data in _values1)
            {

                //var data = " " + _values[random.Next(_values.Length)];
                this.producer1.Produce(FrivolousTopic, new Message<string, string> { Key = "aaaaa", Value = data }, UAEdeliveryReportHandler);
            }
            //this.producer.Produce(FrivolousTopic, new Message<Null, string> {  Value = "Msg to USA Consumer" }, USAdeliveryReportHandler);
            
            //this.producer2.Produce(topic1, new Message<string, int> { Key = "aaaaa", Value =5 }, deliveryReportHandler2);

            return View();
        }

                public async Task<IActionResult> Index1()
        {
            // Simulate a complex request handler by delaying a random amount of time.
            await Task.Delay((int)(new Random((int)DateTime.Now.Ticks).NextDouble()*100));

            // Important note: DO NOT create a new producer instance every time you
            // need to produce a message (this is a common pattern with relational database
            // drivers, but it is extremely inefficient here). Instead, use a long-lived
            // singleton instance, as per this example.

            // Do not delay completion of the page request on the result of produce call.
            // Any errors are handled out of band in deliveryReportHandler.

            this.producer1.Produce(RequestTimeTopic, new Message<string, string> {Key = "123456", Value = "message from UAEProducer" }, UAEdeliveryReportHandler);
            return View();
        }

        public async Task<IActionResult> Page1()
        {
            await Task.Delay((int)(new Random((int)DateTime.Now.Ticks).NextDouble()*100));

            // Delay completion of the page request on the result of the produce call.
            // An exception will be thrown in the case of an error.
           
            await this.producer.ProduceAsync(FrivolousTopic, new Message<Null, string> { Value = "message from USAProducer" });
            return View();
        }

        private void USAdeliveryReportHandler(DeliveryReport<string, string> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                // It is common to write application logs to Kafka (note: this project does not provide
                // an example logger implementation that does this). Such an implementation should
                // ideally fall back to logging messages locally in the case of delivery problems.
                this.logger.Log(LogLevel.Warning, $"Message delivery failed: {deliveryReport.Message.Value}");
            }
            else
            {
                this.logger.Log(LogLevel.Information, $"USAdeliveryReportHandler--Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");
            }
        }
        private void UAEdeliveryReportHandler(DeliveryReport<string, string> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                // It is common to write application logs to Kafka (note: this project does not provide
                // an example logger implementation that does this). Such an implementation should
                // ideally fall back to logging messages locally in the case of delivery problems.
                this.logger.Log(LogLevel.Warning, $"Message delivery failed: {deliveryReport.Message.Value}");
            }
            else
            {
                this.logger.Log(LogLevel.Information, $"UAEdeliveryReportHandler---Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");
            }
        }
        private void INDdeliveryReportHandler(DeliveryReport<Null, string> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                // It is common to write application logs to Kafka (note: this project does not provide
                // an example logger implementation that does this). Such an implementation should
                // ideally fall back to logging messages locally in the case of delivery problems.
                this.logger.Log(LogLevel.Warning, $"Message delivery failed: {deliveryReport.Message.Value}");
            }
            else
            {
                this.logger.Log(LogLevel.Information, $"INDdeliveryReportHandler---Message delivered on Topic: {deliveryReport.Topic} ,at partition : {deliveryReport.TopicPartition} ,Offset:{deliveryReport.TopicPartitionOffset}");
            }
        }
        private void deliveryReportHandler2(DeliveryReport<string, int> deliveryReport)
        {
            if (deliveryReport.Status == PersistenceStatus.NotPersisted)
            {
                this.logger.Log(LogLevel.Warning, $"Failed to log request time for path: {deliveryReport.Message.Key}");
            }
        }



        [HttpPost]
        public ActionResult CreateTopic(string texttopicname , int textpartitioncount)
        {
            //ColorPicker colorPicker = new ColorPicker();
            KafkaServerDetails ks = new KafkaServerDetails();

            new TopicManagerImpl().addTopic(texttopicname, textpartitioncount);

            //ks= new TopicManagerImpl().GetTopicsDetails(bootstrapServers);
            //Models.KafkaServerDetails ks = kf;
          TempData["topicslist"] =new TopicManagerImpl().gettopics(); 
            //var grid = new WebGrid(ViewBag.kafkaserverdetails);
            //@grid.GetHtml();
            return RedirectToAction("Index","Home");
            //return View(ks);
        }

        [HttpPost]
        public ActionResult IncreasePartitions(string texttopicname, int textpartitioncount)
        {
            //ColorPicker colorPicker = new ColorPicker();
            KafkaServerDetails ks = new KafkaServerDetails();

            new TopicManagerImpl().IncreasePartitions(texttopicname, textpartitioncount);

            //ks= new TopicManagerImpl().GetTopicsDetails(bootstrapServers);
            //Models.KafkaServerDetails ks = kf;
           Topic ts= new TopicManagerImpl().gettopic(texttopicname,bootstrapServers);
            TempData["topiclist"] = ts.topicName + ":" + ts.partitionCount;
            //var grid = new WebGrid(ViewBag.kafkaserverdetails);
            //@grid.GetHtml();
            return RedirectToAction("Index", "Home");
            //return View(ks);
        }
        public ActionResult ViewTopics()
        {
            TempData["topicslist"] = new TopicManagerImpl().gettopics();
            //var grid = new WebGrid(ViewBag.kafkaserverdetails);
            //@grid.GetHtml();
            return RedirectToAction("Index", "Home");
            //return View(ks);

        }

        public ActionResult DeleteTopic(string topicname)
        {
            IEnumerable<string> topic = new List<string> { topicname };
            new TopicManagerImpl().deleteTopics(Helper.BootstrapServers, topic);

            TempData["topicslist"] = new TopicManagerImpl().gettopics();
            //var grid = new WebGrid(ViewBag.kafkaserverdetails);
            //@grid.GetHtml();
            return RedirectToAction("Index", "Home");
        }
        public ActionResult ViewKafkaDetails()
        {
            KafkaServerDetails ks = new TopicManagerImpl().GetkafkaserverDetails(bootstrapServers);
            return View(ks);
        }
        public ActionResult GetTopic(string topicname)
        {
            Topic ts = new TopicManagerImpl().gettopic(topicname,bootstrapServers);
            TempData["topiclist"] = ts.topicName+" : "+ts.partitions.Count;
            return RedirectToAction("Index", "Home");
        }

        [HttpPost]
        public ActionResult CreateProducer(string texttopicname)
        {
            foreach (string data in _values)
            {

                //var data = " " + _values[random.Next(_values.Length)];
                this.producer1.Produce(texttopicname, new Message<string, string> { Key = "2345", Value = data }, UAEdeliveryReportHandler);
            }
            return View();
        }
    }
}
