using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public interface TopicManager
    {
      public  void addTopic(String topicValue, int partitionCount);
        //void deleteTopic(String topicValue);
        //List<Topic> getTopics();

        //Topic getTopic(String topicName);
    }
}
