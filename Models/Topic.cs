using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class Topic
    {
        

            public int partitionCount;

            public string topicName;

        public List<Partition> partitions = new List<Partition>();
        public Topic() { }
        public Topic(int partitionCount, string topicName)
            {
                this.partitionCount = partitionCount;
                this.topicName = topicName;
                //this.topicColorHex = topicColorHex;
            }



            public int getPartitionCount()
            {
                return partitionCount;
            }

            public string getTopicName()
            {
                return topicName;
            }

            //public string getTopicColorHex()
            //{
            //    return topicColorHex;
            //}






            public Boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || GetType() != o.GetType()) return false;
                Topic topic = (Topic)o;
                return topicName != null ? topicName.Equals(topic.topicName) : topic.topicName == null;
            }


            public int hashCode()
            {
                return topicName != null ? topicName.GetHashCode() : 0;
            }
        }
    }

