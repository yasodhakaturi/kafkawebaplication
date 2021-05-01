using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    interface ProducerManager
    {
        string addProducer(Topic topic);
        void destroyProducer(string producerId);

        //Dictionary<string, JavaCroProducer> getProducers();
    }
}
