using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class Groupinfo
    {
        public Broker broker { get; set; }
        public string Group { get; set; }

        //public Error Error { get;set; }

        public string State { get; set; }
        
        public string ProtocolType { get; set; }
       
        public string Protocol { get; set; }
        public List<GroupMember> Members { get; set; }

    }
}
