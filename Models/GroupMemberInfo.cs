using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Web.Models
{
    public class GroupMember
    {
        public string MemberId { get; set; }
        
        public string ClientId { get; set; }
        
        public string ClientHost { get; set; }
       
        public byte[] MemberMetadata { get; set; }
        
        public byte[] MemberAssignment { get; set; }
    }
}
