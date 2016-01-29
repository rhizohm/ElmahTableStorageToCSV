using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MoveElmahLogsToCSV
{
    public class ElmahEntity : TableEntity 
    {
        public ElmahEntity() { }
        public string ApplicationName { get; set; }
        public string HostName { get; set; }
        public string Type { get; set; }
        public string Source { get; set; }
        public string Message { get; set; }
        public string User { get; set; }
        public string StatusCode { get; set; }
        public string AllXml { get; set; }
    }
}
