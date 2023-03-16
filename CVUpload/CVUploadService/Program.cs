using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace CVUploadService
{
    internal static class Program
    {
     
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            //var fileParser = new FileParser();
            //fileParser.FileParse();
            //Console.ReadLine();
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new CVUploadService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}

