using System;
using System.Configuration;
using System.IO;

namespace CVUploadService
{
    public sealed class Logger : ILogger
    {
        private static readonly object MyLock = new object();
       
        private readonly string _logFile = "";//@"" + ConfigurationManager.AppSettings["logFile"] + DateTime.Now.ToString("DDMMYY");
        private static readonly Lazy<Logger> LoggerInstance = new Lazy<Logger>(() => new Logger());
        public static Logger GetInstance => LoggerInstance.Value;
        private Logger()
        {
            
            
        }



        public void LogX(string message,string filePath="")
        {
            lock (MyLock)
            {
                //if (!File.Exists(filePath))
                //    File.Create(filePath);
                try
                {
                    using (var file = File.AppendText(filePath))
                    {
                        file.Write(message + " at " + DateTime.Now.ToString("MM/dd/yyyy hh:mm:ss.fff tt") + Environment.NewLine);
                    }

                }
                catch(Exception ex)
                {
                    // ignored
                }
            }
        }
    }
}
