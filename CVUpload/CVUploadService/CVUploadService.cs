using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace CVUploadService
{
    public partial class CVUploadService : ServiceBase
    {
        private readonly ILogger _logger;

        readonly System.Timers.Timer _timer = new System.Timers.Timer();
        private string UploadLogFile = "";
        private System.Timers.Timer timer;


        ArmRepository _iArmRepo = new ArmRepository();
        public CVUploadService()
        {

            UploadLogFile = _iArmRepo.GetFileLocation(3);
            _logger = Logger.GetInstance;
            CreateLogDirectory(UploadLogFile);

            //InitializeComponents();


        }

        private void CreateLogDirectory(string uploadLogFile)
        {

            int index = uploadLogFile.LastIndexOf("Logs");
            if (index > 0)
            {
                uploadLogFile = uploadLogFile.Substring(0, index + 5);
            }
            if (!Directory.Exists(uploadLogFile))
                Directory.CreateDirectory(uploadLogFile);
            _logger.Log("Creating Log Directory", @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

        }

        protected override void OnStart(string[] args)
        {
            try
            {
                _logger.Log("Service started", @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                InitializeComponents();
            }
            catch (Exception ex)
            {
                _logger.Log("Service Exception Occured in OnStart" + ex.Message, @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

            }
        }


        private void InitializeComponents()
        {
            try
            {
                var timerInterVal = Convert.ToInt32(_iArmRepo.GetFileLocation(0));// int.Parse(ConfigurationManager.AppSettings["timeInterVal"]);
                _timer.AutoReset = true;
                _timer.Interval = timerInterVal;
                _timer.Enabled = true;
                _timer.Start();
                _timer.Elapsed += (new FileParser()).FileParse;
            }
            catch (Exception ex)
            {
                _logger.Log("Service Exception Occured in InitializeComponents" + ex.Message, @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

            }
        }


        protected override void OnStop()
        {
            try
            {
                _timer.Enabled = false;
                _timer.Stop();
                _logger.Log("Service stopped", @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
            }
            catch (Exception ex)
            {
                _logger.Log("Service Exception Occured in OnStop" + ex.Message, @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

            }

        }

    }
}
