using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static CVUploadService.Model.Keys;

namespace CVUploadService.Service
{
    public class Logger4net
    {
        private static log4net.ILog log;
        private ConnectionDb _connectionDB;
        string filePath = "";

        public Logger4net()
        {
            _connectionDB = new ConnectionDb();
            filePath = GetFileLocation(0);
            Setup(filePath);
            log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        }

        public void PushLog(string Message, string even = "")
        {

            log.Info(Message);

            if (even != "" && even != null)
            {
                UpdateLogToDb(Message, even);
            }
        }
        public static void Setup(string filePath = "")
        {
            if (filePath == "" || filePath == null)
            {
                filePath = @"Log\";
            }

            if (!Directory.Exists(filePath))
                Directory.CreateDirectory(filePath);

            log4net.GlobalContext.Properties["LogFileName"] = @filePath;
            log4net.Config.XmlConfigurator.Configure();

        }

        public string GetFileLocation(int Key)
        {
            //use condition for key and set property 
            string propertyName = "";
            if (Key == 0)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 3);
            }



            string location = "";
            string sourceTableQuery = "select [dbo].[fnGlobalProperty](@propertyName) AS PropertyValue";

            try
            {
                _connectionDB.con.Open();
                using (SqlCommand cmd = new SqlCommand(sourceTableQuery, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@propertyName", propertyName);

                    location = (string)cmd.ExecuteScalar();

                }
                _connectionDB.con.Close();
                return location;
            }
            catch (Exception ex)
            {
                using (EventLog eventLog = new EventLog("Application"))
                {
                    eventLog.Source = "Application";
                    eventLog.WriteEntry("Harvest Service Error Messege: " + ex.Message, EventLogEntryType.Error, 999, 1);
                }
                log.Info(ex.Message + ex.InnerException);

                return "";
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }

        }

        public void UpdateLogToDb(string ErrorMsg, string Even)
        {
            string query = "EXEC dbo.SP_InsertADErrorLog @ErrorMsg,@Even";


            try
            {
                using (SqlCommand cmd = new SqlCommand(query, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@ErrorMsg", ErrorMsg);
                    cmd.Parameters.AddWithValue("@Even", Even);
                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
            }
            catch (Exception ex)
            {
                log.Info("Insert log to DB Exception: " + ex.Message + ex.InnerException);
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }
    }
}
