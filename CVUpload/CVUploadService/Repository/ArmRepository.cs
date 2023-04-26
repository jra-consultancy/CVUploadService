using CVUploadService.Model;
using CVUploadService.Service;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static CVUploadService.Model.Keys;

namespace CVUploadService
{


    public class ArmRepository : IArmRepository
    {
        private ConnectionDb _connectionDB;
        Logger4net log;

        //private readonly ILogger _logger;
        private string temTableNamePrefix1 = "TMP_RAW_";
        private string temTableNamePrefix2 = "TMP_";
        private string schemaName = "dbo.";
        private string UploadTimeInterval = "";
        private string UploadQueue = "";
        private string UploadCompletePath = "";
       // private string UploadLogFile = "";
        private string defaultSchema = "dbo.";
        private string CvVersion = "CvUploader_version";
        private string CvVersionTime = "CvUploader_InstalledDate";
        private string headerType = "Import";
       
        public ArmRepository()
        {
            //_logger = Logger.GetInstance;

            _connectionDB = new ConnectionDb();
            log = new Logger4net();
            //UploadLogFile = GetFileLocation(3);
        }

        public int AddBulkData(DataTable dt, string tableName)
        {
            try
            {
                DataTable dtSource = new DataTable();
                string sourceTableQuery = "Select top 1 * from [" + temTableNamePrefix1 + tableName + "]";
                using (SqlCommand cmd = new SqlCommand(sourceTableQuery, _connectionDB.con))
                {
                    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                    {
                        da.Fill(dtSource);
                    }
                }
                using (SqlBulkCopy bulk = new SqlBulkCopy(_connectionDB.con) { DestinationTableName = "[" + temTableNamePrefix1 + tableName + "]", BatchSize = 500000000, BulkCopyTimeout = 0 })
                {

                    for (int i = 0; i < dt.Columns.Count; i++)
                    {
                        string destinationColumnName = dt.Columns[i].ToString();

                        // check if destination column exists in source table 
                        // Contains method is not case sensitive    
                        if (dtSource.Columns.Contains(destinationColumnName))
                        {
                            //Once column matched get its index
                            int sourceColumnIndex = dtSource.Columns.IndexOf(destinationColumnName);

                            string sourceColumnName = dtSource.Columns[sourceColumnIndex].ToString();

                            // give column name of source table rather then destination table 
                            // so that it would avoid case sensitivity
                            bulk.ColumnMappings.Add(sourceColumnName, sourceColumnName);
                        }
                    }
                    _connectionDB.con.Open();
                    bulk.WriteToServer(dt);
                    dt.Clear();
                    dt.Dispose();
                    _connectionDB.con.Close();
                }
                //using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(_connectionDB.con))
                //{
                //    //Set the database table name.  
                //    sqlBulkCopy.DestinationTableName = temTableNamePrefix + tableName;
                //    sqlBulkCopy.BulkCopyTimeout = 0;
                //    _connectionDB.con.Open();
                //    sqlBulkCopy.WriteToServer(dt);
                //    _connectionDB.con.Close();
                //}

                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("AddBulkData Exception: " + ex.Message + " Table Name/FileName " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("AddBulkData Exception: : Table Name/FileName " + tableName + ex.Message + ex.InnerException, "AddBulkData");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }


        }
        public int AddBulkDataForLargeFile(string path, string tableName)
        {
            try
            {
                //DataTable dtSource = new DataTable();
                //string sourceTableQuery = "Select top 1 * from [" + temTableNamePrefix1 + tableName + "]";
                //using (SqlCommand cmd = new SqlCommand(sourceTableQuery, _connectionDB.con))
                //{
                //    using (SqlDataAdapter da = new SqlDataAdapter(cmd))
                //    {
                //        da.Fill(dtSource);
                //    }
                //}
                _connectionDB.con.Open();
                using (SqlBulkCopy bulk = new SqlBulkCopy(_connectionDB.con) { DestinationTableName = "[" + temTableNamePrefix1 + tableName + "]", BatchSize = 500000000, BulkCopyTimeout = 0 })
                {
                    bulk.DestinationTableName = "[" + temTableNamePrefix1 + tableName + "]";

                    using (StreamReader sr = new StreamReader(path))
                    {
                        // Read the headers from the CSV file
                        string[] headers = sr.ReadLine().Split(',');

                        // Create the DataTable to hold the data
                        DataTable dataTable = new DataTable();
                        foreach (string header in headers)
                        {
                            dataTable.Columns.Add(header);
                        }

                        // Read the data from the CSV file in chunks and insert it into the database
                        while (!sr.EndOfStream)
                        {
                            for (int i = 0; i < 1000; i++)
                            {
                                if (sr.EndOfStream)
                                {
                                    break;
                                }
                                string[] rows = sr.ReadLine().Split(',');
                                DataRow dr = dataTable.NewRow();
                                for (int j = 0; j < headers.Length; j++)
                                {
                                    dr[j] = rows[j];
                                }
                                dataTable.Rows.Add(dr);
                            }

                            bulk.WriteToServer(dataTable);
                            dataTable.Clear();
                        }
                    }
                }
                    _connectionDB.con.Close();
                //using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(_connectionDB.con))
                //{
                //    //Set the database table name.  
                //    sqlBulkCopy.DestinationTableName = temTableNamePrefix + tableName;
                //    sqlBulkCopy.BulkCopyTimeout = 0;
                //    _connectionDB.con.Open();
                //    sqlBulkCopy.WriteToServer(dt);
                //    _connectionDB.con.Close();
                //}

                    return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("AddBulkData Exception: " + ex.Message + " Table Name/FileName " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("AddBulkDataForLargeFile Exception: Table Name/FileName " + tableName + ex.Message + ex.InnerException, "AddBulkDataForLargeFile");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }


        }

        public int CheckTableExists(string Tablename)
        {
            int tableExist;
            //string query = "SELECT COUNT(*) FROM [FileStore] WHERE [FileName] = @TableName";
            string query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @TableName";
            try
            {
                using (SqlCommand cmd = new SqlCommand(query, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@TableName", temTableNamePrefix1 + Tablename);

                    _connectionDB.con.Open();
                    tableExist = (int)cmd.ExecuteScalar();
                    _connectionDB.con.Close();
                }
                return tableExist;
            }
            catch (Exception ex)
            {
                //_logger.Log("CheckTableExists Exception: " + ex.Message + " Table Name/FileName: " + Tablename, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("CheckTableExists Exception: :  Table Name/FileName: " + Tablename + ex.Message + ex.InnerException, "CheckTableExists");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }

        }

        public int SaveFile(FileStore file)
        {
            string query = "INSERT INTO FileStore (FileName, ExecutionTime, Status,TableName) " +
                   "VALUES (@FileName, @ExecutionTime, @Status,@TableName) ";

            try
            {
                using (SqlCommand cmd = new SqlCommand(query, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@FileName", file.FileName);
                    cmd.Parameters.AddWithValue("@ExecutionTime", file.ExecutionTime);
                    cmd.Parameters.AddWithValue("@Status", file.Status);
                    cmd.Parameters.AddWithValue("@TableName", file.TableName);



                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("SaveFile Exception: " + ex.Message + " Table Name/FileName: " + file.FileName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("SaveFile Exception: : Table Name / FileName: " + file.FileName + ex.Message + ex.InnerException, "SaveFile");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public int SchemeCreate(string schema)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(schema, _connectionDB.con))
                {

                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("SchemeCreate Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("SchemeCreate Exception: :" + ex.Message + ex.InnerException, "SchemeCreate");
                //_connectionDB.con.Close();
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public int TruncateTable(string TableName, string tablePrefix)
        {
            string tableName = tablePrefix + TableName;
            //string query = "truncate table @TableName";
            string strTruncateTable = "TRUNCATE TABLE [" + tableName + "]";


            try
            {
                using (SqlCommand cmd = new SqlCommand(strTruncateTable, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("TruncateTable Exception: " + ex.Message + " Table Name/FileName: " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("TruncateTable Exception: : Table Name / FileName: " + tableName + ex.Message + ex.InnerException, "TruncateTable");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public string GetFileLocation(int Key)
        {
            //use condition for key and set property 
            string propertyName = "";
            if (Key == 0)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 0);
            }

            else if (Key == 1)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 1);
            }
            else if (Key == 2)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 2);
            }
            else if (Key == 3)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 3);

            }
            else if (Key == 4)
            {
                propertyName = Enum.GetName(typeof(KeyNames), 4);

            }


            string location = "";
            //string sourceTableQuery = "Select PropertyValue from [SystemGlobalProperties] WHERE [PropertyName] = @propertyName";
            string sourceTableQuery = "select [dbo].[fnGlobalProperty](@propertyName) AS PropertyValue";

            try
            {
                _connectionDB.con.Open();
                using (SqlCommand cmd = new SqlCommand(sourceTableQuery, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@propertyName", propertyName);

                    //var dr = cmd.ExecuteReader();
                    location = (string)cmd.ExecuteScalar();

                    //if (dr.Read()) // Read() returns TRUE if there are records to read, or FALSE if there is nothing
                    //{
                    //    location = dr["PropertyValue"].ToString();

                    //}

                }
                _connectionDB.con.Close();
                return location;
            }
            catch (Exception ex)
            {
                using (EventLog eventLog = new EventLog("Application"))
                {
                    eventLog.Source = "Application";
                    eventLog.WriteEntry("CV Upload Service Error Messege: " + ex.Message, EventLogEntryType.Error, 999, 1);
                }
                //_logger.Log("GetFileLocation Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("GetFileLocation Exception: :" + ex.Message + ex.InnerException, "GetFileLocation");

                throw ex;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }

        }

        public string GetSqlFromMappingConfig(string key)
        {
            try
            {

                string sql = "";
                string query = "Select SQL from [MapperConfiguration] WHERE [SourceTable] = @sourceTable AND IsActive = 1";
                using (SqlCommand cmd = new SqlCommand(query, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@sourceTable", "dbo." + temTableNamePrefix1 + key);
                    _connectionDB.con.Open();
                    var dr = cmd.ExecuteReader();
                    if (dr.Read()) // Read() returns TRUE if there are records to read, or FALSE if there is nothing
                    {
                        sql = dr["SQL"].ToString();

                    }
                    _connectionDB.con.Close();

                }
                return sql;
            }
            catch (Exception ex)
            {
                //_logger.Log("GetSqlFromMappingConfig Exception: " + ex.Message + " Table Name/FileName: " + temTableNamePrefix1 + key, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("GetSqlFromMappingConfig Exception: Table Name/FileName: " + temTableNamePrefix1 + key + ex.Message + ex.InnerException, "GetSqlFromMappingConfig");

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

        public int InsertDestinationTable(string insertSql)
        {
            try
            {

                using (SqlCommand cmd = new SqlCommand(insertSql, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("InsertDestinationTable Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("InsertDestinationTable Exception: :" + ex.Message + ex.InnerException, "InsertDestinationTable");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public string GetDestinationTableName(string sourceTableName)
        {
            try
            {
                string destinationTableName = "";
                string sql = "SELECT DISTINCT [DestinationTable] FROM [dbo].[MapperConfiguration] WHERE [SourceTable] = @sourceTableName";


                using (SqlCommand cmd = new SqlCommand(sql, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.Parameters.AddWithValue("@sourceTableName", defaultSchema + sourceTableName);

                    destinationTableName = (string)cmd.ExecuteScalar();

                    _connectionDB.con.Close();
                }
                return destinationTableName;

            }
            catch (Exception ex)
            {
                //_logger.Log("GetDestinationTableName Exception: " + ex.Message + " Table Name/FileName: " + defaultSchema + sourceTableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("GetDestinationTableName Exception: : Table Name/FileName: " + defaultSchema + sourceTableName + ex.Message + ex.InnerException, "GetDestinationTableName");
                //throw ex;
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

        public int TruncateTable(string TableName)
        {
            string strTruncateTable = "TRUNCATE TABLE " + TableName;


            try
            {
                using (SqlCommand cmd = new SqlCommand(strTruncateTable, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
                return 1;
            }
            catch (Exception ex)
            {
                //_logger.Log("TruncateTable Exception: " + ex.Message + " Table Name/FileName: " + TableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("TruncateTable2 Exception: Table Name/FileName: " + TableName + ex.Message + ex.InnerException, "TruncateTable2");
                //throw ex;
                return -1;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }
        public void InsertVersionNoIfNotFound(string versionNo)
        {
            string sourceTableQuery = "select [dbo].[fnGlobalProperty](@propertyName) AS PropertyValue";

            string version;
            try
            {
                _connectionDB.con.Open();
                using (SqlCommand cmd = new SqlCommand(sourceTableQuery, _connectionDB.con))
                {
                    cmd.Parameters.AddWithValue("@propertyName", CvVersion);
                    object result = cmd.ExecuteScalar();
                    if (result == DBNull.Value || result == "")
                    {
                        version = null;
                    }
                    else
                    {
                        version = (string)result;
                    }
                }
                _connectionDB.con.Close();
                if (String.IsNullOrEmpty(version) || version != versionNo)
                {
                    string sql = "UPDATE [dbo].[SystemGlobalProperties] SET PropertyValue = @versionNo WHERE PropertyName = @propertyName";
                    _connectionDB.con.Open();
                    using (SqlCommand cmd = new SqlCommand(sql, _connectionDB.con))
                    {
                        cmd.Parameters.AddWithValue("@versionNo", versionNo);
                        cmd.Parameters.AddWithValue("@propertyName", CvVersion);
                        cmd.ExecuteNonQuery();
                    }
                    _connectionDB.con.Close();

                    string sql2 = "UPDATE [dbo].[SystemGlobalProperties] SET PropertyValue = @datetime WHERE PropertyName = @propertyName";
                    _connectionDB.con.Open();
                    using (SqlCommand cmd = new SqlCommand(sql2, _connectionDB.con))
                    {
                        cmd.Parameters.AddWithValue("@datetime", DateTime.Now.ToString("yyyy-MM-dd HH:mm"));
                        cmd.Parameters.AddWithValue("@propertyName", CvVersionTime);
                        cmd.ExecuteNonQuery();
                    }
                    _connectionDB.con.Close();
                }
            }
            catch (Exception ex)
            {
                using (EventLog eventLog = new EventLog("Application"))
                {
                    eventLog.Source = "Application";
                    eventLog.WriteEntry("Harvest Service Error Messege: " + ex.Message, EventLogEntryType.Error, 999, 1);
                }
                //_logger.Log("GetFileLocation Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("GetFileLocation Exception: :" + ex.Message + ex.InnerException, "GetFileLocation");

                throw ex;
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public List<(int,string)> GetHeaderInformation()
        {
            List<(int id, string header)> columnHeaders = new List<(int, string)>();

            try
            {
                string sql = "SELECT ConnectorID,Param1 FROM A_Connector WHERE type = @type";


                using (SqlCommand cmd = new SqlCommand(sql, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.Parameters.AddWithValue("@type", headerType);
                    SqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        // columnHeaders.Add(((int)reader["ConnectorID"], (string)reader["Param1"]));

                        int connectorID = 0;
                        string param1="";

                        if (!DBNull.Value.Equals(reader["ConnectorID"]))
                        {
                            connectorID = (int)reader["ConnectorID"];
                        }

                        if (!DBNull.Value.Equals(reader["Param1"]))
                        {
                            param1 = (string)reader["Param1"];
                        }

                        columnHeaders.Add((connectorID, param1));
                    }
                    reader.Close();

                    _connectionDB.con.Close();
                }
                return columnHeaders;

            }
            catch (Exception ex)
            {
                //_logger.Log("GetHeaderInformation Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("GetHeaderInformation Exception: :" + ex.Message + ex.InnerException, "GetHeaderInformation");
                throw ex;
                
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public string UpdateOdataJson(string jsonData,int id)
        {
            string sql = "UPDATE [dbo].[Odata_json] SET jsonValue = @json WHERE connectorID = @id; SELECT dataUpdateSql FROM [dbo].[Odata_json] WHERE connectorID = @id;";
            string dataUpdateSql = "";
            try
            {
                using (SqlCommand cmd = new SqlCommand(sql, _connectionDB.con))
                {

                    _connectionDB.con.Open();
                    cmd.Parameters.AddWithValue("@json", jsonData);
                    cmd.Parameters.AddWithValue("@id", id);
                    object result = cmd.ExecuteScalar();
                    if (result != DBNull.Value)
                    {
                        dataUpdateSql = (string)result;
                    }
                    _connectionDB.con.Close();
                }
                return dataUpdateSql;

            }
            catch (Exception ex)
            {
                //_logger.Log("UpdateOdataJson Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("UpdateOdataJson Exception: :" + ex.Message + ex.InnerException, "UpdateOdataJson");
                throw ex;
                
            }
            finally
            {
                if (_connectionDB.con.State == System.Data.ConnectionState.Open)
                {
                    _connectionDB.con.Close();
                }
            }
        }

        public void ExecuteSql(string sql)
        {
            try
            {
                using (SqlCommand cmd = new SqlCommand(sql, _connectionDB.con))
                {
                    _connectionDB.con.Open();
                    cmd.ExecuteNonQuery();
                    _connectionDB.con.Close();
                }
            }
            catch (Exception ex)
            {
                //_logger.Log("ExecuteSql Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                log.PushLog("ExecuteSql Exception: :" + ex.Message + ex.InnerException, "ExecuteSql");
                throw ex;
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
