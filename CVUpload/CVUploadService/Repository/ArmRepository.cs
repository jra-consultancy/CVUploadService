using CVUploadService.Model;
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
        private readonly ILogger _logger;
        private string temTableNamePrefix1 = "TMP_RAW_";
        private string temTableNamePrefix2 = "TMP_";
        private string schemaName = "dbo.";
        private string UploadTimeInterval = "";
        private string UploadQueue = "";
        private string UploadCompletePath = "";
        private string UploadLogFile = "";
        private string defaultSchema = "dbo.";
        public ArmRepository()
        {
            _logger = Logger.GetInstance;

            _connectionDB = new ConnectionDb();
            UploadLogFile = GetFileLocation(3);
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
                _logger.Log("AddBulkData Exception: " + ex.Message + " Table Name/FileName " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("AddBulkData Exception: " + ex.Message + " Table Name/FileName " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("CheckTableExists Exception: " + ex.Message + " Table Name/FileName: " + Tablename, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("SaveFile Exception: " + ex.Message + " Table Name/FileName: " + file.FileName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("SchemeCreate Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("TruncateTable Exception: " + ex.Message + " Table Name/FileName: " + tableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("GetFileLocation Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

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
                _logger.Log("GetSqlFromMappingConfig Exception: " + ex.Message + " Table Name/FileName: " + temTableNamePrefix1 + key, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

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
                _logger.Log("InsertDestinationTable Exception: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("GetDestinationTableName Exception: " + ex.Message + " Table Name/FileName: " + defaultSchema + sourceTableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
                _logger.Log("TruncateTable Exception: " + ex.Message + " Table Name/FileName: " + TableName, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
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
    }
}
