using CVUploadService.Model;
using Aspose.Cells;
using CsvHelper;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;

namespace CVUploadService
{
    public class FileParser
    {
        //private readonly string UploadQueue = @"" + ConfigurationManager.AppSettings["armFilePath"];
        //private readonly string UploadCompletePath = @"" + ConfigurationManager.AppSettings["armFileCompletePath"];

        private readonly IArmService _iArmService;
        private IArmRepository _iArmRepo;
        private readonly ILogger _logger;
        private string UploadQueue = "";
        private string UploadCompletePath = "";
        private string temTableNamePrefix1 = "TMP_RAW_";
        private string temTableNamePrefix2 = "TMP_";
        private string UploadLogFile = "";
        private string RejectedFile = "";
        private string serviceName = "CV Upload Service";

        public FileParser()
        {
            _iArmService = new ArmService();
            _iArmRepo = new ArmRepository();
            _logger = Logger.GetInstance;
            UploadLogFile = _iArmRepo.GetFileLocation(3);
        }
        public Dictionary<string, Stream> FileRead()
        {
            try
            {
                var streamList = new Dictionary<string, Stream>();
                foreach (string txtName in Directory.GetFiles(UploadQueue))
                {
                    streamList.Add(Path.GetFileName(txtName), new StreamReader(txtName).BaseStream);
                }
                return streamList;
            }
            catch (Exception ex)
            {
                _logger.Log("FileRead Exception :" + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                return null;
            }
        }
        private static readonly object Mylock = new object();
        public void FileParse(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (!Monitor.TryEnter(Mylock, 0)) return;
            try
            {
                string version = GetServiceVersion(serviceName);
                _iArmRepo.InsertVersionNoIfNotFound(version);
                string isValid = "";
                UploadQueue = _iArmRepo.GetFileLocation(1);
                if (!UploadQueue.EndsWith("\\"))
                {
                    UploadQueue = UploadQueue + "\\";
                }
                if (!Directory.Exists(UploadQueue))
                    Directory.CreateDirectory(UploadQueue);

                UploadCompletePath = _iArmRepo.GetFileLocation(2);
                if (!UploadCompletePath.EndsWith("\\"))
                {
                    UploadCompletePath = UploadCompletePath + "\\";
                }
                if (!Directory.Exists(UploadCompletePath))
                    Directory.CreateDirectory(UploadCompletePath);

                RejectedFile = _iArmRepo.GetFileLocation(4);
                if (!RejectedFile.EndsWith("\\"))
                {
                    RejectedFile = RejectedFile + "\\";
                }
                if (!Directory.Exists(RejectedFile))
                    Directory.CreateDirectory(RejectedFile);

                var stringData = FileRead();

                foreach (var file in stringData)
                {
                    string path = UploadQueue + file.Key;
                    isValid = _iArmService.IsValidFile(path);
                    if (isValid == "" || isValid == string.Empty)
                    {
                        DataTable dt = GetFileData(file.Key, file.Value);
                        _logger.Log("File converted to Datatable Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                        if (dt != null)
                        {

                            int isExists = _iArmRepo.CheckTableExists(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                            _logger.Log("Check Table Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                            if (isExists > 0)
                            {
                                _logger.Log("Table Already Exsist. Insert In If Condition!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                var result = _iArmRepo.TruncateTable(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")), temTableNamePrefix1);
                                _logger.Log("Truncate Table Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                if (result == 1)
                                {
                                    result = _iArmRepo.AddBulkData(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                    _logger.Log("Insert Bulk Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                    if (result == 1)
                                    {
                                        createFileStore(file);
                                        _logger.Log("Insert FileStore Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        string insertSql = GetSQLFromMapping(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                        _logger.Log("Get Sql Mapping Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        if (insertSql != "")
                                        {
                                            string destinationTableName = _iArmRepo.GetDestinationTableName(temTableNamePrefix1 + Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                            _logger.Log("Get Destination Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                            if (destinationTableName != "")
                                            {
                                                result = _iArmRepo.TruncateTable(destinationTableName);
                                                _logger.Log("Destination Table Data Truncate Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                if (result == 1)
                                                {
                                                    result = _iArmRepo.InsertDestinationTable(insertSql);
                                                    _logger.Log("Destination Table Data Insert Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                }
                                            }
                                        }
                                    }

                                }

                            }
                            else if (isExists == -1) break;
                            else
                            {
                                _logger.Log("Table not Exsist. Insert In else Condition!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                string createTableSQL = BuildCreateTableScript(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")), temTableNamePrefix1);
                                if (createTableSQL == null)
                                    return;
                                var result = _iArmRepo.SchemeCreate(createTableSQL);
                                _logger.Log("Schema Created Successfully!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                if (result == 1)
                                {
                                    _iArmRepo.AddBulkData(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                    _logger.Log("Bulk Data Insert Successfully!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                    if (result == 1)
                                    {
                                        createFileStore(file);
                                        _logger.Log("Insert FileStore Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        string insertSql = GetSQLFromMapping(file.Key.Replace(" ", "_"));
                                        _logger.Log("Get Sql Mapping Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        if (insertSql != "")
                                        {
                                            string destinationTableName = _iArmRepo.GetDestinationTableName(temTableNamePrefix1 + Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                            _logger.Log("Get Destination Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                            if (destinationTableName != "")
                                            {
                                                result = _iArmRepo.TruncateTable(destinationTableName);
                                                _logger.Log("Destination Table Data Truncate Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                if (result == 1)
                                                {
                                                    result = _iArmRepo.InsertDestinationTable(insertSql);
                                                    _logger.Log("Destination Table Data Insert Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                }
                                            }
                                        }
                                    }
                                }

                            }
                            dt.Clear();
                            dt.Dispose();
                        }
                        else
                        {
                            file.Value.Close();
                            RemoveFilesFromFolder(file);
                            DeleteFilesFromFolder(file);
                        }
                    }
                    else
                    {
                        file.Value.Close();
                        _logger.Log("File Type Exception :" + isValid, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                        RemoveFilesFromFolder(file);
                        DeleteFilesFromFolder(file);
                    }
                }

                RemoveFilesFromFolder(stringData);
                DeleteFilesFromFolder(stringData);
            }
            catch (Exception ex)
            {
                _logger.Log("File Parser :" + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

            }
            finally
            {
                Monitor.Exit(Mylock);
            }


        }

        private void DeleteFilesFromFolder(KeyValuePair<string, Stream> file)
        {
            try
            {
                File.Delete(UploadQueue + file.Key);
            }
            catch (IOException e)
            {
                Debug.WriteLine(e.Message);
            }
        }

        private void RemoveFilesFromFolder(KeyValuePair<string, Stream> file)
        {
            string moveTo = "";

            string fileToMove = UploadQueue + Path.GetFileName(file.Key);
            moveTo = RejectedFile + Path.GetFileNameWithoutExtension(file.Key) + DateTime.Now.ToString("ddMMyy") + Path.GetExtension(file.Key);

            //moving file
            File.Copy(fileToMove, moveTo, true);
        }

        private string GetSQLFromMapping(string key)
        {
            string sql = "";
            sql = _iArmRepo.GetSqlFromMappingConfig(key);
            sql = sql.Replace("\r", " ").Replace("\n", " ");
            return sql;
        }

        public void FileParse()
        {
            try
            {
                DataTable dt;
                string isValid = "";
                UploadQueue = _iArmRepo.GetFileLocation(1);
                if (!UploadQueue.EndsWith("\\"))
                {
                    UploadQueue = UploadQueue + "\\";
                }
                if (!Directory.Exists(UploadQueue))
                    Directory.CreateDirectory(UploadQueue);

                UploadCompletePath = _iArmRepo.GetFileLocation(2);
                if (!UploadCompletePath.EndsWith("\\"))
                {
                    UploadCompletePath = UploadCompletePath + "\\";
                }
                if (!Directory.Exists(UploadCompletePath))
                    Directory.CreateDirectory(UploadCompletePath);

                RejectedFile = _iArmRepo.GetFileLocation(4);
                if (!RejectedFile.EndsWith("\\"))
                {
                    RejectedFile = RejectedFile + "\\";
                }
                if (!Directory.Exists(RejectedFile))
                    Directory.CreateDirectory(RejectedFile);

                var stringData = FileRead();

                foreach (var file in stringData)
                {
                    string path = UploadQueue + file.Key;
                    isValid = _iArmService.IsValidFile(path);
                    if (isValid == "" || isValid == string.Empty)
                    {
                        if (new System.IO.FileInfo(path).Length > 1000000000)
                        {
                            dt = GetDataTableWithHeader(path);
                        }
                        else
                        {
                            dt = GetFileData(file.Key, file.Value);
                        }
                        //
                        _logger.Log("File converted to Datatable Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                        if (dt != null)
                        {

                            int isExists = _iArmRepo.CheckTableExists(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                            _logger.Log("Check Table Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                            if (isExists > 0)
                            {
                                _logger.Log("Table Already Exsist. Insert In If Condition!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                var result = _iArmRepo.TruncateTable(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")), temTableNamePrefix1);
                                _logger.Log("Truncate Table Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                if (result == 1)
                                {
                                    if (new System.IO.FileInfo(path).Length > 1000000000)
                                    {
                                        //dt = GetDataTableWithHeader(path);
                                        result = _iArmRepo.AddBulkDataForLargeFile(path, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                    }
                                    else
                                    {
                                        result = _iArmRepo.AddBulkData(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                        _logger.Log("Bulk Data Insert Successfully!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                                    }

                                    if (result == 1)
                                    {
                                        createFileStore(file);
                                        _logger.Log("Insert FileStore Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        string insertSql = GetSQLFromMapping(Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                        _logger.Log("Get Sql Mapping Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        if (insertSql != "")
                                        {
                                            string destinationTableName = _iArmRepo.GetDestinationTableName(temTableNamePrefix1 + Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                            _logger.Log("Get Destination Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                            if (destinationTableName != "")
                                            {
                                                result = _iArmRepo.TruncateTable(destinationTableName);
                                                _logger.Log("Destination Table Data Truncate Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                if (result == 1)
                                                {
                                                    result = _iArmRepo.InsertDestinationTable(insertSql);
                                                    _logger.Log("Destination Table Data Insert Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                }
                                            }
                                        }
                                    }
                                    else
                                    {
                                        _logger.Log("Bulk Data Insert Failed!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                                    }

                                }

                            }
                            else if (isExists == -1) break;
                            else
                            {
                                _logger.Log("Table not Exsist. Insert In else Condition!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                string createTableSQL = BuildCreateTableScript(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")), temTableNamePrefix1);
                                if (createTableSQL == null)
                                    return;
                                var result = _iArmRepo.SchemeCreate(createTableSQL);
                                _logger.Log("Schema Created Successfully!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                if (result == 1)
                                {
                                    if (new System.IO.FileInfo(path).Length > 1000000000)
                                    {
                                        dt = GetDataTableWithHeader(path);
                                        _iArmRepo.AddBulkDataForLargeFile(path, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                    }
                                    else
                                    {
                                        _iArmRepo.AddBulkData(dt, Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                        _logger.Log("Bulk Data Insert Successfully!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                                    }

                                    if (result == 1)
                                    {
                                        createFileStore(file);
                                        _logger.Log("Insert FileStore Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        string insertSql = GetSQLFromMapping(file.Key.Replace(" ", "_"));
                                        _logger.Log("Get Sql Mapping Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                        if (insertSql != "")
                                        {
                                            string destinationTableName = _iArmRepo.GetDestinationTableName(temTableNamePrefix1 + Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")));
                                            _logger.Log("Get Destination Data Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                            if (destinationTableName != "")
                                            {
                                                result = _iArmRepo.TruncateTable(destinationTableName);
                                                _logger.Log("Destination Table Data Truncate Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                if (result == 1)
                                                {
                                                    result = _iArmRepo.InsertDestinationTable(insertSql);
                                                    _logger.Log("Destination Table Data Insert Successful!", UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

                                                }
                                            }
                                        }
                                    }
                                }

                            }
                            dt.Clear();
                            dt.Dispose();
                        }
                        else
                        {
                            file.Value.Close();
                            RemoveFilesFromFolder(file);
                            DeleteFilesFromFolder(file);
                        }
                    }
                    else
                    {
                        file.Value.Close();
                        _logger.Log("File Type Exception :" + isValid, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                        RemoveFilesFromFolder(file);
                        DeleteFilesFromFolder(file);
                    }
                }

                RemoveFilesFromFolder(stringData);
                DeleteFilesFromFolder(stringData);
            }
            catch (Exception ex)
            {
                _logger.Log("File Parser :" + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));

            }
            finally
            {
                //Monitor.Exit(Mylock);
            }


        }

        private DataTable GetDataTableWithHeader(string path)
        {
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
                return dataTable;
            }
        }

        private void DeleteFilesFromFolder(Dictionary<string, Stream> stringData)
        {

            foreach (var file in stringData)
            {
                try
                {
                    File.Delete(UploadQueue + file.Key);
                }
                catch (IOException e)
                {
                    Debug.WriteLine(e.Message);
                }
            }

        }

        private void RemoveFilesFromFolder(Dictionary<string, Stream> stringData)
        {

            string[] fileList = System.IO.Directory.GetFiles(UploadQueue);
            string moveTo = "";
            foreach (string file in fileList)
            {

                string fileToMove = UploadQueue + Path.GetFileName(file);
                moveTo = UploadCompletePath + Path.GetFileNameWithoutExtension(file) + DateTime.Now.ToString("ddMMyy") + Path.GetExtension(file);

                //moving file
                File.Copy(fileToMove, moveTo, true);
            }
        }

        private void createFileStore(KeyValuePair<string, Stream> file)
        {
            string tableName = temTableNamePrefix1 + Path.GetFileNameWithoutExtension(file.Key).Replace(" ", "_");
            FileStore xFile = new FileStore
            {
                FileName = Path.GetFileNameWithoutExtension(UploadQueue + file.Key.Replace(" ", "_")),
                ExecutionTime = DateTime.Now,
                Status = true,
                TableName = tableName
            };
            _iArmRepo.SaveFile(xFile);
        }

        public string BuildCreateTableScript(DataTable Table, string tableName, string temTableNamePrefix)
        {
            try
            {
                StringBuilder result = new StringBuilder();


                result.AppendFormat("CREATE TABLE [{0}] ( ", temTableNamePrefix + tableName);

                result.AppendFormat("[{0}] {1} {2} {3} {4}",
                        "ImportID", // 0
                        "[INT] ", // 1
                        "IDENTITY(1,1)",//2
                        "NOT NULL", // 3
                        Environment.NewLine // 4
                    );
                result.Append("   ,");
                bool FirstTime = true;
                foreach (DataColumn column in Table.Columns.OfType<DataColumn>())
                {
                    if (FirstTime) FirstTime = false;
                    else
                        result.Append("   ,");

                    result.AppendFormat("[{0}] {1} {2} {3}",
                        column.ColumnName.Trim(), // 0
                        GetSQLTypeAsString(column.DataType), // 1
                        column.AllowDBNull ? "NULL" : "NOT NULL", // 2
                        Environment.NewLine // 3
                    );
                }
                result.AppendFormat(") ON [PRIMARY]{0}", Environment.NewLine);

                // Build an ALTER TABLE script that adds keys to a table that already exists.
                if (Table.PrimaryKey.Length > 0)
                    result.Append(BuildKeysScript(Table));
                return result.ToString();
            }
            catch (Exception ex)
            {
                _logger.Log("BuildCreateTableScript: " + ex.Message, UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                return null;
            }
        }

        /// <summary>
        /// Builds an ALTER TABLE script that adds a primary or composite key to a table that already exists.
        /// </summary>
        private static string BuildKeysScript(DataTable Table)
        {
            // Already checked by public method CreateTable. Un-comment if making the method public
            // if (Helper.IsValidDatatable(Table, IgnoreZeroRows: true)) return string.Empty;
            if (Table.PrimaryKey.Length < 1) return string.Empty;

            StringBuilder result = new StringBuilder();

            if (Table.PrimaryKey.Length == 1)
                result.AppendFormat("ALTER TABLE {1}{0}   ADD PRIMARY KEY ({2}){0}GO{0}{0}", Environment.NewLine, Table.TableName, Table.PrimaryKey[0].ColumnName);
            else
            {
                List<string> compositeKeys = Table.PrimaryKey.OfType<DataColumn>().Select(dc => dc.ColumnName).ToList();
                string keyName = compositeKeys.Aggregate((a, b) => a + b);
                string keys = compositeKeys.Aggregate((a, b) => string.Format("{0}, {1}", a, b));
                result.AppendFormat("ALTER TABLE {1}{0}ADD CONSTRAINT pk_{3} PRIMARY KEY ({2}){0}GO{0}{0}", Environment.NewLine, Table.TableName, keys, keyName);
            }

            return result.ToString();
        }

        /// <summary>
        /// Returns the SQL data type equivalent, as a string for use in SQL script generation methods.
        /// </summary>
        private static string GetSQLTypeAsString(Type DataType)
        {
            switch (DataType.Name)
            {
                case "Boolean": return "[bit]";
                case "Char": return "[char]";
                case "SByte": return "[tinyint]";
                case "Int16": return "[smallint]";
                case "Int32": return "[int]";
                case "Int64": return "[bigint]";
                case "Byte": return "[tinyint] UNSIGNED";
                case "UInt16": return "[smallint] UNSIGNED";
                case "UInt32": return "[int] UNSIGNED";
                case "UInt64": return "[bigint] UNSIGNED";
                case "Single": return "[float]";
                case "Double": return "[float]";
                case "Decimal": return "[decimal]";
                case "DateTime": return "[datetime]";
                case "Guid": return "[uniqueidentifier]";
                case "Object": return "[variant]";
                case "String": return "[nvarchar](max)";
                default: return "[nvarchar](MAX)";
            }
        }

        private DataTable GetFileData(string key, Stream value)
        {
            DataTable dt = new DataTable();
            try
            {

                if (Path.GetExtension(key) == ".csv")
                {
                    //return CSVToDataTable(UploadQueue + key);

                    //dt = CSVtoDataTable(UploadQueue + key);
                    dt = GetDataTableFromCSVFile(UploadQueue + key);
                    foreach (DataColumn col in dt.Columns)
                    {
                        col.ColumnName = col.ColumnName.Trim();
                    }
                    value.Close();
                    return dt;
                }
                else if (Path.GetExtension(key) == ".xlsx")
                {
                    using (var package = new ExcelPackage(value))
                    {

                        Workbook workbook = new Workbook(value);
                        Worksheet worksheet = workbook.Worksheets[0];
                        //worksheet
                        dt = worksheet.Cells.ExportDataTable(0, 0, worksheet.Cells.MaxDataRow + 1, worksheet.Cells.MaxDataColumn + 1, true);
                        foreach (DataColumn col in dt.Columns)
                        {
                            col.ColumnName = col.ColumnName.Trim();
                        }
                        value.Close();
                        return dt;

                    }
                }
                else
                {
                    StreamReader reader = new StreamReader(UploadQueue + key);
                    string line = reader.ReadLine();

                    DataRow row;
                    string[] txtValue = line.Split(',');

                    foreach (string dc in txtValue)
                    {
                        dt.Columns.Add(new DataColumn(dc));
                    }

                    while (!reader.EndOfStream)
                    {
                        txtValue = reader.ReadLine().Split(',');

                        if (txtValue.Length == dt.Columns.Count)
                        {
                            row = dt.NewRow();
                            row.ItemArray = txtValue;
                            dt.Rows.Add(row);
                        }

                    }
                    return dt;
                }
            }
            catch (Exception ex)
            {
                _logger.Log("Bad File: " + ex.Message.ToString(), @"" + UploadLogFile.Replace("DDMMYY", DateTime.Now.ToString("ddMMyy")));
                value.Close();
                return dt = null;
            }

        }
        private static DataTable GetDataTableFromCSVFile(string filePath)
        {
            try
            {
                DataTable dataTable = new DataTable();
                using (StreamReader sr = new StreamReader(filePath))
                {
                    string[] headers = sr.ReadLine().Split(',');
                    foreach (string header in headers)
                    {
                        dataTable.Columns.Add(header);
                    }
                    while (!sr.EndOfStream)
                    {
                        string[] rows = sr.ReadLine().Split(',');
                        DataRow dr = dataTable.NewRow();
                        for (int i = 0; i < headers.Length; i++)
                        {
                            dr[i] = rows[i];
                        }
                        dataTable.Rows.Add(dr);
                    }
                }
                return dataTable;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public DataTable CSVtoDataTable(string inputpath)
        {

            DataTable csvdt = new DataTable();
            string Fulltext;
            if (File.Exists(inputpath))
            {
                //StreamReader sr = new StreamReader(inputpath);
                using (var reader = new StreamReader(inputpath))
                using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
                {
                    // Do any configuration to `CsvReader` before creating CsvDataReader.
                    using (var dr = new CsvDataReader(csv))
                    {
                        //var dt = new DataTable();
                        csvdt.Load(dr);
                    }
                }
                //while (!sr.EndOfStream)
                //{
                //    Fulltext = sr.ReadToEnd().ToString();//read full content
                //    string[] rows = Fulltext.Split('\n');//split file content to get the rows
                //    for (int i = 0; i < rows.Count() - 1; i++)
                //    {
                //        var regex = new Regex("\\\"(.*?)\\\"");
                //        var output = regex.Replace(rows[i], m => m.Value.Replace(",", "\\c"));//replace commas inside quotes
                //        string[] rowValues = output.Split(',');//split rows with comma',' to get the column values
                //        {
                //            if (i == 0)
                //            {
                //                for (int j = 0; j < rowValues.Count(); j++)
                //                {
                //                    csvdt.Columns.Add(rowValues[j].Replace("\\c", ",").Trim());//headers
                //                }

                //            }
                //            else
                //            {
                //                try
                //                {
                //                    DataRow dr = csvdt.NewRow();
                //                    for (int k = 0; k < rowValues.Count(); k++)
                //                    {
                //                        if (k >= dr.Table.Columns.Count)// more columns may exist
                //                        {
                //                            csvdt.Columns.Add("clmn" + k);
                //                            dr = csvdt.NewRow();
                //                        }
                //                        dr[k] = rowValues[k].Replace("\\c", ",").Trim();

                //                    }
                //                    csvdt.Rows.Add(dr);//add other rows
                //                }
                //                catch
                //                {
                //                    Console.WriteLine("error");
                //                }
                //            }
                //        }
                //    }
                //}
                //sr.Close();

            }
            return csvdt;
        }
        private string GetServiceVersion(string serviceName)
        {

            // Get the service controller for the specified service name
            Version serviceVersion = Assembly.GetExecutingAssembly().GetName().Version;
            return serviceVersion.ToString();
        }
    }
}
