using CVUploadService.Model;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CVUploadService
{
    public interface IArmRepository
    {
        int SchemeCreate(string schema);
        int AddBulkData(DataTable dt,string tableName);
        int SaveFile(FileStore file);
        int CheckTableExists(string Tablename);
        int TruncateTable(string TableName,string tablePrefix);
        string GetFileLocation(int Key);
        string GetSqlFromMappingConfig(string key);
        int InsertDestinationTable(string insertSql);
        string GetDestinationTableName(string sourceTableName);
        int TruncateTable(string TableName);
        int AddBulkDataForLargeFile(string path, string v);
        void InsertVersionNoIfNotFound(string versionNo);
        List<(int,string)> GetHeaderInformation();
        string UpdateOdataJson(string jsonData,int id);
        void ExecuteSql(string sql);

    }
}
