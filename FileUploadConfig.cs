using System;
using System.Collections.Generic;
using System.Text;

namespace CsvToSqlConverter
{
    public class FileUploadConfig    {

        public string DatabaseName { get; set; }
        public string TableName { get; set; }

        public string FolderName { get; set; }

        public string FileName { get; set; }

        public string DefaultFieldType { get; set; } = "NVARCHAR(125)";

        public bool AddEmptyField { get; set; }
        public bool IncludeDropCreateTable { get; set; }


        public string CompleteFileName { get { return $"{FolderName}\\{FileName}";  } }
    }
}