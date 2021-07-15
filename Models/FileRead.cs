using System;
using System.Collections.Generic;
using System.Text;

namespace CsvToSqlConverter.Models
{
    public class FileRead
    {
        public int Rows { get; set; }
        public IEnumerable<string> Content { get; set; }
    }
}
