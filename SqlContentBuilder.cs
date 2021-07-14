using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;

namespace CsvToSqlConverter
{
    public class SqlContentBuilder
    {
        private static int batchNbr = 750;

        public string BuildCreateTable(FileUploadConfig config)
        {
            string MyFile = $"{config.FolderName}\\{config.FileName}";

            StringBuilder SqlStatement = new StringBuilder();
                      
            string line1 = File.ReadLines(MyFile).First();
            string TableFields = GetFields(line1, config.AddEmptyField);

            if (config.IncludeDropCreateTable)
            {
                SqlStatement.Append(DropCreateTableStatement(config, TableFields));
            }

            SqlStatement.Append(ParseContent(MyFile, config.DatabaseName, config.TableName, TableFields));

            return SqlStatement.ToString();
        }

        private static string DropCreateTableStatement(FileUploadConfig config, string tableFields)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine($"USE {config.DatabaseName}");
            sb.AppendLine("GO");
            sb.AppendLine(" ");

            sb.AppendLine($"IF EXISTS(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{config.TableName}') DROP TABLE [{config.TableName}];");

            sb.AppendLine("CREATE TABLE [" + config.TableName + "] ");
            sb.AppendLine("(");
            sb.AppendLine(ParseFields(tableFields, config.DefaultFieldType));
            sb.AppendLine(")");
            sb.AppendLine("GO");

            sb.AppendLine(" ");

            return sb.ToString();
        }

        private static string GetFields(string firstLine, bool addEmptyColumns)
        {
            StringBuilder Flds = new StringBuilder();

            var flds = firstLine.Split(',');
            int counter = 0;

            foreach (var i in flds)
            {
                if (string.IsNullOrEmpty(i))
                {
                    if (addEmptyColumns)
                    {
                        Flds.Append($"[empty_field{counter}],");
                    }
                }
                else
                {
                    string newName = i.ToLower().Replace(" ", "_").Replace("-", "_");

                    newName = newName.Replace("%", "_percent_");
                    newName = newName.Replace("\"", "_");
                    newName = newName.Replace("/", "_");
                    newName = newName.Replace("$", "_dollar_");
                    newName = newName.Replace("&", "_and_");
                    newName = newName.Replace("(", "_");
                    newName = newName.Replace(")", "_");
                    newName = newName.Replace("#", "_");
                    newName = newName.Replace("+", "_plus_");

                    if (newName.StartsWith("_"))
                    {
                        newName = newName.Substring(1, newName.Length - 1);
                    }

                    newName = RemoveEndingUnderScore(newName);

                    Flds.Append($"[{newName}],");
                }

                counter++;
            }

            var s = Flds.ToString();
            var j = s.Substring(0, s.Length - 1);

            return j;
        }

        private static string ParseFields(string fields, string fieldType)
        {
            StringBuilder Flds = new StringBuilder();

            var flds = fields.Split(',');

            foreach (var i in flds)
            {
                if (!string.IsNullOrEmpty(i))
                {
                    Flds.AppendLine($"{i} {fieldType}, ");
                }
            }

            var s = Flds.ToString();
            var j = s.Substring(0, s.Length - 4);

            return j;
        }

        private static string ParseContent(string completeFilePath, string dbName, string tableName, string tableFields)
        {
            StringBuilder BatchSql = new StringBuilder();                     
            int cnter = 0;
            int TotalCounter = 0;
            bool HeaderRow = true;
            var FirstRow = BuildFirstRow(dbName, tableName, tableFields);

            using (Microsoft.VisualBasic.FileIO.TextFieldParser parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(completeFilePath))
            {
                parser.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
                parser.SetDelimiters(",");

                bool FirstRowOfBatch = true;

                while (!parser.EndOfData)
                {
                    StringBuilder LS = new StringBuilder();

                    string[] fields = parser.ReadFields();

                    if (HeaderRow)
                    {
                        HeaderRow = false;
                        continue;
                    }
                    else
                    {
                        if (FirstRowOfBatch)
                        {
                            LS.AppendLine("");
                            LS.AppendLine(FirstRow);
                            LS.Append(SelectRow(fields));

                            FirstRowOfBatch = false;
                        }else
                        {
                            LS.Append(" UNION ALL" + SelectRow(fields));
                        }
                    }                                       

                    cnter += 1;
                    TotalCounter += 1;

                    BatchSql.Append(LS);

                    if (cnter == batchNbr)
                    {
                        BatchSql.AppendLine($"COMMIT;");
                        FirstRowOfBatch = true;
                        cnter = 0;

                        //string jjj = Guid.NewGuid().ToString();
                        //File.AppendAllText("C:\\Users\\mc\\Desktop\\mike.sql", BatchSql.ToString());
                        //BatchSql.Clear();
                    }
                }             
            }

            if((TotalCounter % batchNbr) == 0)
            {
                //do nothing, it ends on a good number
            } else
            {
                BatchSql.AppendLine("COMMIT;");
            }

            return BatchSql.ToString();
        }


        private static string SelectRow(string[] fields)
        {
            StringBuilder LS = new StringBuilder();

            LS.Append($" SELECT ");
            string rowX = Mike(fields);
            LS.AppendLine($"{rowX}");

            return LS.ToString();
        }

        private static string BuildFirstRow(string dbName, string tableName, string tableFields)
        {
            StringBuilder sb = new StringBuilder();

            //sb.AppendLine($"USE {dbName}");
            //sb.AppendLine($"GO");
            sb.AppendLine($"BEGIN TRANSACTION;");
            sb.Append($"INSERT INTO [{tableName}] ({tableFields})");

            return sb.ToString();
        }

        private static string Mike(IEnumerable<string> flds)
        {
            if (flds == null)
            {
                return "";
            }

            int cntr = 1;

            StringBuilder LocalString = new StringBuilder();

            foreach (string field in flds)
            {
                string jj = field.Replace("'", "''");
                LocalString.Append($"'{jj}' As field{cntr},");

                cntr++;
            }

            string v = LocalString.ToString();
            return v.Substring(0, (v.Length - 1));
        }

        private static string RemoveEndingUnderScore(string fieldName)
        {
            if (fieldName == null) return "";

            int counter = 0;

            // this was posted by petebob as well 
            char[] array = fieldName.ToCharArray();
            Array.Reverse(array);

            foreach (var i in array)
            {
                if (i.Equals('_'))
                {
                    counter++;
                }
                else
                {
                    break;
                }
            }

            string finalField = fieldName.Substring(0, (fieldName.Length - counter));

            if (string.IsNullOrEmpty(finalField))
            {
                return "empty_field";
            }
            else
            {
                return finalField;
            }
        }

        public async Task<IEnumerable<string>> HandleBigFile(FileUploadConfig config)
        {
            int RowLimit = 15;
            long LineCount = File.ReadLines(config.CompleteFileName).Count();
            int FileCounter = 1;
            StringBuilder SqlStatement = new StringBuilder();

            bool NeedHeaders = false;

            var ProcessedFiles = new List<string>();

            string line1 = File.ReadLines(config.CompleteFileName).First();
            string TableFields = GetFields(line1, config.AddEmptyField);

            var sb = ParseContent(config.CompleteFileName, config.DatabaseName, config.TableName, TableFields);

            string[] stringArray = sb.ToString().Split("\r\n").ToArray();

            for (int i = 0; i < stringArray.Length; i++)
            {
                if (NeedHeaders)
                {
                    SqlStatement.AppendLine("88888888888888888");
                    NeedHeaders = false;
                }

                string s = stringArray[i];

                SqlStatement.AppendLine(s);

                if (i == (RowLimit*FileCounter))
                {
                    SqlStatement.AppendLine("End it here");

                    string fn = $"{config.TableName }-{ FileCounter}";

                    await File.WriteAllTextAsync($"{config.FolderName}\\{fn}.sql", SqlStatement.ToString());
                    FileCounter++;
                    SqlStatement.Clear();
                    ProcessedFiles.Add(fn);

                    NeedHeaders = true;
                }               
            }

            return ProcessedFiles;
        }
    }
}