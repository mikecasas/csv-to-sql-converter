using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace CsvToSqlConverter
{
    public class SqlHelper
    {
        [DebuggerStepThrough]
        internal static string DropCreateTableStatement(FileUploadConfig config, string tableFields)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(UseDb(config.DatabaseName));
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

        internal static string UseDb(string dbName)
        {
            StringBuilder sb = new StringBuilder();

            sb.AppendLine($"USE {dbName}");
            sb.AppendLine($"GO");
            return sb.ToString();
        }

        internal static string BuildFirstRow(string tableName, string tableFields)
        {
            return $"INSERT INTO [{tableName}] ({tableFields})";          
        }

        internal static string BuildFirstRow(string dbName, string tableName, string tableFields)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append(UseDb(dbName));
            sb.AppendLine(BuildFirstRow(tableName, tableFields));
            return sb.ToString();
        }

        internal static string GetFields(string firstLine, bool addEmptyColumns)
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

        [DebuggerStepThrough]
        internal static string SelectRow(string[] fields)
        {
            StringBuilder LS = new StringBuilder();

            LS.Append($"UNION ALL SELECT ");
            string rowX = FieldMaker(fields);
            LS.AppendLine($"{rowX}");

            return LS.ToString();
        }

        private static string FieldMaker(IEnumerable<string> flds)
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
    }
}