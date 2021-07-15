using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;

namespace CsvToSqlConverter
{
    public class SqlContentBuilder
    {   
        public async Task<IEnumerable<string>> HandleBigFile(FileUploadConfig config, int filesMax, int rowLimit)
        {
            List<string> Main = new List<string>();

            bool FirstFile = true;
            int TakeAmount = rowLimit;

            string line1 = File.ReadLines(config.CompleteFileName).First();
            string TableFields = SqlHelper.GetFields(line1, config.AddEmptyField);

            var FirstRow = SqlHelper.BuildFirstRow(config.TableName, TableFields);
            var FR = BuildContent(config.CompleteFileName);

            int Rounds = CalculateLoops(FR.Rows, rowLimit);
                  
            if (FR.Rows < rowLimit)
            {
                TakeAmount = FR.Rows;
            }

            for (int i = 0; i < Rounds; i++)
            {
                int SkipAmount = (i * TakeAmount);

                var Items = FR.Content.Skip(SkipAmount).Take(TakeAmount);
                var Block = TransactionBlock(Items, FirstRow,config.DatabaseName);

                string Content;

                if (FirstFile)
                {
                    FirstFile = false;
                    Content = SqlHelper.DropCreateTableStatement(config,TableFields) + Block;
                } else
                {
                    Content = Block;
                }

                Main.Add(Content);                              
              
            }

            return await ProcessFiles(Main, filesMax, config);
        }

        private async Task<IEnumerable<string>> ProcessFiles(IEnumerable<string> content, int MaxFiles, FileUploadConfig config)
        {
            var ProcessedFiles = new List<string>();

            StringBuilder sb = new StringBuilder();

            int ContentBlocks = content.Count();

            if ((ContentBlocks < MaxFiles) || MaxFiles==1)
            {
                //Stuff it all in one file

                foreach(var i in content)
                {
                    sb.Append(i);
                }

                var FileName = await WriteOut(config.FolderName, config.TableName, null, sb.ToString());
                ProcessedFiles.Add(FileName);
            } else
            {
                int BlocksInOneFile = CalculateLoops(ContentBlocks, MaxFiles);

                for (int i = 0; i < MaxFiles; i++)
                {
                    int SkipAmount = (i * BlocksInOneFile);
                    var Items = content.Skip(SkipAmount).Take(BlocksInOneFile);

                    foreach (var c in Items)
                    {
                        sb.Append(c);
                    }

                    var FileName2 = await WriteOut(config.FolderName, config.TableName, i, sb.ToString());
                    ProcessedFiles.Add(FileName2);
                    sb.Clear();
                }
            }

            return ProcessedFiles;
        }

        [DebuggerStepThrough]
        private int CalculateLoops(int a, int b)
        {
            var Remainder = a % b;

            if (Remainder == 0)
            {
                return (a / b);
            } else
            {
                return (a / b) + 1;
            }          
        }

        [DebuggerStepThrough]
        private static string TransactionBlock(IEnumerable<string> rows, string firstRow, string dbName)
        {
            bool FirstOne = true;

            StringBuilder sb = new StringBuilder();
            sb.Append(SqlHelper.UseDb(dbName));
            sb.AppendLine("BEGIN TRANSACTION;");
            sb.AppendLine(firstRow);

            foreach (var i in rows)
            {
                if (FirstOne)
                {
                    sb.AppendLine(i.Replace("UNION ALL ",""));
                    FirstOne = false;
                } else
                {
                    sb.AppendLine(i);
                }                
            }

            sb.AppendLine("COMMIT;");

            return sb.ToString();
        }

        private static Models.FileRead BuildContent(string completeFilePath)
        {
            bool HeaderRow = true;
            List<string> LS = new List<string>();
            int Counter = 0;

            using (Microsoft.VisualBasic.FileIO.TextFieldParser parser = new Microsoft.VisualBasic.FileIO.TextFieldParser(completeFilePath))
            {
                parser.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
                parser.SetDelimiters(",");

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    //Skip the first row, assuming it's the header row.
                    if (HeaderRow)
                    {
                        HeaderRow = false;
                        continue;
                    }
                    else
                    {
                        LS.Add(SqlHelper.SelectRow(fields));
                    }
                    Counter++;
                }
            }

            return new Models.FileRead
            {
                Content = LS,
                Rows = Counter
            };
        }

        private async Task<string> WriteOut(string folderName, string tableName, int? n, string content)
        {
            string fn;

            if (n.HasValue)
            {
                fn = $"{tableName }-{n}";
            }else
            {
                fn = $"{tableName }";
            }
                      
            await File.WriteAllTextAsync($"{folderName}\\{fn}.sql", content);

            return fn;
        }
    }
}