using System;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace CsvToSqlConverter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = GetConfig();

            string[] files = GetFiles(config);

            var FilesProcessed = await ProcessFiles(files, config);
            
            string Last = CreateScriptFile(FilesProcessed, config.FolderName);
            await File.WriteAllTextAsync($"{config.FolderName}\\00-run-all.sql", Last);

            Console.WriteLine("Processing Complete!");
        }

        private static FileUploadConfig GetConfig()
        {
            var config = new FileUploadConfig();

            const string A = "C:\\Users\\mcasas\\";

            //config.FolderName = A + "Documents\\Munis-Financials\\csv";
            //config.FolderName = A + "Desktop\\other\\covid-19-public-safety\\May-2021\\csv";
            config.FolderName = A + "Documents\\_energov\\LBTR";
            //config.FolderName = A + "Desktop\\test";
            //config.FolderName = A + "Downloads";

            //config.DatabaseName = "DBScppSpecialProjects";
            config.DatabaseName = "pembrokepines_SOURCE";

            //config.DatabaseName = "td";

            config.IncludeDropCreateTable = true;

            return config;
        }

        private static async Task<IEnumerable<string>> ProcessFiles(string[] files, FileUploadConfig config)
        {
            var B = new SqlContentBuilder();
            var ProcessedFiles = new List<string>();

            foreach (string i in files)
            {
                ProcessedFiles.AddRange(await B.HandleBigFile(config, 10, 200));
            }

            return ProcessedFiles;
        }

        /// <summary>
        /// Get csv files to convert
        /// </summary>
        /// <param name="config"></param>
        /// <returns></returns>
        private static string[] GetFiles(FileUploadConfig config)
        {
            //Option 1. List the files your self.
            //string[] files = { "February.csv", "March.csv"};
            //string[] files = { "M.csv"};

            //Option 2. Read them all from a folder
            string[] files = Directory.GetFiles(config.FolderName, "*.csv");

            return files;
        }

        private static string CreateScriptFile(IEnumerable<string> files, string folder)
        {
            System.Text.StringBuilder sb = new System.Text.StringBuilder();

            sb.AppendLine($":SETVAR BasePath \"{folder}\\\"");

            foreach (string f in files)
            {
                string fn = Path.GetFileName(f);
                sb.AppendLine($":r $(BasePath){fn}.sql");
            }

            return sb.ToString();
        }      
    }
}