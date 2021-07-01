using System;
using System.IO;
using System.Threading.Tasks;

namespace CsvToSqlConverter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = GetConfig();

            string[] files = GetFiles(config);

            await ProcessFiles(files, config);

            Console.WriteLine("Processing Complete!");
        }

        private static FileUploadConfig GetConfig()
        {
            var config = new FileUploadConfig();

            //config.FolderName = "C:\\Users\\mcasas\\Documents\\Munis-Financials\\csv";
            //config.FolderName = "C:\\Users\\mcasas\\Desktop\\other\\covid-19-public-safety\\May-2021\\csv";
            config.FolderName = "C:\\Users\\mcasas\\Documents\\_energov\\LBTR";

            //config.FolderName = "C:\\Users\\mcasas\\Downloads";

            //config.DatabaseName = "DBScppSpecialProjects";
            config.DatabaseName = "pembrokepines_SOURCE";

            //config.DatabaseName = "td";

            config.IncludeDropCreateTable = true;

            return config;
        }

        private static async Task ProcessFiles(string[] files, FileUploadConfig config)
        {
            var B = new SqlContentBuilder();

            foreach (string i in files)
            {
                config.FileName = Path.GetFileName(i); // Not needed + ".csv";
                config.TableName = Path.GetFileNameWithoutExtension(i).Replace(" ", "-");

                string content = B.BuildCreateTable(config);

                //await File.WriteAllTextAsync($"{config.FolderName}\\{config.TableName + "-" + Guid.NewGuid().ToString()}.sql", content);
                await File.WriteAllTextAsync($"{config.FolderName}\\{config.TableName}.sql", content);
            }
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
            string[] files = Directory.GetFiles(config.FolderName, "OLCO*.csv");

            return files;
        }
    }
}