using System;
using System.IO;
using System.Threading.Tasks;

namespace CsvToSqlConverter
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new FileUploadConfig();

            //config.FolderName = "C:\\Users\\mcasas\\Documents\\Munis-Financials\\csv";

            config.FolderName = "C:\\Users\\mcasas\\Desktop\\other\\covid-19-public-safety\\May-2021\\csv";


            config.DatabaseName = "DBScppSpecialProjects";
            config.IncludeDropCreateTable = true;

            var B = new SqlContentBuilder();

            //string[] files = { "apr-budget", "apr-actual" };

            
            //Option 1. List the files your self.
            //string[] files = { "February.csv", "March.csv"};

            //Option 2. Read them all from a folder
            string[] files = Directory.GetFiles(config.FolderName);

            foreach (string i in files)
            {
                config.FileName = Path.GetFileName(i); // Not needed + ".csv";
                config.TableName = Path.GetFileNameWithoutExtension(i).Replace(" ", "-");

                string content = B.BuildCreateTable(config);

                await File.WriteAllTextAsync($"{config.FolderName}\\{config.TableName + "-" + Guid.NewGuid().ToString()}.sql", content);
            }

            Console.WriteLine("Processing Complete!");
        }
    }
}