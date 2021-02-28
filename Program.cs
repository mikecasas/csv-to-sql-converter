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

            config.DatabaseName = "Manbo";
            config.FileName = "Feb-2020.csv";
            config.FolderName = "C:\\Users\\mc\\desktop\\Manbo\\Manbo.Backend\\SqlScripts\\Data";
            config.TableName = "FebTemp";

            config.IncludeDropCreateTable = true;

            var B = new SqlContentBuilder();

            string content = B.BuildCreateTable(config);

            await File.WriteAllTextAsync($"{config.FolderName}\\{config.TableName + Guid.NewGuid().ToString()}.sql", content);

            Console.WriteLine("Processing Complete!");
        }
    }
}
