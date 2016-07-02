using System.Configuration;

namespace prefSQL.SQLParserTest
{
    class Helper
    {
        public static string ConnectionString = ConfigurationManager.ConnectionStrings["localhost"].ConnectionString;
        public static string TestConnectionString = ConfigurationManager.ConnectionStrings["unittest"].ConnectionString;
        public static string ProviderName = ConfigurationManager.ConnectionStrings["localhost"].ProviderName;


    }
}
