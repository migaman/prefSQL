using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using prefSQL.SQLParser;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;
using System.IO;
//Microsoft.SqlServer.Smo.dll
using Microsoft.SqlServer.Management.Smo;
//Microsoft.SqlServer.ConnectionInfo.dll
using Microsoft.SqlServer.Management.Common;


namespace prefSQL.SQLParserTest
{
    [TestClass]
    public class SQLMyTest
    {
        private static String time = DateTime.Now.ToString("yyyyMMdd_hhmmss");
        private static String DBPrefix = "UnitTestDB";
        private static String PlaceholderDBPrefix = "[UnitTestDB]";

        /*[ClassInitialize]
        public static void TestInitDb(TestContext testContext)
        {
            //Create Database

            string scriptDirectory = System.IO.Directory.GetCurrentDirectory();
            string sqlConnectionString = Helper.TestConnectionString;
            DirectoryInfo di = new DirectoryInfo(scriptDirectory);
            FileInfo[] rgFiles = di.GetFiles("*.sql");
            foreach (FileInfo fi in rgFiles)
            {
                FileInfo fileInfo = new FileInfo(fi.FullName);
                string script = fileInfo.OpenText().ReadToEnd();

                script = script.Replace(PlaceholderDBPrefix, DBPrefix + time);
                SqlConnection connection = new SqlConnection(sqlConnectionString);
                Server server = new Server(new ServerConnection(connection));
                server.ConnectionContext.ExecuteNonQuery(script);
                connection.Close();
            }

        }*/


        [TestMethod]
        public void TestDatabaseQuery()
        {
            String sql = "SELECT t.id FROM cars t SKYLINE OF t.price LOW, t.mileage LOW";
            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            PrefSQLModel model = common.GetPrefSqlModelFromPreferenceSql(sql);
            common.SkylineType = new SkylineBNL();
            String connectionString = Helper.TestConnectionString.Replace("master", DBPrefix + time);
            DataTable dt = common.ParseAndExecutePrefSQL(connectionString, Helper.ProviderName, sql);

            //Check tuples (every algorithm should deliver the same amount of tuples)
            Assert.AreEqual(dt.Rows.Count, 4, 0,  "Test tuples do not match");
        }

      
        /*
        [ClassCleanup]
        public static void CleanupDatabase()
        {
            //Clear all pools (when connection pooling is used, it is possible that the database is still open)
            SqlConnection.ClearAllPools();

            //Drop the database
            SqlConnection connection = new SqlConnection(Helper.TestConnectionString);
            SqlCommand cmd = new SqlCommand();
            cmd.CommandText = "DROP DATABASE " + DBPrefix + time;
            cmd.Connection = connection;

            connection.Open();
            cmd.ExecuteNonQuery();
            connection.Close();
        }*/
    }
}
