using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace prefSQL.SQLParser
{
    //internal class
    class Helper
    {
        public String DriverString { get; set;} 
        public String ConnectionString { get; set; }


        public DataTable getResults(String strPrefSQL, SQLCommon.Algorithm algorithm, int upToLevel)
        {
            DataTable dt = new DataTable();
            string str1 = "";
            string str2 = "";
            string str3 = "";

            if (algorithm != SQLCommon.Algorithm.NativeSQL)
            {
                //First parameter
                int iPosStart = strPrefSQL.IndexOf("'") + 1;
                int iPosMiddle = iPosStart;
                bool bEnd = false;
                while (bEnd == false)
                {
                    iPosMiddle = iPosMiddle + strPrefSQL.Substring(iPosMiddle).IndexOf("'") + 1;
                    if (!strPrefSQL.Substring(iPosMiddle, 1).Equals("'"))
                    {
                        bEnd = true;
                    }
                    else
                    {
                        iPosMiddle++;
                    }
                    //Prüfen ob es kein doppeltes Hochkomma ist
                }
                iPosMiddle += 3;

                //Second parameter
                int iPosEnd = iPosMiddle;
                bEnd = false;
                while (bEnd == false)
                {
                    iPosEnd = iPosEnd + strPrefSQL.Substring(iPosEnd).IndexOf("'") + 1;
                    if (iPosEnd == strPrefSQL.Length)
                        break; //Kein 3.Parameter
                    if (!strPrefSQL.Substring(iPosEnd, 1).Equals("'"))
                    {
                        bEnd = true;
                    }
                    else
                    {
                        iPosEnd++;
                    }
                    //Prüfen ob es kein doppeltes Hochkomma ist
                }
                iPosEnd += 3;


                str1 = strPrefSQL.Substring(iPosStart, iPosMiddle - iPosStart - 4);
                str2 = strPrefSQL.Substring(iPosMiddle, iPosEnd - iPosMiddle - 4);
                str3 = "";
                //if (iPosEnd < strPrefSQL.Length - 10)
                if (iPosEnd < strPrefSQL.Length)
                {
                    str3 = strPrefSQL.Substring(iPosEnd).TrimEnd('\'');
                    //str3 = strPrefSQL.Substring(iPosEnd, strPrefSQL.Length - iPosEnd - 10).TrimEnd('\'');
                }
                str1 = str1.Replace("''", "'").Trim('\'');
                str2 = str2.Replace("''", "'").Trim('\'');
                str3 = str3.Replace("''", "'").Trim('\'');

            }

            Stopwatch sw = new Stopwatch();

            sw.Start();

            try
            {
                System.Data.SqlTypes.SqlString strSQL1 = str1;
                System.Data.SqlTypes.SqlString strSQL2 = str2;
                System.Data.SqlTypes.SqlString strSQL3 = str3;
                /*if (algorithm == SQLCommon.Algorithm.BNL)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNL skyline = new SQLSkyline.SP_SkylineBNL();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLLevel skyline = new SQLSkyline.SP_SkylineBNLLevel();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else */
                if (algorithm == SQLCommon.Algorithm.BNLSort)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSort skyline = new SQLSkyline.SP_SkylineBNLSort();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLSortLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSortLevel skyline = new SQLSkyline.SP_SkylineBNLSortLevel();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {
                    prefSQL.SQLSkyline.SP_SkylineHexagon skyline = new SQLSkyline.SP_SkylineHexagon();
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                    //Default stack size is 1MB (1024000) --> Increase to 8MB
                    var thread = new Thread(
                        () =>
                        {
                            dt = skyline.getSkylineTable(str1, str2, str3, ConnectionString);
                        }, 8000000);

                    
                    thread.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    thread.Join();

                }
                else if (algorithm == SQLCommon.Algorithm.HexagonLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineHexagonLevel skyline = new SQLSkyline.SP_SkylineHexagonLevel();
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                    //Default stack size is 1MB (1024000) --> Increase to 8MB
                    var thread = new Thread(
                        () =>
                        {
                            dt = skyline.getSkylineTable(str1, str2, str3, ConnectionString);
                        }, 8000000);


                    thread.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    thread.Join();

                }
                else if (algorithm == SQLCommon.Algorithm.MultipleBNL)
                {
                    prefSQL.SQLSkyline.SP_MultipleSkylineBNL skyline = new SQLSkyline.SP_MultipleSkylineBNL();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString, upToLevel);
                }
                else if (algorithm == SQLCommon.Algorithm.NativeSQL)
                {
                    //Native SQL

                    //Generic database provider
                    // create the provider factory from the namespace provider
                    // you could create any other provider factory.. for Oracle, MySql, etc...
                    DbProviderFactory factory = DbProviderFactories.GetFactory(DriverString);

                    // use the factory object to create Data access objects.
                    DbConnection connection = factory.CreateConnection(); // will return the connection object, in this case, SqlConnection ...
                    connection.ConnectionString = ConnectionString;

                    connection.Open();
                    DbCommand command = connection.CreateCommand();
                    command.CommandText = strPrefSQL;
                    DbDataAdapter db = factory.CreateDataAdapter();
                    db.SelectCommand = command;
                    db.Fill(dt);
                }

            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
            }



            sw.Stop();

            Console.WriteLine("Elapsed={0}", sw.Elapsed);

            return dt;
        }

    }
}
