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
    public class Helper
    {
        public String DriverString { get; set;} 
        public String ConnectionString { get; set; }


        public DataTable getResults(String strPrefSQL, SQLCommon.Algorithm algorithm)
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
                if (iPosEnd < strPrefSQL.Length - 10)
                {
                    //str3 = strSQL.Substring(iPosEnd).TrimEnd('\'');
                    str3 = strPrefSQL.Substring(iPosEnd, strPrefSQL.Length - iPosEnd - 10).TrimEnd('\'');
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
                if (algorithm == SQLCommon.Algorithm.BNL)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNL.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLLevel.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLSort)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSort.getSkyline(str1, str2, true);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLSortLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLSortLevel.getSkyline(str1, str2, true);

                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                    //Default stack size is 1MB (1024000) --> Increase to 8MB
                    Thread T = new Thread(() => prefSQL.SQLSkyline.SP_SkylineHexagon.getSkyline(str1, str2, str3, true), 8000000);
                    T.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    T.Join();

                    //prefSQL.SQLSkyline.SP_SkylineHexagon.getSkyline(str1, str2, str3, true);
                }
                else if (algorithm == SQLCommon.Algorithm.Tree)
                {
                    prefSQL.SQLSkyline.SP_SkylineTree.getSkyline(str1, str2, true, 3);
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
