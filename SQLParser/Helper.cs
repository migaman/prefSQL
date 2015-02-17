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
        /// <summary>
        /// Driver-String, i.e. System.Data.SqlClient
        /// </summary>
        public String DriverString { get; set; }  
        /// <summary>
        /// Connectionstring, i.e. Data Source=localhost;Initial Catalog=eCommerce;Integrated Security=True
        /// </summary>
        public String ConnectionString { get; set; }

        /// <summary>
        /// Returns a datatable with the tuples from the SQL statement
        /// The sql will be resolved into pieces, in order to call the Skyline algorithms without MSSQL CLR
        /// </summary>
        /// <param name="strPrefSQL"></param>
        /// <param name="algorithm"></param>
        /// <param name="upToLevel"></param>
        /// <returns></returns>
        public DataTable getResults(String strPrefSQL, SQLCommon.Algorithm algorithm, int upToLevel)
        {
            DataTable dt = new DataTable();
            string str1 = "";
            string str2 = "";
            string str3 = "";
            string str4 = "";   //4th parameter (only for hexagon)
            int i5 = 0;         //5th parameter (only for hexagon)

            //Native SQL algorithm is already a valid SQL statement
            if (algorithm != SQLCommon.Algorithm.NativeSQL)
            {
                //All other algorithms are developed as stored procedures
                //Resolve now each parameter from this SP calls to single pieces

                //1st parameter
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
                    //Check that it is not a double apostrophe
                }
                iPosMiddle += 3;

                //2nd parameter
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
                    //Check that it is not a double apostrophe
                }
                iPosEnd += 3;



                //Check if it has more than 3 parameters
                if (iPosEnd < strPrefSQL.Length)
                {
                    //3th parameter
                    int iPosEndEnd = iPosEnd;
                    bEnd = false;
                    while (bEnd == false)
                    {
                        iPosEndEnd = iPosEndEnd + strPrefSQL.Substring(iPosEndEnd).IndexOf("'") + 1;
                        if (iPosEndEnd == strPrefSQL.Length)
                            break; //Kein 3.Parameter
                        if (!strPrefSQL.Substring(iPosEndEnd, 1).Equals("'"))
                        {
                            bEnd = true;
                        }
                        else
                        {
                            iPosEndEnd++;
                        }
                        //Check that it is not a double apostrophe
                    }
                    iPosEndEnd += 3;

                    str1 = strPrefSQL.Substring(iPosStart, iPosMiddle - iPosStart - 4);
                    str2 = strPrefSQL.Substring(iPosMiddle, iPosEnd - iPosMiddle - 4);
                    str3 = strPrefSQL.Substring(iPosEnd, iPosEndEnd - iPosEnd - 4);

                    if (iPosEndEnd < strPrefSQL.Length)
                    {
                        //Hexagon algorithm has 5 parameters
                        int iPosComma = strPrefSQL.LastIndexOf(",");
                        str4 = strPrefSQL.Substring(iPosEndEnd, iPosComma-iPosEndEnd).TrimEnd('\'');
                        i5 = int.Parse(strPrefSQL.Substring(iPosComma+1));
                    }
                }
                else
                {
                    str1 = strPrefSQL.Substring(iPosStart, iPosMiddle - iPosStart - 4);
                    str2 = strPrefSQL.Substring(iPosMiddle, iPosEnd - iPosMiddle - 4);
                    if (iPosEnd < strPrefSQL.Length)
                    {
                        str3 = strPrefSQL.Substring(iPosEnd).TrimEnd('\'');
                    }
                }
               


                str1 = str1.Replace("''", "'").Trim('\'');
                str2 = str2.Replace("''", "'").Trim('\'');
                str3 = str3.Replace("''", "'").Trim('\'');
                str4 = str4.Replace("''", "'").Trim('\'');

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
                    prefSQL.SQLSkyline.SP_SkylineBNL skyline = new SQLSkyline.SP_SkylineBNL();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else if (algorithm == SQLCommon.Algorithm.BNLLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineBNLLevel skyline = new SQLSkyline.SP_SkylineBNLLevel();
                    dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                }
                else
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
                else if (algorithm == SQLCommon.Algorithm.DQ)
                {
                    prefSQL.SQLSkyline.SP_SkylineDQ skyline = new SQLSkyline.SP_SkylineDQ();

                    //D&Q algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                    //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                    var thread = new Thread(
                        () =>
                        {
                            dt = skyline.getSkylineTable(str1, str2, ConnectionString);
                        }, 8000000);


                    thread.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    thread.Join();
                    
                    
                    
                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {

                    prefSQL.SQLSkyline.SP_SkylineHexagon skyline = new SQLSkyline.SP_SkylineHexagon();
                    
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                    //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                    var thread = new Thread(
                        () =>
                        {
                            dt = skyline.getSkylineTable(str1, str2, str3, ConnectionString, str4, i5);
                        }, 8000000);

                    
                    thread.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    thread.Join();

                }
                else if (algorithm == SQLCommon.Algorithm.HexagonLevel)
                {
                    prefSQL.SQLSkyline.SP_SkylineHexagonLevel skyline = new SQLSkyline.SP_SkylineHexagonLevel();
                    //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                    //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
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
                    //Create the provider factory from the namespace provider, you could create any other provider factory.. for Oracle, MySql, etc...
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
            System.Diagnostics.Debug.WriteLine("Elapsed={0}", sw.Elapsed);
            return dt;
        }

    }
}
