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
using System.Text.RegularExpressions;

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
        /// <returns></returns>
        public DataTable getResults(String strPrefSQL, SQLCommon.Algorithm algorithm, bool withIncomparable)
        {
            DataTable dt = new DataTable();
            
            //Default Parameter
            string strQuery = "";
            string strOperators = "";
            int numberOfRecords = 0;
            string[] parameter = null;
            

            //Native SQL algorithm is already a valid SQL statement
            if(strPrefSQL.StartsWith("SELECT"))
            {
                //If query doesn't need skyline calculation (i.e. query without preference clause) --> set algorithm to nativeSQL
                algorithm = SQLCommon.Algorithm.NativeSQL;
            }
            if (algorithm != SQLCommon.Algorithm.NativeSQL)
            {
                //Store Parameters in Array (Take care to single quotes inside parameters)
                int iPosStart = strPrefSQL.IndexOf("'");
                String strtmp = strPrefSQL.Substring(iPosStart);
                parameter = Regex.Split(strtmp, ",(?=(?:[^']*'[^']*')*[^']*$)");

                //All other algorithms are developed as stored procedures
                //Resolve now each parameter from this SP calls to single pieces

                //Default parameter
                strQuery = parameter[0].Trim();
                strOperators = parameter[1].Trim();
                numberOfRecords = int.Parse(parameter[2].Trim());
                strQuery = strQuery.Replace("''", "'").Trim('\'');
                strOperators = strOperators.Replace("''", "'").Trim('\'');
            }

            Stopwatch sw = new Stopwatch();

            sw.Start();

            try
            {
                
                if (algorithm == SQLCommon.Algorithm.BNL)
                {
                    if(withIncomparable == true)
                    {
                        prefSQL.SQLSkyline.SP_SkylineBNL skyline = new SQLSkyline.SP_SkylineBNL();
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString);
                    }
                    else
                    {
                        prefSQL.SQLSkyline.SP_SkylineBNLLevel skyline = new SQLSkyline.SP_SkylineBNLLevel();
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString);
                    }
                }
                
                else
                if (algorithm == SQLCommon.Algorithm.BNLSort)
                {
                    if (withIncomparable == true)
                    {
                        prefSQL.SQLSkyline.SP_SkylineBNLSort skyline = new SQLSkyline.SP_SkylineBNLSort();
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString);
                    }
                    else
                    {
                        prefSQL.SQLSkyline.SP_SkylineBNLSortLevel skyline = new SQLSkyline.SP_SkylineBNLSortLevel();
                        dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString);
                    }
                    
                }
                else if (algorithm == SQLCommon.Algorithm.DQ)
                {
                    prefSQL.SQLSkyline.SP_SkylineDQ skyline = new SQLSkyline.SP_SkylineDQ();

                    //D&Q algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                    //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                    var thread = new Thread(
                        () =>
                        {
                            dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString);
                        }, 8000000);


                    thread.Start();

                    //Join method to block the current thread  until the object's thread terminates.
                    thread.Join();
                    
                    
                    
                }
                else if (algorithm == SQLCommon.Algorithm.Hexagon)
                {
                    if (withIncomparable == true)
                    {
                        prefSQL.SQLSkyline.SP_SkylineHexagon skyline = new SQLSkyline.SP_SkylineHexagon();
                        string strQueryConstruction = parameter[3].Trim().Replace("''", "'").Trim('\'');
                        String strHexagonSelectIncomparable = parameter[4].Trim().Replace("''", "'").Trim('\'');
                        int weightHexagonIncomparable = int.Parse(parameter[5].Trim());

                        //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread
                        //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                        var thread = new Thread(
                            () =>
                            {
                                dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, strQueryConstruction, ConnectionString, strHexagonSelectIncomparable, weightHexagonIncomparable);
                            }, 8000000);


                        thread.Start();

                        //Join method to block the current thread  until the object's thread terminates.
                        thread.Join();
                    }
                    else
                    {
                        prefSQL.SQLSkyline.SP_SkylineHexagonLevel skyline = new SQLSkyline.SP_SkylineHexagonLevel();
                        string strQueryConstruction = parameter[3].Trim().Replace("''", "'").Trim('\'');
                        
                        //Hexagon algorithm neads a higher stack (much recursions). Therefore start it with a new thread

                        //Default stack size is 1MB (1024000) --> Increase to 8MB. Otherwise the program might end in a stackoverflow
                        var thread = new Thread(
                            () =>
                            {
                                dt = skyline.getSkylineTable(strQuery, strOperators, numberOfRecords, ConnectionString, strQueryConstruction);
                            }, 8000000);


                        thread.Start();

                        //Join method to block the current thread  until the object's thread terminates.
                        thread.Join();
                    }

                }
                else if (algorithm == SQLCommon.Algorithm.MultipleBNL)
                {
                    if (withIncomparable == true)
                    {
                        prefSQL.SQLSkyline.SP_MultipleSkylineBNL skyline = new SQLSkyline.SP_MultipleSkylineBNL();
                        dt = skyline.getSkylineTable(strQuery, strOperators, ConnectionString, numberOfRecords, int.Parse(parameter[3]));
                    }
                    else
                    {
                        prefSQL.SQLSkyline.SP_MultipleSkylineBNLLevel skyline = new SQLSkyline.SP_MultipleSkylineBNLLevel();
                        dt = skyline.getSkylineTable(strQuery, strOperators, ConnectionString, numberOfRecords, int.Parse(parameter[3]));
                    }
                    
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
