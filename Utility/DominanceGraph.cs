using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using prefSQL.SQLParser;
using prefSQL.SQLSkyline;

namespace Utility
{
    class DominanceGraph
    {
        public void Run()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();

            string strSQL = "SELECT t1.id, t1.title, t1.price FROM cars_small t1 LEFT OUTER JOIN colors t2 ON t1.color_id = t2.ID ";
            string strPreference = " SKYLINE OF HIGH t2.name {'black' >> OTHERS EQUAL} AND LOW t1.price";
            List<int> listIDs = new List<int>();
            bool isSkylineEmpty = false; 
            SQLCommon common = new SQLCommon();
            common.SkylineType = new SkylineSQL();
            common.ShowSkylineAttributes = true;
            SqlConnection cnnSQL = new SqlConnection(Helper.ConnectionString);

            try
            {
                //Open Connection
                cnnSQL.Open();

                //As long as Query returns skyline tuples
                while (isSkylineEmpty == false)
                {
                    //Add WHERE clause with IDs that were already in the skyline
                    String strIDs = "";
                    foreach (int id in listIDs)
                    {
                        strIDs += id + ",";
                    }
                    if (strIDs.Length > 0)
                    {
                        strIDs = "WHERE t1.id NOT IN (" + strIDs.TrimEnd(',') + ")";
                    }
                    //Parse PreferenceSQL into SQL
                    string sqlNative = common.ParsePreferenceSQL(strSQL + strIDs + strPreference);

                    //Execute SQL
                    DbCommand command = cnnSQL.CreateCommand();
                    command.CommandTimeout = 0; //infinite timeout
                    command.CommandText = sqlNative;
                    DbDataReader sqlReader = command.ExecuteReader();

                    if (sqlReader.HasRows)
                    {
                        while (sqlReader.Read())
                        {
                            listIDs.Add(Int32.Parse(sqlReader["id"].ToString()));
                        }
                    }
                    else
                    {
                        isSkylineEmpty = true;
                    }
                    sqlReader.Close();

                }
                

                //Close connection
                cnnSQL.Close();
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }


            sw.Stop();
            Debug.WriteLine("Elapsed={0}", sw.ElapsedMilliseconds);


        }



    }
}
