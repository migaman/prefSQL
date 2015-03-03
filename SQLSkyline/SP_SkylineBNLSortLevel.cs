using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Collections;
using System.Collections.Generic;


//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SP_SkylineBNLSortLevel : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineBNLSortLevel")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineBNLSortLevel skyline = new SP_SkylineBNLSortLevel();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, "");
        }


        public override void addtoWindow(DataTableReader sqlReader, string[] operators, ref ArrayList resultCollection, ref ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, ref DataTable dtResult)
        {
            Helper.addToWindow(sqlReader, operators, ref resultCollection, record, ref dtResult);
        }

        public override bool tupleDomination(ref ArrayList resultCollection, ref ArrayList resultstringCollection, DataTableReader sqlReader, string[] operators, ref DataTable dtResult, int i)
        {
            long[] result = (long[])resultCollection[i];

            //Dominanz
            if (Helper.isTupleDominated(sqlReader, result) == true)
            {
                //New point is dominated. No further testing necessary
                return true;
            }

            //Now, check if the new point dominates the one in the window
            //--> It is not possible that the new point dominates the one in the window --> Reason data is ORDERED

            
            return false;
        }



    }
}