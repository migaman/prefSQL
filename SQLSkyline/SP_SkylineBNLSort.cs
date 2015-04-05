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
    public class SP_SkylineBNLSort : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [Microsoft.SqlServer.Server.SqlProcedure(Name = "SP_SkylineBNLSort")]
        public static void getSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineBNLSort skyline = new SP_SkylineBNLSort();
            skyline.getSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, "");
        }

        protected override void addtoWindow(DataTableReader sqlReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult)
        {
            Helper.addToWindow(sqlReader, operators, resultCollection, resultstringCollection, record, dtResult);
        }

        protected override bool tupleDomination(ArrayList resultCollection, ArrayList resultstringCollection, DataTableReader sqlReader, string[] operators, DataTable dtResult, int i)
        {
            long?[] result = (long?[])resultCollection[i];
            string[] strResult = (string[])resultstringCollection[i];

            //Dominanz
            if (Helper.isTupleDominated(sqlReader, operators, result, strResult) == true)
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