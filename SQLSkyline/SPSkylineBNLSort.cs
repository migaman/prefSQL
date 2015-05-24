using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SPSkylineBNLSort : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        [SqlProcedure(Name = "SP_SkylineBNLSort")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNLSort skyline = new SPSkylineBNLSort();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }

        protected override void AddToWindow(object[] newTuple, List<long[]> window, ArrayList resultstringCollection, string[] operators, int[] dimensions, DataTable dtResult)
        {
            Helper.AddToWindowIncomparable(newTuple, window, dimensions, operators, resultstringCollection, dtResult);
        }


        protected override bool IsTupleDominated(List<long[]> window, long[] newTuple, int[] dimensions, string[] operators, ArrayList incomparableTuples, int listIndex, DataTable dtResult, object[] newTupleAllValues)
        {
            string[] incomparableTuple = (string[])incomparableTuples[listIndex];
            long[] windowTuple = window[listIndex];

            //Dominanz
            if (Helper.IsTupleDominated(windowTuple, newTuple, dimensions, operators, incomparableTuple, newTupleAllValues))
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