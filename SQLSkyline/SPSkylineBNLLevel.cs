using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SPSkylineBNLLevel : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        [SqlProcedure(Name = "SP_SkylineBNLLevel")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNLLevel skyline = new SPSkylineBNLLevel();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }



        protected override void AddToWindow(object[] newTuple, List<long[]> window, ArrayList resultstringCollection, string[] operatorsArray, int[] dimensions, DataTable dtResult)
        {
            Helper.AddToWindow(newTuple, window, operatorsArray, dtResult);
        }


        protected override bool IsTupleDominated(List<long[]> window, long[] newTuple, int[] dimensions, string[] operatorsArray, ArrayList incomparableTuple, int listIndex, DataTable dtResult, object[] newTupleAllValues)
        {
            long[] windowTuple = window[listIndex];

            //Dominanz
            if (Helper.IsTupleDominated(windowTuple, newTuple, dimensions))
            {
                //New point is dominated. No further testing necessary
                return true;
            }

            //Now, check if the new point dominates the one in the window
            //This is only possible with not sorted data
            if (Helper.DoesTupleDominate(windowTuple, newTuple, dimensions))
            {
                //The new record dominates the one in the windows. Remove point from window and test further
                window.RemoveAt(listIndex);
                dtResult.Rows.RemoveAt(listIndex);
            }
            return false;
        }


    }
}