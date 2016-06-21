using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public class SPSkylineBNL : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        [SqlProcedure(Name = "prefSQL_SkylineBNL")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNL skyline = new SPSkylineBNL();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }


        protected override void AddToWindow(object[] newTuple, List<long[]> window, ArrayList resultstringCollection, string[] operators, int[] dimensions, DataTable dtResult)
        {
            Helper.AddToWindowIncomparable(newTuple, window, operators, resultstringCollection, dtResult);
        }

        protected override bool IsTupleDominated(List<long[]> window, long[] newTuple, int[] dimensions, string[] operators, ArrayList incomparableTuples, int listIndex, DataTable dtResult, object[] newTupleAllValues)
        {
            //long?[] result = (long?[])resultCollection[i];
            long[] windowTuple = window[listIndex];
            string[] incomparableTuple = (string[])incomparableTuples[listIndex];


            //Dominanz
            if (Helper.IsTupleDominated(windowTuple, newTuple, dimensions, operators, incomparableTuple, newTupleAllValues))
            {
                //New point is dominated. No further testing necessary
                return true;
            }

            //Now, check if the new point dominates the one in the window
            //This is only possible with not sorted data
            if (Helper.DoesTupleDominate(windowTuple, newTuple, dimensions, operators, incomparableTuple, newTupleAllValues))
            {             
                //The new record dominates the one in the windows. Remove point from window and test further             
                window.RemoveAt(listIndex);
                incomparableTuples.RemoveAt(listIndex);
                dtResult.Rows.RemoveAt(listIndex);
            }
            return false;
        }

        
    }
}