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
        [SqlProcedure(Name = "SP_SkylineBNLLevel")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNLLevel skyline = new SPSkylineBNLLevel();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }



        protected override void AddToWindow(object[] dataReader, List<long[]> resultCollection, ArrayList resultstringCollection, string[] operators, int dimensions, DataTable dtResult)
        {
            Helper.AddToWindowSample(dataReader, operators, resultCollection, dtResult);
        }


        protected override bool IsTupleDominated(long[] windowTuple, long[] newTuple, int dimensions, string[] operators, ArrayList incomparableTuple, int listIndex)
        {
            //incomparableTuples
            //Dominanz
            if (Helper.IsTupleDominated(windowTuple, newTuple, dimensions))
            {
                //New point is dominated. No further testing necessary
                return true;
            }

            //Now, check if the new point dominates the one in the window
            //This is only possible with not sorted data
            /*if (Helper.DoesTupleDominate(dataReader, operators, result, resultToTupleMapping, result.GetUpperBound((0))))
            {
                //The new record dominates the one in the windows. Remove point from window and test further
                resultCollection.RemoveAt(i);
                dtResult.Rows.RemoveAt(i);
            }*/
            return false;
        }


    }
}