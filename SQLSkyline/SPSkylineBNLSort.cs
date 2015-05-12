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
        [SqlProcedure(Name = "SP_SkylineBNLSort")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNLSort skyline = new SPSkylineBNLSort();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }

        protected override void AddToWindow(object[] dataReader, List<long[]> resultCollection, ArrayList resultstringCollection, string[] operators, int dimensions, DataTable dtResult)
        {
            Helper.AddToWindow(dataReader, operators, resultCollection, resultstringCollection, dtResult);
        }


        protected override bool IsTupleDominated(long[] windowTuple, long[] newTuple, int dimensions, string[] operators, ArrayList incomparableTuples, int listIndex)
        {
            string[] incomparableTuple = (string[])incomparableTuples[listIndex];

            //Dominanz
            if (Helper.IsTupleDominated(windowTuple, newTuple, dimensions, operators, incomparableTuple))
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