using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)
namespace prefSQL.SQLSkyline
{
    public class SPSkylineBNLSortLevel : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        [SqlProcedure(Name = "SP_SkylineBNLSortLevel")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords, SqlInt32 sortType)
        {
            SPSkylineBNLSortLevel skyline = new SPSkylineBNLSortLevel();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr, null, sortType.Value);
        }

        protected override void AddToWindow(object[] dataReader, List<long[]> resultCollection, ArrayList resultstringCollection, string[] operators,
            int dimensions, DataTable dtResult)
        {
            Helper.AddToWindow(dataReader, resultCollection, dimensions, dtResult);
            resultstringCollection.Add(new string[1]);
        }


        protected override bool IsTupleDominated(long[] windowTuple, long[] newTuple, int dimensions, string[] operators, ArrayList incomparableTuple, int listIndex)
        {
            //Dominanz
            return Helper.IsTupleDominated(windowTuple, newTuple, dimensions);
            

            //Now, check if the new point dominates the one in the window
            //--> It is not possible that the new point dominates the one in the window --> Reason data is ORDERED

        }

        
    }
}