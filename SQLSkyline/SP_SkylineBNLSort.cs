using System.Collections;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

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
        [SqlProcedure(Name = "SP_SkylineBNLSort")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineBNLSort skyline = new SP_SkylineBNLSort();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr);
        }

        protected override void AddtoWindow(object[] dataReader, string[] operators, ArrayList resultCollection, ArrayList resultstringCollection, SqlDataRecord record, bool isFrameworkMode, DataTable dtResult)
        {
            Helper.AddToWindow(dataReader, operators, resultCollection, resultstringCollection, record, dtResult);
        }

        protected override bool TupleDomination(object[] dataReader, ArrayList resultCollection, ArrayList resultstringCollection, string[] operators, DataTable dtResult, int i, int[] resultToTupleMapping)
        {
            long?[] result = (long?[])resultCollection[i];
            string[] strResult = (string[])resultstringCollection[i];

            //Dominanz
            if (Helper.IsTupleDominated(operators, result, strResult, dataReader))
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