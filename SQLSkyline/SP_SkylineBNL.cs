using System.Collections;
using System.Data;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{
    public class SP_SkylineBNL : TemplateBNL
    {
        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        [SqlProcedure(Name = "SP_SkylineBNL")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineBNL skyline = new SP_SkylineBNL();
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
            //This is only possible with not sorted data
            if (Helper.DoesTupleDominate(dataReader, operators, result, strResult))
            {
                //The new record dominates the one in the windows. Remove point from window and test further
                resultCollection.RemoveAt(i);
                resultstringCollection.RemoveAt(i);
                dtResult.Rows.RemoveAt(i);
            }
            return false;
        }

        
    }
}