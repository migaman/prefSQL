//Caution: Attention small changes in this code can lead to performance issues, i.e. using a startswith instead of an equal can increase by 10 times
//Important: Only use equal for comparing text (otherwise performance issues)

using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace prefSQL.SQLSkyline
{

    public class SP_SkylineDQ : TemplateDQ
    {
        [SqlProcedure(Name = "SP_SkylineDQ")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords)
        {
            SP_SkylineDQ skyline = new SP_SkylineDQ();
            skyline.GetSkylineTable(strQuery.ToString(), strOperators.ToString(), numberOfRecords.Value, false, Helper.CnnStringSqlclr, Helper.ProviderClr);
        }

    }
}
