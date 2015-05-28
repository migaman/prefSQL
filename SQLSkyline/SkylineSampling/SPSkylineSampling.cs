namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Data;
    using System.Data.SqlTypes;
    using Microsoft.SqlServer.Server;

    public class SPSkylineSampling
    {
        [SqlProcedure(Name = "SP_SkylineSampling")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords,
            SqlInt32 sortType, SqlInt32 count, SqlInt32 dimension, SqlString algorithm, SqlBoolean hasIncomparable)
        {
            var str = "x";
            try
            {
                str += "y"+algorithm.ToString(); 
                Type strategyType = Type.GetType("prefSQL.SQLSkyline."+algorithm.ToString());
                str += strategyType.ToString();
                if (!typeof(SkylineStrategy).IsAssignableFrom(strategyType))
                {
                    throw new Exception("passed algorithm is not of type SkylineStrategy.");
                }

                str += "1";
                var strategy = (SkylineStrategy) Activator.CreateInstance(strategyType);
                str += "2";

                strategy.Provider = Helper.ProviderClr;
                strategy.ConnectionString = Helper.CnnStringSqlclr;
                strategy.RecordAmountLimit = numberOfRecords.Value;
                strategy.HasIncomparablePreferences = hasIncomparable.Value;
                strategy.SortType = sortType.Value;

                var skylineSample = new SkylineSampling
                {
                    SubspacesCount = count.Value,
                    SubspaceDimension = dimension.Value,
                    SelectedStrategy = strategy
                };

                str += "X";
                DataTable dataTableReturn = skylineSample.GetSkylineTable(strQuery.ToString(), strOperators.ToString());
                str += "Y";
                SqlDataRecord dataRecordTemplate = skylineSample.DataRecordTemplate;

                if (SqlContext.Pipe != null)
                {
                    SqlContext.Pipe.SendResultsStart(dataRecordTemplate);

                    foreach (DataRow recSkyline in dataTableReturn.Rows)
                    {
                        for (var i = 0; i < recSkyline.Table.Columns.Count; i++)
                        {
                            dataRecordTemplate.SetValue(i, recSkyline[i]);
                        }
                        SqlContext.Pipe.SendResultsRow(dataRecordTemplate);
                    }
                    SqlContext.Pipe.SendResultsEnd();
                }
            }
            catch (Exception ex)
            {
                //Pack Errormessage in a SQL and return the result
                var strError = "Error in SP_SkylineSampling: ";
                strError += ex.Message+str+ex.StackTrace;

                if (SqlContext.Pipe != null)
                {
                    SqlContext.Pipe.Send(strError);
                }
            }
        }
    }
}