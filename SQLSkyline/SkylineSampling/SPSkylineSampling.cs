namespace prefSQL.SQLSkyline.SkylineSampling
{
    using System;
    using System.Data;
    using System.Data.SqlTypes;
    using Microsoft.SqlServer.Server;

    public sealed class SPSkylineSampling
    {
        [SqlProcedure(Name = "SP_SkylineSampling")]
        public static void GetSkyline(SqlString strQuery, SqlString strOperators, SqlInt32 numberOfRecords,
            SqlInt32 sortType, SqlInt32 count, SqlInt32 dimension, SqlString algorithm, SqlBoolean hasIncomparable)
        {
            try
            {
                Type strategyType = Type.GetType("prefSQL.SQLSkyline." + algorithm.ToString());

                if (!typeof (SkylineStrategy).IsAssignableFrom(strategyType))
                {
                    throw new Exception("passed algorithm is not of type SkylineStrategy.");
                }

                var strategy = (SkylineStrategy) Activator.CreateInstance(strategyType);

                strategy.Provider = Helper.ProviderClr;
                strategy.ConnectionString = Helper.CnnStringSqlclr;
                strategy.RecordAmountLimit = numberOfRecords.Value;
                strategy.HasIncomparablePreferences = hasIncomparable.Value;
                strategy.SortType = sortType.Value;

                var skylineSample = new SkylineSampling
                {
                    SubsetCount = count.Value,
                    SubsetDimension = dimension.Value,
                    SelectedStrategy = strategy
                };

                DataTable dataTableReturn = skylineSample.GetSkylineTable(strQuery.ToString(), strOperators.ToString());
                SqlDataRecord dataRecordTemplate = skylineSample.DataRecordTemplateForStoredProcedure;

                //throw new Exception(dataTableReturn.Rows[0].Table.Columns.Count.ToString());

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
                strError += ex.Message;

                if (SqlContext.Pipe != null)
                {
                    SqlContext.Pipe.Send(strError);
                }
            }
        }
    }
}