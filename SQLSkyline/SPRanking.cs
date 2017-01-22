using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Globalization;


namespace prefSQL.SQLSkyline
{
    
    public class SPRanking
    {
        public string Provider { get; set; }

        public string ConnectionString { get; set; }

        public int RecordAmountLimit { get; set; }

        public Boolean ShowInternalAttributes { get; set; }


        /// <summary>
        /// Calculate the skyline points from a dataset
        /// </summary>
        /// <param name="strQuery"></param>
        /// <param name="strOperators"></param>
        /// <param name="numberOfRecords"></param>
        /// <param name="sortType"></param>
        [SqlProcedure(Name = "prefSQL_Ranking")]
        public static void GetRanking(SqlString strQuery, SqlString strSelectExtremas, SqlInt32 numberOfRecords, SqlString strRankingWeights, SqlString strRankingExpressions, SqlBoolean showInternalAttr, SqlString strColumnNames)
        {
            SPRanking ranking = new SPRanking();
            ranking.Provider = Helper.ProviderClr;
            ranking.ConnectionString = Helper.CnnStringSqlclr;
            ranking.RecordAmountLimit = numberOfRecords.Value;
            ranking.ShowInternalAttributes = showInternalAttr.Value;
            ranking.GetRankingTable(strQuery.ToString(), false, strSelectExtremas.ToString(), strRankingWeights.ToString(), strRankingExpressions.ToString(), strColumnNames.ToString());
        }

        public DataTable GetRankingTable(string strInput, string strSelectExtremas, string strRankingWeights, string strRankingExpressions, Boolean showInternalAttr, string strColumnNames)
        {
            ShowInternalAttributes = showInternalAttr;
            return GetRankingTable(strInput, true, strSelectExtremas, strRankingWeights, strRankingExpressions, strColumnNames);
        }

        private DataTable GetRankingTable(string strInput, bool isIndependent, string strSelectExtremas, string strRankingWeights, string strRankingExpressions, string strColumnNames)
        {
            string[] selectExtremas = strSelectExtremas.Split(';');
            string[] rankingWeights = strRankingWeights.Split(';');
            string[] rankingExpressions = strRankingExpressions.Split(';');
            string[] columnNames = strColumnNames.Split(';');

            DataTable dtResult = new DataTable();
            string strQuery = CalculateMaxima(strInput, ConnectionString, Provider, selectExtremas, rankingWeights, rankingExpressions, columnNames);
            dtResult = Helper.executeSQL(strQuery, ConnectionString, Provider);

            //Send results if working with the CLR
            if (isIndependent == false)
            {

                if (SqlContext.Pipe != null)
                {
                    Helper.SendDataTableOverPipe(dtResult);
                }
            }


            return dtResult;
        }

        

        public string GetStoredProcedureCommand(string strQuery, string strSelectExtremas, string strRankingWeights, string strRankingExpressions, string strColumnNames)
        {
            //Quote quotes because it is a parameter of the stored procedure
            strQuery = strQuery.Replace("'", "''");
            strSelectExtremas = strSelectExtremas.Replace("'", "''");
            strRankingExpressions = strRankingExpressions.Replace("'", "''");
            strColumnNames = strColumnNames.Replace("'", "''");

            string strSQLReturn;
            strSQLReturn = "EXEC dbo.prefSQL_Ranking '" + strQuery + "', '" + strSelectExtremas + "', " + RecordAmountLimit + ", '" + strRankingWeights + "', '" + strRankingExpressions + "', " + ShowInternalAttributes + ", '" + strColumnNames + "'"; 
            
            return strSQLReturn;
        }


        private string CalculateMaxima(string strInput,  string strConnection, string strProvider ,string[] selectExtremas, string[] rankingWeights, string[] rankingExpressions, string[] columnNames)
        {
            string strSQLReturn = ""; //The SQL-Query that is built on the basis of the prefSQL 

            //Add all Syntax before the ORDER BY WEIGHTEDSUM-Clause
            strSQLReturn = strInput.Substring(0, strInput.IndexOf("ORDER BY WEIGHTEDSUM", StringComparison.OrdinalIgnoreCase) - 1);

            // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
            NumberFormatInfo format = new NumberFormatInfo();
            format.NumberDecimalSeparator = ".";

            string strInternalSelectList = "";

            //Create  ORDER BY clause with help of the ranking model
            string strOrderBy = "ORDER BY ";
            bool bFirst = true;
            for (int i = 0; i < selectExtremas.Length; i++)
            {
                //Read min and max value of the preference
                DataTable dt = Helper.executeSQL(selectExtremas[i], strConnection, strProvider);
                string strMin;
                string strDividor;

                //Do correct unboxing from datatable
                if (dt.Columns[0].DataType == typeof(Int32))
                {
                    double min = (int)dt.Rows[0][0];
                    double max = (int)dt.Rows[0][1];

                    //Write at least one decimal (in order SQL detects the number as double. Otherwise the result will be int values!!)
                    strMin = string.Format(format, "{0:0.0###########}", min);
                    strDividor = string.Format(format, "{0:0.0###########}", max - min);
                }
                else if (dt.Columns[0].DataType == typeof(Int64))
                {
                    double min = (long)dt.Rows[0][0];
                    double max = (long)dt.Rows[0][1];

                    //Write at least one decimal (in order SQL detects the number as double. Otherwise the result will be int values!!)
                    strMin = string.Format(format, "{0:0.0###########}", min);
                    strDividor = string.Format(format, "{0:0.0###########}", max - min);
                }
                else
                {
                    throw new Exception("New Datatype detected. Please develop unboxing for this first!!");
                }


                //Create Normalization Formula, No Delta is needed
                //(Weight * (((attributevalue - minvalue) / (maxvalue-minvalue))))
                //For example: 0.2 * ((t1.price - 900.0) / 288100.0) + 0.01 AS Norm1
                string strNormalization = "(" + rankingWeights[i].ToString(format) + " * (((" + rankingExpressions[i] + " - " + strMin + ") / " + strDividor + ") ))";


                strInternalSelectList = strInternalSelectList + ", (((" + rankingExpressions[i] + " - " + strMin + ") / " + strDividor + ") ) AS RankingAttribute" + columnNames[i];

                //Mathematical addition except for the first element
                if (bFirst)
                {
                    bFirst = false;
                    strOrderBy += strNormalization;
                }
                else
                {
                    strOrderBy += " + " + strNormalization;
                }
            }


            //Add Skyline Attributes to select list. This option is i.e. useful to create a dominance graph.
            //With help of the skyline values it is easier to create this graph
            if (ShowInternalAttributes)
            {
                //Add the attributes to the existing SELECT clause
                string strSQLSelectClause = strInternalSelectList;
                string strSQLBeforeFrom = strSQLReturn.Substring(0, strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1);
                string strSQLAfterFromShow = strSQLReturn.Substring(strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1);
                strSQLReturn = strSQLBeforeFrom + strSQLSelectClause + " " + strSQLAfterFromShow;

            }


            //Add the OrderBy clause to the new SQL Query
            strSQLReturn += " " + strOrderBy;


            return strSQLReturn;
        }

       


    }
}