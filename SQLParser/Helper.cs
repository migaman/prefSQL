using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text.RegularExpressions;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;

namespace prefSQL.SQLParser
{
    using SQLSkyline.SkylineSampling;
    using System.Globalization;

    internal class Helper
    {
        /// <summary>
        /// Driver-String, i.e. System.Data.SqlClient
        /// </summary>
        public String DriverString { get; set; }  
        /// <summary>
        /// Connectionstring, i.e. Data Source=myserver;Initial Catalog=eCommerce;Integrated Security=True
        /// </summary>
        public String ConnectionString { get; set; }

        public long TimeInMilliseconds { get; set; }

        public int WindowHandling { get; set; }

        public long NumberOfComparisons { get; set; }

        public long NumberOfMoves { get; set; }

        public long Cardinality { get; set; }

        public DataTable ExecuteStatement(String strSQL)
        {
            DataTable dt = new DataTable();

            //Generic database provider
            //Create the provider factory from the namespace provider, you could create any other provider factory.. for Oracle, MySql, etc...
            DbProviderFactory factory = DbProviderFactories.GetFactory(DriverString);

            // use the factory object to create Data access objects.
            DbConnection connection = factory.CreateConnection(); // will return the connection object, in this case, SqlConnection ...
            if (connection != null)
            {
                connection.ConnectionString = ConnectionString;

                connection.Open();
                DbCommand command = connection.CreateCommand();
                command.CommandTimeout = 0; //infinite timeout
                command.CommandText = strSQL;
                DbDataAdapter db = factory.CreateDataAdapter();
                if (db != null)
                {
                    db.SelectCommand = command;
                    db.Fill(dt);
                }
            }

            return dt;
        }


        /// <summary>
        /// Returns a datatable with the tuples from the SQL statement
        /// The sql will be resolved into pieces, in order to call the Skyline algorithms without MSSQL CLR 
        /// </summary>
        /// <param name="strPrefSQL"></param>
        /// <param name="strategy"></param>
        /// <param name="model"></param>
        /// <returns></returns>
 
        public DataTable GetResults(String strPrefSQL, SkylineStrategy strategy, PrefSQLModel model, bool ShowInternalAttributes)
        {     
            DataTable dt = new DataTable();
            //Default Parameter
            string strQuery = "";
            string strOperators = "";
            int numberOfRecords = 0;
            string[] parameter = null;


            //Native SQL algorithm is already a valid SQL statement
            //Trim prefSQL because of queries starting wit empty characters " SELECT ...."
            if (strPrefSQL.Trim().StartsWith("SELECT", true, null))
            {
                if (model == null || !model.HasSkylineSample)
                {
                    //If query doesn't need skyline calculation (i.e. query without preference clause) --> set algorithm to nativeSQL
                    strategy = new SkylineSQL();
                }              
                else
                {
                    throw new Exception("native SQL not yet supported."); // TODO: consider native SQL support
                }
            }
            else
            {
                //Determine parameter only with skyline of clause and not with weihtedsum clause
                DetermineParameters(strPrefSQL, out parameter, out strQuery, out strOperators, out numberOfRecords);
                
            }

            try
            {
                if (model != null && model.Ranking.Count > 0)
                {
                    SPRanking ranking = new SPRanking();
                    ranking.Provider = DriverString;
                    ranking.ConnectionString = ConnectionString;
                    string strSelectExtremas = "";
                    string strRankingWeights = "";
                    string strRankingExpressions = "";
                    string strColumnNames = "";
                    // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
                    NumberFormatInfo format = new NumberFormatInfo();
                    format.NumberDecimalSeparator = ".";

                    foreach (RankingModel rankingModel in model.Ranking)
                    {
                        strSelectExtremas += rankingModel.SelectExtrema + ";";
                        strRankingWeights += rankingModel.Weight.ToString(format) + ";";
                        strRankingExpressions += rankingModel.Expression + ";";
                        strColumnNames += rankingModel.FullColumnName.Replace(".", "_") + ";";
                    }
                    strSelectExtremas = strSelectExtremas.TrimEnd(';');
                    strRankingWeights = strRankingWeights.TrimEnd(';');
                    strRankingExpressions = strRankingExpressions.TrimEnd(';');

                    dt = ranking.GetRankingTable(strQuery, strSelectExtremas, strRankingWeights, strRankingExpressions, ShowInternalAttributes, strColumnNames);
                }
                else if (strategy.IsNative())
                {
                    if (model == null || !model.HasSkylineSample)
                    {
                        //Native SQL

                        //Generic database provider
                        //Create the provider factory from the namespace provider, you could create any other provider factory.. for Oracle, MySql, etc...
                        DbProviderFactory factory = DbProviderFactories.GetFactory(DriverString);

                        // use the factory object to create Data access objects.
                        DbConnection connection = factory.CreateConnection(); // will return the connection object, in this case, SqlConnection ...
                        if (connection != null)
                        {
                            connection.ConnectionString = ConnectionString;

                            connection.Open();
                            DbCommand command = connection.CreateCommand();
                            command.CommandText = strPrefSQL;
                            DbDataAdapter db = factory.CreateDataAdapter();
                            if (db != null)
                            {
                                db.SelectCommand = command;
                                db.Fill(dt);
                            }
                        }
                    }
                    else
                    {
                        throw new Exception("native SQL not yet supported."); // TODO: consider native SQL support
                    }
                }
                
                else
                {
                    if (strategy.SupportImplicitPreference() == false && model.ContainsOpenPreference)
                    {
                        throw new Exception(strategy.GetType() + " does not support implicit preferences!");
                    }
                    if (strategy.SupportIncomparable() == false && model.WithIncomparable)
                    {
                        throw new Exception(strategy.GetType() + " does not support incomparale tuples");
                    }

                    //Set the database provider
                    strategy.Provider = DriverString;
                    strategy.ConnectionString = ConnectionString;
                    strategy.Cardinality = Cardinality;
                    strategy.WindowHandling = WindowHandling;
                    strategy.RecordAmountLimit = numberOfRecords;
                    strategy.HasIncomparablePreferences = model.WithIncomparable;
                    strategy.AdditionParameters = parameter;
                    strategy.SortType = (int)model.Ordering; 
                    if (!model.HasSkylineSample)
                    {
                        
                        dt = strategy.GetSkylineTable(strQuery, strOperators);
                        TimeInMilliseconds = strategy.TimeMilliseconds;
                        NumberOfComparisons = strategy.NumberOfComparisons;
                        NumberOfMoves = strategy.NumberOfMoves;
                    }
                    else
                    {
                        var skylineSample = new SkylineSampling
                        {
                            SubsetCount = model.SkylineSampleCount,
                            SubsetDimension = model.SkylineSampleDimension,
                            SelectedStrategy = strategy
                        };
                        dt = skylineSample.GetSkylineTable(strQuery, strOperators);
                        TimeInMilliseconds = skylineSample.TimeMilliseconds;
                        //NumberOfOperations = skylineSample.NumberOfOperations;
                    }
                }

            }
            catch (Exception e)
            {
                Debug.WriteLine(e.Message);
                throw;
            }

            return dt;            
        }

        internal static void DetermineParameters(string strPrefSQL, out string[] parameter, out string strQuery, out string strOperators,
            out int numberOfRecords)
        {
            //Store Parameters in Array (Take care to single quotes inside parameters)
            int iPosStart = strPrefSQL.IndexOf("'", StringComparison.Ordinal);
            String strtmp = strPrefSQL.Substring(iPosStart);
            parameter = Regex.Split(strtmp, ",(?=(?:[^']*'[^']*')*[^']*$)");

            //All other algorithms are developed as stored procedures
            //Resolve now each parameter from this SP calls to single pieces

            //Default parameter
            strQuery = parameter[0].Trim();
            strOperators = parameter[1].Trim();
            numberOfRecords = int.Parse(parameter[2].Trim());
            strQuery = strQuery.Replace("''", "'").TrimStart('\'');
            strQuery = strQuery.EndsWith("'") ? strQuery.Remove(strQuery.Length - 1) : strQuery;
            strOperators = strOperators.Replace("''", "'").Trim('\'');
        }
    }
}
