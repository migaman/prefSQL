using System;
using System.Data;
using System.Data.Common;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using prefSQL.SQLParser.Models;
using prefSQL.SQLSkyline;
using prefSQL.SQLSkyline.SkylineSampling;
using System.Globalization;
using prefSQL.Grammar;

namespace prefSQL.SQLParser
{

    /// <summary>
    /// Entry point of the library for parsing a PREFERENCE SQL to an ANSI-SQL Statement
    /// </summary>
    /// <remarks>
    /// You can choose the Skyline algorithm, the maxium level in multiple skyline algorithms and if the SELECT List should be extended with the Skyline Values
    /// </remarks>
    public class SQLCommon
    {
        private const string SkylineOf = "SKYLINE OF";
        private SkylineStrategy _skylineType = new SkylineSQL();    //Defines with which Algorithm the Skyline should be calculated
        private int _skylineUpToLevel = 3;                          //Defines the maximum level that should be returned for the multiple skyline algorithnmm
        private readonly Helper _helper = new Helper();

        public long Cardinality {get; set;}

        internal Helper Helper {
            get { return _helper;} 
        }
        public long TimeInMilliseconds { get; set; }

        public long NumberOfComparisons { get; set; }

        public long NumberOfMoves { get; set; }

        public int WindowHandling { get; set; }

        public Ordering WindowSort { get; set; }


        /*
        public enum Algorithm
        {
            NativeSQL,              //Works with ANSI-SQL syntax
            BNL,                    //Block nested loops
            BNLSort,                //Block nested loops with presort
            DQ,                     //Divide and Conquer
            Hexagon,                //Hexagon Augsburg
            MultipleBNL,            //Multiple Skyline
        };*/

        public enum Ordering
        {
            AttributePosition,      //Sorted according to the attribute position in the select list
            RankingSummarize,       //Sorted according to the the sum of the rank of all attributes
            RankingBestOf,          //Sorted according to the best rank of all attributes
            Random,                 //Randomly sorted, Every query results in a different sort order
            AsIs,                   //Without OrderBy-Clause as it comes from the database
            EntropyFunction         //For testing BNL window with entropy function
        }

        public bool ShowInternalAttributes { get; set; }

        public SkylineStrategy SkylineType
        {
            get { return _skylineType; }
            set { _skylineType = value; }
        }

        public int SkylineUpToLevel
        {
            get { return _skylineUpToLevel; }
            set { _skylineUpToLevel = value; }
        }

        /// <summary>
        /// Parse a prefSQL Query and return the result as a DataTable.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="driverString"></param>
        /// <param name="strPrefSql"></param>
        /// <returns>Returns a DataTable with the requested values</returns>
        public DataTable ParseAndExecutePrefSQL(string connectionString, string driverString, String strPrefSql)
        {
            DataTable dt = new DataTable();
            try
            {
            
                //return ParseAndExecutePrefSQL(connectionString, driverString, GetPrefSqlModelFromPreferenceSql(strPrefSql));


                //Do not build 2 times the model!
                Helper.ConnectionString = connectionString;
                Helper.DriverString = driverString;
                Helper.Cardinality = Cardinality;
                Helper.WindowHandling = WindowHandling;

                PrefSQLModel model = GetPrefSqlModelFromPreferenceSql(strPrefSql);
                string sqlCommand = ParsePreferenceSQL(strPrefSql, model);


                dt = Helper.GetResults(sqlCommand, SkylineType, model, ShowInternalAttributes);
                TimeInMilliseconds = Helper.TimeInMilliseconds;
                NumberOfComparisons = Helper.NumberOfComparisons;
                NumberOfMoves = Helper.NumberOfMoves;
            }
            catch (Exception e)
            {
                throw e;
            }
            return dt;

        }

        internal DataTable ParseAndExecutePrefSQL(string connectionString, string driverString, PrefSQLModel prefSqlModel)
        {
            Helper.ConnectionString = connectionString;
            Helper.DriverString = driverString;
            Helper.Cardinality = Cardinality;
            Helper.WindowHandling = WindowHandling;
            DataTable dt = Helper.GetResults(ParsePreferenceSQL(prefSqlModel), SkylineType, prefSqlModel, ShowInternalAttributes);
            TimeInMilliseconds = Helper.TimeInMilliseconds;
            NumberOfComparisons = Helper.NumberOfComparisons;
            NumberOfMoves = Helper.NumberOfMoves;

            return dt;
        }

        /// <summary>Parses a PREFERENE SQL Statement in an ANSI SQL Statement</summary>
        /// <param name="strInput">Preference SQL Statement</param>
        /// <returns>Return the ANSI SQL Statement</returns>
        public string ParsePreferenceSQL(string strInput)
        {
            return ParsePreferenceSQL(strInput, null);
        }

        internal string ParsePreferenceSQL(PrefSQLModel prefSQL)
        {
            return ParsePreferenceSQL(prefSQL.OriginalPreferenceSql, prefSQL);
        }

        /// <summary>
        ///  Parses a PREFERENE SQL Statement in an ANSI SQL Statement
        /// </summary>
        /// <param name="strInput"></param>
        /// <param name="prefSQLParam"></param>
        /// <returns>Return the ANSI SQL Statement</returns>
        /// <exception cref="Exception">This is exception is thrown because the String is not a valid PrefSQL Query</exception>
       
        internal string ParsePreferenceSQL(string strInput, PrefSQLModel prefSQLParam)
        {
            SQLSort sqlSort = new SQLSort();
            SQLCriterion sqlCriterion = new SQLCriterion();
            string strSQLReturn = ""; //The SQL-Query that is built on the basis of the prefSQL 
            PrefSQLModel prefSQL = prefSQLParam ?? GetPrefSqlModelFromPreferenceSql(strInput);

            try
            {                            
                //Check if parse was successful and query contains PrefSQL syntax
                if (prefSQL != null) // && strInput.IndexOf(SkylineOf) > 0
                {
                    if (prefSQL.Skyline.Count > 0)
                    {
                        //Mark as incomparable if needed (to choose the correct algorithm)
                        //withIncomparable = prefSQL.WithIncomparable;

                        //Add all Syntax before the Skyline-Clause
                        strSQLReturn = strInput.Substring(0, strInput.IndexOf(SkylineOf, StringComparison.OrdinalIgnoreCase) - 1).TrimStart(' ');

                        //Add Skyline Attributes to select list. This option is i.e. useful to create a dominance graph.
                        //With help of the skyline values it is easier to create this graph
                        if (ShowInternalAttributes)
                        {
                            //Add the attributes to the existing SELECT clause
                            string strSQLSelectClause = GetSelectClauseForSkylineAttributes(prefSQL);
                            string strSQLBeforeFrom = strSQLReturn.Substring(0, strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1);
                            string strSQLAfterFromShow = strSQLReturn.Substring(strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1);
                            strSQLReturn = strSQLBeforeFrom + strSQLSelectClause + " " + strSQLAfterFromShow;

                        }

                        //Add ORDER BY Clause
                        string strOrderBy = "";
                        if (strInput.IndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase) > 0)
                        {
                            if (prefSQL.Ordering == Ordering.AsIs)
                            {
                                string strTmpInput = strInput;

                                //Replace category clauses
                                //Start with latest order by (otherwise substring start, stop position are changed)
                                for (int iIndex = prefSQL.OrderBy.Count - 1; iIndex >= 0; iIndex--)
                                {
                                    OrderByModel model = prefSQL.OrderBy[iIndex];
                                    strTmpInput = strTmpInput.Substring(0, model.Start) + model.Text + strTmpInput.Substring(model.Stop);
                                }

                                strOrderBy = strTmpInput.Substring(strInput.IndexOf(" ORDER BY ", StringComparison.OrdinalIgnoreCase)+1);
                            }
                            else
                            {
                                strOrderBy = sqlSort.GetSortClause(prefSQL, prefSQL.Ordering); // sqlSort.getSortClause(prefSQL, _OrderType);
                            }
                            //Add space charachter in front of ORDER BY if not already present
                            if (!strOrderBy.Substring(0, 1).Equals(" "))
                            {
                                strOrderBy = " " + strOrderBy;
                            }
                            
                        }


                        ////////////////////////////////////////////
                        //attributes used for native sql algorithm
                        string strWhere = sqlCriterion.GetCriterionClause(prefSQL, strSQLReturn);

                        ////////////////////////////////////////////
                        //attributes used for other algorithms
                        string strOperators;
                        string strAttributesSkyline = BuildPreferencesBNL(prefSQL, out strOperators);
                        //Without SELECT 

                        //Remove TOP keyword, expect for the native SQL algorithm
                        if (prefSQL.NumberOfRecords != 0 && SkylineType.IsNative() == false)
                        {
                            //Remove Top Keyword in inner clause
                            int iPosTop = strSQLReturn.IndexOf(" TOP ", StringComparison.OrdinalIgnoreCase)+1;
                            int iPosTopEnd = strSQLReturn.Substring(iPosTop + 3).TrimStart().IndexOf(" ", StringComparison.Ordinal);
                            string strSQLAfterTop = strSQLReturn.Substring(iPosTop + 3).TrimStart();
                            strSQLReturn = strSQLReturn.Substring(0, iPosTop) + strSQLAfterTop.Substring(iPosTopEnd + 1);
                        }


                        string strAttributesOutput = ", " + strSQLReturn.Substring(7, strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase) - 6);
                        string strSQLAfterFrom = strSQLReturn.Substring(strSQLReturn.IndexOf(" FROM ", StringComparison.OrdinalIgnoreCase)+1);

                        string strFirstSQL = "SELECT " + strAttributesSkyline + " " + strAttributesOutput + strSQLAfterFrom;
                        if (SkylineType.IsNative())
                        {
                            strFirstSQL = strSQLReturn;
                        }

                        string strOrderByAttributes = sqlSort.GetSortClause(prefSQL, WindowSort);


                        ////////////////////////////////////////////
                        //attributes used for hexagon
                        string[] additionalParameters = new string[6];

                        string strOperatorsHexagon;
                        string strAttributesSkylineHexagon = BuildSelectHexagon(prefSQL, out strOperatorsHexagon);
                        //Without SELECT 


                        //Quote quotes because it is a parameter of the stored procedure
                        string strFirstSQLHexagon = "SELECT " + strAttributesSkylineHexagon + " " + strAttributesOutput + strSQLAfterFrom;
                        strFirstSQLHexagon = strFirstSQLHexagon.Replace("'", "''");

                        //Quote quotes because it is a parameter of the stored procedure
                        //string strSelectDistinctIncomparable = "";
                        string weightHexagonIncomparable = "";
                        string strSelectDistinctIncomparable = BuildIncomparableHexagon(prefSQL, ref weightHexagonIncomparable);
                        strSelectDistinctIncomparable = strSelectDistinctIncomparable.Replace("'", "''");

                        additionalParameters[0] = strFirstSQLHexagon;
                        additionalParameters[1] = strOperatorsHexagon;
                        additionalParameters[2] = strSelectDistinctIncomparable;
                        additionalParameters[3] = weightHexagonIncomparable;
                        
                        _skylineType.SortType = (int)prefSQL.Ordering;
                        _skylineType.RecordAmountLimit = prefSQL.NumberOfRecords;
                        _skylineType.MultipleSkylineUpToLevel = _skylineUpToLevel;
                        _skylineType.AdditionParameters = additionalParameters;
                        _skylineType.HasIncomparablePreferences = prefSQL.WithIncomparable;

                        //Now create the query depending on the Skyline algorithm
                        if (!prefSQL.HasSkylineSample)
                        {
                            strSQLReturn = _skylineType.GetStoredProcedureCommand(strWhere, strOrderBy, strFirstSQL,
                                strOperators, strOrderByAttributes);
                        }
                        else
                        {
                            var skylineSample = new SkylineSampling
                            {
                                SubsetCount = prefSQL.SkylineSampleCount,
                                SubsetDimension = prefSQL.SkylineSampleDimension,
                                SelectedStrategy = _skylineType
                            };
                            strSQLReturn = skylineSample.GetStoredProcedureCommand(strWhere, strOrderBy, strFirstSQL,
                                strOperators, strOrderByAttributes);
                        }
                    }
                    if(prefSQL.Ranking.Count > 0)
                    {
                        if(prefSQL.ContainsOpenPreference)
                        {
                            throw new Exception("WeightedSum cannot handle implicit INCOMPARABLE values. Please add the explicit OTHERS EQUAL to the preference");
                        }

                        string strSelectExtremas = "";
                        string strRankingWeights = "";
                        string strRankingExpressions = "";
                        string strColumnNames = "";

                        // Set the decimal seperator, because prefSQL double values are always with decimal separator "."
                        NumberFormatInfo format = new NumberFormatInfo();
                        format.NumberDecimalSeparator = ".";

                        foreach (RankingModel rankingModel in prefSQL.Ranking)
                        {
                            strSelectExtremas += rankingModel.SelectExtrema + ";";
                            strRankingWeights += rankingModel.Weight.ToString(format) + ";";
                            strRankingExpressions += rankingModel.Expression + ";";
                            strColumnNames += rankingModel.FullColumnName.Replace(".", "_") + ";";
                        }
                        strSelectExtremas = strSelectExtremas.TrimEnd(';');
                        strRankingWeights = strRankingWeights.TrimEnd(';');
                        strRankingExpressions = strRankingExpressions.TrimEnd(';');
                        strColumnNames = strColumnNames.TrimEnd(';');

                        SPRanking spRanking = new SPRanking();
                        spRanking.ShowInternalAttributes = ShowInternalAttributes;
                        //Quoting is done in GetStoredProc Command
                        //string strExecSQL = strInput.Replace("'", "''");
                        strSQLReturn = spRanking.GetStoredProcedureCommand(strInput, strSelectExtremas, strRankingWeights, strRankingExpressions, strColumnNames);
                    }
                }
                else
                {
                    //Query does not contain a preference --> return original query
                    strSQLReturn = strInput;
                }
            }

            catch (Exception e)
            {
                //Parse syntaxerror
                throw new Exception(e.Message);
            }
            
            return strSQLReturn;

        }


        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="strOperators">Returns the operators</param>
        /// <returns>TODO</returns>
        private string BuildSelectHexagon(PrefSQLModel model, out string strOperators)
        {
            strOperators = "";
            string strSQL = "";

            //Add a RankColumn for each PRIORITIZE preference
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //Replace ROW_NUMBER with Rank, for the reason that multiple tuples can have the same value (i.e. mileage=0)
                string strRank = model.Skyline[iChild].RankExpression;
                strSQL += ", " + strRank;
                strOperators += "LOW" + ";";
                if (model.Skyline[iChild].Comparable == false && model.Skyline[iChild].AmountOfIncomparables > 0)
                {
                    strSQL += ", " + model.Skyline[iChild].HexagonIncomparable;
                    if (model.Skyline[iChild].AmountOfIncomparables == 99)
                    {
                        strOperators += "CALCULATEINCOMPARABLE;";
                    }
                    else
                    {
                        //CASE WHEN  colors.name IN ('blau') THEN '001' WHEN colors.name IN ('silver') THEN '010' ELSE '100' END AS RankColorNew
                        for (int iIncomparable = 0; iIncomparable < model.Skyline[iChild].AmountOfIncomparables; iIncomparable++)
                        {
                            strOperators += "INCOMPARABLE;";
                        }
                    }
                }
            }

            //Add the ranked column before the FROM keyword
            strSQL = strSQL.TrimStart(',');
            strOperators = strOperators.TrimEnd(';');

            return strSQL;
        }


        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="weight"></param>
        /// <returns>TODO</returns>
        private string BuildIncomparableHexagon(PrefSQLModel model, ref string weight)
        {
            string strDistinctSelect = "";


            //Add a RankColumn for each PRIORITIZE preference
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {

                //Add additional columns if attribute is incomparable
                if (model.Skyline[iChild].Comparable == false && model.Skyline[iChild].AmountOfIncomparables > 0)
                {
                    //strMaxSQL += "+1";
                    //99 means OTHER INCOMPARABLE --> not clear at the moment how many distinct values exists
                    if (model.Skyline[iChild].AmountOfIncomparables == 99)
                    {
                        strDistinctSelect += model.Skyline[iChild].IncomparableAttribute + ";";
                        weight += model.Skyline[iChild].HexagonWeightIncomparable.ToString() + ";";
                    }
                }
            }
            strDistinctSelect = strDistinctSelect.TrimEnd(';');
            weight = weight.TrimEnd(';');


            return strDistinctSelect;
        }


        /// <summary>This method is used for Pareto Dominance Grahps. It adds the Skyline Attributes to the SELECT List.</summary>
        /// <remarks>
        /// For the reason that comparing the values is easier, smaller values are always better than higher.
        /// Therefore HIGH preferences are multiplied with -1 
        /// Every preference gets 2 output values. the 1st declares the level, the second the exact value if an additional comparison
        /// is needed, because of incomparability
        /// </remarks>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <returns>Return the extended SQL Statement</returns>
        private string GetSelectClauseForSkylineAttributes(PrefSQLModel model)
        {
            string strSQL = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 0)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    string strFullColumnName = model.Skyline[iChild].FullColumnName.Replace(".", "_");
                    strSQL += ", " + model.Skyline[iChild].RankExpression + " AS SkylineAttribute" + strFullColumnName;

                    //Incomparable field --> Add string field
                    if (model.Skyline[iChild].Comparable == false)
                    {
                        strSQL += ", " + model.Skyline[iChild].IncomparableAttribute + " AS SkylineAttributeIncomparable" + strFullColumnName;
                    }

                }
            }

            return strSQL;
        }


        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param>Preference SQL Statement WITHOUT PREFERENCES</param>
        /// <param name="strOperators">Returns the operators</param>
        /// <returns>TODO</returns>
        private string BuildPreferencesBNL(PrefSQLModel model, out string strOperators)
        {
            string strSQL = "";
            strOperators = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 0)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    strSQL += ", " + model.Skyline[iChild].RankExpression + " AS SkylineAttribute" + iChild;
                    strOperators += "LOW;";

                    //Incomparable field --> Add string field
                    if (model.Skyline[iChild].Comparable == false)
                    {
                        strSQL += ", " + model.Skyline[iChild].IncomparableAttribute;
                        strOperators += "INCOMPARABLE" + ";";
                    }
                }
            }

            strOperators = strOperators.TrimEnd(';');
            strSQL = strSQL.TrimStart(',');
            return strSQL;
        }

        internal PrefSQLModel GetPrefSqlModelFromPreferenceSql(string preferenceSql)
        {
            PrefSQLParser parser = new PrefSQLParser(new CommonTokenStream(new PrefSQLLexer(new AntlrInputStream(preferenceSql))));

            // An error listener helps to return detailed parser syntax errors
            ErrorListener listener = new ErrorListener();
            parser.AddErrorListener(listener);

            IParseTree tree = parser.parse();
            
            // PrefSQLModel is built during the visit of the parse tree
            SQLVisitor visitor = new SQLVisitor {IsNative = _skylineType.IsNative()};
            visitor.Visit(tree);
            PrefSQLModel prefSql = visitor.Model;
            if (prefSql != null)
            {
                prefSql.OriginalPreferenceSql = preferenceSql;
            }

            return prefSql;
        }

        internal string GetAnsiSqlFromPrefSqlModel(PrefSQLModel prefSqlModel)
        {
            return ParsePreferenceSQL(prefSqlModel.OriginalPreferenceSql, prefSqlModel);
        }

        internal DataTable ExecuteFromPrefSqlModel(string dbConnection, string dbProvider, PrefSQLModel prefSqlModel)
        {
            return ParseAndExecutePrefSQL(dbConnection, dbProvider, prefSqlModel);
        }
    }
}
