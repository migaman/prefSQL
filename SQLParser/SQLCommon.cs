using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Antlr4.Runtime.Tree.Pattern;
using prefSQL.SQLParser.Models;
using System.Diagnostics;

namespace prefSQL.SQLParser
{

    /// <summary>
    /// Entry point of the library for parsing a PREFERENCE SQL to an ANSI-SQL Statement
    /// </summary>
    /// <remarks>
    /// You can choose the Skyline Type, Ordering Type and if the SELECT List should be extended with the Skyline Values
    /// </remarks>
    public class SQLCommon
    {
        private const string SkylineOf = "SKYLINE OF";
        private Algorithm _SkylineType = Algorithm.NativeSQL;   //Defines with which Algorithm the Skyline should be calculated
        private bool _ShowSkylineAttributes = false;            //Defines if the skyline attributes should be added to the SELECT list

        public enum Algorithm
        {
            NativeSQL,              //Works with ANSI-SQL syntax
            BNL,                    //Block nested loops
            BNLSort,                //Block nested loops with presort
            DQ,                     //Divide and Conquer
            Hexagon,                //Hexagon Augsburg
            Tree                    //Treebased for all skylines
        };

        public enum Ordering
        {
            AttributePosition,      //Sorted according to the attribute position in the select list
            RankingSummarize,       //Sorted according to the the sum of the rank of all attributes
            RankingBestOf,          //Sorted according to the best rank of all attributes
            Random,                 //Randomly sorted, Every query results in a different sort order
            AsIs                    //Without OrderBy-Clause as it comes from the database
        }

        public bool ShowSkylineAttributes
        {
            get { return _ShowSkylineAttributes; }
            set { _ShowSkylineAttributes = value; }
        }

        public Algorithm SkylineType
        {
            get { return _SkylineType; }
            set { _SkylineType = value; }
        }


        /// <summary>Parses a PREFERENE SQL Statement in an ANSI SQL Statement</summary>
        /// <param name="strInput">Preference SQL Statement</param>
        /// <returns>Return the ANSI SQL Statement</returns>
        public string parsePreferenceSQL(string strInput)
        {
            AntlrInputStream inputStream = new AntlrInputStream(strInput);
            SQLLexer sqlLexer = new SQLLexer(inputStream);
            CommonTokenStream commonTokenStream = new CommonTokenStream(sqlLexer);
            SQLParser parser = new SQLParser(commonTokenStream);
            string strNewSQL = "";
            SQLSort sqlSort = new SQLSort();
            SQLCriterion sqlCriterion = new SQLCriterion();

            try
            {
                //Add error listener to parser (helps to return detailed parser syntax errors)
                ErrorListener listener = new ErrorListener();
                parser.AddErrorListener(listener);

                //Parse query
                IParseTree tree = parser.parse();
                Debug.WriteLine("Parse Tree: " + tree.ToStringTree(parser));

                //Visit parsetree (PrefSQL model is built during the visit of the parse tree)
                SQLVisitor visitor = new SQLVisitor();
                visitor.Visit(tree);
                PrefSQLModel prefSQL = visitor.Model;

                
                //Check if parse was successful and query contains PrefSQL syntax
                if (prefSQL != null && strInput.IndexOf(SkylineOf) > 0)
                {
                    //All Syntax before Skyline-Clause
                    strNewSQL = strInput.Substring(0, strInput.IndexOf(SkylineOf) - 1);

                    if (prefSQL.HasSkyline == true)
                    {
                        if (_SkylineType == Algorithm.NativeSQL)
                        {
                            if (_ShowSkylineAttributes == true)
                            {
                                string strPreferences = getPreferenceAttributes(prefSQL, strNewSQL);
                                string strSQLBeforeFrom = strNewSQL.Substring(0, strNewSQL.IndexOf("FROM"));
                                string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));
                                strNewSQL = strSQLBeforeFrom + strPreferences + " " + strSQLAfterFrom;
                            }

                            string strWHERE = sqlCriterion.getCriterionClause(prefSQL, strNewSQL);
                            //string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);
                            string strOrderBy = "";
                            if (strInput.IndexOf("ORDER BY") > 0)
                            {
                                if (prefSQL.Ordering == Ordering.AsIs)
                                {
                                    strOrderBy = strInput.Substring(strInput.IndexOf("ORDER BY"));

                                    foreach (KeyValuePair<string, string> orderBy in prefSQL.OrderBy)
                                    {
                                        //String strOrderByNoSpaces = strOrderBy.Replace()
                                        strOrderBy = strOrderBy.Replace(orderBy.Key, orderBy.Value);
                                    }
                                    //Replace category clauses
                                }
                                else
                                {
                                    strOrderBy = sqlSort.getSortClause(prefSQL, prefSQL.Ordering); // sqlSort.getSortClause(prefSQL, _OrderType);
                                }
                            }
                            strNewSQL += strWHERE;
                            strNewSQL += strOrderBy;
                        }
                        else if (_SkylineType == Algorithm.BNL)
                        {
                            string strOperators = "";
                            string strAttributesSkyline = buildPreferencesBNL(prefSQL, strNewSQL, ref strOperators);
                            //Without SELECT 
                            string strAttributesOutput = ", " + strNewSQL.Substring(7, strNewSQL.IndexOf("FROM") - 7);
                            string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));

                            string strFirstSQL = "SELECT " + strAttributesSkyline + " " + strAttributesOutput + strSQLAfterFrom;
                            //Bewusst nicht sortieren
                            strNewSQL = "EXEC dbo.SP_SkylineBNL '" + strFirstSQL + "', '" + strOperators + "', 'false'";
                        }
                        else if (_SkylineType == Algorithm.BNLSort || _SkylineType == Algorithm.Tree)
                        {
                            string strOperators = "";
                            string strAttributesSkyline = buildPreferencesBNL(prefSQL, strNewSQL, ref strOperators);
                            //Without SELECT 
                            string strAttributesOutput = ", " + strNewSQL.Substring(7, strNewSQL.IndexOf("FROM") - 7);
                            string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));

                            string strFirstSQL = "SELECT " + strAttributesSkyline + " " + strAttributesOutput + strSQLAfterFrom;
                            //Sortieren nach Attributen (damit algo funktioniert)
                            string strOrderBy = sqlSort.getSortClause(prefSQL, SQLCommon.Ordering.AttributePosition); // sqlSort.getSortClause(prefSQL, _OrderType);
                            strFirstSQL += strOrderBy.Replace("'", "''");
                            if (_SkylineType == Algorithm.BNLSort)
                            {
                                strNewSQL = "EXEC dbo.SP_SkylineBNLSort '" + strFirstSQL + "', '" + strOperators + "', 'false'";
                            }
                            else
                            {
                                strNewSQL = "EXEC dbo.SP_SkylineTree '" + strFirstSQL + "', '" + strOperators + "', 'false'";
                            }
                            
                        }
                        else if (_SkylineType == Algorithm.Hexagon)
                        {
                            string strOperators = "";
                            string strAttributesSkyline = buildSELECTDENSERank(prefSQL, strNewSQL, ref strOperators);
                            //Without SELECT 
                            string strAttributesOutput = ", " + strNewSQL.Substring(7, strNewSQL.IndexOf("FROM") - 7);
                            string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));

                            string strFirstSQL = "SELECT " + strAttributesSkyline + " " + strAttributesOutput + strSQLAfterFrom;
                            strFirstSQL = strFirstSQL.Replace("'", "''");


                            string strHexagon = buildSELECTHexagon(prefSQL, strNewSQL);
                            strHexagon = strHexagon.Replace("'", "''");

                            strNewSQL = "EXEC dbo.SP_SkylineHexagon '" + strFirstSQL + "', '" + strOperators + "', '" + strHexagon + "', 'false'";

                        }
                    }
                    /*else
                    {
                        //string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);
                        //strNewSQL += strOrderBy;
                    }*/

                }
                else
                {
                    //Query does not contain a preference --> return original query
                    strNewSQL = strInput;
                }
            }

            catch (Exception e)
            {
                //Parse syntaxerror
                /// <exception cref="Exception">This is exception is thrown because the String is not a valid PrefSQL Query</exception>
                throw new Exception(e.Message);
            }
            return strNewSQL;

        }



        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="strPreSQL">Preference SQL Statement WITHOUT PREFERENCES</param>
        /// <param name="strOperators">Returns the operators</param>
        /// <returns>TODO</returns>
        private string buildSELECTDENSERank(PrefSQLModel model, string strPreSQL, ref string strOperators)
        {
            strOperators = "";


            string strSQL = "";

            //Add a RankColumn for each PRIORITIZE preference
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //Replace ROW_NUMBER with Rank, for the reason that multiple tuples can have the same value (i.e. mileage=0)
                string strRank = model.Skyline[iChild].RankHexagon;
                strSQL += ", " + strRank;
                strOperators += "LOW" + ";";
            }

            //Add the ranked column before the FROM keyword
            //posOfFROM = strPreSQL.IndexOf("FROM");
            //strSQL = strPreSQL.Substring(0, posOfFROM - 1) + strSQL + strPreSQL.Substring(posOfFROM - 1);
            strSQL = strSQL.TrimStart(',');
            strOperators = strOperators.TrimEnd(';');

            return strSQL;
        }



        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="strPreSQL">Preference SQL Statement WITHOUT PREFERENCES</param>
        /// <param name="strOperators">Returns the operators</param>
        /// <returns>TODO</returns>
        private string buildSELECTHexagon(PrefSQLModel model, string strPreSQL)
        {
            string strSQL = "";
            string strMaxSQL = "";
            int posOfFROM = 0;

            //Add a RankColumn for each PRIORITIZE preference
            for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
            {
                //Replace ROW_NUMBER with Rank, for the reason that multiple tuples can have the same value (i.e. mileage=0)
                string strRank = model.Skyline[iChild].RankHexagon;
                strSQL += ", " + strRank;
                strMaxSQL += ", MAX(Rank" + model.Skyline[iChild].RankColumnName + ")";
            }
            strMaxSQL = strMaxSQL.TrimStart(',');
            strSQL = strSQL.TrimStart(',');
            strSQL = "SELECT " + strMaxSQL + " FROM (SELECT " + strSQL;

            //Add the ranked column before the FROM keyword
            posOfFROM = strPreSQL.IndexOf("FROM");
            strSQL = strSQL + strPreSQL.Substring(posOfFROM - 1) + ") MyQuery";

            return strSQL;
        }


        /// <summary>This method is used for Pareto Dominance Grahps. It adds the Skyline Attributes to the SELECT List.</summary>
        /// <remarks>
        /// For the reason that comparing the values is easier, smaller values are always better than higher.
        /// Therefore HIGH preferences are multiplied with -1 
        /// </remarks>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="strPreSQL">Preference SQL Statement WITHOUT PREFERENCES</param>
        /// <returns>Return the extended SQL Statement</returns>
        private string getPreferenceAttributes(PrefSQLModel model, string strPreSQL)
        {
            string strSQL = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 1)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    //
                    if (model.Skyline[iChild].Op.Equals("<"))
                    {
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression + " AS SkylineAttribute" + iChild;
                    }
                    else
                    {
                        //Multiply HIGH preferences with -1 --> small values are always better than high 
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression + "*-1 AS SkylineAttribute" + iChild;
                    }

                }
            }

            return strSQL;
        }


        /// <summary>TODO</summary>
        /// <param name="model">model of parsed Preference SQL Statement</param>
        /// <param name="strPreSQL">Preference SQL Statement WITHOUT PREFERENCES</param>
        /// <param name="strOperators">Returns the operators</param>
        /// <returns>TODO</returns>
        private string buildPreferencesBNL(PrefSQLModel model, string strPreSQL, ref string strOperators)
        {
            string strSQL = "";
            strOperators = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 0)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    string op = "";
                    if (model.Skyline[iChild].Op.Equals("<"))
                    {
                        op = "LOW";
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression.Replace("'", "''") + " AS SkylineAttribute" + iChild;
                    }
                    else
                    {
                        op = "LOW";
                        //Trick: Convert HIGH attributes in negative values
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression.Replace("'", "''") + "*-1 AS SkylineAttribute" + iChild;

                    }
                    strOperators += op + ";";



                    //Incomparable field --> Add string field
                    if (model.Skyline[iChild].Comparable == false)
                    {
                        strSQL += ", " + model.Skyline[iChild].IncomparableAttribute.Replace("'", "''");
                        strOperators += "INCOMPARABLE" + ";";
                    }


                }
            }

            strOperators = strOperators.TrimEnd(';');
            strSQL = strSQL.TrimStart(',');
            return strSQL;
        }



    }
}

