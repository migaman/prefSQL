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
    public class SQLCommon
    {
        private Algorithm _SkylineType = Algorithm.NativeSQL;
        private Ordering _OrderType = Ordering.AttributePosition;
        private bool _ShowSkylineAttributes = false;

        public bool ShowSkylineAttributes
        {
            get { return _ShowSkylineAttributes; }
            set { _ShowSkylineAttributes = value; }
        }

        public enum Algorithm
        {
            NativeSQL,
            BNL,
            DQ,
        };

        public enum Ordering
        {
            AttributePosition,
            RankingSummarize,
            RankingBestOf,
            Random,
            AsIs //Without OrderBy-Clause as it comes from the database
        }

        
        public Algorithm SkylineType
        {
            get { return _SkylineType; }
            set { _SkylineType = value; }
        }

        public Ordering OrderType
        {
            get { return _OrderType; }
            set { _OrderType = value; }
        }

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
                //Add error listener to parser
                ErrorListener listener = new ErrorListener();
                parser.AddErrorListener(listener);

                //Parse query
                IParseTree tree = parser.parse();
                Debug.WriteLine("Tree: " + tree.ToStringTree(parser));
                
                //Visit parsetree
                SQLVisitor visitor = new SQLVisitor();
                PrefSQLModel prefSQL = visitor.Visit(tree);


                

                //Check if parse was successful
                if (prefSQL != null && strInput.IndexOf("PREFERENCE") > 0)
                {
                    strNewSQL = strInput.Substring(0, strInput.IndexOf("PREFERENCE") - 1);

                    if(prefSQL.HasSkyline == true)
                    {
                        if (_SkylineType == Algorithm.NativeSQL)
                        {
                            if(_ShowSkylineAttributes == true)
                            {
                                string strPreferences = getPreferenceAttributes(prefSQL, strNewSQL);
                                string strSQLBeforeFrom = strNewSQL.Substring(0, strNewSQL.IndexOf("FROM"));
                                string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));
                                strNewSQL = strSQLBeforeFrom + strPreferences + " " + strSQLAfterFrom;
                            }

                            string strWHERE = sqlCriterion.getCriterionClause(prefSQL, strNewSQL);
                            string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);
                            strNewSQL += strWHERE;
                            strNewSQL += strOrderBy;
                        }
                        else if (_SkylineType == Algorithm.BNL)
                        {
                            string strOperators = "";
                            string strPreferences = buildPreferencesBNL(prefSQL, strNewSQL, ref strOperators);
                            string strSQLBeforeFrom = strNewSQL.Substring(0, strNewSQL.IndexOf("FROM"));
                            string strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));
                            string strTable = "";
                            foreach (KeyValuePair<string, string> table in prefSQL.Tables)
                            {
                                if(table.Value.Equals(""))
                                    strTable = table.Key;
                                else
                                    strTable = table.Value;
                                break;
                            }

                            //TODO: replace id
                            string strFirstSQL = "SELECT " + strTable + ".id " + strPreferences + " " + strSQLAfterFrom;
                            string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);
                            strFirstSQL += strOrderBy.Replace("'", "''");
                            strNewSQL = "EXEC dbo.SP_SkylineBNL '" + strFirstSQL + "', '" + strOperators + "', '" + strNewSQL + "', '" + strTable + "'";
                        }
                    }
                    else if (prefSQL.HasPrioritize == true)
                    {
                        string strWHERE = sqlCriterion.getCriterionClause(prefSQL, strNewSQL);
                        string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);

                        string strSelectRank = buildSELECTRank(prefSQL, strNewSQL);
                        strNewSQL = "SELECT * FROM (" + strSelectRank;
                        strNewSQL += ") RankedResult ";

                        strNewSQL += strWHERE;
                        strNewSQL += strOrderBy;
                    }
                    else
                    {
                        string strOrderBy = sqlSort.getSortClause(prefSQL, _OrderType);
                        strNewSQL += strOrderBy;
                    }

                }
                else
                {
                    //Query does not contain a preference --> return original query
                    strNewSQL = strInput;
                }
            }

            catch(Exception e)
            {
                //Syntaxerror
                /// <exception cref="Exception">This is exception is thrown because the String is not a valid PrefSQL Query</exception>
                throw new Exception(e.Message);
            }
            return strNewSQL;

        }




        private string buildSELECTRank(PrefSQLModel model, string strPreSQL)
        {
            string strSQL = "";
            int posOfFROM = 0;

            //Build the where clause with each column in the skyline
            for (int iChild = 0; iChild < model.Rank.Count; iChild++)
            {
                strSQL += ", " + model.Rank[iChild].RankColumn;
            }

            posOfFROM = strPreSQL.IndexOf("FROM");
            strSQL = strPreSQL.Substring(0, posOfFROM-1) + strSQL + strPreSQL.Substring(posOfFROM-1);

            return strSQL;
        }



        /**
         *  This method is used for Pareto Dominance Grahps
         *  It add the Skyline Attributes to the SELECT List
         *  For the reason that comparing the values is easier, smaller values are always better than higher
         *  Therefore HIGH preferences are multiplied with -1 
         * 
         * */
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


        private string buildPreferencesBNL(PrefSQLModel model, string strPreSQL, ref string strOperators)
        {
            string strSQL = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 1)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    string op = "";
                    if (model.Skyline[iChild].Op.Equals("<"))
                        op = "LOW";
                    else
                        op = "HIGH";
                    strOperators += ";" + op;

                    if (model.Skyline[iChild].ColumnName.Equals(""))
                    {
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression.Replace("'", "''");
                    }
                    else
                    {
                        strSQL += ", " + model.Skyline[iChild].ColumnExpression.Replace("'", "''");
                    }

                    //Incomparable field --> Add string field
                    if (model.Skyline[iChild].Comparable == false)
                    {
                        strSQL += ", " + model.Skyline[iChild].IncomparableAttribute.Replace("'", "''");
                        strOperators += ";INCOMPARABLE";
                    }


                }
            }
            
            return strSQL;
        }


       





       

    }



}

