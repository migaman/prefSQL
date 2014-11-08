﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Antlr4.Runtime.Tree.Pattern;

using prefSQL.SQLParser.Models;
using System.Text.RegularExpressions;
using System.Diagnostics;

namespace prefSQL.SQLParser
{
    public class SQLCommon
    {
        public enum Algorithm
        {
            NativeSQL,
            BNL,
            DQ,
        };


        private Algorithm _SkylineType = Algorithm.NativeSQL;
        
        public Algorithm SkylineType
        {
            get { return _SkylineType; }
            set { _SkylineType = value; }
        }

        
        public String parsePreferenceSQL(String strInput) 
        {
            AntlrInputStream inputStream = new AntlrInputStream(strInput);
            SQLLexer sqlLexer = new SQLLexer(inputStream);
            CommonTokenStream commonTokenStream = new CommonTokenStream(sqlLexer);
            SQLParser parser = new SQLParser(commonTokenStream);
            String strNewSQL = "";

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

                    if (_SkylineType == Algorithm.NativeSQL)
                    {
                        String strWHERE = buildWHEREClause(prefSQL, strNewSQL);
                        String strOrderBy = buildORDERBYClause(prefSQL);


                        strNewSQL += strWHERE;
                        strNewSQL += strOrderBy;
                        Debug.WriteLine("Result: " + strWHERE);
                    }
                    else if (_SkylineType == Algorithm.BNL)
                    {
                        String strOperators = "";
                        String strPreferences = buildPreferencesBNL(prefSQL, strNewSQL, ref strOperators);
                        String strSQLBeforeFrom = strNewSQL.Substring(0, strNewSQL.IndexOf("FROM"));
                        String strSQLAfterFrom = strNewSQL.Substring(strNewSQL.IndexOf("FROM"));
                        //String strFirstSQL = strSQLBeforeFrom + strPreferences + " " + strSQLAfterFrom;
                        String strFirstSQL = "SELECT cars.id " + strPreferences + " " + strSQLAfterFrom;
                        String strOrderBy = buildORDERBYClause(prefSQL);
                        strFirstSQL += strOrderBy.Replace("'", "''");
                        strNewSQL = "EXEC dbo.SP_SkylineBNL '"  + strFirstSQL + "', '" + strOperators + "', '" + strNewSQL + "', 'cars'";


                        


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


        //Create the WHERE-Clause from the preferene model
        private String buildWHEREClause(PrefSQLModel model, String strPreSQL)
        {
            String strSQL = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 1)
            {
                String strWhereEqual = "WHERE ";
                String strWhereBetter = " AND ( ";
                Boolean bFirst = true;

                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    Boolean needsTextORClause = false;

                    //Competition
                    needsTextORClause = !model.Skyline[iChild].ColumnName.Equals("");


                    if (bFirst == false)
                    {
                        strWhereEqual += " AND ";
                        strWhereBetter += " OR ";
                    }

                    //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                    if (needsTextORClause == true)
                    {
                        strWhereEqual += "(";
                    }
                    
                    strWhereEqual += "{INNERcolumn} " + model.Skyline[iChild].Op + "= {column}";
                    strWhereBetter += "{INNERcolumn} " + model.Skyline[iChild].Op + " {column}";

                    strWhereEqual = strWhereEqual.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumnExpression);
                    strWhereBetter = strWhereBetter.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumnExpression);
                    strWhereEqual = strWhereEqual.Replace("{column}", model.Skyline[iChild].ColumnExpression);
                    strWhereBetter = strWhereBetter.Replace("{column}", model.Skyline[iChild].ColumnExpression);

                    //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                    if (needsTextORClause == true)
                    {
                        strWhereEqual += " OR " + model.Skyline[iChild].InnerColumnName + " = " + model.Skyline[iChild].ColumnName;
                        strWhereEqual += ")";
                    }
                    bFirst = false;
                    


                }
                //closing bracket for 2nd condition
                strWhereBetter += ") ";

                //Format strPreSQL
                foreach(String strTable in model.Tables)
                {
                    //Replace tablename 
                    strPreSQL = strPreSQL.Replace(strTable + ".", strTable + "_INNER.");
                    
                    //Add ALIAS to tablename
                    string pattern = @"\b" + strTable + @"\b";
                    string replace = strTable + " " + strTable +  "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }

                //Check if SQL contains TOP Keywords
                if(strPreSQL.Contains("TOP"))
                {
                    //Remove Top Keyword
                    //TODO: Do this in SQLVisitor --> this one might not always work
                    int iPosTop = strPreSQL.IndexOf("TOP");
                    int iPosTopEnd = strPreSQL.Substring(iPosTop+3).TrimStart().IndexOf(" ");
                    String strSQLAfterTOP = strPreSQL.Substring(iPosTop + 3).TrimStart();
                    strPreSQL = strPreSQL.Substring(0, iPosTop) + strSQLAfterTOP.Substring(iPosTopEnd+1);
                }


                strSQL = " WHERE NOT EXISTS(" + strPreSQL + " " + strWhereEqual + strWhereBetter + ") ";
                                

            }
            return strSQL;
        }


        private String buildPreferencesBNL(PrefSQLModel model, String strPreSQL, ref String strOperators)
        {
            String strSQL = "";

            //Build Skyline only if more than one attribute
            if (model.Skyline.Count > 1)
            {
                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    String op = "";
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


        //Create the ORDERBY-Clause from the preferene model
        private String buildORDERBYClause(PrefSQLModel model)
        {
            String strSQL = "";
            
            Boolean bFirst = true;

            for (int iChild = 0; iChild < model.OrderBy.Count; iChild++)
            {

                //First record has a slightly different syntax
                if (bFirst == false)
                {
                    strSQL += ", ";
                }
                strSQL += model.OrderBy[iChild].ToString();
                bFirst = false;
            }

            strSQL = " ORDER BY " + strSQL;
            return strSQL;
        }


    }



}

