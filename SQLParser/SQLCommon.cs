using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Antlr4.Runtime.Tree.Pattern;

using prefSQL.SQLParser.Models;
using System.Text.RegularExpressions;

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
                IParseTree tree = parser.parse();
                Console.WriteLine("Tree: " + tree.ToStringTree(parser));
                

                SQLVisitor visitor = new SQLVisitor();
                PrefSQLModel prefSQL = visitor.Visit(tree);

                //Check if parse was successful
                if (prefSQL != null)
                {
                    if (strInput.IndexOf("PREFERENCE") > 0)
                    {
                        strNewSQL = strInput.Substring(0, strInput.IndexOf("PREFERENCE") - 1);

                        if (_SkylineType == Algorithm.NativeSQL)
                        {
                            String strWHERE = buildWHEREClause(prefSQL, strNewSQL);
                            String strOrderBy = buildORDERBYClause(prefSQL);


                            strNewSQL += strWHERE;
                            strNewSQL += strOrderBy;
                            Console.WriteLine("Result: " + strWHERE);
                        }
                        else if (_SkylineType == Algorithm.BNL)
                        {
                            String strPreferences = buildPreferencesBNL(prefSQL, strNewSQL);

                            strNewSQL = "EXEC dbo.SP_SkylineBNL '" + strNewSQL + "', '" + strPreferences + "'";

                        }
                        
                        Console.WriteLine("--------------------------------------------");


                    }
                    else
                    {
                        strNewSQL = strInput;
                    }
                }

            }
            catch (Antlr4.Runtime.InputMismatchException e)
            {
                Console.WriteLine("Wrong syntax " + e.Message);
            }
            catch(Antlr4.Runtime.NoViableAltException e)
            {
                Console.WriteLine("Wrong syntax " + e.Message);
            }
            catch(Exception e)
            {
                Console.WriteLine("Wrong syntax " + e.Message);
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


                strSQL = " WHERE NOT EXISTS(" + strPreSQL + " " + strWhereEqual + strWhereBetter + ") ";
                                

            }
            return strSQL;
        }


        private String buildPreferencesBNL(PrefSQLModel model, String strPreSQL)
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
                    strSQL += ";" + op;

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

