using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;
using Antlr4.Runtime.Tree.Pattern;

using prefSQL.SQLParser.Models;

namespace prefSQL.SQLParser
{
    public class SQLCommon
    {
        

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

                //Tree
                //Preorder(tree, 0);
               // parser.RemoveErrorListeners();
                
                /*ParseTreePattern p = parser.CompileParseTreePattern("<ID> = <expr>;", SQLParser.RULE_sql_stmt);
                ParseTreeMatch m = p.Match(tree);

                if (m.Succeeded == true) 
                {
                    Console.WriteLine("PRÄFERENZEN!!");
                }*/

                //parser.setErrorStrategy(new BailErrorStrategy());


                Console.WriteLine("Tree: " + tree.ToStringTree(parser));
                

                SQLVisitor visitor = new SQLVisitor();
                PrefSQLModel prefSQL = visitor.Visit(tree);
                

                
                
                if (strInput.IndexOf("PREFERENCE") > 0)
                {
                    strNewSQL = strInput.Substring(0, strInput.IndexOf("PREFERENCE") - 1);

                    String strWHERE = buildWHEREClause(prefSQL, strNewSQL);
                    String strOrderBy = buildORDERBYClause(prefSQL);


                    strNewSQL += strWHERE;
                    strNewSQL += strOrderBy;

                    Console.WriteLine("Result: " + strWHERE);
                    Console.WriteLine("--------------------------------------------");

                    
                }
                else
                {
                    strNewSQL = strInput;
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

                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {

                    //First record has a slightly different syntax
                    if (bFirst == false)
                    {
                        strWhereEqual += " AND ";
                        strWhereBetter += " OR ";
                    }

                    if (model.Skyline[iChild].Table == "")
                    {
                        model.Skyline[iChild].Table = "cars";
                    }


                    strWhereEqual += "h1.{column} " + model.Skyline[iChild].Op + "= {TABLE}.{column}";
                    strWhereBetter += "h1.{column} " + model.Skyline[iChild].Op + " {TABLE}.{column}";
                    //strWhereEqual += model.Skyline[iChild].InnerColumn + " "  + model.Skyline[iChild].Op + " " + model.Skyline[iChild].Table + ".{column}";
                    //strWhereBetter += model.Skyline[iChild].InnerColumn + " " + model.Skyline[iChild].Op + " " + model.Skyline[iChild].Table + ".{column}";
                    //strWhereEqual += model.Skyline[iChild].InnerColumn + " " + model.Skyline[iChild].Op + "= " +  "{column}";
                    //strWhereBetter += model.Skyline[iChild].InnerColumn + " " + model.Skyline[iChild].Op + " " + "{column}";
                    //strSQL = strSQL.Replace("{TABLE}", model.Skyline[iChild].Table);

                    strWhereEqual = strWhereEqual.Replace("{column}", model.Skyline[iChild].Column);
                    strWhereBetter = strWhereBetter.Replace("{column}", model.Skyline[iChild].Column);

                    bFirst = false;



                }
                //closing bracket for 2nd condition
                strWhereBetter += ") ";


                //strSQL = " WHERE NOT EXISTS(SELECT * FROM (" + strPreSQL + ") h1 " + strWhereEqual + strWhereBetter + ") ";
                strSQL = " WHERE NOT EXISTS(SELECT 1 FROM cars h1 " + strWhereEqual + strWhereBetter + ") ";

                //TODO: replace with correct table name
                strSQL = strSQL.Replace("{TABLE}", "cars");
            }
            return strSQL;
        }



        //Create the WHERE-Clause from the preferene model
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



        /*public void Preorder(ITree Tree, int Depth)
        {
            if (Tree == null)
            {
                return;
            }

            for (int i = 0; i < Depth; i++)
            {
                Console.Write("  ");
            }

            if (Tree.GetType() == typeof(prefSQL.SQLParser.SQLParser.ExprandContext))
            {
                //There is an AND-Clause

                String strTest = "Visit AND" + ": " + Tree.ToString();
            }

            if (Tree != null)
            {
                Console.WriteLine(Tree);


                for (int iChild = 0; iChild <= Tree.ChildCount; iChild++)
                {
                    Preorder(Tree.GetChild(iChild), Depth + 1);
                }

                /*if (Tree.ChildCount > 0)
                {
                    Preorder(Tree.GetChild(0), Depth + 1);
                }

                if (Tree.ChildCount > 1)
                {
                    Preorder(Tree.GetChild(1), Depth + 1);
                }*//*

            }



        }*/

    }



}
