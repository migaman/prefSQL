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

        public enum ParetoInterpretation
        {
            Composition,     //better, identical or incomparable in all attribute values
            Accumulation,   //better or identical in all attribute values
            
        };


        private Algorithm _SkylineType = Algorithm.NativeSQL;
        
        public Algorithm SkylineType
        {
            get { return _SkylineType; }
            set { _SkylineType = value; }
        }

        private ParetoInterpretation _ParetoImplementation = ParetoInterpretation.Composition;

        public ParetoInterpretation ParetoImplementation
        {
            get { return _ParetoImplementation; }
            set { _ParetoImplementation = value; }
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

                //Check if parse was successful
                if (prefSQL != null)
                {
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
                //String strWhereIncomparable = "AND ( ";
                Boolean bFirst = true;

                //Build the where clause with each column in the skyline
                for (int iChild = 0; iChild < model.Skyline.Count; iChild++)
                {
                    Boolean needsTextORClause = false;

                    if (_ParetoImplementation == ParetoInterpretation.Composition)
                    {
                        //Competition
                        needsTextORClause = !model.Skyline[iChild].SingleColumn.Equals("") && !model.Skyline[iChild].IncludesOthers;
                    }
                    else
                    {
                        //Accumulation
                        needsTextORClause = !model.Skyline[iChild].SingleColumn.Equals("");
                    }


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

                    
                    if (_ParetoImplementation == ParetoInterpretation.Composition)
                    {
                        strWhereEqual = strWhereEqual.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumn);
                    }
                    else
                    {
                        //Falls Accumulation soll das ELSE (OTHERS) mit einem höheren Wert ausgeführt werdne
                        strWhereEqual = strWhereEqual.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumnAccumulation);
                    }
                    strWhereBetter = strWhereBetter.Replace("{INNERcolumn}", model.Skyline[iChild].InnerColumn);
                    strWhereEqual = strWhereEqual.Replace("{column}", model.Skyline[iChild].Column);
                    strWhereBetter = strWhereBetter.Replace("{column}", model.Skyline[iChild].Column);

                    //Falls Text-Spalte ein zusätzliches OR einbauen für den Vergleich Farbe = Farbe
                    if (needsTextORClause == true)
                    {
                        strWhereEqual += " OR " + model.Skyline[iChild].InnerSingleColumn + " = " + model.Skyline[iChild].SingleColumn;
                        strWhereEqual += ")";
                    }
                    bFirst = false;
                    


                }
                //closing bracket for 2nd condition
                strWhereBetter += ") ";

                //Format strPreSQL
                foreach(String strTable in model.Tables)
                {
                    //Ersetzen von Tabellennamen in Spalten
                    strPreSQL = strPreSQL.Replace(strTable + ".", strTable + "_INNER.");
                    
                    //Tabellenname mit neuem ALIAS ergänzen
                    string pattern = @"\b" + strTable + @"\b";
                    string replace = strTable + " " + strTable +  "_INNER";
                    strPreSQL = Regex.Replace(strPreSQL, pattern, replace, RegexOptions.IgnoreCase);
                }


                //INNER WHERE in einem eigenen SELECT (SELECT * FROM) abhandeln damit nur ein ALIAS nötig!
                //strSQL = " WHERE NOT EXISTS(SELECT * FROM (" + strPreSQL + ") h1 " + strWhereEqual + strWhereBetter + ") ";
                
                strSQL = " WHERE NOT EXISTS(" + strPreSQL + " " + strWhereEqual + strWhereBetter + ") ";
                                

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




/*
 * --Fall 1: rot besser als grün > schwarz --> Funktioniert
SELECT cars.id, cars.Price, colors.Name, colors.id
FROM cars
LEFT OUTER JOIN colors ON cars.Color_Id = Colors.Id
WHERE NOT EXISTS
(
	SELECT b.id, b.Price, c.Name FROM cars b
	LEFT OUTER JOIN colors c ON b.Color_Id = c.Id
	WHERE  b.Price <= cars.Price 
		AND 
		--Mindestens so gut Klausel
			(
				--Rot > Blau
				CASE WHEN c.Name = 'rot' THEN 1 WHEN c.Name = 'grün' THEN 2 WHEN c.Name = 'schwarz' THEN 3 END <= CASE WHEN colors.Name = 'rot' THEN 1 WHEN colors.Name = 'grün' THEN 2 WHEN colors.Name = 'schwarz' THEN 3 END
				OR
				--gleiche Farbe
				c.Name = colors.Name
			)
				
		--Besser-Klausel
		AND (
			b.Price < cars.Price 
			OR 
			CASE WHEN c.Name = 'rot' THEN 1 WHEN c.Name = 'grün' THEN 2 WHEN c.Name = 'schwarz' THEN 3 END < CASE WHEN colors.Name = 'rot' THEN 1 WHEN colors.Name = 'grün' THEN 2 WHEN colors.Name = 'schwarz' THEN 3 END)

)

/*
select * from cars 
--where Color_Id = 3
order by Price

select * from Colors
--blau = 3
--grün = 9
--rot = 12
*/

/*
 * 
 * 
 * --Fall 2: rot besser als schwarz > alle anderen --> Funktioniert

SELECT cars.id, cars.Price, colors.Name
FROM cars
LEFT OUTER JOIN colors ON cars.Color_Id = Colors.Id
WHERE NOT EXISTS
(
	SELECT b.id, b.Price, c.Name
	 
	 FROM cars b
	LEFT OUTER JOIN colors c ON b.Color_Id = c.Id
	WHERE  
		--Mindestens so gut Klausel
		b.Price <= cars.Price 
		AND 
			(
				--Türkis > Gelb > alles andere
				--Speziell ist beim OTHERS, dass die Bedingung dann nicht zutreffen darf weil sonst z.B. grün mit rot verglichen wird!!
				CASE WHEN c.Name = 'türkis' THEN 1 WHEN c.Name = 'gelb' THEN 2 ELSE 999 END <= CASE WHEN colors.Name = 'türkis' THEN 1 WHEN colors.Name = 'gelb' THEN 2 ELSE 99 END
				OR
				--gleiche Farbe
				c.Name = colors.Name
			)
				
		--Besser-Klausel
	AND (
			b.Price < cars.Price 
			OR 
			CASE WHEN c.Name = 'türkis' THEN 1 WHEN c.Name = 'gelb' THEN 2 ELSE 99 END  < CASE WHEN colors.Name = 'türkis' THEN 1 WHEN colors.Name = 'gelb' THEN 2 ELSE 99 END)
			
)

ORDER by cars.price
 * 
 * */

