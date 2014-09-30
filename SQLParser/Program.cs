using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Forms;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;



namespace prefSQL.SQLParser
{
    class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        private static void Main(string[] args) 
        {
            (new Program()).Run();
        }

        public void Run()
        {
            try
            {
                
                
                String strSQL = "SELECT price, horsepower AS PS FROM cars " +
                    "LEFT OUTER JOIN transmission ON cars.transmission_ID = transmission.ID " +
                    "WHERE horsepower > 10 AND price < 10000 " +
                    "PREFERENCE LOW mileage ";
                    //"PREFERENCE HIGH horsepower ";
                    //"PREFERENCE price AROUND 15000 ";
                Console.WriteLine(strSQL);
                Console.WriteLine("--------------------------------------------");
                RunParserSQL(strSQL);



                Console.WriteLine("------------------------------------------\nDONE");
                

            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: " + ex);
                Console.Write("Hit RETURN to exit: ");
            }

            Environment.Exit(0);
        }


        private void RunParserSQL(String strInput)
        {
            AntlrInputStream inputStream = new AntlrInputStream(strInput);
            SQLLexer sqlLexer = new SQLLexer(inputStream);
            CommonTokenStream commonTokenStream = new CommonTokenStream(sqlLexer);
            SQLParser parser = new SQLParser(commonTokenStream);

            try
            {
                IParseTree tree = parser.parse();
                Console.WriteLine("Tree: " + tree.ToStringTree(parser));

                SQLVisitor visitor = new SQLVisitor();
                String strResult = visitor.Visit(tree);
                Console.WriteLine("Result: " + strResult);



                Console.WriteLine("--------------------------------------------");

                String strNewSQL = "";
                if (strInput.IndexOf("PREFERENCE") > 0)
                {
                    strNewSQL = strInput.Substring(0, strInput.IndexOf("PREFERENCE") - 1) + strResult;
                }
                else
                {
                    strNewSQL = strInput;
                }
                Console.WriteLine(strNewSQL);
            }
            catch (Antlr4.Runtime.InputMismatchException e)
            {
                Console.WriteLine("Wrong syntax " + e.Message);
            }
            
        }


        
        public void Preorder(ITree Tree, int Depth)
        {
            if (Tree == null)
            {
                return;
            }

            for (int i = 0; i < Depth; i++)
            {
                Console.Write("  ");
            }

            if (Tree != null)
            {
                Console.WriteLine(Tree);
                

                for(int iChild = 0; iChild <= Tree.ChildCount; iChild++)
                {
                    Preorder(Tree.GetChild(iChild), Depth + 1);
                }

                if (Tree.ChildCount > 0)
                {
                    Preorder(Tree.GetChild(0), Depth + 1);
                }

                if (Tree.ChildCount > 1)
                {
                    Preorder(Tree.GetChild(1), Depth + 1);
                }

            }

            
            
        }





      


    }
}
