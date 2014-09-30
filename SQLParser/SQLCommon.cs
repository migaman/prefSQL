using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;



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
                Console.WriteLine("Tree: " + tree.ToStringTree(parser));

                SQLVisitor visitor = new SQLVisitor();
                String strResult = visitor.Visit(tree);
                //Console.WriteLine("Result: " + strResult);

                Console.WriteLine("--------------------------------------------");

                
                if (strInput.IndexOf("PREFERENCE") > 0)
                {
                    strNewSQL = strInput.Substring(0, strInput.IndexOf("PREFERENCE") - 1) + strResult;
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
            return strNewSQL;

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


                for (int iChild = 0; iChild <= Tree.ChildCount; iChild++)
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
