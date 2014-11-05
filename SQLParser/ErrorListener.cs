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
    class ErrorListener : IAntlrErrorListener<IToken>
    {

        //Error Handling for parser error
        public void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg, RecognitionException e)
        {
            throw new Exception("Error at line " + line + "\n" + msg);
        }
    }
}
