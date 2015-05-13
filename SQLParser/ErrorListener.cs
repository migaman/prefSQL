using System;
using Antlr4.Runtime;

namespace prefSQL.SQLParser
{
    //internal class
    class ErrorListener : IAntlrErrorListener<IToken>
    {

        //Error Handling for parser error
        public void SyntaxError(IRecognizer recognizer, IToken offendingSymbol, int line, int charPositionInLine, string msg, RecognitionException e)
        {
            throw new Exception("Error at line " + line + "\n" + msg);
        }
    }
}
