using Antlr4.Runtime.Tree;
using prefSQL.Grammar;

namespace prefSQL.SQLParser.Udf
{
    internal class SqlUdfParamVisitor : PrefSQLBaseVisitor<UdfParamModel>
    {
        public override UdfParamModel VisitUdfLiteralParam(PrefSQLParser.UdfLiteralParamContext context)
        {
            var ret = new UdfParamModel {
                IsLiteral = true,
                Literal = context.GetChild(0).GetText()
            };
            return ret;
        }

        public override UdfParamModel VisitUdfColParam(PrefSQLParser.UdfColParamContext context)
        {
            var ret = new UdfParamModel {
                IsLiteral = false,
                Table = GetTableName(context.GetChild(0)),
                Column = GetColumnName(context.GetChild(0))
            };
            return ret;
        }
        
        private string GetTableName(IParseTree tree)
        {
            return tree.ChildCount == 1 ? "" : tree.GetChild(0).GetText();
        }

        private string GetColumnName(IParseTree tree)
        {
            return tree.ChildCount == 1 ? tree.GetText() : tree.GetChild(2).GetText();
        }
    }
}
