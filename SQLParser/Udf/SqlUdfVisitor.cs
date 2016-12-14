using Antlr4.Runtime.Tree;
using prefSQL.Grammar;

namespace prefSQL.SQLParser.Udf
{
    internal class SqlUdfVisitor : PrefSQLBaseVisitor<UdfModel>
    {
        public override UdfModel VisitSkylinePreferenceUdf(PrefSQLParser.SkylinePreferenceUdfContext context)
        {
            // extract schema and function
            var model = new UdfModel();
            ExtractFunctionName(context, model);

            // extract parameter
            ExtractParameterList(context.exprUdfParam(), model);

            // extract leveling
            ExtractLevelingData(context, model);

            ExtractHighLowData(context, model);

            return model;
        }

        private void ExtractHighLowData(PrefSQLParser.SkylinePreferenceUdfContext context, UdfModel model)
        {
            // set defaults
            model.OppositeOperator = string.Empty;

            // operators not found
            model.HasHighLowOperator = (context.op.Type == PrefSQLParser.K_LOW || context.op.Type == PrefSQLParser.K_HIGH);
            if (!model.HasHighLowOperator) return;

            // mode HIGH
            model.LevelAdditionaly = model.LevelAdd;
            if (context.op.Type == PrefSQLParser.K_HIGH) {
                model.LevelAdditionaly = model.LevelMinus;
                //Multiply with -1 (result: every value can be minimized!)
                model.OppositeOperator = " * -1";
            }

        }

        private void ExtractLevelingData(PrefSQLParser.SkylinePreferenceUdfContext context, UdfModel model)
        {
            // set defaults
            model.IsLevelStepEqual = true;
            model.LevelStep = string.Empty;
            model.LevelAdd = string.Empty;
            model.LevelMinus = string.Empty;
            model.IsComparable = true;
            model.HasIncomparableTuples = false;
            model.ContainsOpenPreference = false;

            // no STEP operator
            if (context.ChildCount != 4) return;

            //If a third parameter is given, it is the Level Step  (i.e. LOW price 1000 means prices divided through 1000)
            //The user doesn't care about a price difference of 1000
            //This results in a smaller skyline
            model.LevelStep = " / " + context.GetChild(2).GetText();
            model.LevelAdd = " + " + context.GetChild(2).GetText();
            model.LevelMinus = " - " + context.GetChild(2).GetText();

            // 
            if (context.GetChild(3).GetText().Equals("EQUAL")) return;
            model.IsLevelStepEqual = false;
            model.IsComparable = false;
            model.HasIncomparableTuples = true;
            //Some algorithms cannot handle this incomparable preference --> It is like a categorical preference without explicit OTHERS
            model.ContainsOpenPreference = true;
        }

        private void ExtractFunctionName(PrefSQLParser.SkylinePreferenceUdfContext context, UdfModel model)
        {
            model.SchemaName = GetSchemaName(context.GetChild(0));
            model.FunctionName = GetFunctionName(context.GetChild(0));
            //model.FullFunctionName = $"{model.SchemaName}.{model.FunctionName}";
            model.FullFunctionName = model.SchemaName + "." + model.FunctionName;
        }
        private void ExtractParameterList(PrefSQLParser.ExprUdfParamContext[] context, UdfModel model)
        {
            var upv = new SqlUdfParamVisitor();
            foreach (var paramExp in context) {
                var param = upv.Visit(paramExp);
                model.Parameter.Add(param);
            }
        }

        private string GetFunctionName(IParseTree tree)
        {
            // function name w/ or w/o schema
            return tree.ChildCount == 1 ? tree.GetText() : tree.GetChild(2).GetText();
        }

        private string GetSchemaName(IParseTree tree)
        {
            // function name w/ or w/o schema
            return tree.ChildCount == 1 ? "" : tree.GetChild(0).GetText();
        }

    }
}
