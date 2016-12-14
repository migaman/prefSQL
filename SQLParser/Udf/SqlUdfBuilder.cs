using System.Collections.Generic;

namespace prefSQL.SQLParser.Udf
{
    internal class SqlUdfBuilder
    {

        private readonly UdfModel _model;
        private readonly string _innerTableSuffix;

        public SqlUdfBuilder(UdfModel model, string innerTableSuffix)
        {
            _model = model;
            _innerTableSuffix = innerTableSuffix;
        }

        public string CreateRankingExpr()
        {
            if (!_model.HasHighLowOperator) { return string.Empty; }
            var paramList = CreateParamList();
            //return $"CAST({_model.FullFunctionName}({paramList}){_model.LevelStep}{_model.OppositeOperator} AS bigint)";
            return "CAST(" + _model.FullFunctionName + "(" + paramList + ")" + _model.LevelStep + _model.OppositeOperator + " AS bigint)";
        }

        public string CreateExpression()
        {
            if (!_model.HasHighLowOperator) { return string.Empty; }
            var paramList = CreateParamList();
            //return $"{_model.FullFunctionName}({paramList}){_model.LevelStep}{_model.OppositeOperator}";
            return _model.FullFunctionName + "(" + paramList + ")" + _model.LevelStep + _model.OppositeOperator;
        }

        private string CreateParamList(string tableSuffix = "")
        {
            var ret = new List<string>();
            foreach (var p in _model.Parameter) {
                if (p.IsLiteral) {
                    ret.Add(p.Literal);
                } else {
                    //ret.Add($"{p.Table}{tableSuffix}.{p.Column}");
                    ret.Add(p.Table + tableSuffix + "." + p.Column);
                }
            }
            return string.Join(", ", ret);
        }

        public string CreateInnerExpr()
        {
            if (!_model.HasHighLowOperator) { return string.Empty; }

            var paramList = CreateParamList(_innerTableSuffix);
            if (_model.IsLevelStepEqual) {
                //return $"{_model.FullFunctionName}({paramList}){_model.LevelStep}{_model.OppositeOperator}";
                return _model.FullFunctionName + "(" + paramList + ")" + _model.LevelStep + _model.OppositeOperator;
            }
            //Values from the same step are Incomparable
            // return $"({_model.FullFunctionName}({paramList})){_model.LevelAdditionaly}){_model.LevelStep}{_model.OppositeOperator}";
            return "(" + _model.FullFunctionName + "(" + paramList + "))" + _model.LevelAdditionaly+ ")" + _model.LevelStep + _model.OppositeOperator;

        }

        public string CreateIncomporableAttribute()
        {
            if (!_model.HasHighLowOperator || _model.IsLevelStepEqual) { return string.Empty; }
            return "'INCOMPARABLE'";
        }

    }
}
