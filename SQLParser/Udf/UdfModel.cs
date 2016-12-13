using System.Collections.Generic;

namespace prefSQL.SQLParser.Udf
{
    internal class UdfModel
    {
        public string SchemaName { get; set; }
        public string FunctionName { get; set; }
        public string FullFunctionName { get; set; }
        public readonly List<UdfParamModel> Parameter = new List<UdfParamModel>();

        // STEP operator
        public bool IsLevelStepEqual { get; set; }
        public string LevelStep { get; set; }
        public string LevelAdd { get; set; }
        public string LevelMinus { get; set; }

        public bool IsComparable { get; set; }
        public bool HasIncomparableTuples { get; set; }
        public bool ContainsOpenPreference { get; set; }

        // High Low Operator
        public bool HasHighLowOperator { get; set; }
        public string OppositeOperator { get; set; }
        public string LevelAdditionaly { get; set; }
    }
}
