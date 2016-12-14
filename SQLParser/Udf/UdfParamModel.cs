namespace prefSQL.SQLParser.Udf
{
    internal class UdfParamModel
    {
        public bool IsLiteral { get; set; }
        public string Literal { get; set; }
        public string Table { get; set; }
        public string Column { get; set; }
    }
}
