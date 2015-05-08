namespace prefSQL.SQLSkyline.SamplingSkyline
{
    using System;
    using System.Collections.ObjectModel;
    using System.Linq;

    internal sealed class SkylineAlgorithmParameters
    {
        private readonly string _operators;
        private readonly ReadOnlyCollection<string> _operatorsCollection;
        private readonly int _numberOfRecords;
        private readonly bool _hasIncomparable;
        private readonly ReadOnlyCollection<string> _additionalParameters;

        public SkylineAlgorithmParameters(string operators, int numberOfRecords, bool hasIncomparable,
            string[] additionalParameters)
        {
            _operators = operators;
            _operatorsCollection = Array.AsReadOnly(_operators.Split(';'));
            _numberOfRecords = numberOfRecords;
            _hasIncomparable = hasIncomparable;
            _additionalParameters = Array.AsReadOnly(additionalParameters);
        }

        public SkylineAlgorithmParameters(string subpaceOperators, SkylineAlgorithmParameters skylineAlgorithmParameters)
            : this(
                subpaceOperators, skylineAlgorithmParameters.NumberOfRecords, skylineAlgorithmParameters.HasIncomparable,
                skylineAlgorithmParameters.AdditionalParameters.ToArray())
        {
        }

        public string Operators
        {
            get { return _operators; }
        }

        public ReadOnlyCollection<string> OperatorsCollection
        {
            get { return _operatorsCollection; }
        }

        public int NumberOfRecords
        {
            get { return _numberOfRecords; }
        }

        public bool HasIncomparable
        {
            get { return _hasIncomparable; }
        }

        public ReadOnlyCollection<string> AdditionalParameters
        {
            get { return _additionalParameters; }
        }
    }
}