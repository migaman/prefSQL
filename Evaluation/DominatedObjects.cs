namespace prefSQL.Evaluation
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;

    public sealed class DominatedObjects
    {
        private readonly IDictionary<long, long[]> _entireDatabase;
        private readonly IDictionary<long, long[]> _skylineDatabase;
        private readonly int[] _useColumns;
        private int _numberOfDistinctDominatedObjects;
        private int _numberOfDominatedObjectsIncludingDuplicates;
        private IDictionary<long, long> _numberOfObjectsDominatedByEachObjectOrderedByDescCount;
        private int _numberOfObjectsDominatingOtherObjects;
        private IDictionary<long, ISet<long>> _result;

        public int NumberOfObjectsDominatingOtherObjects
        {
            get
            {
                if (_numberOfObjectsDominatingOtherObjects == -1)
                {
                    _numberOfObjectsDominatingOtherObjects = Result.Count;
                }
                return _numberOfObjectsDominatingOtherObjects;
            }
        }

        public int NumberOfDominatedObjectsIncludingDuplicates
        {
            get
            {
                if (_numberOfDominatedObjectsIncludingDuplicates == -1)
                {
                    _numberOfDominatedObjectsIncludingDuplicates = Result.Values.Aggregate(0,
                        (accu, item) => accu + item.Count);
                }
                return _numberOfDominatedObjectsIncludingDuplicates;
            }
        }

        public int NumberOfDistinctDominatedObjects
        {
            get
            {
                if (_numberOfDistinctDominatedObjects == -1)
                {
                    var s = new HashSet<long>();
                    foreach (long j in Result.SelectMany(i => i.Value))
                    {
                        s.Add(j);
                    }

                    _numberOfDistinctDominatedObjects = s.Count;                 
                }
                return _numberOfDistinctDominatedObjects;
            }
        }

        public IDictionary<long, long> NumberOfObjectsDominatedByEachObjectOrderedByDescCount
        {
            get
            {
                return _numberOfObjectsDominatedByEachObjectOrderedByDescCount ??
                       (_numberOfObjectsDominatedByEachObjectOrderedByDescCount =
                           GetDominatedObjectsByEachObjectOrderedByDescCount());
            }
        }

        internal IDictionary<long, ISet<long>> Result
        {
            get { return _result ?? (_result = GetDominatedObjects()); }
        }

        /// <summary>
        /// TODO X. Lin, Y. Yuan, Q. Zhang, and Y. Zhang (2007).
        /// </summary>
        /// <remarks>
        /// Publication:
        /// X. Lin, Y. Yuan, Q. Zhang, and Y. Zhang, “Selecting Stars: The k Most Representative Skyline Operator,” in 2007 IEEE 23rd International Conference on Data Engineering, 2007, pp. 86–95.
        /// </remarks>
        /// <param name="entireDatabase"></param>
        /// <param name="skylineDatabase"></param>
        /// <param name="useColumns"></param>
        public DominatedObjects(IReadOnlyDictionary<long, object[]> entireDatabase,
            IReadOnlyDictionary<long, object[]> skylineDatabase, int[] useColumns)
        {
            _entireDatabase = ConvertToLongObjects(entireDatabase,useColumns);
            _skylineDatabase = ConvertToLongObjects(skylineDatabase, useColumns);
            _useColumns = useColumns;
            _numberOfDistinctDominatedObjects = -1;
            _numberOfDominatedObjectsIncludingDuplicates = -1;
            _numberOfDistinctDominatedObjects = -1;
            _numberOfObjectsDominatingOtherObjects = -1;
        }

        private IDictionary<long, long[]> ConvertToLongObjects(IReadOnlyDictionary<long, object[]> database,
            int[] useColumns)
        {
            var ret = new Dictionary<long, long[]>();

            foreach (KeyValuePair<long, object[]> databaseObject in database)
            {
                var rowLong = new long[useColumns.Length];

                for (var i = 0; i < useColumns.Length; i++)
                {
                    int index = useColumns[i];
                    rowLong[i] = (long) databaseObject.Value[index];
                }

                ret.Add(databaseObject.Key, rowLong);
            }

            return ret;
        }

        private Dictionary<long, long> GetDominatedObjectsByEachObjectOrderedByDescCount()
        {
            var dominatedObjectsByEachObject = new Dictionary<long, long>();

            foreach (
                KeyValuePair<long, ISet<long>> dominatingObject in
                    Result.OrderByDescending(
                        key => key.Value.Count))
            {
                long numberOfDominatedObjects = dominatingObject.Value.Count;
                dominatedObjectsByEachObject.Add(dominatingObject.Key, numberOfDominatedObjects);
            }

            return dominatedObjectsByEachObject;
        }

        private IDictionary<long, ISet<long>> GetDominatedObjects()
        {
            var dominatedObjects = new Dictionary<long, ISet<long>>();

            foreach (KeyValuePair<long, long[]> entireRow in _entireDatabase)
            {
                foreach (KeyValuePair<long, long[]> skylineRow in _skylineDatabase)
                {
                    if (SQLSkyline.Helper.DoesTupleDominate(entireRow.Value, skylineRow.Value,
                        _useColumns))
                    {
                        if (!dominatedObjects.ContainsKey(skylineRow.Key))
                        {
                            dominatedObjects.Add(skylineRow.Key, new HashSet<long>());
                        }

                        dominatedObjects[skylineRow.Key].Add(entireRow.Key);
                    }
                }
            }

            return dominatedObjects;
        }
    }
}