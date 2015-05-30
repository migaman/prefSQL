namespace prefSQL.Evaluation
{
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;

    public class DominatedObjects
    {
        private readonly IReadOnlyDictionary<long, object[]> _entireDatabase;
        private readonly IReadOnlyDictionary<long, object[]> _skylineDatabase;
        private readonly int[] _useColumns;
        private int _numberOfDistinctDominatedObjects;
        private int _numberOfDominatedObjectsIncludingDuplicates;
        private IDictionary<long, long> _numberOfObjectsDominatedByEachObjectOrderedByDescCount;
        private int _numberOfObjectsDominatingOtherObjects;
        private IReadOnlyDictionary<long, List<IReadOnlyDictionary<long, object[]>>> _result;

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
                    _numberOfDistinctDominatedObjects =
                        Result.Values.SelectMany(
                            objectsDominatedByOneObject =>
                                objectsDominatedByOneObject.SelectMany(
                                    objectDominatedByOneObject => objectDominatedByOneObject.Keys)).Distinct().Count();
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

        internal IReadOnlyDictionary<long, List<IReadOnlyDictionary<long, object[]>>> Result
        {
            get { return _result ?? (_result = GetDominatedObjects()); }
        }

        public DominatedObjects(IReadOnlyDictionary<long, object[]> entireDatabase,
            IReadOnlyDictionary<long, object[]> skylineDatabase, int[] useColumns)
        {
            _entireDatabase = entireDatabase;
            _skylineDatabase = skylineDatabase;
            _useColumns = useColumns;
            _numberOfDistinctDominatedObjects = -1;
            _numberOfDominatedObjectsIncludingDuplicates = -1;
            _numberOfDistinctDominatedObjects = -1;
            _numberOfObjectsDominatingOtherObjects = -1;
        }

        private Dictionary<long, long> GetDominatedObjectsByEachObjectOrderedByDescCount()
        {
            var dominatedObjectsByEachObject = new Dictionary<long, long>();

            foreach (
                KeyValuePair<long, List<IReadOnlyDictionary<long, object[]>>> dominatingObject in
                    Result.OrderByDescending(
                        key => key.Value.Aggregate(0, (accu, item) => accu + item.Count)))
            {
                long numberOfDominatedObjects = dominatingObject.Value.Aggregate(0, (accu, item) => accu + item.Count);
                dominatedObjectsByEachObject.Add(dominatingObject.Key, numberOfDominatedObjects);
            }

            return dominatedObjectsByEachObject;
        }

        private IReadOnlyDictionary<long, List<IReadOnlyDictionary<long, object[]>>> GetDominatedObjects()
        {
            var dominatedObjects = new Dictionary<long, List<IReadOnlyDictionary<long, object[]>>>();

            foreach (KeyValuePair<long, object[]> entireRow in _entireDatabase)
            {
                var potentiallyDominatedTuple = new long[_useColumns.Length];

                for (var column = 0; column < _useColumns.Length; column++)
                {
                    int index = _useColumns[column];
                    potentiallyDominatedTuple[column] = (long) entireRow.Value[index];
                }

                foreach (KeyValuePair<long, object[]> skylineRow in _skylineDatabase)
                {
                    var potentiallyDominatingTuple = new long[_useColumns.Length];

                    for (var column = 0; column < _useColumns.Length; column++)
                    {
                        int index = _useColumns[column];
                        potentiallyDominatingTuple[column] = (long) skylineRow.Value[index];
                    }

                    if (SQLSkyline.Helper.DoesTupleDominate(potentiallyDominatedTuple, potentiallyDominatingTuple,
                        _useColumns))
                    {
                        if (!dominatedObjects.ContainsKey(skylineRow.Key))
                        {
                            dominatedObjects.Add(skylineRow.Key, new List<IReadOnlyDictionary<long, object[]>>());
                        }

                        dominatedObjects[skylineRow.Key].Add(
                            new ReadOnlyDictionary<long, object[]>(new Dictionary<long, object[]>
                            {
                                {entireRow.Key, entireRow.Value}
                            }));
                    }
                }
            }

            return new ReadOnlyDictionary<long, List<IReadOnlyDictionary<long, object[]>>>(dominatedObjects);
        }
    }
}