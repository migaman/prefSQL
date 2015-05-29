namespace prefSQL.SQLSkyline
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    public sealed class CLRSafeHashSet<T> : ISet<T>
    {
        private readonly IDictionary<T, bool> _backingCollection;

        /// <summary>
        /// TODO: http://stackoverflow.com/questions/23140885/why-is-hashsett-attributed-with-mayleakonabort-but-dictionaryk-v-not/23141331#23141331
        /// TODO: http://www.dotnetframework.org/default.aspx/4@0/4@0/untmp/DEVDIV_TFS/Dev10/Releases/RTMRel/ndp/fx/src/Core/System/Collections/Generic/HashSet@cs/1305376/HashSet@cs
        /// </summary>
        public CLRSafeHashSet()
        {
            _backingCollection = new Dictionary<T, bool>();
        }

        public CLRSafeHashSet(IEnumerable<T> collection)
        {
            _backingCollection = new Dictionary<T, bool>();

            foreach (T item in collection)
            {
                Add(item);
            }
        }

        /// <summary>
        /// TODO: WARNING, convenience, NOT FOR CLR
        /// </summary>
        /// <returns></returns>
        public HashSet<T> ToHashSet()
        {
            return new HashSet<T>(_backingCollection.Keys);
        }

        #region Implementation of IEnumerable

        public IEnumerator<T> GetEnumerator()
        {
            return _backingCollection.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Implementation of ICollection<T>

        void ICollection<T>.Add(T item)
        {
            if (!_backingCollection.ContainsKey(item))
            {
                _backingCollection.Add(item, true);
            }
        }

        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            if (other == null)
            {
                throw new ArgumentNullException("other");
            }

            var count = 0;

            foreach (T item in other)
            {
                count++;
                if (!Contains(item))
                {
                    return false;
                }
            }

            return count == Count;
        }

        public bool Add(T item)
        {
            if (_backingCollection.ContainsKey(item))
            {
                return false;
            }

            _backingCollection.Add(item, true);

            return true;
        }

        public void Clear()
        {
            _backingCollection.Clear();
        }

        public bool Contains(T item)
        {
            return _backingCollection.ContainsKey(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _backingCollection.Keys.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return _backingCollection.Remove(item);
        }

        public int Count
        {
            get { return _backingCollection.Count; }
        }

        public bool IsReadOnly
        {
            get { return _backingCollection.IsReadOnly; }
        }

        #endregion
    }
}