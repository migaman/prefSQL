using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace prefSQL.SQLParserTest
{
    internal sealed class SkylineSamplingHelper
    {
        private static readonly Random MyRandom = new Random();

        public static void NormalizeColumns(IDictionary<long, object[]> data, int[] columsToNormalize)
        {
            long[] highestValue;
            long[] lowestValue;
            long[] zeroBasedHighestValue;

            InitializeBoundsArrays(columsToNormalize.Length, out lowestValue, out highestValue,
                out zeroBasedHighestValue);
            CalculateBoundsOfColumns(data, columsToNormalize, lowestValue, highestValue, zeroBasedHighestValue);

            foreach (var row in data)
            {
                for (var column = 0; column < columsToNormalize.Length; column++)
                {
                    var index = columsToNormalize[column];
                    row.Value[index] = (Convert.ToDouble(row.Value[index]) - lowestValue[column])/
                                       zeroBasedHighestValue[column];
                }
            }
        }

        internal static void InitializeBoundsArrays(int columnsCount, out long[] lowestValue, out long[] highestValue,
            out long[] zeroBasedHighestValue)
        {
            highestValue = new long[columnsCount];
            lowestValue = new long[columnsCount];
            zeroBasedHighestValue = new long[columnsCount];

            for (var i = 0; i < columnsCount; i++)
            {
                highestValue[i] = long.MinValue;
                lowestValue[i] = long.MaxValue;
                zeroBasedHighestValue[i] = long.MinValue;
            }
        }

        internal static void CalculateBoundsOfColumns(IDictionary<long, object[]> data, int[] useColumns,
            long[] lowestValue, long[] highestValue, long[] rangeValuesZeroBased)
        {
            foreach (var row in data)
            {
                for (var column = 0; column < useColumns.Length; column++)
                {
                    var rowValue = (long) row.Value[useColumns[column]];
                    if (rowValue > highestValue[column])
                    {
                        highestValue[column] = rowValue;
                    }
                    if (rowValue < lowestValue[column])
                    {
                        lowestValue[column] = rowValue;
                    }
                }
            }

            for (var i = 0; i < rangeValuesZeroBased.Length; i++)
            {
                rangeValuesZeroBased[i] = highestValue[i] - lowestValue[i];
            }
        }

        public static int[] GetSkylineAttributeColumns(DataTable dataTable)
        {
            return GetSkylineAttributeColumns(dataTable, "SkylineAttribute");
        }

        public static int[] GetSkylineAttributeColumns(DataTable dataTable, string skylineAttributePrefix)
        {
            var skylineAttributeColumnsStart = GetSkylineAttributeColumnsStart(dataTable, skylineAttributePrefix);

            var arrayReturn = new int[dataTable.Columns.Count - skylineAttributeColumnsStart];

            for (var i = 0; i < arrayReturn.Length; i++)
            {
                arrayReturn[i] = i + skylineAttributeColumnsStart;
            }

            return arrayReturn;
        }

        internal static int GetSkylineAttributeColumnsStart(DataTable dataTable, string skylineAttributePrefix)
        {
            var skylineAttributeColumnsStart = 0;

            for (var i = 0; i < dataTable.Columns.Count; i++)
            {
                if (dataTable.Columns[i].Caption.StartsWith(skylineAttributePrefix))
                {
                    skylineAttributeColumnsStart = i;
                    break;
                }
            }

            return skylineAttributeColumnsStart;
        }

        public static IDictionary<long, object[]> GetRandomSample(IDictionary<long, object[]> data, int size)
        {
            var randomSampleReturn = new Dictionary<long, object[]>();

            var allKeys = data.Keys.ToList();

            while (randomSampleReturn.Count < size)
            {
                var randomKey = MyRandom.Next(allKeys.Count);

                if (!randomSampleReturn.ContainsKey(allKeys[randomKey]))
                {
                    randomSampleReturn.Add(allKeys[randomKey], data[allKeys[randomKey]]);
                }
            }

            return randomSampleReturn;
        }
    }
}