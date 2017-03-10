using System;
using System.Collections.Generic;
using System.Data;

//!!!Caution: Attention small changes in this code can lead to remarkable performance issues!!!!
namespace prefSQL.SQLSkyline
{
    using System.Linq;

    /// <summary>
    /// DQ Algorithm implemented according to algorithm pseudocode in Börzsönyi et al. (2001) and Rost(2006)
    /// </summary>
    /// <remarks>
    /// Börzsönyi, Stephan; Kossmann, Donald; Stocker, Konrad (2001): The Skyline Operator. In : 
    /// Proceedings of the 17th International Conference on Data Engineering. Washington, DC, USA: 
    /// IEEE Computer Society, pp. 421–430. Available online at http://dl.acm.org/citation.cfm?id=645484.656550.
    /// 
    /// Rost, Steffen Thomas (2006): Skyline query processing: University Heidelberg; Fakultät für Mathematik und Informatik. Institut für Informatik.
    /// 
    /// Profiling considersations:
    /// - Always use equal when comparins test --> i.e. using a startswith instead of an equal can decrease performance by 10 times
    /// - Write objects from DataReader into an object[] an work with the object. 
    /// - Don't use DataTable through the function (just use an object[] array. 10 times faster!!)
    /// - Explicity convert (i.e. (int)reader[0]) value from DataReader and don't use the given methods (i.e. reader.getInt32(0))
    /// </remarks>
    public class TemplateDQ : TemplateStrategy
    {
        protected override DataTable GetSkylineFromAlgorithm(IEnumerable<object[]> database, DataTable dataTableTemplate, string[] operatorsArray, string[] additionalParameters)
        {
            DataTable dataTableReturn = dataTableTemplate;

            //Work with object[]-array (more than 10 times faster than datatable)
            List<object[]> listResult = ComputeSkyline(database.ToList(), operatorsArray, operatorsArray.GetUpperBound(0));

            //Stores all the skyline values from the relevant rows
            //Used for the special sorting function like BEST_RANK(), SUM_RANK(),...
            List<long[]> skylineValues = new List<long[]>();

            //Write object in datatable
            foreach (object[] row in listResult)
            {

                //int validDataFrom = dataTableReturn.Columns.Count - operatorsArray.GetUpperBound(0) - 1;
                int validDataFrom = dataTableReturn.Columns.Count;
                object[] resultArray = new object[validDataFrom];
                //First columns are skyline columns, there start with index after skyline column
                Array.Copy(row, operatorsArray.GetUpperBound(0) + 1, resultArray, 0, validDataFrom);
                dataTableReturn.Rows.Add(resultArray);

                //Copy skyline values to special array (needed for special sorting functions)
                long[] sValues = new long[operatorsArray.GetUpperBound(0) + 1];
                for(int i=0;i < operatorsArray.GetUpperBound(0) + 1; i++) {
                    sValues[i] = (long)row[i];
                }
                skylineValues.Add(sValues);

            }

            //Special orderings need the skyline values. Store it in a property
            SkylineValues = skylineValues;

            return dataTableReturn;
        }



        private List<object[]> ComputeSkyline(List<object[]> listObjects, string[] operators, int dim)
        {
            if (listObjects.Count <= 1)
                return listObjects;


            //compute first median for some dimension
            double pivot = GetMedian(listObjects, dim);

            List<object[]> list1 = new List<object[]>();
            List<object[]> list2 = new List<object[]>();

            //divide input intwo 2 partitions
            Partition(listObjects, dim, pivot, ref list1, ref list2);


            //daten waren nicht weiter splittbar
            if (list1.Count == 0 || list2.Count == 0)
            {

                if (listObjects.Count == 1)
                {
                    return listObjects;
                }
                else
                {
                    //in dieser dimension nicht weiter splittbar --> versuchen in einer dimension tiefer zu splitten
                    if (dim > 0)
                    {
                        return ComputeSkyline(listObjects, operators, dim - 1);
                    }
                    else
                    {
                        //alle in skyline, keine weitere trennung möglich
                        return listObjects;
                    }
                }
            }



            //Rekursiv aufrufen
            List<object[]> skyline1 = ComputeSkyline(list1, operators, dim);
            List<object[]> skyline2 = ComputeSkyline(list2, operators, dim);


            List<object[]> dtMerge = MergeBasic(skyline1, skyline2, operators, operators.GetUpperBound(0));
            if (dtMerge.Count > 0)
            {
                foreach (object[] row in dtMerge)
                {
                    skyline1.Add(row);
                }
            }

            return skyline1;
        }


        private void Partition(List<object[]> listObjects, int dim, double pivot, ref List<object[]> list1, ref List<object[]> list2)
        {
            //divide input intwo 2 partitions
            for (int iRow = 0; iRow < listObjects.Count; iRow++)
            {
                if ((long)listObjects[iRow][dim] <= pivot)
                {
                    list1.Add(listObjects[iRow]);
                }
                else
                {
                    list2.Add(listObjects[iRow]);
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="s1"></param>
        /// <param name="s2"></param>
        /// <param name="operators"></param>
        /// <param name="dim"></param>
        /// <returns>tuples from skyline table 2 that are not dominated</returns>
        private List<object[]> MergeBasic(List<object[]> s1, List<object[]> s2, string[] operators, int dim)
        {
            //Clone the structure (not the record) from table1
            List<object[]> listSkyline = new List<object[]>();// s1.Clone();

            if (dim == 0)
            {
                //No Operation, return only the left list
            }
            else if (s1.Count == 0) 
            {
                return s2;
            }
            else if (s2.Count == 0)
            {
                return listSkyline; // dtSkyline;
            } 
            else if (s1.Count == 1)
            {
                //OK, falls p nicht dominiert wird von q
                object[] p = s1[0];
                for (int i = 0; i < s2.Count; i++)
                {
                    object[] q = s2[i];

                    for (int iDim = dim - 1; iDim >= 0; iDim--)
                    {
                        if ((long)q[iDim] < (long)p[iDim])
                        {
                            listSkyline.Add(q);
                            break;
                        }
                    }

                }

            }
            else if (s2.Count == 1)
            {
                //wenn p von q dominiert weg --> list

                //Add tuple only if is is not dominated from one of the others
                listSkyline.Add(s2[0]);
                object[] q = s2[0];

                for (int i = 0; i < s1.Count; i++)
                {
                    object[] p = s1[i];
                    bool doesDominate = false;

                    for (int iDim = dim- 1; iDim >= 0; iDim--)
                    {
                        //Is better in at least one dimension!
                        //<= is wrong, otherwise tuples with equal values in one dimension are not removed from the skyline
                        if ((long)q[iDim] < (long)p[iDim])
                        {
                            doesDominate = true;
                            break;
                        }
                    }

                    if (doesDominate == false)
                    {
                        listSkyline.Clear();
                        break;
                    }
                }
            }
            else if (operators.GetUpperBound(0) == 1)
            {
                //Nur 2 Dimensionen
                long min = (long)s1[0][dim - 1];
                for (int i = 1; i < s1.Count; i++)
                {
                    if ((long)s1[i][dim - 1] < min)
                        min = (long)s1[i][dim - 1];
                }

                for (int i = 0; i < s2.Count; i++)
                {
                    object[] q = s2[i];
                    if ((long)q[dim - 1] < min)
                    {
                        listSkyline.Add(q);
                    }
                }
            }
            else
            {
                double pivot1 = GetMedian(s1, dim - 1);
                List<object[]> s11 = new List<object[]>();
                List<object[]> s12 = new List<object[]>();
                List<object[]> s21 = new List<object[]>();
                List<object[]> s22 = new List<object[]>();

                Partition(s1, dim - 1, pivot1, ref s11, ref s12);
                Partition(s2, dim - 1, pivot1, ref s21, ref s22);

                if(s12.Count == 0 && s22.Count == 0)
                {
                    if(s11.Count > 1 && s21.Count > 1)
                    {
                        //all elements have same value --> compare all tuples against all tuples (BNL-Style)
                        
                        //compare all from s21 against s11
                        for (int i = 0; i < s21.Count; i++)
                        {
                            //Import row
                            bool isDominated = false;
                            for(int ii = 0; ii < s11.Count; ii++)
                            {
                                bool isNotDominated = false;
                                for (int iDim = dim - 1; iDim >= 0; iDim--)
                                {
                                    if ((long)s21[i][iDim] < (long)s11[ii][iDim])
                                    {
                                        isNotDominated = true;
                                        break;
                                    }
                                }
                                if (isNotDominated == false)
                                {
                                    isDominated = true;
                                    break;
                                }
                            }
                            if (isDominated == false)
                            {
                                listSkyline.Add(s21[i]);
                            }
                        }

                        return listSkyline;
                    }                    
                }

                List<object[]> r1 = MergeBasic(s11, s21, operators, dim);
                List<object[]> r2 = MergeBasic(s12, s22, operators, dim);
                List<object[]> r3 = MergeBasic(s11, r2, operators, dim - 1);
                listSkyline.AddRange(r1);
                listSkyline.AddRange(r3);
            }

            return listSkyline;
        }

        /// <summary>
        /// Computes the median of an object[]-array
        /// </summary>
        /// <param name="listObjects"></param>
        /// <param name="dim"></param>
        /// <returns></returns>
        private double GetMedian(List<object[]> listObjects, int dim)
        {
            //Framework 2.0 version of this method. there is an easier way in F4        
            if (listObjects == null || listObjects.Count == 0)
                return 0;

            //HashSet<long> uniqueNumbers = new HashSet<long>();
            //HashSet is not supported with CLR --> use Dictionary and set all values true
            var uniqueNumbers = new CLRSafeHashSet<long>();
            //HashSet is not allowed inside MS SQL CLR
            //generate list of unique integers of this dimension
            for (int i = 0; i < listObjects.Count; i++)
            {
                uniqueNumbers.Add((long)listObjects[i][dim]);
            }
            long[] sourceNumbers = new long[uniqueNumbers.Count];
            uniqueNumbers.CopyTo(sourceNumbers, 0);

            //make sure the list is sorted, but use a new array
            long[] sortedPNumbers = (long[])sourceNumbers.Clone();
            sourceNumbers.CopyTo(sortedPNumbers, 0);
            Array.Sort(sortedPNumbers);

            //get the median
            int size = sortedPNumbers.Length;
            int mid = size / 2;

            //compute the double value because if one element is 1 and the other 2, otherwise the tuples are not splitted
            double median = (size % 2 != 0) ? sortedPNumbers[mid] : (double)((sortedPNumbers[mid] + sortedPNumbers[mid - 1])) / 2;

            return median;
        }


    }
}
