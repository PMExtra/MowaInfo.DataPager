using System;

namespace MowaInfo.DataPager
{
    internal static class QueryGenerator
    {
        internal static string WherePredict(string name, FilterComparator comparator, int argIndex = 0)
        {
            switch (comparator)
            {
                case FilterComparator.Contains:
                    return $"{name}.Contains(@{argIndex})";

                case FilterComparator.Equals:
                    return $"{name} == @{argIndex}";

                case FilterComparator.GreaterThan:
                    return $"{name} > @{argIndex}";

                case FilterComparator.GreaterOrEquals:
                    return $"{name} >= @{argIndex}";

                case FilterComparator.LessOrEquals:
                    return $"{name} <= @{argIndex}";

                case FilterComparator.LessThan:
                    return $"{name} < @{argIndex}";

                case FilterComparator.Unequals:
                    return $"{name} != @{argIndex}";

                case FilterComparator.Default:
                case FilterComparator.Custom:
                    throw new ArgumentOutOfRangeException(nameof(comparator), "");

                default:
                    throw new NotImplementedException();
            }
        }
    }
}
