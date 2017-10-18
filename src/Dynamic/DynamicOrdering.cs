using System.Linq.Expressions;

namespace MowaInfo.DataPager.Dynamic
{
    internal class DynamicOrdering
    {
        public bool Ascending;
        public string MethodName;
        public Expression Selector;
    }
}