using System.Collections.Generic;

namespace MowaInfo.DataPager
{
    public abstract class SafePagingParam : PagingParam
    {
        protected abstract IEnumerable<string> OrderableFields { get; }

        internal IEnumerable<string> InternalOrderableFields => OrderableFields;
    }
}
