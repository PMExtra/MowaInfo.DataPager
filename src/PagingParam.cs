using System.Linq;

namespace MowaInfo.DataPager
{
    public class PagingParam
    {
        /// <summary>
        ///     页码（从1开始）
        /// </summary>
        public int Page { get; set; } = 1;

        /// <summary>
        ///     每页数量
        /// </summary>
        public int PageSize { get; set; } = 20;

        /// <summary>
        ///     排序依据
        /// </summary>
        public string[] OrderBy { get; set; }

        /// <summary>
        ///     flase = 正序 / true = 逆序
        /// </summary>
        public bool[] Descending { get; set; }

        /// <summary>
        ///     自定义过滤器
        /// </summary>
        /// <param name="query">查询语句</param>
        /// <returns>修改后的查询语句</returns>
        public virtual IQueryable CustomFilter(IQueryable query)
        {
            return query;
        }
    }
}