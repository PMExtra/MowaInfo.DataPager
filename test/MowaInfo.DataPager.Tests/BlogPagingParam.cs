using System;
using System.Linq;

namespace MowaInfo.DataPager.Tests
{
    internal class BlogPagingParam : PagingParam
    {
        [PropertyFilter(Comparator = FilterComparator.Default)]
        public int? BlogId { get; set; }

        [PropertyFilter(Comparator = FilterComparator.Equals)]
        public DateTime? PublishTime { get; set; }

        [PropertyFilter(Comparator = FilterComparator.Contains)]
        public string BlogTitle { get; set; }

        [PropertyFilter(Comparator = FilterComparator.GreaterThan)]
        public int? Gift { get; set; }

        [PropertyFilter(Comparator = FilterComparator.GreaterOrEquals)]
        public int? UserId { get; set; }

        [PropertyFilter(Comparator = FilterComparator.LessThan)]
        public int? TimesLiked { get; set; }


        [PropertyFilter(Comparator = FilterComparator.LessOrEquals)]
        public int? TimesWatched { get; set; }


        [PropertyFilter(Comparator = FilterComparator.Unequals)]
        public string Url { get; set; }


        [PropertyFilter(Comparator = FilterComparator.Custom)]
        public string UserName { get; set; }


        [PropertyFilter(Comparator = FilterComparator.Default)]
        public string BlogType { get; set; }

        /// <summary>
        ///     自定义过滤器
        /// </summary>
        /// <param name="query">查询语句</param>
        /// <returns>修改后的查询语句</returns>
        public override IQueryable CustomFilter(IQueryable query)
        {
            return query;
        }
    }
}
