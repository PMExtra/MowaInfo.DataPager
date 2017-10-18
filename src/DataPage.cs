using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;

namespace MowaInfo.DataPager
{
    public class DataPage<T>
    {
        /// <summary>
        ///     页面数据
        /// </summary>
        [Required]
        public IEnumerable<T> Data { get; set; }

        /// <summary>
        ///     总数据量
        /// </summary>
        [Required]
        public int Total { get; set; }
    }
}