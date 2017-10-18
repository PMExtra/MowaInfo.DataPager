using System;
using System.Collections;

namespace MowaInfo.DataPager
{
    [AttributeUsage(AttributeTargets.Property)]
    public class PropertyFilterAttribute : Attribute
    {
        /// <summary>
        ///     Filtered property name. Default by this property name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        ///     Specified how to filter between source and this value.
        ///     Default be <see cref="FilterComparator.Contains" /> of property which is assiged from <see cref="IEnumerable" />,
        ///     else be <see cref="FilterComparator.Equals" /> by default.
        /// </summary>
        public FilterComparator Comparator { get; set; }
    }
}
