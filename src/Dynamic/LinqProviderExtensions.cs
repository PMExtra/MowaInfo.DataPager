using System.Linq;
using System.Reflection;

namespace MowaInfo.DataPager.Dynamic
{
    internal static class LinqProviderExtensions
    {
        /// <summary>
        ///     Check if the Provider from IQueryable is a LinqToObjects provider.
        /// </summary>
        /// <param name="source">The IQueryable</param>
        /// <returns>true if provider is LinqToObjects, else false</returns>
        public static bool IsLinqToObjects(this IQueryable source)
        {
            return IsProviderEnumerableQuery(source.Provider);
        }

        private static bool IsProviderEnumerableQuery(IQueryProvider provider)
        {
            var baseType = provider.GetType().GetTypeInfo().BaseType;

            var isLinqToObjects = baseType == typeof(EnumerableQuery);

            if (!isLinqToObjects)
            {
                try
                {
                    var property = baseType.GetProperty("OriginalProvider");
                    var originalProvider = property.GetValue(provider, null) as IQueryProvider;
                    return originalProvider != null && IsProviderEnumerableQuery(originalProvider);
                }
                catch
                {
                    return false;
                }
            }

            return isLinqToObjects;
        }
    }
}
