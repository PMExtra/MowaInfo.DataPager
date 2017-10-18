using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

namespace MowaInfo.DataPager.Dynamic
{
    /// <summary>
    ///     Provides a set of static (Shared in Visual Basic) methods for querying data structures that implement
    ///     <see cref="IQueryable" />.
    ///     It allows dynamic string based querying. Very handy when, at compile time, you don't know the type of queries that
    ///     will be generated,
    ///     or when downstream components only return column names to sort and filter by.
    /// </summary>
    public static class DynamicQueryableExtensions
    {
#if !(WINDOWS_APP45x || SILVERLIGHT)
        private static readonly TraceSource TraceSource = new TraceSource(typeof(DynamicQueryableExtensions).Name);
#endif

        private static Expression OptimizeExpression(Expression expression)
        {
            if (ExtensibilityPoint.QueryOptimizer != null)
            {
                var optimized = ExtensibilityPoint.QueryOptimizer(expression);

#if !(WINDOWS_APP45x || SILVERLIGHT)
                if (optimized != expression)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, 0, "Expression before : {0}", expression);
                    TraceSource.TraceEvent(TraceEventType.Verbose, 0, "Expression after  : {0}", optimized);
                }
#endif
                return optimized;
            }

            return expression;
        }

        #region OrderBy

        /// <summary>
        ///     Sorts the elements of a sequence in ascending or descending order according to a key.
        /// </summary>
        /// <typeparam name="TSource">The type of the elements of source.</typeparam>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="ordering">An expression string to indicate values to order by.</param>
        /// <param name="args">
        ///     An object array that contains zero or more objects to insert into the predicate as parameters.
        ///     Similar to the way String.Format formats strings.
        /// </param>
        /// <returns>
        ///     A <see cref="IQueryable{T}" /> whose elements are sorted according to the specified
        ///     <paramref name="ordering" />.
        /// </returns>
        /// <example>
        ///     <code>
        /// <![CDATA[
        /// var resultSingle = queryable.OrderBy<User>("NumberProperty");
        /// var resultSingleDescending = queryable.OrderBy<User>("NumberProperty DESC");
        /// var resultMultiple = queryable.OrderBy<User>("NumberProperty, StringProperty");
        /// ]]>
        /// </code>
        /// </example>
        public static IOrderedQueryable<TSource> OrderBy<TSource>(this IQueryable<TSource> source, string ordering,
            params object[] args)
        {
            return (IOrderedQueryable<TSource>)OrderBy((IQueryable)source, ordering, args);
        }

        /// <summary>
        ///     Sorts the elements of a sequence in ascending or descending order according to a key.
        /// </summary>
        /// <param name="source">A sequence of values to order.</param>
        /// <param name="ordering">An expression string to indicate values to order by.</param>
        /// <param name="args">
        ///     An object array that contains zero or more objects to insert into the predicate as parameters.
        ///     Similar to the way String.Format formats strings.
        /// </param>
        /// <returns>A <see cref="IQueryable" /> whose elements are sorted according to the specified <paramref name="ordering" />.</returns>
        /// <example>
        ///     <code>
        /// var resultSingle = queryable.OrderBy("NumberProperty");
        /// var resultSingleDescending = queryable.OrderBy("NumberProperty DESC");
        /// var resultMultiple = queryable.OrderBy("NumberProperty, StringProperty DESC");
        /// </code>
        /// </example>
        public static IOrderedQueryable OrderBy(this IQueryable source, string ordering, params object[] args)
        {
            ParameterExpression[] parameters = { Expression.Parameter(source.ElementType, "") };
            var parser = new ExpressionParser(parameters, ordering, args);
            var dynamicOrderings = parser.ParseOrdering();

            var queryExpr = source.Expression;

            foreach (var dynamicOrdering in dynamicOrderings)
            {
                queryExpr = Expression.Call(
                    typeof(Queryable), dynamicOrdering.MethodName,
                    new[] { source.ElementType, dynamicOrdering.Selector.Type },
                    queryExpr, Expression.Quote(Expression.Lambda(dynamicOrdering.Selector, parameters)));
            }

            var optimized = OptimizeExpression(queryExpr);
            return (IOrderedQueryable)source.Provider.CreateQuery(optimized);
        }

        #endregion OrderBy
    }
}
