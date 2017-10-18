using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;

namespace MowaInfo.DataPager.Dynamic
{
    // ReSharper disable once InconsistentNaming
    internal static class IQueryableExtensions
    {
        private static readonly TraceSource TraceSource = new TraceSource(typeof(IQueryableExtensions).Name);

        private static Expression OptimizeExpression(Expression expression)
        {
            if (ExtensibilityPoint.QueryOptimizer != null)
            {
                var optimized = ExtensibilityPoint.QueryOptimizer(expression);
                if (optimized != expression)
                {
                    TraceSource.TraceEvent(TraceEventType.Verbose, 0, "Expression before : {0}", expression);
                    TraceSource.TraceEvent(TraceEventType.Verbose, 0, "Expression after  : {0}", optimized);
                }
                return optimized;
            }

            return expression;
        }

        public static IQueryable Where(this IQueryable source, string predicate, params object[] args)
        {
            var createParameterCtor = source.IsLinqToObjects();
            var lambda = DynamicExpressionParser.ParseLambda(createParameterCtor, source.ElementType, typeof(bool),
                predicate, args);

            var optimized = OptimizeExpression(Expression.Call(typeof(Queryable), "Where", new[] { source.ElementType },
                source.Expression, Expression.Quote(lambda)));
            return source.Provider.CreateQuery(optimized);
        }

        public static IQueryable Where(this IQueryable source, string name, FilterComparator comparator,
            params object[] args)
        {
            var lambda = DynamicExpressionParser.ParseLambda(comparator, source.ElementType, typeof(bool), name, args);

            var optimized = OptimizeExpression(Expression.Call(typeof(Queryable), "Where", new[] { source.ElementType },
                source.Expression, Expression.Quote(lambda)));
            return source.Provider.CreateQuery(optimized);
        }
    }
}
