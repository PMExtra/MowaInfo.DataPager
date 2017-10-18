using System;
using System.Linq.Expressions;

namespace MowaInfo.DataPager.Dynamic
{
    public class DynamicExpressionParser
    {
        public static LambdaExpression ParseLambda(bool createParameterCtor, Type itType, Type resultType,
            string expression, params object[] values)
        {
            return ParseLambda(createParameterCtor, new[] {Expression.Parameter(itType, "")}, resultType, expression,
                values);
        }

        public static LambdaExpression ParseLambda(FilterComparator comparator, Type itType, Type resultType,
            string name, params object[] values)
        {
            return ParseLambda(comparator, new[] {Expression.Parameter(itType, "")}, resultType, name, values);
        }

        public static LambdaExpression ParseLambda(bool createParameterCtor, ParameterExpression[] parameters,
            Type resultType, string expression, params object[] values)
        {
            var parser = new ExpressionParser(parameters, expression, values);

            return Expression.Lambda(parser.Parse(resultType, createParameterCtor), parameters);
        }

        public static LambdaExpression ParseLambda(FilterComparator comparator, ParameterExpression[] parameters,
            Type resultType, string name, params object[] values)
        {
            var parser = new ExpressionParser();

            return Expression.Lambda(parser.MyTest(parameters, name, comparator, values), parameters);
        }
    }
}