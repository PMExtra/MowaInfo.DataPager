using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace MowaInfo.DataPager.Dynamic
{
    internal class ExpressionParser
    {
        private const string SYMBOL_IT = "$";
        private const string SYMBOL_PARENT = "^";
        private const string SYMBOL_ROOT = "~";

        private const string KEYWORD_IT = "it";
        private const string KEYWORD_PARENT = "parent";
        private const string KEYWORD_ROOT = "root";
        private const string KEYWORD_IIF = "iif";
        private const string KEYWORD_NEW = "new";
        private const string KEYWORD_ISNULL = "isnull";

        // These shorthands have different name than actual type and therefore not recognized by default from the _predefinedTypes
        private static readonly Dictionary<string, Type> _predefinedTypesShorthands = new Dictionary<string, Type>
        {
            {"int", typeof(int)},
            {"uint", typeof(uint)},
            {"short", typeof(short)},
            {"ushort", typeof(ushort)},
            {"long", typeof(long)},
            {"ulong", typeof(ulong)},
            {"bool", typeof(bool)},
            {"float", typeof(float)}
        };

        private static readonly Dictionary<Type, int> _predefinedTypes = new Dictionary<Type, int>
        {
            {typeof(object), 0},
            {typeof(bool), 0},
            {typeof(char), 0},
            {typeof(string), 0},
            {typeof(sbyte), 0},
            {typeof(byte), 0},
            {typeof(short), 0},
            {typeof(ushort), 0},
            {typeof(int), 0},
            {typeof(uint), 0},
            {typeof(long), 0},
            {typeof(ulong), 0},
            {typeof(float), 0},
            {typeof(double), 0},
            {typeof(decimal), 0},
            {typeof(DateTime), 0},
            {typeof(DateTimeOffset), 0},
            {typeof(TimeSpan), 0},
            {typeof(Guid), 0},
            {typeof(Math), 0},
            {typeof(Convert), 0},
            {typeof(Uri), 0}
        };

        private static readonly Expression TrueLiteral = Expression.Constant(true);
        private static readonly Expression FalseLiteral = Expression.Constant(false);
        private static readonly Expression NullLiteral = Expression.Constant(null);

        private static readonly string methodOrderBy = nameof(Queryable.OrderBy);
        private static readonly string methodOrderByDescending = nameof(Queryable.OrderByDescending);
        private static readonly string methodThenBy = nameof(Queryable.ThenBy);
        private static readonly string methodThenByDescending = nameof(Queryable.ThenByDescending);

        private static Dictionary<string, object> _keywords;
        private readonly Dictionary<string, object> _internals;
        private readonly Dictionary<Expression, string> _literals;

        private readonly Dictionary<string, object> _symbols;
        private readonly TextParser _textParser;
        private bool _createParameterCtor;
        private IDictionary<string, object> _externals;
        private ParameterExpression _it;
        private ParameterExpression _parent;

        private Type _resultType;
        private ParameterExpression _root;

        public ExpressionParser()
        {
        }

        public ExpressionParser(ParameterExpression[] parameters, string expression, object[] values)
        {
            if (_keywords == null)
                _keywords = CreateKeywords();

            _symbols = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            _internals = new Dictionary<string, object>();
            _literals = new Dictionary<Expression, string>();

            if (parameters != null)
                ProcessParameters(parameters);

            if (values != null)
                ProcessValues(values);

            _textParser = new TextParser(expression);
        }

        private static void UpdatePredefinedTypes(string typeName, int x)
        {
            try
            {
                var efType = Type.GetType(typeName);
                if (efType != null)
                    _predefinedTypes.Add(efType, x);
            }
            catch
            {
                // in case of exception, do not add
            }
        }

        private void ProcessParameters(ParameterExpression[] parameters)
        {
            foreach (var pe in parameters.Where(p => !string.IsNullOrEmpty(p.Name)))
                AddSymbol(pe.Name, pe);

            // If there is only 1 ParameterExpression, do also allow access using 'it'
            if (parameters.Length == 1)
            {
                _parent = _it;
                _it = parameters[0];

                if (_root == null)
                    _root = _it;
            }
        }

        private void ProcessValues(object[] values)
        {
            for (var i = 0; i < values.Length; i++)
            {
                var value = values[i];
                IDictionary<string, object> externals;

                if (i == values.Length - 1 && (externals = value as IDictionary<string, object>) != null)
                    _externals = externals;
                else
                    AddSymbol("@" + i.ToString(CultureInfo.InvariantCulture), value);
            }
        }

        private void AddSymbol(string name, object value)
        {
            if (_symbols.ContainsKey(name))
                throw ParseError(Res.DuplicateIdentifier, name);

            _symbols.Add(name, value);
        }

        public Expression Parse(Type resultType, bool createParameterCtor)
        {
            _resultType = resultType;
            _createParameterCtor = createParameterCtor;

            var exprPos = _textParser.CurrentToken.Pos;
            var expr = ParseConditionalOperator();

            if (resultType != null)
                if ((expr = PromoteExpression(expr, resultType, true, false)) == null)
                    throw ParseError(exprPos, Res.ExpressionTypeMismatch, GetTypeName(resultType));

            _textParser.ValidateToken(TokenId.End, Res.SyntaxError);

            return expr;
        }

#pragma warning disable 0219
        public IList<DynamicOrdering> ParseOrdering(bool forceThenBy = false)
        {
            var orderings = new List<DynamicOrdering>();
            while (true)
            {
                var expr = ParseConditionalOperator();
                var ascending = true;
                if (TokenIdentifierIs("asc") || TokenIdentifierIs("ascending"))
                {
                    _textParser.NextToken();
                }
                else if (TokenIdentifierIs("desc") || TokenIdentifierIs("descending"))
                {
                    _textParser.NextToken();
                    ascending = false;
                }

                string methodName;
                if (forceThenBy || orderings.Count > 0)
                    methodName = ascending ? methodThenBy : methodThenByDescending;
                else
                    methodName = ascending ? methodOrderBy : methodOrderByDescending;

                orderings.Add(new DynamicOrdering {Selector = expr, Ascending = ascending, MethodName = methodName});

                if (_textParser.CurrentToken.Id != TokenId.Comma)
                    break;

                _textParser.NextToken();
            }

            _textParser.ValidateToken(TokenId.End, Res.SyntaxError);
            return orderings;
        }
#pragma warning restore 0219

        // ?: operator
        private Expression ParseConditionalOperator()
        {
            var errorPos = _textParser.CurrentToken.Pos;
            var expr = ParseNullCoalescingOperator();
            if (_textParser.CurrentToken.Id == TokenId.Question)
            {
                _textParser.NextToken();
                var expr1 = ParseConditionalOperator();
                _textParser.ValidateToken(TokenId.Colon, Res.ColonExpected);
                _textParser.NextToken();
                var expr2 = ParseConditionalOperator();
                expr = GenerateConditional(expr, expr1, expr2, errorPos);
            }
            return expr;
        }

        // ?? (null-coalescing) operator
        private Expression ParseNullCoalescingOperator()
        {
            var expr = ParseLambdaOperator();
            if (_textParser.CurrentToken.Id == TokenId.NullCoalescing)
            {
                _textParser.NextToken();
                var right = ParseConditionalOperator();
                expr = Expression.Coalesce(expr, right);
            }
            return expr;
        }

        // => operator - Added Support for projection operator
        private Expression ParseLambdaOperator()
        {
            var expr = ParseOrOperator();
            if (_textParser.CurrentToken.Id == TokenId.Lambda && _it.Type == expr.Type)
            {
                _textParser.NextToken();
                if (_textParser.CurrentToken.Id == TokenId.Identifier ||
                    _textParser.CurrentToken.Id == TokenId.OpenParen)
                {
                    var right = ParseConditionalOperator();
                    return Expression.Lambda(right, (ParameterExpression) expr);
                }
                _textParser.ValidateToken(TokenId.OpenParen, Res.OpenParenExpected);
            }
            return expr;
        }

        // isnull(a,b) operator
        private Expression ParseIsNull()
        {
            var errorPos = _textParser.CurrentToken.Pos;
            _textParser.NextToken();
            var args = ParseArgumentList();
            if (args.Length != 2)
                throw ParseError(errorPos, Res.IsNullRequiresTwoArgs);

            return Expression.Coalesce(args[0], args[1]);
        }

        // ||, or operator
        private Expression ParseOrOperator()
        {
            var left = ParseAndOperator();
            while (_textParser.CurrentToken.Id == TokenId.DoubleBar || TokenIdentifierIs("or"))
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseAndOperator();
                CheckAndPromoteOperands(typeof(ILogicalSignatures), op.Text, ref left, ref right, op.Pos);
                left = Expression.OrElse(left, right);
            }
            return left;
        }

        // &&, and operator
        private Expression ParseAndOperator()
        {
            var left = ParseIn();
            while (_textParser.CurrentToken.Id == TokenId.DoubleAmphersand || TokenIdentifierIs("and"))
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseComparisonOperator();
                CheckAndPromoteOperands(typeof(ILogicalSignatures), op.Text, ref left, ref right, op.Pos);
                left = Expression.AndAlso(left, right);
            }
            return left;
        }

        // in operator for literals - example: "x in (1,2,3,4)"
        // in operator to mimic contains - example: "x in @0", compare to @0.Contains(x)
        // Adapted from ticket submitted by github user mlewis9548 
        private Expression ParseIn()
        {
            var left = ParseLogicalAndOrOperator();
            var accumulate = left;

            while (TokenIdentifierIs("in"))
            {
                var op = _textParser.CurrentToken;

                _textParser.NextToken();
                if (_textParser.CurrentToken.Id == TokenId.OpenParen) //literals (or other inline list)
                {
                    while (_textParser.CurrentToken.Id != TokenId.CloseParen)
                    {
                        _textParser.NextToken();

                        //we need to parse unary expressions because otherwise 'in' clause will fail in use cases like 'in (-1, -1)' or 'in (!true)'
                        var right = ParseUnary();

                        // if the identifier is an Enum, try to convert the right-side also to an Enum.
                        if (left.Type.GetTypeInfo().IsEnum && right is ConstantExpression)
                            right = ParseEnumToConstantExpression(op.Pos, left.Type, right as ConstantExpression);

                        // else, check for direct type match
                        else if (left.Type != right.Type)
                            CheckAndPromoteOperands(typeof(IEqualitySignatures), "==", ref left, ref right, op.Pos);

                        if (accumulate.Type != typeof(bool))
                            accumulate = GenerateEqual(left, right);
                        else
                            accumulate = Expression.OrElse(accumulate, GenerateEqual(left, right));

                        if (_textParser.CurrentToken.Id == TokenId.End)
                            throw ParseError(op.Pos, Res.CloseParenOrCommaExpected);
                    }
                }
                else if (_textParser.CurrentToken.Id == TokenId.Identifier) //a single argument
                {
                    var right = ParsePrimary();

                    if (!typeof(IEnumerable).IsAssignableFrom(right.Type))
                        throw ParseError(_textParser.CurrentToken.Pos, Res.IdentifierImplementingInterfaceExpected,
                            typeof(IEnumerable));

                    var args = new[] {left};

                    MethodBase containsSignature;
                    if (FindMethod(typeof(IEnumerableSignatures), "Contains", false, args, out containsSignature) != 1)
                        throw ParseError(op.Pos, Res.NoApplicableAggregate, "Contains");

                    var typeArgs = new[] {left.Type};
                    args = new[] {right, left};

                    accumulate = Expression.Call(typeof(Enumerable), containsSignature.Name, typeArgs, args);
                }
                else
                {
                    throw ParseError(op.Pos, Res.OpenParenOrIdentifierExpected);
                }

                _textParser.NextToken();
            }

            return accumulate;
        }

        // &, | bitwise operators
        private Expression ParseLogicalAndOrOperator()
        {
            var left = ParseComparisonOperator();
            while (_textParser.CurrentToken.Id == TokenId.Amphersand || _textParser.CurrentToken.Id == TokenId.Bar)
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseComparisonOperator();

                if (left.Type.GetTypeInfo().IsEnum)
                    left = Expression.Convert(left, Enum.GetUnderlyingType(left.Type));

                if (right.Type.GetTypeInfo().IsEnum)
                    right = Expression.Convert(right, Enum.GetUnderlyingType(right.Type));

                switch (op.Id)
                {
                    case TokenId.Amphersand:
                        // When at least one side of the operator is a string, consider it's a VB-style concatenation operator.
                        // Doesn't break any other function since logical AND with a string is invalid anyway.
                        if (left.Type == typeof(string) || right.Type == typeof(string))
                        {
                            left = GenerateStringConcat(left, right);
                        }
                        else
                        {
                            ConvertNumericTypeToBiggestCommonTypeForBinaryOperator(ref left, ref right);
                            left = Expression.And(left, right);
                        }
                        break;

                    case TokenId.Bar:
                        ConvertNumericTypeToBiggestCommonTypeForBinaryOperator(ref left, ref right);
                        left = Expression.Or(left, right);
                        break;
                }
            }
            return left;
        }

        // =, ==, !=, <>, >, >=, <, <= operators
        private Expression ParseComparisonOperator()
        {
            var left = ParseShiftOperator();
            Console.WriteLine(left.ToString());
            while (_textParser.CurrentToken.Id == TokenId.Equal || _textParser.CurrentToken.Id == TokenId.DoubleEqual ||
                   _textParser.CurrentToken.Id == TokenId.ExclamationEqual ||
                   _textParser.CurrentToken.Id == TokenId.LessGreater ||
                   _textParser.CurrentToken.Id == TokenId.GreaterThan ||
                   _textParser.CurrentToken.Id == TokenId.GreaterThanEqual ||
                   _textParser.CurrentToken.Id == TokenId.LessThan ||
                   _textParser.CurrentToken.Id == TokenId.LessThanEqual)
            {
                ConstantExpression constantExpr;
                TypeConverter typeConverter;
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseShiftOperator();
                var isEquality = op.Id == TokenId.Equal || op.Id == TokenId.DoubleEqual ||
                                 op.Id == TokenId.ExclamationEqual;

                if (isEquality && (!left.Type.GetTypeInfo().IsValueType && !right.Type.GetTypeInfo().IsValueType ||
                                   left.Type == typeof(Guid) && right.Type == typeof(Guid)))
                {
                    if (left.Type != right.Type)
                        if (left.Type.IsAssignableFrom(right.Type))
                            right = Expression.Convert(right, left.Type);
                        else if (right.Type.IsAssignableFrom(left.Type))
                            left = Expression.Convert(left, right.Type);
                        else
                            throw IncompatibleOperandsError(op.Text, left, right, op.Pos);
                }
                else if (IsEnumType(left.Type) || IsEnumType(right.Type))
                {
                    if (left.Type != right.Type)
                    {
                        Expression e;
                        if ((e = PromoteExpression(right, left.Type, true, false)) != null)
                            right = e;
                        else if ((e = PromoteExpression(left, right.Type, true, false)) != null)
                            left = e;
                        else if (IsEnumType(left.Type) && (constantExpr = right as ConstantExpression) != null)
                            right = ParseEnumToConstantExpression(op.Pos, left.Type, constantExpr);
                        else if (IsEnumType(right.Type) && (constantExpr = left as ConstantExpression) != null)
                            left = ParseEnumToConstantExpression(op.Pos, right.Type, constantExpr);
                        else
                            throw IncompatibleOperandsError(op.Text, left, right, op.Pos);
                    }
                }
                else if ((constantExpr = right as ConstantExpression) != null && constantExpr.Value is string &&
                         (typeConverter = TypeConverterFactory.GetConverter(left.Type)) != null)
                {
                    right = Expression.Constant(typeConverter.ConvertFromInvariantString((string) constantExpr.Value),
                        left.Type);
                }
                else if ((constantExpr = left as ConstantExpression) != null && constantExpr.Value is string &&
                         (typeConverter = TypeConverterFactory.GetConverter(right.Type)) != null)
                {
                    left = Expression.Constant(typeConverter.ConvertFromInvariantString((string) constantExpr.Value),
                        right.Type);
                }
                else
                {
                    var typesAreSameAndImplementCorrectInterface = false;
                    if (left.Type == right.Type)
                    {
                        var interfaces = left.Type.GetInterfaces().Where(x => x.GetTypeInfo().IsGenericType);
                        if (isEquality)
                            typesAreSameAndImplementCorrectInterface = interfaces.Any(x =>
                                x.GetGenericTypeDefinition() == typeof(IEquatable<>));
                        else
                            typesAreSameAndImplementCorrectInterface = interfaces.Any(x =>
                                x.GetGenericTypeDefinition() == typeof(IComparable<>));
                    }

                    if (!typesAreSameAndImplementCorrectInterface)
                        CheckAndPromoteOperands(
                            isEquality ? typeof(IEqualitySignatures) : typeof(IRelationalSignatures), op.Text, ref left,
                            ref right, op.Pos);
                }

                switch (op.Id)
                {
                    case TokenId.Equal:
                    case TokenId.DoubleEqual:
                        left = GenerateEqual(left, right);
                        break;
                    case TokenId.ExclamationEqual:
                    case TokenId.LessGreater:
                        left = GenerateNotEqual(left, right);
                        break;
                    case TokenId.GreaterThan:
                        left = GenerateGreaterThan(left, right);
                        break;
                    case TokenId.GreaterThanEqual:
                        left = GenerateGreaterThanEqual(left, right);
                        break;
                    case TokenId.LessThan:
                        left = GenerateLessThan(left, right);
                        break;
                    case TokenId.LessThanEqual:
                        left = GenerateLessThanEqual(left, right);
                        break;
                }
            }

            return left;
        }

        private ConstantExpression ParseEnumToConstantExpression(int pos, Type leftType,
            ConstantExpression constantExpr)
        {
            return Expression.Constant(ParseConstantExpressionToEnum(pos, leftType, constantExpr), leftType);
        }

        private object ParseConstantExpressionToEnum(int pos, Type leftType, ConstantExpression constantExpr)
        {
            try
            {
                if (constantExpr.Value is string)
                    return Enum.Parse(GetNonNullableType(leftType), (string) constantExpr.Value, true);

                return Enum.ToObject(leftType, constantExpr.Value);
            }
            catch
            {
                throw ParseError(pos, Res.ExpressionTypeMismatch, leftType);
            }
        }

        // <<, >> operators
        private Expression ParseShiftOperator()
        {
            var left = ParseAdditive();
            while (_textParser.CurrentToken.Id == TokenId.DoubleLessThan ||
                   _textParser.CurrentToken.Id == TokenId.DoubleGreaterThan)
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseAdditive();
                switch (op.Id)
                {
                    case TokenId.DoubleLessThan:
                        CheckAndPromoteOperands(typeof(IShiftSignatures), op.Text, ref left, ref right, op.Pos);
                        left = Expression.LeftShift(left, right);
                        break;
                    case TokenId.DoubleGreaterThan:
                        CheckAndPromoteOperands(typeof(IShiftSignatures), op.Text, ref left, ref right, op.Pos);
                        left = Expression.RightShift(left, right);
                        break;
                }
            }
            return left;
        }

        // +, - operators
        private Expression ParseAdditive()
        {
            var left = ParseMultiplicative();
            while (_textParser.CurrentToken.Id == TokenId.Plus || _textParser.CurrentToken.Id == TokenId.Minus)
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseMultiplicative();
                switch (op.Id)
                {
                    case TokenId.Plus:
                        if (left.Type == typeof(string) || right.Type == typeof(string))
                        {
                            left = GenerateStringConcat(left, right);
                        }
                        else
                        {
                            CheckAndPromoteOperands(typeof(IAddSignatures), op.Text, ref left, ref right, op.Pos);
                            left = GenerateAdd(left, right);
                        }
                        break;
                    case TokenId.Minus:
                        CheckAndPromoteOperands(typeof(ISubtractSignatures), op.Text, ref left, ref right, op.Pos);
                        left = GenerateSubtract(left, right);
                        break;
                }
            }
            return left;
        }

        // *, /, %, mod operators
        private Expression ParseMultiplicative()
        {
            var left = ParseUnary();
            while (_textParser.CurrentToken.Id == TokenId.Asterisk || _textParser.CurrentToken.Id == TokenId.Slash ||
                   _textParser.CurrentToken.Id == TokenId.Percent || TokenIdentifierIs("mod"))
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                var right = ParseUnary();
                CheckAndPromoteOperands(typeof(IArithmeticSignatures), op.Text, ref left, ref right, op.Pos);
                switch (op.Id)
                {
                    case TokenId.Asterisk:
                        left = Expression.Multiply(left, right);
                        break;
                    case TokenId.Slash:
                        left = Expression.Divide(left, right);
                        break;
                    case TokenId.Percent:
                    case TokenId.Identifier:
                        left = Expression.Modulo(left, right);
                        break;
                }
            }
            return left;
        }

        // -, !, not unary operators
        private Expression ParseUnary()
        {
            if (_textParser.CurrentToken.Id == TokenId.Minus || _textParser.CurrentToken.Id == TokenId.Exclamation ||
                TokenIdentifierIs("not"))
            {
                var op = _textParser.CurrentToken;
                _textParser.NextToken();
                if (op.Id == TokenId.Minus && (_textParser.CurrentToken.Id == TokenId.IntegerLiteral ||
                                               _textParser.CurrentToken.Id == TokenId.RealLiteral))
                {
                    _textParser.CurrentToken.Text = "-" + _textParser.CurrentToken.Text;
                    _textParser.CurrentToken.Pos = op.Pos;
                    return ParsePrimary();
                }

                var expr = ParseUnary();
                if (op.Id == TokenId.Minus)
                {
                    CheckAndPromoteOperand(typeof(INegationSignatures), op.Text, ref expr, op.Pos);
                    expr = Expression.Negate(expr);
                }
                else
                {
                    CheckAndPromoteOperand(typeof(INotSignatures), op.Text, ref expr, op.Pos);
                    expr = Expression.Not(expr);
                }

                return expr;
            }

            return ParsePrimary();
        }

        private Expression ParsePrimary()
        {
            var expr = ParsePrimaryStart();
            while (true)
                if (_textParser.CurrentToken.Id == TokenId.Dot)
                {
                    _textParser.NextToken();
                    expr = ParseMemberAccess(null, expr);
                }
                else if (_textParser.CurrentToken.Id == TokenId.NullPropagation)
                {
                    throw new NotSupportedException(
                        "An expression tree lambda may not contain a null propagating operator");
                }
                else if (_textParser.CurrentToken.Id == TokenId.OpenBracket)
                {
                    expr = ParseElementAccess(expr);
                }
                else
                {
                    break;
                }
            return expr;
        }

        private Expression ParsePrimaryStart()
        {
            switch (_textParser.CurrentToken.Id)
            {
                case TokenId.Identifier:
                    return ParseIdentifier();
                case TokenId.StringLiteral:
                    return ParseStringLiteral();
                case TokenId.IntegerLiteral:
                    return ParseIntegerLiteral();
                case TokenId.RealLiteral:
                    return ParseRealLiteral();
                case TokenId.OpenParen:
                    return ParseParenExpression();
                default:
                    throw ParseError(Res.ExpressionExpected);
            }
        }

        private Expression ParseStringLiteral()
        {
            _textParser.ValidateToken(TokenId.StringLiteral);
            var quote = _textParser.CurrentToken.Text[0];
            var s = _textParser.CurrentToken.Text.Substring(1, _textParser.CurrentToken.Text.Length - 2);

            if (quote == '\'')
            {
                if (s.Length != 1)
                    throw ParseError(Res.InvalidCharacterLiteral);
                _textParser.NextToken();
                return CreateLiteral(s[0], s);
            }
            _textParser.NextToken();
            return CreateLiteral(s, s);
        }

        private Expression ParseIntegerLiteral()
        {
            _textParser.ValidateToken(TokenId.IntegerLiteral);

            var text = _textParser.CurrentToken.Text;
            string qualifier = null;
            var last = text[text.Length - 1];
            var isHexadecimal =
                text.StartsWith(text[0] == '-' ? "-0x" : "0x", StringComparison.CurrentCultureIgnoreCase);
            var qualifierLetters = isHexadecimal
                ? new[] {'U', 'u', 'L', 'l'}
                : new[] {'U', 'u', 'L', 'l', 'F', 'f', 'D', 'd', 'M', 'm'};

            if (qualifierLetters.Contains(last))
            {
                int pos = text.Length - 1, count = 0;
                while (qualifierLetters.Contains(text[pos]))
                {
                    ++count;
                    --pos;
                }
                qualifier = text.Substring(text.Length - count, count);
                text = text.Substring(0, text.Length - count);
            }

            if (text[0] != '-')
            {
                if (isHexadecimal)
                    text = text.Substring(2);

                ulong value;
                if (!ulong.TryParse(text, isHexadecimal ? NumberStyles.HexNumber : NumberStyles.Integer,
                    CultureInfo.CurrentCulture, out value))
                    throw ParseError(Res.InvalidIntegerLiteral, text);

                _textParser.NextToken();
                if (!string.IsNullOrEmpty(qualifier))
                {
                    if (qualifier == "U" || qualifier == "u") return CreateLiteral((uint) value, text);
                    if (qualifier == "L" || qualifier == "l") return CreateLiteral((long) value, text);

                    // in case of UL, just return
                    return CreateLiteral(value, text);
                }

                // if (value <= (int)short.MaxValue) return CreateLiteral((short)value, text);
                if (value <= int.MaxValue) return CreateLiteral((int) value, text);
                if (value <= uint.MaxValue) return CreateLiteral((uint) value, text);
                if (value <= long.MaxValue) return CreateLiteral((long) value, text);

                return CreateLiteral(value, text);
            }
            else
            {
                if (isHexadecimal)
                    text = text.Substring(3);

                long value;
                if (!long.TryParse(text, isHexadecimal ? NumberStyles.HexNumber : NumberStyles.Integer,
                    CultureInfo.CurrentCulture, out value))
                    throw ParseError(Res.InvalidIntegerLiteral, text);

                if (isHexadecimal)
                    value = -value;

                _textParser.NextToken();
                if (!string.IsNullOrEmpty(qualifier))
                {
                    if (qualifier == "L" || qualifier == "l")
                        return CreateLiteral(value, text);

                    if (qualifier == "F" || qualifier == "f")
                        return TryParseAsFloat(text, qualifier[0]);

                    if (qualifier == "D" || qualifier == "d")
                        return TryParseAsDouble(text, qualifier[0]);

                    if (qualifier == "M" || qualifier == "m")
                        return TryParseAsDecimal(text, qualifier[0]);

                    throw ParseError(Res.MinusCannotBeAppliedToUnsignedInteger);
                }

                if (value <= int.MaxValue) return CreateLiteral((int) value, text);

                return CreateLiteral(value, text);
            }
        }

        private Expression ParseRealLiteral()
        {
            _textParser.ValidateToken(TokenId.RealLiteral);

            var text = _textParser.CurrentToken.Text;
            var qualifier = text[text.Length - 1];

            _textParser.NextToken();
            return TryParseAsFloat(text, qualifier);
        }

        private Expression TryParseAsFloat(string text, char qualifier)
        {
            if (qualifier == 'F' || qualifier == 'f')
            {
                float f;
                if (float.TryParse(text.Substring(0, text.Length - 1), NumberStyles.Float, CultureInfo.InvariantCulture,
                    out f))
                    return CreateLiteral(f, text);
            }

            // not possible to find float qualifier, so try to parse as double
            return TryParseAsDecimal(text, qualifier);
        }

        private Expression TryParseAsDecimal(string text, char qualifier)
        {
            if (qualifier == 'M' || qualifier == 'm')
            {
                decimal d;
                if (decimal.TryParse(text.Substring(0, text.Length - 1), NumberStyles.Number,
                    CultureInfo.InvariantCulture, out d))
                    return CreateLiteral(d, text);
            }

            // not possible to find float qualifier, so try to parse as double
            return TryParseAsDouble(text, qualifier);
        }

        private Expression TryParseAsDouble(string text, char qualifier)
        {
            double d;
            if (qualifier == 'D' || qualifier == 'd')
                if (double.TryParse(text.Substring(0, text.Length - 1), NumberStyles.Number,
                    CultureInfo.InvariantCulture, out d))
                    return CreateLiteral(d, text);

            if (double.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out d))
                return CreateLiteral(d, text);

            throw ParseError(Res.InvalidRealLiteral, text);
        }

        private Expression CreateLiteral(object value, string text)
        {
            var expr = Expression.Constant(value);
            _literals.Add(expr, text);
            return expr;
        }

        private Expression ParseParenExpression()
        {
            _textParser.ValidateToken(TokenId.OpenParen, Res.OpenParenExpected);
            _textParser.NextToken();
            var e = ParseConditionalOperator();
            _textParser.ValidateToken(TokenId.CloseParen, Res.CloseParenOrOperatorExpected);
            _textParser.NextToken();
            return e;
        }

        private Expression ParseIdentifier()
        {
            _textParser.ValidateToken(TokenId.Identifier);
            object value;

            if (_keywords.TryGetValue(_textParser.CurrentToken.Text, out value))
            {
                var typeValue = value as Type;
                if (typeValue != null) return ParseTypeAccess(typeValue);

                if (value == KEYWORD_IT) return ParseIt();
                if (value == KEYWORD_PARENT) return ParseParent();
                if (value == KEYWORD_ROOT) return ParseRoot();
                if (value == SYMBOL_IT) return ParseIt();
                if (value == SYMBOL_PARENT) return ParseParent();
                if (value == SYMBOL_ROOT) return ParseRoot();
                if (value == KEYWORD_IIF) return ParseIif();
                if (value == KEYWORD_NEW) return ParseNew();
                if (value == KEYWORD_ISNULL) return ParseIsNull();

                _textParser.NextToken();

                return (Expression) value;
            }

            if (_symbols.TryGetValue(_textParser.CurrentToken.Text, out value) ||
                _externals != null && _externals.TryGetValue(_textParser.CurrentToken.Text, out value) ||
                _internals.TryGetValue(_textParser.CurrentToken.Text, out value))
            {
                var expr = value as Expression;
                if (expr == null)
                {
                    expr = Expression.Constant(value);
                }
                else
                {
                    var lambda = expr as LambdaExpression;
                    if (lambda != null) return ParseLambdaInvocation(lambda);
                }

                _textParser.NextToken();

                return expr;
            }

            if (_it != null)
                return ParseMemberAccess(null, _it);

            throw ParseError(Res.UnknownIdentifier, _textParser.CurrentToken.Text);
        }

        private Expression ParseIt()
        {
            if (_it == null)
                throw ParseError(Res.NoItInScope);
            _textParser.NextToken();
            return _it;
        }

        private Expression ParseParent()
        {
            if (_parent == null)
                throw ParseError(Res.NoParentInScope);
            _textParser.NextToken();
            return _parent;
        }

        private Expression ParseRoot()
        {
            if (_root == null)
                throw ParseError(Res.NoRootInScope);
            _textParser.NextToken();
            return _root;
        }

        private Expression ParseIif()
        {
            var errorPos = _textParser.CurrentToken.Pos;
            _textParser.NextToken();
            var args = ParseArgumentList();
            if (args.Length != 3)
                throw ParseError(errorPos, Res.IifRequiresThreeArgs);

            return GenerateConditional(args[0], args[1], args[2], errorPos);
        }

        private Expression GenerateConditional(Expression test, Expression expr1, Expression expr2, int errorPos)
        {
            if (test.Type != typeof(bool))
                throw ParseError(errorPos, Res.FirstExprMustBeBool);
            if (expr1.Type != expr2.Type)
            {
                var expr1As2 = expr2 != NullLiteral ? PromoteExpression(expr1, expr2.Type, true, false) : null;
                var expr2As1 = expr1 != NullLiteral ? PromoteExpression(expr2, expr1.Type, true, false) : null;
                if (expr1As2 != null && expr2As1 == null)
                {
                    expr1 = expr1As2;
                }
                else if (expr2As1 != null && expr1As2 == null)
                {
                    expr2 = expr2As1;
                }
                else
                {
                    var type1 = expr1 != NullLiteral ? expr1.Type.Name : "null";
                    var type2 = expr2 != NullLiteral ? expr2.Type.Name : "null";
                    if (expr1As2 != null)
                        throw ParseError(errorPos, Res.BothTypesConvertToOther, type1, type2);

                    throw ParseError(errorPos, Res.NeitherTypeConvertsToOther, type1, type2);
                }
            }

            return Expression.Condition(test, expr1, expr2);
        }

        private Expression ParseNew()
        {
            _textParser.NextToken();
            if (_textParser.CurrentToken.Id != TokenId.OpenParen &&
                _textParser.CurrentToken.Id != TokenId.OpenCurlyParen &&
                _textParser.CurrentToken.Id != TokenId.OpenBracket &&
                _textParser.CurrentToken.Id != TokenId.Identifier)
                throw ParseError(Res.OpenParenOrIdentifierExpected);

            Type newType = null;
            if (_textParser.CurrentToken.Id == TokenId.Identifier)
            {
                var newTypeName = _textParser.CurrentToken.Text;
                newType = FindType(newTypeName);
                if (newType == null)
                    throw ParseError(_textParser.CurrentToken.Pos, Res.TypeNotFound, newTypeName);
                _textParser.NextToken();
                if (_textParser.CurrentToken.Id != TokenId.OpenParen &&
                    _textParser.CurrentToken.Id != TokenId.OpenBracket &&
                    _textParser.CurrentToken.Id != TokenId.OpenCurlyParen)
                    throw ParseError(Res.OpenParenExpected);
            }

            var arrayInitializer = false;
            if (_textParser.CurrentToken.Id == TokenId.OpenBracket)
            {
                _textParser.NextToken();
                _textParser.ValidateToken(TokenId.CloseBracket, Res.CloseBracketExpected);
                _textParser.NextToken();
                _textParser.ValidateToken(TokenId.OpenCurlyParen, Res.OpenCurlyParenExpected);
                arrayInitializer = true;
            }

            _textParser.NextToken();

            var properties = new List<DynamicProperty>();
            var expressions = new List<Expression>();

            while (_textParser.CurrentToken.Id != TokenId.CloseParen
                   && _textParser.CurrentToken.Id != TokenId.CloseCurlyParen)
            {
                var exprPos = _textParser.CurrentToken.Pos;
                var expr = ParseConditionalOperator();
                if (!arrayInitializer)
                {
                    string propName;
                    if (TokenIdentifierIs("as"))
                    {
                        _textParser.NextToken();
                        propName = GetIdentifier();
                        _textParser.NextToken();
                    }
                    else
                    {
                        if (!TryGetMemberName(expr, out propName)) throw ParseError(exprPos, Res.MissingAsClause);
                    }

                    properties.Add(new DynamicProperty(propName, expr.Type));
                }

                expressions.Add(expr);

                if (_textParser.CurrentToken.Id != TokenId.Comma)
                    break;

                _textParser.NextToken();
            }

            if (_textParser.CurrentToken.Id != TokenId.CloseParen &&
                _textParser.CurrentToken.Id != TokenId.CloseCurlyParen)
                throw ParseError(Res.CloseParenOrCommaExpected);
            _textParser.NextToken();

            if (arrayInitializer)
                return CreateArrayInitializerExpression(expressions, newType);

            return CreateNewExpression(properties, expressions, newType);
        }

        private Expression CreateArrayInitializerExpression(List<Expression> expressions, Type newType)
        {
            if (expressions.Count == 0)
                return Expression.NewArrayInit(newType ?? typeof(object));

            if (newType != null)
                return Expression.NewArrayInit(
                    newType,
                    expressions.Select(expression => PromoteExpression(expression, newType, true, true)));

            return Expression.NewArrayInit(
                expressions.All(expression => expression.Type == expressions[0].Type)
                    ? expressions[0].Type
                    : typeof(object),
                expressions);
        }

        private Expression CreateNewExpression(List<DynamicProperty> properties, List<Expression> expressions,
            Type newType)
        {
            // http://solutionizing.net/category/linq/
            var type = newType ?? _resultType;

            if (type == null)
            {
#if UAP10_0
                type = typeof(DynamicClass);
                Type typeForKeyValuePair = typeof(KeyValuePair<string, object>);
                ConstructorInfo constructorForKeyValuePair =
typeForKeyValuePair.GetTypeInfo().DeclaredConstructors.First();

                var arrayIndexParams = new List<Expression>();
                for (int i = 0; i < expressions.Count; i++)
                {
                    // Just convert the expression always to an object expression.
                    UnaryExpression boxingExpression = Expression.Convert(expressions[i], typeof(object));
                    NewExpression parameter =
Expression.New(constructorForKeyValuePair, new[] { (Expression)Expression.Constant(properties[i].Name), boxingExpression });

                    arrayIndexParams.Add(parameter);
                }

                // Create an expression tree that represents creating and initializing a one-dimensional array of type KeyValuePair<string, object>.
                NewArrayExpression newArrayExpression =
Expression.NewArrayInit(typeof(KeyValuePair<string, object>), arrayIndexParams);

                // Get the "public DynamicClass(KeyValuePair<string, object>[] propertylist)" constructor
                ConstructorInfo constructor = type.GetTypeInfo().DeclaredConstructors.First();
                return Expression.New(constructor, newArrayExpression);
#else
                type = DynamicClassFactory.CreateType(properties, _createParameterCtor);
#endif
            }

            var propertyTypes = type.GetProperties().Select(p => p.PropertyType).ToArray();
            var ctor = type.GetConstructor(propertyTypes);
            if (ctor != null)
                return Expression.New(ctor, expressions);

            var bindings = new MemberBinding[properties.Count];
            for (var i = 0; i < bindings.Length; i++)
                bindings[i] = Expression.Bind(type.GetProperty(properties[i].Name), expressions[i]);
            return Expression.MemberInit(Expression.New(type), bindings);
        }

        private Expression ParseLambdaInvocation(LambdaExpression lambda)
        {
            var errorPos = _textParser.CurrentToken.Pos;
            _textParser.NextToken();
            var args = ParseArgumentList();
            MethodBase method;
            if (FindMethod(lambda.Type, "Invoke", false, args, out method) != 1)
                throw ParseError(errorPos, Res.ArgsIncompatibleWithLambda);

            return Expression.Invoke(lambda, args);
        }

        private Expression ParseTypeAccess(Type type)
        {
            var errorPos = _textParser.CurrentToken.Pos;
            _textParser.NextToken();

            if (_textParser.CurrentToken.Id == TokenId.Question)
            {
                if (!type.GetTypeInfo().IsValueType || IsNullableType(type))
                    throw ParseError(errorPos, Res.TypeHasNoNullableForm, GetTypeName(type));

                type = typeof(Nullable<>).MakeGenericType(type);
                _textParser.NextToken();
            }

            // This is a shorthand for explicitely converting a string to something
            var shorthand = _textParser.CurrentToken.Id == TokenId.StringLiteral;
            if (_textParser.CurrentToken.Id == TokenId.OpenParen || shorthand)
            {
                var args = shorthand ? new[] {ParseStringLiteral()} : ParseArgumentList();

                // If only 1 argument, and if the type is a ValueType and argType is also a ValueType, just Convert
                if (args.Length == 1)
                {
                    var argType = args[0].Type;

                    if (type.GetTypeInfo().IsValueType && IsNullableType(type) && argType.GetTypeInfo().IsValueType)
                        return Expression.Convert(args[0], type);
                }

                MethodBase method;
                switch (FindBestMethod(type.GetConstructors(), args, out method))
                {
                    case 0:
                        if (args.Length == 1)
                            return GenerateConversion(args[0], type, errorPos);

                        throw ParseError(errorPos, Res.NoMatchingConstructor, GetTypeName(type));

                    case 1:
                        return Expression.New((ConstructorInfo) method, args);

                    default:
                        throw ParseError(errorPos, Res.AmbiguousConstructorInvocation, GetTypeName(type));
                }
            }

            _textParser.ValidateToken(TokenId.Dot, Res.DotOrOpenParenOrStringLiteralExpected);
            _textParser.NextToken();

            return ParseMemberAccess(type, null);
        }

        private static Expression GenerateConversion(Expression expr, Type type, int errorPos)
        {
            var exprType = expr.Type;
            if (exprType == type)
                return expr;

            if (exprType.GetTypeInfo().IsValueType && type.GetTypeInfo().IsValueType)
            {
                if ((IsNullableType(exprType) || IsNullableType(type)) &&
                    GetNonNullableType(exprType) == GetNonNullableType(type))
                    return Expression.Convert(expr, type);

                if ((IsNumericType(exprType) || IsEnumType(exprType)) && IsNumericType(type) || IsEnumType(type))
                    return Expression.ConvertChecked(expr, type);
            }

            if (exprType.IsAssignableFrom(type) || type.IsAssignableFrom(exprType) ||
                exprType.GetTypeInfo().IsInterface || type.GetTypeInfo().IsInterface)
                return Expression.Convert(expr, type);

            // Try to Parse the string rather that just generate the convert statement
            if (expr.NodeType == ExpressionType.Constant && exprType == typeof(string))
            {
                var text = (string) ((ConstantExpression) expr).Value;

                // DateTime is parsed as UTC time.
                DateTime dateTime;
                if (type == typeof(DateTime) && DateTime.TryParse(text, CultureInfo.InvariantCulture,
                        DateTimeStyles.None, out dateTime))
                    return Expression.Constant(dateTime, type);

                object[] arguments = {text, null};
                var method = type.GetMethod("TryParse", new[] {typeof(string), type.MakeByRefType()});
                //MethodInfo method = type.GetMethod("TryParse", BindingFlags.Public | BindingFlags.Static, null, new Type[] { typeof(string), type.MakeByRefType() }, null);
                if (method != null && (bool) method.Invoke(null, arguments))
                    return Expression.Constant(arguments[1], type);
            }

            throw ParseError(errorPos, Res.CannotConvertValue, GetTypeName(exprType), GetTypeName(type));
        }

        private Expression ParseMemberAccess(Type type, Expression instance)
        {
            if (instance != null)
                type = instance.Type;

            var errorPos = _textParser.CurrentToken.Pos;
            var id = GetIdentifier();
            _textParser.NextToken();

            if (_textParser.CurrentToken.Id == TokenId.OpenParen)
            {
                if (instance != null && type != typeof(string))
                {
                    var enumerableType = FindGenericType(typeof(IEnumerable<>), type);
                    if (enumerableType != null)
                    {
                        var elementType = enumerableType.GetTypeInfo().GetGenericTypeArguments()[0];
                        return ParseAggregate(instance, elementType, id, errorPos);
                    }
                }

                var args = ParseArgumentList();
                MethodBase mb;
                switch (FindMethod(type, id, instance == null, args, out mb))
                {
                    case 0:
                        throw ParseError(errorPos, Res.NoApplicableMethod, id, GetTypeName(type));

                    case 1:
                        var method = (MethodInfo) mb;
                        if (!IsPredefinedType(method.DeclaringType) &&
                            !(method.IsPublic && IsPredefinedType(method.ReturnType)))
                            throw ParseError(errorPos, Res.MethodsAreInaccessible, GetTypeName(method.DeclaringType));

                        if (method.ReturnType == typeof(void))
                            throw ParseError(errorPos, Res.MethodIsVoid, id, GetTypeName(method.DeclaringType));

                        return Expression.Call(instance, method, args);

                    default:
                        throw ParseError(errorPos, Res.AmbiguousMethodInvocation, id, GetTypeName(type));
                }
            }

            if (type.GetTypeInfo().IsEnum)
            {
                var @enum = Enum.Parse(type, id, true);

                return Expression.Constant(@enum);
            }

#if NETFX_CORE
            if (type == typeof(DynamicObjectClass))
            {
                return Expression.MakeIndex(instance, typeof(DynamicObjectClass).GetProperty("Item"), new[] { Expression.Constant(id) });
            }
#endif
            var member = FindPropertyOrField(type, id, instance == null);
            if (member == null)
            {
                if (_textParser.CurrentToken.Id == TokenId.Lambda && _it.Type == type)
                {
                    // This might be an internal variable for use within a lambda expression, so store it as such
                    _internals.Add(id, _it);
                    _textParser.NextToken();

                    return ParseConditionalOperator();
                }

                throw ParseError(errorPos, Res.UnknownPropertyOrField, id, GetTypeName(type));
            }

            var property = member as PropertyInfo;
            if (property != null)
                return Expression.Property(instance, property);

            return Expression.Field(instance, (FieldInfo) member);
        }

        private static Type FindGenericType(Type generic, Type type)
        {
            while (type != null && type != typeof(object))
            {
                if (type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == generic)
                    return type;

                if (generic.GetTypeInfo().IsInterface)
                    foreach (var intfType in type.GetInterfaces())
                    {
                        var found = FindGenericType(generic, intfType);
                        if (found != null) return found;
                    }

                type = type.GetTypeInfo().BaseType;
            }

            return null;
        }

        private Type FindType(string name)
        {
            object type;
            _keywords.TryGetValue(name, out type);
            var result = type as Type;
            if (result != null)
                return result;
            if (_it != null && _it.Type.Name == name)
                return _it.Type;
            if (_parent != null && _parent.Type.Name == name)
                return _parent.Type;
            if (_root != null && _root.Type.Name == name)
                return _root.Type;
            if (_it != null && _it.Type.Namespace + "." + _it.Type.Name == name)
                return _it.Type;
            if (_parent != null && _parent.Type.Namespace + "." + _parent.Type.Name == name)
                return _parent.Type;
            if (_root != null && _root.Type.Namespace + "." + _root.Type.Name == name)
                return _root.Type;
            return null;
        }

        private Expression ParseAggregate(Expression instance, Type elementType, string methodName, int errorPos)
        {
            var oldParent = _parent;

            var outerIt = _it;
            var innerIt = Expression.Parameter(elementType, "");

            _parent = _it;

            if (methodName == "Contains" || methodName == "Skip" || methodName == "Take")
                _it = outerIt;
            else
                _it = innerIt;

            var args = ParseArgumentList();

            _it = outerIt;
            _parent = oldParent;

            MethodBase signature;
            if (FindMethod(typeof(IEnumerableSignatures), methodName, false, args, out signature) != 1)
                throw ParseError(errorPos, Res.NoApplicableAggregate, methodName);

            Type[] typeArgs;
            if (new[] {"Min", "Max", "Select", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending", "GroupBy"}
                .Contains(signature.Name))
            {
                if (args.Length == 2)
                    typeArgs = new[] {elementType, args[0].Type, args[1].Type};
                else
                    typeArgs = new[] {elementType, args[0].Type};
            }
            else if (signature.Name == "SelectMany")
            {
                var type = Expression.Lambda(args[0], innerIt).Body.Type;
                var interfaces = type.GetInterfaces().Union(new[] {type});
                var interfaceType = interfaces.Single(i => i.Name == typeof(IEnumerable<>).Name);
                var resultType = interfaceType.GetTypeInfo().GetGenericTypeArguments()[0];
                typeArgs = new[] {elementType, resultType};
            }
            else
            {
                typeArgs = new[] {elementType};
            }

            if (args.Length == 0)
            {
                args = new[] {instance};
            }
            else
            {
                if (new[] {"Contains", "Take", "Skip", "DefaultIfEmpty"}.Contains(signature.Name))
                {
                    args = new[] {instance, args[0]};
                }
                else
                {
                    if (args.Length == 2)
                        args = new[]
                            {instance, Expression.Lambda(args[0], innerIt), Expression.Lambda(args[1], innerIt)};
                    else
                        args = new[] {instance, Expression.Lambda(args[0], innerIt)};
                }
            }

            return Expression.Call(typeof(Enumerable), signature.Name, typeArgs, args);
        }

        private Expression[] ParseArgumentList()
        {
            _textParser.ValidateToken(TokenId.OpenParen, Res.OpenParenExpected);
            _textParser.NextToken();
            var args = _textParser.CurrentToken.Id != TokenId.CloseParen ? ParseArguments() : new Expression[0];
            _textParser.ValidateToken(TokenId.CloseParen, Res.CloseParenOrCommaExpected);
            _textParser.NextToken();
            return args;
        }

        private Expression[] ParseArguments()
        {
            var argList = new List<Expression>();
            while (true)
            {
                argList.Add(ParseConditionalOperator());

                if (_textParser.CurrentToken.Id != TokenId.Comma)
                    break;

                _textParser.NextToken();
            }

            return argList.ToArray();
        }

        private Expression ParseElementAccess(Expression expr)
        {
            var errorPos = _textParser.CurrentToken.Pos;
            _textParser.ValidateToken(TokenId.OpenBracket, Res.OpenParenExpected);
            _textParser.NextToken();
            var args = ParseArguments();
            _textParser.ValidateToken(TokenId.CloseBracket, Res.CloseBracketOrCommaExpected);
            _textParser.NextToken();
            if (expr.Type.IsArray)
            {
                if (expr.Type.GetArrayRank() != 1 || args.Length != 1)
                    throw ParseError(errorPos, Res.CannotIndexMultiDimArray);
                var index = PromoteExpression(args[0], typeof(int), true, false);
                if (index == null)
                    throw ParseError(errorPos, Res.InvalidIndex);
                return Expression.ArrayIndex(expr, index);
            }
            MethodBase mb;
            switch (FindIndexer(expr.Type, args, out mb))
            {
                case 0:
                    throw ParseError(errorPos, Res.NoApplicableIndexer,
                        GetTypeName(expr.Type));
                case 1:
                    return Expression.Call(expr, (MethodInfo) mb, args);
                default:
                    throw ParseError(errorPos, Res.AmbiguousIndexerInvocation,
                        GetTypeName(expr.Type));
            }
        }

        private static bool IsPredefinedType(Type type)
        {
            if (_predefinedTypes.ContainsKey(type)) return true;

            if (GlobalConfig.CustomTypeProvider != null &&
                GlobalConfig.CustomTypeProvider.GetCustomTypes().Contains(type)) return true;

            return false;
        }

        public static bool IsNullableType(Type type)
        {
            return type.GetTypeInfo().IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>);
        }

        public static Type ToNullableType(Type type)
        {
            if (!type.GetTypeInfo().IsValueType || IsNullableType(type))
                throw ParseError(-1, Res.TypeHasNoNullableForm, GetTypeName(type));

            return typeof(Nullable<>).MakeGenericType(type);
        }

        public static Type GetNonNullableType(Type type)
        {
            return IsNullableType(type) ? type.GetTypeInfo().GetGenericTypeArguments()[0] : type;
        }

        public static Type GetUnderlyingType(Type type)
        {
            var genericTypeArguments = type.GetGenericArguments();
            if (genericTypeArguments.Any())
            {
                var outerType = GetUnderlyingType(genericTypeArguments.LastOrDefault());
                return Nullable.GetUnderlyingType(type) == outerType ? type : outerType;
            }

            return type;
        }

        private static string GetTypeName(Type type)
        {
            var baseType = GetNonNullableType(type);
            var s = baseType.Name;
            if (type != baseType) s += '?';
            return s;
        }

        private static bool TryGetMemberName(Expression expression, out string memberName)
        {
            var memberExpression = expression as MemberExpression;
            if (memberExpression == null && expression.NodeType == ExpressionType.Coalesce)
                memberExpression = (expression as BinaryExpression).Left as MemberExpression;

            if (memberExpression != null)
            {
                memberName = memberExpression.Member.Name;
                return true;
            }

#if NETFX_CORE
            var indexExpression = expression as IndexExpression;
            if (indexExpression != null && indexExpression.Indexer.DeclaringType == typeof(DynamicObjectClass))
            {
                memberName = ((ConstantExpression)indexExpression.Arguments.First()).Value as string;
                return true;
            }
#endif
            //#if !NET35
            //            var dynamicExpression = expression as Expressions.DynamicExpression;
            //            if (dynamicExpression != null)
            //            {
            //                memberName = ((GetMemberBinder)dynamicExpression.Binder).Name;
            //                return true;
            //            }
            //#endif

            memberName = null;
            return false;
        }

        private static bool IsNumericType(Type type)
        {
            return GetNumericTypeKind(type) != 0;
        }

        private static bool IsSignedIntegralType(Type type)
        {
            return GetNumericTypeKind(type) == 2;
        }

        private static bool IsUnsignedIntegralType(Type type)
        {
            return GetNumericTypeKind(type) == 3;
        }

        private static int GetNumericTypeKind(Type type)
        {
            type = GetNonNullableType(type);
            if (type.GetTypeInfo().IsEnum) return 0;

            if (type == typeof(char) || type == typeof(float) || type == typeof(double) || type == typeof(decimal))
                return 1;
            if (type == typeof(sbyte) || type == typeof(short) || type == typeof(int) || type == typeof(long))
                return 2;
            if (type == typeof(byte) || type == typeof(ushort) || type == typeof(uint) || type == typeof(ulong))
                return 3;
            return 0;
        }

        private static bool IsEnumType(Type type)
        {
            return GetNonNullableType(type).GetTypeInfo().IsEnum;
        }

        private void CheckAndPromoteOperand(Type signatures, string opName, ref Expression expr, int errorPos)
        {
            Expression[] args = {expr};

            MethodBase method;
            if (FindMethod(signatures, "F", false, args, out method) != 1)
                throw IncompatibleOperandError(opName, expr, errorPos);

            expr = args[0];
        }

        private void CheckAndPromoteOperands(Type signatures, string opName, ref Expression left, ref Expression right,
            int errorPos)
        {
            Expression[] args = {left, right};

            MethodBase method;
            if (FindMethod(signatures, "F", false, args, out method) != 1)
                throw IncompatibleOperandsError(opName, left, right, errorPos);

            left = args[0];
            right = args[1];
        }

        private static Exception IncompatibleOperandError(string opName, Expression expr, int errorPos)
        {
            return ParseError(errorPos, Res.IncompatibleOperand, opName, GetTypeName(expr.Type));
        }

        private static Exception IncompatibleOperandsError(string opName, Expression left, Expression right,
            int errorPos)
        {
            return ParseError(errorPos, Res.IncompatibleOperands, opName, GetTypeName(left.Type),
                GetTypeName(right.Type));
        }

        private static MemberInfo FindPropertyOrField(Type type, string memberName, bool staticAccess)
        {
            foreach (var t in SelfAndBaseTypes(type))
            {
                // Try to find a property with the specified memberName
                MemberInfo member = t.GetTypeInfo().DeclaredProperties
                    .FirstOrDefault(x => x.Name.ToLowerInvariant() == memberName.ToLowerInvariant());
                if (member != null)
                    return member;

                // If no property is found, try to get a field with the specified memberName
                member = t.GetTypeInfo().DeclaredFields.FirstOrDefault(x =>
                    (x.IsStatic || !staticAccess) && x.Name.ToLowerInvariant() == memberName.ToLowerInvariant());
                if (member != null)
                    return member;

                // No property or field is found, try the base type.
            }
            return null;
        }

        private int FindMethod(Type type, string methodName, bool staticAccess, Expression[] args,
            out MethodBase method)
        {
            foreach (var t in SelfAndBaseTypes(type))
            {
                var methods = t.GetTypeInfo().DeclaredMethods.Where(x =>
                        (x.IsStatic || !staticAccess) && x.Name.ToLowerInvariant() == methodName.ToLowerInvariant())
                    .ToArray();
                var count = FindBestMethod(methods, args, out method);
                if (count != 0) return count;
            }

            method = null;
            return 0;
        }

        private int FindIndexer(Type type, Expression[] args, out MethodBase method)
        {
            foreach (var t in SelfAndBaseTypes(type))
            {
                //#if !(NETFX_CORE || WINDOWS_APP || DOTNET5_1 || UAP10_0 || NETSTANDARD)
                var members = t.GetDefaultMembers();
                //#else
                //                MemberInfo[] members = new MemberInfo[0];
                //#endif
                if (members.Length != 0)
                {
                    var methods = members
                        .OfType<PropertyInfo>().Select(p => (MethodBase) p.GetMethod);

                    var count = FindBestMethod(methods, args, out method);
                    if (count != 0) return count;
                }
            }
            method = null;
            return 0;
        }

        private static IEnumerable<Type> SelfAndBaseTypes(Type type)
        {
            if (type.GetTypeInfo().IsInterface)
            {
                var types = new List<Type>();
                AddInterface(types, type);
                return types;
            }
            return SelfAndBaseClasses(type);
        }

        private static IEnumerable<Type> SelfAndBaseClasses(Type type)
        {
            while (type != null)
            {
                yield return type;
                type = type.GetTypeInfo().BaseType;
            }
        }

        private static void AddInterface(List<Type> types, Type type)
        {
            if (!types.Contains(type))
            {
                types.Add(type);
                foreach (var t in type.GetInterfaces()) AddInterface(types, t);
            }
        }

        private int FindBestMethod(IEnumerable<MethodBase> methods, Expression[] args, out MethodBase method)
        {
            var applicable = methods.Select(m => new MethodData {MethodBase = m, Parameters = m.GetParameters()})
                .Where(m => IsApplicable(m, args)).ToArray();

            if (applicable.Length > 1)
                applicable = applicable.Where(m => applicable.All(n => m == n || IsBetterThan(args, m, n))).ToArray();

            if (args.Length == 2 && applicable.Length > 1 &&
                (args[0].Type == typeof(Guid?) || args[1].Type == typeof(Guid?)))
                applicable = applicable.Take(1).ToArray();

            if (applicable.Length == 1)
            {
                var md = applicable[0];
                for (var i = 0; i < args.Length; i++) args[i] = md.Args[i];
                method = md.MethodBase;
            }
            else
            {
                method = null;
            }

            return applicable.Length;
        }

        private bool IsApplicable(MethodData method, Expression[] args)
        {
            if (method.Parameters.Length != args.Length) return false;
            var promotedArgs = new Expression[args.Length];
            for (var i = 0; i < args.Length; i++)
            {
                var pi = method.Parameters[i];
                if (pi.IsOut) return false;
                var promoted = PromoteExpression(args[i], pi.ParameterType, false,
                    method.MethodBase.DeclaringType != typeof(IEnumerableSignatures));
                if (promoted == null) return false;
                promotedArgs[i] = promoted;
            }
            method.Args = promotedArgs;
            return true;
        }

        private Expression PromoteExpression(Expression expr, Type type, bool exact, bool convertExpr)
        {
            if (expr.Type == type)
                return expr;

            var ce = expr as ConstantExpression;

            if (ce != null)
                if (ce == NullLiteral || ce.Value == null)
                {
                    if (!type.GetTypeInfo().IsValueType || IsNullableType(type))
                        return Expression.Constant(null, type);
                }
                else
                {
                    string text;
                    if (_literals.TryGetValue(ce, out text))
                    {
                        var target = GetNonNullableType(type);
                        object value = null;

                        if (ce.Type == typeof(int) || ce.Type == typeof(uint) || ce.Type == typeof(long) ||
                            ce.Type == typeof(ulong))
                        {
                            value = ParseNumber(text, target);

                            // Make sure an enum value stays an enum value
                            if (target.GetTypeInfo().IsEnum)
                                value = Enum.ToObject(target, value);
                        }
                        else if (ce.Type == typeof(double))
                        {
                            if (target == typeof(decimal)) value = ParseNumber(text, target);
                        }
                        else if (ce.Type == typeof(string))
                        {
                            value = ParseEnum(text, target);
                        }
                        if (value != null)
                            return Expression.Constant(value, type);
                    }
                }

            if (IsCompatibleWith(expr.Type, type))
            {
                if (type.GetTypeInfo().IsValueType || exact || expr.Type.GetTypeInfo().IsValueType && convertExpr)
                    return Expression.Convert(expr, type);

                return expr;
            }

            return null;
        }

        private static object ParseNumber(string text, Type type)
        {
            var tp = GetNonNullableType(type);
            if (tp == typeof(sbyte))
            {
                sbyte sb;
                if (sbyte.TryParse(text, out sb)) return sb;
            }
            else if (tp == typeof(byte))
            {
                byte b;
                if (byte.TryParse(text, out b)) return b;
            }
            else if (tp == typeof(short))
            {
                short s;
                if (short.TryParse(text, out s)) return s;
            }
            else if (tp == typeof(ushort))
            {
                ushort us;
                if (ushort.TryParse(text, out us)) return us;
            }
            else if (tp == typeof(int))
            {
                int i;
                if (int.TryParse(text, out i)) return i;
            }
            else if (tp == typeof(uint))
            {
                uint ui;
                if (uint.TryParse(text, out ui)) return ui;
            }
            else if (tp == typeof(long))
            {
                long l;
                if (long.TryParse(text, out l)) return l;
            }
            else if (tp == typeof(ulong))
            {
                ulong ul;
                if (ulong.TryParse(text, out ul)) return ul;
            }
            else if (tp == typeof(float))
            {
                float f;
                if (float.TryParse(text, out f)) return f;
            }
            else if (tp == typeof(double))
            {
                double d;
                if (double.TryParse(text, out d)) return d;
            }
            else if (tp == typeof(decimal))
            {
                decimal e;
                if (decimal.TryParse(text, out e)) return e;
            }
            return null;
        }

        private static object ParseEnum(string value, Type type)
        {
            if (type.GetTypeInfo().IsEnum)
                return Enum.Parse(type, value, true);
            return null;
        }

        private static bool IsCompatibleWith(Type source, Type target)
        {
            if (source == target) return true;
            if (!target.GetTypeInfo().IsValueType) return target.IsAssignableFrom(source);
            var st = GetNonNullableType(source);
            var tt = GetNonNullableType(target);
            if (st != source && tt == target) return false;
            var sc = st.GetTypeInfo().IsEnum ? typeof(object) : st;
            var tc = tt.GetTypeInfo().IsEnum ? typeof(object) : tt;

            if (sc == typeof(sbyte))
            {
                if (tc == typeof(sbyte) || tc == typeof(short) || tc == typeof(int) || tc == typeof(long) ||
                    tc == typeof(float) || tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(byte))
            {
                if (tc == typeof(byte) || tc == typeof(short) || tc == typeof(ushort) || tc == typeof(int) ||
                    tc == typeof(uint) || tc == typeof(long) || tc == typeof(ulong) || tc == typeof(float) ||
                    tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(short))
            {
                if (tc == typeof(short) || tc == typeof(int) || tc == typeof(long) || tc == typeof(float) ||
                    tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(ushort))
            {
                if (tc == typeof(ushort) || tc == typeof(int) || tc == typeof(uint) || tc == typeof(long) ||
                    tc == typeof(ulong) || tc == typeof(float) || tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(int))
            {
                if (tc == typeof(int) || tc == typeof(long) || tc == typeof(float) || tc == typeof(double) ||
                    tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(uint))
            {
                if (tc == typeof(uint) || tc == typeof(long) || tc == typeof(ulong) || tc == typeof(float) ||
                    tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(long))
            {
                if (tc == typeof(long) || tc == typeof(float) || tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(ulong))
            {
                if (tc == typeof(ulong) || tc == typeof(float) || tc == typeof(double) || tc == typeof(decimal))
                    return true;
            }
            else if (sc == typeof(float))
            {
                if (tc == typeof(float) || tc == typeof(double))
                    return true;
            }

            if (st == tt)
                return true;
            return false;
        }

        private static bool IsBetterThan(Expression[] args, MethodData first, MethodData second)
        {
            var better = false;
            for (var i = 0; i < args.Length; i++)
            {
                var result = CompareConversions(args[i].Type, first.Parameters[i].ParameterType,
                    second.Parameters[i].ParameterType);

                // If second is better, return false
                if (result == CompareConversionType.Second)
                    return false;

                // If first is better, return true
                if (result == CompareConversionType.First)
                    return true;

                // If both are same, just set better to true and continue
                if (result == CompareConversionType.Both)
                    better = true;
            }

            return better;
        }

        // Return "First" if s -> t1 is a better conversion than s -> t2
        // Return "Second" if s -> t2 is a better conversion than s -> t1
        // Return "Both" if neither conversion is better
        private static CompareConversionType CompareConversions(Type source, Type first, Type second)
        {
            if (first == second) return CompareConversionType.Both;
            if (source == first) return CompareConversionType.First;
            if (source == second) return CompareConversionType.Second;

            var firstIsCompatibleWithSecond = IsCompatibleWith(first, second);
            var secondIsCompatibleWithFirst = IsCompatibleWith(second, first);

            if (firstIsCompatibleWithSecond && !secondIsCompatibleWithFirst) return CompareConversionType.First;
            if (secondIsCompatibleWithFirst && !firstIsCompatibleWithSecond) return CompareConversionType.Second;

            if (IsSignedIntegralType(first) && IsUnsignedIntegralType(second)) return CompareConversionType.First;
            if (IsSignedIntegralType(second) && IsUnsignedIntegralType(first)) return CompareConversionType.Second;

            return CompareConversionType.Both;
        }

        private static Expression GenerateEqual(Expression left, Expression right)
        {
            OptimizeForEqualityIfPossible(ref left, ref right);
            return Expression.Equal(left, right);
        }

        private static Expression GenerateNotEqual(Expression left, Expression right)
        {
            OptimizeForEqualityIfPossible(ref left, ref right);
            return Expression.NotEqual(left, right);
        }

        private static Expression GenerateGreaterThan(Expression left, Expression right)
        {
            if (left.Type == typeof(string))
                return Expression.GreaterThan(GenerateStaticMethodCall("Compare", left, right), Expression.Constant(0));

            if (left.Type.GetTypeInfo().IsEnum || right.Type.GetTypeInfo().IsEnum)
            {
                var leftPart = left.Type.GetTypeInfo().IsEnum
                    ? Expression.Convert(left, Enum.GetUnderlyingType(left.Type))
                    : left;
                var rightPart = right.Type.GetTypeInfo().IsEnum
                    ? Expression.Convert(right, Enum.GetUnderlyingType(right.Type))
                    : right;
                return Expression.GreaterThan(leftPart, rightPart);
            }

            return Expression.GreaterThan(left, right);
        }

        private static Expression GenerateGreaterThanEqual(Expression left, Expression right)
        {
            if (left.Type == typeof(string))
                return Expression.GreaterThanOrEqual(GenerateStaticMethodCall("Compare", left, right),
                    Expression.Constant(0));

            if (left.Type.GetTypeInfo().IsEnum || right.Type.GetTypeInfo().IsEnum)
                return Expression.GreaterThanOrEqual(
                    left.Type.GetTypeInfo().IsEnum ? Expression.Convert(left, Enum.GetUnderlyingType(left.Type)) : left,
                    right.Type.GetTypeInfo().IsEnum
                        ? Expression.Convert(right, Enum.GetUnderlyingType(right.Type))
                        : right);

            return Expression.GreaterThanOrEqual(left, right);
        }

        private static Expression GenerateLessThan(Expression left, Expression right)
        {
            if (left.Type == typeof(string))
                return Expression.LessThan(GenerateStaticMethodCall("Compare", left, right), Expression.Constant(0));

            if (left.Type.GetTypeInfo().IsEnum || right.Type.GetTypeInfo().IsEnum)
                return Expression.LessThan(
                    left.Type.GetTypeInfo().IsEnum ? Expression.Convert(left, Enum.GetUnderlyingType(left.Type)) : left,
                    right.Type.GetTypeInfo().IsEnum
                        ? Expression.Convert(right, Enum.GetUnderlyingType(right.Type))
                        : right);

            return Expression.LessThan(left, right);
        }

        private static Expression GenerateLessThanEqual(Expression left, Expression right)
        {
            if (left.Type == typeof(string))
                return Expression.LessThanOrEqual(GenerateStaticMethodCall("Compare", left, right),
                    Expression.Constant(0));

            if (left.Type.GetTypeInfo().IsEnum || right.Type.GetTypeInfo().IsEnum)
                return Expression.LessThanOrEqual(
                    left.Type.GetTypeInfo().IsEnum ? Expression.Convert(left, Enum.GetUnderlyingType(left.Type)) : left,
                    right.Type.GetTypeInfo().IsEnum
                        ? Expression.Convert(right, Enum.GetUnderlyingType(right.Type))
                        : right);

            return Expression.LessThanOrEqual(left, right);
        }

        private static Expression GenerateAdd(Expression left, Expression right)
        {
            if (left.Type == typeof(string) && right.Type == typeof(string))
                return GenerateStaticMethodCall("Concat", left, right);
            return Expression.Add(left, right);
        }

        private static Expression GenerateSubtract(Expression left, Expression right)
        {
            return Expression.Subtract(left, right);
        }

        private static Expression GenerateStringConcat(Expression left, Expression right)
        {
            // Allow concat String with something else
            return Expression.Call(null, typeof(string).GetMethod("Concat", new[] {left.Type, right.Type}),
                new[] {left, right});
        }

        private static MethodInfo GetStaticMethod(string methodName, Expression left, Expression right)
        {
            return left.Type.GetMethod(methodName, new[] {left.Type, right.Type});
        }

        private static Expression GenerateStaticMethodCall(string methodName, Expression left, Expression right)
        {
            return Expression.Call(null, GetStaticMethod(methodName, left, right), new[] {left, right});
        }

        private static void OptimizeForEqualityIfPossible(ref Expression left, ref Expression right)
        {
            // The goal here is to provide the way to convert some types from the string form in a way that is compatible with Linq to Entities.
            //
            // The Expression.Call(typeof(Guid).GetMethod("Parse"), right); does the job only for Linq to Object but Linq to Entities.
            //
            var leftType = left.Type;
            var rightType = right.Type;

            if (rightType == typeof(string) && right.NodeType == ExpressionType.Constant)
                right = OptimizeStringForEqualityIfPossible((string) ((ConstantExpression) right).Value, leftType) ??
                        right;

            if (leftType == typeof(string) && left.NodeType == ExpressionType.Constant)
                left = OptimizeStringForEqualityIfPossible((string) ((ConstantExpression) left).Value, rightType) ??
                       left;
        }

        private static Expression OptimizeStringForEqualityIfPossible(string text, Type type)
        {
            DateTime dateTime;

            if (type == typeof(DateTime) &&
                DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.None, out dateTime))
                return Expression.Constant(dateTime, typeof(DateTime));
            Guid guid;
            if (type == typeof(Guid) && Guid.TryParse(text, out guid))
                return Expression.Constant(guid, typeof(Guid));
            return null;
        }

        private bool TokenIdentifierIs(string id)
        {
            return _textParser.CurrentToken.Id == TokenId.Identifier && string.Equals(id, _textParser.CurrentToken.Text,
                       StringComparison.OrdinalIgnoreCase);
        }

        private string GetIdentifier()
        {
            _textParser.ValidateToken(TokenId.Identifier, Res.IdentifierExpected);
            var id = _textParser.CurrentToken.Text;
            if (id.Length > 1 && id[0] == '@') id = id.Substring(1);
            return id;
        }

        private Exception ParseError(string format, params object[] args)
        {
            return ParseError(_textParser?.CurrentToken.Pos ?? 0, format, args);
        }

        private static Exception ParseError(int pos, string format, params object[] args)
        {
            return new ParseException(string.Format(CultureInfo.CurrentCulture, format, args), pos);
        }

        private static Dictionary<string, object> CreateKeywords()
        {
            var d = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
            {
                {"true", TrueLiteral},
                {"false", FalseLiteral},
                {"null", NullLiteral}
            };

            if (GlobalConfig.AreContextKeywordsEnabled)
            {
                d.Add(KEYWORD_IT, KEYWORD_IT);
                d.Add(KEYWORD_PARENT, KEYWORD_PARENT);
                d.Add(KEYWORD_ROOT, KEYWORD_ROOT);
            }

            d.Add(SYMBOL_IT, SYMBOL_IT);
            d.Add(SYMBOL_PARENT, SYMBOL_PARENT);
            d.Add(SYMBOL_ROOT, SYMBOL_ROOT);
            d.Add(KEYWORD_IIF, KEYWORD_IIF);
            d.Add(KEYWORD_NEW, KEYWORD_NEW);
            d.Add(KEYWORD_ISNULL, KEYWORD_ISNULL);

            foreach (var type in _predefinedTypes.OrderBy(kvp => kvp.Value).Select(kvp => kvp.Key))
            {
                d[type.FullName] = type;
                d[type.Name] = type;
            }

            foreach (var pair in _predefinedTypesShorthands)
                d.Add(pair.Key, pair.Value);

            if (GlobalConfig.CustomTypeProvider != null)
                foreach (var type in GlobalConfig.CustomTypeProvider.GetCustomTypes())
                {
                    d[type.FullName] = type;
                    d[type.Name] = type;
                }

            return d;
        }

        internal static void ResetDynamicLinqTypes()
        {
            _keywords = null;
        }

        private static void ConvertNumericTypeToBiggestCommonTypeForBinaryOperator(ref Expression left,
            ref Expression right)
        {
            if (left.Type == right.Type)
                return;

            if (left.Type == typeof(ulong) || right.Type == typeof(ulong))
            {
                right = right.Type != typeof(ulong) ? Expression.Convert(right, typeof(ulong)) : right;
                left = left.Type != typeof(ulong) ? Expression.Convert(left, typeof(ulong)) : left;
            }
            else if (left.Type == typeof(long) || right.Type == typeof(long))
            {
                right = right.Type != typeof(long) ? Expression.Convert(right, typeof(long)) : right;
                left = left.Type != typeof(long) ? Expression.Convert(left, typeof(long)) : left;
            }
            else if (left.Type == typeof(uint) || right.Type == typeof(uint))
            {
                right = right.Type != typeof(uint) ? Expression.Convert(right, typeof(uint)) : right;
                left = left.Type != typeof(uint) ? Expression.Convert(left, typeof(uint)) : left;
            }
            else if (left.Type == typeof(int) || right.Type == typeof(int))
            {
                right = right.Type != typeof(int) ? Expression.Convert(right, typeof(int)) : right;
                left = left.Type != typeof(int) ? Expression.Convert(left, typeof(int)) : left;
            }
            else if (left.Type == typeof(ushort) || right.Type == typeof(ushort))
            {
                right = right.Type != typeof(ushort) ? Expression.Convert(right, typeof(ushort)) : right;
                left = left.Type != typeof(ushort) ? Expression.Convert(left, typeof(ushort)) : left;
            }
            else if (left.Type == typeof(short) || right.Type == typeof(short))
            {
                right = right.Type != typeof(short) ? Expression.Convert(right, typeof(short)) : right;
                left = left.Type != typeof(short) ? Expression.Convert(left, typeof(short)) : left;
            }
            else if (left.Type == typeof(byte) || right.Type == typeof(byte))
            {
                right = right.Type != typeof(byte) ? Expression.Convert(right, typeof(byte)) : right;
                left = left.Type != typeof(byte) ? Expression.Convert(left, typeof(byte)) : left;
            }
        }

        public Expression MyTest(ParameterExpression[] parameters, string name, FilterComparator comparator,
            params object[] values)
        {
            var it = parameters[0];
            Type type = null;
            if (it != null)
                type = it.Type;
            var member = FindPropertyOrField(type, name, it == null);
            Expression expr = null;
            var property = member as PropertyInfo;
            if (property != null)
                expr = Expression.Property(it, property);
            var args = new List<Expression>();
            foreach (var t in values)
                if (!(t is Expression expression))
                {
                    expression = Expression.Constant(t);
                    args.Add(expression);
                }
            type = expr.Type;
            var argsArray = args.ToArray();
            if (argsArray.Length == 0)
                throw new ArgumentNullException(nameof(argsArray), "");
            if (argsArray.Length == 1 || property.PropertyType == typeof(string))
            {
                switch (comparator)
                {
                    case FilterComparator.Contains:
                        expr = GetContainsExpr(type, expr, argsArray, "Contains");
                        break;
                    case FilterComparator.Equals:
                    case FilterComparator.GreaterThan:
                    case FilterComparator.GreaterOrEquals:
                    case FilterComparator.LessOrEquals:
                    case FilterComparator.LessThan:
                    case FilterComparator.Unequals:
                        expr = GetComparisonExpression(expr, comparator, args[0]);
                        break;
                    case FilterComparator.Default:
                        throw new ArgumentOutOfRangeException(nameof(comparator), "");
                    default:
                        throw new NotImplementedException();
                }
            }
            else if (argsArray.Length > 1)
            {
                Expression left = null;
                Expression right = null;
                foreach (var t in argsArray)
                {
                    switch (comparator)
                    {
                        case FilterComparator.Equals:
                        case FilterComparator.GreaterThan:
                        case FilterComparator.GreaterOrEquals:
                        case FilterComparator.LessOrEquals:
                        case FilterComparator.LessThan:
                        case FilterComparator.Unequals:
                            right = GetComparisonExpression(expr, comparator, t);
                            break;
                        case FilterComparator.Default:
                            throw new ArgumentOutOfRangeException(nameof(comparator), "");
                        default:
                            throw new NotImplementedException();
                    }
                    left = left == null ? right : Expression.OrElse(left, right);
                }
                expr = left;
            }
            return expr;
        }

        private Expression GetContainsExpr(Type type, Expression instance, Expression[] args, string id)
        {
            if (instance != null && type != typeof(string))
            {
                var enumerableType = FindGenericType(typeof(IEnumerable<>), type);
                if (enumerableType != null)
                {
                    var elementType = enumerableType.GetTypeInfo().GetGenericTypeArguments()[0];
                    return ParseAggregate(instance, elementType, id, -1);
                }
            }
            MethodBase mb;
            switch (FindMethod(type, id, instance == null, args, out mb))
            {
                case 0:
                    throw new ParseException(Res.NoApplicableMethod);

                case 1:
                    var method = (MethodInfo) mb;
                    if (!IsPredefinedType(method.DeclaringType) &&
                        !(method.IsPublic && IsPredefinedType(method.ReturnType)))
                        throw new ParseException(Res.MethodsAreInaccessible);

                    if (method.ReturnType == typeof(void))
                        throw new ParseException(Res.MethodIsVoid);

                    return Expression.Call(instance, method, args);

                default:
                    throw new ParseException(Res.AmbiguousMethodInvocation);
            }
        }

        private Expression GetComparisonExpression(Expression left, FilterComparator comparator, Expression right)
        {
            ConstantExpression constantExpr;
            TypeConverter typeConverter;
            var isEquality = comparator == FilterComparator.Equals || comparator == FilterComparator.Unequals;

            if (isEquality && (!left.Type.GetTypeInfo().IsValueType && !right.Type.GetTypeInfo().IsValueType ||
                               left.Type == typeof(Guid) && right.Type == typeof(Guid)))
            {
                if (left.Type != right.Type)
                    if (left.Type.IsAssignableFrom(right.Type))
                        right = Expression.Convert(right, left.Type);
                    else if (right.Type.IsAssignableFrom(left.Type))
                        left = Expression.Convert(left, right.Type);
                    else
                        throw IncompatibleOperandsError("", left, right, -1);
            }
            else if (IsEnumType(left.Type) || IsEnumType(right.Type))
            {
                if (left.Type != right.Type)
                {
                    Expression e;
                    if ((e = PromoteExpression(right, left.Type, true, false)) != null)
                        right = e;
                    else if ((e = PromoteExpression(left, right.Type, true, false)) != null)
                        left = e;
                    else if (IsEnumType(left.Type) && (constantExpr = right as ConstantExpression) != null)
                        right = ParseEnumToConstantExpression(left.Type, constantExpr);
                    else if (IsEnumType(right.Type) && (constantExpr = left as ConstantExpression) != null)
                        left = ParseEnumToConstantExpression(right.Type, constantExpr);
                    else
                        throw IncompatibleOperandsError("", left, right, -1);
                }
            }
            else if ((constantExpr = right as ConstantExpression) != null && constantExpr.Value is string &&
                     (typeConverter = TypeConverterFactory.GetConverter(left.Type)) != null)
            {
                right = Expression.Constant(typeConverter.ConvertFromInvariantString((string) constantExpr.Value),
                    left.Type);
            }
            else if ((constantExpr = left as ConstantExpression) != null && constantExpr.Value is string &&
                     (typeConverter = TypeConverterFactory.GetConverter(right.Type)) != null)
            {
                left = Expression.Constant(typeConverter.ConvertFromInvariantString((string) constantExpr.Value),
                    right.Type);
            }
            else
            {
                var typesAreSameAndImplementCorrectInterface = false;
                if (left.Type == right.Type)
                {
                    var interfaces = left.Type.GetInterfaces().Where(x => x.GetTypeInfo().IsGenericType);
                    if (isEquality)
                        typesAreSameAndImplementCorrectInterface =
                            interfaces.Any(x => x.GetGenericTypeDefinition() == typeof(IEquatable<>));
                    else
                        typesAreSameAndImplementCorrectInterface =
                            interfaces.Any(x => x.GetGenericTypeDefinition() == typeof(IComparable<>));
                }

                if (!typesAreSameAndImplementCorrectInterface)
                    CheckAndPromoteOperands(isEquality ? typeof(IEqualitySignatures) : typeof(IRelationalSignatures),
                        ref left, ref right);
            }

            switch (comparator)
            {
                case FilterComparator.Equals:
                    left = GenerateEqual(left, right);
                    break;
                case FilterComparator.Unequals:
                    left = GenerateNotEqual(left, right);
                    break;
                case FilterComparator.GreaterThan:
                    left = GenerateGreaterThan(left, right);
                    break;
                case FilterComparator.GreaterOrEquals:
                    left = GenerateGreaterThanEqual(left, right);
                    break;
                case FilterComparator.LessThan:
                    left = GenerateLessThan(left, right);
                    break;
                case FilterComparator.LessOrEquals:
                    left = GenerateLessThanEqual(left, right);
                    break;
            }
            return left;
        }

        private ConstantExpression ParseEnumToConstantExpression(Type leftType, ConstantExpression constantExpr)
        {
            return Expression.Constant(ParseConstantExpressionToEnum(leftType, constantExpr), leftType);
        }

        private object ParseConstantExpressionToEnum(Type leftType, ConstantExpression constantExpr)
        {
            try
            {
                if (constantExpr.Value is string)
                    return Enum.Parse(GetNonNullableType(leftType), (string) constantExpr.Value, true);

                return Enum.ToObject(leftType, constantExpr.Value);
            }
            catch
            {
                throw ParseError(Res.ExpressionTypeMismatch);
            }
        }

        private void CheckAndPromoteOperands(Type signatures, ref Expression left, ref Expression right)
        {
            Expression[] args = {left, right};

            MethodBase method;
            if (FindMethod(signatures, "F", false, args, out method) != 1)
                throw ParseError("", left, right, -1);

            left = args[0];
            right = args[1];
        }

        private interface ILogicalSignatures
        {
            void F(bool x, bool y);
            void F(bool? x, bool? y);
        }

        private interface IShiftSignatures
        {
            void F(int x, int y);
            void F(uint x, int y);
            void F(long x, int y);
            void F(ulong x, int y);
            void F(int? x, int y);
            void F(uint? x, int y);
            void F(long? x, int y);
            void F(ulong? x, int y);
            void F(int x, int? y);
            void F(uint x, int? y);
            void F(long x, int? y);
            void F(ulong x, int? y);
            void F(int? x, int? y);
            void F(uint? x, int? y);
            void F(long? x, int? y);
            void F(ulong? x, int? y);
        }

        private interface IArithmeticSignatures
        {
            void F(int x, int y);
            void F(uint x, uint y);
            void F(long x, long y);
            void F(ulong x, ulong y);
            void F(float x, float y);
            void F(double x, double y);
            void F(decimal x, decimal y);
            void F(int? x, int? y);
            void F(uint? x, uint? y);
            void F(long? x, long? y);
            void F(ulong? x, ulong? y);
            void F(float? x, float? y);
            void F(double? x, double? y);
            void F(decimal? x, decimal? y);
        }

        private interface IRelationalSignatures : IArithmeticSignatures
        {
            void F(string x, string y);
            void F(char x, char y);
            void F(DateTime x, DateTime y);
            void F(DateTimeOffset x, DateTimeOffset y);
            void F(TimeSpan x, TimeSpan y);
            void F(char? x, char? y);
            void F(DateTime? x, DateTime? y);
            void F(DateTimeOffset? x, DateTimeOffset? y);
            void F(TimeSpan? x, TimeSpan? y);
        }

        private interface IEqualitySignatures : IRelationalSignatures
        {
            void F(bool x, bool y);
            void F(bool? x, bool? y);

            // Disabled 4 lines below because of : https://github.com/StefH/System.Linq.Dynamic.Core/issues/19
            //void F(DateTime x, string y);
            //void F(DateTime? x, string y);
            //void F(string x, DateTime y);
            //void F(string x, DateTime? y);

            void F(Guid x, Guid y);
            void F(Guid? x, Guid? y);
            void F(Guid x, string y);
            void F(Guid? x, string y);
            void F(string x, Guid y);
            void F(string x, Guid? y);
        }

        private interface IAddSignatures : IArithmeticSignatures
        {
            void F(DateTime x, TimeSpan y);
            void F(TimeSpan x, TimeSpan y);
            void F(DateTime? x, TimeSpan? y);
            void F(TimeSpan? x, TimeSpan? y);
        }

        private interface ISubtractSignatures : IAddSignatures
        {
            void F(DateTime x, DateTime y);
            void F(DateTime? x, DateTime? y);
        }

        private interface INegationSignatures
        {
            void F(int x);
            void F(long x);
            void F(float x);
            void F(double x);
            void F(decimal x);
            void F(int? x);
            void F(long? x);
            void F(float? x);
            void F(double? x);
            void F(decimal? x);
        }

        private interface INotSignatures
        {
            void F(bool x);
            void F(bool? x);
        }

        private interface IEnumerableSignatures
        {
            void All(bool predicate);
            void Any();
            void Any(bool predicate);
            void Average(decimal? selector);
            void Average(decimal selector);
            void Average(double? selector);
            void Average(double selector);
            void Average(float? selector);
            void Average(float selector);
            void Average(int? selector);
            void Average(int selector);
            void Average(long? selector);
            void Average(long selector);
            void Contains(object selector);
            void Count();
            void Count(bool predicate);
            void DefaultIfEmpty();
            void DefaultIfEmpty(object defaultValue);
            void Distinct();
            void First(bool predicate);
            void FirstOrDefault(bool predicate);
            void GroupBy(object keySelector);
            void GroupBy(object keySelector, object elementSelector);
            void Last(bool predicate);
            void LastOrDefault(bool predicate);
            void Max(object selector);
            void Min(object selector);
            void OrderBy(object selector);
            void OrderByDescending(object selector);
            void Select(object selector);
            void SelectMany(object selector);
            void Single(bool predicate);
            void SingleOrDefault(bool predicate);
            void Skip(int count);
            void SkipWhile(bool predicate);
            void Sum(decimal? selector);
            void Sum(decimal selector);
            void Sum(double? selector);
            void Sum(double selector);
            void Sum(float? selector);
            void Sum(float selector);
            void Sum(int? selector);
            void Sum(int selector);
            void Sum(long? selector);
            void Sum(long selector);
            void Take(int count);
            void TakeWhile(bool predicate);
            void ThenBy(object selector);
            void ThenByDescending(object selector);
            void Where(bool predicate);

            // Executors
            void First();

            void FirstOrDefault();
            void Last();
            void LastOrDefault();
            void Single();
            void SingleOrDefault();
            void ToArray();
            void ToList();
        }

        private class MethodData
        {
            public Expression[] Args;
            public MethodBase MethodBase;
            public ParameterInfo[] Parameters;
        }

        private enum CompareConversionType
        {
            Both = 0,
            First = 1,
            Second = -1
        }
    }
}