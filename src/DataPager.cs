using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Reflection;
using System.Threading.Tasks;
using AutoMapper.QueryableExtensions;
using Microsoft.EntityFrameworkCore;

namespace MowaInfo.DataPager
{
    public static class DataPager
    {
        private static IQueryable<T> ParseParam<T>(this IQueryable<T> source, PagingParam param)
        {
            return (IQueryable<T>) ParseParam((IQueryable) source, param);
        }

        private static IQueryable ParseParam(this IQueryable source, PagingParam param)
        {
            if (param is SafePagingParam safeParam)
            {
                param.OrderBy = param.OrderBy.Where(field => safeParam.InternalOrderableFields.Contains(field)).ToArray();
            }

            return source
                .FilterBy(param)
                .OrderBy(param.OrderBy, param.Descending);
        }

        public static async Task<DataPage<TDto>> MapPageAsync<TDto>(this IQueryable source, PagingParam param)
        {
            return await source
                .ParseParam(param)
                .ProjectTo<TDto>()
                .PageAsync(param.Page, param.PageSize);
        }

        public static async Task<DataPage<T>> PageAsync<T>(this IQueryable<T> source, PagingParam param)
        {
            return await source
                .ParseParam(param)
                .PageAsync(param.Page, param.PageSize);
        }

        public static DataPage<TDto> MapPage<TDto>(this IQueryable source, PagingParam param)
        {
            return source
                .ParseParam(param)
                .ProjectTo<TDto>()
                .Page(param.Page, param.PageSize);
        }

        public static DataPage<T> Page<T>(this IQueryable<T> source, PagingParam param)
        {
            return source
                .ParseParam(param)
                .Page(param.Page, param.PageSize);
        }

        public static IQueryable<T> FilterBy<T>(this IQueryable<T> source, PagingParam param)
        {
            return (IQueryable<T>) FilterBy((IQueryable) source, param);
        }

        public static IQueryable FilterBy(this IQueryable source, PagingParam param)
        {
            var sourceProperties = source.ElementType.GetProperties();

            var filter = param.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.DeclaringType != typeof(PagingParam))
                .Select(p => new KeyValuePair<PropertyInfo, object>(p, p.GetValue(param)))
                .Where(p => p.Value != null)
                .ToArray();

            foreach (var kvp in filter)
            {
                var filterAttribute = kvp.Key.GetCustomAttribute<PropertyFilterAttribute>();
                if (filterAttribute == null)
                {
                    continue;
                }

                var name = filterAttribute.Name ?? kvp.Key.Name;
                var comparator = filterAttribute.Comparator;
                var sourceProperty = sourceProperties.FirstOrDefault(p => p.Name == name);

                if (sourceProperty == null)
                {
                    throw new ArgumentException($"Could not found property `{name}` of queried type `{source.ElementType.Name}`", kvp.Key.Name);
                }

                if (comparator == FilterComparator.Default)
                {
                    comparator = typeof(IEnumerable).IsAssignableFrom(sourceProperty.PropertyType) ? FilterComparator.Contains : FilterComparator.Equals;
                }

                if (kvp.Value is IEnumerable values && kvp.Key.PropertyType != typeof(string))
                {
                    var args = values as object[] ?? values.Cast<object>().ToArray();
                    var predicts = Enumerable.Range(0, args.Length).Select(i => QueryGenerator.WherePredict(name, comparator, i));
                    source = source.Where(string.Join(" or ", predicts), args);
                }
                else
                {
                    var predict = QueryGenerator.WherePredict(name, comparator);
                    if (predict != null)
                    {
                        source = source.Where(predict, kvp.Value);
                    }
                }
            }

            source = param.CustomFilter(source);

            return source;
        }

        public static IQueryable<T> OrderBy<T>(this IQueryable<T> source, string[] fields, bool[] descendings)
        {
            return (IQueryable<T>) OrderBy((IQueryable) source, fields, descendings);
        }

        public static IQueryable OrderBy(this IQueryable source, string[] fields, bool[] descendings)
        {
            if (!(fields?.Length > 0))
            {
                return source;
            }

            var orders = new List<string>();
            for (var i = 0; i < fields.Length; i++)
            {
                var field = fields[i];
                if (descendings?.Length > i && descendings[i])
                {
                    field += " DESC";
                }

                orders.Add(field);
            }

            return source.OrderBy(string.Join(", ", orders));
        }

        public static async Task<DataPage<T>> PageAsync<T>(this IQueryable<T> source, int pageIndex, int pageSize)
        {
            return new DataPage<T>
            {
                Data = await source.Skip((pageIndex - 1) * pageSize).Take(pageSize).ToArrayAsync(),
                Total = await source.CountAsync()
            };
        }

        public static DataPage<T> Page<T>(this IQueryable<T> source, int pageIndex, int pageSize)
        {
            return new DataPage<T>
            {
                Data = source.Skip((pageIndex - 1) * pageSize).Take(pageSize).ToArray(),
                Total = source.Count()
            };
        }
    }
}
