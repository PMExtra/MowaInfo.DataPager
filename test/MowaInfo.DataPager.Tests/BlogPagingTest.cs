using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace MowaInfo.DataPager.Tests
{
    public class Compare : IEqualityComparer<Blog>
    {
        public bool Equals(Blog x, Blog y)
        {
            return x.BlogId == y.BlogId && x.Url == y.Url && x.BlogTitle == y.BlogTitle && x.BlogType == y.BlogType && x.UserId == y.UserId && x.UserName == y.UserName &&
                   x.PublishTime == y.PublishTime && x.TimesWatched == y.TimesWatched && x.TimesLiked == y.TimesLiked && x.Gift == y.Gift;
        }

        public int GetHashCode(Blog obj)
        {
            throw new NotImplementedException();
        }
    }

    public class BlogPagingTest
    {
        private readonly Compare _compare = new Compare();

        private DbContextOptions<BloggingContext> _options;

        private void Init(string dbName)
        {
            _options = new DbContextOptionsBuilder<BloggingContext>()
                .UseInMemoryDatabase(dbName)
                .Options;

            using (var context = new BloggingContext(_options))
            {
                var service = new BlogService(context);
                for (var i = 0; i < 5; i++)
                {
                    service.Add(new Blog
                    {
                        BlogId = i + 1,
                        Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                        BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                        BlogType = "c",
                        UserId = i,
                        UserName = "d",
                        PublishTime = DateTime.Now,
                        TimesWatched = i * 10, //0,10,20,30,40
                        TimesLiked = i * 2, // 0, 2, 4, 6, 8
                        Gift = i
                    });
                }
                for (var i = 0; i < 5; i++)
                {
                    service.Add(new Blog
                    {
                        BlogId = i + 6,
                        Url = "ea",
                        BlogTitle = "bf",
                        BlogType = "gc",
                        UserId = i,
                        UserName = "h",
                        PublishTime = DateTime.Now,
                        TimesWatched = 1 + i * 10, //01,11,21,31,41
                        TimesLiked = 1 + i * 2, // 1, 3, 5, 7, 9
                        Gift = 1 + i
                    });
                }
                for (var i = 0; i < 5; i++)
                {
                    service.Add(new Blog
                    {
                        BlogId = i + 11,
                        Url = "i",
                        BlogTitle = "j",
                        BlogType = "k",
                        UserId = i,
                        UserName = "l",
                        PublishTime = DateTime.Now,
                        TimesWatched = 2 + i * 10, //02,12,22,32,42
                        TimesLiked = 2 + i * 2, // 2, 4, 6, 8, 10
                        Gift = 2 + i
                    });
                }
            }
        }

        // BlogId  Default, int
        [Fact]
        public void BlogIdDefaultInt()
        {
            Init("Test2");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    BlogId = 3
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 1);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 3,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 2,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 20,
                    TimesLiked = 4,
                    Gift = 2
                }, _compare);
            }
        }

        // BlogTitle  Contains
        [Fact]
        public void BlogTitleContains()
        {
            Init("Test4");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    BlogTitle = "b"
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 10);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // BlogType  Default, String
        [Fact]
        public void BlogTypeDefaultString()
        {
            Init("Test11");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    BlogType = "c"
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 10);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // Gift  GreaterThan
        [Fact]
        public void GiftGreaterThan()
        {
            Init("Test5");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    Gift = 4
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 3);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 10,
                    Url = "ea",
                    BlogTitle = "bf",
                    BlogType = "gc",
                    UserId = 4,
                    UserName = "h",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 41,
                    TimesLiked = 9,
                    Gift = 5
                }, _compare);
            }
        }

        // 没有FilterComparator
        [Fact]
        public void NoFilter()
        {
            Init("Test1");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 15);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // PublishTime  Equals
        [Fact]
        public void PublishTimeEquals()
        {
            Init("Test3");
            var time = DateTime.UtcNow;
            using (var context = new BloggingContext(_options))
            {
                var service = new BlogService(context);
                service.Add(new Blog
                {
                    BlogId = 16,
                    Url = "i",
                    BlogTitle = "j",
                    BlogType = "k",
                    UserId = 16,
                    UserName = "l",
                    PublishTime = time,
                    TimesWatched = 162,
                    TimesLiked = 34,
                    Gift = 18
                });
            }
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    PublishTime = time
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 1);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 16,
                    Url = "i",
                    BlogTitle = "j",
                    BlogType = "k",
                    UserId = 16,
                    UserName = "l",
                    PublishTime = time,
                    TimesWatched = 162,
                    TimesLiked = 34,
                    Gift = 18
                }, _compare);
            }
        }

        // TimesLiked  LessThan
        [Fact]
        public void TimesLikedLessThan()
        {
            Init("Test7");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    TimesLiked = 6
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 8);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // TimesWatched  LessOrEquals
        [Fact]
        public void TimesWatchedLessOrEquals()
        {
            Init("Test8");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    TimesWatched = 21
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 8);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // Url Unequals
        [Fact]
        public void UrlUnequals()
        {
            Init("Test9");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    Url = "ea"
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 10);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }

        // UserId  GreaterOrEquals
        [Fact]
        public void UserIdGreaterOrEquals()
        {
            Init("Test6");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    UserId = 3
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 6);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 4,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 3,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 30,
                    TimesLiked = 6,
                    Gift = 3
                }, _compare);
            }
        }

        //UserName Custom
        [Fact]
        public void UserNameCustom()
        {
            Init("Test10");
            using (var context = new BloggingContext(_options))
            {
                var param = new BlogPagingParam
                {
                    Page = 1,
                    Descending = new[] { true },
                    PageSize = 2,
                    UserName = "l"
                };
                var a = context.Blogs.AsNoTracking()
                    .Page(param);
                Assert.Equal(a.Total, 15);
                // ReSharper disable once GenericEnumeratorNotDisposed
                var one = a.Data.GetEnumerator();
                one.MoveNext();
                var cur = one.Current;
                Assert.Equal(cur, new Blog
                {
                    BlogId = 1,
                    Url = "aaaaaaaaaaaaaaaaaaaaaaaaaa",
                    BlogTitle = "bbbbbbbbbbbbbbbbbbb",
                    BlogType = "c",
                    UserId = 0,
                    UserName = "d",
                    PublishTime = cur.PublishTime,
                    TimesWatched = 0,
                    TimesLiked = 0,
                    Gift = 0
                }, _compare);
            }
        }
    }
}
