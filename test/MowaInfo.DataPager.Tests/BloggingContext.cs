using Microsoft.EntityFrameworkCore;

namespace MowaInfo.DataPager.Tests
{
    public class BloggingContext : DbContext
    {
        public DbSet<Blog> Blogs { get; set; }

        #region OnConfiguring

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                optionsBuilder.UseSqlServer(@"Server=(localdb)\mssqllocaldb;Database=EFProviders.InMemory;Trusted_Connection=True;");
            }
        }

        #endregion

        #region Constructors

        public BloggingContext()
        {
        }

        public BloggingContext(DbContextOptions<BloggingContext> options)
            : base(options)
        {
        }

        #endregion
    }
}
