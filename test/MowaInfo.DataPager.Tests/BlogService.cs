namespace MowaInfo.DataPager.Tests
{
    public class BlogService
    {
        private readonly BloggingContext _context;

        public BlogService(BloggingContext context)
        {
            _context = context;
        }

        public void Add(Blog blog)
        {
            _context.Blogs.Add(blog);
            _context.SaveChanges();
        }
    }
}
