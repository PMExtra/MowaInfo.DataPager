using System;

namespace MowaInfo.DataPager.Tests
{
    public class Blog
    {
        public int BlogId { get; set; }

        public string Url { get; set; }

        public string BlogTitle { get; set; }

        public string BlogType { get; set; }

        public int UserId { get; set; }

        public string UserName { get; set; }

        public DateTime PublishTime { get; set; }

        public int TimesWatched { get; set; }

        public int TimesLiked { get; set; }

        public int Gift { get; set; }
    }
}
